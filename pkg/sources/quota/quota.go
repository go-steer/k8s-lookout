// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package quota is the quota signal source (DESIGN.md §7.2 row 8,
// §10.2): a leading countdown over cloud quota exhaustion, deployed
// ONCE PER GCP PROJECT — fifty clusters in a project must not each
// poll the quota APIs, so Scope() is Project (§11) and exactly one
// sentinel per project enables this source.
//
// Each poll (Config.Poll, default 15m) reads the provider's quota
// inventory (cloud.QuotaAPI.Quotas — on GKE the cheap-80% compute
// regions.get/projects.get usage/limit pairs) and, for the WATCHED
// quotas only (the Config.WatchTop nearest exhaustion by usage/limit
// ratio, plus every quota at or above Config.WarnPct), fetches
// usage-vs-limit history (cloud.QuotaAPI.History — on GKE the Cloud
// Monitoring serviceruntime quota series) over Config.Window
// (default 7d) and applies the saturation source's regression
// (saturation.LeastSquaresSlope — §10.2: the slope math applies
// directly): not "at 87%" but "exhausted in ~6 days at current
// slope".
//
// It emits `quota.forecast` with Forecast{ETA, "linear-7d-window"}
// when a positive-slope projection exists, evidence in the message
// (quota name, region, usage/limit, slope/day), and — always — the
// §10.3 DRAFTED increase request attached (draft.go documents the
// suggested-limit formula and the justification text). lookout only
// drafts: the agent files the request through core-agent's
// permission gate; nothing here mutates quota.
//
// Severity (design-fixed, not flags):
//
//   - warning:  ETA < 7d, or usage ≥ 90% of limit;
//   - critical: ETA < 48h, or usage ≥ 98% of limit.
//
// Forecast gates (§13 trend testing, same posture as saturation): no
// ETA when the usage history has < minPoints samples or spans <
// Window/2 (quota allocation series are ~daily points, so 7d of
// history is a handful of samples — the gates are sized for that),
// and no ETA when the fitted slope is non-positive; a quota already
// at/over a usage threshold still fires on the threshold alone, with
// no Forecast attachment.
//
// Hysteresis: the poll interval (15m) is far longer than the default
// dedup window (5m), so re-emitting every poll would open a fresh
// session each time. Each quota UID latches the highest severity it
// fired; the same severity never re-fires, escalation
// (warning→critical) fires once more, and the latch releases — a
// fresh approach then fires again — only when the quota has receded
// with margin: usage below releaseUsageFrac (85%) AND (slope
// non-positive OR ETA beyond 2×7d).
//
// Correlation (§10.3): the dedup identity is the QUOTA, not any
// nodegroup — Key.UID is UID(name, scope) ("quota:CPUS/us-east1")
// and Key.Reason is "quota_forecast", canonicalized (engine
// reasonCanonical, append-only) into the QuotaExhausted family
// together with the capacity source's "quota_blocked". A
// GCE_QUOTA_EXCEEDED scaleup failure whose decision message names
// the same quota therefore dedups into the open quota session and
// attaches as a followup instead of opening a second incident —
// see UIDFromDecisionMessage (uid.go) for the reactive side's key.
//
// Provider boundary (AGENTS.md): this package imports pkg/cloud,
// never a cloud SDK. Constructing the source WITHOUT a quota-capable
// provider is a hard error, not a degraded mode: a project-tier
// deployment without a cloud makes no sense, so New fails loudly
// naming the source (§11 posture), and `lookout watch` refuses to
// start.
package quota

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "quota"

// KindForecast is the one kind this source emits (§7.3). APPEND-ONLY.
const KindForecast = "quota.forecast"

// Reason is the dedup/fingerprint reason on every quota.forecast
// signal. Canonicalized to QuotaExhausted (engine reasonCanonical) —
// the family it shares with the capacity source's quota_blocked.
const Reason = "quota_forecast"

// Design-fixed thresholds (§10.2; deliberately not flags — the
// warn/crit geometry is part of the hysteresis contract):
const (
	// WarnETA: a positive-slope projection inside 7 days fires at
	// warning.
	WarnETA = 7 * 24 * time.Hour
	// CritETA: inside 48 hours fires at critical.
	CritETA = 48 * time.Hour
	// WarnUsageFrac / CritUsageFrac: usage-ratio thresholds that
	// fire even without a trustworthy slope.
	WarnUsageFrac = 0.90
	CritUsageFrac = 0.98
	// releaseUsageFrac is the hysteresis release margin: the latch
	// only releases once usage recedes below 85% (and the ETA is no
	// longer urgent), so a quota oscillating around a threshold
	// neither re-fires every poll nor clears prematurely.
	releaseUsageFrac = 0.85
	// minPoints is the minimum usage-history sample count for a fit.
	// Quota allocation series are ~daily points; 4 points over at
	// least half the 7d window is the smallest span worth trusting.
	minPoints = 4
)

// Config are the source's knobs. Zero values take the shipped
// defaults.
type Config struct {
	// Poll is the inventory/history poll interval (`--quota-poll`).
	// Default 15m — quota moves on human timescales; the APIs are
	// per-project and metered.
	Poll time.Duration
	// Window is the history window the slope is fitted over
	// (`--quota-window`). Default 7d — the §8
	// "linear-7d-window" confidence basis.
	Window time.Duration
	// WarnPct is the usage/limit ratio above which a quota is always
	// watched (history fetched every poll) regardless of ranking
	// (`--quota-warn`). Default 0.80 — the cheap 80%.
	WarnPct float64
	// WatchTop is how many quotas nearest exhaustion (highest
	// usage/limit ratio) are watched in addition to everything above
	// WarnPct. Default 10. Deliberately not a flag: it bounds API
	// spend, not alerting policy — WarnPct is the policy knob.
	WatchTop int
}

// DefaultConfig returns the shipped knobs.
func DefaultConfig() Config {
	return Config{
		Poll:     15 * time.Minute,
		Window:   7 * 24 * time.Hour,
		WarnPct:  0.80,
		WatchTop: 10,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.Poll <= 0 {
		c.Poll = d.Poll
	}
	if c.Window <= 0 {
		c.Window = d.Window
	}
	if c.WarnPct <= 0 || c.WarnPct >= 1 {
		c.WarnPct = d.WarnPct
	}
	if c.WatchTop <= 0 {
		c.WatchTop = d.WatchTop
	}
	return c
}

// basis is the §8 confidence-basis string for this window.
func (c Config) basis() string {
	if days := c.Window / (24 * time.Hour); days >= 1 && c.Window == days*24*time.Hour {
		return fmt.Sprintf("linear-%dd-window", days)
	}
	return fmt.Sprintf("linear-%dh-window", int(c.Window.Hours()))
}

// minSpan is the smallest usage-history span a fit is trusted over.
func (c Config) minSpan() time.Duration { return c.Window / 2 }

// Source implements sources.Source for the quota row of §7.2.
type Source struct {
	api cloud.QuotaAPI
	cfg Config

	mu   sync.Mutex
	emit func(engine.Signal)
	// fired is the per-quota hysteresis latch: UID → highest
	// severity fired. See the package comment.
	fired map[string]engine.Severity
	// inventoryFailing / historyFailing throttle the transient-error
	// logs to state edges (the §11 loud-not-silent posture for a
	// running source, without a log line per poll).
	inventoryFailing bool
	historyFailing   map[string]bool

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
	// logf overrides log.Printf for testing. nil = log.Printf.
	logf func(format string, args ...any)
}

// New constructs the source from the §2 provider boundary. A
// provider without the quota capability is a STARTUP ERROR naming
// the source (§11): the quota source is the project-tier deployment
// (§11 Project row), and a project-tier deployment without a cloud
// provider makes no sense — failing loudly here beats a resident
// process that silently watches nothing. Zero-valued cfg fields take
// the shipped defaults.
func New(provider cloud.Provider, cfg Config) (*Source, error) {
	if provider == nil {
		provider = cloud.NoProvider
	}
	api, ok := provider.Quota()
	if !ok {
		u := cloud.Unavailable(provider, cloud.CapabilityQuota)
		return nil, fmt.Errorf(
			"source %q requires a cloud provider with the %s capability (Scope()=%s — one instance per GCP project, §10.2/§11): %s; build with -tags gke/allproviders and run with cloud credentials, or drop %q from --sources",
			Name, cloud.CapabilityQuota, sources.ScopeProject, u.Marker(), Name)
	}
	return &Source{
		api:            api,
		cfg:            cfg.normalize(),
		fired:          make(map[string]engine.Severity),
		historyFailing: make(map[string]bool),
	}, nil
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: one instance per GCP project,
// regardless of cluster count (§10.2 normative, §11 Project tier).
// No Kubernetes RBAC at all — the source deliberately does not
// implement sources.AccessDeclarer; its capability probe is New's
// provider check.
func (s *Source) Scope() sources.Scope { return sources.ScopeProject }

func (s *Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Source) printf(format string, args ...any) {
	if s.logf != nil {
		s.logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// send delivers signals to the pipeline. Never called under s.mu.
func (s *Source) send(sigs []engine.Signal) {
	s.mu.Lock()
	emit := s.emit
	s.mu.Unlock()
	if emit == nil {
		return // not running (unit tests drive pollOnce directly)
	}
	for _, sig := range sigs {
		emit(sig)
	}
}

// Run implements sources.Source: polls once synchronously — a quota
// inventory that cannot be read at startup is a credentials/API-
// enablement error the operator must see (§11 fail loudly), not a
// silent gap — then drives the poll ticker until ctx is cancelled.
// Later transient failures skip the cycle with an edge-throttled log.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	first, err := s.poll(ctx, s.clock())
	if err != nil {
		return fmt.Errorf("quota: quota inventory unavailable at startup: %w (fix cloud credentials / API enablement, or drop %q from --sources)", err, Name)
	}
	s.send(first)

	ticker := time.NewTicker(s.cfg.Poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			sigs, err := s.poll(ctx, s.clock())
			if err != nil {
				// Transient (startup already proved the API works):
				// skip the cycle, log the edge only.
				s.mu.Lock()
				firstFailure := !s.inventoryFailing
				s.inventoryFailing = true
				s.mu.Unlock()
				if firstFailure {
					s.printf("quota: inventory poll failed (%v) — skipping cycles until it recovers", err)
				}
				continue
			}
			s.mu.Lock()
			if s.inventoryFailing {
				s.inventoryFailing = false
				s.mu.Unlock()
				s.printf("quota: inventory poll recovered")
			} else {
				s.mu.Unlock()
			}
			s.send(sigs)
		}
	}
}

// poll runs one full cycle: inventory → watchlist → per-quota
// history + fit → signals. Exported to the package's tests, which
// drive it with a fake clock and a scripted cloud.QuotaAPI. An
// inventory error fails the cycle (the caller decides fatal vs
// skip); a HISTORY error only degrades that quota to threshold-only
// judgment (usage ratio still fires; the log is edge-throttled per
// quota).
func (s *Source) poll(ctx context.Context, now time.Time) ([]engine.Signal, error) {
	inventory, err := s.api.Quotas(ctx)
	if err != nil {
		return nil, err
	}
	var out []engine.Signal
	for _, q := range watchlist(inventory, s.cfg.WarnPct, s.cfg.WatchTop) {
		hist, err := s.api.History(ctx, q.Name, q.Scope, cloud.TimeWindow{Start: now.Add(-s.cfg.Window), End: now})
		key := UID(q.Name, q.Scope)
		if err != nil {
			s.mu.Lock()
			firstFailure := !s.historyFailing[key]
			s.historyFailing[key] = true
			s.mu.Unlock()
			if firstFailure {
				s.printf("quota: history for %s unavailable (%v) — judging on usage thresholds alone until it recovers", key, err)
			}
			hist = cloud.QuotaHistory{Name: q.Name, Scope: q.Scope}
		} else {
			s.mu.Lock()
			if s.historyFailing[key] {
				delete(s.historyFailing, key)
				s.mu.Unlock()
				s.printf("quota: history for %s recovered", key)
			} else {
				s.mu.Unlock()
			}
		}
		if sig := s.judge(q, hist, now); sig != nil {
			out = append(out, *sig)
		}
	}
	return out, nil
}

// watchlist selects the quotas worth a history query this cycle
// (§10.2): every quota at or above warnPct of its limit, plus the
// top `top` nearest exhaustion by usage/limit ratio. Quotas with no
// limit (unlimited or unreported) are never watched.
func watchlist(inventory []cloud.QuotaUsage, warnPct float64, top int) []cloud.QuotaUsage {
	eligible := make([]cloud.QuotaUsage, 0, len(inventory))
	for _, q := range inventory {
		if q.Limit > 0 {
			eligible = append(eligible, q)
		}
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		return eligible[i].Usage/eligible[i].Limit > eligible[j].Usage/eligible[j].Limit
	})
	out := eligible
	if len(eligible) > top {
		out = eligible[:top]
		for _, q := range eligible[top:] {
			if q.Usage/q.Limit >= warnPct {
				out = append(out, q)
			}
		}
	}
	return out
}

// judge evaluates one watched quota: fit the usage history, project
// the ETA, apply the severity thresholds and the hysteresis latch,
// and compose the signal (with the §10.3 draft attached) when a
// threshold newly fires.
func (s *Source) judge(q cloud.QuotaUsage, hist cloud.QuotaHistory, now time.Time) *engine.Signal {
	ratio := q.Usage / q.Limit
	slope := s.usableSlope(hist) // units per second; 0 = no projection
	slopePerDay := slope * 86400

	var (
		eta    time.Duration
		hasETA bool
	)
	if slope > 0 {
		headroom := q.Limit - q.Usage
		if headroom < 0 {
			headroom = 0
		}
		eta = time.Duration(headroom / slope * float64(time.Second))
		hasETA = true
	}

	var sev engine.Severity
	switch {
	case ratio >= CritUsageFrac || (hasETA && eta < CritETA):
		sev = engine.SeverityCritical
	case ratio >= WarnUsageFrac || (hasETA && eta < WarnETA):
		sev = engine.SeverityWarning
	}

	uid := UID(q.Name, q.Scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	if sev == "" {
		// Hysteresis release only with margin (package comment): the
		// usage has receded below releaseUsageFrac AND no urgent
		// projection remains (non-positive slope, or ETA beyond
		// 2×WarnETA).
		if ratio < releaseUsageFrac && (!hasETA || eta > 2*WarnETA) {
			delete(s.fired, uid)
		}
		return nil
	}
	if severityRank(sev) <= severityRank(s.fired[uid]) {
		return nil // same severity never re-fires; only escalation does
	}
	s.fired[uid] = sev

	draft := Draft(q, slopePerDay, eta, hasETA, now)
	sig := engine.Signal{
		Kind:     KindForecast,
		Source:   engine.SourceSentinel,
		Severity: sev,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: uid, Reason: Reason},
			KindOfObject: "Quota",
			Name:         q.Name,
			Message:      s.message(q, ratio, slopePerDay, eta, hasETA, len(hist.Usage), draft),
			FirstSeen:    now,
			LastSeen:     now,
			Count:        1,
		},
		QuotaDraft: &draft,
	}
	if hasETA {
		sig.Forecast = &engine.Forecast{ETA: now.Add(eta), ConfidenceBasis: s.cfg.basis()}
	}
	return &sig
}

// usableSlope fits the usage history and returns the slope in units
// per second, or 0 when the fit fails a trust gate (§13): fewer than
// minPoints samples, or a span under half the window.
func (s *Source) usableSlope(hist cloud.QuotaHistory) float64 {
	pts := hist.Usage
	if len(pts) < minPoints {
		return 0
	}
	if pts[len(pts)-1].Time.Sub(pts[0].Time) < s.cfg.minSpan() {
		return 0
	}
	times := make([]time.Time, len(pts))
	values := make([]float64, len(pts))
	for i, p := range pts {
		times[i], values[i] = p.Time, p.Value
	}
	return saturation.LeastSquaresSlope(times, values)
}

// message renders the evidence line (§10.2): quota name, region,
// usage/limit, slope/day, and the projection — "exhausted in ~6d at
// current slope", never just "at 87%" — plus the draft pointer.
func (s *Source) message(q cloud.QuotaUsage, ratio, slopePerDay float64, eta time.Duration, hasETA bool, points int, d engine.QuotaIncreaseDraft) string {
	head := fmt.Sprintf("quota %s in %s at %.1f%% (usage %s / limit %s%s)",
		q.Name, q.Scope, ratio*100, formatQty(q.Usage), formatQty(q.Limit), unitSuffix(q.Unit))
	var trend string
	if hasETA {
		trend = fmt.Sprintf(", growing %s/day over the last %s (%d points) — exhausted in ~%s at current slope",
			formatQty(slopePerDay), formatWindow(s.cfg.Window), points, formatETA(eta))
	} else {
		trend = " with no trustworthy growth trend (threshold crossing)"
	}
	return head + trend + fmt.Sprintf("; drafted increase to %s attached — file it via core-agent's permission gate", formatQty(d.SuggestedLimit))
}

// severityRank orders the hysteresis latch: none < warning < critical.
func severityRank(sev engine.Severity) int {
	switch sev {
	case engine.SeverityCritical:
		return 2
	case engine.SeverityWarning:
		return 1
	}
	return 0
}

// formatQty renders a quota quantity: integers without decimals
// (quota units are counts), else one decimal.
func formatQty(v float64) string {
	if v == math.Trunc(v) {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

// unitSuffix renders " <unit>" when the provider reports one.
func unitSuffix(unit string) string {
	if unit == "" {
		return ""
	}
	return " " + unit
}

// formatETA renders a duration on quota timescales (days + hours, or
// hours + minutes under two days).
func formatETA(eta time.Duration) string {
	if eta >= 48*time.Hour {
		days := int(eta.Hours()) / 24
		hours := int(eta.Hours()) % 24
		if hours == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd%dh", days, hours)
	}
	return eta.Truncate(time.Minute).String()
}

// formatWindow renders the config window ("7d" for whole days).
func formatWindow(w time.Duration) string {
	if days := w / (24 * time.Hour); days >= 1 && w == days*24*time.Hour {
		return fmt.Sprintf("%dd", days)
	}
	return w.String()
}
