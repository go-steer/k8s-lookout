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

// Package tokenburn is the token-burn signal source (DESIGN.md §7.2
// row 9, §12): token spend as a first-class saturation dimension —
// a runaway agent loop is an OOM in the currency that matters. Each
// poll (Config.Poll, default 60s) reads the core-agent cost stack
// (shipped v2.7.0: GET /sessions for the inventory, GET
// /sessions/{app}/{sid}/usage for the cumulative per-session token +
// cost totals — the #222 UsageMetadata surface) and applies the same
// slope → ETA math as the saturation source
// (saturation.LeastSquaresSlope, the shared regression seam) to each
// active session's cumulative spend series.
//
// Per-session metric: billed tokens — overall input_tokens +
// output_tokens + thoughts_tokens, a cumulative counter — kept in a
// ring buffer pruned to Config.Window (default 15m; poll-sized, not
// the saturation source's 90m: burn incidents play out in minutes).
// The RATE is the least-squares slope over that buffer (tokens/sec);
// a non-positive slope reads as idle (rate 0).
//
// Baseline (documented, load-bearing): the TRAILING MEDIAN RATE
// ACROSS SESSIONS — the median of the positive per-session rates
// observed in the same poll cycle (each of which is already a
// trailing-window slope). The median is deliberate: one runaway
// session cannot drag the baseline up behind itself as long as it is
// a minority of the fleet. Degenerate case, also deliberate: with a
// single active session the session IS the median, its multiple is
// exactly 1, and the rate trigger is inert — a lone session has no
// peers to be anomalous against; the budget trigger (below) is the
// covering alarm for that shape.
//
// Triggers (§12):
//
//   - rate: session rate ≥ Config.BurnMultiple (default 4×) × baseline,
//     sustained for ≥ sustainPolls (2) consecutive polls → severity
//     warning. Never on the first poll: a rate needs Config.MinSamples
//     (3) points before the fit is trusted at all.
//   - budget: when a per-session budget is known (Config.BudgetUSD)
//     and the cost-series slope projects exhaustion inside
//     Config.BurnETA (default 30m) — or the budget is already
//     exhausted — → severity critical, with Forecast{ETA,
//     "linear-<window>-window"} attached (budget-based forecasts
//     only; rate-based warnings carry no ETA claim). The projection
//     needs ≥ 2 cost samples, so this too never fires on the first
//     poll — one snapshot of a counter is not evidence of burn.
//
// Budget provenance — TODO(core-agent): core-agent v2.7.0 tracks
// per-session spend caps in-process (agent.CostCeiling{MaxTurnUSD,
// MaxSessionUSD}, pkg/agent/cost_ceiling.go, wired per-session since
// #275) but exposes NEITHER the ceiling nor a spend fraction on any
// attach endpoint — /usage (attach.UsageInfo) and /status
// (attach.StatusInfo) carry no budget field as of v2.7.0. Until the
// daemon surfaces its CostCeiling (natural home: a `budget_usd` /
// `budget_fraction` pair on GET /sessions/{sid}/usage), the budget
// here is lookout-side configuration: `--token-budget-usd` declares
// one fleet-wide per-session budget, and 0 (the default) means
// "unknown" — the budget trigger stays disarmed and only the
// rate trigger runs. Same posture as pkg/memory's stand-in for the
// unshipped core-agent Memory interface.
//
// Hysteresis (same contract as saturation): each session latches the
// highest severity it fired. The same severity never re-fires;
// escalation (warning → critical) fires once more. The latch
// releases — and the §7.4 clearance reports the symptom absent —
// when the burn condition stays absent (rate below the multiple AND
// no urgent budget ETA, with a hold band up to 2×BurnETA) for
// sustainPolls consecutive polls; a session that vanishes from the
// daemon's active list has its series dropped after Config.StaleAfter
// and clears as object_deleted (the session ended — the spend is
// over, but nothing was "fixed").
package tokenburn

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
)

// Name is the stable source name (§7.2 table) used in the signal
// schema and the `--sources` flag.
const Name = "token-burn"

// KindBurn is the one kind this source emits (§7.3). APPEND-ONLY.
const KindBurn = "token.burn"

// Reason is the dedup/fingerprint reason on every token.burn signal.
// Self-canonical under engine.CanonicalReason.
const Reason = "token_burn"

// sustainPolls is how many consecutive polls the rate condition must
// hold before firing, and how many calm polls release the hysteresis
// latch. Design-fixed (part of the hysteresis geometry, like
// saturation's CritETA), not a flag.
const sustainPolls = 2

// UID returns the Signal/dedup UID for a daemon session — the
// clearance lookup and the dedup key agree on it.
func UID(app, sid string) string { return "session:" + app + "/" + sid }

// Config are the source's polling and threshold knobs. Zero values
// take the defaults.
type Config struct {
	// Poll is the cost-stack poll interval (`--token-poll`).
	// Default 60s.
	Poll time.Duration
	// BurnMultiple (`--burn-multiple`): rate ≥ this multiple of the
	// cross-session baseline (sustained) fires the warning trigger.
	// Default 4.
	BurnMultiple float64
	// BurnETA (`--burn-eta`): a budget-exhaustion projection inside
	// this window fires the critical trigger. Default 30m.
	BurnETA time.Duration
	// BudgetUSD (`--token-budget-usd`) is the lookout-side
	// per-session spend budget; 0 (default) = unknown, budget
	// trigger disarmed. See the package TODO(core-agent): v2.7.0
	// does not expose its CostCeiling over the attach API.
	BudgetUSD float64
	// Window is the ring-buffer regression window. Default 15m
	// (design-fixed, not a flag — the §8 "linear-15m-window" basis).
	Window time.Duration
	// MinSamples is the minimum buffer size for a rate fit.
	// Default 3.
	MinSamples int
	// StaleAfter drops a session's series when the daemon stops
	// listing it as active for this long — the clearance then
	// reports object_deleted. Default 10m.
	StaleAfter time.Duration
}

// DefaultConfig returns the shipped knobs.
func DefaultConfig() Config {
	return Config{
		Poll:         60 * time.Second,
		BurnMultiple: 4,
		BurnETA:      30 * time.Minute,
		Window:       15 * time.Minute,
		MinSamples:   3,
		StaleAfter:   10 * time.Minute,
	}
}

// normalize fills zero fields with defaults.
func (c Config) normalize() Config {
	d := DefaultConfig()
	if c.Poll <= 0 {
		c.Poll = d.Poll
	}
	if c.BurnMultiple <= 1 {
		c.BurnMultiple = d.BurnMultiple
	}
	if c.BurnETA <= 0 {
		c.BurnETA = d.BurnETA
	}
	if c.BudgetUSD < 0 {
		c.BudgetUSD = 0
	}
	if c.Window <= 0 {
		c.Window = d.Window
	}
	if c.MinSamples < 2 {
		c.MinSamples = d.MinSamples
	}
	if c.StaleAfter <= 0 {
		c.StaleAfter = d.StaleAfter
	}
	return c
}

// clearETA is the recede threshold: a budget projection beyond it no
// longer holds the hysteresis band closed.
func (c Config) clearETA() time.Duration { return 2 * c.BurnETA }

// sample is one (time, cumulative tokens, cumulative cost)
// observation.
type sample struct {
	t      time.Time
	tokens float64
	cost   float64
}

// series is one session's ring buffer + hysteresis state.
type series struct {
	app string
	sid string

	samples []sample

	// rate is the last fitted token rate (tokens/sec; 0 = idle or
	// unfit). baseline is the cross-session median it was judged
	// against. Kept for evidence and tests.
	rate     float64
	baseline float64

	// hot counts consecutive polls with rate ≥ BurnMultiple×baseline;
	// calm counts consecutive polls with the burn condition absent.
	hot  int
	calm int
	// calmSince is when the current calm run started (zero while the
	// condition is present or in the hold band) — the clearance's
	// StableSince.
	calmSince time.Time

	// firedSeverity is the hysteresis latch: "" (none) | warning |
	// critical.
	firedSeverity engine.Severity

	// lastActive is the last poll that listed the session active.
	lastActive time.Time
}

// Source implements sources.Source (and engine.ClearanceObserver)
// for the token-burn row of §7.2.
type Source struct {
	cfg    Config
	client CostStackClient

	mu     sync.Mutex
	emit   func(engine.Signal)
	series map[string]*series // keyed by UID(app, sid)
	// polled flips true after the first successful sessions fetch;
	// Clearance declines to judge before it.
	polled bool
	// listFailing / usageFailing throttle the transient-failure logs.
	listFailing  bool
	usageFailing bool

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
	// logf overrides log.Printf for testing. nil = log.Printf.
	logf func(format string, args ...any)
}

// New constructs the source over a cost-stack client (the shipped
// wiring passes NewHTTPClient against the same daemon the injector
// talks to; tests substitute fakes).
func New(client CostStackClient, cfg Config) *Source {
	return &Source{
		cfg:    cfg.normalize(),
		client: client,
		series: make(map[string]*series),
	}
}

// Name implements sources.Source.
func (s *Source) Name() string { return Name }

// Scope implements sources.Source: the source talks ONLY to the
// core-agent daemon — no Kubernetes API at all — so it works under
// the namespace tier's bare-minimum RBAC (and declares no
// sources.Requirement for the §11 probe).
func (s *Source) Scope() sources.Scope { return sources.ScopeNamespace }

// ClearanceObserver returns the §7.4 clearance predicate for
// token.burn incidents. It claims every token_burn incident even
// while symptomatic, so no generic observer ever judges them.
func (s *Source) ClearanceObserver() engine.ClearanceObserver { return s }

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
	if len(sigs) == 0 {
		return
	}
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

// Run implements sources.Source: verifies the cost stack answers
// (fail loudly at startup, §11 — an unreachable daemon would
// otherwise be a silently empty trend source), then drives the poll
// loop until ctx is cancelled.
func (s *Source) Run(ctx context.Context, emit func(sources.Signal)) error {
	s.mu.Lock()
	s.emit = emit
	s.mu.Unlock()

	// First fetch synchronously: a dead cost stack at startup is a
	// config/version error the operator must see. The /sessions +
	// /sessions/{app}/{sid}/usage surface ships in core-agent v2.7.0.
	refs, err := s.client.Sessions(ctx)
	if err != nil {
		return fmt.Errorf("token-burn: core-agent cost stack unavailable: %w (GET /sessions — the §12 surface ships in core-agent v2.7.0; check --daemon-url / --token-endpoint and the daemon version, or disable the source)", err)
	}
	s.send(s.ingest(ctx, refs, s.clock()))

	ticker := time.NewTicker(s.cfg.Poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.send(s.pollOnce(ctx, s.clock()))
		}
	}
}

// pollOnce runs one full poll cycle and returns the signals to emit.
// Exported to the package's tests, which drive it with a fake clock
// and a fake client.
func (s *Source) pollOnce(ctx context.Context, now time.Time) []engine.Signal {
	refs, err := s.client.Sessions(ctx)
	if err != nil {
		// Transient (startup already proved the surface exists):
		// skip the cycle, log the edge only.
		s.mu.Lock()
		firstFailure := !s.listFailing
		s.listFailing = true
		s.mu.Unlock()
		if firstFailure {
			s.printf("token-burn: session list fetch failed (%v) — skipping poll cycles until the cost stack recovers", err)
		}
		return nil
	}
	s.mu.Lock()
	if s.listFailing {
		s.listFailing = false
		s.mu.Unlock()
		s.printf("token-burn: cost stack recovered")
	} else {
		s.mu.Unlock()
	}
	return s.ingest(ctx, refs, now)
}

// ingest samples every active session's usage, evaluates the
// triggers, and prunes stale series.
func (s *Source) ingest(ctx context.Context, refs []SessionRef, now time.Time) []engine.Signal {
	// Fetch usage OUTSIDE s.mu (network); collect the successes.
	type obs struct {
		ref SessionRef
		u   Usage
	}
	var observed []obs
	var anyUsageErr error
	for _, ref := range refs {
		if ref.Status != SessionActive {
			// Idle sessions are registry-evicted: their tracker is
			// not live (and /usage on them is not a poll target) —
			// the series ages out via StaleAfter below.
			continue
		}
		u, err := s.client.Usage(ctx, ref)
		if err != nil {
			anyUsageErr = err
			continue
		}
		observed = append(observed, obs{ref: ref, u: u})
	}
	s.mu.Lock()
	firstUsageFailure := anyUsageErr != nil && !s.usageFailing
	s.usageFailing = anyUsageErr != nil
	s.mu.Unlock()
	if firstUsageFailure {
		s.printf("token-burn: per-session usage fetch failed (%v) — affected sessions skipped this cycle; a persistent 404 here means the daemon predates the core-agent v2.7.0 cost stack", anyUsageErr)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.polled = true

	// Record samples + fit rates for the sessions observed this poll.
	sampled := make([]*series, 0, len(observed))
	for _, o := range observed {
		uid := UID(o.ref.App, o.ref.ID)
		ser, ok := s.series[uid]
		if !ok {
			ser = &series{app: o.ref.App, sid: o.ref.ID}
			s.series[uid] = ser
		}
		ser.lastActive = now
		ser.samples = append(ser.samples, sample{t: now, tokens: float64(o.u.TotalTokens), cost: o.u.CostUSD})
		cutoff := now.Add(-s.cfg.Window)
		kept := ser.samples[:0]
		for _, p := range ser.samples {
			if p.t.After(cutoff) {
				kept = append(kept, p)
			}
		}
		ser.samples = kept
		ser.rate = s.tokenRate(ser)
		sampled = append(sampled, ser)
	}

	// Baseline: trailing median rate across sessions (package doc).
	baseline := medianPositive(sampled)

	var out []engine.Signal
	for _, ser := range sampled {
		if sig := s.evaluate(ser, baseline, now); sig != nil {
			out = append(out, *sig)
		}
	}

	// Prune series the daemon stopped listing as active (session
	// ended / evicted) — clearance then reports object_deleted.
	for uid, ser := range s.series {
		if now.Sub(ser.lastActive) > s.cfg.StaleAfter {
			delete(s.series, uid)
		}
	}
	return out
}

// tokenRate fits the token series and returns the burn rate in
// tokens/sec (0 when unfit or non-positive — cumulative counters
// only ever read as idle, never as negative burn). Called under s.mu.
func (s *Source) tokenRate(ser *series) float64 {
	if len(ser.samples) < s.cfg.MinSamples {
		return 0
	}
	times := make([]time.Time, len(ser.samples))
	values := make([]float64, len(ser.samples))
	for i, p := range ser.samples {
		times[i], values[i] = p.t, p.tokens
	}
	slope := saturation.LeastSquaresSlope(times, values)
	if slope <= 0 {
		return 0
	}
	return slope
}

// costRate fits the cost series in USD/sec (0 when unfit or
// non-positive). The budget projection trusts 2 points where the
// rate fit wants MinSamples: the ETA math divides headroom by the
// slope, and even a 2-point slope of a cumulative counter bounds the
// projection usefully — but nothing fires off a single sample.
// Called under s.mu.
func (s *Source) costRate(ser *series) float64 {
	if len(ser.samples) < 2 {
		return 0
	}
	times := make([]time.Time, len(ser.samples))
	values := make([]float64, len(ser.samples))
	for i, p := range ser.samples {
		times[i], values[i] = p.t, p.cost
	}
	slope := saturation.LeastSquaresSlope(times, values)
	if slope <= 0 {
		return 0
	}
	return slope
}

// medianPositive returns the median of the positive fitted rates
// among the sessions sampled this poll (0 when none) — the trailing
// median baseline of the package doc. Even count → mean of the two
// middles.
func medianPositive(sampled []*series) float64 {
	var rates []float64
	for _, ser := range sampled {
		if ser.rate > 0 {
			rates = append(rates, ser.rate)
		}
	}
	if len(rates) == 0 {
		return 0
	}
	sort.Float64s(rates)
	n := len(rates)
	if n%2 == 1 {
		return rates[n/2]
	}
	return (rates[n/2-1] + rates[n/2]) / 2
}

// evaluate runs both triggers and the hysteresis latch for one
// sampled session. Called under s.mu; returns a signal to emit, if
// any.
func (s *Source) evaluate(ser *series, baseline float64, now time.Time) *engine.Signal {
	ser.baseline = baseline

	// Trigger (a): rate ≥ multiple × baseline, sustained.
	rateHot := baseline > 0 && ser.rate >= s.cfg.BurnMultiple*baseline
	if rateHot {
		ser.hot++
	} else {
		ser.hot = 0
	}

	// Trigger (b): budget exhaustion projected inside BurnETA (or
	// already exhausted). Needs a known budget and ≥ 2 cost samples.
	var (
		budgetHot bool
		exhausted bool
		fraction  = -1.0 // <0 = unknown
		eta       = time.Duration(-1)
		costRate  float64
	)
	n := len(ser.samples)
	if s.cfg.BudgetUSD > 0 && n >= 2 {
		spent := ser.samples[n-1].cost
		fraction = spent / s.cfg.BudgetUSD
		costRate = s.costRate(ser)
		if fraction >= 1 {
			budgetHot, exhausted = true, true
			eta = 0
		} else if costRate > 0 {
			// ok=false is the overflow clamp (issue #80): a projection
			// beyond the representable horizon is no projection, same
			// as costRate <= 0 — eta stays -1 (unknown) so the calm
			// path below is undisturbed.
			if d, ok := saturation.ETAFromSeconds((s.cfg.BudgetUSD - spent) / costRate); ok {
				eta = d
				if eta < s.cfg.BurnETA {
					budgetHot = true
				}
			}
		}
	}

	var sev engine.Severity
	switch {
	case budgetHot:
		sev = engine.SeverityCritical
	case rateHot && ser.hot >= sustainPolls:
		sev = engine.SeverityWarning
	}

	if sev == "" {
		// No trigger this poll. Calm only when the condition is
		// genuinely absent: not rate-hot, and any budget projection
		// receded beyond 2×BurnETA (between BurnETA and 2×BurnETA is
		// the hold band — the latch neither fires nor releases, so
		// an ETA oscillating around the line cannot flap).
		calmNow := !rateHot &&
			(fraction < 0 || fraction < 1) &&
			(eta < 0 || costRate <= 0 || eta >= s.cfg.clearETA())
		if calmNow {
			if ser.calm == 0 {
				ser.calmSince = now
			}
			ser.calm++
			if ser.firedSeverity != "" && ser.calm >= sustainPolls {
				ser.firedSeverity = "" // latch released; clearance agrees
			}
		} else {
			ser.calm = 0
			ser.calmSince = time.Time{}
		}
		return nil
	}
	ser.calm = 0
	ser.calmSince = time.Time{}

	if severityRank(sev) <= severityRank(ser.firedSeverity) {
		return nil // same severity never re-fires; only escalation does
	}
	ser.firedSeverity = sev
	sig := s.newBurn(ser, sev, fraction, eta, exhausted, budgetHot, now)
	return &sig
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

// newBurn composes the token.burn Signal with the §12 evidence
// (session id, rate, baseline, multiple, budget fraction) and — for
// budget-based fires only — the §8 Forecast attachment. Called under
// s.mu.
func (s *Source) newBurn(ser *series, sev engine.Severity, fraction float64, eta time.Duration, exhausted, budgetBased bool, now time.Time) engine.Signal {
	multiple := "n/a"
	if ser.baseline > 0 {
		multiple = fmt.Sprintf("%.1fx", ser.rate/ser.baseline)
	}
	budget := "budget=unknown"
	if fraction >= 0 {
		budget = fmt.Sprintf("budget_fraction=%.2f", fraction)
	}
	var verdict string
	switch {
	case exhausted:
		verdict = "session budget EXHAUSTED"
	case budgetBased:
		verdict = fmt.Sprintf("budget exhausted in ~%s at the current spend", eta.Truncate(time.Second))
	default:
		verdict = fmt.Sprintf("rate sustained >=%.0fx the cross-session baseline for %d polls", s.cfg.BurnMultiple, sustainPolls)
	}
	msg := fmt.Sprintf(
		"token burn on session %s/%s: rate=%.0f tok/min baseline=%.0f tok/min multiple=%s %s — %s (%d samples over %s)",
		ser.app, ser.sid, ser.rate*60, ser.baseline*60, multiple, budget, verdict,
		len(ser.samples), s.cfg.Window)
	sig := engine.Signal{
		Kind:     KindBurn,
		Source:   engine.SourceSentinel,
		Severity: sev,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: UID(ser.app, ser.sid), Reason: Reason},
			KindOfObject: "Session",
			Name:         ser.sid,
			Message:      msg,
			FirstSeen:    now,
			LastSeen:     now,
			Count:        1,
		},
	}
	if budgetBased {
		sig.Forecast = &engine.Forecast{
			ETA:             now.Add(eta),
			ConfidenceBasis: fmt.Sprintf("linear-%dm-window", int(s.cfg.Window.Minutes())),
		}
	}
	return sig
}

// Clearance implements engine.ClearanceObserver for token.burn
// incidents. It CLAIMS every token_burn incident (ok=true) even
// while symptomatic, so no generic observer judges them.
//
// Cleared when (package comment, hysteresis contract):
//   - the session's series is gone after a successful poll (the
//     daemon stopped listing it active for StaleAfter — the session
//     ended) → object_deleted;
//   - the burn condition stayed absent for sustainPolls consecutive
//     polls (latch released) → recovered, stable since the calm run
//     started.
func (s *Source) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	if engine.CanonicalReason(inc.Key.Reason) != Reason {
		return engine.Clearance{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.polled {
		return engine.Clearance{}, false // no poll yet — cannot judge
	}
	ser, exists := s.series[inc.Key.UID]
	if !exists {
		return engine.Clearance{Cleared: true, Resolution: engine.ResolutionObjectDeleted}, true
	}
	if ser.firedSeverity == "" && !ser.calmSince.IsZero() && ser.calm >= sustainPolls {
		return engine.Clearance{Cleared: true, StableSince: ser.calmSince, Resolution: engine.ResolutionRecovered}, true
	}
	return engine.Clearance{Cleared: false}, true
}
