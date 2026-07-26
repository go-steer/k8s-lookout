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

package quota

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// quotaProvider is a test cloud.Provider whose only capability is a
// scripted QuotaAPI.
type quotaProvider struct {
	cloud.Provider // embed NoProvider for the other capabilities
	api            cloud.QuotaAPI
}

func newQuotaProvider(api cloud.QuotaAPI) quotaProvider {
	return quotaProvider{Provider: cloud.NoProvider, api: api}
}

func (p quotaProvider) Name() string                  { return "test" }
func (p quotaProvider) Quota() (cloud.QuotaAPI, bool) { return p.api, p.api != nil }

// scriptedQuotaAPI is the §13 scripted backend: a fixed inventory
// plus per-quota histories, recording the history queries it serves.
type scriptedQuotaAPI struct {
	mu        sync.Mutex
	inventory []cloud.QuotaUsage
	invErr    error
	histories map[string]cloud.QuotaHistory
	histErr   error
	// asked records History calls as "name/scope"; windows the query
	// windows, in call order.
	asked   []string
	windows []cloud.TimeWindow
}

func (s *scriptedQuotaAPI) Quotas(context.Context) ([]cloud.QuotaUsage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.invErr != nil {
		return nil, s.invErr
	}
	out := make([]cloud.QuotaUsage, len(s.inventory))
	copy(out, s.inventory)
	return out, nil
}

func (s *scriptedQuotaAPI) History(_ context.Context, name, scope string, w cloud.TimeWindow) (cloud.QuotaHistory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked = append(s.asked, name+"/"+scope)
	s.windows = append(s.windows, w)
	if s.histErr != nil {
		return cloud.QuotaHistory{}, s.histErr
	}
	return s.histories[name+"/"+scope], nil
}

// dailySeries builds n daily usage points ending at end, growing
// perDay per point, with the last value last — the §13 synthetic
// series with a known slope.
func dailySeries(end time.Time, n int, last, perDay float64) []cloud.Point {
	pts := make([]cloud.Point, n)
	for i := range pts {
		back := time.Duration(n-1-i) * 24 * time.Hour
		pts[i] = cloud.Point{Time: end.Add(-back), Value: last - float64(n-1-i)*perDay}
	}
	return pts
}

func newTestSource(t *testing.T, api cloud.QuotaAPI, cfg Config) *Source {
	t.Helper()
	s, err := New(newQuotaProvider(api), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.logf = t.Logf
	return s
}

var testNow = time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)

func TestSourceContract(t *testing.T) {
	t.Parallel()
	s := newTestSource(t, &scriptedQuotaAPI{}, Config{})
	if s.Name() != "quota" {
		t.Errorf("Name() = %q, want quota (frozen; feeds --sources and the signal schema)", s.Name())
	}
	if s.Scope() != sources.ScopeProject {
		t.Errorf("Scope() = %v, want Project (§10.2: one instance per GCP project)", s.Scope())
	}
	if KindForecast != "quota.forecast" || Reason != "quota_forecast" {
		t.Error("kind/reason drifted from the frozen §7.3 strings")
	}
}

// TestNew_ProviderWithoutQuotaFailsLoudly is the §11 posture: a
// project-tier deployment without a quota-capable cloud provider is
// a STARTUP ERROR naming the source — never a silent empty source.
func TestNew_ProviderWithoutQuotaFailsLoudly(t *testing.T) {
	t.Parallel()
	for name, provider := range map[string]cloud.Provider{
		"NoProvider":      cloud.NoProvider,
		"nil":             nil,
		"capability-less": newQuotaProvider(nil),
	} {
		_, err := New(provider, Config{})
		if err == nil {
			t.Fatalf("%s: New must fail without the quota capability", name)
		}
		for _, want := range []string{`source "quota"`, "project", "unavailable reason="} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: error %q must contain %q (name the source, the tier, and the reason)", name, err, want)
			}
		}
	}
}

// TestPoll_KnownSlopeExactForecast pins the §13 trend contract: a
// synthetic series with slope 50/day and 300 headroom projects ETA =
// exactly 6 days, fires at warning (ETA < 7d), and carries the
// linear-7d-window basis plus the drafted increase request.
func TestPoll_KnownSlopeExactForecast(t *testing.T) {
	t.Parallel()
	api := &scriptedQuotaAPI{
		inventory: []cloud.QuotaUsage{{
			Name: "CPUS", Scope: "us-east1", Usage: 1700, Limit: 2000,
			ID: "compute.googleapis.com/CpusPerProjectPerRegion",
		}},
		histories: map[string]cloud.QuotaHistory{
			"CPUS/us-east1": {Name: "CPUS", Scope: "us-east1", Usage: dailySeries(testNow, 8, 1700, 50)},
		},
	}
	s := newTestSource(t, api, Config{})
	sigs, err := s.poll(context.Background(), testNow)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(sigs) != 1 {
		t.Fatalf("poll emitted %d signals, want 1", len(sigs))
	}
	sig := sigs[0]
	if sig.Kind != KindForecast || sig.Severity != engine.SeverityWarning {
		t.Errorf("kind/severity = %s/%s, want quota.forecast/warning (ETA 6d < 7d)", sig.Kind, sig.Severity)
	}
	if sig.Key.UID != "quota:CPUS/us-east1" || sig.Key.Reason != Reason {
		t.Errorf("key = %+v, want the canonical quota UID + quota_forecast", sig.Key)
	}
	if sig.KindOfObject != "Quota" || sig.Name != "CPUS" {
		t.Errorf("object = %s/%s, want Quota/CPUS", sig.KindOfObject, sig.Name)
	}
	if sig.Forecast == nil {
		t.Fatal("Forecast missing on a positive-slope projection")
	}
	wantETA := testNow.Add(6 * 24 * time.Hour) // 300 headroom / 50 per day
	if d := sig.Forecast.ETA.Sub(wantETA); d < -time.Second || d > time.Second {
		t.Errorf("ETA = %v, want %v (300/50 = exactly 6d)", sig.Forecast.ETA, wantETA)
	}
	if sig.Forecast.ConfidenceBasis != "linear-7d-window" {
		t.Errorf("basis = %q, want linear-7d-window", sig.Forecast.ConfidenceBasis)
	}
	if !strings.Contains(sig.Message, "exhausted in ~6d at current slope") {
		t.Errorf("message %q must state the projection, not just the percentage", sig.Message)
	}
	d := sig.QuotaDraft
	if d == nil {
		t.Fatal("QuotaDraft missing — every quota.forecast carries the §10.3 draft")
	}
	if d.QuotaID != "compute.googleapis.com/CpusPerProjectPerRegion" {
		t.Errorf("draft QuotaID = %q, want the provider's canonical id passed through", d.QuotaID)
	}
	// Formula: max(2000×1.5, 1700 + 2×50×7) = max(3000, 2400) = 3000.
	if d.SuggestedLimit != 3000 {
		t.Errorf("SuggestedLimit = %v, want 3000 (the 1.5x term dominates)", d.SuggestedLimit)
	}
	if d.SlopePerDay < 49.999 || d.SlopePerDay > 50.001 {
		t.Errorf("SlopePerDay = %v, want 50 (exact synthetic slope)", d.SlopePerDay)
	}
	if !strings.Contains(d.Justification, "50/day") || !strings.Contains(d.Justification, "3000") {
		t.Errorf("justification %q must carry the slope math and the ask", d.Justification)
	}
	// The history query used the configured window ending at the poll.
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.windows) != 1 || !api.windows[0].End.Equal(testNow) || !api.windows[0].Start.Equal(testNow.Add(-7*24*time.Hour)) {
		t.Errorf("history window = %+v, want [now-7d, now)", api.windows)
	}
}

// TestPoll_Thresholds walks the design-fixed severity table.
func TestPoll_Thresholds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		usage, limit float64
		perDay       float64 // 0 = no history
		wantSev      engine.Severity
		wantForecast bool
	}{
		{"below everything", 1700, 2000, 5, "", false},                                  // 85%, ETA 60d
		{"usage warning without trend", 1830, 2000, 0, engine.SeverityWarning, false},   // 91.5% flat
		{"usage critical without trend", 1970, 2000, 0, engine.SeverityCritical, false}, // 98.5% flat
		{"eta critical", 1950, 2000, 50, engine.SeverityCritical, true},                 // 50/50 = 1d
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := &scriptedQuotaAPI{
				inventory: []cloud.QuotaUsage{{Name: "CPUS", Scope: "us-east1", Usage: tc.usage, Limit: tc.limit}},
				histories: map[string]cloud.QuotaHistory{},
			}
			if tc.perDay > 0 {
				api.histories["CPUS/us-east1"] = cloud.QuotaHistory{Usage: dailySeries(testNow, 8, tc.usage, tc.perDay)}
			}
			s := newTestSource(t, api, Config{})
			sigs, err := s.poll(context.Background(), testNow)
			if err != nil {
				t.Fatalf("poll: %v", err)
			}
			if tc.wantSev == "" {
				if len(sigs) != 0 {
					t.Fatalf("emitted %d signals, want none", len(sigs))
				}
				return
			}
			if len(sigs) != 1 || sigs[0].Severity != tc.wantSev {
				t.Fatalf("got %d signals (sev %v), want 1 at %s", len(sigs), sigs, tc.wantSev)
			}
			if (sigs[0].Forecast != nil) != tc.wantForecast {
				t.Errorf("Forecast presence = %v, want %v (attached only with a positive-slope projection)", sigs[0].Forecast != nil, tc.wantForecast)
			}
			if sigs[0].QuotaDraft == nil {
				t.Error("QuotaDraft must ride every quota.forecast")
			}
		})
	}
}

// TestPoll_InsufficientWindowNoForecast: too few points (or too
// short a span) means no ETA — a usage threshold still fires, but
// with no Forecast attachment (§13 no-forecast case).
func TestPoll_InsufficientWindowNoForecast(t *testing.T) {
	t.Parallel()
	api := &scriptedQuotaAPI{
		inventory: []cloud.QuotaUsage{{Name: "CPUS", Scope: "us-east1", Usage: 1830, Limit: 2000}},
		histories: map[string]cloud.QuotaHistory{
			// 3 points < minPoints — steep but untrustworthy.
			"CPUS/us-east1": {Usage: dailySeries(testNow, 3, 1830, 100)},
		},
	}
	s := newTestSource(t, api, Config{})
	sigs, err := s.poll(context.Background(), testNow)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(sigs) != 1 || sigs[0].Severity != engine.SeverityWarning || sigs[0].Forecast != nil {
		t.Fatalf("got %+v, want one warning with NO forecast (insufficient window)", sigs)
	}
	// Span gate: plenty of points but under Window/2.
	pts := make([]cloud.Point, 6)
	for i := range pts {
		pts[i] = cloud.Point{Time: testNow.Add(time.Duration(i-5) * 12 * time.Hour), Value: 1000 + float64(i)*100}
	}
	api2 := &scriptedQuotaAPI{
		inventory: []cloud.QuotaUsage{{Name: "CPUS", Scope: "us-east1", Usage: 1830, Limit: 2000}},
		histories: map[string]cloud.QuotaHistory{"CPUS/us-east1": {Usage: pts}},
	}
	s2 := newTestSource(t, api2, Config{})
	sigs2, err := s2.poll(context.Background(), testNow)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(sigs2) != 1 || sigs2[0].Forecast != nil {
		t.Fatalf("got %+v, want one threshold-only warning (span 2.5d < window/2)", sigs2)
	}
}

// TestPoll_HistoryErrorDegradesToThresholds: a failing history query
// must not blind the source — the usage ratio still fires.
func TestPoll_HistoryErrorDegradesToThresholds(t *testing.T) {
	t.Parallel()
	api := &scriptedQuotaAPI{
		inventory: []cloud.QuotaUsage{{Name: "CPUS", Scope: "us-east1", Usage: 1990, Limit: 2000}},
		histErr:   errors.New("monitoring 403"),
	}
	s := newTestSource(t, api, Config{})
	sigs, err := s.poll(context.Background(), testNow)
	if err != nil {
		t.Fatalf("poll must not fail on a history error: %v", err)
	}
	if len(sigs) != 1 || sigs[0].Severity != engine.SeverityCritical {
		t.Fatalf("got %+v, want one critical (99.5%% on thresholds alone)", sigs)
	}
}

// TestLatch pins the hysteresis contract from the package comment:
// same severity never re-fires, escalation fires once more, recede
// with margin releases, and a fresh approach fires again.
func TestLatch(t *testing.T) {
	t.Parallel()
	api := &scriptedQuotaAPI{
		inventory: []cloud.QuotaUsage{{Name: "CPUS", Scope: "us-east1", Usage: 1830, Limit: 2000}},
		histories: map[string]cloud.QuotaHistory{},
	}
	s := newTestSource(t, api, Config{})
	poll := func(usage float64) []engine.Signal {
		api.mu.Lock()
		api.inventory[0].Usage = usage
		api.mu.Unlock()
		sigs, err := s.poll(context.Background(), testNow)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		return sigs
	}
	if got := poll(1830); len(got) != 1 || got[0].Severity != engine.SeverityWarning {
		t.Fatalf("first crossing: %+v, want one warning", got)
	}
	if got := poll(1835); len(got) != 0 {
		t.Fatalf("same severity re-fired: %+v (poll 15m > dedup window — the latch must hold)", got)
	}
	if got := poll(1980); len(got) != 1 || got[0].Severity != engine.SeverityCritical {
		t.Fatalf("escalation: %+v, want one critical", got)
	}
	if got := poll(1985); len(got) != 0 {
		t.Fatalf("critical re-fired: %+v", got)
	}
	// Recede WITHOUT margin (87% > releaseUsageFrac): latch holds,
	// nothing fires, nothing releases.
	if got := poll(1740); len(got) != 0 {
		t.Fatalf("recede inside the hysteresis band fired: %+v", got)
	}
	if got := poll(1980); len(got) != 0 {
		t.Fatalf("re-crossing while latched fired: %+v (latch must still be critical)", got)
	}
	// Recede WITH margin (50% < 85%): released; a fresh approach fires.
	if got := poll(1000); len(got) != 0 {
		t.Fatalf("release poll fired: %+v", got)
	}
	if got := poll(1830); len(got) != 1 || got[0].Severity != engine.SeverityWarning {
		t.Fatalf("fresh approach after release: %+v, want one warning", got)
	}
}

// TestWatchlist pins the §10.2 selection: top-N nearest exhaustion
// by usage/limit ratio, plus everything at/above the warn ratio;
// unlimited quotas are never watched.
func TestWatchlist(t *testing.T) {
	t.Parallel()
	inv := []cloud.QuotaUsage{
		{Name: "A", Scope: "r", Usage: 90, Limit: 100}, // 0.90 — top
		{Name: "B", Scope: "r", Usage: 85, Limit: 100}, // 0.85 — top
		{Name: "C", Scope: "r", Usage: 82, Limit: 100}, // 0.82 — over warn, beyond top
		{Name: "D", Scope: "r", Usage: 70, Limit: 100}, // 0.70 — dropped
		{Name: "E", Scope: "r", Usage: 999, Limit: 0},  // unlimited — never watched
	}
	got := watchlist(inv, 0.80, 2)
	names := make([]string, len(got))
	for i, q := range got {
		names[i] = q.Name
	}
	if len(got) != 3 || names[0] != "A" || names[1] != "B" || names[2] != "C" {
		t.Errorf("watchlist = %v, want [A B C] (top-2 by ratio + the over-warn stragglers)", names)
	}
}

// TestRun_StartupInventoryFailureIsLoud: Run's first poll is
// synchronous and fatal (§11) — a dead quota API at startup is a
// credentials/config error, not a silent gap.
func TestRun_StartupInventoryFailureIsLoud(t *testing.T) {
	t.Parallel()
	s := newTestSource(t, &scriptedQuotaAPI{invErr: errors.New("compute 403")}, Config{})
	err := s.Run(context.Background(), func(engine.Signal) {})
	if err == nil || !strings.Contains(err.Error(), "startup") || !strings.Contains(err.Error(), "compute 403") {
		t.Fatalf("Run = %v, want a loud startup error wrapping the cause", err)
	}
}

// TestRun_PollLoopEmits drives the resident loop end to end: the
// synchronous first poll emits, the ticker keeps polling, ctx
// cancellation is a clean shutdown.
func TestRun_PollLoopEmits(t *testing.T) {
	t.Parallel()
	api := &scriptedQuotaAPI{
		inventory: []cloud.QuotaUsage{{Name: "CPUS", Scope: "us-east1", Usage: 1990, Limit: 2000}},
		histories: map[string]cloud.QuotaHistory{},
	}
	s := newTestSource(t, api, Config{Poll: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var got []engine.Signal
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx, func(sig engine.Signal) {
			mu.Lock()
			got = append(got, sig)
			mu.Unlock()
		})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no signal emitted within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Give the ticker a few cycles: the latch must keep the repeat
	// count at exactly one signal.
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run after cancel = %v, want nil (clean shutdown)", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("emitted %d signals across polls, want exactly 1 (hysteresis latch)", len(got))
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.asked) < 2 {
		t.Errorf("history asked %d times, want the ticker to keep polling", len(api.asked))
	}
}

// TestDraft_SlopeTermDominates: the second formula term wins when
// drift outruns the 1.5x step: max(100×1.5, 90 + 2×10×7) = 230.
func TestDraft_SlopeTermDominates(t *testing.T) {
	t.Parallel()
	d := Draft(cloud.QuotaUsage{Name: "N2_CPUS", Scope: "us-east1", Usage: 90, Limit: 100}, 10, 24*time.Hour, true, testNow)
	if d.SuggestedLimit != 230 {
		t.Errorf("SuggestedLimit = %v, want 230 (usage + 2×slope×7d leadtime)", d.SuggestedLimit)
	}
	if d.QuotaID != "N2_CPUS" {
		t.Errorf("QuotaID = %q, want the name fallback when the provider maps no canonical id", d.QuotaID)
	}
	if !strings.Contains(d.Justification, "2026-07-27") {
		t.Errorf("justification %q must date the projected exhaustion", d.Justification)
	}
	// Flat variant: the 1.5x term is the whole answer.
	flat := Draft(cloud.QuotaUsage{Name: "CPUS", Scope: "global", Usage: 1970, Limit: 2000}, 0, 0, false, testNow)
	if flat.SuggestedLimit != 3000 || !strings.Contains(flat.Justification, "1.5x") {
		t.Errorf("flat draft = %+v, want 3000 with the 1.5x rationale", flat)
	}
}
