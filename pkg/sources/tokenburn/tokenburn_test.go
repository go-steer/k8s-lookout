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

package tokenburn

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// fakeClient scripts the cost stack: per-poll cumulative totals are
// installed by the test before each pollOnce call (§13 trend
// testing: synthetic series with known slopes).
type fakeClient struct {
	refs        []SessionRef
	usage       map[string]Usage // keyed by UID(app, id)
	sessionsErr error
	usageErr    error
}

func (f *fakeClient) Sessions(_ context.Context) ([]SessionRef, error) {
	if f.sessionsErr != nil {
		return nil, f.sessionsErr
	}
	return f.refs, nil
}

func (f *fakeClient) Usage(_ context.Context, ref SessionRef) (Usage, error) {
	if f.usageErr != nil {
		return Usage{}, f.usageErr
	}
	return f.usage[UID(ref.App, ref.ID)], nil
}

func active(ids ...string) []SessionRef {
	refs := make([]SessionRef, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, SessionRef{App: "core-agent", ID: id, Status: SessionActive})
	}
	return refs
}

// harness drives pollOnce with a fake clock: one poll per Poll
// interval, cumulative totals advanced by the given per-poll deltas.
type harness struct {
	t   *testing.T
	src *Source
	fc  *fakeClient
	now time.Time
}

func newHarness(t *testing.T, cfg Config, ids ...string) *harness {
	t.Helper()
	fc := &fakeClient{refs: active(ids...), usage: map[string]Usage{}}
	src := New(fc, cfg)
	src.logf = t.Logf
	return &harness{t: t, src: src, fc: fc, now: time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)}
}

// poll advances the clock by one Poll interval (except the very
// first call), adds each session's per-poll spend delta, and runs
// one cycle. deltas is keyed by session id.
func (h *harness) poll(deltas map[string]Usage) []engine.Signal {
	h.t.Helper()
	for id, d := range deltas {
		uid := UID("core-agent", id)
		cur := h.fc.usage[uid]
		h.fc.usage[uid] = Usage{
			TotalTokens: cur.TotalTokens + d.TotalTokens,
			CostUSD:     cur.CostUSD + d.CostUSD,
			Turns:       cur.Turns + d.Turns,
		}
	}
	sigs := h.src.pollOnce(context.Background(), h.now)
	h.now = h.now.Add(h.src.cfg.Poll)
	return sigs
}

// TestRateTrigger_MathExactAndSustained: three sessions, one burning
// at 8× the other two. Rates and baseline are exact (constant
// increments → exact least-squares slope), the multiple crosses 4×,
// and the signal fires only once the condition sustained 2 polls —
// never on the first poll (cold start), never before MinSamples.
func TestRateTrigger_MathExactAndSustained(t *testing.T) {
	t.Parallel()
	h := newHarness(t, DefaultConfig(), "burner", "calm-a", "calm-b")
	// 6000 tok/poll at 60s polls = exactly 100 tok/s for the burner;
	// 750 tok/poll = 12.5 tok/s for the calm pair.
	deltas := map[string]Usage{
		"burner": {TotalTokens: 6000},
		"calm-a": {TotalTokens: 750},
		"calm-b": {TotalTokens: 750},
	}
	// Poll 1 (cold start): no rates (MinSamples=3), nothing fires.
	if sigs := h.poll(deltas); len(sigs) != 0 {
		t.Fatalf("poll 1 (cold start) emitted %d signal(s); must never fire on the first poll", len(sigs))
	}
	// Poll 2: still under MinSamples.
	if sigs := h.poll(deltas); len(sigs) != 0 {
		t.Fatalf("poll 2 emitted %d signal(s); rates need MinSamples=3", len(sigs))
	}
	// Poll 3: rates exist, burner is hot (multiple 8 >= 4) — first
	// hot poll, not yet sustained.
	if sigs := h.poll(deltas); len(sigs) != 0 {
		t.Fatalf("poll 3 emitted %d signal(s); rate trigger requires 2 sustained polls", len(sigs))
	}
	// Poll 4: sustained → exactly one warning for the burner.
	sigs := h.poll(deltas)
	if len(sigs) != 1 {
		t.Fatalf("poll 4 emitted %d signal(s), want exactly 1", len(sigs))
	}
	sig := sigs[0]
	if sig.Kind != KindBurn {
		t.Errorf("Kind = %q, want %q", sig.Kind, KindBurn)
	}
	if sig.Severity != engine.SeverityWarning {
		t.Errorf("Severity = %q, want warning (rate-based)", sig.Severity)
	}
	if sig.Key.UID != "session:core-agent/burner" {
		t.Errorf("UID = %q, want session:core-agent/burner", sig.Key.UID)
	}
	if sig.Key.Reason != Reason {
		t.Errorf("Reason = %q, want %q", sig.Key.Reason, Reason)
	}
	if sig.KindOfObject != "Session" || sig.Name != "burner" {
		t.Errorf("object = %s/%s, want Session/burner", sig.KindOfObject, sig.Name)
	}
	if sig.Forecast != nil {
		t.Error("rate-based warning must not carry a Forecast (budget-based only)")
	}
	// Evidence: exact rate, baseline, multiple; budget unknown.
	for _, want := range []string{
		"rate=6000 tok/min",    // 100 tok/s exactly
		"baseline=750 tok/min", // median(100, 12.5, 12.5) = 12.5 tok/s
		"multiple=8.0x",        // 100 / 12.5
		"budget=unknown",       // no --token-budget-usd
		"session core-agent/burner",
	} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("message missing %q:\n%s", want, sig.Message)
		}
	}
	// Poll 5: same severity never re-fires (hysteresis latch).
	if sigs := h.poll(deltas); len(sigs) != 0 {
		t.Fatalf("poll 5 re-fired %d signal(s) at the same severity", len(sigs))
	}
}

// TestRateTrigger_SingleSessionIsInert: a lone session IS the
// median, its multiple is exactly 1, and the rate trigger never
// fires — the documented degenerate case the budget trigger covers.
func TestRateTrigger_SingleSessionIsInert(t *testing.T) {
	t.Parallel()
	h := newHarness(t, DefaultConfig(), "solo")
	deltas := map[string]Usage{"solo": {TotalTokens: 60000}} // huge burn
	for i := 1; i <= 6; i++ {
		if sigs := h.poll(deltas); len(sigs) != 0 {
			t.Fatalf("poll %d fired for a single session (multiple must be exactly 1 against its own median)", i)
		}
	}
}

// TestBudgetTrigger_ETAExactAndCritical: budget known, spend slope
// projects exhaustion in 9m < 30m → critical with an exact linear
// Forecast. Also proves the budget path never fires on the first
// poll (one sample is not evidence of burn).
func TestBudgetTrigger_ETAExactAndCritical(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.BudgetUSD = 10
	h := newHarness(t, cfg, "solo")
	// Pre-existing spend: $8.00 at poll 1, then +$0.20/poll (60s)
	// = $0.20/min.
	h.fc.usage[UID("core-agent", "solo")] = Usage{TotalTokens: 100000, CostUSD: 7.8}
	first := map[string]Usage{"solo": {TotalTokens: 1000, CostUSD: 0.2}}
	if sigs := h.poll(first); len(sigs) != 0 {
		t.Fatalf("first poll fired %d signal(s) despite a single sample", len(sigs))
	}
	// Poll 2: spend $8.20, remaining $1.80 at $0.20/min → ETA 9m.
	fireAt := h.now
	sigs := h.poll(first)
	if len(sigs) != 1 {
		t.Fatalf("poll 2 emitted %d signal(s), want the budget-ETA critical", len(sigs))
	}
	sig := sigs[0]
	if sig.Severity != engine.SeverityCritical {
		t.Errorf("Severity = %q, want critical (budget ETA < 30m)", sig.Severity)
	}
	if sig.Forecast == nil {
		t.Fatal("budget-based fire must carry a Forecast")
	}
	wantETA := fireAt.Add(9 * time.Minute)
	if d := sig.Forecast.ETA.Sub(wantETA); d < -time.Second || d > time.Second {
		t.Errorf("Forecast.ETA = %v, want %v (±1s)", sig.Forecast.ETA, wantETA)
	}
	if sig.Forecast.ConfidenceBasis != "linear-15m-window" {
		t.Errorf("ConfidenceBasis = %q, want linear-15m-window", sig.Forecast.ConfidenceBasis)
	}
	if !strings.Contains(sig.Message, "budget_fraction=0.82") {
		t.Errorf("message missing budget_fraction=0.82:\n%s", sig.Message)
	}
	if !strings.Contains(sig.Message, "budget exhausted in ~") {
		t.Errorf("message missing the ETA verdict:\n%s", sig.Message)
	}
	// Critical is the top of the latch: nothing further fires.
	if sigs := h.poll(first); len(sigs) != 0 {
		t.Fatalf("critical re-fired %d signal(s)", len(sigs))
	}
}

// TestBudgetTrigger_Exhausted: spend at/over the budget → critical
// with the EXHAUSTED verdict and ETA=now, regardless of slope
// urgency — but still never on the first poll.
func TestBudgetTrigger_Exhausted(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.BudgetUSD = 5
	h := newHarness(t, cfg, "solo")
	h.fc.usage[UID("core-agent", "solo")] = Usage{TotalTokens: 100000, CostUSD: 6}
	still := map[string]Usage{"solo": {TotalTokens: 10, CostUSD: 0.001}}
	if sigs := h.poll(still); len(sigs) != 0 {
		t.Fatal("exhausted budget must not fire off a single sample (cold start)")
	}
	fireAt := h.now
	sigs := h.poll(still)
	if len(sigs) != 1 {
		t.Fatalf("poll 2 emitted %d signal(s), want the exhausted critical", len(sigs))
	}
	sig := sigs[0]
	if sig.Severity != engine.SeverityCritical {
		t.Errorf("Severity = %q, want critical", sig.Severity)
	}
	if !strings.Contains(sig.Message, "EXHAUSTED") {
		t.Errorf("message missing the EXHAUSTED verdict:\n%s", sig.Message)
	}
	if sig.Forecast == nil || !sig.Forecast.ETA.Equal(fireAt) {
		t.Errorf("Forecast = %+v, want ETA pinned at the fire time (already exhausted)", sig.Forecast)
	}
}

// TestEscalation_WarningToCriticalOnce: a rate-based warning
// escalates to critical exactly once when the budget projection
// turns urgent; neither severity re-fires afterwards.
func TestEscalation_WarningToCriticalOnce(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.BudgetUSD = 1000 // huge: budget path silent at first
	h := newHarness(t, cfg, "burner", "calm-a", "calm-b")
	deltas := map[string]Usage{
		"burner": {TotalTokens: 6000, CostUSD: 0.01},
		"calm-a": {TotalTokens: 750, CostUSD: 0.001},
		"calm-b": {TotalTokens: 750, CostUSD: 0.001},
	}
	var fired []engine.Signal
	for i := 0; i < 4; i++ {
		fired = append(fired, h.poll(deltas)...)
	}
	if len(fired) != 1 || fired[0].Severity != engine.SeverityWarning {
		t.Fatalf("setup: want exactly 1 warning after 4 polls, got %d signal(s)", len(fired))
	}
	// The burner's spend jumps: $200/poll against a $1000 budget →
	// ETA ~4m < 30m → escalation to critical, once.
	hot := map[string]Usage{
		"burner": {TotalTokens: 6000, CostUSD: 200},
		"calm-a": {TotalTokens: 750, CostUSD: 0.001},
		"calm-b": {TotalTokens: 750, CostUSD: 0.001},
	}
	// The cost slope is fitted over the whole window, so the first
	// hot poll may not yet project inside 30m; collect until the
	// escalation lands, then require silence.
	var crit []engine.Signal
	for i := 0; i < 6; i++ {
		crit = append(crit, h.poll(hot)...)
	}
	if len(crit) != 1 || crit[0].Severity != engine.SeverityCritical {
		t.Fatalf("escalation: want exactly 1 critical, got %d signal(s): %+v", len(crit), crit)
	}
	if crit[0].Forecast == nil {
		t.Error("escalated budget-based critical must carry a Forecast")
	}
	if sigs := h.poll(hot); len(sigs) != 0 {
		t.Fatalf("critical re-fired %d signal(s)", len(sigs))
	}
}

// TestClearance_RecoveredAfterCalm: after a fire, the burn stopping
// releases the latch (2 calm polls once the hot samples age out of
// the window) and Clearance reports recovered with a StableSince.
func TestClearance_RecoveredAfterCalm(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.Window = 4 * time.Minute // small window so the hot slope ages out fast
	h := newHarness(t, cfg, "burner", "calm-a", "calm-b")
	deltas := map[string]Usage{
		"burner": {TotalTokens: 6000},
		"calm-a": {TotalTokens: 750},
		"calm-b": {TotalTokens: 750},
	}
	var fired []engine.Signal
	for i := 0; i < 4; i++ {
		fired = append(fired, h.poll(deltas)...)
	}
	if len(fired) != 1 {
		t.Fatalf("setup: want 1 warning, got %d", len(fired))
	}
	inc := engine.Incident{Key: engine.EventKey{UID: "session:core-agent/burner", Reason: Reason}}
	if c, ok := h.src.Clearance(inc); !ok || c.Cleared {
		t.Fatalf("Clearance while symptomatic = (%+v, %v), want claimed and not cleared", c, ok)
	}
	// The burner goes fully idle; the calm pair keeps its pace so a
	// baseline still exists. Within the small window the hot samples
	// age out, the rate drops below 4×, and after 2 calm polls the
	// latch releases.
	idle := map[string]Usage{
		"burner": {},
		"calm-a": {TotalTokens: 750},
		"calm-b": {TotalTokens: 750},
	}
	deadline := 12
	cleared := false
	for i := 0; i < deadline; i++ {
		if sigs := h.poll(idle); len(sigs) != 0 {
			t.Fatalf("calm-down poll %d emitted %d signal(s)", i+1, len(sigs))
		}
		if c, ok := h.src.Clearance(inc); ok && c.Cleared {
			if c.Resolution != engine.ResolutionRecovered {
				t.Fatalf("Resolution = %q, want recovered", c.Resolution)
			}
			if c.StableSince.IsZero() {
				t.Fatal("recovered clearance must carry StableSince (start of the calm run)")
			}
			cleared = true
			break
		}
	}
	if !cleared {
		t.Fatalf("incident never cleared within %d idle polls", deadline)
	}
	// A fresh burn after release fires again (latch fully reset).
	var again []engine.Signal
	for i := 0; i < 6; i++ {
		again = append(again, h.poll(deltas)...)
	}
	if len(again) != 1 || again[0].Severity != engine.SeverityWarning {
		t.Fatalf("post-release re-approach: want exactly 1 fresh warning, got %d signal(s)", len(again))
	}
}

// TestClearance_SessionGoneIsObjectDeleted: a session the daemon
// stops listing ages out after StaleAfter and clears as
// object_deleted — the session ended, nothing was "fixed".
func TestClearance_SessionGoneIsObjectDeleted(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.StaleAfter = 2 * time.Minute
	h := newHarness(t, cfg, "burner", "calm-a", "calm-b")
	deltas := map[string]Usage{
		"burner": {TotalTokens: 6000},
		"calm-a": {TotalTokens: 750},
		"calm-b": {TotalTokens: 750},
	}
	for i := 0; i < 4; i++ {
		h.poll(deltas)
	}
	inc := engine.Incident{Key: engine.EventKey{UID: "session:core-agent/burner", Reason: Reason}}
	if c, ok := h.src.Clearance(inc); !ok || c.Cleared {
		t.Fatalf("pre-delete Clearance = (%+v, %v)", c, ok)
	}
	// The daemon stops listing the burner; after StaleAfter its
	// series is pruned.
	h.fc.refs = active("calm-a", "calm-b")
	calm := map[string]Usage{"calm-a": {TotalTokens: 750}, "calm-b": {TotalTokens: 750}}
	for i := 0; i < 4; i++ {
		h.poll(calm)
	}
	c, ok := h.src.Clearance(inc)
	if !ok || !c.Cleared || c.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("Clearance after session gone = (%+v, %v), want cleared/object_deleted", c, ok)
	}
}

// TestClearance_Boundaries: no judging before the first poll, no
// claiming foreign reasons, and idle sessions in the daemon list are
// not polled.
func TestClearance_Boundaries(t *testing.T) {
	t.Parallel()
	h := newHarness(t, DefaultConfig(), "solo")
	inc := engine.Incident{Key: engine.EventKey{UID: "session:core-agent/solo", Reason: Reason}}
	if _, ok := h.src.Clearance(inc); ok {
		t.Fatal("Clearance before the first poll must decline to judge")
	}
	if _, ok := h.src.Clearance(engine.Incident{Key: engine.EventKey{UID: "x", Reason: "CrashLoopBackOff"}}); ok {
		t.Fatal("Clearance must not claim foreign reasons")
	}
	// An idle session is never polled for usage.
	h.fc.refs = []SessionRef{
		{App: "core-agent", ID: "solo", Status: SessionActive},
		{App: "core-agent", ID: "old", Status: "idle"},
	}
	h.poll(map[string]Usage{"solo": {TotalTokens: 100}})
	h.src.mu.Lock()
	_, idleTracked := h.src.series[UID("core-agent", "old")]
	h.src.mu.Unlock()
	if idleTracked {
		t.Error("idle sessions must not be polled or tracked")
	}
}

// TestPoll_ListFailureSkipsCycleLoudlyOnce: transient session-list
// failures skip the cycle with one log edge, and the source recovers
// silently on the next success.
func TestPoll_ListFailureSkipsCycleLoudlyOnce(t *testing.T) {
	t.Parallel()
	h := newHarness(t, DefaultConfig(), "solo")
	var logged []string
	h.src.logf = func(format string, args ...any) { logged = append(logged, format) }
	h.poll(map[string]Usage{"solo": {TotalTokens: 100}})
	h.fc.sessionsErr = context.DeadlineExceeded
	for i := 0; i < 3; i++ {
		if sigs := h.poll(nil); len(sigs) != 0 {
			t.Fatal("failed cycle must emit nothing")
		}
	}
	if len(logged) != 1 {
		t.Fatalf("list failure logged %d time(s), want exactly 1 (throttled edge)", len(logged))
	}
	h.fc.sessionsErr = nil
	h.poll(map[string]Usage{"solo": {TotalTokens: 100}})
	if len(logged) != 2 { // the recovery line
		t.Fatalf("recovery logged %d extra line(s), want exactly 1", len(logged)-1)
	}
}
