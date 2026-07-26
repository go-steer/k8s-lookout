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

package watch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/quota"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// The M4 exit-criterion drill (DESIGN.md §14: "Staged quota
// exhaustion yields correlated incident + drafted increase
// request"), run end-to-end at the dispatcher level: the REAL quota
// and capacity sources (scripted cloud APIs per §13 — recorded/fake
// fixtures stand in for GCP, no live project) drive the REAL
// pipeline (filter → routing → dedup → watchboard/session → inject)
// against the fake daemon, wall-clock time compressed via the
// sources' poll knobs and a short dedup window.
//
// The scripted sequence and what it must produce:
//
//  1. Quota poll 1: CPUS/us-east1 at 85% with a fitted 50/day slope
//     → ETA ~6d < WarnETA → quota.forecast WARNING → watchboard
//     digest (leading indicators don't page, §7.7).
//  2. Quota poll 2: usage jumps to 98% (CritUsageFrac) with a 60/day
//     slope → the hysteresis latch escalates once → quota.forecast
//     CRITICAL → its own session, with forecast{eta,basis} AND the
//     §10.3 quota_increase_draft attached (formula pinned below).
//  3. Capacity decision poll: a GCE_QUOTA_EXCEEDED noScaleUp whose
//     message names the same quota+region → decisionSignal re-keys
//     it to quota:CPUS/us-east1 → the QuotaExhausted dedup family
//     collapses it INTO the open critical session (§10.3: one
//     diagnosed incident, not two) — no new session, the store
//     records the join (route=suppressed, the critical session's
//     sid).
//  4. Quota poll 3 (same critical state): the latch holds — nothing
//     re-fires.
type quotaDrillAPI struct {
	mu    sync.Mutex
	polls int
	now   func() time.Time
}

// series builds a linear ~daily usage history spanning the full 7d
// window (8 points ≥ minPoints, span ≥ Window/2) ending at `last`,
// growing perDay — the recorded-fixture stand-in for the Cloud
// Monitoring serviceruntime allocation series (§13: recorded API
// fixtures behind small client interfaces).
func (a *quotaDrillAPI) series(last, perDay float64) []cloud.Point {
	end := a.now()
	pts := make([]cloud.Point, 8)
	for i := range pts {
		back := time.Duration(7-i) * 24 * time.Hour
		pts[i] = cloud.Point{Time: end.Add(-back), Value: last - perDay*float64(7-i)}
	}
	return pts
}

func (a *quotaDrillAPI) phase() (usage, perDay float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.polls <= 1 {
		return 1700, 50 // 85%: ETA (2000-1700)/50 = 6d → warning
	}
	return 1960, 60 // 98%: ratio ≥ CritUsageFrac, ETA 16h → critical
}

func (a *quotaDrillAPI) Quotas(context.Context) ([]cloud.QuotaUsage, error) {
	a.mu.Lock()
	a.polls++
	a.mu.Unlock()
	usage, _ := a.phase()
	return []cloud.QuotaUsage{{
		Name:  "CPUS",
		Scope: "us-east1",
		Usage: usage,
		Limit: 2000,
		ID:    "compute.googleapis.com/cpus",
	}}, nil
}

func (a *quotaDrillAPI) History(context.Context, string, string, cloud.TimeWindow) (cloud.QuotaHistory, error) {
	usage, perDay := a.phase()
	return cloud.QuotaHistory{Name: "CPUS", Scope: "us-east1", Usage: a.series(usage, perDay)}, nil
}

// drillQuotaProvider grants exactly the quota capability.
type drillQuotaProvider struct {
	cloud.Provider
	api cloud.QuotaAPI
}

func (drillQuotaProvider) Name() string                    { return "drill" }
func (p drillQuotaProvider) Quota() (cloud.QuotaAPI, bool) { return p.api, true }

// gatedDecisions is a cloud.CapacityAPI that returns no scale
// decisions until armed — the drill controls WHEN the reactive
// GCE_QUOTA_EXCEEDED failure lands.
type gatedDecisions struct {
	armed atomic.Bool
	now   func() time.Time
}

func (g *gatedDecisions) ScaleDecisions(context.Context, cloud.TimeWindow) ([]cloud.ScaleDecision, error) {
	if !g.armed.Load() {
		return nil, nil
	}
	return []cloud.ScaleDecision{{
		Time:      g.now(),
		Decision:  "noScaleUp",
		NodeGroup: "mig-prod-a",
		Reason:    "GCE_QUOTA_EXCEEDED",
		Message:   "Instance creation failed: Quota 'CPUS' exceeded. Limit: 2000.0 in region us-east1.",
	}}, nil
}

// drillCapacityProvider grants exactly the capacity capability.
type drillCapacityProvider struct {
	cloud.Provider
	api cloud.CapacityAPI
}

func (drillCapacityProvider) Name() string                          { return "drill" }
func (p drillCapacityProvider) Capacity() (cloud.CapacityAPI, bool) { return p.api, true }

// drillDaemon is newFakeDaemon's shape made safe for CONCURRENT
// observation: the drill's sources inject from their own goroutines
// while the test polls the capture, so the capture is mutex-guarded
// (the shared newFakeDaemon helper is only read after synchronous
// dispatch and needs none).
type drillDaemon struct {
	mu       sync.Mutex
	injects  []string
	sessions int32
}

func newDrillDaemon(t *testing.T) (baseURL string, dd *drillDaemon) {
	t.Helper()
	dd = &drillDaemon{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sessions":
			id := atomic.AddInt32(&dd.sessions, 1)
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"app":"core-agent","user":"alice","sessionID":"sess-%s","url":"http://x"}`, strings.Repeat("x", int(id)))
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sessions/") && strings.HasSuffix(r.URL.Path, "/inject"):
			body, _ := io.ReadAll(r.Body)
			dd.mu.Lock()
			dd.injects = append(dd.injects, string(body))
			dd.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, dd
}

func (dd *drillDaemon) snapshot() []string {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	return append([]string(nil), dd.injects...)
}

// waitInjects polls the drill daemon's capture until want inject
// bodies have arrived (or fails the test after 15s).
func waitInjects(t *testing.T, dd *drillDaemon, want int, what string) []string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = dd.snapshot()
		if len(got) >= want {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("waiting for %s: %d injects after 15s, want >= %d:\n%s",
		what, len(got), want, strings.Join(got, "\n"))
	return nil
}

func TestDrill_QuotaExhaustion_CorrelatedIncidentWithDraft(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second wall-clock drill; skipped under -short")
	}
	t.Parallel()

	base, daemon := newDrillDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}

	// Dedup window compressed to keep the drill in seconds: the
	// warning→critical escalation must cross it (quota poll 2s >
	// 1.5s) exactly as the M3 leak drill's escalation crossed the
	// real 5m window; the reactive join must land inside it.
	const dedupWindow = 1500 * time.Millisecond
	dedup, err := engine.NewDedupCache(dedupWindow, "")
	if err != nil {
		t.Fatal(err)
	}
	occ, err := store.Open(filepath.Join(t.TempDir(), "lookout.db"), store.WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer occ.Close()

	m := newMetrics()
	d := &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)),
		dedup:    dedup,
		injector: inj,
		metrics:  m,
		cluster:  "kl-m4-drill",
		mode:     "per-incident",
		routing:  engine.NewRoutingPolicy(nil),
		store:    occ,
	}
	d.board = newWatchboard(watchboardConfig{
		injector:      inj,
		metrics:       m,
		cluster:       "kl-m4-drill",
		batch:         1, // flush each warning immediately — drill value
		flushInterval: time.Second,
		rotateAfter:   50,
	})
	d.board.bind = d.bindWatchboardIncident

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	emit := func(sig engine.Signal) { d.DispatchSignal(ctx, sig) }

	// The REAL quota source over the scripted API (merged path: New
	// + Run, provider-boundary check included), poll compressed to
	// 2s.
	qAPI := &quotaDrillAPI{now: time.Now}
	qSrc, err := quota.New(drillQuotaProvider{Provider: cloud.NoProvider, api: qAPI}, quota.Config{Poll: 2 * time.Second})
	if err != nil {
		t.Fatalf("quota.New: %v", err)
	}
	// The REAL capacity source over the gated decisions API; only
	// the provider-decision sub-source matters here (no pods, no CA
	// ConfigMap in the fake clientset).
	decisions := &gatedDecisions{now: time.Now}
	cSrc := capacity.New(fake.NewSimpleClientset(), drillCapacityProvider{Provider: cloud.NoProvider, api: decisions}, capacity.Config{PollInterval: 200 * time.Millisecond})

	srcDone := make(chan error, 2)
	go func() { srcDone <- qSrc.Run(ctx, emit) }()
	go func() { srcDone <- cSrc.Run(ctx, emit) }()

	// --- Step 1: the warning forecast routes to the watchboard. ---
	got := waitInjects(t, daemon, 1, "the warning digest")
	var digestEnvelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(got[0]), &digestEnvelope); err != nil {
		t.Fatalf("digest envelope: %v", err)
	}
	if !strings.Contains(digestEnvelope.Message, `"kind":"watchboard.digest"`) ||
		!strings.Contains(digestEnvelope.Message, `"quota.forecast"`) ||
		!strings.Contains(digestEnvelope.Message, "quota:CPUS/us-east1") {
		t.Fatalf("first inject is not the warning quota digest:\n%s", digestEnvelope.Message)
	}
	t.Logf("drill step 1 — warning digest payload:\n%s", digestEnvelope.Message)

	// --- Step 2: the critical escalation opens its own session with
	// the drafted increase request attached. ---
	got = waitInjects(t, daemon, 2, "the critical quota.forecast inject")
	var criticalEnvelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(got[1]), &criticalEnvelope); err != nil {
		t.Fatalf("critical envelope: %v", err)
	}
	var payload inject.Payload
	if err := json.Unmarshal([]byte(criticalEnvelope.Message), &payload); err != nil {
		t.Fatalf("critical payload: %v\n%s", err, criticalEnvelope.Message)
	}
	if payload.Kind != quota.KindForecast || payload.Reason != quota.Reason || payload.UID != "quota:CPUS/us-east1" {
		t.Fatalf("second inject is not the quota.forecast incident: %+v", payload)
	}
	if payload.Forecast == nil || payload.Forecast.ConfidenceBasis != "linear-7d-window" {
		t.Fatalf("critical forecast attachment missing/wrong: %+v", payload.Forecast)
	}
	// ETA sanity: 40 CPUs headroom at 60/day ≈ 16h out.
	if eta := time.Until(payload.Forecast.ETA); eta < 12*time.Hour || eta > 20*time.Hour {
		t.Errorf("forecast ETA %v out of the scripted ~16h horizon", eta)
	}
	// The §10.3 draft, against the documented formula
	// (pkg/sources/quota/draft.go): suggested = ceil(max(limit×1.5,
	// usage + 2×slopePerDay×7)) = ceil(max(3000, 1960+840)) = 3000.
	draft := payload.QuotaIncreaseDraft
	if draft == nil {
		t.Fatalf("critical quota.forecast carries no quota_increase_draft:\n%s", criticalEnvelope.Message)
	}
	if draft.QuotaID != "compute.googleapis.com/cpus" || draft.Region != "us-east1" {
		t.Errorf("draft identity = (%q, %q), want (compute.googleapis.com/cpus, us-east1)", draft.QuotaID, draft.Region)
	}
	if draft.CurrentUsage != 1960 || draft.CurrentLimit != 2000 {
		t.Errorf("draft usage/limit = %v/%v, want 1960/2000", draft.CurrentUsage, draft.CurrentLimit)
	}
	if draft.SuggestedLimit != 3000 {
		t.Errorf("draft suggested_limit = %v, want 3000 (ceil(max(2000×1.5, 1960+2×60×7)))", draft.SuggestedLimit)
	}
	if math.Abs(draft.SlopePerDay-60) > 0.5 {
		t.Errorf("draft slope_per_day = %v, want ~60 (scripted series)", draft.SlopePerDay)
	}
	if !strings.Contains(draft.Justification, "Requesting an increase to 3000") {
		t.Errorf("draft justification lost the ask: %q", draft.Justification)
	}
	t.Logf("drill step 2 — critical quota.forecast payload (with draft):\n%s", criticalEnvelope.Message)

	// The critical incident's bound session (fake daemon session ids
	// are deterministic: sess-x, sess-xx, ...).
	criticalSid, ok := dedup.LookupSession(engine.EventKey{UID: "quota:CPUS/us-east1", Reason: quota.Reason})
	if !ok || criticalSid == "" {
		t.Fatal("critical quota incident has no bound session")
	}

	// --- Step 3: the reactive GCE_QUOTA_EXCEEDED scaleup failure
	// joins the OPEN session instead of opening a second one. ---
	decisions.armed.Store(true)
	joinDeadline := time.Now().Add(10 * time.Second)
	var joined []store.Occurrence
	for time.Now().Before(joinDeadline) {
		occ.Flush()
		rows, err := occ.RecentByObject(ctx, "quota:CPUS/us-east1", time.Now().Add(-time.Minute))
		if err != nil {
			t.Fatalf("RecentByObject: %v", err)
		}
		for _, r := range rows {
			if r.Kind == capacity.KindQuotaBlocked {
				joined = append(joined, r)
			}
		}
		if len(joined) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(joined) == 0 {
		t.Fatal("capacity.quota_blocked never recorded against the quota UID")
	}
	if joined[0].Route != store.RouteSuppressed {
		t.Errorf("quota_blocked route = %q, want %q (folded into the open incident, §10.3)", joined[0].Route, store.RouteSuppressed)
	}
	if joined[0].SessionID != criticalSid {
		t.Errorf("quota_blocked recorded against session %q, want the open quota session %q", joined[0].SessionID, criticalSid)
	}
	if joined[0].CanonicalReason != "QuotaExhausted" {
		t.Errorf("quota_blocked canonical reason = %q, want QuotaExhausted", joined[0].CanonicalReason)
	}
	if sid, _ := dedup.LookupSession(engine.EventKey{UID: "quota:CPUS/us-east1", Reason: "quota_blocked"}); sid != criticalSid {
		t.Errorf("dedup routes quota_blocked to %q, want %q (the family join)", sid, criticalSid)
	}
	t.Logf("drill step 3 — quota_blocked joined session %s: route=%s canonical_reason=%s message=%q",
		joined[0].SessionID, joined[0].Route, joined[0].CanonicalReason, joined[0].Message)

	// --- Step 4: hysteresis — the next critical poll (same state)
	// must not re-fire; the reactive join must not have injected. ---
	time.Sleep(2500 * time.Millisecond) // covers quota poll 3
	finalInjects := daemon.snapshot()
	if len(finalInjects) != 2 {
		t.Fatalf("drill produced %d injects, want exactly 2 (warning digest + critical incident):\n%s",
			len(finalInjects), strings.Join(finalInjects, "\n"))
	}

	cancel()
	select {
	case err := <-srcDone:
		if err != nil {
			t.Fatalf("source Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sources did not stop on cancel")
	}
}
