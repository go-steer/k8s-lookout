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

package capacity

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// capacityProvider is a test cloud.Provider whose only capability is
// a scripted CapacityAPI.
type capacityProvider struct {
	cloud.Provider // embed NoProvider for the other capabilities
	api            cloud.CapacityAPI
}

func newCapacityProvider(api cloud.CapacityAPI) capacityProvider {
	return capacityProvider{Provider: cloud.NoProvider, api: api}
}

func (p capacityProvider) Name() string                        { return "test" }
func (p capacityProvider) Capacity() (cloud.CapacityAPI, bool) { return p.api, p.api != nil }

type scriptedDecisions struct {
	mu        sync.Mutex
	decisions []cloud.ScaleDecision
	windows   []cloud.TimeWindow
	err       error
}

func (s *scriptedDecisions) ScaleDecisions(_ context.Context, w cloud.TimeWindow) ([]cloud.ScaleDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windows = append(s.windows, w)
	if s.err != nil {
		return nil, s.err
	}
	return s.decisions, nil
}

func TestSourceContract(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	if s.Name() != "capacity" {
		t.Errorf("Name() = %q, want capacity (§7.2 table)", s.Name())
	}
	if s.Scope() != sources.ScopeCluster {
		t.Errorf("Scope() = %v, want cluster", s.Scope())
	}
	var _ sources.Source = s
	var _ sources.AccessDeclarer = s
}

// TestRequiredAccess pins the §11 declaration, in particular the
// namespace- AND name-scoped ConfigMap get that the kube-system Role
// (deploy/14-role-watcher-capacity.yaml) satisfies.
func TestRequiredAccess(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	reqs := s.RequiredAccess()
	want := map[string]bool{
		"list events cluster-wide":  false,
		"watch events cluster-wide": false,
		"list pods cluster-wide":    false,
		"watch pods cluster-wide":   false,
		"get configmaps cluster-autoscaler-status in namespace kube-system": false,
	}
	for _, r := range reqs {
		key := r.String()
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected requirement %q", key)
			continue
		}
		want[key] = true
	}
	for key, seen := range want {
		if !seen {
			t.Errorf("missing requirement %q", key)
		}
	}
}

// TestRun_ArmAfterSync is the integration cut of the arming rule: a
// NotTriggerScaleUp event present BEFORE Run (the informer's initial
// LIST — up to an hour of stale history) never emits; the same event
// arriving after sync does.
func TestRun_ArmAfterSync(t *testing.T) {
	t.Parallel()
	stale := caEvent("NotTriggerScaleUp", "pod didn't trigger scale-up: 1 max node group size reached")
	stale.Name = "stale.evt"
	client := fake.NewSimpleClientset(stale)

	s := New(client, cloud.NoProvider, Config{PollInterval: time.Hour})
	s.logf = func(string, ...any) {}
	sigs := make(chan engine.Signal, 16)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, func(sig engine.Signal) { sigs <- sig }) }()

	// Wait for arming (sync completed).
	waitFor(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.armed })

	live := caEvent("NotTriggerScaleUp", "pod didn't trigger scale-up: 2 Insufficient cpu")
	live.Name = "live.evt"
	if _, err := client.CoreV1().Events("shop").Create(ctx, live, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	select {
	case sig := <-sigs:
		if sig.Kind != KindPending || !strings.Contains(sig.Message, "Insufficient cpu") {
			t.Fatalf("got %+v, want the LIVE capacity.pending", sig)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live NotTriggerScaleUp never emitted")
	}
	select {
	case sig := <-sigs:
		t.Fatalf("extra signal %+v — the pre-sync event leaked through arming", sig)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error on clean shutdown: %v", err)
	}
}

// TestProviderAbsent_ExplicitNotSilent is the §2 portability path: no
// provider → sub-source 3 off with the standard explicit log naming
// the reason; the source still runs and the portable sub-sources
// still work (a status gap fires below).
func TestProviderAbsent_ExplicitNotSilent(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{PollInterval: time.Hour})
	if s.decisions != nil {
		t.Fatal("decisions API present with NoProvider")
	}
	var logs []string
	var mu sync.Mutex
	s.logf = func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, sprintf(format, args...))
		mu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx, func(engine.Signal) {}) }()
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, l := range logs {
			if strings.Contains(l, `unavailable reason="no cloud provider configured"`) &&
				strings.Contains(l, "still fire on scaleup failures") {
				return true
			}
		}
		return false
	})
	cancel()

	// And the polled sub-sources still judge: provider absence does
	// not disable the portable gap detection.
	var got []engine.Signal
	s2 := New(fake.NewSimpleClientset(), cloud.NoProvider, Config{})
	s2.emit = func(sig engine.Signal) { got = append(got, sig) }
	groups, err := parseStatus(yamlStatusDoc)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	s2.judgeStatus(groups, t0)
	s2.judgeStatus(groups, t0.Add(3*time.Minute))
	if len(got) != 1 {
		t.Fatalf("portable gap path fired %d, want 1", len(got))
	}
}

// TestPollDecisions_KindsAndSeverities maps provider decisions to the
// three remedy-disjoint kinds; unmatched reasons are context, not
// signals.
func TestPollDecisions_KindsAndSeverities(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 26, 9, 59, 0, 0, time.UTC)
	api := &scriptedDecisions{decisions: []cloud.ScaleDecision{
		{Time: at, Decision: "noScaleUp", NodeGroup: "mig-a", Reason: "GCE_STOCKOUT", Message: "zone exhausted"},
		{Time: at, Decision: "noScaleUp", NodeGroup: "mig-b", Reason: "GCE_QUOTA_EXCEEDED"},
		{Time: at, Decision: "noScaleUp", NodeGroup: "mig-c", Reason: "IP_SPACE_EXHAUSTED"},
		{Time: at, Decision: "noScaleUp", NodeGroup: "mig-d", Reason: "no.scale.up.mig.failing.predicate"},
		{Time: at, Decision: "scaleUp", NodeGroup: "mig-e", Reason: "TRIGGERED"},
	}}
	s := New(fake.NewSimpleClientset(), newCapacityProvider(api), Config{})
	var got []engine.Signal
	s.emit = func(sig engine.Signal) { got = append(got, sig) }

	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	if err := s.pollDecisions(context.Background(), now); err != nil {
		t.Fatalf("pollDecisions: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("emitted %d signals (%v), want exactly the 3 remedy-bearing kinds", len(got), got)
	}
	wantKinds := map[string]string{
		"mig-a": KindStockout,
		"mig-b": KindQuotaBlocked,
		"mig-c": KindIPExhausted,
	}
	for _, sig := range got {
		if sig.Kind != wantKinds[sig.Name] {
			t.Errorf("nodegroup %s → kind %s, want %s", sig.Name, sig.Kind, wantKinds[sig.Name])
		}
		if sig.Severity != engine.SeverityCritical {
			t.Errorf("%s severity = %s, want critical", sig.Kind, sig.Severity)
		}
		if sig.KindOfObject != "NodeGroup" || !strings.HasPrefix(sig.Key.UID, "nodegroup:") {
			t.Errorf("%s identity = %s/%s", sig.Kind, sig.KindOfObject, sig.Key.UID)
		}
	}

	// Window bookkeeping: first poll looks back one interval; the
	// next continues from the last success.
	if err := s.pollDecisions(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.windows) != 2 || !api.windows[1].Start.Equal(now) {
		t.Errorf("windows = %+v, want the second to start at the first's end", api.windows)
	}
}

// TestPollDecisions_ErrorKeepsWindow: a failed poll must re-query the
// same window next time instead of dropping it.
func TestPollDecisions_ErrorKeepsWindow(t *testing.T) {
	t.Parallel()
	api := &scriptedDecisions{err: context.DeadlineExceeded}
	s := New(fake.NewSimpleClientset(), newCapacityProvider(api), Config{})
	s.emit = func(engine.Signal) {}

	now := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	if err := s.pollDecisions(context.Background(), now); err == nil {
		t.Fatal("scripted error did not surface")
	}
	api.mu.Lock()
	api.err = nil
	api.mu.Unlock()
	if err := s.pollDecisions(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	first, second := api.windows[0], api.windows[1]
	if !second.Start.Equal(first.Start) {
		t.Errorf("failed window dropped: first=%+v second=%+v (want same start)", first, second)
	}
}

// TestKindsAreFrozenStrings pins the §7.3 kind names — playbooks and
// AX match on these exact strings.
func TestKindsAreFrozenStrings(t *testing.T) {
	t.Parallel()
	frozen := map[string]string{
		KindPending:      "capacity.pending",
		KindScaleUp:      "capacity.scaleup",
		KindScaleDown:    "capacity.scaledown",
		KindScaleUpGap:   "capacity.scaleup_gap",
		KindStockout:     "capacity.stockout",
		KindQuotaBlocked: "capacity.quota_blocked",
		KindIPExhausted:  "capacity.ip_exhausted",
		KindPendingAged:  "capacity.pending-aged",
	}
	for got, want := range frozen {
		if got != want {
			t.Errorf("kind %q, want %q (frozen)", got, want)
		}
	}
	// §7.4/§7.5-adjacent: pending family joins FailedScheduling.
	if engine.CanonicalReason("pending") != "FailedScheduling" ||
		engine.CanonicalReason("pending-aged") != "FailedScheduling" {
		t.Error("capacity pending reasons must share the FailedScheduling dedup family (one stuck pod = one session)")
	}
	if engine.CanonicalReason("scaleup_gap") != "scaleup_gap" {
		t.Error("nodegroup-keyed reasons map to themselves")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition never became true within 5s")
}

func sprintf(format string, args ...any) string {
	return strings.TrimSpace(fmt.Sprintf(format, args...))
}
