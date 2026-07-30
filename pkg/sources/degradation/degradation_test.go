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

package degradation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

func boolPtr(b bool) *bool { return &b }

// collector gathers emitted signals thread-safely.
type collector struct {
	mu   sync.Mutex
	sigs []engine.Signal
}

func (c *collector) emit(sig engine.Signal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sigs = append(c.sigs, sig)
}

func (c *collector) all() []engine.Signal {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]engine.Signal, len(c.sigs))
	copy(out, c.sigs)
	return out
}

func (c *collector) kinds() []string {
	var out []string
	for _, s := range c.all() {
		out = append(out, s.Kind)
	}
	return out
}

// newTestSource returns an armed source driven directly through its
// handlers (no informers) with a settable fake clock.
func newTestSource(t *testing.T, cfg Config) (*Source, *collector, *time.Time) {
	t.Helper()
	s := New(fake.NewSimpleClientset(), cfg)
	col := &collector{}
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	clock := &now
	s.now = func() time.Time { return *clock }
	s.emit = col.emit
	s.armed = true
	return s, col, clock
}

// slice builds an EndpointSlice for service svc with ready/unready
// endpoint counts.
func slice(name, ns, svc, svcUID string, ready, unready int) *discoveryv1.EndpointSlice {
	sl := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{discoveryv1.LabelServiceName: svc},
		},
	}
	if svcUID != "" {
		sl.OwnerReferences = []metav1.OwnerReference{{Kind: "Service", UID: types.UID(svcUID)}}
	}
	for i := 0; i < ready; i++ {
		sl.Endpoints = append(sl.Endpoints, discoveryv1.Endpoint{Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(true)}})
	}
	for i := 0; i < unready; i++ {
		sl.Endpoints = append(sl.Endpoints, discoveryv1.Endpoint{Conditions: discoveryv1.EndpointConditions{Ready: boolPtr(false)}})
	}
	return sl
}

func pod(uid, ns, name string, ready corev1.ConditionStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Namespace: ns, Name: name},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: ready},
		}},
	}
}

// TestCapacity_GradualDeclineFires: 5/5 → 4/5 → 3/5 across the window
// is a trend (2 downward steps, drop 0.4 ≥ 0.3) — fires warning with
// the from/to/desired evidence and the compact timeline.
func TestCapacity_GradualDeclineFires(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "payment-backend", "svc-1", 5, 0))
	*clock = clock.Add(5 * time.Minute)
	s.onSlice(slice("web-a", "prod", "payment-backend", "svc-1", 4, 1))
	*clock = clock.Add(5 * time.Minute)
	s.onSlice(slice("web-a", "prod", "payment-backend", "svc-1", 3, 2))

	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %v, want exactly one", col.kinds())
	}
	sig := sigs[0]
	if sig.Kind != KindCapacity {
		t.Errorf("Kind = %q, want %q", sig.Kind, KindCapacity)
	}
	if sig.Severity != engine.SeverityWarning {
		t.Errorf("Severity = %q, want warning (ratio 0.6 > 0.5)", sig.Severity)
	}
	if sig.Key.Reason != "capacity" {
		t.Errorf("Reason = %q, want capacity (kind suffix)", sig.Key.Reason)
	}
	if sig.Key.UID != "svc-1" || sig.Namespace != "prod" || sig.Name != "payment-backend" || sig.KindOfObject != "Service" {
		t.Errorf("object identity wrong: %+v", sig.TriageEvent)
	}
	for _, want := range []string{"5/5 → 3/5", "over 10m0s", "window 15m0s", "5/5 → 4/5 → 3/5"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("Message %q missing %q", sig.Message, want)
		}
	}
}

// TestCapacity_CriticalWhenRatioHalves: landing at or below
// CriticalRatio escalates the same signal to critical.
func TestCapacity_CriticalWhenRatioHalves(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "web", "svc-1", 6, 0))
	*clock = clock.Add(3 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 4, 2))
	*clock = clock.Add(3 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 2, 4))

	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %v, want exactly one", col.kinds())
	}
	if sigs[0].Severity != engine.SeverityCritical {
		t.Errorf("Severity = %q, want critical (ratio 0.33 ≤ 0.5)", sigs[0].Severity)
	}
}

// TestCapacity_SingleBlipDoesNotFire: one downward step — however
// deep — is a blip, not a trend.
func TestCapacity_SingleBlipDoesNotFire(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "web", "svc-1", 5, 0))
	*clock = clock.Add(5 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 2, 3)) // drop 0.6, but ONE step
	*clock = clock.Add(5 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 5, 0)) // recovered

	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("single blip fired: %v", got)
	}
}

// TestCapacity_BurstCoalescedIntoOneStep: near-simultaneous slice
// writes (the EndpointSlice controller sharding one change) coalesce
// into one sample — still a single step, still no fire.
func TestCapacity_BurstCoalescedIntoOneStep(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "web", "svc-1", 5, 0))
	*clock = clock.Add(5 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 4, 1))
	*clock = clock.Add(2 * time.Second) // within SampleFloor
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 3, 2))

	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("coalesced burst fired: %v", got)
	}
}

// TestCapacity_NeverFiresAtZero: a decline that lands at 0 ready is
// objectstate.endpoints_empty's critical — this source stays silent.
func TestCapacity_NeverFiresAtZero(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "web", "svc-1", 4, 0))
	*clock = clock.Add(5 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 2, 2))
	*clock = clock.Add(5 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 0, 4)) // empty — not ours

	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("fired at ratio 0 (objectstate's transition): %v", got)
	}
}

// TestCapacity_SingleFirePerEpisode: after firing, further decline
// does not re-fire; recovery to baseline clears the latch (and the
// §7.4 observer), after which a NEW decline fires fresh.
func TestCapacity_SingleFireAndClearance(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "web", "svc-1", 5, 0))
	*clock = clock.Add(3 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 4, 1))
	*clock = clock.Add(3 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 3, 2))
	if len(col.all()) != 1 {
		t.Fatalf("signals = %v, want one", col.kinds())
	}

	inc := engine.Incident{
		Key: engine.EventKey{UID: "svc-1", Reason: "capacity"},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Service", Name: "web"},
	}
	if verdict, ok := s.ClearanceObserver().Clearance(inc); !ok || verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want judged + NOT cleared while degraded", verdict, ok)
	}

	// Further decline: latched, no second signal.
	*clock = clock.Add(3 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 2, 3))
	if len(col.all()) != 1 {
		t.Fatalf("re-fired inside one episode: %v", col.kinds())
	}

	// Recovery to the recorded baseline clears.
	*clock = clock.Add(3 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 5, 0))
	verdict, ok := s.ClearanceObserver().Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("verdict = %+v ok=%v, want cleared/recovered", verdict, ok)
	}
	if verdict.StableSince.IsZero() {
		t.Error("StableSince not stamped on recovery")
	}

	// A fresh decline after clearance is a new episode.
	*clock = clock.Add(7 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 4, 1))
	*clock = clock.Add(7 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 3, 2))
	// Window start by now is the recovered 5/5 sample (older pruned).
	if got := col.kinds(); len(got) != 2 {
		t.Fatalf("new episode after clearance did not fire: %v", got)
	}
}

// TestCapacity_ServiceDeletedIsObjectDeleted: last slice gone =
// object removal, and the observer closes the incident accordingly.
func TestCapacity_ServiceDeletedIsObjectDeleted(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "web", "svc-1", 5, 0))
	*clock = clock.Add(3 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 4, 1))
	*clock = clock.Add(3 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 3, 2))
	if len(col.all()) != 1 {
		t.Fatalf("setup fire missing: %v", col.kinds())
	}
	s.onSliceDelete(slice("web-a", "prod", "web", "svc-1", 3, 2))

	inc := engine.Incident{
		Key: engine.EventKey{UID: "svc-1", Reason: "capacity"},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Service", Name: "web"},
	}
	verdict, ok := s.ClearanceObserver().Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("verdict = %+v ok=%v, want cleared/object_deleted", verdict, ok)
	}
}

// TestCapacity_MultiSliceAggregation: the ratio is per-SERVICE across
// slices, so a decline spread over two slices still trends.
func TestCapacity_MultiSliceAggregation(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "web", "svc-1", 3, 0))
	*clock = clock.Add(time.Minute)
	s.onSlice(slice("web-b", "prod", "web", "svc-1", 3, 0)) // 6/6
	*clock = clock.Add(4 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 2, 1)) // 5/6
	*clock = clock.Add(4 * time.Minute)
	s.onSlice(slice("web-b", "prod", "web", "svc-1", 1, 2)) // 3/6: drop 0.5, 2 steps

	sigs := col.all()
	if len(sigs) != 1 || sigs[0].Kind != KindCapacity {
		t.Fatalf("signals = %v, want one capacity", col.kinds())
	}
	if sigs[0].Severity != engine.SeverityCritical {
		t.Errorf("Severity = %q, want critical (3/6 = 0.5)", sigs[0].Severity)
	}
}

// TestCapacity_UnarmedRecordsWithoutFiring: §7.2 restart discipline.
func TestCapacity_UnarmedRecordsWithoutFiring(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})
	s.armed = false

	s.onSlice(slice("web-a", "prod", "web", "svc-1", 5, 0))
	*clock = clock.Add(5 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 4, 1))
	*clock = clock.Add(5 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 3, 2))

	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("unarmed source fired: %v", got)
	}
}

// TestCapacity_TickAgesWindow: a service whose slices stop updating
// still gets samples from the tick, so the trend can complete.
func TestCapacity_TickAgesWindow(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "web", "svc-1", 5, 0))
	*clock = clock.Add(4 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 4, 1))
	*clock = clock.Add(4 * time.Minute)
	s.onSlice(slice("web-a", "prod", "web", "svc-1", 3, 2))
	if len(col.all()) != 1 {
		t.Fatalf("trend fire missing: %v", col.kinds())
	}
	// Ticks alone keep the series alive (recovery path exercised in
	// the clearance test; here just prove tick sampling is safe).
	for i := 0; i < 20; i++ {
		*clock = clock.Add(time.Minute)
		s.send(s.tick(*clock))
	}
	if len(col.all()) != 1 {
		t.Fatalf("tick re-fired a latched episode: %v", col.kinds())
	}
}

// ---- Probe flaps ----

// TestProbeFlap_CountsFlips: 4 readiness transitions within the
// window fire; the reset re-counts toward the next burst.
func TestProbeFlap_CountsFlips(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	seq := []corev1.ConditionStatus{
		corev1.ConditionTrue,  // baseline
		corev1.ConditionFalse, // flip 1
		corev1.ConditionTrue,  // flip 2
		corev1.ConditionFalse, // flip 3
	}
	for _, st := range seq {
		s.onPod(pod("p1", "prod", "web-x", st))
		*clock = clock.Add(time.Minute)
	}
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("3 flips fired early: %v", got)
	}
	s.onPod(pod("p1", "prod", "web-x", corev1.ConditionTrue)) // flip 4

	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %v, want one", col.kinds())
	}
	sig := sigs[0]
	if sig.Kind != KindProbeFlap || sig.Severity != engine.SeverityWarning {
		t.Errorf("got %q/%q, want probe_flap warning", sig.Kind, sig.Severity)
	}
	if sig.Key.UID != "p1" || sig.KindOfObject != "Pod" || sig.Name != "web-x" {
		t.Errorf("object identity wrong: %+v", sig.TriageEvent)
	}
	if !strings.Contains(sig.Message, "flipped 4 times within 15m0s") {
		t.Errorf("Message %q missing flip count/window", sig.Message)
	}

	// Reset: the next flip alone must not immediately re-fire.
	*clock = clock.Add(time.Minute)
	s.onPod(pod("p1", "prod", "web-x", corev1.ConditionFalse))
	if len(col.all()) != 1 {
		t.Fatalf("re-fired right after reset: %v", col.kinds())
	}
}

// TestProbeFlap_SustainedFailureNeverFires: down-and-stays-down is
// ONE transition — the k8s-events Unhealthy path's territory.
func TestProbeFlap_SustainedFailureNeverFires(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	s.onPod(pod("p1", "prod", "web-x", corev1.ConditionTrue))
	*clock = clock.Add(time.Minute)
	s.onPod(pod("p1", "prod", "web-x", corev1.ConditionFalse))
	for i := 0; i < 10; i++ {
		*clock = clock.Add(time.Minute)
		s.onPod(pod("p1", "prod", "web-x", corev1.ConditionFalse)) // no transition
	}
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("sustained unreadiness fired probe_flap: %v", got)
	}
}

// TestProbeFlap_FlipsOutsideWindowDontCount.
func TestProbeFlap_FlipsOutsideWindowDontCount(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Window: 10 * time.Minute})

	states := []corev1.ConditionStatus{corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionTrue, corev1.ConditionFalse}
	for _, st := range states {
		s.onPod(pod("p1", "prod", "web-x", st))
		*clock = clock.Add(6 * time.Minute) // each flip ages out before the next two land
	}
	s.onPod(pod("p1", "prod", "web-x", corev1.ConditionTrue))
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("stale flips counted: %v", got)
	}
}

// TestProbeFlap_Clearance: flap incident clears only when the pod is
// Ready with no flips left in the window; a deleted pod closes as
// object_deleted. The source's own observer must be asked BEFORE the
// generic pod observer (registration order in setupRecovery) — pin
// that it judges, and judges flap-aware.
func TestProbeFlap_Clearance(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})

	seq := []corev1.ConditionStatus{
		corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionTrue,
		corev1.ConditionFalse, corev1.ConditionTrue,
	}
	for _, st := range seq {
		s.onPod(pod("p1", "prod", "web-x", st))
		*clock = clock.Add(time.Minute)
	}
	if len(col.all()) != 1 {
		t.Fatalf("setup fire missing: %v", col.kinds())
	}

	inc := engine.Incident{
		Key: engine.EventKey{UID: "p1", Reason: "probe_flap"},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Pod", Name: "web-x"},
	}
	// Immediately after the burst the pod is Ready — but flips are
	// still in-window after another one arrives, so NOT cleared (this
	// is exactly where the generic "pod is Ready" observer would be
	// wrong).
	s.onPod(pod("p1", "prod", "web-x", corev1.ConditionFalse))
	*clock = clock.Add(time.Second)
	s.onPod(pod("p1", "prod", "web-x", corev1.ConditionTrue))
	if verdict, ok := s.ClearanceObserver().Clearance(inc); !ok || verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want judged + NOT cleared while flips in window", verdict, ok)
	}

	// All flips age out and the pod stays Ready → cleared.
	*clock = clock.Add(16 * time.Minute)
	verdict, ok := s.ClearanceObserver().Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("verdict = %+v ok=%v, want cleared/recovered", verdict, ok)
	}

	// Deleted pod → object_deleted.
	s.onPodDelete(pod("p1", "prod", "web-x", corev1.ConditionTrue))
	verdict, ok = s.ClearanceObserver().Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("verdict = %+v ok=%v, want cleared/object_deleted", verdict, ok)
	}
}

// TestClearance_DeclinesForeignIncidents: the observer judges ONLY
// this source's kinds, so it composes with PodClearance in the
// tracker's observer list.
func TestClearance_DeclinesForeignIncidents(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	inc := engine.Incident{
		Key: engine.EventKey{UID: "p1", Reason: "CrashLoopBackOff"},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Pod", Name: "web-x"},
	}
	if _, ok := s.ClearanceObserver().Clearance(inc); ok {
		t.Fatal("observer claimed a k8s-event incident it cannot judge")
	}
	if _, ok := s.ClearanceObserver().Clearance(engine.Incident{
		Key: engine.EventKey{UID: "n1", Reason: "capacity"},
		Ref: engine.IncidentRef{KindOfObject: "Node", Name: "node-1"},
	}); ok {
		t.Fatal("observer claimed a capacity reason on a non-Service object")
	}
}

// TestArmAfterSync: seeded state fires nothing on the initial LIST; a
// live decline post-arm fires through the real informer path.
func TestArmAfterSync(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset(
		slice("web-a", "prod", "web", "svc-1", 5, 0),
		pod("p1", "prod", "web-x", corev1.ConditionTrue),
	)
	s := New(client, Config{TickInterval: 10 * time.Millisecond, SampleFloor: time.Nanosecond})
	col := &collector{}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, col.emit) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		armed := s.armed
		s.mu.Unlock()
		if armed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("initial LIST fired: %v", got)
	}

	// Live decline post-arm: 5/5 → 4/5 → 3/5.
	for _, counts := range [][2]int{{4, 1}, {3, 2}} {
		if _, err := client.DiscoveryV1().EndpointSlices("prod").Update(ctx, slice("web-a", "prod", "web", "svc-1", counts[0], counts[1]), metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update slice: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // let the update land as its own sample
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fmt.Sprint(col.kinds()) == fmt.Sprint([]string{KindCapacity}) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if fmt.Sprint(col.kinds()) != fmt.Sprint([]string{KindCapacity}) {
		t.Fatalf("signals = %v, want [%s]", col.kinds(), KindCapacity)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v on clean shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestWithFactory_SharedInformerSet: informers register on the
// externally owned factory (§6.3) — no private factory is built.
func TestWithFactory_SharedInformerSet(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(client, 0)
	s := New(client, Config{TickInterval: time.Hour})
	s.WithFactory(factory)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = s.Run(ctx, func(engine.Signal) {}) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		armed := s.armed
		s.mu.Unlock()
		if armed {
			return // synced through the shared factory
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("source never armed via the shared factory")
}

// TestRequiredAccess_CoversEveryWatch pins the §11 declaration to the
// informer set — both grants already exist in the shipped ClusterRole.
func TestRequiredAccess_CoversEveryWatch(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), Config{})
	want := map[string]bool{
		"list pods cluster-wide":                             true,
		"watch pods cluster-wide":                            true,
		"list endpointslices.discovery.k8s.io cluster-wide":  true,
		"watch endpointslices.discovery.k8s.io cluster-wide": true,
	}
	got := map[string]bool{}
	for _, req := range s.RequiredAccess() {
		got[req.String()] = true
	}
	for k := range want {
		if !got[k] {
			t.Errorf("missing declared requirement %q", k)
		}
	}
	if len(got) != len(want) {
		t.Errorf("declared %d requirements, want %d: %v", len(got), len(want), got)
	}
}

// TestProbe_FailsLoudly: §11 — a denied grant is a startup error
// naming the source, never a silent empty watch.
func TestProbe_FailsLoudly(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), Config{})
	_, err := sources.Probe(context.Background(), denyReviewer{}, s)
	if err == nil {
		t.Fatal("Probe passed with all access denied")
	}
	if !strings.Contains(err.Error(), Name) {
		t.Errorf("error %q does not name the source", err)
	}
}

type denyReviewer struct{}

func (denyReviewer) Allowed(context.Context, sources.Requirement) (sources.Decision, error) {
	return sources.Decision{}, nil
}

// TestKindInventoryFrozen: kinds are wire contract (§7.3) — pinned.
func TestKindInventoryFrozen(t *testing.T) {
	t.Parallel()
	if KindCapacity != "degradation.capacity" {
		t.Errorf("KindCapacity = %q", KindCapacity)
	}
	if KindProbeFlap != "degradation.probe_flap" {
		t.Errorf("KindProbeFlap = %q", KindProbeFlap)
	}
	if Name != "degradation" {
		t.Errorf("Name = %q", Name)
	}
}
