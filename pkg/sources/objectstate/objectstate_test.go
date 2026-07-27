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

package objectstate

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

func boolPtr(b bool) *bool    { return &b }
func int32Ptr(n int32) *int32 { return &n }

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

func node(uid, name string, ready corev1.ConditionStatus) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: ready, Message: "kubelet " + string(ready)},
		}},
	}
}

func TestNode_ReadyTrueToFalse_EmitsNodeNotReady(t *testing.T) {
	t.Parallel()
	// High flap threshold: this test exercises the plain Ready flip
	// only (flap detection has its own tests).
	s, col, _ := newTestSource(t, Config{FlapTransitions: 100})

	s.onNode(node("n1", "gke-pool-a-1", corev1.ConditionTrue))
	s.onNode(node("n1", "gke-pool-a-1", corev1.ConditionFalse))

	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %v, want exactly one", col.kinds())
	}
	sig := sigs[0]
	if sig.Kind != KindNodeNotReady {
		t.Errorf("Kind = %q, want %q", sig.Kind, KindNodeNotReady)
	}
	if sig.Severity != engine.SeverityCritical {
		t.Errorf("Severity = %q, want critical", sig.Severity)
	}
	if sig.Key.Reason != "node_notready" {
		t.Errorf("Reason = %q, want node_notready (kind suffix)", sig.Key.Reason)
	}
	if sig.Key.UID != "n1" || sig.Name != "gke-pool-a-1" || sig.KindOfObject != "Node" || sig.Node != "gke-pool-a-1" {
		t.Errorf("object identity wrong: %+v", sig.TriageEvent)
	}
	if sig.Namespace != "" {
		t.Errorf("Namespace = %q, want empty (cluster-scoped)", sig.Namespace)
	}

	// Unknown counts as not ready too (node controller lost contact).
	s.onNode(node("n1", "gke-pool-a-1", corev1.ConditionTrue))
	s.onNode(node("n1", "gke-pool-a-1", corev1.ConditionUnknown))
	if got := col.kinds(); len(got) != 2 || got[1] != KindNodeNotReady {
		t.Errorf("Ready→Unknown: signals = %v, want a second node_notready", got)
	}
}

func TestNode_FirstObservationNotReady_NoSignal(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{})
	// A node first seen NotReady (creation, or initial LIST) is not a
	// transition.
	s.onNode(node("n1", "gke-pool-a-1", corev1.ConditionFalse))
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("signals = %v, want none for first observation", got)
	}
	// Recovery (False→True) is the tracker's business, not a signal.
	s.onNode(node("n1", "gke-pool-a-1", corev1.ConditionTrue))
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("signals = %v, want none for False→True", got)
	}
}

func TestNode_FlapDetection(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{FlapTransitions: 3, FlapWindow: 10 * time.Minute})

	base := *clock
	flip := func(status corev1.ConditionStatus, at time.Duration) {
		*clock = base.Add(at)
		s.onNode(node("n1", "node-1", status))
	}
	flip(corev1.ConditionTrue, 0)              // baseline
	flip(corev1.ConditionFalse, 1*time.Minute) // transition 1 → node_notready
	flip(corev1.ConditionTrue, 2*time.Minute)  // transition 2
	flip(corev1.ConditionFalse, 3*time.Minute) // transition 3 → notready + flapping

	kinds := col.kinds()
	want := []string{KindNodeNotReady, KindNodeNotReady, KindNodeFlapping}
	if fmt.Sprint(kinds) != fmt.Sprint(want) {
		t.Fatalf("signals = %v, want %v", kinds, want)
	}
	flap := col.all()[2]
	if flap.Severity != engine.SeverityWarning || flap.Key.Reason != "node_flapping" {
		t.Errorf("flap signal = severity %q reason %q, want warning/node_flapping", flap.Severity, flap.Key.Reason)
	}

	// The transition memory reset after firing: the NEXT transition
	// must not immediately re-fire flapping.
	flip(corev1.ConditionTrue, 4*time.Minute)
	if got := col.kinds(); len(got) != 3 {
		t.Errorf("after reset: signals = %v, want no new flapping yet", got)
	}
}

func TestNode_TransitionsOutsideWindowDontCountTowardFlap(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{FlapTransitions: 3, FlapWindow: 10 * time.Minute})

	base := *clock
	flip := func(status corev1.ConditionStatus, at time.Duration) {
		*clock = base.Add(at)
		s.onNode(node("n1", "node-1", status))
	}
	flip(corev1.ConditionTrue, 0)
	flip(corev1.ConditionFalse, 1*time.Minute)  // t1
	flip(corev1.ConditionTrue, 20*time.Minute)  // t2 — t1 aged out
	flip(corev1.ConditionFalse, 40*time.Minute) // t3 — t2 aged out

	for _, k := range col.kinds() {
		if k == KindNodeFlapping {
			t.Fatalf("flapping fired from transitions spread over 40m (window 10m): %v", col.kinds())
		}
	}
}

// deploymentFixture builds a mid-rollout deployment.
type deploymentFixture struct {
	uid                string
	name               string
	generation         int64
	observedGeneration int64
	replicas           int32
	updated            int32
	available          int32
	total              int32
	deadlineSeconds    *int32
	paused             bool
	progressingSince   time.Time
	progressingReason  string
	noCondition        bool
}

func (f deploymentFixture) build() *appsv1.Deployment {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			UID:        types.UID(f.uid),
			Namespace:  "prod",
			Name:       f.name,
			Generation: f.generation,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas:                &f.replicas,
			Paused:                  f.paused,
			ProgressDeadlineSeconds: f.deadlineSeconds,
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: f.observedGeneration,
			Replicas:           f.total,
			UpdatedReplicas:    f.updated,
			AvailableReplicas:  f.available,
		},
	}
	if !f.noCondition {
		d.Status.Conditions = []appsv1.DeploymentCondition{{
			Type:           appsv1.DeploymentProgressing,
			Status:         corev1.ConditionTrue,
			Reason:         f.progressingReason,
			LastUpdateTime: metav1.NewTime(f.progressingSince),
		}}
	}
	return d
}

func TestDeployment_ProgressDeadlineApproaching(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{})
	now := *clock

	// Default deadline (nil → 600s), stuck for 9m = 90% > 80%.
	stuck := deploymentFixture{
		uid: "d1", name: "checkout", generation: 3, observedGeneration: 3,
		replicas: 3, updated: 1, available: 2, total: 4,
		progressingSince:  now.Add(-9 * time.Minute),
		progressingReason: "ReplicaSetUpdated",
	}
	s.onDeployment(stuck.build())

	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %v, want exactly one progress_deadline", col.kinds())
	}
	sig := sigs[0]
	if sig.Kind != KindProgressDeadline || sig.Severity != engine.SeverityWarning {
		t.Errorf("got kind %q severity %q, want %q/warning", sig.Kind, sig.Severity, KindProgressDeadline)
	}
	if sig.Key.Reason != "progress_deadline" {
		t.Errorf("Reason = %q, want progress_deadline", sig.Key.Reason)
	}
	if sig.Namespace != "prod" || sig.Name != "checkout" || sig.KindOfObject != "Deployment" {
		t.Errorf("object identity wrong: %+v", sig.TriageEvent)
	}
	if !strings.Contains(sig.Message, "progressDeadlineSeconds=600") {
		t.Errorf("message should carry the deadline: %q", sig.Message)
	}

	// Fired-once-per-generation gate: further sweeps stay quiet.
	s.send(s.sweep(now.Add(time.Minute)))
	if got := col.kinds(); len(got) != 1 {
		t.Errorf("re-sweep re-fired: %v", got)
	}

	// A new generation stalling re-arms the gate.
	stuck.generation, stuck.observedGeneration = 4, 4
	stuck.progressingSince = now.Add(-9 * time.Minute)
	s.onDeployment(stuck.build())
	if got := col.kinds(); len(got) != 2 {
		t.Errorf("new generation did not re-fire: %v", got)
	}
}

func TestDeployment_ProgressDeadlineMath(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		fix  deploymentFixture
		want bool
	}{
		{"below threshold", deploymentFixture{
			generation: 1, observedGeneration: 1, replicas: 2, updated: 1, available: 1, total: 2,
			deadlineSeconds: int32Ptr(1000), progressingSince: now.Add(-500 * time.Second),
		}, false},
		{"above threshold", deploymentFixture{
			generation: 1, observedGeneration: 1, replicas: 2, updated: 1, available: 1, total: 2,
			deadlineSeconds: int32Ptr(1000), progressingSince: now.Add(-900 * time.Second),
		}, true},
		{"deadline disabled (MaxInt32) never fires", deploymentFixture{
			generation: 1, observedGeneration: 1, replicas: 2, updated: 1, available: 1, total: 2,
			deadlineSeconds: int32Ptr(math.MaxInt32), progressingSince: now.Add(-24 * time.Hour),
		}, false},
		{"nil deadline uses the 600s API default", deploymentFixture{
			generation: 1, observedGeneration: 1, replicas: 2, updated: 1, available: 1, total: 2,
			progressingSince: now.Add(-481 * time.Second), // 80% of 600s = 480s
		}, true},
		{"complete rollout never fires", deploymentFixture{
			generation: 1, observedGeneration: 1, replicas: 2, updated: 2, available: 2, total: 2,
			progressingSince: now.Add(-1 * time.Hour),
		}, false},
		{"paused deployment never fires", deploymentFixture{
			generation: 1, observedGeneration: 1, replicas: 2, updated: 1, available: 1, total: 2, paused: true,
			progressingSince: now.Add(-1 * time.Hour),
		}, false},
		{"unreconciled generation is stale clock — no fire", deploymentFixture{
			generation: 5, observedGeneration: 4, replicas: 2, updated: 1, available: 1, total: 2,
			progressingSince: now.Add(-1 * time.Hour),
		}, false},
		{"already exceeded belongs to k8s-events", deploymentFixture{
			generation: 1, observedGeneration: 1, replicas: 2, updated: 1, available: 1, total: 2,
			progressingSince: now.Add(-1 * time.Hour), progressingReason: "ProgressDeadlineExceeded",
		}, false},
		{"no progressing condition — nothing to clock against", deploymentFixture{
			generation: 1, observedGeneration: 1, replicas: 2, updated: 1, available: 1, total: 2,
			noCondition: true,
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fire, msg := assessProgress(tc.fix.build(), 0.8, now)
			if fire != tc.want {
				t.Errorf("assessProgress = %v (msg %q), want %v", fire, msg, tc.want)
			}
		})
	}
}

// slice builds an EndpointSlice for service svc with the given ready
// flags (nil = unset, which the API contract counts as ready).
func slice(name, ns, svc, svcUID string, ready ...*bool) *discoveryv1.EndpointSlice {
	sl := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{discoveryv1.LabelServiceName: svc},
		},
	}
	if svcUID != "" {
		sl.OwnerReferences = []metav1.OwnerReference{{Kind: "Service", Name: svc, UID: types.UID(svcUID)}}
	}
	for _, r := range ready {
		sl.Endpoints = append(sl.Endpoints, discoveryv1.Endpoint{
			Conditions: discoveryv1.EndpointConditions{Ready: r},
		})
	}
	return sl
}

func TestEndpoints_ReadyCountToZero(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{})

	s.onSlice(slice("web-abc", "prod", "web", "svc-1", boolPtr(true), boolPtr(true)))
	s.onSlice(slice("web-abc", "prod", "web", "svc-1", boolPtr(false), boolPtr(false)))

	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %v, want exactly one endpoints_empty", col.kinds())
	}
	sig := sigs[0]
	if sig.Kind != KindEndpointsEmpty || sig.Severity != engine.SeverityCritical {
		t.Errorf("got kind %q severity %q, want %q/critical", sig.Kind, sig.Severity, KindEndpointsEmpty)
	}
	if sig.KindOfObject != "Service" || sig.Name != "web" || sig.Namespace != "prod" || sig.Key.UID != "svc-1" {
		t.Errorf("signal must reference the SERVICE: %+v", sig.TriageEvent)
	}
	if sig.Key.Reason != "endpoints_empty" {
		t.Errorf("Reason = %q, want endpoints_empty", sig.Key.Reason)
	}
}

func TestEndpoints_CreatedEmptyDoesNotFire(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{})

	// A brand-new Service whose first slice arrives empty: 0→0 is not
	// a transition (the §7.2 example is count → 0, not born-at-0).
	s.onSlice(slice("new-abc", "prod", "new", "svc-2"))
	s.onSlice(slice("new-abc", "prod", "new", "svc-2", boolPtr(false)))
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("created-empty service fired: %v", got)
	}
}

func TestEndpoints_MultiSliceAggregation(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{})

	s.onSlice(slice("web-a", "prod", "web", "svc-1", boolPtr(true)))
	s.onSlice(slice("web-b", "prod", "web", "svc-1", boolPtr(true)))

	// One shard drains; the other still has a ready endpoint.
	s.onSlice(slice("web-a", "prod", "web", "svc-1", boolPtr(false)))
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("service still has ready endpoints in web-b; fired %v", got)
	}
	// The second shard drains too → now the SERVICE is empty.
	s.onSlice(slice("web-b", "prod", "web", "svc-1", boolPtr(false)))
	if got := col.kinds(); len(got) != 1 || got[0] != KindEndpointsEmpty {
		t.Fatalf("signals = %v, want one endpoints_empty", got)
	}
}

func TestEndpoints_SliceDeletion(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{})

	// Deleting one shard while another remains, taking ready → 0: fires.
	s.onSlice(slice("web-a", "prod", "web", "svc-1", boolPtr(true)))
	s.onSlice(slice("web-b", "prod", "web", "svc-1", boolPtr(false)))
	s.onSliceDelete(slice("web-a", "prod", "web", "svc-1", boolPtr(true)))
	if got := col.kinds(); len(got) != 1 || got[0] != KindEndpointsEmpty {
		t.Fatalf("shard deletion draining the service: signals = %v, want one endpoints_empty", got)
	}

	// Deleting the LAST slice is Service teardown, not an outage.
	s.onSlice(slice("gone-a", "prod", "gone", "svc-9", boolPtr(true)))
	s.onSliceDelete(slice("gone-a", "prod", "gone", "svc-9", boolPtr(true)))
	if got := col.kinds(); len(got) != 1 {
		t.Errorf("last-slice deletion fired: %v", got)
	}
}

func TestEndpoints_NilReadyCountsAsReady(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{})
	// discovery/v1: nil Ready must be interpreted as ready.
	s.onSlice(slice("web-a", "prod", "web", "svc-1", nil))
	s.onSlice(slice("web-a", "prod", "web", "svc-1", boolPtr(false)))
	if got := col.kinds(); len(got) != 1 || got[0] != KindEndpointsEmpty {
		t.Errorf("nil-ready → false transition: signals = %v, want one endpoints_empty", got)
	}
}

func pdb(uid, name string, allowed, expected, healthy int32) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Namespace: "prod", Name: name},
		Status: policyv1.PodDisruptionBudgetStatus{
			DisruptionsAllowed: allowed,
			ExpectedPods:       expected,
			CurrentHealthy:     healthy,
			DesiredHealthy:     healthy,
		},
	}
}

func TestPDB_TransitionToZero(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{})

	s.onPDB(pdb("p1", "web-pdb", 1, 3, 3))
	s.onPDB(pdb("p1", "web-pdb", 0, 3, 2))

	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %v, want exactly one pdb_gridlocked", col.kinds())
	}
	sig := sigs[0]
	if sig.Kind != KindPDBGridlocked || sig.Severity != engine.SeverityWarning {
		t.Errorf("got kind %q severity %q, want %q/warning", sig.Kind, sig.Severity, KindPDBGridlocked)
	}
	if sig.Key.Reason != "pdb_gridlocked" || sig.KindOfObject != "PodDisruptionBudget" {
		t.Errorf("identity wrong: %+v", sig.TriageEvent)
	}
}

func TestPDB_NoTransitionNoSignal(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{})

	// Created at 0: not a transition (this is delta's scan territory).
	s.onPDB(pdb("p1", "born-zero", 0, 3, 3))
	s.onPDB(pdb("p1", "born-zero", 0, 3, 3))

	// Transition to 0 with no pods behind the selector: blocks no one.
	s.onPDB(pdb("p2", "empty-selector", 1, 0, 0))
	s.onPDB(pdb("p2", "empty-selector", 0, 0, 0))

	// Recovery 0→1: the tracker's business, not a signal.
	s.onPDB(pdb("p1", "born-zero", 1, 3, 3))

	if got := col.kinds(); len(got) != 0 {
		t.Errorf("signals = %v, want none", got)
	}
}

func pod(uid, name string, restarts int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Namespace: "prod", Name: name},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: restarts}},
		},
	}
}

func TestPod_RestartBurst(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{RestartBurstThreshold: 3, RestartBurstWindow: 10 * time.Minute})

	base := *clock
	at := func(d time.Duration, restarts int32) {
		*clock = base.Add(d)
		s.onPod(pod("u1", "web-abc", restarts))
	}
	at(0, 0)             // baseline
	at(2*time.Minute, 1) // +1
	at(4*time.Minute, 2) // +1
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("fired below threshold: %v", got)
	}
	at(6*time.Minute, 3) // +1 → 3 within 10m
	sigs := col.all()
	if len(sigs) != 1 || sigs[0].Kind != KindRestartBurst {
		t.Fatalf("signals = %v, want one restart_burst", col.kinds())
	}
	sig := sigs[0]
	if sig.Severity != engine.SeverityWarning || sig.Key.Reason != "restart_burst" {
		t.Errorf("severity/reason = %q/%q, want warning/restart_burst", sig.Severity, sig.Key.Reason)
	}
	if sig.Key.UID != "u1" || sig.KindOfObject != "Pod" || sig.Node != "node-1" {
		t.Errorf("identity wrong: %+v", sig.TriageEvent)
	}
	// The bump memory reset: one more restart isn't a fresh burst.
	at(7*time.Minute, 4)
	if got := col.kinds(); len(got) != 1 {
		t.Errorf("re-fired immediately after reset: %v", got)
	}
}

func TestPod_RestartGrowthOutsideWindowDoesNotFire(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{RestartBurstThreshold: 3, RestartBurstWindow: 10 * time.Minute})

	base := *clock
	at := func(d time.Duration, restarts int32) {
		*clock = base.Add(d)
		s.onPod(pod("u1", "web-abc", restarts))
	}
	at(0, 0)
	at(15*time.Minute, 1)
	at(30*time.Minute, 2)
	at(45*time.Minute, 3) // 3 restarts total, but never 3 within any 10m
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("slow restart growth fired: %v", got)
	}
}

func TestPod_FirstObservationWithHighRestartsIsBaseline(t *testing.T) {
	t.Parallel()
	s, col, _ := newTestSource(t, Config{})
	// A pod first seen with restarts=50 is history, not observed
	// growth (initial LIST discipline).
	s.onPod(pod("u1", "web-abc", 50))
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("baseline observation fired: %v", got)
	}
}

// TestArmAfterSync runs the REAL informer path: fixtures seeded before
// Run must not fire (the initial LIST rebuilds memory silently); a
// live transition after sync must fire.
func TestArmAfterSync(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset(
		node("n1", "node-1", corev1.ConditionFalse),            // already NotReady
		pod("u1", "crashy", 42),                                // already restart-heavy
		pdb("p1", "web-pdb", 0, 3, 2),                          // already gridlocked
		slice("web-a", "prod", "web", "svc-1", boolPtr(false)), // already empty
	)
	s := New(client, Config{TickInterval: 10 * time.Millisecond})
	col := &collector{}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, col.emit) }()

	// Wait for arming (sync completed).
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
	// Give the ticker a few sweeps to prove the seeded state stays
	// quiet.
	time.Sleep(50 * time.Millisecond)
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("initial LIST fired transition signals: %v", got)
	}

	// A live transition post-arm fires. (The fake node was seeded
	// NotReady; flip it Ready then NotReady to make a real True→False
	// transition.)
	if _, err := client.CoreV1().Nodes().Update(ctx, node("n1", "node-1", corev1.ConditionTrue), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update node: %v", err)
	}
	waitForKinds(t, col, nil) // settle: no signal expected for False→True
	if _, err := client.CoreV1().Nodes().Update(ctx, node("n1", "node-1", corev1.ConditionFalse), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update node: %v", err)
	}
	waitForKinds(t, col, []string{KindNodeNotReady})

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

// waitForKinds polls until the collector holds exactly want (nil =
// allow informer delivery to settle without asserting emptiness).
func waitForKinds(t *testing.T, col *collector, want []string) {
	t.Helper()
	if want == nil {
		time.Sleep(50 * time.Millisecond)
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fmt.Sprint(col.kinds()) == fmt.Sprint(want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("signals = %v, want %v", col.kinds(), want)
}

// TestClearanceObserver_BackedBySourceInformer proves the absorbed pod
// observer works through the source's own informer: a Ready pod seeded
// before Run judges as cleared once synced.
func TestClearanceObserver_BackedBySourceInformer(t *testing.T) {
	t.Parallel()
	readyPod := pod("u-ready", "web-abc", 0)
	readyPod.Status.Conditions = []corev1.PodCondition{{
		Type:               corev1.PodReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Minute)),
	}}
	client := fake.NewSimpleClientset(readyPod)
	s := New(client, Config{TickInterval: time.Hour})

	inc := engine.Incident{
		Key: engine.EventKey{UID: "u-ready", Reason: "CrashLoopBackOff"},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Pod", Name: "web-abc"},
	}

	// Before Run: the observer declines to judge (not synced).
	if _, ok := s.ClearanceObserver().Clearance(inc); ok {
		t.Fatal("observer judged before its informer synced")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = s.Run(ctx, func(engine.Signal) {}) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if verdict, ok := s.ClearanceObserver().Clearance(inc); ok {
			if !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
				t.Fatalf("verdict = %+v, want cleared/recovered", verdict)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("observer never became able to judge")
}

func TestStateTTL_PrunesStaleEntries(t *testing.T) {
	t.Parallel()
	s, _, clock := newTestSource(t, Config{StateTTL: time.Hour})

	s.onNode(node("n1", "node-1", corev1.ConditionTrue))
	s.onPDB(pdb("p1", "web-pdb", 1, 3, 3))
	s.onPod(pod("u1", "web-abc", 0))
	s.onSlice(slice("web-a", "prod", "web", "svc-1", boolPtr(true)))

	*clock = clock.Add(2 * time.Hour)
	s.sweep(*clock)

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.nodes)+len(s.pdbs)+len(s.restarts)+len(s.services) != 0 {
		t.Errorf("TTL sweep left state behind: nodes=%d pdbs=%d restarts=%d services=%d",
			len(s.nodes), len(s.pdbs), len(s.restarts), len(s.services))
	}
}

func TestRequiredAccess_CoversEveryWatch(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), Config{})
	if s.Scope() != sources.ScopeCluster {
		t.Errorf("Scope = %v, want cluster (§11: nodes need cluster RBAC)", s.Scope())
	}
	got := make(map[string]bool)
	for _, r := range s.RequiredAccess() {
		got[r.Group+"/"+r.Resource+"/"+r.Verb] = true
		if r.Namespace != "" {
			t.Errorf("requirement %v must be cluster-wide", r)
		}
	}
	for _, want := range []string{
		"/pods/list", "/pods/watch",
		"/nodes/list", "/nodes/watch",
		"apps/deployments/list", "apps/deployments/watch",
		"discovery.k8s.io/endpointslices/list", "discovery.k8s.io/endpointslices/watch",
		"policy/poddisruptionbudgets/list", "policy/poddisruptionbudgets/watch",
	} {
		if !got[want] {
			t.Errorf("RequiredAccess missing %q (the §11 probe would not catch its absence)", want)
		}
	}
}

// denyReviewer denies one requirement, allows the rest.
type denyReviewer struct{ deny sources.Requirement }

func (r denyReviewer) Allowed(_ context.Context, req sources.Requirement) (bool, error) {
	return req != r.deny, nil
}

func TestProbe_FailsLoudlyForObjectState(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), Config{})
	deny := sources.Requirement{Resource: "nodes", Verb: "watch"}
	err := sources.Probe(context.Background(), denyReviewer{deny: deny}, s)
	if err == nil {
		t.Fatal("Probe must fail when a declared permission is missing (§11)")
	}
	for _, want := range []string{Name, "nodes", "watch"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("probe error %q must name %q", err, want)
		}
	}
}

// TestKindInventoryFrozen pins the kind strings and their default
// severities: kinds are append-only schema (playbooks and fleet
// consumers match on
// them), and the reason is always the kind suffix.
func TestKindInventoryFrozen(t *testing.T) {
	t.Parallel()
	want := map[string]engine.Severity{
		"objectstate.node_notready":     engine.SeverityCritical,
		"objectstate.node_flapping":     engine.SeverityWarning,
		"objectstate.progress_deadline": engine.SeverityWarning,
		"objectstate.endpoints_empty":   engine.SeverityCritical,
		"objectstate.pdb_gridlocked":    engine.SeverityWarning,
		"objectstate.restart_burst":     engine.SeverityWarning,
	}
	if len(kindSeverity) != len(want) {
		t.Errorf("kind inventory changed size: got %d kinds, want %d (append-only — update this pin deliberately)", len(kindSeverity), len(want))
	}
	for kind, sev := range want {
		if got, ok := kindSeverity[kind]; !ok || got != sev {
			t.Errorf("kind %q: severity %q (present=%v), want %q", kind, got, ok, sev)
		}
		if !strings.HasPrefix(kind, kindPrefix) {
			t.Errorf("kind %q must carry the source prefix %q", kind, kindPrefix)
		}
		if reasonOf(kind) == kind || strings.Contains(reasonOf(kind), ".") {
			t.Errorf("reasonOf(%q) = %q, want the bare kind suffix", kind, reasonOf(kind))
		}
	}
}

// TestWithFactory_SharedInformerSet proves the §6.3 shared-factory
// path: the source runs its informers on an externally owned factory
// (as the sentinel wires when storm correlation shares one informer
// set between sources and the graph) with behavior identical to the
// private-factory default — arm-after-sync silence, then live
// transitions fire.
func TestWithFactory_SharedInformerSet(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset(node("n1", "node-1", corev1.ConditionTrue))
	factory := informers.NewSharedInformerFactory(client, 0)
	s := New(client, Config{TickInterval: 10 * time.Millisecond})
	s.WithFactory(factory)
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
	if _, err := client.CoreV1().Nodes().Update(ctx, node("n1", "node-1", corev1.ConditionFalse), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update node: %v", err)
	}
	waitForKinds(t, col, []string{KindNodeNotReady})

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
