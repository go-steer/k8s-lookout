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

package rollout

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	clock := &now
	s.now = func() time.Time { return *clock }
	s.emit = col.emit
	s.armed = true
	return s, col, clock
}

// deployment builds a Deployment fixture mid-rollout (generation
// observed, counts explicit; complete=false unless the counts say so).
func deployment(uid, ns, name string, replicas, updated, available, total int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Namespace: ns, Name: name, Generation: 2},
		Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(replicas)},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 2,
			Replicas:           total,
			UpdatedReplicas:    updated,
			AvailableReplicas:  available,
		},
	}
}

func replicaSet(uid, ns, name, ownerUID, revision string, desired, ready int32) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(uid), Namespace: ns, Name: name,
			Annotations:     map[string]string{revisionAnnotation: revision},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "owner", UID: types.UID(ownerUID), Controller: boolPtr(true)}},
		},
		Spec:   appsv1.ReplicaSetSpec{Replicas: int32Ptr(desired)},
		Status: appsv1.ReplicaSetStatus{ReadyReplicas: ready},
	}
}

func rolloutPod(uid, ns, name, ownerUID, revisionHash string, ready bool, waiting string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(uid), Namespace: ns, Name: name,
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "owner", UID: types.UID(ownerUID), Controller: boolPtr(true)}},
		},
	}
	if revisionHash != "" {
		p.Labels = map[string]string{revisionHashLabel: revisionHash}
	}
	readyStatus := corev1.ConditionFalse
	if ready {
		readyStatus = corev1.ConditionTrue
	}
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: readyStatus}}
	if waiting != "" {
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: waiting}}}}
	}
	return p
}

func statefulSet(uid, ns, name string, replicas, ready int32, current, update string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Namespace: ns, Name: name, Generation: 2},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(replicas)},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 2,
			ReadyReplicas:      ready,
			CurrentRevision:    current,
			UpdateRevision:     update,
		},
	}
}

// seedBadDeploy loads the normative §7.2 fixture: old RS healthy
// (3/3), new RS 0/1 ready, one crash-looping new pod.
func seedBadDeploy(s *Source) {
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "1", 3, 3))
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 1, 0))
	s.onPod(rolloutPod("p-new", "prod", "web-7b9-x1", "rs-new", "", false, "CrashLoopBackOff"))
	s.onDeployment(deployment("d1", "prod", "web", 3, 1, 3, 4))
}

func TestDeployment_BadDeploy_FiresOnceWithEvidence(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	seedBadDeploy(s)
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("fired before the observe window elapsed: %v", got)
	}

	*clock = clock.Add(3 * time.Minute)
	s.send(s.sweep(*clock))
	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %v, want exactly one rollout.stall", col.kinds())
	}
	sig := sigs[0]
	if sig.Kind != KindStall {
		t.Errorf("Kind = %q, want %q", sig.Kind, KindStall)
	}
	if sig.Severity != engine.SeverityWarning {
		t.Errorf("Severity = %q, want warning", sig.Severity)
	}
	if sig.Key.UID != "d1" || sig.Key.Reason != ReasonStall {
		t.Errorf("dedup key = %+v, want uid=d1 reason=%s (workload UID, fire once per revision)", sig.Key, ReasonStall)
	}
	if sig.KindOfObject != "Deployment" || sig.Namespace != "prod" || sig.Name != "web" {
		t.Errorf("object identity wrong: %+v", sig.TriageEvent)
	}
	for _, want := range []string{"new_ready=0/1", "old_ready=3/3", "elapsed=3m0s", "top_waiting_reason=CrashLoopBackOff", "web-7b9"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("message %q missing evidence %q", sig.Message, want)
		}
	}

	// Fire once per revision: more sweeps, same stall, no repeat.
	*clock = clock.Add(10 * time.Minute)
	s.send(s.sweep(*clock))
	if got := col.kinds(); len(got) != 1 {
		t.Errorf("signals = %v, want the stall fired exactly once per revision", got)
	}
}

func TestDeployment_FiresWellBeforeProgressDeadline(t *testing.T) {
	t.Parallel()
	// The §7.2 wording is normative: with the default
	// progressDeadlineSeconds=600 the stall (default observe 3m) must
	// fire with plenty of deadline budget left.
	s, col, clock := newTestSource(t, Config{})
	seedBadDeploy(s)
	*clock = clock.Add(DefaultConfig().Observe)
	s.send(s.sweep(*clock))
	if len(col.all()) != 1 {
		t.Fatalf("signals = %v, want one stall at the default observe window", col.kinds())
	}
	if DefaultConfig().Observe >= 600*time.Second {
		t.Errorf("default observe %s is not ahead of the 600s progress deadline", DefaultConfig().Observe)
	}
}

func TestDeployment_SlowButProgressingRollout_NeverFires(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "1", 3, 3))
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 3, 0))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 6))

	// One new pod becomes ready every 2 minutes — slower than a happy
	// rollout, but progress. Each increase resets the observe window.
	for ready := int32(1); ready <= 3; ready++ {
		*clock = clock.Add(2 * time.Minute)
		s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 3, ready))
	}
	// Completion.
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))
	*clock = clock.Add(10 * time.Minute)
	s.send(s.sweep(*clock))
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("signals = %v, want none for a slow-but-progressing rollout", got)
	}
}

func TestDeployment_HealthyFastRollout_NeverFires(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "1", 3, 3))
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 3, 3))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))
	*clock = clock.Add(30 * time.Minute)
	s.send(s.sweep(*clock))
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("signals = %v, want none for a healthy rollout", got)
	}
}

func TestDeployment_OldRevisionUnhealthy_NoFire(t *testing.T) {
	t.Parallel()
	// Old revision at 1/3 ready: whatever is wrong is not "bad new
	// deploy while old healthy" — other sources own it.
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "1", 3, 1))
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 1, 0))
	s.onDeployment(deployment("d1", "prod", "web", 3, 1, 1, 4))
	*clock = clock.Add(10 * time.Minute)
	s.send(s.sweep(*clock))
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("signals = %v, want none when the old revision is unhealthy too", got)
	}
}

func TestDeployment_InitialDeploy_NoOldRevision_NoFire(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "1", 3, 0))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 0, 3))
	*clock = clock.Add(10 * time.Minute)
	s.send(s.sweep(*clock))
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("signals = %v, want none for an initial deploy (no old-healthy baseline; objectstate.progress_deadline owns it)", got)
	}
}

func TestDeployment_NotArmed_RecordsButDoesNotFire(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	s.armed = false
	seedBadDeploy(s)
	*clock = clock.Add(10 * time.Minute)
	s.send(s.sweep(*clock))
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("signals = %v, want none before arming", got)
	}
	// Arming later fires from the already-observed evidence.
	s.armed = true
	s.send(s.sweep(*clock))
	if got := col.kinds(); len(got) != 1 {
		t.Errorf("signals = %v, want one stall after arming", got)
	}
}

func TestDeployment_CompletionClears(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	seedBadDeploy(s)
	*clock = clock.Add(3 * time.Minute)
	s.send(s.sweep(*clock))
	if len(col.all()) != 1 {
		t.Fatalf("fixture did not fire: %v", col.kinds())
	}
	inc := engine.Incident{
		Key: engine.EventKey{UID: "d1", Reason: ReasonStall},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Deployment", Name: "web"},
	}
	verdict, ok := s.Clearance(inc)
	if !ok {
		t.Fatal("observer must claim rollout_stall incidents")
	}
	if verdict.Cleared {
		t.Fatal("incident cleared while the rollout is still stalled")
	}

	// The fixed revision completes.
	*clock = clock.Add(2 * time.Minute)
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 3, 3))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))
	verdict, ok = s.Clearance(inc)
	if !ok || !verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want cleared after completion", verdict, ok)
	}
	if verdict.Resolution != engine.ResolutionRecovered {
		t.Errorf("Resolution = %q, want recovered", verdict.Resolution)
	}
	if !verdict.StableSince.Equal(*clock) {
		t.Errorf("StableSince = %v, want completion time %v", verdict.StableSince, *clock)
	}
}

func TestDeployment_RollbackClears(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	seedBadDeploy(s)
	*clock = clock.Add(3 * time.Minute)
	s.send(s.sweep(*clock))
	if len(col.all()) != 1 {
		t.Fatalf("fixture did not fire: %v", col.kinds())
	}
	// Rollback: the controller re-promotes the old RS (revision bumps
	// to 3), scales the bad RS to 0, and the deployment completes
	// immediately — the old pods never went away.
	*clock = clock.Add(time.Minute)
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 0, 0))
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "3", 3, 3))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))

	inc := engine.Incident{
		Key: engine.EventKey{UID: "d1", Reason: ReasonStall},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Deployment", Name: "web"},
	}
	verdict, ok := s.Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("verdict = %+v ok=%v, want cleared/recovered after rollback", verdict, ok)
	}
	// And the rollback itself must not have fired a second stall.
	if got := col.kinds(); len(got) != 1 {
		t.Errorf("signals = %v, want no second fire on rollback", got)
	}
}

func TestDeployment_DeletedClears_ObjectDeleted(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	seedBadDeploy(s)
	*clock = clock.Add(3 * time.Minute)
	s.send(s.sweep(*clock))
	if len(col.all()) != 1 {
		t.Fatalf("fixture did not fire: %v", col.kinds())
	}
	s.onDeploymentDelete(deployment("d1", "prod", "web", 3, 1, 3, 4))
	verdict, ok := s.Clearance(engine.Incident{
		Key: engine.EventKey{UID: "d1", Reason: ReasonStall},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Deployment", Name: "web"},
	})
	if !ok || !verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want cleared", verdict, ok)
	}
	if verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("Resolution = %q, want object_deleted (a deletion is not a fix, §9.3)", verdict.Resolution)
	}
}

func TestClearance_DeclinesForeignIncidents(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	if _, ok := s.Clearance(engine.Incident{
		Key: engine.EventKey{UID: "p1", Reason: "CrashLoopBackOff"},
		Ref: engine.IncidentRef{KindOfObject: "Pod"},
	}); ok {
		t.Error("observer must not claim non-rollout incidents")
	}
	// Not armed → cannot judge even its own reason.
	s.armed = false
	if _, ok := s.Clearance(engine.Incident{
		Key: engine.EventKey{UID: "d1", Reason: ReasonStall},
		Ref: engine.IncidentRef{KindOfObject: "Deployment"},
	}); ok {
		t.Error("observer must decline before its caches sync")
	}
}

// ---- StatefulSet path ----

func seedBadSTSRollout(s *Source) {
	s.onPod(rolloutPod("p0", "prod", "db-0", "s1", "rev-a", true, ""))
	s.onPod(rolloutPod("p1", "prod", "db-1", "s1", "rev-a", true, ""))
	s.onPod(rolloutPod("p2", "prod", "db-2", "s1", "rev-b", false, "ImagePullBackOff"))
	s.onStatefulSet(statefulSet("s1", "prod", "db", 3, 2, "rev-a", "rev-b"))
}

func TestStatefulSet_BadRollout_FiresWithEvidence(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	seedBadSTSRollout(s)
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("fired before the observe window elapsed: %v", got)
	}
	*clock = clock.Add(3 * time.Minute)
	s.send(s.sweep(*clock))
	sigs := col.all()
	if len(sigs) != 1 {
		t.Fatalf("signals = %v, want exactly one rollout.stall", col.kinds())
	}
	sig := sigs[0]
	if sig.Kind != KindStall || sig.KindOfObject != "StatefulSet" || sig.Key.UID != "s1" {
		t.Errorf("signal identity wrong: kind=%q object=%q uid=%q", sig.Kind, sig.KindOfObject, sig.Key.UID)
	}
	for _, want := range []string{"rev-b", "new_ready=0/3", "old_ready=2/2", "top_waiting_reason=ImagePullBackOff"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("message %q missing evidence %q", sig.Message, want)
		}
	}
	// Once per revision.
	*clock = clock.Add(5 * time.Minute)
	s.send(s.sweep(*clock))
	if got := col.kinds(); len(got) != 1 {
		t.Errorf("signals = %v, want the stall fired once", got)
	}
}

func TestStatefulSet_CompletionClears(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	seedBadSTSRollout(s)
	*clock = clock.Add(3 * time.Minute)
	s.send(s.sweep(*clock))
	if len(col.all()) != 1 {
		t.Fatalf("fixture did not fire: %v", col.kinds())
	}
	inc := engine.Incident{
		Key: engine.EventKey{UID: "s1", Reason: ReasonStall},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "StatefulSet", Name: "db"},
	}
	if verdict, ok := s.Clearance(inc); !ok || verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want claimed + not cleared mid-stall", verdict, ok)
	}
	*clock = clock.Add(2 * time.Minute)
	s.onStatefulSet(statefulSet("s1", "prod", "db", 3, 3, "rev-b", "rev-b"))
	verdict, ok := s.Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("verdict = %+v ok=%v, want cleared/recovered after STS completion", verdict, ok)
	}
}

func TestStatefulSet_ProgressResetsWindow(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	seedBadSTSRollout(s)
	// The new-revision pod becomes ready at 2m: progress.
	*clock = clock.Add(2 * time.Minute)
	s.onPod(rolloutPod("p2", "prod", "db-2", "s1", "rev-b", true, ""))
	s.onStatefulSet(statefulSet("s1", "prod", "db", 3, 3, "rev-a", "rev-b"))
	// 2 more minutes: only 2m since progress — no fire yet.
	*clock = clock.Add(2 * time.Minute)
	s.send(s.sweep(*clock))
	if got := col.kinds(); len(got) != 0 {
		t.Errorf("signals = %v, want none while the rollout progresses", got)
	}
}

// ---- source plumbing ----

func TestRequiredAccess_DeclaresAllInformerTargets(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), Config{})
	got := make(map[string]bool)
	for _, req := range s.RequiredAccess() {
		got[req.String()] = true
	}
	for _, want := range []string{
		"list pods cluster-wide", "watch pods cluster-wide",
		"list deployments.apps cluster-wide", "watch deployments.apps cluster-wide",
		"list replicasets.apps cluster-wide", "watch replicasets.apps cluster-wide",
		"list statefulsets.apps cluster-wide", "watch statefulsets.apps cluster-wide",
	} {
		if !got[want] {
			t.Errorf("RequiredAccess missing %q (have %v)", want, got)
		}
	}
}

func TestProbe_MissingGrantNamesThisSource(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), Config{})
	_, err := sources.Probe(context.Background(), denyReviewer{}, s)
	if err == nil {
		t.Fatal("Probe must fail loudly when a grant is missing (§11)")
	}
	if !strings.Contains(err.Error(), Name) {
		t.Errorf("error %q should name the source", err)
	}
}

type denyReviewer struct{}

func (denyReviewer) Allowed(context.Context, sources.Requirement) (sources.Decision, error) {
	return sources.Decision{}, nil
}

// TestArmAfterSync runs the REAL informer path: a rollout already
// stalled at startup must stay silent through the initial LIST, then
// fire once the observe window elapses post-arm.
func TestArmAfterSync(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset(
		deployment("d1", "prod", "web", 3, 1, 3, 4),
		replicaSet("rs-old", "prod", "web-5f6", "d1", "1", 3, 3),
		replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 1, 0),
		rolloutPod("p-new", "prod", "web-7b9-x1", "rs-new", "", false, "CrashLoopBackOff"),
	)
	s := New(client, Config{Observe: 100 * time.Millisecond, TickInterval: 10 * time.Millisecond})
	col := &collector{}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, col.emit) }()

	// Wait for arming, proving nothing fired during/immediately after
	// the initial LIST.
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
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("fired before the post-arm observe window: %v", got)
	}

	// The stall persists past the observe window → exactly one fire.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(col.all()) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // a few more ticks: still once
	if got := col.kinds(); fmt.Sprint(got) != fmt.Sprint([]string{KindStall}) {
		t.Fatalf("signals = %v, want exactly [%s]", got, KindStall)
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

// TestDeployment_RollbackAfterPriorCompletion_StampsClearanceAtRollback
// is the M3 drill observation 4 regression: a Deployment that was
// COMPLETE long before the bad deploy (the steady state every
// sentinel observes for hours) must not lend that old completion
// timestamp to the incident's clearance. StableSince must be the
// sweep that observed the post-rollback completion — the §7.4 tracker
// counts the stability window from it, so anything earlier (the drill
// saw fire-time after clamping) skips the debounce and produces
// resolved records with cleared_after=0 and observed_stable_for
// counted from fire time.
func TestDeployment_RollbackAfterPriorCompletion_StampsClearanceAtRollback(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})

	// Steady state: the deployment has been complete for hours.
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "1", 3, 3))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))
	priorCompletion := *clock

	// The bad deploy, two hours later; the stall fires after the
	// observe window.
	*clock = clock.Add(2 * time.Hour)
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 1, 0))
	s.onPod(rolloutPod("p-new", "prod", "web-7b9-x1", "rs-new", "", false, "CrashLoopBackOff"))
	s.onDeployment(deployment("d1", "prod", "web", 3, 1, 3, 4))
	*clock = clock.Add(3 * time.Minute)
	s.send(s.sweep(*clock))
	if len(col.all()) != 1 {
		t.Fatalf("fixture did not fire: %v", col.kinds())
	}
	inc := engine.Incident{
		Key: engine.EventKey{UID: "d1", Reason: ReasonStall},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Deployment", Name: "web"},
	}
	if verdict, ok := s.Clearance(inc); !ok || verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want claimed + not cleared mid-stall", verdict, ok)
	}

	// Rollback 29s after the fire: bad RS scaled to 0, old RS
	// re-promoted (revision 3), deployment completes.
	*clock = clock.Add(29 * time.Second)
	rollbackObserved := *clock
	s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 0, 0))
	s.onReplicaSet(replicaSet("rs-old", "prod", "web-5f6", "d1", "3", 3, 3))
	s.onDeployment(deployment("d1", "prod", "web", 3, 3, 3, 3))

	verdict, ok := s.Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("verdict = %+v ok=%v, want cleared/recovered after rollback", verdict, ok)
	}
	if !verdict.StableSince.Equal(rollbackObserved) {
		t.Errorf("StableSince = %v, want the observed rollback completion %v — not the pre-incident completion %v (skips the stability window and zeroes cleared_after)",
			verdict.StableSince, rollbackObserved, priorCompletion)
	}

	// Later sweeps that keep observing completion must NOT move the
	// stamp forward: the stability window keeps counting from the
	// transition.
	*clock = clock.Add(45 * time.Second)
	s.send(s.sweep(*clock))
	verdict, ok = s.Clearance(inc)
	if !ok || !verdict.Cleared || !verdict.StableSince.Equal(rollbackObserved) {
		t.Errorf("StableSince drifted to %v on a repeat sweep, want stable %v (ok=%v)", verdict.StableSince, rollbackObserved, ok)
	}
}
