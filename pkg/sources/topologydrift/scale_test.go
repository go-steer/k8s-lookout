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

package topologydrift

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

func want32(n int32) *int32 { return &n }

func deploy(ns, name string, want *int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: want},
	}
}

func stateful(ns, name string, want *int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.StatefulSetSpec{Replicas: want},
	}
}

func deploySubject(ns, name string) leeway.SubjectRef {
	return leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: ns, Name: name}
}

// A workload we are meeting for the first time has not been *seen* to change.
// Stamping now instead would relax every subject in the cluster for a whole
// settle window after every restart, which is the failure mode §7.6 calls
// worse than under-reporting.
func TestScaleLog_AFirstSightingIsNotAScaleEvent(t *testing.T) {
	l := newScaleLog()
	l.Observe(deploySubject("prod", "web"), 3, t0)

	if got := l.ScaledAt(deploySubject("prod", "web")); !got.IsZero() {
		t.Errorf("ScaledAt after one sighting = %v, want the zero time", got)
	}
}

func TestScaleLog_AChangedReplicaCountIsStamped(t *testing.T) {
	l := newScaleLog()
	sub := deploySubject("prod", "web")
	l.Observe(sub, 3, t0)
	l.Observe(sub, 9, t0.Add(time.Minute))

	if got := l.ScaledAt(sub); !got.Equal(t0.Add(time.Minute)) {
		t.Errorf("ScaledAt = %v, want the moment of the change", got)
	}
}

// The informer relists every object periodically and re-delivers every update
// that touched anything at all. Re-stamping on each would turn the resync into
// a permanent scale event and suppress the whole cluster forever.
func TestScaleLog_ReSeeingTheSameSizeDoesNotReStamp(t *testing.T) {
	l := newScaleLog()
	sub := deploySubject("prod", "web")
	l.Observe(sub, 3, t0)
	l.Observe(sub, 9, t0.Add(time.Minute))
	l.Observe(sub, 9, t0.Add(time.Hour))
	l.Observe(sub, 9, t0.Add(2*time.Hour))

	if got := l.ScaledAt(sub); !got.Equal(t0.Add(time.Minute)) {
		t.Errorf("ScaledAt after two resyncs = %v, want the original change", got)
	}
}

func TestScaleLog_ScalingBackDownIsAlsoAChange(t *testing.T) {
	l := newScaleLog()
	sub := deploySubject("prod", "web")
	l.Observe(sub, 3, t0)
	l.Observe(sub, 9, t0.Add(time.Minute))
	l.Observe(sub, 3, t0.Add(2*time.Minute))

	if got := l.ScaledAt(sub); !got.Equal(t0.Add(2 * time.Minute)) {
		t.Errorf("ScaledAt after scaling back = %v, want the second change", got)
	}
}

// A name that comes back is a different object, and its replica count was
// never compared against the old one's. Inheriting the old entry would make a
// recreate at a different size look like a scale event that never happened.
func TestScaleLog_AForgottenWorkloadStartsOver(t *testing.T) {
	l := newScaleLog()
	sub := deploySubject("prod", "web")
	l.Observe(sub, 3, t0)
	l.Observe(sub, 9, t0.Add(time.Minute))
	l.Forget(sub)
	l.Observe(sub, 20, t0.Add(2*time.Minute))

	if got := l.ScaledAt(sub); !got.IsZero() {
		t.Errorf("ScaledAt after a recreate = %v, want the zero time of a first sighting", got)
	}
	if l.Len() != 1 {
		t.Errorf("Len() = %d, want 1", l.Len())
	}
}

// A DaemonSet, a Job, or a Deployment in a cluster where the apps informers
// never started. §7.6 reads this as "not recently scaled" rather than as an
// error, because the rows relax and an absent answer costs a widened threshold
// rather than a spurious finding.
func TestScaleLog_AnUnknownSubjectReadsZero(t *testing.T) {
	l := newScaleLog()
	sub := leeway.SubjectRef{Kind: leeway.SubjectDaemonSet, Namespace: "kube-system", Name: "agent"}
	if got := l.ScaledAt(sub); !got.IsZero() {
		t.Errorf("ScaledAt of an unknown subject = %v, want the zero time", got)
	}
	if l.Len() != 0 {
		t.Errorf("a read created an entry: Len() = %d", l.Len())
	}
}

func TestSource_TheScaleHandlersRecordBothKinds(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})

	s.onScalable(deploy("prod", "web", want32(3)))
	s.onScalable(deploy("prod", "web", want32(9)))
	s.onScalable(stateful("prod", "db", want32(3)))
	s.onScalable(stateful("prod", "db", want32(5)))

	dep := deploySubject("prod", "web")
	sts := leeway.SubjectRef{Kind: leeway.SubjectStatefulSet, Namespace: "prod", Name: "db"}
	if s.scales.ScaledAt(dep).IsZero() {
		t.Error("the Deployment's scale event was not recorded")
	}
	if s.scales.ScaledAt(sts).IsZero() {
		t.Error("the StatefulSet's scale event was not recorded")
	}
}

// An unset spec.replicas is one. Read as zero, every default-sized Deployment
// would look as though it had scaled the first time somebody set the field
// explicitly to 1.
func TestSource_AnUnsetReplicaCountIsOneNotZero(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.onScalable(deploy("prod", "web", nil))
	s.onScalable(deploy("prod", "web", want32(1)))

	if got := s.scales.ScaledAt(deploySubject("prod", "web")); !got.IsZero() {
		t.Errorf("ScaledAt = %v, want the zero time — nil and 1 are the same size", got)
	}
}

func TestSource_TheScaleHandlersIgnoreWhatTheyCannotSize(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.onScalable(&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "agent"}})
	s.onScalable("not an object at all")
	s.onScalableDelete(&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "agent"}})
	s.onScalableDelete(42)

	if s.scales.Len() != 0 {
		t.Errorf("Len() = %d, want 0 — an unsizable object is not a scale entry", s.scales.Len())
	}
}

func TestSource_ADeletedWorkloadIsForgotten(t *testing.T) {
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	s.onScalable(deploy("prod", "web", want32(3)))
	s.onScalable(stateful("prod", "db", want32(3)))
	if s.scales.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", s.scales.Len())
	}

	s.onScalableDelete(deploy("prod", "web", want32(3)))
	// The informer hands over a tombstone when it missed the delete itself.
	s.onScalableDelete(cache.DeletedFinalStateUnknown{
		Key: "prod/db", Obj: stateful("prod", "db", want32(3)),
	})

	if s.scales.Len() != 0 {
		t.Errorf("Len() = %d, want 0 — both deletes should have landed", s.scales.Len())
	}
}

func TestSource_ARecentScaleRelaxesRatherThanSuppresses(t *testing.T) {
	cfg := Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}
	s := armedSource(cfg, node("n-a", "us-central1-a"), node("n-b", "us-central1-b"))
	sub := deploySubject("prod", "web")
	s.scales.Observe(sub, 3, t0)
	s.scales.Observe(sub, 9, t0)

	got := s.suppression(sub, t0.Add(time.Minute))(zoneKey, evenlyEligible("us-central1-a", "us-central1-b"))
	if got.State != leeway.TransientScale || got.Suppress || got.Multiplier <= 1 {
		t.Fatalf("suppression a minute after a scale = %+v, want a relaxing recent-scale", got)
	}

	// Past the settle window it is just a workload again.
	late := t0.Add(s.cfg.Transient.ScaleSettleWindow + time.Minute)
	if got := s.suppression(sub, late)(zoneKey, evenlyEligible("us-central1-a", "us-central1-b")); got.State != leeway.TransientNone {
		t.Errorf("suppression after the window = %+v, want no transient", got)
	}
}

// The scale row is per subject, unlike warmup, outage and drain. A cluster
// where one workload was resized must not stop judging the rest.
func TestSource_AScaleEventReachesOnlyTheWorkloadThatScaled(t *testing.T) {
	cfg := Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}
	s := armedSource(cfg, node("n-a", "us-central1-a"), node("n-b", "us-central1-b"))
	scaled, quiet := deploySubject("prod", "web"), deploySubject("prod", "api")
	s.scales.Observe(scaled, 3, t0)
	s.scales.Observe(scaled, 9, t0)
	s.scales.Observe(quiet, 3, t0)

	eligible := evenlyEligible("us-central1-a", "us-central1-b")
	if got := s.suppression(scaled, t0)(zoneKey, eligible); got.State != leeway.TransientScale {
		t.Errorf("the scaled subject = %+v, want recent-scale", got)
	}
	if got := s.suppression(quiet, t0)(zoneKey, eligible); got.State != leeway.TransientNone {
		t.Errorf("the untouched subject = %+v, want no transient", got)
	}
}

// End to end through the real informers, because everything above drives the
// handlers by hand and would keep passing if Run stopped registering them. The
// apps watches are also the two new LIST+WATCH streams this adds, so a change
// that quietly drops them should fail something.
func TestSource_RunWatchesTheDeclaredSizeOfBothKinds(t *testing.T) {
	client := fake.NewSimpleClientset(
		node("n-a", "us-central1-a"),
		deploy("prod", "web", want32(3)),
		stateful("prod", "db", want32(3)),
	)
	s := New(client, Config{TopologyKeys: []leeway.TopologyKey{zoneKey}, CoalesceWindow: tickWindow})
	s.logf = func(string, ...any) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx, func(sources.Signal) {}) }()
	waitFor(t, "the source to sync", s.HasSynced)

	web := deploySubject("prod", "web")
	db := leeway.SubjectRef{Kind: leeway.SubjectStatefulSet, Namespace: "prod", Name: "db"}
	// Both were on file before Run started, so both are first sightings: the
	// initial LIST must not read as a cluster-wide scale event.
	waitFor(t, "both workloads to be on file", func() bool { return s.scales.Len() == 2 })
	if !s.scales.ScaledAt(web).IsZero() || !s.scales.ScaledAt(db).IsZero() {
		t.Fatal("the initial list stamped a scale event; a restart would relax the whole cluster")
	}

	if _, err := client.AppsV1().Deployments("prod").Update(ctx, deploy("prod", "web", want32(9)), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("scale the Deployment: %v", err)
	}
	if _, err := client.AppsV1().StatefulSets("prod").Update(ctx, stateful("prod", "db", want32(5)), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("scale the StatefulSet: %v", err)
	}
	waitFor(t, "the Deployment's scale to be seen", func() bool { return !s.scales.ScaledAt(web).IsZero() })
	waitFor(t, "the StatefulSet's scale to be seen", func() bool { return !s.scales.ScaledAt(db).IsZero() })

	if err := client.AppsV1().Deployments("prod").Delete(ctx, "web", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete the Deployment: %v", err)
	}
	waitFor(t, "the deleted Deployment to be forgotten", func() bool { return s.scales.Len() == 1 })
}

func TestSource_TheRolloutOracleRelaxesTheSubjectsItNames(t *testing.T) {
	cfg := Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}
	s := armedSource(cfg, node("n-a", "us-central1-a"), node("n-b", "us-central1-b"))
	rolling, quiet := deploySubject("prod", "web"), deploySubject("prod", "api")
	s.WithRolloutOracle(func() []leeway.SubjectRef { return []leeway.SubjectRef{rolling} })
	s.sampleCluster(t0)

	eligible := evenlyEligible("us-central1-a", "us-central1-b")
	got := s.suppression(rolling, t0)(zoneKey, eligible)
	if got.State != leeway.TransientRollout || got.Suppress || got.Multiplier <= 1 {
		t.Fatalf("the rolling subject = %+v, want a relaxing rollout", got)
	}
	if got := s.suppression(quiet, t0)(zoneKey, eligible); got.State != leeway.TransientNone {
		t.Errorf("the settled subject = %+v, want no transient", got)
	}

	// And it un-relaxes: the oracle is re-read on every cluster sample, not
	// latched. A rollout that never ended for leeway would be worse than one
	// it never noticed.
	s.WithRolloutOracle(func() []leeway.SubjectRef { return nil })
	s.sampleCluster(t0.Add(time.Minute))
	if got := s.suppression(rolling, t0.Add(time.Minute))(zoneKey, eligible); got.State != leeway.TransientNone {
		t.Errorf("after the rollout finished = %+v, want no transient", got)
	}
}

// No rollout source in --sources, or a sample that has not run yet. The row is
// unanswered, which reads as false: an un-relaxed threshold, never a finding
// that should not exist.
func TestSource_AnUnwiredOracleLeavesTheRowUnanswered(t *testing.T) {
	cfg := Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}
	s := armedSource(cfg, node("n-a", "us-central1-a"), node("n-b", "us-central1-b"))
	sub := deploySubject("prod", "web")

	// sampleCluster still has to do its other half with no oracle set.
	s.sampleCluster(t0)
	if got := s.inv.ReadyHistory(zoneKey, "us-central1-a"); len(got) != 1 {
		t.Errorf("ready history = %v, want the sample to have been taken anyway", got)
	}
	if got := s.suppression(sub, t0)(zoneKey, evenlyEligible("us-central1-a", "us-central1-b")); got.State != leeway.TransientNone {
		t.Errorf("suppression with no oracle = %+v, want no transient", got)
	}
}
