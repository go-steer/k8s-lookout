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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
)

// Restored incidents whose ControllerRef names a Deployment (#583):
// the k8s-events source reports a ReplicaSet pod's Deployment, while
// the clearance indexes live pods by their ReplicaSet. These run the
// pod observer the way setupRecovery wires it — shared factory, the
// ReplicaSet owners lookup on the same factory — with the crashed pod
// never seen (the sentinel restarted after it was deleted).

// webRS is ReplicaSet name in "ns", controlled by a Deployment of the
// given apiVersion (example.com/v1 makes it a CRD's, not apps').
func webRS(name, apiVersion, deployment string) *appsv1.ReplicaSet {
	yes := true
	return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: name, UID: types.UID("rs-" + name),
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: apiVersion, Kind: "Deployment", Name: deployment,
			UID: types.UID("dep-" + deployment), Controller: &yes,
		}},
	}}
}

// startRSObserver starts the observer on a shared factory over objs,
// with the ReplicaSet owners lookup installed, and waits for both the
// pod and the ReplicaSet caches.
func startRSObserver(t *testing.T, objs ...runtime.Object) *podClearanceObserver {
	t.Helper()
	client := fake.NewClientset(objs...)
	factory := informers.NewSharedInformerFactory(client, 0)
	obs := newPodClearanceObserver(client, factory)
	owners := objectstate.NewReplicaSetOwners(factory.Apps().V1().ReplicaSets())
	obs.state.SetReplicaSetOwners(owners)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := obs.Start(ctx); err != nil {
		t.Fatalf("observer Start: %v", err)
	}
	waitFor(t, "ReplicaSet cache sync", owners.HasSynced)
	return obs
}

// (a) The old ReplicaSet's crashed pod is gone; the Deployment's new
// ReplicaSet has a Ready pod. Recovered through the Deployment — not
// object_deleted, which is what the direct-controller lookup said.
func TestPodObserver_RestoredDeploymentRefReadyReplacement(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	obs := startRSObserver(t,
		webRS("web-old", "apps/v1", "web"),
		webRS("web-new", "apps/v1", "web"),
		podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)

	verdict, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "Deployment/web"))
	if !ok {
		t.Fatal("observer must judge a restored Deployment-owned incident")
	}
	if !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Errorf("Deployment/web with a Ready pod under its new ReplicaSet: want cleared/recovered, got %+v", verdict)
	}
	if !verdict.StableSince.Equal(readyAt) {
		t.Errorf("StableSince = %v, want the replacement's %v", verdict.StableSince, readyAt)
	}
}

// A crash-looping replacement under the new ReplicaSet is the symptom
// persisting: judged, not cleared.
func TestPodObserver_RestoredDeploymentRefCrashingReplacement(t *testing.T) {
	t.Parallel()
	obs := startRSObserver(t,
		webRS("web-new", "apps/v1", "web"),
		podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new"}.build(),
	)
	verdict, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "Deployment/web"))
	if !ok || verdict.Cleared {
		t.Errorf("crash-looping replacement: want judged + not cleared, got (%+v, %v)", verdict, ok)
	}
}

// (b) No pods under the Deployment at all — and a Ready pod under
// another Deployment's ReplicaSet, or under a CRD that calls itself
// Deployment, does not count: object_deleted, as before.
func TestPodObserver_RestoredDeploymentRefNoPods(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	obs := startRSObserver(t,
		webRS("web-old", "apps/v1", "web"),
		webRS("api-1", "apps/v1", "api"),
		webRS("web-crd", "example.com/v1", "web"),
		podFixture{uid: "u-api", namespace: "ns", name: "api-1-cccc", owner: "ReplicaSet/api-1", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
		podFixture{uid: "u-crd", namespace: "ns", name: "web-crd-dddd", owner: "ReplicaSet/web-crd", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)

	verdict, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "Deployment/web"))
	if !ok {
		t.Fatal("observer must judge")
	}
	if !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("Deployment/web with no pods: want cleared/object_deleted, got %+v", verdict)
	}
}

// (c) A ReplicaSet ControllerRef keeps its direct-controller meaning:
// a Ready pod under the SAME ReplicaSet clears it, one under a
// sibling ReplicaSet of the same Deployment does not.
func TestPodObserver_RestoredReplicaSetRefUnchanged(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	obs := startRSObserver(t,
		webRS("web-old", "apps/v1", "web"),
		webRS("web-new", "apps/v1", "web"),
		podFixture{uid: "u-same", namespace: "ns", name: "web-old-eeee", owner: "ReplicaSet/web-old", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
		podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)
	verdict, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "ReplicaSet/web-old"))
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Errorf("ReplicaSet/web-old with a Ready pod under it: want cleared/recovered, got (%+v, %v)", verdict, ok)
	}

	obs = startRSObserver(t,
		webRS("web-old", "apps/v1", "web"),
		webRS("web-new", "apps/v1", "web"),
		podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)
	verdict, ok = obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "ReplicaSet/web-old"))
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("ReplicaSet/web-old with pods only under web-new: want cleared/object_deleted (direct controller, as before), got (%+v, %v)", verdict, ok)
	}
}

// unsyncedOwners is a ReplicaSet cache that has not listed yet.
type unsyncedOwners struct{}

func (unsyncedOwners) HasSynced() bool                         { return false }
func (unsyncedOwners) DeploymentOf(_, _ string) (string, bool) { return "", false }

// An unsynced ReplicaSet cache must not turn "not listed yet" into
// "workload gone": Deployment-owned incidents wait. Empty and
// ReplicaSet refs are judged as before.
func TestPodObserver_DeploymentRefWaitsForReplicaSetSync(t *testing.T) {
	t.Parallel()
	obs, _ := startObserver(t)
	obs.state.SetReplicaSetOwners(unsyncedOwners{})
	if _, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "Deployment/web")); ok {
		t.Error("Deployment ref with an unsynced ReplicaSet cache: want ok=false (cannot judge yet)")
	}
	for _, ref := range []string{"", "ReplicaSet/web-old"} {
		v, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", ref))
		if !ok || v.Resolution != engine.ResolutionObjectDeleted {
			t.Errorf("ref %q: want judged object_deleted as before, got (%+v, %v)", ref, v, ok)
		}
	}
}

// Without the ReplicaSet lookup (replicasets denied), a Deployment ref
// is judged exactly as before #583's follow-up: object_deleted.
func TestPodObserver_DeploymentRefWithoutReplicaSetOwners(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	obs, _ := startObserver(t,
		podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)
	v, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "Deployment/web"))
	if !ok || v.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("no ReplicaSet lookup: want object_deleted as before, got (%+v, %v)", v, ok)
	}
}

var _ objectstate.ReplicaSetOwners = unsyncedOwners{}
