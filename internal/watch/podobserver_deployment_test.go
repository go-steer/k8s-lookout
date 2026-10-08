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
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	appsv1informers "k8s.io/client-go/informers/apps/v1"
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

// rev stamps rs with the Deployment controller's revision annotation —
// what picks the current ReplicaSet, the only other one whose Ready
// pods may vouch for a gone pod's incident.
func rev(rs *appsv1.ReplicaSet, r string) *appsv1.ReplicaSet {
	rs.Annotations = map[string]string{"deployment.kubernetes.io/revision": r}
	return rs
}

// created stamps rs's creationTimestamp — the fallback that picks the
// current ReplicaSet when revisions cannot (#600).
func created(rs *appsv1.ReplicaSet, at time.Time) *appsv1.ReplicaSet {
	rs.CreationTimestamp = metav1.NewTime(at)
	return rs
}

// ownedBy re-points rs's controller ownerReference at the Deployment
// incarnation with UID uid.
func ownedBy(rs *appsv1.ReplicaSet, uid string) *appsv1.ReplicaSet {
	rs.OwnerReferences[0].UID = types.UID(uid)
	return rs
}

// liveDeployment is Deployment name in "ns" with UID uid.
func liveDeployment(name, uid string) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID(uid)}}
}

// startRSObserver starts the observer on a shared factory over objs,
// with the ReplicaSet owners lookup installed, and waits for both the
// pod and the ReplicaSet caches.
func startRSObserver(t *testing.T, objs ...runtime.Object) *podClearanceObserver {
	t.Helper()
	obs, _ := startRSObserverClient(t, objs...)
	return obs
}

// startRSObserverClient is startRSObserver that also hands back the
// fake clientset, for the live-path tests that delete a pod.
func startRSObserverClient(t *testing.T, objs ...runtime.Object) (*podClearanceObserver, *fake.Clientset) {
	t.Helper()
	return startRSObserverWith(t, false, objs...)
}

// startRSObserverWith is the shared body; withDeployments also backs
// the lookup with the Deployment cache (UID matching, #601) — without
// it ReplicaSets match their Deployment by name, the degraded mode a
// sentinel without the deployments grant runs in.
func startRSObserverWith(t *testing.T, withDeployments bool, objs ...runtime.Object) (*podClearanceObserver, *fake.Clientset) {
	t.Helper()
	client := fake.NewClientset(objs...)
	factory := informers.NewSharedInformerFactory(client, 0)
	obs := newPodClearanceObserver(client, factory)
	var deployments appsv1informers.DeploymentInformer
	if withDeployments {
		deployments = factory.Apps().V1().Deployments()
	}
	owners := objectstate.NewReplicaSetOwners(factory.Apps().V1().ReplicaSets(), deployments)
	obs.state.SetReplicaSetOwners(owners)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := obs.Start(ctx); err != nil {
		t.Fatalf("observer Start: %v", err)
	}
	waitFor(t, "ReplicaSet cache sync", owners.HasSynced)
	return obs, client
}

// (a) The old ReplicaSet's crashed pod is gone; the Deployment's new
// ReplicaSet has a Ready pod. Recovered through the Deployment — not
// object_deleted, which is what the direct-controller lookup said.
func TestPodObserver_RestoredDeploymentRefReadyReplacement(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	obs := startRSObserver(t,
		rev(webRS("web-old", "apps/v1", "web"), "1"),
		rev(webRS("web-new", "apps/v1", "web"), "2"),
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
		rev(webRS("web-new", "apps/v1", "web"), "2"),
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
		rev(webRS("web-old", "apps/v1", "web"), "1"),
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
		rev(webRS("web-old", "apps/v1", "web"), "1"),
		rev(webRS("web-new", "apps/v1", "web"), "2"),
		podFixture{uid: "u-same", namespace: "ns", name: "web-old-eeee", owner: "ReplicaSet/web-old", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
		podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)
	verdict, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "ReplicaSet/web-old"))
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Errorf("ReplicaSet/web-old with a Ready pod under it: want cleared/recovered, got (%+v, %v)", verdict, ok)
	}

	obs = startRSObserver(t,
		rev(webRS("web-old", "apps/v1", "web"), "1"),
		rev(webRS("web-new", "apps/v1", "web"), "2"),
		podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)
	verdict, ok = obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "ReplicaSet/web-old"))
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("ReplicaSet/web-old with pods only under web-new: want cleared/object_deleted (direct controller, as before), got (%+v, %v)", verdict, ok)
	}
}

// The live path: the sentinel sees the old ReplicaSet's pod deleted
// (a tombstone naming ReplicaSet web-old), and the rollout's Ready
// replacement sits under web-new. The tombstone's ReplicaSet is
// walked to its Deployment, so the incident recovers — it used to
// read as object_deleted because web-old has no pods left.
func TestPodObserver_LiveRolloutDeletedPodReadyReplacement(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	obs, client := startRSObserverClient(t,
		rev(webRS("web-old", "apps/v1", "web"), "1"),
		rev(webRS("web-new", "apps/v1", "web"), "2"),
		podFixture{uid: "u-old", namespace: "ns", name: "web-old-aaaa", owner: "ReplicaSet/web-old"}.build(),
		podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)
	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-old-aaaa", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "delete to reach the observer", func() bool { return !obs.state.HasLive("u-old") })

	// Both controller_ref shapes the incident can carry: the
	// Deployment (owner resolution on) and none at all (off) — the
	// tombstone's ReplicaSet is enough either way.
	for _, ref := range []string{"Deployment/web", ""} {
		verdict, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", ref))
		if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
			t.Errorf("ref %q: deleted old-ReplicaSet pod with a Ready pod under the new one: want cleared/recovered, got (%+v, %v)", ref, verdict, ok)
		}
		if !verdict.StableSince.Equal(readyAt) {
			t.Errorf("ref %q: StableSince = %v, want the replacement's %v", ref, verdict.StableSince, readyAt)
		}
	}
}

// The old ReplicaSet is already gone from the cache when its pod's
// incident is judged: the incident's Deployment controller_ref (from
// the ownerReferences when the event arrived) still finds the new
// ReplicaSet's Ready pod.
func TestPodObserver_LiveRolloutOldReplicaSetGoneUsesControllerRef(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	obs, client := startRSObserverClient(t,
		rev(webRS("web-new", "apps/v1", "web"), "2"),
		podFixture{uid: "u-old", namespace: "ns", name: "web-old-aaaa", owner: "ReplicaSet/web-old"}.build(),
		podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)
	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-old-aaaa", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "delete to reach the observer", func() bool { return !obs.state.HasLive("u-old") })

	verdict, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "Deployment/web"))
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Errorf("want cleared/recovered via the Deployment controller_ref, got (%+v, %v)", verdict, ok)
	}
	// Without a Deployment ref nothing proves the old ReplicaSet's
	// owner: judged on web-old alone, as before.
	verdict, ok = obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", ""))
	if !ok || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("no ref, old ReplicaSet uncached: want object_deleted as before, got (%+v, %v)", verdict, ok)
	}
}

// The live no-pods case: the deleted pod's Deployment has no pods
// left under any ReplicaSet (a Ready pod of another Deployment does
// not count) — object_deleted.
func TestPodObserver_LiveDeletedPodDeploymentHasNoPods(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	obs, client := startRSObserverClient(t,
		rev(webRS("web-old", "apps/v1", "web"), "1"),
		rev(webRS("web-new", "apps/v1", "web"), "2"),
		webRS("api-1", "apps/v1", "api"),
		podFixture{uid: "u-old", namespace: "ns", name: "web-old-aaaa", owner: "ReplicaSet/web-old"}.build(),
		podFixture{uid: "u-api", namespace: "ns", name: "api-1-cccc", owner: "ReplicaSet/api-1", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	)
	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-old-aaaa", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "delete to reach the observer", func() bool { return !obs.state.HasLive("u-old") })

	verdict, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "Deployment/web"))
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("Deployment with no pods left: want cleared/object_deleted, got (%+v, %v)", verdict, ok)
	}
}

// A stuck rollout (review of #596): the old ReplicaSet's pods stay
// Ready (maxUnavailable), the new ReplicaSet's pod crash-loops, is
// deleted, and its replacement crash-loops too. The old pods prove a
// replacement exists — not that the symptom is gone. Not cleared, on
// the live path and on the restored path.
func TestPodObserver_StuckRolloutOldReadyDoesNotVouch(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-time.Hour).Truncate(time.Second)
	objs := []runtime.Object{
		rev(webRS("web-old", "apps/v1", "web"), "1"),
		rev(webRS("web-new", "apps/v1", "web"), "2"),
		podFixture{uid: "u-o1", namespace: "ns", name: "web-old-o1", owner: "ReplicaSet/web-old", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
		podFixture{uid: "u-o2", namespace: "ns", name: "web-old-o2", owner: "ReplicaSet/web-old", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
		podFixture{uid: "u-n2", namespace: "ns", name: "web-new-n2", owner: "ReplicaSet/web-new"}.build(),
	}

	// Live: n1 seen, then deleted.
	live := slices.Concat(objs, []runtime.Object{podFixture{uid: "u-n1", namespace: "ns", name: "web-new-n1", owner: "ReplicaSet/web-new"}.build()})
	obs, client := startRSObserverClient(t, live...)
	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-new-n1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "delete to reach the observer", func() bool { return !obs.state.HasLive("u-n1") })
	for _, ref := range []string{"Deployment/web", ""} {
		verdict, ok := obs.Clearance(podIncident("u-n1", "ns", "web-new-n1", ref))
		if !ok || verdict.Cleared {
			t.Errorf("live, ref %q: crash-looping new ReplicaSet beside a Ready old one: want judged + NOT cleared, got (%+v, %v)", ref, verdict, ok)
		}
	}

	// Restored: n1 never seen; the incident names the Deployment.
	obs = startRSObserver(t, objs...)
	verdict, ok := obs.Clearance(podIncident("u-n1", "ns", "web-new-n1", "Deployment/web"))
	if !ok || verdict.Cleared {
		t.Errorf("restored: crash-looping new ReplicaSet beside a Ready old one: want judged + NOT cleared, got (%+v, %v)", verdict, ok)
	}
}

// When revisions cannot pick the current ReplicaSet — one missing or
// unparseable — the newest by creationTimestamp is current (#600), so
// a Ready pod there recovers the deleted pod's incident instead of
// leaving it open forever.
func TestPodObserver_UnusableRevisionNewestCreatedVouches(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	older, newer := readyAt.Add(-time.Hour), readyAt.Add(-30*time.Minute)
	for _, revision := range []string{"", "two"} {
		newRS := created(webRS("web-new", "apps/v1", "web"), newer)
		if revision != "" {
			newRS = rev(newRS, revision)
		}
		obs, client := startRSObserverClient(t,
			created(rev(webRS("web-old", "apps/v1", "web"), "1"), older),
			newRS,
			podFixture{uid: "u-old", namespace: "ns", name: "web-old-aaaa", owner: "ReplicaSet/web-old"}.build(),
			podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
		)
		if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-old-aaaa", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete pod: %v", err)
		}
		waitFor(t, "delete to reach the observer", func() bool { return !obs.state.HasLive("u-old") })

		verdict, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "Deployment/web"))
		if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered || !verdict.StableSince.Equal(readyAt) {
			t.Errorf("revision %q: Ready pod in the newest-created ReplicaSet: want cleared/recovered since %v, got (%+v, %v)", revision, readyAt, verdict, ok)
		}
	}
}

// The same fallback, but the newest ReplicaSet is the one crash-looping
// while an older one stays Ready: the workload is still broken, so the
// incident stays open — live and restored.
func TestPodObserver_UnusableRevisionNewestCrashingStaysOpen(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-time.Hour).Truncate(time.Second)
	objs := []runtime.Object{
		created(rev(webRS("web-old", "apps/v1", "web"), "1"), readyAt.Add(-2*time.Hour)),
		created(webRS("web-new", "apps/v1", "web"), readyAt.Add(-time.Hour)), // no revision
		podFixture{uid: "u-o1", namespace: "ns", name: "web-old-o1", owner: "ReplicaSet/web-old", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
		podFixture{uid: "u-n2", namespace: "ns", name: "web-new-n2", owner: "ReplicaSet/web-new"}.build(),
	}
	live := slices.Concat(objs, []runtime.Object{podFixture{uid: "u-n1", namespace: "ns", name: "web-new-n1", owner: "ReplicaSet/web-new"}.build()})
	obs, client := startRSObserverClient(t, live...)
	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-new-n1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "delete to reach the observer", func() bool { return !obs.state.HasLive("u-n1") })
	if v, ok := obs.Clearance(podIncident("u-n1", "ns", "web-new-n1", "Deployment/web")); !ok || v.Cleared {
		t.Errorf("live: newest ReplicaSet crash-looping: want judged + NOT cleared, got (%+v, %v)", v, ok)
	}

	obs = startRSObserver(t, objs...)
	if v, ok := obs.Clearance(podIncident("u-n1", "ns", "web-new-n1", "Deployment/web")); !ok || v.Cleared {
		t.Errorf("restored: newest ReplicaSet crash-looping: want judged + NOT cleared, got (%+v, %v)", v, ok)
	}
}

// A revision tie is broken by creationTimestamp too; only a tie on
// both leaves the current ReplicaSet unknown — then nothing outside
// the pod's own ReplicaSet vouches (not cleared, not object_deleted).
func TestPodObserver_RevisionTieByCreation(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	cases := []struct {
		name           string
		oldAt, newAt   time.Time
		wantCleared    bool
		wantResolution engine.Resolution
	}{
		{"newer breaks the tie", readyAt.Add(-time.Hour), readyAt.Add(-30 * time.Minute), true, engine.ResolutionRecovered},
		{"both tie", readyAt.Add(-time.Hour), readyAt.Add(-time.Hour), false, engine.ResolutionRecovered},
	}
	for _, tc := range cases {
		obs, client := startRSObserverClient(t,
			created(rev(webRS("web-old", "apps/v1", "web"), "2"), tc.oldAt),
			created(rev(webRS("web-new", "apps/v1", "web"), "2"), tc.newAt),
			podFixture{uid: "u-old", namespace: "ns", name: "web-old-aaaa", owner: "ReplicaSet/web-old"}.build(),
			podFixture{uid: "u-new", namespace: "ns", name: "web-new-bbbb", owner: "ReplicaSet/web-new", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
		)
		if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-old-aaaa", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete pod: %v", err)
		}
		waitFor(t, "delete to reach the observer", func() bool { return !obs.state.HasLive("u-old") })

		v, ok := obs.Clearance(podIncident("u-old", "ns", "web-old-aaaa", "Deployment/web"))
		if !ok || v.Cleared != tc.wantCleared || v.Resolution != tc.wantResolution {
			t.Errorf("%s: want cleared=%v resolution=%s, got (%+v, %v)", tc.name, tc.wantCleared, tc.wantResolution, v, ok)
		}
	}
}

// A Deployment deleted and recreated under the same name (#601): the
// old incarnation's ReplicaSet — higher revision, Ready pod, still
// awaiting garbage collection — is not the new Deployment's. With the
// Deployment cache, ReplicaSets match by the live Deployment's UID: the
// new incarnation's ReplicaSet is current, and the old one's pods
// neither vouch nor count as the workload's.
func TestPodObserver_RecreatedDeploymentMatchesByUID(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	oldIncarnation := func() []runtime.Object {
		return []runtime.Object{
			liveDeployment("web", "dep-web-2"),
			rev(webRS("web-gen1", "apps/v1", "web"), "5"), // owner UID dep-web: the deleted incarnation
			podFixture{uid: "u-g1", namespace: "ns", name: "web-gen1-aaaa", owner: "ReplicaSet/web-gen1", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
		}
	}

	// New incarnation crash-looping: the old incarnation's Ready pod,
	// though under the higher revision, must not clear it.
	obs, _ := startRSObserverWith(t, true, slices.Concat(oldIncarnation(), []runtime.Object{
		ownedBy(rev(webRS("web-gen2", "apps/v1", "web"), "1"), "dep-web-2"),
		podFixture{uid: "u-g2", namespace: "ns", name: "web-gen2-bbbb", owner: "ReplicaSet/web-gen2"}.build(),
	})...)
	if v, ok := obs.Clearance(podIncident("u-gone", "ns", "web-gen2-cccc", "Deployment/web")); !ok || v.Cleared {
		t.Errorf("new incarnation crash-looping beside the old one's Ready pod: want judged + NOT cleared, got (%+v, %v)", v, ok)
	}

	// New incarnation Ready: its ReplicaSet is current and vouches.
	obs, _ = startRSObserverWith(t, true, slices.Concat(oldIncarnation(), []runtime.Object{
		ownedBy(rev(webRS("web-gen2", "apps/v1", "web"), "1"), "dep-web-2"),
		podFixture{uid: "u-g2", namespace: "ns", name: "web-gen2-bbbb", owner: "ReplicaSet/web-gen2", ready: true, readyAt: readyAt, startedAt: readyAt}.build(),
	})...)
	if v, ok := obs.Clearance(podIncident("u-gone", "ns", "web-gen2-cccc", "Deployment/web")); !ok || !v.Cleared || v.Resolution != engine.ResolutionRecovered {
		t.Errorf("new incarnation Ready: want cleared/recovered, got (%+v, %v)", v, ok)
	}

	// New incarnation without pods: the old incarnation's pods are not
	// this workload's, so it has none — object_deleted.
	obs, _ = startRSObserverWith(t, true, oldIncarnation()...)
	if v, ok := obs.Clearance(podIncident("u-gone", "ns", "web-gen2-cccc", "Deployment/web")); !ok || !v.Cleared || v.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("new incarnation without pods: want cleared/object_deleted, got (%+v, %v)", v, ok)
	}
}

// unsyncedOwners is a ReplicaSet cache that has not listed yet.
type unsyncedOwners struct{}

func (unsyncedOwners) HasSynced() bool                              { return false }
func (unsyncedOwners) DeploymentOf(_, _ string) (string, bool)      { return "", false }
func (unsyncedOwners) CurrentReplicaSet(_, _ string) (string, bool) { return "", false }

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
