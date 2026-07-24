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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// splitControllerRef parses the payload-shaped "Kind/name" controller
// ref for fixtures (the production copy lives with the shared state
// machine in pkg/sources/objectstate).
func splitControllerRef(s string) (kind, name string, ok bool) {
	i := strings.IndexByte(s, '/')
	if i <= 0 || i == len(s)-1 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

// podFixture builds a pod for the observer tests.
type podFixture struct {
	uid       string
	namespace string
	name      string
	owner     string // "Kind/name", "" for bare pods
	ready     bool
	readyAt   time.Time
	restarts  int32
	startedAt time.Time
}

func (f podFixture) build() *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(f.uid),
			Namespace: f.namespace,
			Name:      f.name,
		},
	}
	if f.owner != "" {
		kind, name, _ := splitControllerRef(f.owner)
		ctrl := true
		pod.OwnerReferences = []metav1.OwnerReference{{Kind: kind, Name: name, Controller: &ctrl}}
	}
	status := corev1.ConditionFalse
	if f.ready {
		status = corev1.ConditionTrue
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:               corev1.PodReady,
		Status:             status,
		LastTransitionTime: metav1.NewTime(f.readyAt),
	}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "app",
		RestartCount: f.restarts,
		State: corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(f.startedAt)},
		},
	}}
	return pod
}

// startObserver spins the observer against a fake clientset seeded
// with the given pods and waits for the initial sync.
func startObserver(t *testing.T, pods ...*corev1.Pod) (*podClearanceObserver, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset()
	for _, p := range pods {
		if _, err := client.CoreV1().Pods(p.Namespace).Create(context.Background(), p, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed pod %s: %v", p.Name, err)
		}
	}
	obs := newPodClearanceObserver(client)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := obs.Start(ctx); err != nil {
		t.Fatalf("observer Start: %v", err)
	}
	return obs, client
}

// waitFor polls cond until true or the deadline expires — informer
// delivery from the fake clientset is asynchronous.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func podIncident(uid, ns, name, controllerRef string) engine.Incident {
	return engine.Incident{
		Key:       engine.EventKey{UID: uid, Reason: "CrashLoopBackOff"},
		SessionID: "sess-1",
		FirstSeen: time.Now().Add(-10 * time.Minute),
		Ref: engine.IncidentRef{
			Namespace:     ns,
			KindOfObject:  "Pod",
			Name:          name,
			ControllerRef: controllerRef,
		},
	}
}

func TestPodObserver_LivePodReadiness(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	obs, client := startObserver(t,
		podFixture{uid: "u-ready", namespace: "ns", name: "pod-ready", ready: true, readyAt: readyAt, startedAt: readyAt.Add(-time.Minute)}.build(),
		podFixture{uid: "u-notready", namespace: "ns", name: "pod-notready", ready: false}.build(),
	)

	verdict, ok := obs.Clearance(podIncident("u-ready", "ns", "pod-ready", ""))
	if !ok {
		t.Fatal("observer must judge a live Pod incident")
	}
	if !verdict.Cleared {
		t.Error("Ready pod: want Cleared")
	}
	if verdict.Resolution != engine.ResolutionRecovered {
		t.Errorf("Resolution = %q, want recovered", verdict.Resolution)
	}
	if !verdict.StableSince.Equal(readyAt) {
		t.Errorf("StableSince = %v, want the Ready transition %v", verdict.StableSince, readyAt)
	}

	verdict, ok = obs.Clearance(podIncident("u-notready", "ns", "pod-notready", ""))
	if !ok || verdict.Cleared {
		t.Errorf("not-Ready pod: want judged + not cleared, got (%+v, %v)", verdict, ok)
	}

	// A restart-count bump pushes StableSince forward even when the
	// pod reads Ready again: the stability window must restart.
	before := time.Now()
	bumped := podFixture{uid: "u-ready", namespace: "ns", name: "pod-ready", ready: true, readyAt: readyAt, restarts: 1, startedAt: readyAt.Add(-time.Minute)}.build()
	if _, err := client.CoreV1().Pods("ns").Update(context.Background(), bumped, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update pod: %v", err)
	}
	waitFor(t, "restart bump to reach the observer", func() bool {
		v, _ := obs.Clearance(podIncident("u-ready", "ns", "pod-ready", ""))
		return v.StableSince.After(before) || v.StableSince.Equal(before)
	})
	verdict, _ = obs.Clearance(podIncident("u-ready", "ns", "pod-ready", ""))
	if !verdict.Cleared {
		t.Error("pod is Ready after restart: still cleared (window restart is the tracker's job)")
	}
}

func TestPodObserver_GonePodWithReadyReplacement(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-3 * time.Minute).Truncate(time.Second)
	crashing := podFixture{uid: "u-crash", namespace: "ns", name: "web-7b9d-aaaa", owner: "ReplicaSet/web-7b9d"}.build()
	replacement := podFixture{uid: "u-repl", namespace: "ns", name: "web-7b9d-bbbb", owner: "ReplicaSet/web-7b9d", ready: true, readyAt: readyAt, startedAt: readyAt}.build()
	obs, client := startObserver(t, crashing, replacement)

	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-7b9d-aaaa", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "delete to reach the observer", func() bool {
		return !obs.state.HasLive("u-crash")
	})

	verdict, ok := obs.Clearance(podIncident("u-crash", "ns", "web-7b9d-aaaa", "ReplicaSet/web-7b9d"))
	if !ok {
		t.Fatal("observer must judge a gone pod with a tombstone")
	}
	if !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Errorf("gone pod + Ready replacement: want cleared/recovered, got %+v", verdict)
	}
	if !verdict.StableSince.Equal(readyAt) {
		t.Errorf("StableSince = %v, want the replacement's %v", verdict.StableSince, readyAt)
	}
}

func TestPodObserver_GonePodWithCrashingReplacement(t *testing.T) {
	t.Parallel()
	crashing := podFixture{uid: "u-crash", namespace: "ns", name: "web-7b9d-aaaa", owner: "ReplicaSet/web-7b9d"}.build()
	replacement := podFixture{uid: "u-repl", namespace: "ns", name: "web-7b9d-bbbb", owner: "ReplicaSet/web-7b9d", ready: false}.build()
	obs, client := startObserver(t, crashing, replacement)

	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-7b9d-aaaa", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "delete to reach the observer", func() bool {
		return !obs.state.HasLive("u-crash")
	})

	verdict, ok := obs.Clearance(podIncident("u-crash", "ns", "web-7b9d-aaaa", "ReplicaSet/web-7b9d"))
	if !ok {
		t.Fatal("observer must judge")
	}
	if verdict.Cleared {
		t.Error("a not-Ready replacement IS the symptom persisting: want not cleared")
	}
}

func TestPodObserver_GonePodOwnerGone(t *testing.T) {
	t.Parallel()
	crashing := podFixture{uid: "u-crash", namespace: "ns", name: "web-7b9d-aaaa", owner: "ReplicaSet/web-7b9d"}.build()
	obs, client := startObserver(t, crashing)

	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "web-7b9d-aaaa", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "delete to reach the observer", func() bool {
		return !obs.state.HasLive("u-crash")
	})

	verdict, ok := obs.Clearance(podIncident("u-crash", "ns", "web-7b9d-aaaa", "ReplicaSet/web-7b9d"))
	if !ok {
		t.Fatal("observer must judge")
	}
	if !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("gone pod, no pods left under owner: want cleared/object_deleted (explicitly distinct from fixed), got %+v", verdict)
	}
}

func TestPodObserver_BarePodDeleted(t *testing.T) {
	t.Parallel()
	bare := podFixture{uid: "u-bare", namespace: "ns", name: "oneoff"}.build()
	obs, client := startObserver(t, bare)

	if err := client.CoreV1().Pods("ns").Delete(context.Background(), "oneoff", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitFor(t, "delete to reach the observer", func() bool {
		return !obs.state.HasLive("u-bare")
	})

	verdict, ok := obs.Clearance(podIncident("u-bare", "ns", "oneoff", ""))
	if !ok {
		t.Fatal("observer must judge")
	}
	if !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("bare pod deleted: want cleared/object_deleted, got %+v", verdict)
	}
}

// Restart case: the observer never saw the pod alive (no tombstone),
// but a same-name replacement exists — StatefulSet pods keep their
// name across recreation.
func TestPodObserver_RestoredIncidentSameNameReplacement(t *testing.T) {
	t.Parallel()
	readyAt := time.Now().Add(-time.Minute).Truncate(time.Second)
	replacement := podFixture{uid: "u-new", namespace: "ns", name: "db-0", ready: true, readyAt: readyAt, startedAt: readyAt}.build()
	obs, _ := startObserver(t, replacement)

	// The incident references the OLD UID, unknown to the observer.
	verdict, ok := obs.Clearance(podIncident("u-old", "ns", "db-0", ""))
	if !ok {
		t.Fatal("observer must judge a restored pod incident")
	}
	if !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Errorf("same-name Ready replacement: want cleared/recovered, got %+v", verdict)
	}
}

func TestPodObserver_NotItsScope(t *testing.T) {
	t.Parallel()
	obs, _ := startObserver(t)
	inc := podIncident("u-node", "", "node-1", "")
	inc.Ref.KindOfObject = "Node"
	if _, ok := obs.Clearance(inc); ok {
		t.Error("pod observer must decline non-Pod incidents (ok=false)")
	}
}
