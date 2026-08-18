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

package triage

// The store-backed tests seed a REAL SQLite store through the
// production graph writer + change log (§6.6), never hand-inserted
// rows: what `lookout watch` would have written is what `triage
// changes` reads.

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// changesT0 anchors the seeded history; fixedNow (10:30) is 30
// minutes after it, so the default window is exactly (t0, now].
var changesT0 = time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)

func webPodV(image string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "web-1",
			Labels:          labels,
			OwnerReferences: ownedBy("ReplicaSet", "web-rs"),
		},
		Spec: corev1.PodSpec{
			NodeName:   "n1",
			Containers: []corev1.Container{{Name: "app", Image: image}},
			Volumes: []corev1.Volume{{
				Name:         "cfg",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm-app"}}},
			}},
		},
		Status: corev1.PodStatus{Conditions: podReady(true)},
	}
}

func webRS(replicas int32) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-rs", OwnerReferences: ownedBy("Deployment", "web")},
		Spec:       appsv1.ReplicaSetSpec{Replicas: &replicas},
	}
}

func webCM(value string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "cm-app"},
		Data:       map[string]string{"flag": value},
	}
}

func bystander(image string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "bystander"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: image}}},
	}
}

// seedChangeLog writes the delta log an incident post-mortem reads:
// baseline snapshot at t0, then image bump (10:05), ConfigMap edit
// (10:10), ReplicaSet rescale (10:15), pod label flip (10:20), a
// change in an unrelated namespace (10:25 — must be filtered out by
// neighborhood scoping), and a second image bump at 10:40 (outside a
// window ending 10:30).
func seedChangeLog(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lookout.db")
	cur := changesT0
	clock := func() time.Time { return cur }

	st, err := store.Open(path, store.WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New(graph.Options{SwapInterval: -1, OnChange: st.RecordGraphChange, Now: clock})
	w := g.Writer()
	replicas := int32(2)
	if err := w.FromObjects(slices.Values([]any{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		},
		webRS(2),
		webPodV("img:v1", map[string]string{"app": "web"}),
		webCM("v1"),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}},
		bystander("img:v1"),
	})); err != nil {
		t.Fatal(err)
	}
	snap, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutGraphSnapshot(context.Background(), snap); err != nil {
		t.Fatal(err)
	}

	step := func(minutes int, obj any) {
		cur = changesT0.Add(time.Duration(minutes) * time.Minute)
		if err := w.Apply(graph.Delta{Op: graph.OpUpdate, Object: obj}); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	step(5, webPodV("img:v2", map[string]string{"app": "web"}))
	step(10, webCM("v2"))
	step(15, webRS(3))
	step(20, webPodV("img:v2", map[string]string{"app": "web", "track": "blue"}))
	step(25, bystander("img:v2"))
	step(40, webPodV("img:v3", map[string]string{"app": "web", "track": "blue"}))
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestChanges_AtGolden is the incident-onset question end to end:
// --at + --store answers entirely from history (no cluster access),
// chronological, neighborhood-scoped, with hashes instead of values.
func TestChanges_AtGolden(t *testing.T) {
	path := seedChangeLog(t)
	res := checktest.Run(t, ChangesCommand(noClusterDeps(t)),
		"Deployment/prod/web", "--at=2026-07-25T10:30:00Z", "--store="+path)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/changes-at.golden", res.Stdout)

	for _, want := range []string{
		"kind=change.rollout",
		"kind=change.config",
		"kind=change.scale",
		"kind=change.label",
		"relation=upstream",   // the ReplicaSet rescale
		"relation=downstream", // the ConfigMap edit
		"source=history",
		"at=2026-07-25T10:30:00Z",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
		}
	}
	if strings.Contains(res.Stdout, "bystander") {
		t.Errorf("unrelated-namespace change leaked past neighborhood scoping:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "img:v3") {
		t.Errorf("change after --at leaked into the window:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "flag") || strings.Contains(res.Stdout, "=v1→v2") {
		t.Errorf("ConfigMap content leaked — only hashes may appear:\n%s", res.Stdout)
	}
}

// TestChanges_AtWindowSemantics: the window is (at-since, at] — a
// change stamped exactly at --at is in; anything later is not.
func TestChanges_AtWindowSemantics(t *testing.T) {
	path := seedChangeLog(t)
	res := checktest.Run(t, ChangesCommand(noClusterDeps(t)),
		"Deployment/prod/web", "--at=2026-07-25T10:15:00Z", "--store="+path)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "replicas=2→3") {
		t.Errorf("the rescale stamped exactly at --at belongs to the window:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "change.label") {
		t.Errorf("the 10:20 label flip is after --at=10:15:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "findings=3") {
		t.Errorf("want exactly the 10:05/10:10/10:15 changes:\n%s", res.Stdout)
	}
}

// liveObjects is the cluster state matching the end of the seeded
// history, for store-mode-without---at (live neighborhood, logged
// changes).
func liveObjects() []runtime.Object {
	return []runtime.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"}},
		webRS(3),
		webPodV("img:v2", map[string]string{"app": "web", "track": "blue"}),
		webCM("v2"),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}},
	}
}

func rescaleEvent(name string, at time.Time, reason, involvedKind, involvedName, msg string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: "prod", Name: name},
		InvolvedObject: corev1.ObjectReference{Kind: involvedKind, Namespace: "prod", Name: involvedName},
		Reason:         reason,
		Message:        msg,
		LastTimestamp:  metav1.Time{Time: at},
	}
}

// TestChanges_StoreLive: --store without --at — neighborhood from
// the live graph, changes from the delta log ending now, HPA
// SuccessfulRescale joined from the live event timeline (the HPA
// keeps no replica history, §5); the deployment controller's
// ScalingReplicaSet stays out (the log's replicas change says it
// better).
func TestChanges_StoreLive(t *testing.T) {
	path := seedChangeLog(t)
	objs := append(liveObjects(),
		runtime.Object(rescaleEvent("hpa-up", changesT0.Add(22*time.Minute), "SuccessfulRescale", "Deployment", "web", "New size: 3; reason: cpu resource utilization above target")),
		runtime.Object(rescaleEvent("ctrl", changesT0.Add(23*time.Minute), "ScalingReplicaSet", "Deployment", "web", "Scaled up replica set web-rs to 3")),
		runtime.Object(rescaleEvent("hpa-old", changesT0.Add(-2*time.Hour), "SuccessfulRescale", "Deployment", "web", "ancient")),
	)
	res := checktest.Run(t, ChangesCommand(fakeDeps(objs...)),
		"Deployment/prod/web", "--store="+path)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "reason=SuccessfulRescale") ||
		!strings.Contains(res.Stdout, "origin=event") {
		t.Errorf("HPA rescale event missing:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "ScalingReplicaSet") {
		t.Errorf("controller scaling event must stay out in store mode:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "ancient") {
		t.Errorf("event outside the window leaked:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "source=history") {
		t.Errorf("summary must say source=history:\n%s", res.Stdout)
	}
	// Chronology: the 10:20 label flip precedes the 10:22 rescale.
	if li, ei := strings.Index(res.Stdout, "change.label"), strings.Index(res.Stdout, "SuccessfulRescale"); li < 0 || ei < 0 || li > ei {
		t.Errorf("entries out of order (label@10:20 then rescale@10:22):\n%s", res.Stdout)
	}
	// The 10:40 image bump is after now (10:30): out.
	if strings.Contains(res.Stdout, "img:v3") {
		t.Errorf("change after now leaked:\n%s", res.Stdout)
	}
}

// liveApproxObjects is a pure-live cluster mid-rollout: the new
// ReplicaSet (revision 2) was created inside the window, the old one
// long before; the HPA and the deployment controller left scaling
// events.
func liveApproxObjects() []runtime.Object {
	rev := func(rs *appsv1.ReplicaSet, r, image string, created time.Time) *appsv1.ReplicaSet {
		rs.Annotations = map[string]string{"deployment.kubernetes.io/revision": r}
		rs.CreationTimestamp = metav1.Time{Time: created}
		rs.Spec.Template = corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: image}}},
		}
		return rs
	}
	oldRS := rev(&appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-rs-1", OwnerReferences: ownedBy("Deployment", "web")},
	}, "1", "img:v1", changesT0.Add(-3*time.Hour))
	newRS := rev(&appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-rs-2", OwnerReferences: ownedBy("Deployment", "web")},
	}, "2", "img:v2", changesT0.Add(20*time.Minute))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "web-1",
			OwnerReferences: ownedBy("ReplicaSet", "web-rs-2"),
		},
		Spec:   corev1.PodSpec{NodeName: "n1", Containers: []corev1.Container{{Name: "app", Image: "img:v2"}}},
		Status: corev1.PodStatus{Conditions: podReady(true)},
	}
	return []runtime.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"}},
		oldRS, newRS, pod,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}},
		rescaleEvent("hpa-up", changesT0.Add(25*time.Minute), "SuccessfulRescale", "Deployment", "web", "New size: 3; reason: cpu resource utilization above target"),
		rescaleEvent("ctrl", changesT0.Add(22*time.Minute), "ScalingReplicaSet", "Deployment", "web", "Scaled up replica set web-rs-2 to 3"),
		rescaleEvent("stranger", changesT0.Add(26*time.Minute), "SuccessfulRescale", "Deployment", "unrelated", "not our neighborhood"),
	}
}

// TestChanges_LiveApproximationGolden: no store — the command falls
// back to what the API can still tell (ReplicaSet revisions, scaling
// events), chronological, and says source=live-approximation.
func TestChanges_LiveApproximationGolden(t *testing.T) {
	res := checktest.Run(t, ChangesCommand(fakeDeps(liveApproxObjects()...)), "Deployment/prod/web")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr %q", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/changes-live.golden", res.Stdout)

	for _, want := range []string{
		"kind=change.rollout", "name=web-rs-2", "revision=2", "image=img:v2", "origin=api",
		"reason=ScalingReplicaSet", "reason=SuccessfulRescale",
		"source=live-approximation",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
		}
	}
	if strings.Contains(res.Stdout, "web-rs-1") {
		t.Errorf("pre-window ReplicaSet leaked in:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "unrelated") {
		t.Errorf("event outside the neighborhood leaked in:\n%s", res.Stdout)
	}
	// Chronological: rollout (10:20) → controller scale (10:22) →
	// HPA rescale (10:25).
	ri := strings.Index(res.Stdout, "change.rollout")
	ci := strings.Index(res.Stdout, "ScalingReplicaSet")
	hi := strings.Index(res.Stdout, "SuccessfulRescale")
	if ri < 0 || ci <= ri || hi <= ci {
		t.Errorf("entries not chronological:\n%s", res.Stdout)
	}
}

// TestChanges_UsageAndFailures.
func TestChanges_UsageAndFailures(t *testing.T) {
	cmd := ChangesCommand(fakeDeps(liveApproxObjects()...))
	for name, args := range map[string][]string{
		"no target":        {},
		"zero depth":       {"Deployment/prod/web", "--depth=0"},
		"at without store": {"Deployment/prod/web", "--at=20m"},
	} {
		res := checktest.Run(t, cmd, args...)
		if res.Code != emit.ExitUsage {
			t.Errorf("%s: exit %d, want %d (stderr %q)", name, res.Code, emit.ExitUsage, res.Stderr)
		}
	}
	// A dangling --store path is a runtime failure, stdout clean.
	res := checktest.Run(t, cmd, "Deployment/prod/web", "--store="+filepath.Join(t.TempDir(), "absent.db"))
	if res.Code != emit.ExitRuntime || res.Stdout != "" {
		t.Errorf("absent store: exit %d stdout %q", res.Code, res.Stdout)
	}
}

// TestChanges_Contract: §13 round-trip, live approximation and
// history modes.
func TestChanges_Contract(t *testing.T) {
	checktest.VerifyContract(t, ChangesCommand(fakeDeps(liveApproxObjects()...)), "Deployment/prod/web")
	path := seedChangeLog(t)
	checktest.VerifyContract(t, ChangesCommand(Deps{Now: func() time.Time { return fixedNow }}),
		"Deployment/prod/web", "--at=2026-07-25T10:30:00Z", "--store="+path)
}

// TestChangesRegistered: default-registry presence + MCP name.
func TestChangesRegistered(t *testing.T) {
	c, ok := checks.Lookup("triage changes")
	if !ok {
		t.Fatal("triage changes is not registered")
	}
	if c.MCPName != "k8s_recent_changes" {
		t.Errorf("MCP name = %q, want k8s_recent_changes", c.MCPName)
	}
	if !c.GraphBacked {
		t.Error("triage changes must be graph-backed (--at/--store)")
	}
}
