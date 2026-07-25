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

package graph

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// assertSnapshotsEqual is the §13 deep-equality: nodes (identity,
// kind, observed, namespace link), both adjacency maps, edge count,
// interner view, and generation must all match — NodeIDs included
// (replay preserves the live interner's ID assignment by design).
func assertSnapshotsEqual(t *testing.T, got, want *Snapshot) {
	t.Helper()
	if got.generation != want.generation {
		t.Errorf("generation: got %d, want %d", got.generation, want.generation)
	}
	if !slices.Equal(got.keys, want.keys) {
		t.Errorf("interner keys diverge: got %d keys, want %d", len(got.keys), len(want.keys))
	}
	if !reflect.DeepEqual(got.nodes, want.nodes) {
		t.Errorf("nodes diverge: got %d, want %d", len(got.nodes), len(want.nodes))
		for id, info := range want.nodes {
			if g, ok := got.nodes[id]; !ok || g != info {
				t.Errorf("  node %d (%s): got %+v want %+v", id, want.keys[id-1], got.nodes[id], info)
			}
		}
		for id := range got.nodes {
			if _, ok := want.nodes[id]; !ok {
				t.Errorf("  extra node %d (%s)", id, got.keys[id-1])
			}
		}
	}
	if !reflect.DeepEqual(got.out, want.out) {
		t.Errorf("out adjacency diverges")
	}
	if !reflect.DeepEqual(got.in, want.in) {
		t.Errorf("in adjacency diverges")
	}
	if got.edges != want.edges {
		t.Errorf("edge count: got %d, want %d", got.edges, want.edges)
	}
}

// historyGraph builds a graph with the delta log armed and a fake
// clock, returning the collector slice pointer.
func historyGraph(t *testing.T) (*Graph, *[]ChangeRecord, *time.Time) {
	t.Helper()
	now := time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)
	recs := &[]ChangeRecord{}
	clock := &now
	g := New(Options{
		SwapInterval: -1,
		OnChange:     func(r ChangeRecord) { *recs = append(*recs, r) },
		Now:          func() time.Time { return *clock },
	})
	return g, recs, clock
}

// TestHistoryRoundTrip is the §13 invariant: snapshot + replayed
// deltas ≡ live graph at the same generation. Synthetic cluster,
// live churn, snapshot cut mid-stream, more churn, replay.
func TestHistoryRoundTrip(t *testing.T) {
	t.Parallel()
	g, recs, _ := historyGraph(t)
	w := g.Writer()
	objs := synthCluster(11, 400)
	if err := w.FromObjects(slices.Values(objs)); err != nil {
		t.Fatal(err)
	}
	if len(*recs) != 0 {
		t.Fatalf("FromObjects must not emit change records, got %d", len(*recs))
	}

	// Phase 1 churn (pre-snapshot).
	pods := podsOf(objs)
	churn := func(i int) {
		pod := pods[i%len(pods)].DeepCopy()
		pod.Spec.Containers[0].Image = fmt.Sprintf("registry/app:v%d", i)
		mustApply(t, w, Delta{Op: OpUpdate, Object: pod})
		if i%3 == 0 {
			mustApply(t, w, Delta{Op: OpDelete, Object: pods[(i+7)%len(pods)]})
		}
		if i%4 == 0 {
			mustApply(t, w, Delta{Op: OpAdd, Object: newPod(pod.Namespace, fmt.Sprintf("new-%d", i), pod.Spec.NodeName)})
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 10 {
		churn(i)
	}

	mid, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	enc, err := mid.Encode()
	if err != nil {
		t.Fatal(err)
	}

	// Phase 2 churn (post-snapshot): updates, deletes, adds of brand
	// new identities, node cordon, service selector changes.
	for i := 10; i < 25; i++ {
		churn(i)
	}
	cordoned := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-0000", Labels: map[string]string{corev1.LabelTopologyZone: "zone-a"}},
		Spec:       corev1.NodeSpec{Unschedulable: true},
	}
	mustApply(t, w, Delta{Op: OpUpdate, Object: cordoned})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	live, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	base, err := Restore(enc)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotsEqual(t, base, mid)

	r := NewReplayer(base)
	replayed := 0
	for _, rec := range *recs {
		if rec.Generation <= mid.Generation() {
			continue
		}
		if err := r.Apply(rec.Generation, rec.Effect); err != nil {
			t.Fatalf("replay generation %d: %v", rec.Generation, err)
		}
		replayed++
	}
	if replayed == 0 {
		t.Fatal("no post-snapshot records to replay — test is vacuous")
	}
	got := r.Snapshot()
	assertSnapshotsEqual(t, got, live)

	// The replayed snapshot must answer queries, not just compare
	// equal: resolve the cordoned node through the public API.
	id, ok := got.Lookup(KindNode, "", "node-0000")
	if !ok {
		t.Fatal("replayed snapshot cannot Lookup node-0000")
	}
	if ref, _ := got.Resolve(id); ref.Name != "node-0000" || !ref.Observed {
		t.Fatalf("replayed Resolve: %+v", ref)
	}
}

// TestReplayFromEmptyLog: replaying zero records reproduces the base
// snapshot exactly (the --at boundary where t equals the snapshot's
// own time).
func TestReplayFromEmptyLog(t *testing.T) {
	t.Parallel()
	g, _, _ := historyGraph(t)
	if err := g.Writer().FromObjects(slices.Values(synthCluster(3, 50))); err != nil {
		t.Fatal(err)
	}
	snap, _ := g.Snapshot()
	enc, err := snap.Encode()
	if err != nil {
		t.Fatal(err)
	}
	base, err := Restore(enc)
	if err != nil {
		t.Fatal(err)
	}
	got := NewReplayer(base).Snapshot()
	assertSnapshotsEqual(t, got, snap)
}

// TestEncodeRestore_RejectsCorruption is the serialization
// "fuzz-lite": header damage, unknown versions, truncations, and
// scattered bit flips must all return ErrBadSnapshot-class errors —
// never a panic, never a silently wrong graph.
func TestEncodeRestore_RejectsCorruption(t *testing.T) {
	t.Parallel()
	g, _, _ := historyGraph(t)
	if err := g.Writer().FromObjects(slices.Values(synthCluster(5, 120))); err != nil {
		t.Fatal(err)
	}
	snap, _ := g.Snapshot()
	enc, err := snap.Encode()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Restore(nil); err == nil {
		t.Error("Restore(nil) must fail")
	}
	if _, err := Restore([]byte("LKGX")); err == nil {
		t.Error("short/wrong magic must fail")
	}
	badMagic := slices.Clone(enc)
	badMagic[0] = 'X'
	if _, err := Restore(badMagic); err == nil {
		t.Error("corrupt magic must fail")
	}
	badVersion := slices.Clone(enc)
	badVersion[4] = 99
	if _, err := Restore(badVersion); err == nil {
		t.Error("unknown format version must fail")
	}
	for _, cut := range []int{5, 6, len(enc) / 2, len(enc) - 1} {
		if _, err := Restore(enc[:cut]); err == nil {
			t.Errorf("truncation at %d must fail", cut)
		}
	}
	// Bit flips: gzip's CRC or the structural validation must catch
	// every one of these (never panic; wrong-but-accepted would be a
	// silent lie).
	for i := 5; i < len(enc); i += max(1, len(enc)/64) {
		flipped := slices.Clone(enc)
		flipped[i] ^= 0x40
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("bit flip at %d: Restore panicked: %v", i, p)
				}
			}()
			if s, err := Restore(flipped); err == nil {
				// Accepting a flipped body is only tolerable if the
				// result still deep-equals the original (the flip may
				// land in gzip padding).
				if !reflect.DeepEqual(s.nodes, snap.nodes) || !reflect.DeepEqual(s.out, snap.out) {
					t.Errorf("bit flip at %d: accepted a corrupted snapshot", i)
				}
			}
		}()
	}
}

// TestReplayer_RejectsCorruptEffects mirrors fuzz-lite for the
// effect blobs.
func TestReplayer_RejectsCorruptEffects(t *testing.T) {
	t.Parallel()
	g, recs, _ := historyGraph(t)
	w := g.Writer()
	if err := w.FromObjects(slices.Values([]any{testNodeObj("n1")})); err != nil {
		t.Fatal(err)
	}
	snap, _ := g.Snapshot()
	enc, _ := snap.Encode()
	mustApply(t, w, Delta{Op: OpAdd, Object: newPod("ns1", "p1", "n1")})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(*recs) != 1 || len((*recs)[0].Effect) == 0 {
		t.Fatalf("want one record with an effect, got %+v", *recs)
	}
	effect := (*recs)[0].Effect

	base, err := Restore(enc)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewReplayer(base).Apply(2, []byte{99, 1}); err == nil {
		t.Error("unknown effect version must fail")
	}
	for cut := 1; cut < len(effect); cut++ {
		base, _ := Restore(enc)
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("effect truncated at %d: Apply panicked: %v", cut, p)
				}
			}()
			_ = NewReplayer(base).Apply(2, effect[:cut])
		}()
	}
}

// TestChangeRecord_ImageAndLabelChanges: an image bump and a label
// edit produce the documented paths; label VALUES never appear (only
// short hashes), and only changed keys are named.
func TestChangeRecord_ImageAndLabelChanges(t *testing.T) {
	t.Parallel()
	g, recs, _ := historyGraph(t)
	w := g.Writer()
	pod := newPod("shop", "pay-1", "n1")
	pod.Labels = map[string]string{"app": "pay", "tier": "backend"}
	pod.Spec.Containers[0].Image = "registry/pay:v1"
	if err := w.FromObjects(slices.Values([]any{pod})); err != nil {
		t.Fatal(err)
	}

	upd := pod.DeepCopy()
	upd.Spec.Containers[0].Image = "registry/pay:v2"
	upd.Labels["tier"] = "frontend" // changed
	// "app" unchanged — must not appear.
	mustApply(t, w, Delta{Op: OpUpdate, Object: upd})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(*recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(*recs))
	}
	rec := (*recs)[0]
	if rec.Op != OpUpdate || rec.Kind != KindPod || rec.Namespace != "shop" || rec.Name != "pay-1" {
		t.Fatalf("record identity: %+v", rec)
	}
	byPath := fieldsByPath(rec.FieldChanges)
	img, ok := byPath["container/app/image"]
	if !ok || img.From != "registry/pay:v1" || img.To != "registry/pay:v2" {
		t.Errorf("image change: %+v (all: %+v)", img, rec.FieldChanges)
	}
	lbl, ok := byPath["label/tier"]
	if !ok {
		t.Fatalf("missing label/tier change: %+v", rec.FieldChanges)
	}
	if lbl.From == "" || lbl.To == "" || lbl.From == lbl.To {
		t.Errorf("label change must carry distinct value hashes: %+v", lbl)
	}
	for _, fc := range rec.FieldChanges {
		if fc.Path == "label/app" {
			t.Errorf("unchanged label key leaked into the summary: %+v", fc)
		}
		for _, v := range []string{"backend", "frontend"} {
			if fc.From == v || fc.To == v {
				t.Errorf("label VALUE %q leaked (hashes only): %+v", v, fc)
			}
		}
	}
}

// TestChangeRecord_ReplicasAndSchedulability covers the workload and
// node paths.
func TestChangeRecord_ReplicasAndSchedulability(t *testing.T) {
	t.Parallel()
	g, recs, _ := historyGraph(t)
	w := g.Writer()
	three := int32(3)
	five := int32(5)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "pay", UID: "uid-dep"},
		Spec:       appsv1.DeploymentSpec{Replicas: &three},
	}
	node := testNodeObj("n1")
	if err := w.FromObjects(slices.Values([]any{dep, node})); err != nil {
		t.Fatal(err)
	}

	upd := dep.DeepCopy()
	upd.Spec.Replicas = &five
	mustApply(t, w, Delta{Op: OpUpdate, Object: upd})
	cordoned := node.DeepCopy()
	cordoned.Spec.Unschedulable = true
	mustApply(t, w, Delta{Op: OpUpdate, Object: cordoned})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(*recs) != 2 {
		t.Fatalf("want 2 records, got %d", len(*recs))
	}
	repl := fieldsByPath((*recs)[0].FieldChanges)["replicas"]
	if repl.From != "3" || repl.To != "5" {
		t.Errorf("replicas change: %+v", (*recs)[0].FieldChanges)
	}
	if (*recs)[0].UID != "uid-dep" {
		t.Errorf("UID not carried: %+v", (*recs)[0])
	}
	sched := fieldsByPath((*recs)[1].FieldChanges)["unschedulable"]
	if sched.From != "false" || sched.To != "true" {
		t.Errorf("unschedulable change: %+v", (*recs)[1].FieldChanges)
	}
}

// TestChangeRecord_MountRefChanges: swapping a mounted ConfigMap
// shows up as a remove + add of named references.
func TestChangeRecord_MountRefChanges(t *testing.T) {
	t.Parallel()
	g, recs, _ := historyGraph(t)
	w := g.Writer()
	pod := newPod("shop", "pay-1", "n1")
	pod.Spec.Volumes = []corev1.Volume{{
		Name: "cfg",
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "cm-old"},
		}},
	}}
	if err := w.FromObjects(slices.Values([]any{pod})); err != nil {
		t.Fatal(err)
	}
	upd := pod.DeepCopy()
	upd.Spec.Volumes[0].ConfigMap.Name = "cm-new"
	mustApply(t, w, Delta{Op: OpUpdate, Object: upd})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	byPath := fieldsByPath((*recs)[0].FieldChanges)
	if fc := byPath["mount/ConfigMap/cm-old"]; fc.From != "mounted" || fc.To != "" {
		t.Errorf("removed mount: %+v", (*recs)[0].FieldChanges)
	}
	if fc := byPath["mount/ConfigMap/cm-new"]; fc.From != "" || fc.To != "mounted" {
		t.Errorf("added mount: %+v", (*recs)[0].FieldChanges)
	}
}

// TestChangeRecord_ContentHashes: ConfigMap/Secret updates through
// the ingest produce content-hash changes — and for Secrets the
// VALUE must never appear anywhere in the record (§6.5: the hash is
// the graph's entire contact with secret material). This path is
// dormant in the shipped sentinel (its informers do not watch
// ConfigMaps/Secrets) but must work the moment they flow through
// Apply.
func TestChangeRecord_ContentHashes(t *testing.T) {
	t.Parallel()
	g, recs, _ := historyGraph(t)
	w := g.Writer()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "cm-app"},
		Data:       map[string]string{"key": "v1"},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "db-credentials"},
		Data:       map[string][]byte{"token": []byte(secretCanary)},
	}
	if err := w.FromObjects(slices.Values([]any{cm, sec})); err != nil {
		t.Fatal(err)
	}

	cm2 := cm.DeepCopy()
	cm2.Data["key"] = "v2"
	sec2 := sec.DeepCopy()
	sec2.Data["token"] = []byte(secretCanary + "-rotated")
	mustApply(t, w, Delta{Op: OpUpdate, Object: cm2}, Delta{Op: OpUpdate, Object: sec2})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(*recs) != 2 {
		t.Fatalf("want 2 records, got %d", len(*recs))
	}
	for _, rec := range *recs {
		fc, ok := fieldsByPath(rec.FieldChanges)["data"]
		if !ok || fc.From == "" || fc.To == "" || fc.From == fc.To || len(fc.To) != 16 {
			t.Errorf("%s: content-hash change malformed: %+v", rec.Name, rec.FieldChanges)
		}
		blob, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(blob), secretCanary) {
			t.Errorf("secret VALUE leaked into change record: %s", blob)
		}
	}

	// An update with identical content is not a change.
	mustApply(t, w, Delta{Op: OpUpdate, Object: sec2.DeepCopy()})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	last := (*recs)[len(*recs)-1]
	if len(last.FieldChanges) != 0 {
		t.Errorf("no-op update produced field changes: %+v", last.FieldChanges)
	}
}

// TestChangeRecord_AddDeleteShape: adds and deletes carry identity +
// op with no field summary, and deletes clear tracked state (a
// re-add then update diffs against the NEW baseline).
func TestChangeRecord_AddDeleteShape(t *testing.T) {
	t.Parallel()
	g, recs, clock := historyGraph(t)
	w := g.Writer()
	if err := w.FromObjects(slices.Values([]any{testNodeObj("n1")})); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(time.Minute)
	pod := newPod("shop", "pay-1", "n1")
	pod.Spec.Containers[0].Image = "img:v1"
	mustApply(t, w, Delta{Op: OpAdd, Object: pod})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	mustApply(t, w, Delta{Op: OpDelete, Object: pod})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	pod2 := pod.DeepCopy()
	pod2.Spec.Containers[0].Image = "img:v9"
	mustApply(t, w, Delta{Op: OpAdd, Object: pod2})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(*recs) != 3 {
		t.Fatalf("want 3 records, got %d", len(*recs))
	}
	add, del, readd := (*recs)[0], (*recs)[1], (*recs)[2]
	if add.Op != OpAdd || len(add.FieldChanges) != 0 || !add.At.Equal(*clock) {
		t.Errorf("add record: %+v", add)
	}
	if del.Op != OpDelete || len(del.FieldChanges) != 0 {
		t.Errorf("delete record: %+v", del)
	}
	// Tracked state was dropped on delete: the re-add is a fresh
	// baseline, not an update against img:v1.
	if readd.Op != OpAdd || len(readd.FieldChanges) != 0 {
		t.Errorf("re-add record after delete must have no diff: %+v", readd)
	}
	// Generations are the swap counters: strictly increasing across
	// the three flushes.
	if add.Generation >= del.Generation || del.Generation >= readd.Generation {
		t.Errorf("generations not increasing: %d %d %d", add.Generation, del.Generation, readd.Generation)
	}
}

// TestOpString pins the persisted op spellings (store wire values).
func TestOpString(t *testing.T) {
	t.Parallel()
	for op, want := range map[Op]string{OpAdd: "add", OpUpdate: "update", OpDelete: "delete", opInvalid: "invalid"} {
		if got := op.String(); got != want {
			t.Errorf("Op(%d).String() = %q, want %q", op, got, want)
		}
	}
}

// --- helpers ---

func mustApply(t *testing.T, w *Writer, deltas ...Delta) {
	t.Helper()
	if err := w.Apply(deltas...); err != nil {
		t.Fatal(err)
	}
}

func fieldsByPath(fcs []FieldChange) map[string]FieldChange {
	out := map[string]FieldChange{}
	for _, fc := range fcs {
		out[fc.Path] = fc
	}
	return out
}

func podsOf(objs []any) []*corev1.Pod {
	var pods []*corev1.Pod
	for _, o := range objs {
		if p, ok := o.(*corev1.Pod); ok {
			pods = append(pods, p)
		}
	}
	return pods
}

func newPod(ns, name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("uid-" + name)},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "app", Image: "img:latest"}},
		},
	}
}

func testNodeObj(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{corev1.LabelTopologyZone: "zone-a"}},
	}
}
