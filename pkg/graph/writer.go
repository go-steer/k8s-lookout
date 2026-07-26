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
	"errors"
	"fmt"
	"iter"
	"maps"
	"sort"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// Op is the delta operation, mirroring informer event types.
type Op uint8

const (
	opInvalid Op = iota
	// OpAdd — object created (or seen for the first time).
	OpAdd
	// OpUpdate — object changed. Add and Update are handled
	// identically (upsert); both spellings exist so informer
	// handlers map 1:1.
	OpUpdate
	// OpDelete — object removed. Callers wiring client-go informers
	// must unwrap cache.DeletedFinalStateUnknown tombstones and pass
	// the typed object; pkg/graph deliberately has no client-go
	// dependency.
	OpDelete
)

// Delta is the neutral ingestion unit (§6.3): one typed Kubernetes
// object plus the operation that happened to it. Object must be a
// pointer to one of the supported types — see validateObject.
type Delta struct {
	Op     Op
	Object any
}

// ErrClosed is returned by Writer methods after Close.
var ErrClosed = errors.New("graph: writer closed")

// halfEdge is a fully specified directed edge, used for writer-side
// bookkeeping of which object declared which edges.
type halfEdge struct {
	From, To NodeID
	Kind     EdgeKind
}

// selEntry is a registered label selector (Service or NetworkPolicy)
// that must be re-evaluated when pods in its namespace change.
type selEntry struct {
	sel  labels.Selector
	kind EdgeKind
}

// Writer is the single-writer ingest side of a Graph. Apply queues
// deltas; they are folded into the next snapshot and swapped in at
// most once per swap interval (Flush forces an immediate swap).
// Methods are serialized by an internal mutex, but the design is
// single-writer: exactly one ingest loop should own this.
type Writer struct {
	g        *Graph
	interval time.Duration
	onChange func(ChangeRecord)
	now      func() time.Time

	mu      sync.Mutex
	closed  bool
	pending []pendingDelta
	timer   *time.Timer

	intern *interner
	gen    uint64
	// watched is Options.WatchedKinds as a set, stamped into every
	// published snapshot. Nil means "everything watched".
	watched map[NodeKind]bool

	// Writer-private incremental-maintenance state. Never shared
	// with snapshots; mutated in place.
	declared  map[NodeID][]halfEdge            // edges declared by each object
	refcnt    map[NodeID]int                   // edges incident per node, for GC
	podLabels map[string]map[NodeID]labels.Set // namespace → pod → labels
	selectors map[string]map[NodeID]selEntry   // namespace → source → selector

	// §6.6 delta-log state, populated only when onChange is set:
	// tracked holds the per-object fingerprints FieldChanges diff
	// against (changes.go); rec is armed around one applyDelta to
	// capture its primitive mutations as the replay effect
	// (replay.go).
	tracked map[NodeID]*trackedState
	rec     *effectLog
}

func newWriter(g *Graph, interval time.Duration, onChange func(ChangeRecord), now func() time.Time) *Writer {
	w := &Writer{
		g:         g,
		interval:  interval,
		onChange:  onChange,
		now:       now,
		intern:    &interner{},
		declared:  make(map[NodeID][]halfEdge),
		refcnt:    make(map[NodeID]int),
		podLabels: make(map[string]map[NodeID]labels.Set),
		selectors: make(map[string]map[NodeID]selEntry),
	}
	if onChange != nil {
		w.tracked = make(map[NodeID]*trackedState)
	}
	return w
}

// pendingDelta is one queued delta plus whether it belongs to initial
// sync (ApplyInitial): sync deltas fold into topology and change
// tracking but emit no §6.6 ChangeRecords.
type pendingDelta struct {
	Delta
	sync bool
}

// batch is the next snapshot under construction: full map clones of
// the previous snapshot (plain-map COW, §6.2), mutated freely until
// published. Edge slices are cloned on first modification so the
// previous snapshot's storage is never touched.
type batch struct {
	nodes map[NodeID]nodeInfo
	out   map[NodeID][]Edge
	in    map[NodeID][]Edge
	edges int
}

// FromObjects is the initial-sync path (§6.3): it consumes the full
// object stream (as produced by paged Lists), builds the graph off
// to the side, and publishes exactly one snapshot at the end.
// Readers keep getting ErrNotReady for the whole duration and never
// observe a partially built graph.
//
// The stream is validated before anything is applied: an unsupported
// object aborts with no state change and no swap.
func (w *Writer) FromObjects(objs iter.Seq[any]) error {
	var list []any
	for o := range objs {
		if err := validateObject(o); err != nil {
			return err
		}
		list = append(list, o)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	b := w.newBatch()
	for _, o := range list {
		if w.onChange != nil {
			// Seed change tracking without emitting records: initial
			// sync is covered by the first stored snapshot (§6.6).
			w.trackSeed(o)
		}
		w.applyDelta(b, Delta{Op: OpAdd, Object: o})
	}
	w.publish(b)
	return nil
}

// Apply queues deltas for the next snapshot. All deltas are
// validated before any is queued (invalid input changes nothing).
// The batch is swapped in when the swap interval elapses, or on
// Flush.
func (w *Writer) Apply(deltas ...Delta) error {
	return w.queue(deltas, false)
}

// ApplyInitial queues deltas that are part of INITIAL SYNC rather
// than live change: they fold into the topology and seed change
// tracking exactly like Apply, but emit no §6.6 ChangeRecords. This
// is the informer-side companion of FromObjects' no-record seeding
// (M3 drill observation 5): handler deltas replayed from the initial
// LIST window are state the first stored snapshot covers, not
// changes, and logging them would report every pre-existing object
// as "Added" at the sync instant.
func (w *Writer) ApplyInitial(deltas ...Delta) error {
	return w.queue(deltas, true)
}

func (w *Writer) queue(deltas []Delta, sync bool) error {
	for i := range deltas {
		if deltas[i].Op <= opInvalid || deltas[i].Op > OpDelete {
			return fmt.Errorf("graph: delta %d: invalid op %d", i, deltas[i].Op)
		}
		if err := validateObject(deltas[i].Object); err != nil {
			return fmt.Errorf("graph: delta %d: %w", i, err)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	for _, d := range deltas {
		w.pending = append(w.pending, pendingDelta{Delta: d, sync: sync})
	}
	if w.timer == nil && w.interval > 0 {
		w.timer = time.AfterFunc(w.interval, w.timedFlush)
	}
	return nil
}

func (w *Writer) timedFlush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.timer = nil
	if w.closed {
		return
	}
	w.flushLocked()
}

// Flush applies all pending deltas and publishes a snapshot
// immediately. It also publishes when nothing is pending but no
// snapshot exists yet (an explicitly empty-but-ready graph).
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	w.flushLocked()
	return nil
}

// Close stops the batching timer and rejects further mutations.
// Pending unflushed deltas are discarded; published snapshots remain
// valid and readable forever.
func (w *Writer) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	w.pending = nil
}

// flushLocked builds and publishes the next snapshot. Caller holds mu.
func (w *Writer) flushLocked() {
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	if len(w.pending) == 0 && w.g.snap.Load() != nil {
		return
	}
	b := w.newBatch()
	var recs []ChangeRecord
	for _, pd := range w.pending {
		if w.onChange == nil {
			w.applyDelta(b, pd.Delta)
			continue
		}
		if pd.sync {
			// Initial-sync delta (ApplyInitial): fold state and keep
			// change tracking current, but record nothing — the first
			// stored snapshot is the baseline (§6.6).
			_ = w.trackChange(pd.Delta)
			w.applyDelta(b, pd.Delta)
			continue
		}
		// §6.6 delta log: derive the changed-field summary from the
		// typed object (only place it is visible), then capture the
		// apply's primitive mutations as the replay effect.
		rec := w.trackChange(pd.Delta)
		w.rec = &effectLog{}
		w.applyDelta(b, pd.Delta)
		rec.Effect = w.rec.encode()
		w.rec = nil
		recs = append(recs, rec)
	}
	w.pending = nil
	w.publish(b)
	// Deliver after publish so Generation is the swap counter of the
	// snapshot that first contains each change — the store's replay
	// cursor. Still under mu: records reach the hook in apply order.
	for i := range recs {
		recs[i].Generation = w.gen
		w.onChange(recs[i])
	}
}

// newBatch clones the current snapshot's maps (or starts empty).
// This full-map clone per swap is the headline cost of the plain-map
// COW representation; it is measured by BenchmarkSnapshotSwap and is
// a primary input to the §15 Q5 compaction gate.
func (w *Writer) newBatch() *batch {
	prev := w.g.snap.Load()
	if prev == nil {
		return &batch{
			nodes: make(map[NodeID]nodeInfo),
			out:   make(map[NodeID][]Edge),
			in:    make(map[NodeID][]Edge),
		}
	}
	return &batch{
		nodes: maps.Clone(prev.nodes),
		out:   maps.Clone(prev.out),
		in:    maps.Clone(prev.in),
		edges: prev.edges,
	}
}

// publish swaps the built batch in as the new current snapshot.
// Caller holds mu.
func (w *Writer) publish(b *batch) {
	w.gen++
	w.g.snap.Store(&Snapshot{
		nodes:      b.nodes,
		out:        b.out,
		in:         b.in,
		edges:      b.edges,
		keys:       w.intern.keys,
		ids:        &w.intern.ids,
		watched:    w.watched,
		generation: w.gen,
	})
}

// node interns the identity and ensures a (possibly unobserved) node
// record exists in the batch. Namespaced identities also ensure
// their Namespace node.
func (w *Writer) node(b *batch, kind NodeKind, namespace, name string) NodeID {
	key := nodeKey(kind, namespace, name)
	id := w.intern.intern(key)
	if _, ok := b.nodes[id]; !ok {
		var nsID NodeID
		if namespace != "" {
			nsID = w.node(b, KindNamespace, "", namespace)
		}
		// A namespaced identity proves its namespace exists, so
		// Namespace nodes are observed from the start; everything
		// else starts unobserved until its object is actually seen.
		b.nodes[id] = nodeInfo{kind: kind, observed: kind == KindNamespace, ns: nsID}
		if w.rec != nil {
			w.rec.nodeNew(id, kind, key)
		}
	}
	return id
}

// observe marks a node as backed by an object actually seen from the
// API server.
func (w *Writer) observe(b *batch, id NodeID, kind NodeKind) {
	info := b.nodes[id]
	info.kind = kind
	info.observed = true
	b.nodes[id] = info
	if w.rec != nil {
		w.rec.observe(id, kind)
	}
}

// markDeleted flips a node back to unobserved and garbage-collects
// it if nothing references it anymore. Nodes that other objects
// still point at survive as Observed=false — a dangling reference is
// triage signal, not garbage.
func (w *Writer) markDeleted(b *batch, id NodeID) {
	info, ok := b.nodes[id]
	if !ok {
		return
	}
	info.observed = false
	b.nodes[id] = info
	if w.rec != nil {
		w.rec.unobserve(id)
	}
	w.maybeGC(b, id)
}

// maybeGC removes a node that no edge touches and no object backs.
// Namespace nodes are exempt (they are referenced via nodeInfo.ns,
// which is not refcounted; their count is trivially bounded).
// Container and Zone nodes are derived, not API objects, so they
// are collected as soon as the last edge goes away even though they
// are "observed".
func (w *Writer) maybeGC(b *batch, id NodeID) {
	if w.refcnt[id] > 0 {
		return
	}
	info, ok := b.nodes[id]
	if !ok || info.kind == KindNamespace {
		return
	}
	if info.observed && info.kind != KindContainer && info.kind != KindZone {
		return
	}
	delete(b.nodes, id)
	delete(w.refcnt, id)
	if w.rec != nil {
		w.rec.gc(id)
	}
}

// setDeclared reconciles the full set of edges declared by src:
// edges no longer declared are removed, new ones added, everything
// else untouched. want may contain duplicates (e.g. a ConfigMap
// referenced from both env and a volume); they are folded.
func (w *Writer) setDeclared(b *batch, src NodeID, want []halfEdge) {
	old := w.declared[src]
	if len(old) == 0 && len(want) == 0 {
		return
	}
	wantSet := make(map[halfEdge]struct{}, len(want))
	uniq := make([]halfEdge, 0, len(want))
	for _, e := range want {
		if _, dup := wantSet[e]; !dup {
			wantSet[e] = struct{}{}
			uniq = append(uniq, e)
		}
	}
	oldSet := make(map[halfEdge]struct{}, len(old))
	for _, e := range old {
		oldSet[e] = struct{}{}
		if _, keep := wantSet[e]; !keep {
			w.removeEdge(b, e)
		}
	}
	for _, e := range uniq {
		if _, had := oldSet[e]; !had {
			w.addEdge(b, e)
		}
	}
	if len(uniq) == 0 {
		delete(w.declared, src)
	} else {
		w.declared[src] = uniq
	}
}

// setSelectorEdge adds or removes a single selector-derived edge
// (Service Selects / NetworkPolicy Governs → pod), keeping the
// source's declared set in sync. Used on the pod-churn path where
// re-deriving the source's full edge set would be O(pods).
func (w *Writer) setSelectorEdge(b *batch, src, pod NodeID, kind EdgeKind, want bool) {
	e := halfEdge{From: src, To: pod, Kind: kind}
	cur := w.declared[src]
	i := -1
	for j := range cur {
		if cur[j] == e {
			i = j
			break
		}
	}
	switch {
	case want && i < 0:
		w.addEdge(b, e)
		w.declared[src] = append(cur, e)
	case !want && i >= 0:
		w.removeEdge(b, e)
		cur[i] = cur[len(cur)-1]
		cur = cur[:len(cur)-1]
		if len(cur) == 0 {
			delete(w.declared, src)
		} else {
			w.declared[src] = cur
		}
	}
}

// addEdge inserts e into the batch adjacency (idempotent).
func (w *Writer) addEdge(b *batch, e halfEdge) {
	out, added := insertEdge(b.out[e.From], Edge{To: e.To, Kind: e.Kind})
	if !added {
		return
	}
	b.out[e.From] = out
	in, _ := insertEdge(b.in[e.To], Edge{To: e.From, Kind: e.Kind})
	b.in[e.To] = in
	w.refcnt[e.From]++
	w.refcnt[e.To]++
	b.edges++
	if w.rec != nil {
		w.rec.edge(effEdgeAdd, e)
	}
}

// removeEdge deletes e from the batch adjacency (idempotent) and
// garbage-collects endpoints that end up unreferenced.
func (w *Writer) removeEdge(b *batch, e halfEdge) {
	out, removed := deleteEdge(b.out[e.From], Edge{To: e.To, Kind: e.Kind})
	if !removed {
		return
	}
	storeEdges(b.out, e.From, out)
	in, _ := deleteEdge(b.in[e.To], Edge{To: e.From, Kind: e.Kind})
	storeEdges(b.in, e.To, in)
	w.refcnt[e.From]--
	w.refcnt[e.To]--
	b.edges--
	if w.rec != nil {
		w.rec.edge(effEdgeRemove, e)
	}
	w.maybeGC(b, e.From)
	w.maybeGC(b, e.To)
}

func storeEdges(m map[NodeID][]Edge, id NodeID, edges []Edge) {
	if len(edges) == 0 {
		delete(m, id)
		return
	}
	m[id] = edges
}

// edgeLess orders adjacency slices by (Kind, To) — deterministic
// snapshots regardless of map iteration order upstream.
func edgeLess(a, b Edge) bool {
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	return a.To < b.To
}

// findEdge binary-searches s for e, returning the insertion index
// and whether e is present.
func findEdge(s []Edge, e Edge) (int, bool) {
	i := sort.Search(len(s), func(i int) bool { return !edgeLess(s[i], e) })
	return i, i < len(s) && s[i] == e
}

// insertEdge returns a *fresh* slice with e inserted in order
// (published snapshots may alias the input's backing array, so
// in-place mutation is forbidden). added=false if already present.
func insertEdge(s []Edge, e Edge) (out []Edge, added bool) {
	i, present := findEdge(s, e)
	if present {
		return s, false
	}
	out = make([]Edge, 0, len(s)+1)
	out = append(out, s[:i]...)
	out = append(out, e)
	out = append(out, s[i:]...)
	return out, true
}

// deleteEdge returns a fresh slice with e removed; removed=false if
// absent.
func deleteEdge(s []Edge, e Edge) (out []Edge, removed bool) {
	i, present := findEdge(s, e)
	if !present {
		return s, false
	}
	out = make([]Edge, 0, len(s)-1)
	out = append(out, s[:i]...)
	out = append(out, s[i+1:]...)
	return out, true
}

// validateObject checks that obj is a supported typed object.
// Supported groups per §15 Q6: core, apps, batch, discovery.k8s.io,
// storage (PVCs), networking.k8s.io.
func validateObject(obj any) error {
	switch obj.(type) {
	case *corev1.Pod, *corev1.Service, *corev1.Node, *corev1.Namespace,
		*corev1.ConfigMap, *corev1.Secret, *corev1.PersistentVolumeClaim,
		*appsv1.Deployment, *appsv1.ReplicaSet, *appsv1.StatefulSet, *appsv1.DaemonSet,
		*batchv1.Job, *batchv1.CronJob,
		*discoveryv1.EndpointSlice,
		*netv1.Ingress, *netv1.NetworkPolicy:
		return nil
	case nil:
		return errors.New("nil object")
	default:
		return fmt.Errorf("unsupported object type %T", obj)
	}
}

// applyDelta folds one delta into the batch. Caller holds mu and has
// validated the object.
func (w *Writer) applyDelta(b *batch, d Delta) {
	switch o := d.Object.(type) {
	case *corev1.Pod:
		w.applyPod(b, d.Op, o)
	case *corev1.Service:
		w.applyService(b, d.Op, o)
	case *corev1.Node:
		w.applyNode(b, d.Op, o)
	case *corev1.Namespace:
		w.applyPlain(b, d.Op, KindNamespace, "", o.Name, o.OwnerReferences)
	case *corev1.ConfigMap:
		w.applyPlain(b, d.Op, KindConfigMap, o.Namespace, o.Name, o.OwnerReferences)
	case *corev1.Secret:
		// ObjectMeta only — Secret.Data is never read (§6.5).
		w.applyPlain(b, d.Op, KindSecret, o.Namespace, o.Name, o.OwnerReferences)
	case *corev1.PersistentVolumeClaim:
		w.applyPlain(b, d.Op, KindPersistentVolumeClaim, o.Namespace, o.Name, o.OwnerReferences)
	case *appsv1.Deployment:
		w.applyPlain(b, d.Op, KindDeployment, o.Namespace, o.Name, o.OwnerReferences)
	case *appsv1.ReplicaSet:
		w.applyPlain(b, d.Op, KindReplicaSet, o.Namespace, o.Name, o.OwnerReferences)
	case *appsv1.StatefulSet:
		w.applyPlain(b, d.Op, KindStatefulSet, o.Namespace, o.Name, o.OwnerReferences)
	case *appsv1.DaemonSet:
		w.applyPlain(b, d.Op, KindDaemonSet, o.Namespace, o.Name, o.OwnerReferences)
	case *batchv1.Job:
		w.applyPlain(b, d.Op, KindJob, o.Namespace, o.Name, o.OwnerReferences)
	case *batchv1.CronJob:
		w.applyPlain(b, d.Op, KindCronJob, o.Namespace, o.Name, o.OwnerReferences)
	case *discoveryv1.EndpointSlice:
		w.applyEndpointSlice(b, d.Op, o)
	case *netv1.Ingress:
		w.applyIngress(b, d.Op, o)
	case *netv1.NetworkPolicy:
		w.applyNetworkPolicy(b, d.Op, o)
	}
}
