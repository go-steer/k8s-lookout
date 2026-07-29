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
	"fmt"
	"log"
	"slices"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// graphFeed wires pkg/graph into the sentinel for storm correlation
// (DESIGN.md §6.3 + §7.5): a single-writer ingest loop fed by the SAME
// shared informer factory the signal sources use, publishing COW
// topology snapshots the StormCorrelator's AncestorResolver reads.
//
// Informer set — deliberately minimal, chosen from what the §7.5
// blast-radius query (Snapshot.CommonAncestors: inbound Owns, outbound
// RunsOn/Mounts, namespace) can actually consume:
//
//   - pods: RunsOn(node) from spec.nodeName, Mounts(ConfigMap/Secret/
//     PVC) from env/envFrom/volumes, and Owns edges from the pod's own
//     ownerReferences (ReplicaSet/StatefulSet/DaemonSet/Job/Node).
//     Shared with the object-state source's pod informer — same
//     factory, one watch.
//   - nodes: observed Node identity + Node→Zone. Shared with
//     object-state.
//   - replicasets: the only informer no source watches yet — an RS's
//     ownerReferences extend the owner chain to the Deployment, the
//     ancestor operators actually recognize ("Deployment payment", not
//     "ReplicaSet payment-7b9d"). Needs one new RBAC grant
//     (apps/replicasets list+watch), verified loudly below (§11).
//
// Deliberately NOT watched here (and why the keys still work):
//   - ConfigMaps/Secrets/PVCs: Mounts edges are declared by pod specs,
//     so the shared-config ancestor exists as a referenced identity
//     (Observed=false) without paying for informer caches of ConfigMap
//     payloads — the correlation key needs the identity, not the data.
//   - Services/EndpointSlices/Ingresses/NetworkPolicies/Jobs: not on
//     the CommonAncestors relation (or marginal — CronJob-level
//     grouping needs a jobs watch); they arrive with the full §6.1
//     graph in the enrichment milestone (M3), not here.
//   - Zone grouping is excluded from storm keys entirely: zone-tier
//     correlation is the fleet layer's job (§7.5).
type graphFeed struct {
	factory informers.SharedInformerFactory
	graph   *graph.Graph

	mu    sync.Mutex
	armed bool
	buf   []graph.Delta
}

// newGraphFeed constructs the feed over an externally owned shared
// informer factory (the same one the object-state source registers
// on when both are enabled). onChange, when non-nil, arms the §6.6
// delta log: every informer delta applied AFTER initial sync emits a
// ChangeRecord the sentinel routes into the store's buffered writer
// (nil when no --store is configured — the graph then skips change
// tracking entirely). Initial-sync deltas — the objects listed at
// startup, including the handler Adds that race the listing — emit
// nothing: they are baseline, covered by the first stored snapshot.
func newGraphFeed(factory informers.SharedInformerFactory, onChange func(graph.ChangeRecord)) *graphFeed {
	return &graphFeed{
		factory: factory,
		graph: graph.New(graph.Options{
			OnChange: onChange,
			// The honesty declaration behind Ref.Observed: exactly the
			// informer set Run registers (pods/nodes/replicasets). For
			// these kinds an unobserved node is a REAL absence; every
			// other kind exists here identity-only (see the "not
			// watched" list above), so radius consumers must report it
			// as unknown, never as missing.
			WatchedKinds: []graph.NodeKind{graph.KindPod, graph.KindNode, graph.KindReplicaSet},
		}),
	}
}

// graphAccess is the RBAC the feed's informers need. pods/nodes match
// the object-state source's grants; replicasets is the graph-only
// addition (see deploy/12-clusterrole-watcher.yaml).
var graphAccess = []sources.Requirement{
	{Resource: "pods", Verb: "list"},
	{Resource: "pods", Verb: "watch"},
	{Resource: "nodes", Verb: "list"},
	{Resource: "nodes", Verb: "watch"},
	{Group: "apps", Resource: "replicasets", Verb: "list"},
	{Group: "apps", Resource: "replicasets", Verb: "watch"},
}

// probeGraphAccess is the §11 loud startup check for the graph feed's
// informers. Storm correlation is explicitly opted into (--storm), so
// — unlike the recovery observer's degrade-with-a-log posture — a
// missing grant is a hard startup error naming it: the operator asked
// for correlation and a silently keyless correlator would lie.
func probeGraphAccess(ctx context.Context, reviewer sources.AccessReviewer) error {
	for _, req := range graphAccess {
		allowed, err := reviewer.Allowed(ctx, req)
		if err != nil {
			return fmt.Errorf("storm: capability probe for %q failed: %w", req, err)
		}
		if !allowed {
			return fmt.Errorf("storm: --storm requires permission to %q for the topology graph informers and this ServiceAccount does not have it; grant it (see deploy/12-clusterrole-watcher.yaml) or drop --storm — refusing to run correlation over a silently empty graph", req)
		}
	}
	return nil
}

// Run registers the feed's handlers, waits for the initial sync,
// publishes the first snapshot (one atomic swap — readers get
// ErrNotReady, never a partial graph, until then; §6.3), then applies
// deltas until ctx is cancelled. Blocking; the sentinel runs it in a
// goroutine and treats an error as fatal.
func (g *graphFeed) Run(ctx context.Context) error {
	podInf := g.factory.Core().V1().Pods().Informer()
	nodeInf := g.factory.Core().V1().Nodes().Informer()
	rsInf := g.factory.Apps().V1().ReplicaSets().Informer()

	var regs []cache.ResourceEventHandlerRegistration
	for _, inf := range []cache.SharedIndexInformer{podInf, nodeInf, rsInf} {
		h, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { g.enqueue(graph.OpAdd, obj) },
			UpdateFunc: func(_, obj any) { g.enqueue(graph.OpUpdate, obj) },
			DeleteFunc: func(obj any) { g.enqueue(graph.OpDelete, tombstoneObj(obj)) },
		})
		if err != nil {
			return fmt.Errorf("storm: register graph handler: %w", err)
		}
		regs = append(regs, h)
	}

	g.factory.Start(ctx.Done())
	synced := make([]cache.InformerSynced, 0, len(regs))
	for _, h := range regs {
		synced = append(synced, h.HasSynced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return fmt.Errorf("storm: graph cache sync failed (informer stopped before initial list completed)")
	}

	// Initial sync (§6.3): build from the freshly synced informer
	// stores off to the side, publish with one swap. Handler deltas
	// that raced the store listing were buffered by enqueue and are
	// replayed on top — the apply is an idempotent upsert, so an
	// object present in both is harmless and a delete observed during
	// the listing wins on replay. The replay uses ApplyInitial: those
	// buffered deltas are overwhelmingly the initial LIST's Adds, and
	// change logging arms only for deltas applied AFTER this point —
	// the same discipline the signal sources use (arm-after-sync), so
	// pre-existing objects never masquerade as "Added" changes in the
	// §6.6 delta log. The first snapshot stored after arming captures
	// the baseline.
	var objs []any
	for _, inf := range []cache.SharedIndexInformer{podInf, nodeInf, rsInf} {
		objs = append(objs, inf.GetStore().List()...)
	}
	w := g.graph.Writer()
	if err := w.FromObjects(slices.Values(objs)); err != nil {
		return fmt.Errorf("storm: graph initial sync: %w", err)
	}
	if err := g.armAndReplay(w); err != nil {
		return fmt.Errorf("storm: replay buffered graph deltas: %w", err)
	}
	if snap, err := g.graph.Snapshot(); err == nil {
		log.Printf("storm: topology graph ready (%d nodes, %d edges) — blast-radius correlation armed", snap.NumNodes(), snap.NumEdges())
	}

	<-ctx.Done()
	w.Close()
	return nil
}

// armAndReplay drains the pre-arm buffer, replays it onto the writer,
// and arms the feed — all under a SINGLE critical section (issue #107).
//
// The lock is held across the ApplyInitial replay on purpose: enqueue
// also takes g.mu, so a live delta arriving during arming blocks until
// this method releases the lock — by which point every buffered
// (initial-sync) delta is already queued into the writer. That
// ordering is the invariant: buffered deltas always precede live ones,
// so a stale buffered placement can never be replayed on top of (and
// clobber) a newer live change for the same object. Setting g.armed and
// releasing the lock BEFORE ApplyInitial would reopen that window.
//
// Holding g.mu across ApplyInitial briefly blocks the informer handler
// during initial sync — intended: a one-time cost on the sync path.
// An empty buffer still arms; the error from ApplyInitial is returned
// unwrapped (the Run call site keeps the §6.3 replay wrap).
func (g *graphFeed) armAndReplay(w *graph.Writer) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	buffered := g.buf
	g.buf = nil
	g.armed = true
	if len(buffered) > 0 {
		return w.ApplyInitial(buffered...)
	}
	return nil
}

// snapshot exposes the live topology snapshot for the enrichment
// stage (§7.6: "sharing the live topology index"). ErrNotReady until
// the initial sync published — enrichment falls back to its scoped
// read path rather than wait (same posture as Ancestors below).
func (g *graphFeed) snapshot() (*graph.Snapshot, error) {
	return g.graph.Snapshot()
}

// tombstoneObj unwraps cache.DeletedFinalStateUnknown tombstones —
// pkg/graph deliberately has no client-go dependency, so the unwrap
// happens on the informer side (Writer contract).
func tombstoneObj(obj any) any {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return t.Obj
	}
	return obj
}

// enqueue routes one informer delta into the writer: buffered before
// the first snapshot (so a partial graph is never published), applied
// directly after.
func (g *graphFeed) enqueue(op graph.Op, obj any) {
	switch obj.(type) {
	case *corev1.Pod, *corev1.Node, *appsv1.ReplicaSet:
	default:
		return // tombstone with unexpected content, etc.
	}
	d := graph.Delta{Op: op, Object: obj}
	g.mu.Lock()
	if !g.armed {
		g.buf = append(g.buf, d)
		g.mu.Unlock()
		return
	}
	g.mu.Unlock()
	if err := g.graph.Writer().Apply(d); err != nil {
		log.Printf("storm: graph delta apply: %v", err)
	}
}

// stormObjectKinds maps a Signal's KindOfObject to the graph node
// kind, for resolving incident objects into the topology. Kinds
// absent here (Event, PodDisruptionBudget, custom resources) simply
// don't correlate — their incidents proceed per-incident.
var stormObjectKinds = map[string]graph.NodeKind{
	"Pod":                   graph.KindPod,
	"Node":                  graph.KindNode,
	"Namespace":             graph.KindNamespace,
	"Service":               graph.KindService,
	"Deployment":            graph.KindDeployment,
	"ReplicaSet":            graph.KindReplicaSet,
	"StatefulSet":           graph.KindStatefulSet,
	"DaemonSet":             graph.KindDaemonSet,
	"Job":                   graph.KindJob,
	"CronJob":               graph.KindCronJob,
	"ConfigMap":             graph.KindConfigMap,
	"Secret":                graph.KindSecret,
	"PersistentVolumeClaim": graph.KindPersistentVolumeClaim,
}

// ancestorClass ranks ancestor kinds by the §7.5 key priority:
// node > owner chain > shared config/PVC > namespace. -1 excludes the
// kind from storm keys entirely: Zone (fleet tier — the fleet layer's
// join, §7.5),
// Pod/Container (an object, not a blast-radius key), and the
// traffic-layer kinds (not ancestors on the CommonAncestors relation).
func ancestorClass(k graph.NodeKind) int {
	switch k {
	case graph.KindNode:
		return 0
	case graph.KindDeployment, graph.KindReplicaSet, graph.KindStatefulSet,
		graph.KindDaemonSet, graph.KindJob, graph.KindCronJob:
		return 1
	case graph.KindConfigMap, graph.KindSecret, graph.KindPersistentVolumeClaim:
		return 2
	case graph.KindNamespace:
		return 3
	default:
		return -1
	}
}

// Ancestors implements engine.AncestorResolver over the live topology
// snapshot: the object's correlation ancestors (Snapshot.
// CommonAncestors with a single input: transitive owners, placement,
// mounted config, namespace — including the object itself when it IS
// a groupable ancestor kind, which is how a Node incident seeds the
// storm keyed on that node), filtered to storm-key kinds and ordered
// by (§7.5 class priority, nearest-first within a class — the order
// pkg/graph returns).
//
// Before the first snapshot (initial sync still running) this returns
// nil: incidents proceed per-incident rather than stall — correlation
// is an optimization of session count, never a gate on paging.
func (g *graphFeed) Ancestors(ref engine.ObjectRef) []engine.Ancestor {
	snap, err := g.graph.Snapshot()
	if err != nil {
		return nil
	}
	kind, ok := stormObjectKinds[ref.Kind]
	if !ok {
		return nil
	}
	id, ok := snap.Lookup(kind, ref.Namespace, ref.Name)
	if !ok {
		return nil
	}
	type cand struct {
		class, idx int
		ref        graph.Ref
	}
	var cands []cand
	for idx, aid := range snap.CommonAncestors(id) {
		r, ok := snap.Resolve(aid)
		if !ok {
			continue
		}
		class := ancestorClass(r.Kind)
		if class < 0 {
			continue
		}
		cands = append(cands, cand{class: class, idx: idx, ref: r})
	}
	slices.SortStableFunc(cands, func(a, b cand) int {
		if a.class != b.class {
			return a.class - b.class
		}
		return a.idx - b.idx
	})
	out := make([]engine.Ancestor, 0, len(cands))
	for _, c := range cands {
		out = append(out, engine.Ancestor{
			Kind:      c.ref.Kind.String(),
			Namespace: c.ref.Namespace,
			Name:      c.ref.Name,
		})
	}
	return out
}
