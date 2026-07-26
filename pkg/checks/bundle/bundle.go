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

// Package bundle implements `lookout bundle` (DESIGN.md §5): the
// first tool call of every incident. Given one workload (or the
// inject payload that opened the incident session), it runs the
// read-path checks scoped to that target and emits ONE correlated
// payload — sanitized spec, abnormal-state delta, dependency-edge
// validity, blast radius, distilled logs — converting 4–5 agent
// round trips into one.
//
// Composition, not new checks: a single List pass (state.LoadCluster)
// feeds the topology graph, the edge validity checks, the delta
// derivations, and the log-target resolution; the spec section
// renders the very object that List returned. Findings carry a
// `section` field (spec|delta|edges|radius|logs) so the stream reads
// as one document. A `triage events` section joins in M3 when that
// command exists.
package bundle

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/delta"
	"github.com/go-steer/k8s-lookout/pkg/checks/logs"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

func init() {
	checks.Register(New(Deps{}))
}

// Deps injects the cluster-access seams. The zero value is the
// production wiring.
type Deps struct {
	// Client yields the Kubernetes client. Nil means kube.BuildClient
	// with default config resolution.
	Client kube.ClientSource
	// Logs streams one container's logs. Nil means the Client's
	// GetLogs subresource (tests inject fixture streams, §13).
	Logs logs.PodLogGetter
	// Now is the scan clock. Nil means time.Now.
	Now func() time.Time
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// sections, in emission order.
const (
	sectionSpec   = "spec"
	sectionDelta  = "delta"
	sectionEdges  = "edges"
	sectionRadius = "radius"
	sectionLogs   = "logs"
)

// New builds the `bundle` command around deps.
func New(deps Deps) checks.Command {
	return checks.Command{
		Name:    "bundle",
		MCPName: "k8s_triage_workload",
		Summary: "The first call of every incident: one correlated snapshot of a workload — sanitized spec, everything abnormal, broken dependency edges, blast radius, distilled logs — sectioned into a single payload instead of 4–5 separate reads.",
		Flags: []emit.FlagSpec{
			{Name: "incident", Type: emit.FlagString, Default: "",
				Help: "inject payload JSON (the message a lookout-watch incident session starts with); its object reference resolves to the target workload via the owner chain — alternative to --workload"},
			{Name: "depth", Type: emit.FlagInt, Default: "2",
				Help: "blast-radius traversal depth: graph edges followed per direction in the radius section"},
			{Name: "max-templates", Type: emit.FlagInt, Default: "15",
				Help: "cap distilled log template clusters in the logs section (triage logs defaults to 40; the bundle keeps the tighter budget)"},
			{Name: "cert-warn", Type: emit.FlagDuration, Default: "720h",
				Help: "report TLS certificates expiring within this window (edges section)"},
			{Name: "store", Type: emit.FlagString, Default: "",
				Help: "path to a sentinel's SQLite store (its --store file); merges open §9.4 triage-status records so the bundle's findings carry triage_* fields and severity reflects the agent's override"},
		},
		Output: append([]checks.OutputField{
			{Name: "section", Doc: "which bundle section the finding belongs to: spec|delta|edges|radius|logs (a triage-events section joins in M3)"},
			{Name: "sections", Doc: "on the bundle.target head finding: the sections that follow"},
			{Name: "relation", Doc: "radius neighbor's relation to the target: upstream (routes/owns/governs it), downstream (it points at), lateral (shares a node/volume/config)"},
			{Name: "hop", Doc: "radius neighbor's BFS depth from the target (1 = direct edge)"},
			{Name: memory.DetailTriageStatus, Doc: "triage state from the matched §9.4 record (investigating|triaged|actioned|escalated) — present only with --store on merged findings"},
			{Name: memory.DetailTriageRootCause, Doc: "the incident agent's root-cause hypothesis, from the matched triage-status record"},
			{Name: memory.DetailTriageAction, Doc: "the incident agent's paper trail (PRs opened, escalations), from the matched triage-status record"},
			{Name: memory.DetailTriageSession, Doc: "incident session that wrote the matched triage-status record"},
			{Name: memory.DetailTriageAge, Doc: "how long ago the matched triage-status record was last updated"},
		}, composedOutput()...),
		Examples: []string{
			"lookout bundle --workload=Deployment/prod/api",
			"lookout bundle --workload=StatefulSet/db/postgres --since=30m --format=json",
			"lookout bundle --incident='{\"namespace\":\"prod\",\"kind_of_object\":\"Pod\",\"name\":\"api-6d5f8c-x2v9k\"}'",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return run(ctx, deps, inv)
		},
	}
}

// composedOutput is the union of the composed sections' glossaries:
// every Details key the spec renderer, the delta derivations, the
// edge checks, and the log distiller may emit, deduplicated. Sourced
// from the composed commands' own metadata where they are registered,
// so the glossaries cannot drift apart.
func composedOutput() []checks.OutputField {
	seen := map[string]bool{
		"section": true, "sections": true, "relation": true, "hop": true,
		memory.DetailTriageStatus: true, memory.DetailTriageRootCause: true,
		memory.DetailTriageAction: true, memory.DetailTriageSession: true,
		memory.DetailTriageAge: true,
	}
	var out []checks.OutputField
	for _, name := range []string{"triage spec", "triage delta", "triage logs", "state edges"} {
		c, ok := checks.Lookup(name)
		if !ok {
			// Not registered (isolated test registry) — the §13
			// contract test in this package asserts all four are
			// present in the default registry.
			continue
		}
		for _, f := range c.Output {
			if seen[f.Name] {
				continue
			}
			seen[f.Name] = true
			out = append(out, f)
		}
	}
	return out
}

// run is the CheckFunc.
func run(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	depth := inv.Flags.Int("depth")
	maxTemplates := inv.Flags.Int("max-templates")
	if depth < 1 {
		return 0, emit.UsageErrorf("--depth must be at least 1, got %d", depth)
	}
	if maxTemplates < 1 {
		return 0, emit.UsageErrorf("--max-templates must be at least 1, got %d", maxTemplates)
	}
	seed, err := targetSeed(inv)
	if err != nil {
		return 0, err
	}

	client, err := clientFor(ctx, deps)
	if err != nil {
		return 0, err
	}
	listNS := seed.wl.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}
	cluster, err := state.LoadCluster(ctx, client, listNS)
	if err != nil {
		return 0, err
	}
	wl, err := resolveTarget(cluster, seed)
	if err != nil {
		return 0, err
	}
	id, err := cluster.WorkloadNode(wl)
	if err != nil {
		return 0, err
	}
	pods, err := cluster.WorkloadPods(wl)
	if err != nil {
		return 0, err
	}

	// Memory merge (§9.4): with --store, every bundle finding —
	// the head included — joins against the sentinel's open
	// triage-status records, so an incident bundle regenerated
	// mid-triage carries the diagnosis and paper trail instead of
	// re-presenting the raw symptom.
	var joiner *memory.Joiner
	if storePath := inv.Flags.String("store"); storePath != "" {
		st, err := store.OpenRead(storePath)
		if err != nil {
			return 0, err
		}
		defer func() { _ = st.Close() }()
		records, err := st.TriageStatuses(ctx, memory.TriageQuery{OpenOnly: true})
		if err != nil {
			return 0, err
		}
		joiner = memory.NewJoiner(records, deps.now())
	}

	out := func(section string, findings []emit.Finding) error {
		for _, f := range findings {
			f.Details = append([]emit.Field{{Key: "section", Value: section}}, f.Details...)
			if joiner != nil {
				joiner.Annotate(&f)
			}
			if err := inv.Out.Emit(f); err != nil {
				return err
			}
		}
		return nil
	}

	// Head: what this bundle is about and how to read it.
	head := emit.Finding{
		Kind:         "bundle.target",
		Severity:     emit.SeverityInfo,
		Namespace:    wl.Namespace,
		KindOfObject: wl.Kind,
		Name:         wl.Name,
		Details: []emit.Field{
			{Key: "workload", Value: wl.String()},
			{Key: "pods", Value: strconv.Itoa(len(pods))},
			{Key: "sections", Value: strings.Join([]string{sectionSpec, sectionDelta, sectionEdges, sectionRadius, sectionLogs}, ",")},
		},
	}
	if joiner != nil {
		joiner.Annotate(&head)
	}
	if err := inv.Out.Emit(head); err != nil {
		return 0, err
	}

	// spec: the sanitized target object, straight from the List pass.
	if obj := cluster.WorkloadObject(wl); obj != nil {
		specFs, err := checks.SpecFindings(wl.Kind, wl.Namespace, wl.Name, obj)
		if err != nil {
			return 0, err
		}
		if err := out(sectionSpec, specFs); err != nil {
			return 0, err
		}
	}

	// delta: abnormal state of the target's own objects.
	if err := out(sectionDelta, delta.ScanObjects(deps.now(), delta.Config{}, deltaObjectsFor(cluster.WorkloadObject(wl), pods))); err != nil {
		return 0, err
	}

	// edges: dependency-edge validity, same Cluster, same graph.
	edgeFs, err := cluster.EdgeFindings(wl, inv.Flags.Duration("cert-warn"), deps.now())
	if err != nil {
		return 0, err
	}
	if err := out(sectionEdges, edgeFs); err != nil {
		return 0, err
	}

	// radius: the blast-radius neighborhood (§6.4).
	if err := out(sectionRadius, radiusFindings(cluster.Snapshot(), id, depth)); err != nil {
		return 0, err
	}

	// logs: the pods' streams, distilled.
	getter := deps.Logs
	if getter == nil {
		getter = logs.ClientGetter(client)
	}
	var targets []logs.Target
	for _, p := range pods {
		targets = append(targets, logs.PodTargets(p)...)
	}
	lines, logFs, err := logs.Distill(ctx, getter, targets, logs.DistillOptions{
		Since:        inv.Scope.Since,
		MaxTemplates: maxTemplates,
	})
	if err != nil {
		return 0, err
	}
	if err := out(sectionLogs, logFs); err != nil {
		return 0, err
	}

	// The summary counts the whole scan: every listed object plus
	// every raw log line distilled.
	return cluster.Scanned() + lines, nil
}

func clientFor(ctx context.Context, deps Deps) (kubernetes.Interface, error) {
	if deps.Client != nil {
		return deps.Client(ctx)
	}
	return kube.DefaultSource()(ctx)
}

// seed is the pre-List target reference: either the workload itself
// or an incident payload's object reference awaiting owner-chain
// resolution.
type seed struct {
	wl            emit.WorkloadRef
	fromIncident  bool
	controllerRef string // "Kind/name" fallback when the object is gone
}

// targetSeed merges --workload and --incident into one target
// reference, usage-checking the combination.
func targetSeed(inv emit.Invocation) (seed, error) {
	incident := inv.Flags.String("incident")
	wl := inv.Scope.Workload
	switch {
	case incident != "" && !wl.IsZero():
		return seed{}, emit.UsageErrorf("give the target either via --workload or --incident, not both")
	case incident == "" && wl.IsZero():
		return seed{}, emit.UsageErrorf("no target: pass --workload=<Kind>/<namespace>/<name> or --incident=<inject payload JSON>")
	case incident == "":
		if inv.Scope.Namespace != "" && inv.Scope.Namespace != wl.Namespace {
			return seed{}, emit.UsageErrorf("--namespace=%s contradicts --workload namespace %s", inv.Scope.Namespace, wl.Namespace)
		}
		return seed{wl: wl}, nil
	}

	var p inject.Payload
	if err := json.Unmarshal([]byte(incident), &p); err != nil {
		return seed{}, emit.UsageErrorf("--incident: not valid inject payload JSON: %v", err)
	}
	if p.Namespace == "" || p.KindOfObject == "" || p.Name == "" {
		return seed{}, emit.UsageErrorf("--incident: payload needs namespace, kind_of_object, and name")
	}
	return seed{
		wl:            emit.WorkloadRef{Kind: p.KindOfObject, Namespace: p.Namespace, Name: p.Name},
		fromIncident:  true,
		controllerRef: p.Context.ControllerRef,
	}, nil
}

// workloadNodeKinds are the graph kinds bundle accepts as a resolved
// target — the same set `state edges` supports.
var workloadNodeKinds = map[graph.NodeKind]bool{
	graph.KindPod:         true,
	graph.KindDeployment:  true,
	graph.KindReplicaSet:  true,
	graph.KindStatefulSet: true,
	graph.KindDaemonSet:   true,
	graph.KindJob:         true,
	graph.KindCronJob:     true,
}

// resolveTarget turns the seed into the workload the bundle is
// about. A --workload seed passes through; an --incident seed
// resolves its object reference up the owner chain to the topmost
// owning workload (Pod → ReplicaSet → Deployment), falling back to
// the payload's controller_ref when the object itself is already
// gone.
func resolveTarget(cluster *state.Cluster, s seed) (emit.WorkloadRef, error) {
	if !s.fromIncident {
		return s.wl, nil
	}
	snap := cluster.Snapshot()
	id, err := cluster.WorkloadNode(s.wl)
	if err != nil {
		// The incident object may be gone (crashlooped pod deleted
		// by a rollout); its recorded controller is the next-best
		// target.
		if k, n, ok := strings.Cut(s.controllerRef, "/"); ok && k != "" && n != "" {
			ref := emit.WorkloadRef{Kind: k, Namespace: s.wl.Namespace, Name: n}
			if _, err2 := cluster.WorkloadNode(ref); err2 == nil {
				return resolveOwners(cluster, ref)
			}
		}
		return emit.WorkloadRef{}, fmt.Errorf("--incident object %s: %w", s.wl, err)
	}
	return topmostOwner(snap, id, s.wl)
}

// resolveOwners re-enters owner-chain resolution for a reference
// already known to exist.
func resolveOwners(cluster *state.Cluster, wl emit.WorkloadRef) (emit.WorkloadRef, error) {
	id, err := cluster.WorkloadNode(wl)
	if err != nil {
		return emit.WorkloadRef{}, err
	}
	return topmostOwner(cluster.Snapshot(), id, wl)
}

// topmostOwner walks the owner chain and returns the highest
// observed workload-kind owner (the Deployment behind the pod's
// ReplicaSet), or the reference itself when unowned.
func topmostOwner(snap *graph.Snapshot, id graph.NodeID, fallback emit.WorkloadRef) (emit.WorkloadRef, error) {
	chain := snap.OwnerChain(id)
	for i := len(chain) - 1; i >= 0; i-- {
		ref, ok := snap.Resolve(chain[i])
		if !ok || !ref.Observed || !workloadNodeKinds[ref.Kind] {
			continue
		}
		return emit.WorkloadRef{Kind: ref.Kind.String(), Namespace: ref.Namespace, Name: ref.Name}, nil
	}
	return fallback, nil
}

// deltaObjectsFor scopes the delta derivations to the target: its
// pods plus the workload object itself. Cluster-scoped delta classes
// (nodes, PDBs, quotas, system add-ons) belong to `triage delta` and
// `health`, not to a single-workload bundle. Exported via
// DeltaObjectsFor for the §7.6 enrichment stage.
func deltaObjectsFor(obj any, pods []*corev1.Pod) delta.Objects {
	objs := delta.Objects{}
	for _, p := range pods {
		objs.Pods = append(objs.Pods, *p)
	}
	switch o := obj.(type) {
	case *appsv1.Deployment:
		objs.Deployments = []appsv1.Deployment{*o}
	case *appsv1.StatefulSet:
		objs.StatefulSets = []appsv1.StatefulSet{*o}
	case *appsv1.DaemonSet:
		objs.DaemonSets = []appsv1.DaemonSet{*o}
	case *batchv1.Job:
		objs.Jobs = []batchv1.Job{*o}
	}
	return objs
}

// relation precedence for merging and for emission order.
var relations = []string{"upstream", "lateral", "downstream"}

// Neighbor is one merged blast-radius neighbor of a target: the
// resolved node plus how it relates to the target. It is the shared
// §6.4 radius answer behind the bundle's radius section, the §7.6
// enrichment, and `triage radius` — one traversal-and-merge
// implementation, several renderers.
type Neighbor struct {
	// Ref is the resolved neighbor (Observed=false means it exists
	// only as a dangling reference — triage signal in itself).
	Ref graph.Ref
	// Direction is upstream (routes to / owns / governs the target),
	// lateral (shares a node, volume, or config), or downstream (the
	// target points at it).
	Direction string
	// Hop is the smallest BFS depth at which any of the target's pods
	// reached the neighbor (1 = direct edge).
	Hop int
	// Via is the edge kind that first reached the neighbor at that
	// hop — RoutesTo, Owns, Selects, Governs, RunsOn, Mounts. For
	// lateral neighbors it is the co-tenant's own edge to the shared
	// node (RunsOn or Mounts).
	Via graph.EdgeKind
	// Anchor is the shared-infrastructure node behind a lateral
	// neighbor (resolved: the Node, Zone, ConfigMap, Secret, or PVC
	// both parties touch). Zero for upstream/downstream neighbors.
	Anchor graph.Ref
}

// radiusNeighbors computes the merged §6.4 blast-radius neighborhood:
// upstream (everything that routes to, owns, or governs the target —
// the owner chain and the traffic chain), lateral (co-tenants of its
// nodes/volumes/configs), downstream (everything it points at). The
// traversal starts at the target's pods — a workload object has no
// inbound edges of its own; Services, EndpointSlices, Ingresses, and
// policies all attach at the pod — falling back to the workload node
// when it currently has none. Hits are merged across pods on the
// smallest hop count (direction precedence upstream < lateral <
// downstream on ties). Container nodes are elided (spec detail, not
// neighbors) and the target's own pods are not their own
// neighborhood. Deterministically sorted by (direction, hop, node).
func radiusNeighbors(snap *graph.Snapshot, id graph.NodeID, depth int) []Neighbor {
	origins := snap.PodsUnder(id)
	if len(origins) == 0 {
		origins = []graph.NodeID{id}
	}
	self := map[graph.NodeID]bool{id: true}
	for _, o := range origins {
		self[o] = true
	}

	type hit struct {
		relation int // index into relations
		hop      int
		via      graph.EdgeKind
		anchor   graph.NodeID
	}
	merged := map[graph.NodeID]hit{}
	absorb := func(relation int, hits []graph.Hit) {
		for _, h := range hits {
			if self[h.ID] {
				continue
			}
			cur, seen := merged[h.ID]
			if !seen || h.Depth < cur.hop || (h.Depth == cur.hop && relation < cur.relation) {
				merged[h.ID] = hit{relation: relation, hop: h.Depth, via: h.Via, anchor: h.Anchor}
			}
		}
	}
	for _, o := range origins {
		res := snap.Radius(o, depth)
		absorb(0, res.Up)
		absorb(1, res.Lateral)
		absorb(2, res.Down)
	}

	ids := make([]graph.NodeID, 0, len(merged))
	for n := range merged {
		ids = append(ids, n)
	}
	slices.SortFunc(ids, func(a, b graph.NodeID) int {
		ha, hb := merged[a], merged[b]
		if ha.relation != hb.relation {
			return ha.relation - hb.relation
		}
		if ha.hop != hb.hop {
			return ha.hop - hb.hop
		}
		return int(a) - int(b)
	})

	var out []Neighbor
	for _, n := range ids {
		ref, ok := snap.Resolve(n)
		if !ok || ref.Kind == graph.KindContainer {
			continue
		}
		h := merged[n]
		nb := Neighbor{Ref: ref, Direction: relations[h.relation], Hop: h.hop, Via: h.via}
		if h.anchor != graph.NoNode {
			if aref, ok := snap.Resolve(h.anchor); ok {
				nb.Anchor = aref
			}
		}
		out = append(out, nb)
	}
	return out
}

// radiusFindings renders the neighborhood as the bundle's radius
// section: relation (= direction) + hop, one line per neighbor; a
// neighbor that exists only as a dangling reference is a warning, not
// an info line.
//
// The missing claim is honest about the index's blind spots
// (Snapshot.Watches): radius.missing reason=ReferencedNotFound is
// asserted only for kinds the snapshot's ingest actually watches —
// there, unobserved really means "not on the API server". A neighbor
// of an UNWATCHED kind (the sentinel's live graph holds mounted
// ConfigMaps/Secrets/PVCs identity-only, §6.3) stays a plain
// radius.neighbor carrying observed=unknown: its existence was never
// checked, and claiming a dangling reference would mislead the agent.
// One-shot full-List snapshots watch everything, so CLI bundles are
// unchanged.
func radiusFindings(snap *graph.Snapshot, id graph.NodeID, depth int) []emit.Finding {
	var out []emit.Finding
	for _, nb := range radiusNeighbors(snap, id, depth) {
		f := emit.Finding{
			Kind:         "radius.neighbor",
			Severity:     emit.SeverityInfo,
			Namespace:    nb.Ref.Namespace,
			KindOfObject: nb.Ref.Kind.String(),
			Name:         nb.Ref.Name,
			Details: []emit.Field{
				{Key: "relation", Value: nb.Direction},
				{Key: "hop", Value: strconv.Itoa(nb.Hop)},
			},
		}
		if !nb.Ref.Observed {
			if snap.Watches(nb.Ref.Kind) {
				f.Kind = "radius.missing"
				f.Severity = emit.SeverityWarning
				f.Reason = "ReferencedNotFound"
				f.Message = "referenced by the neighborhood but not observed on the API server"
			} else {
				f.Details = append(f.Details, emit.Field{Key: "observed", Value: "unknown"})
			}
		}
		out = append(out, f)
	}
	return out
}
