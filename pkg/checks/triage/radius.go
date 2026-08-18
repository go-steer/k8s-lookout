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

import (
	"context"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// defaultRadiusDepth is the --depth default: three edges reaches the
// full traffic chain (pod ← slice ← service ← ingress) and the
// second-order co-tenants without dragging in half the cluster.
const defaultRadiusDepth = 3

// The radius kinds live in bundle, which emits them too from its own
// traversal of the same graph and sits below this package in the
// import order. Both render the claim from one declaration.
const (
	kindRadiusNeighbor = bundle.KindRadiusNeighbor
	kindRadiusMissing  = bundle.KindRadiusMissing
)

// RadiusCommand builds `lookout triage radius` (DESIGN.md §5): the
// blast radius of one pod/workload as a pure topology-index query
// (§6.4). It complements `state edges` — edges verifies *correctness*
// of dependencies, radius enumerates *impact*: what is upstream
// (routes/owns/governs), lateral (same node, shared PVC/ConfigMap),
// downstream (depended on). With --at it answers "what was the blast
// radius when the incident started" from a sentinel store (§6.6).
func RadiusCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:        "triage radius",
		MCPName:     "k8s_blast_radius",
		GraphBacked: true,
		Summary:     "Blast radius of one pod/workload — who is upstream (routes/owns/governs it), lateral (same node, shared config/volume), downstream (it depends on); --at answers it as of incident onset from a sentinel store.",
		Positional: &checks.Positional{
			Meta: "<Kind>/[<namespace>/]<name>",
			Doc:  targetDoc,
		},
		Flags: []emit.FlagSpec{
			{Name: "depth", Type: emit.FlagInt, Default: strconv.Itoa(defaultRadiusDepth),
				Help: "graph edges followed per direction from the target's pods"},
		},
		Kinds: bundle.RadiusKinds(),
		Output: []checks.OutputField{
			{Name: "direction", Doc: "neighbor's direction from the target: upstream (routes/owns/governs it), lateral (shares a node/volume/config), downstream (the target points at it)"},
			{Name: "relation", Doc: "how the neighbor attaches: the edge kind (RoutesTo, Owns, Selects, Governs, RunsOn, Mounts) for upstream/downstream, shared-node|shared-zone|shared-config|shared-secret|shared-pvc for lateral"},
			{Name: "hop", Doc: "BFS depth from the target at which the neighbor was first reached (1 = direct edge)"},
			{Name: "shared", Doc: "on lateral neighbors: the shared object as <Kind>/<name>"},
			{Name: "ready", Doc: "pod readiness (live mode only — history stores topology, not status)"},
			{Name: "source", Doc: "summary-line note: live (one-shot List pass) or history (reconstructed from --store)"},
			{Name: "at", Doc: "summary-line note: the resolved --at instant the history answer is as of, RFC 3339"},
		},
		Examples: []string{
			"lookout triage radius Deployment/prod/api",
			"lookout triage radius payments-api-7d9c4b-x2n8p --namespace=prod",
			"lookout triage radius --workload=StatefulSet/db/postgres --depth=2",
			"lookout triage radius Deployment/prod/api --at=20m --store=/var/lib/lookout/lookout.db",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runRadius(ctx, deps, inv)
		},
	}
}

func runRadius(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	depth := inv.Flags.Int("depth")
	if depth < 1 {
		return 0, emit.UsageErrorf("--depth must be at least 1, got %d", depth)
	}
	wl, err := parseTarget(inv)
	if err != nil {
		return 0, err
	}

	var (
		snap    *graph.Snapshot
		cluster *state.Cluster // nil in --at mode: readiness is unknowable from history
		scanned int
	)
	if !inv.Scope.At.IsZero() {
		snap, err = historicalSnapshot(ctx, inv.Scope.Store, inv.Scope.At)
		if err != nil {
			return 0, err
		}
		scanned = snap.NumNodes()
		if err := inv.Out.Note("source", "history"); err != nil {
			return 0, err
		}
		if err := inv.Out.Note("at", inv.Scope.At.UTC().Format(time.RFC3339)); err != nil {
			return 0, err
		}
	} else {
		client, err := deps.client(ctx)
		if err != nil {
			return 0, err
		}
		listNS := wl.Namespace
		if inv.Scope.AllNamespaces {
			listNS = metav1.NamespaceAll
		}
		cluster, err = state.LoadCluster(ctx, client, listNS)
		if err != nil {
			return 0, err
		}
		snap = cluster.Snapshot()
		scanned = cluster.Scanned()
		if err := inv.Out.Note("source", "live"); err != nil {
			return 0, err
		}
	}

	id, err := lookupTarget(snap, wl, inv.Scope.At)
	if err != nil {
		return 0, err
	}
	for _, nb := range bundle.RadiusNeighbors(snap, id, depth) {
		if err := inv.Out.Emit(neighborFinding(nb, snap, cluster)); err != nil {
			return 0, err
		}
	}
	return scanned, nil
}

// lateralRelations names the shared-object relation per anchor kind.
var lateralRelations = map[graph.NodeKind]string{
	graph.KindNode:                  "shared-node",
	graph.KindZone:                  "shared-zone",
	graph.KindConfigMap:             "shared-config",
	graph.KindSecret:                "shared-secret",
	graph.KindPersistentVolumeClaim: "shared-pvc",
}

// neighborFinding renders one blast-radius neighbor. cluster supplies
// live pod readiness and is nil in --at mode. snap supplies the
// watched-kind honesty check (Snapshot.Watches): the ReferencedNotFound
// claim is made only for kinds the snapshot's ingest actually observes
// — an unobserved neighbor of an unwatched kind is reported with
// observed=unknown instead (same rule as the bundle's radius section).
// The live one-shot List watches everything; history snapshots carry
// the sentinel feed's partial watched set through the store (LKGH v2),
// so identity-only neighbors of unwatched kinds keep the #46
// unknown-vs-missing honesty in --at answers too.
func neighborFinding(nb bundle.Neighbor, snap *graph.Snapshot, cluster *state.Cluster) emit.Finding {
	relation := nb.Via.String()
	details := []emit.Field{
		{Key: "direction", Value: nb.Direction},
		{Key: "relation", Value: relation},
		{Key: "hop", Value: strconv.Itoa(nb.Hop)},
	}
	if nb.Direction == "lateral" && nb.Anchor.ID != graph.NoNode {
		if rel, ok := lateralRelations[nb.Anchor.Kind]; ok {
			details[1].Value = rel
		}
		details = append(details, emit.Field{Key: "shared", Value: nb.Anchor.Kind.String() + "/" + nb.Anchor.Name})
	}
	if cluster != nil && nb.Ref.Kind == graph.KindPod {
		if pod := cluster.Pod(nb.Ref.Namespace, nb.Ref.Name); pod != nil {
			details = append(details, emit.Field{Key: "ready", Value: strconv.FormatBool(isPodReady(pod))})
		}
	}
	f := emit.Finding{
		Kind:         kindRadiusNeighbor,
		Severity:     emit.SeverityInfo,
		Namespace:    nb.Ref.Namespace,
		KindOfObject: nb.Ref.Kind.String(),
		Name:         nb.Ref.Name,
		Details:      details,
	}
	if !nb.Ref.Observed {
		if snap.Watches(nb.Ref.Kind) {
			f.Kind = kindRadiusMissing
			f.Severity = emit.SeverityWarning
			f.Reason = "ReferencedNotFound"
			f.Message = "referenced by the neighborhood but not observed"
		} else {
			f.Details = append(f.Details, emit.Field{Key: "observed", Value: "unknown"})
		}
	}
	return f
}
