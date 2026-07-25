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

package bundle

// Exported seams for the §7.6 enrichment stage: `lookout watch` runs
// the SAME bundle composition in-process before injecting an incident
// session's first message, so the pieces the CLI command wires
// together privately are exported here for the sentinel to reuse.
// Each entry point is a thin veneer over the corresponding internal
// function — no second implementation, per the composition-not-new-
// checks rule this package is built on.

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/checks/delta"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
)

// ResolveIncidentTarget resolves an incident's object reference to
// the workload the bundle should be about: the reference's topmost
// observed owning workload (Pod → ReplicaSet → Deployment), falling
// back to controllerRef ("Kind/name", the inject payload's
// context.controller_ref) when the object itself is already gone.
// This is exactly the --incident resolution `lookout bundle` runs;
// the sentinel calls it with the Signal's object fields (§7.6).
func ResolveIncidentTarget(cluster *state.Cluster, ref emit.WorkloadRef, controllerRef string) (emit.WorkloadRef, error) {
	return resolveTarget(cluster, seed{wl: ref, fromIncident: true, controllerRef: controllerRef})
}

// RadiusFindings renders the §6.4 blast-radius neighborhood of id as
// the bundle's radius section findings. It only reads the snapshot,
// so it serves both the CLI bundle's one-shot graph and the
// sentinel's live topology snapshot (§4.3 surface 3).
func RadiusFindings(snap *graph.Snapshot, id graph.NodeID, depth int) []emit.Finding {
	return radiusFindings(snap, id, depth)
}

// DeltaObjectsFor scopes the delta derivations to one workload: its
// pods plus the workload object itself (any of the apps/batch kinds
// the bundle targets; other types contribute only the pods).
// Cluster-scoped delta classes (nodes, PDBs, quotas, system add-ons)
// belong to `triage delta` and `health`, not to a single-workload
// bundle — the same scoping rule the CLI bundle applies.
func DeltaObjectsFor(obj any, pods []*corev1.Pod) delta.Objects {
	return deltaObjectsFor(obj, pods)
}
