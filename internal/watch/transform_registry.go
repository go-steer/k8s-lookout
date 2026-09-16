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

// The shared-transform field registry.
//
// A transform on the shared informer factory is process-wide state. It mutates
// the objects every consumer sees, and downstream a stripped field is
// indistinguishable from a field that was genuinely empty — so the source that
// needed it does not error, it silently stops finding things. The failure mode
// is a detector that never fires again, discovered by a user, months later.
//
// This registry is the control for that. Every field the transform touches has
// an entry saying why, and every field it deliberately preserves has an entry
// naming the informer-reachable reader that requires it, with the file:line
// where that read happens. TestSharedTransform_StripsExactlyTheRegistry runs
// the transform over a fully-populated Pod and Node and compares the fields
// that actually changed against this table, so the two cannot drift: adding a
// strip without an entry fails, and an entry describing something the code no
// longer does fails too.
//
// Adding a field to a strip list therefore costs one entry and one sentence of
// justification. That is the intended price.
//
// "Informer-reachable" is the test for whether a reader counts. Read-path code
// that lists from the API server directly is unaffected by the transform. The
// enricher is the trap: internal/watch/enrich.go holds a livePod that reads
// straight from the shared pod lister (enrich.go:129-131) and hands the result
// to pkg/checks/state, pkg/checks/delta and pkg/checks/logs (enrich.go:77-79),
// so all three are informer-reachable even though they look like read-path
// check packages.

// fieldAction is what the shared transform does to a field.
type fieldAction int

const (
	// fieldStripped means the transform zeroes, nils or filters this field.
	fieldStripped fieldAction = iota
	// fieldPreserved means the transform deliberately leaves it intact because
	// a named consumer reads it. These are the entries that make the registry
	// worth having: they are the fields a future strip would break.
	fieldPreserved
)

// fieldEntry documents one field the shared transform has an opinion about.
type fieldEntry struct {
	// Kind is "Pod" or "Node".
	Kind string
	// Path is the JSON path with slice indices collapsed to "[]", e.g.
	// "spec.containers[].env[].value". It must match what the reflective diff
	// in transform_test.go produces.
	Path string
	// Action is whether the transform strips or preserves this field.
	Action fieldAction
	// Why justifies the action in one sentence.
	Why string
	// Readers lists informer-reachable readers as "file:line — what it reads".
	// Required for fieldPreserved (a field preserved for nobody should be
	// stripped) and required to be empty for fieldStripped (the claim being
	// made is precisely that nothing reads it).
	Readers []string
}

// sharedTransformRegistry is the single source of truth for the shared
// transform's effect on other consumers.
var sharedTransformRegistry = []fieldEntry{
	// ---------------------------------------------------------------- Pod: stripped
	{
		Kind:   "Pod",
		Path:   "metadata.managedFields",
		Action: fieldStripped,
		Why:    "Server-side-apply bookkeeping. Large, high-churn, and nothing in the repo reads it.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers[].env[].value",
		Action: fieldStripped,
		Why: "The literal resolved value of an environment variable, which is where " +
			"secret material lands in a Pod. This is the field the transform exists " +
			"for: spike S9 checked every informer-reachable reader and not one of " +
			"them reads Value — they read Name, ValueFrom, or both.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers[].command",
		Action: fieldStripped,
		Why:    "Not read by any consumer, and a common place for credentials passed as flags.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers[].args",
		Action: fieldStripped,
		Why:    "Not read by any consumer, and a common place for credentials passed as flags.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers[].lifecycle",
		Action: fieldStripped,
		Why:    "Lifecycle hooks are not read by any consumer and can embed command lines.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers[].livenessProbe",
		Action: fieldStripped,
		Why:    "Probe definitions are not read by any consumer; probe RESULTS are read, and those live in status.containerStatuses, which is preserved.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers[].readinessProbe",
		Action: fieldStripped,
		Why:    "Probe definitions are not read by any consumer; probe RESULTS are read, and those live in status.containerStatuses, which is preserved.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers[].startupProbe",
		Action: fieldStripped,
		Why:    "Probe definitions are not read by any consumer; probe RESULTS are read, and those live in status.containerStatuses, which is preserved.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.initContainers[].env[].value",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].env[].value; init containers carry secrets just as readily.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.initContainers[].command",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].command.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.initContainers[].args",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].args.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.initContainers[].lifecycle",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].lifecycle.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.initContainers[].livenessProbe",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].livenessProbe.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.initContainers[].readinessProbe",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].readinessProbe.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.initContainers[].startupProbe",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].startupProbe.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.ephemeralContainers[].env[].value",
		Action: fieldStripped,
		Why: "An ephemeral container has the same shape as a regular one, so " +
			"`kubectl debug --env` writes literal secret values straight into the " +
			"cache. The leeway design's S9 table did not cover ephemeral " +
			"containers; stripping them closes the hole the other two strips leave.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.ephemeralContainers[].command",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].command.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.ephemeralContainers[].args",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].args.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.ephemeralContainers[].lifecycle",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].lifecycle.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.ephemeralContainers[].livenessProbe",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].livenessProbe.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.ephemeralContainers[].readinessProbe",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].readinessProbe.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.ephemeralContainers[].startupProbe",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers[].startupProbe.",
	},

	// --------------------------------------------------------------- Pod: preserved
	{
		Kind:   "Pod",
		Path:   "spec.containers[].env[].name",
		Action: fieldPreserved,
		Why:    "The variable NAME is how two checks identify a binding without ever seeing its value.",
		Readers: []string{
			"pkg/checks/state/edges_checks.go:391 — pairs Name with ValueFrom to build config/secret edges",
			"pkg/checks/state/wi.go:330 — reads Name alone for the Workload Identity check",
		},
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers[].env[].valueFrom",
		Action: fieldPreserved,
		Why: "The REFERENCE to a ConfigMap or Secret. This is how pkg/graph builds " +
			"config and secret edges, and stripping it empties the graph silently " +
			"because a nil slice is a valid empty slice.",
		Readers: []string{
			"pkg/graph/derive.go:160 — ConfigMapKeyRef/SecretKeyRef to derive edges",
			"pkg/graph/changes.go:273 — same, on the change path",
			"pkg/checks/state/edges_checks.go:391 — dangling-reference edge checks",
		},
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers[].envFrom",
		Action: fieldPreserved,
		Why:    "Bulk ConfigMap/Secret references, the other half of the graph's edge derivation.",
		Readers: []string{
			"pkg/graph/derive.go:169 — ConfigMapRef/SecretRef",
			"pkg/graph/changes.go:282 — same, on the change path",
			"pkg/checks/state/edges_checks.go:404 — dangling-reference edge checks",
		},
	},
	{
		Kind:   "Pod",
		Path:   "spec.ephemeralContainers[].name",
		Action: fieldPreserved,
		Why: "pkg/checks/logs enumerates container names to decide which logs it can " +
			"fetch, and it is informer-reachable through the enricher. This is why " +
			"the ephemeral-container strip nulls the common fields individually " +
			"rather than dropping the slice.",
		Readers: []string{
			"pkg/checks/logs/fetch.go:183 — enumerates ephemeral container names",
		},
	},
	{
		Kind:   "Pod",
		Path:   "status.containerStatuses",
		Action: fieldPreserved,
		Why:    "Live container state. Four informer-reachable consumers read it; it is the largest preserved item and the main reason a trimmed pod is well above the 1.5 KiB the standalone design assumed.",
		Readers: []string{
			"pkg/sources/objectstate/podclearance.go:349 — pod clearance",
			"pkg/sources/rollout/rollout.go:501 — rollout progress",
			"pkg/checks/delta/pods.go:98 — per-container delta checks",
			"pkg/checks/inventory/render.go:77 — inventory rendering",
		},
	},
	{
		Kind:   "Pod",
		Path:   "status.initContainerStatuses",
		Action: fieldPreserved,
		Why:    "Same as status.containerStatuses, for init containers.",
		Readers: []string{
			"pkg/sources/rollout/rollout.go:501 — rollout progress",
			"pkg/checks/delta/pods.go:95 — per-container delta checks",
		},
	},
	{
		Kind:   "Pod",
		Path:   "status.phase",
		Action: fieldPreserved,
		Why: "objectstate's eviction-burst detector reads Phase==Failed. This entry " +
			"also guards the §6.1 field selector, which excludes Succeeded " +
			"server-side but must NOT exclude Failed — spike S9 caught that as a " +
			"proposed change that would have deleted eviction detection with no error.",
		Readers: []string{
			"pkg/sources/objectstate/objectstate.go:1220 — podEvicted() reads Phase==Failed && Reason==Evicted",
		},
	},

	// --------------------------------------------------------------- Node: stripped
	{
		Kind:   "Node",
		Path:   "metadata.managedFields",
		Action: fieldStripped,
		Why:    "Server-side-apply bookkeeping. Nothing reads it.",
	},
	{
		Kind:   "Node",
		Path:   "metadata.annotations",
		Action: fieldStripped,
		Why: "Filtered, not dropped: reduced to retainedNodeAnnotations. Node " +
			"annotations are unbounded and carry large cloud-provider blobs. Any " +
			"annotation a source depends on must be added to that set — see the " +
			"metadata.annotations[ccc_priority_index] entry below.",
	},
	{
		Kind:   "Node",
		Path:   "status.images",
		Action: fieldStripped,
		Why:    "The single largest node-side line item at 10-40 KiB per node — roughly 2 GiB at 50k nodes — and nothing in the repo reads it.",
	},
	{
		Kind:   "Node",
		Path:   "status.volumesInUse",
		Action: fieldStripped,
		Why:    "Attach/detach bookkeeping, unbounded in cluster size, not read.",
	},
	{
		Kind:   "Node",
		Path:   "status.volumesAttached",
		Action: fieldStripped,
		Why:    "Attach/detach bookkeeping, unbounded in cluster size, not read.",
	},
	{
		Kind:   "Node",
		Path:   "status.nodeInfo",
		Action: fieldStripped,
		Why:    "Kernel, OS, container-runtime and kubelet version strings. Not read on the watch path.",
	},
	{
		Kind:   "Node",
		Path:   "status.conditions",
		Action: fieldStripped,
		Why:    "Filtered, not dropped: reduced to the Ready condition. The others (MemoryPressure, DiskPressure, PIDPressure, NetworkUnavailable) are not read on the watch path.",
	},

	// -------------------------------------------------------------- Node: preserved
	{
		Kind:   "Node",
		Path:   "metadata.annotations[ccc_priority_index]",
		Action: fieldPreserved,
		Why: "GKE records the provisioned compute-class priority here. It is " +
			"unprefixed, undocumented and GKE-internal, so it looks exactly like " +
			"the provider noise the annotation filter exists to drop — dropping it " +
			"would silently disable preference-rank tracking with no error anywhere.",
		Readers: []string{
			"pkg/sources/compute-class — rank resolution, docs/leeway-design.md §7.7.2",
		},
	},
	{
		Kind:   "Node",
		Path:   "status.conditions[Ready]",
		Action: fieldPreserved,
		Why:    "Node readiness gates whether a domain counts as available; the condition filter keeps exactly this one.",
		Readers: []string{
			"pkg/sources/topology-drift — domain inventory, docs/leeway-design.md §6.1",
		},
	},
	{
		Kind:   "Node",
		Path:   "status.allocatable",
		Action: fieldPreserved,
		Why:    "Capacity weighting for node-group subjects.",
		Readers: []string{
			"pkg/sources/topology-drift — capacity weighting, docs/leeway-design.md §6.1",
		},
	},
	{
		Kind:   "Node",
		Path:   "spec.taints",
		Action: fieldPreserved,
		Why:    "Eligibility: a node whose taints a pod does not tolerate is not an eligible domain, and GKE auto-injects compute-class and GPU taints that make this non-obvious.",
		Readers: []string{
			"pkg/sources/topology-drift — eligible-domain computation, docs/leeway-design.md §7.1",
		},
	},
}
