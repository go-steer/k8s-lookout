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

// NodeID is the compact, interned identity of a graph node
// (DESIGN.md §6.1). IDs are assigned once per distinct
// (kind, namespace, name) identity and are stable for the lifetime of
// the Graph — an object that is deleted and re-created gets the same
// NodeID back. Names live in the interner; queries return NodeIDs and
// callers resolve them via Snapshot.Resolve.
type NodeID uint32

// NoNode is the zero NodeID; it never identifies a real node.
const NoNode NodeID = 0

// NodeKind is the typed kind of a graph node. v1 covers the pod-nexus
// kinds from core + apps + batch + discovery + storage +
// networking.k8s.io (§15 Q6); Gateway API and mesh CRDs widen the
// northbound layer later, detected via discovery.
type NodeKind uint8

const (
	KindUnknown NodeKind = iota
	KindNamespace
	KindNode
	KindZone
	KindPod
	KindContainer
	KindService
	KindEndpointSlice
	KindIngress
	KindNetworkPolicy
	KindConfigMap
	KindSecret
	KindPersistentVolumeClaim
	KindDeployment
	KindReplicaSet
	KindStatefulSet
	KindDaemonSet
	KindJob
	KindCronJob

	// numNodeKinds bounds kind-indexed lookup tables.
	numNodeKinds
)

// kindNames doubles as the wire spelling inside interner keys — keep
// entries stable; they are also what Resolve/String expose to output
// surfaces.
var kindNames = [numNodeKinds]string{
	KindUnknown:               "Unknown",
	KindNamespace:             "Namespace",
	KindNode:                  "Node",
	KindZone:                  "Zone",
	KindPod:                   "Pod",
	KindContainer:             "Container",
	KindService:               "Service",
	KindEndpointSlice:         "EndpointSlice",
	KindIngress:               "Ingress",
	KindNetworkPolicy:         "NetworkPolicy",
	KindConfigMap:             "ConfigMap",
	KindSecret:                "Secret",
	KindPersistentVolumeClaim: "PersistentVolumeClaim",
	KindDeployment:            "Deployment",
	KindReplicaSet:            "ReplicaSet",
	KindStatefulSet:           "StatefulSet",
	KindDaemonSet:             "DaemonSet",
	KindJob:                   "Job",
	KindCronJob:               "CronJob",
}

func (k NodeKind) String() string {
	if k < numNodeKinds {
		return kindNames[k]
	}
	return "Unknown"
}

// EdgeKind is the typed kind of a directed edge (DESIGN.md §6.1).
type EdgeKind uint8

const (
	EdgeInvalid EdgeKind = iota
	// EdgeRoutesTo: traffic path — Ingress→Service,
	// Service→EndpointSlice, EndpointSlice→Pod.
	EdgeRoutesTo
	// EdgeSelects: label-selector match — Service→Pod.
	EdgeSelects
	// EdgeGoverns: policy scope — NetworkPolicy→Pod.
	EdgeGoverns
	// EdgeRunsOn: placement — Pod→Node, Node→Zone.
	EdgeRunsOn
	// EdgeContains: composition — Pod→Container.
	EdgeContains
	// EdgeMounts: config/storage dependency — Pod→ConfigMap,
	// Pod→Secret, Pod→PersistentVolumeClaim. Only the *name* of the
	// mounted object is ever recorded; the graph holds no secret
	// payloads (§6.5).
	EdgeMounts
	// EdgeOwns: ownerReferences — owner→owned (Deployment→ReplicaSet,
	// ReplicaSet→Pod, …). Derived from the owned object's metadata.
	EdgeOwns

	numEdgeKinds
)

var edgeKindNames = [numEdgeKinds]string{
	EdgeInvalid:  "Invalid",
	EdgeRoutesTo: "RoutesTo",
	EdgeSelects:  "Selects",
	EdgeGoverns:  "Governs",
	EdgeRunsOn:   "RunsOn",
	EdgeContains: "Contains",
	EdgeMounts:   "Mounts",
	EdgeOwns:     "Owns",
}

func (k EdgeKind) String() string {
	if k < numEdgeKinds {
		return edgeKindNames[k]
	}
	return "Invalid"
}

// Edge is one directed adjacency entry. For Snapshot.Out the edge
// reads "this node --Kind--> To"; for Snapshot.In it reads
// "To --Kind--> this node" (To is the source).
type Edge struct {
	To   NodeID
	Kind EdgeKind
}

// Ref is a resolved node: NodeID plus the human identity behind it.
// Observed reports whether the backing object has actually been seen
// from the API server: false means the node exists only because
// something references it (e.g. a Pod mounting a ConfigMap that does
// not exist, or an ownerReference to an object not yet synced) —
// which is itself triage signal.
type Ref struct {
	ID        NodeID
	Kind      NodeKind
	Namespace string // empty for cluster-scoped kinds
	Name      string
	Observed  bool
}

// nodeInfo is the per-node record inside a Snapshot. The identity
// string lives in the interner (indexed by NodeID); ns points at the
// node's Namespace node (NoNode for cluster-scoped kinds) so
// namespace-ancestor queries need no string handling.
type nodeInfo struct {
	kind     NodeKind
	observed bool
	ns       NodeID
}
