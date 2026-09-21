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

package topologydrift

import (
	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// DefaultNodeGroupLabelKeys is §10.2's node-group precedence list: the label
// keys a node's group name is read from, most specific first.
//
// Compute class outranks node pool, and that ordering is the one decision in
// this list worth arguing about (§11). With `nodePoolAutoCreation`, GKE names
// pools per machine type — `nap-n4-highmem-2-zsqafil2` — and creates and
// destroys them on demand. Keyed on the pool label, a cluster running node
// auto-provisioning would produce a stream of short-lived subjects, each of
// which learns nothing before it goes away and each of which is a distinct
// label value in every series here. The compute class is what the operator
// actually declared and it outlives the pools that serve it.
//
// The list is ordered rather than a set because a node routinely carries
// several of these at once: a NAP node has both `compute-class` and
// `gke-nodepool`. First match wins.
var DefaultNodeGroupLabelKeys = []string{
	"cloud.google.com/compute-class",
	"karpenter.sh/nodepool",
	"eks.amazonaws.com/nodegroup",
	"cloud.google.com/gke-nodepool",
	"kops.k8s.io/instancegroup",
	"agentpool",
}

// DefaultMaxNodeGroups bounds how many node-group subjects are tracked (FR-3).
//
// Two hundred is generous for a list of things an operator declared — the
// largest estates in §6.6's sizing run a few dozen pools — and that is the
// point of the bound rather than its cost. A misconfigured precedence list,
// or a provider label this package has not heard of that happens to be
// per-node, turns every node in the cluster into its own group; the bound is
// what makes that fail visibly instead of quietly exporting a series per node.
const DefaultMaxNodeGroups = 200

// nodeGroupOf returns the node's group name under the first key in the
// precedence list it carries a non-empty value for.
//
// False means the node belongs to no group leeway can name, which is the
// honest answer for an unlabelled node and a common one: control-plane nodes
// on self-managed clusters, bare-metal nodes, kind and kwok nodes. Such a node
// is left out of the group subjects entirely rather than collected under an
// "unknown" group, because the domains of a set of nodes whose only shared
// property is that nobody labelled them is not a distribution about anything.
func nodeGroupOf(labels map[string]string, keys []string) (string, bool) {
	for _, key := range keys {
		if v := labels[key]; v != "" {
			return v, true
		}
	}
	return "", false
}

// NodeGroups partitions the inventory into node groups and returns each
// group's distribution over every configured axis (FR-3).
//
// Usable nodes — Ready and schedulable — count as StateRunning and everything
// else as StateUnschedulable. That split is not cosmetic: §7.2 sums
// DomainStats.Allocatable* over usable nodes only, so an expectation is
// apportioned over the capacity that can actually take work. Counting a
// NotReady node in the actual while its capacity is absent from the
// expectation would manufacture drift out of a broken node, which is
// somebody else's finding. Only StateRunning is scored (see ScoreAxis), and
// the rest still reach lookout_leeway_domain_objects, where a pool's dead
// nodes are exactly what a reader wants next to its live ones.
//
// An empty key list returns nothing at all, which is FR-3's off switch.
func (inv *Inventory) NodeGroups(labelKeys []string) map[string]map[leeway.TopologyKey]*leeway.Distribution {
	if len(labelKeys) == 0 {
		return nil
	}

	inv.mu.RLock()
	defer inv.mu.RUnlock()

	out := make(map[string]map[leeway.TopologyKey]*leeway.Distribution)
	for _, n := range inv.nodes {
		group, ok := nodeGroupOf(n.labels, labelKeys)
		if !ok {
			continue
		}
		byKey, ok := out[group]
		if !ok {
			byKey = make(map[leeway.TopologyKey]*leeway.Distribution, len(inv.keys))
			out[group] = byKey
		}
		state := leeway.StateUnschedulable
		if n.ready && n.schedulable {
			state = leeway.StateRunning
		}
		for ordinal, key := range inv.keys {
			dist, ok := byKey[key]
			if !ok {
				dist = leeway.NewDistribution()
				byKey[key] = dist
			}
			// Never pinned. Pinning is §5.1's "this object cannot be moved
			// because a zonal volume holds it", which is a statement about a
			// pod. A node is not going anywhere either, but that is what a
			// node is rather than a constraint on it, and counting every node
			// as pinned would make the pinned share of every group 100 % and
			// say nothing.
			dist.Add(n.domain(ordinal), state, false)
		}
	}
	return out
}
