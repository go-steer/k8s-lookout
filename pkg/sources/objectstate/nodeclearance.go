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

package objectstate

import (
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// NodeClearance is the node-scoped clearance predicate of §7.4,
// closing the gap the M2 node-failure drill exposed (docs/milestones/
// M2.md §Observations item 2): the object-state source emits
// objectstate.node_notready — and the k8s-events source the reactive
// NodeNotReady family — but only pods were judgeable, so the node's
// own incident never resolved and a node-anchored storm could never
// emit its aggregate kind=resolved.
//
// For Node-scoped incidents it answers "is the symptom absent?" from
// mirrored node state, exactly the PodClearance model:
//
//   - live node: cleared when the Ready condition is True; the
//     vouched-stable-since instant is the condition's last transition
//     time, so a NotReady blip between tracker ticks — visible as a
//     forward jump of StableSince — restarts the stability window
//     even if the node reads Ready at every tick. This also judges
//     node_flapping honestly: every flap forwards the transition
//     time, so "Ready and stable for the window" means the flapping
//     actually settled.
//   - deleted node, same-name replacement exists (recreated node —
//     new UID): judged against the replacement, resolution=recovered
//     when it is Ready. The incident is about the capacity behind the
//     name, and that capacity is healthy again.
//   - deleted node, no replacement: cleared with
//     resolution=object_deleted — the node left the cluster; the
//     incident closes, explicitly distinguishable from a fix (§9.3).
//
// Like PodClearance the state machine is informer-agnostic: it is fed
// Upsert/Delete by the object-state source's node informer (the
// source already watches nodes for the transition signals — §7.4:
// each source that can observe a symptom can observe its absence).
type NodeClearance struct {
	mu sync.Mutex
	// synced reports whether the feeding informer completed its
	// initial list; until then Clearance declines to judge.
	synced func() bool
	// nodes mirrors the recovery-relevant slice of live node status,
	// keyed by UID (the incident key).
	nodes map[types.UID]*nodeClearState
	// byName indexes live nodes by name — the fallback for judging an
	// incident whose node was deleted and recreated under the same
	// name (a new UID) while the incident was open.
	byName map[string]types.UID
	// tombstones remembers recently deleted nodes so gone-node
	// incidents carry a deletion instant for the stability window.
	// TTL-swept on the rare delete path, like PodClearance.
	tombstones map[types.UID]nodeTombstone

	// now overrides time.Now for testing. nil = real clock.
	now func() time.Time
}

// nodeClearState is the minimal live-node status the predicate needs.
type nodeClearState struct {
	name  string
	ready bool
	// readySince is the Ready condition's LastTransitionTime.
	// Meaningful when ready.
	readySince time.Time
}

type nodeTombstone struct {
	name      string
	deletedAt time.Time
}

// NewNodeClearance returns an empty node clearance state machine.
// Callers wire an informer to Upsert/Delete and hand its HasSynced to
// SetSynced.
func NewNodeClearance() *NodeClearance {
	return &NodeClearance{
		nodes:      make(map[types.UID]*nodeClearState),
		byName:     make(map[string]types.UID),
		tombstones: make(map[types.UID]nodeTombstone),
	}
}

// SetSynced installs the feeding informer's HasSynced check. Until it
// reports true, Clearance answers ok=false ("cannot judge yet") so
// the tracker never resolves an incident against an empty cache.
func (o *NodeClearance) SetSynced(fn func() bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.synced = fn
}

func (o *NodeClearance) clock() time.Time {
	if o.now != nil {
		return o.now()
	}
	return time.Now()
}

// Upsert records/refreshes a live node's recovery-relevant state.
func (o *NodeClearance) Upsert(n *corev1.Node) {
	ready, since := nodeReadiness(n)

	o.mu.Lock()
	defer o.mu.Unlock()
	o.nodes[n.UID] = &nodeClearState{name: n.Name, ready: ready, readySince: since}
	o.byName[n.Name] = n.UID
	delete(o.tombstones, n.UID) // resurrection (informer replay) beats tombstone
}

// Delete moves a node to the tombstone map and prunes expired
// tombstones.
func (o *NodeClearance) Delete(n *corev1.Node) {
	now := o.clock()
	o.mu.Lock()
	defer o.mu.Unlock()
	if prev, ok := o.nodes[n.UID]; ok {
		if o.byName[prev.name] == n.UID {
			delete(o.byName, prev.name)
		}
		delete(o.nodes, n.UID)
	}
	o.tombstones[n.UID] = nodeTombstone{name: n.Name, deletedAt: now}
	for uid, ts := range o.tombstones {
		if now.Sub(ts.deletedAt) > tombstoneTTL {
			delete(o.tombstones, uid)
		}
	}
}

// Clearance implements engine.ClearanceObserver for Node-scoped
// incidents (objectstate.node_notready, the NodeNotReady reactive
// family, node_flapping). ok=false when the incident isn't
// Node-scoped or the feeding informer hasn't synced yet.
func (o *NodeClearance) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	if !strings.EqualFold(inc.Ref.KindOfObject, "Node") {
		return engine.Clearance{}, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.synced == nil || !o.synced() {
		return engine.Clearance{}, false
	}
	uid := types.UID(inc.Key.UID)

	// Live node: Ready == symptom absent; readySince carries the
	// blip-restart vouching (see type comment).
	if ns, ok := o.nodes[uid]; ok {
		return engine.Clearance{
			Cleared:     ns.ready,
			StableSince: ns.readySince,
			Resolution:  engine.ResolutionRecovered,
		}, true
	}

	// Node gone. A same-name replacement (recreated node, fresh UID)
	// counts as the capacity the incident is about.
	name := inc.Ref.Name
	deletedAt := time.Time{}
	if ts, ok := o.tombstones[uid]; ok {
		if ts.name != "" {
			name = ts.name
		}
		deletedAt = ts.deletedAt
	}
	if repUID, ok := o.byName[name]; ok {
		if rep := o.nodes[repUID]; rep != nil {
			return engine.Clearance{
				Cleared:     rep.ready,
				StableSince: rep.readySince,
				Resolution:  engine.ResolutionRecovered,
			}, true
		}
	}

	// No replacement: the node left the cluster. Closed as
	// object_deleted after the window (a replacement appearing
	// mid-window flips the verdict above before anything is emitted).
	return engine.Clearance{
		Cleared:     true,
		StableSince: deletedAt,
		Resolution:  engine.ResolutionObjectDeleted,
	}, true
}

// nodeReadiness returns whether the Ready condition is True and its
// last transition time. Unknown counts as NOT ready, matching
// nodeReady (the node controller losing contact IS the symptom).
func nodeReadiness(n *corev1.Node) (bool, time.Time) {
	for _, cond := range n.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue, cond.LastTransitionTime.Time
		}
	}
	return false, time.Time{}
}
