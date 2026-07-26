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

// §6.6 replay: the Effect blob inside a ChangeRecord is the delta's
// graph mutations recorded at the primitive level — node created /
// observed / unobserved / collected, edge added / removed — exactly
// as the single writer performed them. Replaying effects in order on
// top of a restored snapshot reproduces the live graph BY
// CONSTRUCTION (there is no re-derivation to drift: selector
// matching, owner-chain and mount extraction all happened once, in
// the writer), which is what makes the §13 round-trip invariant —
// snapshot + replayed deltas ≡ live graph at the same generation —
// testable as deep equality, NodeIDs included.
//
// A FieldChanges summary alone cannot serve this purpose: it names
// what changed (images, hashes, counts) but deliberately does not
// carry the edge topology (§6.5 posture: the log is names/hashes/
// counts). The Effect blob is the replay half; FieldChanges is the
// `triage changes` half; one record carries both.
//
// Effect wire format: 1 version byte, uvarint entry count, then
// entries:
//
//	1 nodeNew:    id, kind byte, key string   (interner append when
//	                                           id == len(keys)+1)
//	2 observe:    id, kind byte
//	3 unobserve:  id
//	4 gc:         id
//	5 edgeAdd:    from, to, edge-kind byte
//	6 edgeRemove: from, to, edge-kind byte

import (
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// effectVersion versions the Effect encoding independently of the
// snapshot format.
const effectVersion = 1

// ErrBadEffect is wrapped by every Replayer.Apply decode/consistency
// failure.
var ErrBadEffect = errors.New("graph: bad change effect")

const (
	effNodeNew byte = iota + 1
	effObserve
	effUnobserve
	effGC
	effEdgeAdd
	effEdgeRemove
)

// effectLog accumulates one delta's primitive mutations in the
// writer. Armed (non-nil on the Writer) only while an OnChange-
// instrumented applyDelta runs.
type effectLog struct {
	buf []byte
	n   uint64
}

func (l *effectLog) nodeNew(id NodeID, kind NodeKind, key string) {
	l.n++
	l.buf = append(l.buf, effNodeNew)
	l.buf = binary.AppendUvarint(l.buf, uint64(id))
	l.buf = append(l.buf, byte(kind))
	l.buf = appendString(l.buf, key)
}

func (l *effectLog) observe(id NodeID, kind NodeKind) {
	l.n++
	l.buf = append(l.buf, effObserve)
	l.buf = binary.AppendUvarint(l.buf, uint64(id))
	l.buf = append(l.buf, byte(kind))
}

func (l *effectLog) unobserve(id NodeID) {
	l.n++
	l.buf = append(l.buf, effUnobserve)
	l.buf = binary.AppendUvarint(l.buf, uint64(id))
}

func (l *effectLog) gc(id NodeID) {
	l.n++
	l.buf = append(l.buf, effGC)
	l.buf = binary.AppendUvarint(l.buf, uint64(id))
}

func (l *effectLog) edge(op byte, e halfEdge) {
	l.n++
	l.buf = append(l.buf, op)
	l.buf = binary.AppendUvarint(l.buf, uint64(e.From))
	l.buf = binary.AppendUvarint(l.buf, uint64(e.To))
	l.buf = append(l.buf, byte(e.Kind))
}

// encode returns the versioned blob (nil when nothing mutated — a
// delta that changed no topology, e.g. a status-only pod update).
func (l *effectLog) encode() []byte {
	if l.n == 0 {
		return nil
	}
	out := make([]byte, 0, len(l.buf)+8)
	out = append(out, effectVersion)
	out = binary.AppendUvarint(out, l.n)
	return append(out, l.buf...)
}

// Replayer rebuilds point-in-time topology: seed it with a restored
// snapshot, Apply the ChangeRecord effects logged after that
// snapshot in order, then take the result with Snapshot. Single
// goroutine use; the Replayer must not be touched after Snapshot is
// called (the returned snapshot owns the maps).
type Replayer struct {
	nodes map[NodeID]nodeInfo
	out   map[NodeID][]Edge
	in    map[NodeID][]Edge
	edges int
	keys  []string
	ids   map[string]NodeID
	// watched carries the base snapshot's watched-kind set through
	// replay unchanged: effects mutate topology, never the ingest's
	// honesty declaration.
	watched map[NodeKind]bool
	gen     uint64
	done    bool
}

// NewReplayer starts replay from base (typically graph.Restore of a
// stored snapshot). The base snapshot is not modified.
func NewReplayer(base *Snapshot) *Replayer {
	r := &Replayer{
		nodes:   maps.Clone(base.nodes),
		out:     maps.Clone(base.out),
		in:      maps.Clone(base.in),
		edges:   base.edges,
		keys:    slices.Clone(base.keys),
		ids:     make(map[string]NodeID, len(base.keys)),
		watched: base.watched,
		gen:     base.generation,
	}
	for i, k := range r.keys {
		r.ids[k] = NodeID(i + 1) // #nosec G115 -- bounded by interner invariant
	}
	return r
}

// Apply replays one ChangeRecord's effect. generation is the
// record's Generation and becomes the replayed snapshot's; records
// must be applied in generation-then-log order (the order the store
// returns them). A nil/empty effect is a no-op with the generation
// still advanced.
func (r *Replayer) Apply(generation uint64, effect []byte) error {
	if r.done {
		return errors.New("graph: Replayer used after Snapshot")
	}
	if generation >= r.gen {
		r.gen = generation
	}
	if len(effect) == 0 {
		return nil
	}
	if effect[0] != effectVersion {
		return fmt.Errorf("%w: version %d (this binary reads %d)", ErrBadEffect, effect[0], effectVersion)
	}
	d := &decoder{buf: effect[1:]}
	n := d.uvarint()
	for range n {
		op := d.byte()
		if d.err != nil {
			break
		}
		var err error
		switch op {
		case effNodeNew:
			err = r.nodeNew(d)
		case effObserve:
			err = r.observe(d)
		case effUnobserve:
			err = r.unobserve(d)
		case effGC:
			err = r.gc(d)
		case effEdgeAdd, effEdgeRemove:
			err = r.edge(op, d)
		default:
			err = fmt.Errorf("unknown entry op %d", op)
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrBadEffect, err)
		}
		if d.err != nil {
			break
		}
	}
	if d.err != nil {
		return fmt.Errorf("%w: %v", ErrBadEffect, d.err)
	}
	if len(d.buf) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrBadEffect, len(d.buf))
	}
	return nil
}

func (r *Replayer) nodeNew(d *decoder) error {
	id := d.id()
	kind := NodeKind(d.byte())
	key := d.str()
	if d.err != nil {
		return d.err
	}
	if kind >= numNodeKinds {
		return fmt.Errorf("nodeNew %d: unknown kind %d", id, kind)
	}
	switch {
	case uint64(id) == uint64(len(r.keys))+1:
		// Identity interned after the base snapshot: extend exactly
		// like the live interner did, so NodeIDs stay identical.
		r.keys = append(r.keys, key)
		r.ids[key] = id
	case id == NoNode || uint64(id) > uint64(len(r.keys)):
		return fmt.Errorf("nodeNew %d: id outside interner (have %d identities)", id, len(r.keys))
	case r.keys[id-1] != key:
		return fmt.Errorf("nodeNew %d: key %q does not match interned %q", id, key, r.keys[id-1])
	}
	if _, ok := r.nodes[id]; ok {
		return nil // already present (idempotent, mirrors writer.node)
	}
	var nsID NodeID
	if ns, _ := splitKey(key); ns != "" {
		ok := false
		if nsID, ok = r.ids[nodeKey(KindNamespace, "", ns)]; !ok {
			// The writer creates the Namespace node before its child
			// (writer.node recursion), so its entry always precedes.
			return fmt.Errorf("nodeNew %d: namespace %q not interned yet", id, ns)
		}
	}
	r.nodes[id] = nodeInfo{kind: kind, observed: kind == KindNamespace, ns: nsID}
	return nil
}

func (r *Replayer) observe(d *decoder) error {
	id := d.id()
	kind := NodeKind(d.byte())
	if d.err != nil {
		return d.err
	}
	info, ok := r.nodes[id]
	if !ok {
		return fmt.Errorf("observe %d: unknown node", id)
	}
	if kind >= numNodeKinds {
		return fmt.Errorf("observe %d: unknown kind %d", id, kind)
	}
	info.kind = kind
	info.observed = true
	r.nodes[id] = info
	return nil
}

func (r *Replayer) unobserve(d *decoder) error {
	id := d.id()
	if d.err != nil {
		return d.err
	}
	info, ok := r.nodes[id]
	if !ok {
		return fmt.Errorf("unobserve %d: unknown node", id)
	}
	info.observed = false
	r.nodes[id] = info
	return nil
}

func (r *Replayer) gc(d *decoder) error {
	id := d.id()
	if d.err != nil {
		return d.err
	}
	if _, ok := r.nodes[id]; !ok {
		return fmt.Errorf("gc %d: unknown node", id)
	}
	delete(r.nodes, id)
	return nil
}

func (r *Replayer) edge(op byte, d *decoder) error {
	from := d.id()
	to := d.id()
	kind := EdgeKind(d.byte())
	if d.err != nil {
		return d.err
	}
	if kind == EdgeInvalid || kind >= numEdgeKinds {
		return fmt.Errorf("edge %d→%d: unknown kind %d", from, to, kind)
	}
	if op == effEdgeAdd {
		out, added := insertEdge(r.out[from], Edge{To: to, Kind: kind})
		if !added {
			return nil
		}
		r.out[from] = out
		in, _ := insertEdge(r.in[to], Edge{To: from, Kind: kind})
		r.in[to] = in
		r.edges++
		return nil
	}
	out, removed := deleteEdge(r.out[from], Edge{To: to, Kind: kind})
	if !removed {
		return nil
	}
	storeEdges(r.out, from, out)
	in, _ := deleteEdge(r.in[to], Edge{To: from, Kind: kind})
	storeEdges(r.in, to, in)
	r.edges--
	return nil
}

// Snapshot finalizes replay into an immutable Snapshot whose
// Generation is the last applied record's. The Replayer is dead
// afterwards.
func (r *Replayer) Snapshot() *Snapshot {
	r.done = true
	ids := &sync.Map{}
	for i, k := range r.keys {
		ids.Store(k, NodeID(i+1)) // #nosec G115 -- bounded by interner invariant
	}
	return &Snapshot{
		nodes:      r.nodes,
		out:        r.out,
		in:         r.in,
		edges:      r.edges,
		keys:       r.keys,
		ids:        ids,
		watched:    r.watched,
		generation: r.gen,
	}
}
