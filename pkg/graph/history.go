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

// Snapshot binary serialization (DESIGN.md §6.6): the periodic graph
// snapshots persisted by the sentinel are "compressed binary of the
// node/edge arrays". The format is deliberately dumb and versioned:
//
//	4-byte magic "LKGH" | 1 format-version byte | gzip(body)
//
// body (all integers unsigned varints, all strings varint-length
// prefixed):
//
//	generation
//	numKeys, keys[0..n)            — the interner view at swap time
//	numNodes, then per node sorted by NodeID:
//	    id, kind byte, observed byte, namespace-node id
//	numSources, then per source sorted by NodeID:
//	    id, numEdges, per edge: kind byte, to id
//
// Only the OUT adjacency is serialized; Restore rebuilds the reverse
// index. The interner keys are serialized in full (identities of
// GC'd nodes included) so NodeIDs in a restored snapshot are
// IDENTICAL to the live graph's at encode time — which is what lets
// the §6.6 delta log reference nodes by compact ID (see replay.go).
//
// Compression is stdlib compress/gzip on purpose: snapshots are cut
// every ~5 minutes (§6.6), so encoder throughput is irrelevant next
// to a new dependency.

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
)

// SnapshotFormatVersion is the current serialization format. Bump on
// any body change; Restore refuses versions it does not understand
// (a snapshot written by a newer lookout is unreadable, never
// misread).
const SnapshotFormatVersion = 1

// snapshotMagic brands the header: LooKout Graph History.
var snapshotMagic = [4]byte{'L', 'K', 'G', 'H'}

// ErrBadSnapshot is wrapped by every Restore failure: corrupt header,
// unknown version, truncated or inconsistent body.
var ErrBadSnapshot = errors.New("graph: bad snapshot encoding")

// maxRestoreStringLen bounds one decoded string so a corrupt varint
// cannot ask for a multi-GiB allocation. Identity keys are
// "Kind/namespace/name(/container)"; k8s caps each segment well below
// this.
const maxRestoreStringLen = 1 << 12

// Encode serializes the snapshot to the versioned, compressed binary
// format above. The snapshot is immutable, so Encode is safe to call
// from any goroutine at any time.
func (s *Snapshot) Encode() ([]byte, error) {
	var body []byte
	body = binary.AppendUvarint(body, s.generation)

	body = binary.AppendUvarint(body, uint64(len(s.keys)))
	for _, k := range s.keys {
		body = appendString(body, k)
	}

	nodeIDs := make([]NodeID, 0, len(s.nodes))
	for id := range s.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	body = binary.AppendUvarint(body, uint64(len(nodeIDs)))
	for _, id := range nodeIDs {
		info := s.nodes[id]
		body = binary.AppendUvarint(body, uint64(id))
		body = append(body, byte(info.kind), boolByte(info.observed))
		body = binary.AppendUvarint(body, uint64(info.ns))
	}

	srcIDs := make([]NodeID, 0, len(s.out))
	for id := range s.out {
		srcIDs = append(srcIDs, id)
	}
	sort.Slice(srcIDs, func(i, j int) bool { return srcIDs[i] < srcIDs[j] })
	body = binary.AppendUvarint(body, uint64(len(srcIDs)))
	for _, id := range srcIDs {
		edges := s.out[id]
		body = binary.AppendUvarint(body, uint64(id))
		body = binary.AppendUvarint(body, uint64(len(edges)))
		for _, e := range edges {
			body = append(body, byte(e.Kind))
			body = binary.AppendUvarint(body, uint64(e.To))
		}
	}

	var out bytes.Buffer
	out.Write(snapshotMagic[:])
	out.WriteByte(SnapshotFormatVersion)
	zw := gzip.NewWriter(&out)
	if _, err := zw.Write(body); err != nil {
		return nil, fmt.Errorf("graph: encode snapshot: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("graph: encode snapshot: %w", err)
	}
	return out.Bytes(), nil
}

// Restore decodes a snapshot produced by Encode. Every structural
// invariant is re-validated (IDs within the interner range, edge
// endpoints present, kinds known) so a corrupt or truncated blob is
// an error — never a panic, never a silently wrong graph.
func Restore(data []byte) (*Snapshot, error) {
	if len(data) < len(snapshotMagic)+1 || !bytes.Equal(data[:4], snapshotMagic[:]) {
		return nil, fmt.Errorf("%w: missing LKGH header", ErrBadSnapshot)
	}
	if v := data[4]; v != SnapshotFormatVersion {
		return nil, fmt.Errorf("%w: format version %d (this binary reads %d)", ErrBadSnapshot, v, SnapshotFormatVersion)
	}
	zr, err := gzip.NewReader(bytes.NewReader(data[5:]))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadSnapshot, err)
	}
	body, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadSnapshot, err)
	}
	if err := zr.Close(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadSnapshot, err)
	}

	d := &decoder{buf: body}
	s := &Snapshot{
		nodes: map[NodeID]nodeInfo{},
		out:   map[NodeID][]Edge{},
		in:    map[NodeID][]Edge{},
		ids:   &sync.Map{},
	}
	s.generation = d.uvarint()

	numKeys := d.uvarint()
	if numKeys > uint64(len(body)) { // each key costs >= 1 byte
		return nil, fmt.Errorf("%w: key count %d exceeds body", ErrBadSnapshot, numKeys)
	}
	s.keys = make([]string, numKeys)
	for i := range s.keys {
		s.keys[i] = d.str()
		s.ids.Store(s.keys[i], NodeID(i+1)) // #nosec G115 -- bounded by interner invariant
	}

	numNodes := d.uvarint()
	if numNodes > numKeys {
		return nil, fmt.Errorf("%w: %d nodes but only %d identities", ErrBadSnapshot, numNodes, numKeys)
	}
	for range numNodes {
		id := d.id()
		kind := NodeKind(d.byte())
		observed := d.byte()
		ns := d.id()
		if d.err != nil {
			break
		}
		if id == NoNode || uint64(id) > numKeys || kind >= numNodeKinds || observed > 1 ||
			(ns != NoNode && uint64(ns) > numKeys) {
			return nil, fmt.Errorf("%w: invalid node record (id=%d kind=%d ns=%d)", ErrBadSnapshot, id, kind, ns)
		}
		s.nodes[id] = nodeInfo{kind: kind, observed: observed == 1, ns: ns}
	}

	numSources := d.uvarint()
	for range numSources {
		src := d.id()
		n := d.uvarint()
		if d.err != nil {
			break
		}
		if _, ok := s.nodes[src]; !ok {
			return nil, fmt.Errorf("%w: edges from unknown node %d", ErrBadSnapshot, src)
		}
		if n > uint64(len(body)) {
			return nil, fmt.Errorf("%w: edge count %d exceeds body", ErrBadSnapshot, n)
		}
		edges := make([]Edge, 0, n)
		for range n {
			kind := EdgeKind(d.byte())
			to := d.id()
			if d.err != nil {
				break
			}
			if kind == EdgeInvalid || kind >= numEdgeKinds {
				return nil, fmt.Errorf("%w: invalid edge kind %d", ErrBadSnapshot, kind)
			}
			if _, ok := s.nodes[to]; !ok {
				return nil, fmt.Errorf("%w: edge to unknown node %d", ErrBadSnapshot, to)
			}
			edges = append(edges, Edge{To: to, Kind: kind})
			s.in[to] = append(s.in[to], Edge{To: src, Kind: kind})
			s.edges++
		}
		if d.err == nil {
			s.out[src] = edges
		}
	}
	if d.err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadSnapshot, d.err)
	}
	if len(d.buf) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrBadSnapshot, len(d.buf))
	}
	// The reverse index must obey the same (Kind, To) ordering the
	// writer maintains, or restored snapshots would not be
	// deep-equal to live ones.
	for id, edges := range s.in {
		sort.Slice(edges, func(i, j int) bool { return edgeLess(edges[i], edges[j]) })
		s.in[id] = edges
	}
	return s, nil
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// decoder is a tiny sticky-error reader over the decompressed body.
type decoder struct {
	buf []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.buf)
	if n <= 0 {
		d.err = errors.New("truncated varint")
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

func (d *decoder) byte() byte {
	if d.err != nil {
		return 0
	}
	if len(d.buf) == 0 {
		d.err = errors.New("truncated byte")
		return 0
	}
	b := d.buf[0]
	d.buf = d.buf[1:]
	return b
}

// id decodes a NodeID with an explicit uint32 range check so a
// corrupt varint can never silently truncate into a plausible ID.
func (d *decoder) id() NodeID {
	v := d.uvarint()
	if d.err == nil && v > math.MaxUint32 {
		d.err = fmt.Errorf("node id %d out of range", v)
		return NoNode
	}
	return NodeID(v) // #nosec G115 -- range-checked above
}

func (d *decoder) str() string {
	n := d.uvarint()
	if d.err != nil {
		return ""
	}
	if n > maxRestoreStringLen || n > uint64(len(d.buf)) || n > math.MaxInt {
		d.err = fmt.Errorf("string length %d out of range", n)
		return ""
	}
	s := string(d.buf[:n])
	d.buf = d.buf[n:]
	return s
}
