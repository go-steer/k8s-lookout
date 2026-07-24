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

import (
	"math"
	"strings"
	"sync"
)

// interner maps identity keys ("Kind/namespace/name") to NodeIDs and
// back. It is append-only: a key, once assigned an ID, keeps it for
// the lifetime of the Graph (delete + re-create returns the same ID;
// key→ID pairs are never removed or reassigned).
//
// Concurrency model, per the COW discipline (§6.2):
//
//   - intern is called by the single writer only, under the writer
//     mutex.
//   - ids is a sync.Map so reader-side Lookup is lock-free; because
//     the map is append-only, a reader can at worst see a key that is
//     *newer* than its snapshot — Snapshot.Lookup filters that by
//     checking membership in the snapshot's node set.
//   - keys is append-only; each Snapshot captures the slice header at
//     swap time. A concurrent append never writes an index below a
//     published snapshot's length, so reads within the snapshot's
//     bounds are race-free even when the writer grows the slice.
type interner struct {
	ids  sync.Map // string → NodeID
	keys []string // NodeID-1 → key; writer-owned, append-only
}

// intern returns the NodeID for key, assigning the next ID on first
// sight. Writer-only.
func (in *interner) intern(key string) NodeID {
	if v, ok := in.ids.Load(key); ok {
		return v.(NodeID)
	}
	if len(in.keys) >= math.MaxUint32-1 {
		// uint32 identity space exhausted. Unreachable in practice
		// (4B distinct identities over one process lifetime); the
		// panic beats silent ID aliasing.
		panic("graph: interner overflow: more than 2^32-2 distinct node identities")
	}
	id := NodeID(len(in.keys) + 1) // #nosec G115 -- bounded above
	in.keys = append(in.keys, key)
	in.ids.Store(key, id)
	return id
}

// nodeKey builds the canonical identity key. Cluster-scoped kinds use
// an empty namespace segment ("Node//gke-node-1"). Container names
// embed the pod name ("Container/ns/pod-name/container-name"); the
// trailing segment is everything after the second slash.
func nodeKey(kind NodeKind, namespace, name string) string {
	var b strings.Builder
	ks := kind.String()
	b.Grow(len(ks) + len(namespace) + len(name) + 2)
	b.WriteString(ks)
	b.WriteByte('/')
	b.WriteString(namespace)
	b.WriteByte('/')
	b.WriteString(name)
	return b.String()
}

// splitKey recovers (namespace, name) from an identity key. The kind
// prefix is skipped, not parsed — the authoritative kind lives in
// nodeInfo.
func splitKey(key string) (namespace, name string) {
	i := strings.IndexByte(key, '/')
	if i < 0 {
		return "", key
	}
	rest := key[i+1:]
	j := strings.IndexByte(rest, '/')
	if j < 0 {
		return "", rest
	}
	return rest[:j], rest[j+1:]
}
