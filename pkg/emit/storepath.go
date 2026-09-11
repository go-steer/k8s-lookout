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

package emit

import (
	"path/filepath"
	"strings"
)

// StoreClusterFlag returns the --store-cluster spec, which every command
// taking a --store path registers alongside it. One definition, because
// the help text IS the contract between the sentinel's stem and the CLI's
// derived path.
//
// It is deliberately NOT the same flag as `findings diff --cluster`, and
// deliberately not implied by it. --cluster is a label written INTO the
// store (the first segment of a finding_state subject key); this one
// selects a FILE. A single-cluster sentinel — the default deployment —
// writes the literal --store path while still having a cluster name, so a
// flag that derived a path from the cluster identity would silently send
// `triage status` to a file the sentinel never reads. Naming the stem
// explicitly is the only way the caller can say which of the two layouts
// it is talking to.
func StoreClusterFlag() FlagSpec {
	return FlagSpec{Name: "store-cluster", Type: FlagString, Default: "",
		Help: "read/write the store for THIS cluster, treating --store as the multi-cluster stem the sentinel was given: --store=/var/lib/lookout/lookout.db --store-cluster=prod-us opens /var/lib/lookout/lookout-prod-us.db (issue #410). Set it only against a sentinel running --clusters/--clusters-from; a single-cluster sentinel writes the literal --store path"}
}

// StorePath resolves the --store flag against --store-cluster: the
// literal path in the single-cluster case, that cluster's file when the
// caller declared the value a fleet stem. Commands must call this rather
// than reading --store directly, so the two flags cannot drift apart.
func StorePath(v FlagValues) string {
	return PerClusterPath(v.String("store"), v.String("store-cluster"))
}

// PerClusterPath turns a state-file path into one path per cluster, so N
// runners in one process never share a file (issues #386, #410):
//
//	/var/lib/lookout/lookout.db → /var/lib/lookout/lookout-prod-us.db
//
// In multi-cluster mode the sentinel treats --store and --dedup-persist
// as STEMS and opens the derived path instead. The single-cluster
// default never calls this, so an existing deployment's file does not
// move on upgrade.
//
// It lives in emit, and is exported, because the store is not
// sentinel-private: `lookout triage status --store` WRITES §9.4 records
// that the sentinel's severity routing then reads, and `health --store`,
// `findings diff --store`, `findings ack --store`, `bundle --store` and
// the graph-history `--at --store` family read it. Those callers pass
// the same stem the sentinel was given plus --store-cluster, and must
// land on the same file — which only works if exactly one implementation
// of this rule exists. Do not re-derive it. (pkg/store imports emit, so
// emit is the lowest package both the CLI flag layer and the store can
// share.)
//
// The suffix is the cluster NAME alone, which is the fleet-wide cluster
// identity everywhere else too: the `cluster` metrics label, the frozen
// `cluster` field on the wire, the /readyz entry, finding_state.cluster,
// and every distilled fact's scope. A fleet whose discovery returns two
// clusters with one name is ambiguous in all of those before it is
// ambiguous here, which is why the sentinel skips such a pair rather
// than papering over it with a longer path.
//
// An empty base (the disabled state) or an empty cluster returns the
// base unchanged.
func PerClusterPath(base, cluster string) string {
	slug := pathSlug(cluster)
	if base == "" || slug == "" {
		return base
	}
	// Insert before the extension rather than appending, so the file
	// keeps whatever suffix the operator (and their tooling, and
	// SQLite's -wal/-shm siblings) expects.
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext) + "-" + slug + ext
}

// pathSlug reduces a cluster name to something safe to put in a
// filename. GKE cluster names are already [a-z0-9-], but a --clusters
// pair and a kubeconfig context name are both operator-supplied, and a
// name is not permitted to steer where the file lands: everything
// outside [A-Za-z0-9_-] — separators and dots included — becomes a
// single dash, so the result can only ever be one path component next
// to the stem.
func pathSlug(s string) string {
	var b strings.Builder
	dash := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
			dash = false
		case !dash && b.Len() > 0:
			b.WriteByte('-')
			dash = true
		}
	}
	return strings.TrimRight(b.String(), "-")
}
