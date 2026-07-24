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

package engine

import (
	"crypto/sha256"
	"encoding/hex"
)

// Fingerprint computes the §8 signal fingerprint: a stable hash of
// the incident CLASS, deliberately NOT of the affected object. Two
// pods crash-looping in different clusters of the same zone carry the
// same fingerprint; that is the point — AX rolls up a fleet-wide
// symptom as a join on (fingerprint, cluster/project/zone) instead of
// parsing payloads, and `lookout health` merges "the sentinel paged
// on this" with "the scan still sees it" into one finding.
//
// Inputs:
//
//   - kind: the §7.3 signal kind, e.g. "k8s-event", "capacity.stockout".
//   - reasonClass: the CANONICALIZED reason — pass the raw reason
//     through CanonicalReason first so ErrImagePull and
//     ImagePullBackOff produce the same fingerprint, mirroring the
//     dedup family collapse.
//   - objectClass: the KIND of the affected object ("Pod", "Node",
//     "Deployment") — never its name or UID.
//   - zone: the failure domain, empty when unknown. Zone is in the
//     hash (not cluster) because zone-scoped causes — stockouts,
//     zonal outages — are exactly what fleet rollup must group; the
//     cluster dimension rides alongside the fingerprint in the
//     schema, not inside it.
//
// FROZEN CONTRACT — do not change without a design revision: the
// definition is
//
//	"sha256:" + hex(sha256(kind || NUL || reasonClass || NUL || objectClass || NUL || zone))
//
// with NUL (0x00) as the field separator (a byte that cannot appear
// in any input). Fingerprints are persisted in incident records and
// compared ACROSS clusters and lookout versions by AX; changing the
// encoding, separator, field order, or field set silently splits
// every fleet-wide rollup into disjoint halves during a rolling
// upgrade. The pinned vectors in fingerprint_test.go are the
// cross-cluster contract; treat a failing pin as a breaking change,
// never as a test to update.
func Fingerprint(kind, reasonClass, objectClass, zone string) string {
	h := sha256.New()
	h.Write([]byte(kind))
	h.Write([]byte{0})
	h.Write([]byte(reasonClass))
	h.Write([]byte{0})
	h.Write([]byte(objectClass))
	h.Write([]byte{0})
	h.Write([]byte(zone))
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}
