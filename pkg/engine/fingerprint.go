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
// same fingerprint; that is the point — the fleet layer rolls up a fleet-wide
// symptom as a join on (fingerprint, cluster/project/zone) instead of
// parsing payloads, and `lookout health` merges "the sentinel paged
// on this" with "the scan still sees it" into one finding.
//
// Inputs:
//
//   - kind: the §7.3 signal kind, e.g. "k8s-event", "capacity.stockout".
//   - reasonClass: the CANONICALIZED reason — pass the raw reason
//     through CanonicalReasonForEvent (message in hand — the
//     dispatcher's push path) or CanonicalReason (messageless) first
//     so ErrImagePull and ImagePullBackOff produce the same
//     fingerprint, mirroring the dedup family collapse.
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
// compared ACROSS clusters and lookout versions by fleet consumers;
// changing the
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

// ScanFingerprint is the FROZEN scan-source mapping of the §8
// contract (docs/signal-schema-v1.md): the fingerprint a read-path
// finding carries so push (sentinel) and pull (scan) dedupe on one
// key. A point-in-time scan observes a SYMPTOM — the reactive
// incident class the sentinel's k8s-events source would push for the
// same breakage — so scan findings always fingerprint under the
// frozen reactive kind "k8s-event", never under a scan-local or
// source-namespaced kind:
//
//	ScanFingerprint(reason, objectClass, zone)
//	  = Fingerprint("k8s-event", CanonicalReason(reason), objectClass, zone)
//
// The reason passes through the MESSAGELESS CanonicalReason on
// purpose: scan findings derive their reasons from object STATUS
// (container waiting.reason is already the specific
// ImagePullBackOff / CrashLoopBackOff, never kubelet's generic
// BackOff/Failed event spellings), so the message-aware
// CanonicalReasonForEvent would change nothing here — and scan
// findings carry no event message to feed it. The push path's
// dispatcher stamp uses the message-aware variant on the same
// families, so `lookout health`'s pod.crashloop finding and the
// sentinel's CrashLoopBackOff inject carry identical fingerprints
// (the §8 merge, and the fleet-level cross-path dedup). This is the one
// recipe the §9.4 join has used since it shipped; changing it
// desynchronizes every open triage-status record.
func ScanFingerprint(reason, objectClass, zone string) string {
	return Fingerprint(KindK8sEvent, CanonicalReason(reason), objectClass, zone)
}

// PostureFingerprint is the incident-class hash for a POSTURE finding
// — the `audit` group's claim that a currently-healthy workload lacks
// a safety net (issue #182). It is a §8 ADDITION, not a change:
// ScanFingerprint keeps its exact behaviour, because it is the recipe
// every open §9.4 triage-status record was joined under.
//
//	PostureFingerprint(kind, reason, objectClass)
//	  = Fingerprint(kind, reason, objectClass, "")
//
// Three deliberate differences from the scan recipe, each of which
// would be a bug if copied from it:
//
//   - kind is the DETECTOR's kind ("audit.no_pdb"), not "k8s-event".
//     A posture finding is not the pull-path view of a symptom the
//     sentinel could push; there is no event for "this Deployment has
//     no PodDisruptionBudget", so there is nothing to dedupe against
//     and no reason to borrow the reactive kind. For posture the check
//     slug IS the incident class.
//
//   - reason is passed through UNCANONICALIZED. CanonicalReason maps
//     k8s event reasons onto their families; a posture reason is not
//     an event reason, so running it through that table can only
//     mis-map (a posture reason that happened to collide with a table
//     key would be silently rewritten into someone else's class) and
//     can never merge two classes that should be merged. Including the
//     reason at all is a superset of "kind + objectClass": it costs
//     nothing when a detector has one reason and keeps classes
//     distinct for a detector that grows a second.
//
//   - zone is EMPTY. Zone scopes a reactive incident to the failure
//     domain it happened in — a stockout in us-central1-a is not the
//     one in -b. Posture is a property of a spec, identical in every
//     zone the workload lands in, so stamping a zone would fragment
//     the #189 fleet rollup into one class per zone and defeat the
//     join it exists for. Instance identity (which cluster, which
//     object) is the SUBJECT KEY's job, not the fingerprint's — see
//     docs/audit-ingestion-contract.md §4.
func PostureFingerprint(kind, reason, objectClass string) string {
	return Fingerprint(kind, reason, objectClass, "")
}
