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

// Package findings implements the run-to-run transition surface
// (issue #212): consecutive scans are diffed against each other so a
// consumer reports what CHANGED — new / ongoing / escalated / resolved
// / suppressed — instead of re-listing every open finding on every
// run.
//
// The problem it solves is digest fatigue. §9.4 triage-status gives
// "triaged reality at a point in time"; it does not classify
// transitions BETWEEN two scans. Without that, an unattended agent
// re-reports the same forty findings every fifteen minutes and an
// operator stops reading them. With it, the digest says "3 things
// changed."
//
// # Two grains of identity, both needed
//
// This package introduces a SECOND key alongside engine.Fingerprint,
// and the distinction is the crux of the design:
//
//   - engine.Fingerprint(kind, reasonClass, objectClass, zone) is the
//     incident CLASS hash. It deliberately excludes the affected
//     object so fleet consumers can roll up "ImagePullBackOff on Pods
//     in us-east4" across clusters. It is a FROZEN cross-cluster
//     contract (pkg/engine/fingerprint.go) and this package does not
//     touch it — widening it for the diff would silently split every
//     fleet rollup in half during a rolling upgrade.
//
//   - SubjectKey is INSTANCE identity: which specific workload is
//     broken. A diff needs it because "the class ImagePullBackOff/Pod
//     is still present" is not the question — "is payment-backend
//     still broken, and is it the same breakage as last run" is.
//
// The class fingerprint keeps driving rollup; the subject key drives
// the diff. Both are carried on every transition so a consumer can
// join either way.
//
// # Scope
//
// MVP is SINGLE-CLUSTER and durable, built by generalizing the §9.1
// occurrence store rather than adding a second tracker beside it.
// Multi-cluster finding state (#208 shipped the watch side) is a
// follow-on: in the multi-cluster model the per-runner state is
// in-memory, so acks would evaporate on restart and everything would
// re-alert as `new`. That limitation is named here rather than left
// for an operator to discover when a four-hour ack silently expires
// after a rollout.
package findings

import "strings"

// SubjectKey composes the normalized instance identity a run-to-run
// diff keys on:
//
//	<cluster>/<namespace>/<KindOfObject>/<normalized-name>/<canonicalReason>
//
// Empty segments are legal and meaningful — a cluster-scoped Node
// finding from a single-cluster sentinel is `//Node/gke-pool-a/NodeNotReady`
// — which is why the separator is fixed-arity rather than "join the
// non-empty parts": a key must never be ambiguous about WHICH segment
// was empty. `/` is safe as a separator because no Kubernetes object
// name, namespace, or kind may contain one.
//
// The five fields, and why each is in:
//
//   - cluster: two clusters' payment-backend are different subjects.
//     Blank today for a single-cluster sentinel, and stamped once
//     multi-cluster state lands — which is why it is in the key from
//     day one rather than added later, since adding a segment to a
//     persisted key orphans every existing row.
//   - namespace + kindOfObject: ordinary Kubernetes identity.
//   - name: passed through NormalizeName, so a rescheduled pod stays
//     the same subject. This is the field that makes the key a
//     SUBJECT key rather than an object key.
//   - canonicalReason: a pod that was ImagePullBackOff last run and is
//     CrashLoopBackOff this run has not "continued" — one failure
//     ended and another began, and a digest that calls that `ongoing`
//     hides a new breakage. Pass the reason through
//     engine.CanonicalReason (or CanonicalReasonForEvent with a
//     message in hand) first, so the ErrImagePull/ImagePullBackOff
//     family collapse matches the dedup and fingerprint paths rather
//     than splitting one incident into two subjects.
func SubjectKey(cluster, namespace, kindOfObject, name, canonicalReason string) string {
	return strings.Join([]string{
		cluster,
		namespace,
		kindOfObject,
		NormalizeName(name),
		canonicalReason,
	}, "/")
}
