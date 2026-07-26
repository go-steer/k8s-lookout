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

package quota

import "regexp"

// UID semantics (§10.3, documented per the correlation contract):
// the dedup identity of a quota incident is the QUOTA ITSELF —
// "quota:<NAME>/<SCOPE>" (scope = region, or "global") — not a
// nodegroup and not the project. The quota source runs once per
// project (§11), so the UID is already project-unique within its
// sentinel, and §8 deployment identity (cluster/project) carries the
// project for fleet rollup. Both halves of the QuotaExhausted dedup
// family key on it: quota.forecast always, capacity.quota_blocked
// whenever the provider's decision message names the quota — that
// shared (uid, canonical-reason) key is what turns "scaleup failed"
// + "CPUS at 98%" into ONE session with a followup instead of two.

// uidPrefix namespaces quota UIDs against object UIDs and the
// capacity source's synthetic "nodegroup:" keys.
const uidPrefix = "quota:"

// UID returns the canonical dedup/§7.4 identity for one quota:
// "quota:<NAME>/<SCOPE>".
func UID(name, scope string) string { return uidPrefix + name + "/" + scope }

// GCE quota-exceeded operation messages carry the quota name and
// scope in a fixed grammar, e.g.:
//
//	Quota 'CPUS' exceeded. Limit: 2000.0 in region us-east1.
//	Quota 'BACKEND_SERVICES' exceeded. Limit: 9.0 globally.
//
// (Documented GCE error format; the gke provider's visibility-log
// parser passes these through in ScaleDecision.Message.)
var (
	quotaNameRe   = regexp.MustCompile(`Quota '([A-Z][A-Z0-9_]*)' exceeded`)
	quotaRegionRe = regexp.MustCompile(`in region ([a-z][a-z0-9-]*)`)
	quotaGlobalRe = regexp.MustCompile(`\bglobally\b`)
)

// UIDFromDecisionMessage extracts the canonical quota UID from a
// provider scale-decision message (the capacity source calls this
// for GCE_QUOTA_EXCEEDED decisions, §10.3). ok is false when the
// message does not name BOTH the quota and its scope — a partial
// match must not join the wrong region's session, so the caller
// keeps its nodegroup key and only the reason family is shared
// (documented limitation; conservative by design).
func UIDFromDecisionMessage(msg string) (uid string, ok bool) {
	name := quotaNameRe.FindStringSubmatch(msg)
	if name == nil {
		return "", false
	}
	if region := quotaRegionRe.FindStringSubmatch(msg); region != nil {
		return UID(name[1], region[1]), true
	}
	if quotaGlobalRe.MatchString(msg) {
		return UID(name[1], "global"), true
	}
	return "", false
}
