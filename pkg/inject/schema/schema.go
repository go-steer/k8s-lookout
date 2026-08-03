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

// Package schema exports the signal-schema v1 kind ledger as data
// (docs/signal-schema-v1.md; frozen at M5): every shipped signal
// kind, the wire struct that serializes it, and its one-line role.
//
// Two consumers share this slice so they cannot drift apart: the
// freeze test in pkg/inject (TestSchemaV1_KindInventory pins the
// count and the kind→struct mapping) and the docs-site generator
// (internal/sitedoc renders the signal-kind catalog page from it).
// Because every Kind field references the owning package's constant,
// renaming a constant without updating the ledger is a compile
// error, not a doc-rot risk.
//
// Additions are v1-additive (§8): extend this ledger, the field pins
// in pkg/inject/schema_freeze_test.go, and docs/signal-schema-v1.md
// in the same change. Removing or renaming an entry is a v2
// negotiation with fleet consumers, never a routine edit.
package schema

import (
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/degradation"
	"github.com/go-steer/k8s-lookout/pkg/sources/expiry"
	"github.com/go-steer/k8s-lookout/pkg/sources/ingress"
	"github.com/go-steer/k8s-lookout/pkg/sources/notifications"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
	"github.com/go-steer/k8s-lookout/pkg/sources/quota"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
	"github.com/go-steer/k8s-lookout/pkg/sources/tokenburn"
	"github.com/go-steer/k8s-lookout/pkg/sources/workload"
)

// KindSpec is one shipped signal kind in the v1 ledger.
type KindSpec struct {
	// Kind is the wire value of the payload's `kind` field.
	Kind string
	// Payload is the zero value of the wire struct that serializes
	// this kind (reflect.TypeOf(Payload) is the freeze test's pin).
	Payload any
	// Source is the `--sources` name of the emitting source, or ""
	// for the cross-cutting kinds the dispatcher itself emits
	// (outcome records, storms, watchboard, triage evidence).
	Source string
	// Doc is the one-line role, for listings and the docs site.
	Doc string
}

// Kinds returns the full v1 kind inventory in ledger order:
// the frozen M0 pair, the cross-cutting dispatcher kinds, then the
// source-namespaced kinds source by source (§7.3). The returned
// slice is a copy; callers may reorder it freely.
func Kinds() []KindSpec {
	out := make([]KindSpec, len(kinds))
	copy(out, kinds)
	return out
}

var kinds = []KindSpec{
	// Frozen M0 pair (byte-identical to the original watcher; the
	// dispatcher never stamps §8 identity fields on these).
	{inject.KindEvent, inject.Payload{}, "k8s-events",
		"Frozen reactive kind: the opening inject of a per-incident session, byte-identical to the original k8s-event-watcher."},
	{inject.KindFollowup, inject.Payload{}, "k8s-events",
		"Frozen reactive kind: a dedup-window recurrence injected into the already-open incident session."},

	// §7.4 outcome records (the §9.3 ground-truth labels).
	{inject.KindResolved, inject.ResolvedPayload{}, "",
		"§7.4 outcome record: the symptom stayed clear for --recovery-stable-for; carries resolution=recovered|object_deleted."},
	{inject.KindResolvedReverted, inject.ResolvedPayload{}, "",
		"§7.4 outcome record: the symptom recurred within the revert window after a resolve."},

	// §7.5 storm kinds.
	{inject.KindStorm, inject.StormPayload{}, "",
		"§7.5 aggregate incident: opened when --storm-min incidents share a blast-radius key within --storm-window."},
	{inject.KindStormMember, inject.StormMemberPayload{}, "",
		"§7.5 membership record injected into the storm session for each folded incident."},
	{inject.KindStormMemberSuperseded, inject.StormMemberPayload{}, "",
		"§7.5 supersede pointer left in a pre-storm incident session that the storm absorbed."},
	{inject.KindStormUpdate, inject.StormUpdatePayload{}, "",
		"§7.5 storm size refresh (latest wins): membership grew past a reporting threshold."},

	// §7.7 watchboard kinds.
	{inject.KindWatchboardDigest, inject.WatchboardDigestPayload{}, "",
		"§7.7 warning-class batch flushed to the shared watchboard session (--watchboard-batch / --watchboard-flush)."},
	{inject.KindWatchboardRotated, inject.WatchboardRotatedPayload{}, "",
		"§15 Q2 size-based rotation pointer naming the successor watchboard session after --watchboard-rotate digests."},

	// §9.4 regression evidence.
	{inject.KindTriageRegressed, inject.TriageRegressedPayload{}, "",
		"§9.4 regression evidence: a downgraded incident's recurrence count reached --triage-regress-factor times its count at downgrade — evidence only, never a re-page."},

	// §10.3 cross-source join notice.
	{inject.KindFamilyMember, inject.FamilyMemberPayload{}, "",
		"§10.3 cross-source join: a signal from a different source family attached to this session's incident (leading↔reactive) — at most one per source family per incident per dedup window; storm members never fan these out."},

	// Source-namespaced kinds (§7.3): all ride inject.Payload with
	// the full §8 identity stamped.
	{objectstate.KindNodeNotReady, inject.Payload{}, "object-state",
		"A Node's Ready condition transitioned True→False/Unknown — workloads on that node are next."},
	{objectstate.KindNodeFlapping, inject.Payload{}, "object-state",
		"A Node's Ready condition flapped repeatedly within the flap window."},
	{objectstate.KindProgressDeadline, inject.Payload{}, "object-state",
		"A Deployment rollout made no progress with unready replicas — fired BEFORE the control plane's ProgressDeadlineExceeded event."},
	{objectstate.KindEndpointsEmpty, inject.Payload{}, "object-state",
		"A Service's ready-endpoint count transitioned >0 → 0."},
	{objectstate.KindPDBGridlocked, inject.Payload{}, "object-state",
		"A PodDisruptionBudget's disruptionsAllowed transitioned >0 → 0 while pods behind it exist — drains will stall."},
	{objectstate.KindRestartBurst, inject.Payload{}, "object-state",
		"A pod's summed container restart count grew past the burst threshold — the leading edge of a crash loop, ahead of BackOff events."},
	{objectstate.KindNodePressure, inject.Payload{}, "object-state",
		"A Node's kubelet pressure condition (MemoryPressure/DiskPressure/PIDPressure) went False→True; escalates to critical when sustained or paired with eviction activity on the node."},
	{objectstate.KindEvictionBurst, inject.Payload{}, "object-state",
		"N pod evictions on one node within the burst window, folded into ONE node-scoped signal — the storm-off fallback for the per-pod Evicted event family."},
	{rollout.KindStall, inject.Payload{}, "rollout",
		"A new revision made zero ready-count progress for --rollout-observe while the old revision stayed healthy."},
	{workload.KindJobFailed, inject.Payload{}, "workload",
		"A Job's Failed condition went True (BackoffLimitExceeded, DeadlineExceeded, ...) — batch failure with no crashlooping pod behind it."},
	{workload.KindCronMissed, inject.Payload{}, "workload",
		"An unsuspended CronJob passed a scheduled activation without lastScheduleTime advancing; consecutive misses escalate to critical."},
	{saturation.KindForecast, inject.Payload{}, "saturation",
		"A linear-regression forecast says a resource dimension exhausts within --saturation-warn (critical below 15m)."},
	{degradation.KindCapacity, inject.Payload{}, "degradation",
		"A Service's ready-endpoint ratio declined stepwise across --degradation-window — capacity eroding before the outage."},
	{degradation.KindProbeFlap, inject.Payload{}, "degradation",
		"A pod's readiness gate flipped repeatedly without ever sustaining failure long enough for the reactive Unhealthy path."},
	{expiry.KindWarning, inject.Payload{}, "expiry",
		"An expiry countdown (certificate/token) crossed a threshold: warning at --expiry-warn, critical at the design-fixed 72h."},
	{capacity.KindPending, inject.Payload{}, "capacity",
		"A NotTriggerScaleUp event: the autoscaler declined a pending pod, with per-nodegroup rejection reasons."},
	{capacity.KindScaleUp, inject.Payload{}, "capacity",
		"A TriggeredScaleUp event: the autoscaler asked the cloud for nodes (info; stored context for later gaps)."},
	{capacity.KindScaleDown, inject.Payload{}, "capacity",
		"The ScaleDown event family (info; warning for ScaleDownFailed)."},
	{capacity.KindScaleUpGap, inject.Payload{}, "capacity",
		"A nodegroup's cloudProviderTarget exceeded its ready count beyond the sustain window — asked for a node, didn't get one."},
	{capacity.KindStockout, inject.Payload{}, "capacity",
		"A provider scale decision names a stockout: the zone/machine-type has no capacity. Remedy-disjoint from quota."},
	{capacity.KindQuotaBlocked, inject.Payload{}, "capacity",
		"A provider scale decision names quota exhaustion: file a quota increase (§10.3). Remedy-disjoint from stockout."},
	{capacity.KindIPExhausted, inject.Payload{}, "capacity",
		"A provider scale decision names IP exhaustion: new nodes/pods cannot get addresses."},
	{capacity.KindPendingAged, inject.Payload{}, "capacity",
		"A pod stayed Pending+Unschedulable past --pending-age (critical past the design-fixed 15m)."},
	{ingress.KindSyncFailed, inject.Payload{}, "ingress",
		"An ingress-gce Warning Sync event on an Ingress: GCLB programming is failing while the Ingress object looks fine."},
	{ingress.KindTranslateFailed, inject.Payload{}, "ingress",
		"An ingress-gce Warning Translate event on an Ingress: the spec could not be translated into GCLB resources."},
	{ingress.KindNEGFailed, inject.Payload{}, "ingress",
		"A NEG-controller failure on a Service (sync/attach/detach/retry): endpoints are not reaching the load balancer."},
	{quota.KindForecast, inject.Payload{}, "quota",
		"A GCP quota's usage slope projects exhaustion (warning ETA<7d or usage>=90%; critical ETA<48h or >=98%), with a quota-increase draft attached."},
	{notifications.KindUpgrade, inject.Payload{}, "notifications",
		"The provider announced a control-plane or node-pool upgrade starting — store-recorded evidence for incident-window correlation."},
	{notifications.KindUpgradeAvailable, inject.Payload{}, "notifications",
		"The provider offered a new version for auto-upgrade."},
	{notifications.KindSecurityBulletin, inject.Payload{}, "notifications",
		"A provider security bulletin affects this cluster — batched to the watchboard."},
	{tokenburn.KindBurn, inject.Payload{}, "token-burn",
		"An agent session's token rate ran at --burn-multiple times the cross-session baseline, or projects budget exhaustion within --burn-eta."},
}
