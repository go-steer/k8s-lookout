# Post-M5 sensor & triage roadmap — coverage gaps, tiered

DESIGN.md §14 is complete: the nine §7.2 sources shipped, every §4.1
command exists, and the v0.10.0 hardening release closed two rounds of
adversarial review. This note is the successor backlog: a systematic
sweep of what lookout still cannot see across K8s, GKE, and the GCP
services that fail into GKE workloads. It was produced by inventorying
the shipped sources/commands/provider surface against the failure
domains, and it supersedes nothing — DESIGN.md's boundaries (§2, §3)
still govern; Tier D re-states them.

Tiers are ordered by leverage, not effort. Tier A items are promises
the repo already makes to itself (a design comment, a skill reference,
a drill observation); Tiers B and C are new coverage; Tier D is the
list of things we keep deliberately saying no to.

**Status refreshed 2026-08-12.** Ten of the twenty Tier A–C items have
shipped since this note was written, and reading it as a live gap list
was producing wrong answers (it was cited as such during the
`langchain-samples/sre-agent` assessment — see
[`assessments/langchain-sre-agent.md`](./assessments/langchain-sre-agent.md)
§6.4). Every item now carries an explicit marker:

- **✅ shipped** — done; the parenthetical names where it landed.
- **◐ partial** — one half shipped, the named remainder is still open.
- **○ open** — unchanged since this note was written.

The kind inventory in
[`signal-schema-v1.md`](./signal-schema-v1.md) (48 kinds, additive-only
since the 32-kind M5 freeze) is the authoritative record of what
shipped; this note records *why* each item was wanted.

## Tier A — already promised in-repo

1. **✅ shipped** (#128 — `cloud.AuditAPI.ObjectWrites`,
   `pkg/cloud/capabilities.go:303-311`, consumed by `stab drift
   --identity`). **Cloud Audit Logs query capability + packs.** DESIGN.md §5 and the
   gitops-drift skill both promise a "later query pack" that resolves
   `stab drift`'s manager strings to real caller identities. Ship it
   as a `pkg/cloud` capability (patterned on the CA-visibility reader
   in `pkg/cloud/gke/capacity.go`, with the §2 explicit-unavailability
   posture). One capability unlocks four features: drift identity, the
   indefinitely-deferred exec-spy, node-pool ops in `triage changes`
   (§5 names the change class; nothing feeds it), and spot-preemption
   attribution (C.2).
2. **✅ shipped** (#132 — `engine.KindFamilyMember`,
   `pkg/engine/signal.go:81`; `Injector.InjectFamilyMember`,
   `pkg/inject/injector.go:223-227`).
   **`family.member` followup inject.** M4 drill observation 4: when a
   reactive signal joins an existing forecast session (capacity's
   `quota_blocked` folding into a `quota.forecast` incident), the join
   is store-recorded but never injected — the agent working the
   forecast never hears "the autoscaler just hit this for real."
   Schema-stable followup, already shaped by the observation.
3. **✅ shipped** (§7.7 severity-routing policy, wired at
   `internal/watch/dispatch.go:77-83,185-188`; a nil policy preserves
   the pre-§7.7 pipeline byte-for-byte).
   **§7.7 severity/threshold config.** The object-state source's
   thresholds and per-class severities are hardcoded defaults that
   explicitly await the severity-routing config file
   (`pkg/sources/objectstate/objectstate.go`).
4. **◐ partial** — the cluster half shipped (`--cluster-name`,
   `internal/watch/flags.go:321`, in every inject payload); the **zone**
   half is still open: `pkg/sources/k8sevents` stamps no zone, so
   `fingerprint.go:40`'s "empty when unknown" still describes the
   k8s-events path.
   **Zone/cluster stamping for the k8s-events path.** Fingerprints
   hash `zone=""` today (self-consistent, but wrong the moment two
   clusters share a store); M5.md calls the cluster-metadata wiring
   "future flag surface." Prerequisite for trustworthy fleet rollup.
5. **○ open** (AGENTS.md still lists the `GraphAt` restart-replay fix
   among the key open items).
   **The five M3 graph-history observations** (docs/milestones/M3.md):
   `GraphAt` restart replay, `Snapshot.Watches` round-trip,
   Deployment-targeted history, `rollout_stall` corpus fields,
   `origin=sync` marker. Every post-mortem whose window crosses a
   sentinel restart hits one of these.

## Tier B — K8s-native sensor blind spots

1. **✅ shipped** (#129 — `pkg/sources/workload`, kinds
   `workload.job_failed` / `workload.cron_missed` at
   `workload.go:98-99`, clearance semantics at `:55-64`). A residual
   gap this item did not name: a Job that never fails and never
   finishes — the silent batch hang — is still invisible. See
   [`assessments/langchain-sre-agent.md`](./assessments/langchain-sre-agent.md)
   §6.1 R3.
   **Batch workloads: Job/CronJob sensor.** The biggest pure-K8s gap.
   A failed Job (`BackoffLimitExceeded`, `DeadlineExceeded`) or a
   CronJob that stops being scheduled (`FailedNeedsStart`,
   too-many-missed-start-times) is invisible today unless its pods
   happen to crashloop into the event allow-list. New kinds
   `workload.job_failed` / `workload.cron_missed`; needs §7.4
   clearance semantics defined up front (a Job that "recovers" is a
   new Job — clearance is probably next-successful-run for CronJobs
   and terminal for Jobs).
2. **✅ shipped** (#131 — `pkg/sources/autoscaling`, kinds
   `autoscaling.hpa_pinned` and `autoscaling.hpa_metrics_dead` at
   `autoscaling.go:114-115`).
   **HPA saturation.** `triage events` detects HPA *thrash* on the
   read path only. No sensor fires when an HPA sits pinned at
   maxReplicas with its metric still over target
   (`autoscaling.hpa_pinned`) or when `FailedGetResourceMetric` means
   autoscaling has been silently dead for an hour. The workload-layer
   complement to the capacity source's node-layer signals.
3. **✅ shipped** (#131 — `capacity.cluster_forecast`,
   `pkg/sources/capacity/forecast.go:365,422`, registered before the
   reactive capacity kinds).
   **Cluster bin-packing forecast.** Saturation forecasts
   per-container; quota forecasts the cloud layer; nothing forecasts
   the layer between — schedulable headroom. Slope of
   sum(requests)/sum(allocatable) per scheduling domain →
   `capacity.cluster_forecast` ("cluster full in ~N hours"). Lives in
   the capacity source next to `pending`; same linear-window forecast
   machinery as saturation.
4. **✅ shipped** (#134 — object-state now watches
   MemoryPressure/DiskPressure/PIDPressure,
   `pkg/sources/objectstate/objectstate.go:262-264`, with node
   clearance at `nodeclearance.go:88`).
   **Node pressure conditions.** Object-state watches Ready only.
   MemoryPressure/DiskPressure/PIDPressure transitions and
   eviction-storm detection are the leading edge of node death — the
   `Evicted` event is allow-listed but arrives after the fact.
5. **○ open** — `pkg/sources/saturation` has no ephemeral dimension;
   its dimensions remain CPU/Memory/PVC.
   **Ephemeral-storage saturation dimension.** The kubelet
   stats-summary call saturation already makes for PVCs carries
   ephemeral-storage usage in the same payload; ephemeral evictions
   are common and currently surface only post-hoc as `Evicted`.
6. **○ open** — no source reads ResourceQuota; it remains a read-path
   check only.
   **Namespace ResourceQuota forecast.** `triage delta` reads
   ResourceQuotas; nothing forecasts them. The k8s-layer mirror of
   `quota.forecast`, same ETA thresholds.
7. **○ open**.
   **API-deprecation sensor.** `apiserver_requested_deprecated_apis`
   is available where GKE control-plane metrics are enabled (the perf
   packs' existing gate): warn while the workload still works, before
   the auto-upgrade removes the API. Pairs with C.1's upgrade events.

## Tier C — GKE / GCP surfaces not yet tapped

1. **✅ shipped** (#130 — `pkg/sources/notifications`, kinds
   `notification.upgrade`, `notification.upgrade_available`,
   `notification.security_bulletin` at `notifications.go:80-82`;
   bulletins route to the watchboard as proposed).
   **GKE cluster notifications (Pub/Sub).** `UpgradeEvent`,
   `UpgradeAvailableEvent`, `SecurityBulletinEvent`. The single most
   useful correlation in real GKE triage is "your incident started
   four minutes after the node-pool upgrade began" — today lookout
   cannot say it. Security bulletins routed to the watchboard are pure
   win. Project-tier sub-source like quota; provider capability for
   the subscription read.
2. **◐ partial** — the portable half shipped: `delta/nodes.go:139`
   emits `node.preempt` from the on-node reclaim taints
   (`cloud.google.com/impending-node-termination`, `:55`). The
   audit-log attribution this item actually proposed — distinguishing
   "GCP reclaimed it" from "the node died" — is still open, and A.1
   has now landed the capability it was waiting on.
   **Spot/preemption reclaim attribution.** Correlate node deletions
   with `compute.instances.preempted` audit entries: "GCP reclaimed
   the node" (expected churn on spot pools) vs "node died" (incident).
   Kills a class of false-positive NotReady storms. Rides A.1's
   audit-log capability.
3. **○ open**.
   **Cloud NAT port exhaustion.** `router.googleapis.com/nat/*`
   metrics (allocation failures, dropped sent packets). The classic
   GKE outage that is invisible from inside the cluster — egress just
   times out. `cloud nat` check plus a degradation tie-in.
4. **○ open**.
   **PD performance throttling.** `instance/disk/throttled_*_ops`
   joined to PVC-backed workloads explains "app slow, Kubernetes
   healthy." `state volumes` already builds the attachment join the
   metric needs.
5. **◐ partial** — event-reason half shipped (#135,
   `pkg/sources/ingress`, 3 `ingress.*` kinds); the LB backend
   unhealthy-ratio metrics half is still open and #135 stays open to
   track it.
   **GCLB/Ingress health.** (Event-reason half done, #135: the
   `ingress` source owns the ingress-gce failure reasons —
   `Sync`/`Translate` on Ingresses, NEG sync/attach failures on
   Services — with its own Warning-only Event informer, the
   capacity-source ownership precedent; a default allow-list entry
   was disqualified because ingress-gce reuses reason `Sync` for
   Normal housekeeping and the reactive path carries no event type.
   Remaining: LB backend unhealthy-ratio metrics.) ingress-gce event
   reasons (`Sync`, `Translate`, NEG attach failures) are not in the
   default allow-list, and LB backend unhealthy-ratio metrics are
   untapped; `cloud orphans` only catches rules with zero backends.
6. **◐ partial** — unchanged since this note was written: no
   `autopilot` detection exists in `pkg/cloud`, so the `clusters.get`
   half and the capacity re-badging are both still open.
   **Autopilot posture.** (Partially done post-#145: the probe
   reports platform denials in the authorizer's words, saturation
   degrades its PVC dimension instead of dying, and Autopilot is
   documented in portability/install. Remaining: provider-side
   detection via `clusters.get` and capacity-semantics re-badging.)
   Minimum viable: detect it (`clusters.get` autopilot field), loudly
   degrade the nodes-proxy paths it blocks (PVC/ephemeral stats), and
   re-badge capacity semantics — node provisioning is Google's there,
   while pending-pod aging remains ours.

## Tier D — explicitly out of scope (still current)

Fleet/multi-cluster rollup (external layer, §3/§11) · EKS/AKS
providers (until a consumer exists, §2) · non-GKE GCP domains —
CloudSQL, GCS, etc. (the hypothetical `gcp-lookout`, §2) ·
mesh/Gateway-API graph kinds (§15 Q6, until a consuming deployment
runs them) · Prometheus metrics backend (§15 Q4, until a non-GKE
consumer) · daemon-gated triage-status writes (blocked on core-agent's
Memory surface).

This tier is **unchanged as policy**, but one entry now needs reading
carefully. A Gateway API *source* shipped (#168, `pkg/sources/gateway`,
2 `gateway.*` kinds, discovery-gated on `GatewayAPIServed`). The Tier D
"no" was and remains about mesh/Gateway-API **graph kinds** — `pkg/graph`
still has no gateway node or edge type, and adding one still waits on a
consuming deployment. Watching Gateway API objects and modelling them in
the topology graph are separate decisions.

## Shortlist

The original shortlist is complete except for the two halves noted
below. Ordering was as-recommended; status verified against the tree on
2026-08-12.

| # | Item | Tier | Issue | Status |
|---|------|------|-------|--------|
| 1 | Cloud Audit Logs capability + identity pack | A.1 | #128 | ✅ shipped |
| 2 | Job/CronJob sensor | B.1 | #129 | ✅ shipped |
| 3 | GKE cluster notifications source | C.1 | #130 | ✅ shipped |
| 4 | HPA pinned + cluster bin-packing forecast | B.2 + B.3 | #131 | ✅ shipped (issue closed) |
| 5 | `family.member` followup inject | A.2 | #132 | ✅ shipped |
| 6 | Node pressure conditions sensor | B.4 | #134 | ✅ shipped |
| 7 | GCLB/Ingress health (event reasons first) | C.5 | #135 | ◐ event half shipped; metrics half open, issue stays open |

Still open in Tiers A–C, unpromoted: A.4 (zone half), A.5, B.5, B.6,
B.7, C.2 (audit-log attribution half — now unblocked by A.1), C.3, C.4,
C.6. Everything here stays in this doc until promoted to an issue —
#136 records the per-item disposition. Tier D stays as the standing no.

## Candidates from outside this sweep

This note inventoried lookout against failure domains. A later
comparison against a differently-shaped tool —
[`assessments/langchain-sre-agent.md`](./assessments/langchain-sre-agent.md)
— surfaced candidates this sweep did not, because they are gaps in
*explanatory* coverage rather than detection blind spots: a
missing-`Resources.Requests` census with LimitRange awareness (§6.1
R1+R2, the highest value-to-effort item found), stuck-Job detection
(R3, the residual noted under B.1 above), PV `Released`/`Failed`
orphans (R4), and Helm release labelling on graph objects (R5). That
assessment also parks two posture checks behind
[`fleet-audit-detectors-design.md`](./fleet-audit-detectors-design.md)
Open Question 1, and records what should *not* be adopted.
