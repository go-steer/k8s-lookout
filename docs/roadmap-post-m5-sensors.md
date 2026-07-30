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

## Tier A — already promised in-repo

1. **Cloud Audit Logs query capability + packs.** DESIGN.md §5 and the
   gitops-drift skill both promise a "later query pack" that resolves
   `stab drift`'s manager strings to real caller identities. Ship it
   as a `pkg/cloud` capability (patterned on the CA-visibility reader
   in `pkg/cloud/gke/capacity.go`, with the §2 explicit-unavailability
   posture). One capability unlocks four features: drift identity, the
   indefinitely-deferred exec-spy, node-pool ops in `triage changes`
   (§5 names the change class; nothing feeds it), and spot-preemption
   attribution (C.2).
2. **`family.member` followup inject.** M4 drill observation 4: when a
   reactive signal joins an existing forecast session (capacity's
   `quota_blocked` folding into a `quota.forecast` incident), the join
   is store-recorded but never injected — the agent working the
   forecast never hears "the autoscaler just hit this for real."
   Schema-stable followup, already shaped by the observation.
3. **§7.7 severity/threshold config.** The object-state source's
   thresholds and per-class severities are hardcoded defaults that
   explicitly await the severity-routing config file
   (`pkg/sources/objectstate/objectstate.go`).
4. **Zone/cluster stamping for the k8s-events path.** Fingerprints
   hash `zone=""` today (self-consistent, but wrong the moment two
   clusters share a store); M5.md calls the cluster-metadata wiring
   "future flag surface." Prerequisite for trustworthy fleet rollup.
5. **The five M3 graph-history observations** (docs/milestones/M3.md):
   `GraphAt` restart replay, `Snapshot.Watches` round-trip,
   Deployment-targeted history, `rollout_stall` corpus fields,
   `origin=sync` marker. Every post-mortem whose window crosses a
   sentinel restart hits one of these.

## Tier B — K8s-native sensor blind spots

1. **Batch workloads: Job/CronJob sensor.** The biggest pure-K8s gap.
   A failed Job (`BackoffLimitExceeded`, `DeadlineExceeded`) or a
   CronJob that stops being scheduled (`FailedNeedsStart`,
   too-many-missed-start-times) is invisible today unless its pods
   happen to crashloop into the event allow-list. New kinds
   `workload.job_failed` / `workload.cron_missed`; needs §7.4
   clearance semantics defined up front (a Job that "recovers" is a
   new Job — clearance is probably next-successful-run for CronJobs
   and terminal for Jobs).
2. **HPA saturation.** `triage events` detects HPA *thrash* on the
   read path only. No sensor fires when an HPA sits pinned at
   maxReplicas with its metric still over target
   (`autoscaling.hpa_pinned`) or when `FailedGetResourceMetric` means
   autoscaling has been silently dead for an hour. The workload-layer
   complement to the capacity source's node-layer signals.
3. **Cluster bin-packing forecast.** Saturation forecasts
   per-container; quota forecasts the cloud layer; nothing forecasts
   the layer between — schedulable headroom. Slope of
   sum(requests)/sum(allocatable) per scheduling domain →
   `capacity.cluster_forecast` ("cluster full in ~N hours"). Lives in
   the capacity source next to `pending`; same linear-window forecast
   machinery as saturation.
4. **Node pressure conditions.** Object-state watches Ready only.
   MemoryPressure/DiskPressure/PIDPressure transitions and
   eviction-storm detection are the leading edge of node death — the
   `Evicted` event is allow-listed but arrives after the fact.
5. **Ephemeral-storage saturation dimension.** The kubelet
   stats-summary call saturation already makes for PVCs carries
   ephemeral-storage usage in the same payload; ephemeral evictions
   are common and currently surface only post-hoc as `Evicted`.
6. **Namespace ResourceQuota forecast.** `triage delta` reads
   ResourceQuotas; nothing forecasts them. The k8s-layer mirror of
   `quota.forecast`, same ETA thresholds.
7. **API-deprecation sensor.** `apiserver_requested_deprecated_apis`
   is available where GKE control-plane metrics are enabled (the perf
   packs' existing gate): warn while the workload still works, before
   the auto-upgrade removes the API. Pairs with C.1's upgrade events.

## Tier C — GKE / GCP surfaces not yet tapped

1. **GKE cluster notifications (Pub/Sub).** `UpgradeEvent`,
   `UpgradeAvailableEvent`, `SecurityBulletinEvent`. The single most
   useful correlation in real GKE triage is "your incident started
   four minutes after the node-pool upgrade began" — today lookout
   cannot say it. Security bulletins routed to the watchboard are pure
   win. Project-tier sub-source like quota; provider capability for
   the subscription read.
2. **Spot/preemption reclaim attribution.** Correlate node deletions
   with `compute.instances.preempted` audit entries: "GCP reclaimed
   the node" (expected churn on spot pools) vs "node died" (incident).
   Kills a class of false-positive NotReady storms. Rides A.1's
   audit-log capability.
3. **Cloud NAT port exhaustion.** `router.googleapis.com/nat/*`
   metrics (allocation failures, dropped sent packets). The classic
   GKE outage that is invisible from inside the cluster — egress just
   times out. `cloud nat` check plus a degradation tie-in.
4. **PD performance throttling.** `instance/disk/throttled_*_ops`
   joined to PVC-backed workloads explains "app slow, Kubernetes
   healthy." `state volumes` already builds the attachment join the
   metric needs.
5. **GCLB/Ingress health.** ingress-gce event reasons (`Sync`,
   `Translate`, NEG attach failures) are not in the default
   allow-list, and LB backend unhealthy-ratio metrics are untapped;
   `cloud orphans` only catches rules with zero backends.
6. **Autopilot posture.** Autopilot appears nowhere in the repo.
   Minimum viable: detect it (`clusters.get` autopilot field), loudly
   degrade the nodes-proxy paths it blocks (PVC/ephemeral stats), and
   re-badge capacity semantics — node provisioning is Google's there,
   while pending-pod aging remains ours.

## Tier D — explicitly out of scope (unchanged)

Fleet/multi-cluster rollup (external layer, §3/§11) · EKS/AKS
providers (until a consumer exists, §2) · non-GKE GCP domains —
CloudSQL, GCS, etc. (the hypothetical `gcp-lookout`, §2) ·
mesh/Gateway-API graph kinds (§15 Q6, until a consuming deployment
runs them) · Prometheus metrics backend (§15 Q4, until a non-GKE
consumer) · daemon-gated triage-status writes (blocked on core-agent's
Memory surface).

## Shortlist

In recommended order, each tracked by its own issue:

| # | Item | Tier | Issue |
|---|------|------|-------|
| 1 | Cloud Audit Logs capability + identity pack | A.1 | #128 |
| 2 | Job/CronJob sensor | B.1 | #129 |
| 3 | GKE cluster notifications source | C.1 | #130 |
| 4 | HPA pinned + cluster bin-packing forecast | B.2 + B.3 | #131 |
| 5 | `family.member` followup inject | A.2 | #132 |

Everything else in Tiers A–C stays in this doc until promoted to an
issue; Tier D stays here as the standing no.
