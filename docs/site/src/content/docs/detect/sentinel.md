---
title: The sentinel
description: The failure classes the sentinel monitors, which signal source covers each, and what --sources=auto (the default) turns on by probing your deployment's grants.
sidebar:
  order: 3
---

The sentinel is one process per cluster, and what it watches is the
set of signal sources that are enabled. Out of the box that is
`--sources=auto`: at startup the sentinel probes each portable
source's needs — RBAC grants, plus a metrics API for `saturation` and
the Gateway API CRDs for `gateway` — and enables everything your
deployment supports, announcing each decision with one startup line.
Three of the fourteen sources are never auto-enabled and stay
explicit opt-ins: `quota` (a per-GCP-project deployment decision),
`notifications` (needs an operator-created Pub/Sub subscription), and
`token-burn` (a polling loop against the core-agent daemon's cost
stack).

## The coverage map

Organized by what fails, not by how the code is arranged. Every
"Example trigger" is either captured drill output or the source's own
shipped threshold.

| Watches for | Example trigger | Source name | On by default? | Extra needs |
| --- | --- | --- | --- | --- |
| Failures the control plane already reported | A pod enters `CrashLoopBackOff`; an image tag that doesn't exist (`ErrImagePull`) | `k8s-events` | **Auto** (always on — a sentinel that cannot watch events refuses to start) | none |
| Nodes going bad | A node's Ready condition flips to NotReady, or flaps 3 times inside 10 minutes | `object-state` | **Auto** | none |
| Nodes running out of room, and the evictions that follow | A node's `MemoryPressure`/`DiskPressure`/`PIDPressure` condition goes True and stays True for 5 minutes; 3 pod evictions on one node inside 10 minutes, folded into one node-scoped signal | `object-state` | **Auto** | none |
| Services going dark, drains about to stall | A Service's ready-endpoint count drops to zero; a PodDisruptionBudget's allowed disruptions hit 0 with pods behind it | `object-state` | **Auto** | none |
| Crash loops and stuck rollouts, before the events | A pod's restart count climbs 3 in 10 minutes — ahead of the kubelet's `BackOff` events; a Deployment burns 80% of its progress deadline with unready replicas | `object-state` | **Auto** | none |
| Bad deploys, while the old version still serves | New pods crash-looping while the old version still serves: zero ready-count progress for 3 minutes (`--rollout-observe`) with the old ReplicaSet healthy | `rollout` | **Auto** | none |
| Failed batch work and dead schedules | A Job's `Failed` condition goes True (`BackoffLimitExceeded`, `DeadlineExceeded`); an unsuspended CronJob passes a scheduled activation without running — three consecutive misses escalate to critical | `workload` | **Auto** | none |
| Autoscalers out of headroom, or silently dead | An HPA sits at `maxReplicas` with its metric still over target for 10 minutes (critical past 30); an HPA's `ScalingActive` goes False with a `FailedGet*` reason for 15 minutes — scaling has stopped and nothing says so | `autoscaling` | **Auto** | none |
| Resources trending toward exhaustion | A pod leaking ~1 MiB every 30 s, forecast to hit its 64 Mi memory limit in ~14 minutes; a PVC filling in ~3 h | `saturation` | **Auto** | metrics-server (`metrics.k8s.io`) — absent, auto skips the source with one loud line |
| Service capacity eroding before the outage | A backend's ready endpoints declining 5/5 → 3/5 across the trend window; a readiness probe that keeps flapping below the reactive threshold | `degradation` | **Auto** | none |
| Certificates and tokens running out | A TLS certificate 13 days from expiry; a cert-manager `Certificate` whose last renewal failed | `expiry` | **Auto** | none |
| The autoscaler failing to deliver nodes | A pod Pending and unschedulable past 5 minutes; a nodegroup that asked the cloud for a node and didn't get one for 3 minutes | `capacity` | **Auto** | a running cluster-autoscaler; GCP provider (`-gke` image) for the structured whys — stockout vs quota vs IP exhaustion |
| Load balancers that never get programmed (Ingress) | An `ingress-gce` Warning `Sync` ("Error syncing to GCP: …") or `Translate` event on an Ingress; a NEG-controller `AttachFailed`/`SyncNetworkEndpointGroupFailed` on a Service — endpoints never reach the load balancer while the Ingress object looks fine | `ingress` | **Auto** | none (nothing fires on clusters without `ingress-gce`/NEG controllers) |
| Load balancers that never get programmed (Gateway API) | A Gateway or listener holds `Programmed=False` past the 5-minute grace, with `observedGeneration` caught up and the reason not `Pending`; an HTTPRoute parent holds `Accepted=False`/`ResolvedRefs=False` — the route config never became routable | `gateway` | **Auto** | the Gateway API CRDs served — absent, auto skips the source with one loud line (RBAC alone can't tell, so this is a discovery check) |
| Cloud quota exhaustion, days out | `CPUS/us-east1` at 98% of limit, exhausted in ~16 h at the current slope — drafted increase request attached | `quota` | No — explicit | GCP provider (`-gke` image); project tier — exactly one sentinel per GCP project enables it |
| Agent token spend burning out of control | One session's token rate at 4× the cross-session median, sustained two polls; a session budget projected to exhaust inside 30 minutes | `token-burn` | No — explicit | `core-agent` daemon — its cost stack is the data source |
| The provider's own announcements: upgrades and security bulletins | A control-plane or node-pool upgrade starts (recorded for incident-window correlation); a security bulletin affecting the cluster lands on the watchboard | `notifications` | No — explicit | GKE notificationConfig topic + a Pub/Sub subscription (`--notifications-subscription`) |

"Auto" means the source is on whenever the startup probe finds its
grants (the shipped `deploy/` manifests carry all of them) — a miss
skips the source with a startup line naming the missing grant and the
fix, never silently.

Every kind these sources can emit — 48 in the frozen schema — is
cataloged in the [Signal kinds reference](/reference/signal-kinds/);
every threshold above is a flag documented in the
[`lookout watch` reference](/reference/watch/).

## What happens when something fires

A source emits a signal; the pipeline dedups it per object and reason,
so a pod that crashes forty times inside the dedup window is one
incident with a rising count — not forty pages. Severity then decides
the route: a critical signal opens its own agent session on the
daemon, warnings batch into the shared watchboard digest, and info
signals are stored (with `--store`) rather than surfaced. A critical
session arrives enriched: the initial inject carries a pre-warmed,
size-capped bundle — sanitized spec, recent changes, dependency edges,
blast radius, distilled log tails — so the agent's first tool calls
are already answered. And when the symptom clears and stays clear, the
sentinel injects a `kind=resolved` record into the same session: the
incident ends with verified proof, not silence. The full mechanics —
recovery, storms, the watchboard, triage-status — are in
[The closed loop](/concepts/closed-loop/).

## What auto gives you

With no `--sources` flag at all, startup resolves the portable set
against what your deployment can actually do and prints one line per
decision — the summary block, enabled lines included:

```
sources: auto — probing the portable set (RBAC per source; metrics.k8s.io for saturation); misses are skipped loudly — pin --sources explicitly to make a miss fatal (§11)
source k8s-events: enabled (always on — a sentinel that cannot watch events is misdeployed)
source object-state: enabled
source rollout: enabled
source workload: enabled
source autoscaling: enabled
source saturation: disabled (metrics.k8s.io unavailable — install metrics-server)
source degradation: enabled
source expiry: enabled
source capacity: enabled
source ingress: enabled
source gateway: disabled (Gateway API CRDs not served — install a GKE Gateway class or the upstream gateway.networking.k8s.io CRDs, or name gateway in --sources to make this fatal)
sources: auto resolved → k8s-events,object-state,rollout,workload,autoscaling,degradation,expiry,capacity,ingress (quota, notifications, and token-burn stay explicit-only: project tier, the notification subscription, and the core-agent cost stack)
```

`--storm` defaults to auto the same way: the graph informer grants
(pods/nodes/replicasets list+watch) present resolve storm correlation
on, a miss resolves it off with a line naming the grant. Storm is
what turns a dead node's thirty pod incidents into one session naming
the node. The one skip auto never makes is `k8s-events`: a sentinel
that cannot watch events is misdeployed, and that is a fatal startup
error, not a line in the block.

Add `--store` to complete the experience — the store is what makes
info signals durable, scans aware of prior triage, and post-mortem
queries possible; its path is deliberately always explicit
(`--store=/var/lib/lookout/lookout.db`, on a volume — the shipped
Deployment now wires one).

## Pinning sources explicitly

An explicit list is the strict mode, and its semantics are unchanged:
every named source's startup probe failure is a fatal error naming
the exact grant (`source "object-state" requires permission to "list
nodes cluster-wide" …`) — never a silently empty watch, and never a
skip. Pin a list when you'd rather crash-loop than run with less than
you asked for — the shipped `deploy/51` manifest carries the strict
list as a ready-to-uncomment alternative, since it ships alongside the
full RBAC:

```
--sources=k8s-events,object-state,rollout,workload,autoscaling,saturation,degradation,expiry,capacity,ingress,gateway,token-burn --storm=on --store=/var/lib/lookout/lookout.db --enrich=critical
```

`--sources=k8s-events` reproduces the pre-auto default surface
byte-for-byte. The three explicit-only sources have deployment-specific
homes: `quota` is a per-GCP-project opt-in on the `-gke` image,
`notifications` needs a Pub/Sub subscription on the project's GKE
notification topic (`--notifications-subscription`), and `token-burn`
reads the `core-agent` daemon's cost stack (it disables itself,
loudly, under the webhook sink) — which is why the strict list above
names `token-burn` explicitly and leaves the other two out. Naming
`gateway` in an explicit list makes a cluster without the Gateway API
CRDs a fatal startup error rather than a skip. The shipped manifests in
`deploy/` carry everything every portable source needs; see
[Troubleshooting](/operations/troubleshooting/) for the
source-by-source requirements and the summary-block anatomy.

## Where next

- [Deploy the sentinel](/getting-started/deploy/) — the manifests,
  RBAC tiers, and the rest of the flag walkthrough.
- [Signal kinds](/reference/signal-kinds/) — the exhaustive catalog of
  everything that can go on the wire.
- [`lookout watch`](/reference/watch/) — every flag, generated from
  the live flag surface.
