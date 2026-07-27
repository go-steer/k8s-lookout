---
title: What the sentinel watches
description: The failure classes the sentinel monitors, which signal source covers each, and what --sources=auto (the default) turns on by probing your deployment's grants.
sidebar:
  order: 4
---

The sentinel is one process per cluster, and what it watches is the
set of signal sources that are enabled. Out of the box that is
`--sources=auto`: at startup the sentinel probes each portable
source's needs — RBAC grants, plus a metrics API for `saturation` —
and enables everything your deployment supports, announcing each
decision with one startup line. Two sources are never auto-enabled
and stay explicit opt-ins: `quota` (a per-GCP-project deployment
decision) and `token-burn` (a polling loop against the core-agent
daemon's cost stack).

## The coverage map

Organized by what fails, not by how the code is arranged. Every
"Example trigger" is either captured drill output or the source's own
shipped threshold.

| Watches for | Example trigger | Source name | On by default? | Extra needs |
| --- | --- | --- | --- | --- |
| Failures the control plane already reported | A pod enters `CrashLoopBackOff`; an image tag that doesn't exist (`ErrImagePull`) | `k8s-events` | **Auto** (always on — a sentinel that cannot watch events refuses to start) | none |
| Nodes going bad | A node's Ready condition flips to NotReady, or flaps 3 times inside 10 minutes | `object-state` | **Auto** | none |
| Services going dark, drains about to stall | A Service's ready-endpoint count drops to zero; a PodDisruptionBudget's allowed disruptions hit 0 with pods behind it | `object-state` | **Auto** | none |
| Crash loops and stuck rollouts, before the events | A pod's restart count climbs 3 in 10 minutes — ahead of the kubelet's `BackOff` events; a Deployment burns 80% of its progress deadline with unready replicas | `object-state` | **Auto** | none |
| Bad deploys, while the old version still serves | New pods crash-looping while the old version still serves: zero ready-count progress for 3 minutes (`--rollout-observe`) with the old ReplicaSet healthy | `rollout` | **Auto** | none |
| Resources trending toward exhaustion | A pod leaking ~1 MiB every 30 s, forecast to hit its 64 Mi memory limit in ~14 minutes; a PVC filling in ~3 h | `saturation` | **Auto** | metrics-server (`metrics.k8s.io`) — absent, auto skips the source with one loud line |
| Service capacity eroding before the outage | A backend's ready endpoints declining 5/5 → 3/5 across the trend window; a readiness probe that keeps flapping below the reactive threshold | `degradation` | **Auto** | none |
| Certificates and tokens running out | A TLS certificate 13 days from expiry; a cert-manager `Certificate` whose last renewal failed | `expiry` | **Auto** | none |
| The autoscaler failing to deliver nodes | A pod Pending and unschedulable past 5 minutes; a nodegroup that asked the cloud for a node and didn't get one for 3 minutes | `capacity` | **Auto** | a running cluster-autoscaler; GCP provider (`-gke` image) for the structured whys — stockout vs quota vs IP exhaustion |
| Cloud quota exhaustion, days out | `CPUS/us-east1` at 98% of limit, exhausted in ~16 h at the current slope — drafted increase request attached | `quota` | No — explicit | GCP provider (`-gke` image); project tier — exactly one sentinel per GCP project enables it |
| Agent token spend burning out of control | One session's token rate at 4× the cross-session median, sustained two polls; a session budget projected to exhaust inside 30 minutes | `token-burn` | No — explicit | `core-agent` daemon — its cost stack is the data source |

"Auto" means the source is on whenever the startup probe finds its
grants (the shipped `deploy/` manifests carry all of them) — a miss
skips the source with a startup line naming the missing grant and the
fix, never silently.

Every kind these sources can emit — 32 in the frozen schema — is
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
source saturation: disabled (metrics.k8s.io unavailable — install metrics-server)
source degradation: enabled
source expiry: enabled
source capacity: enabled
sources: auto resolved → k8s-events,object-state,rollout,degradation,expiry,capacity (quota and token-burn stay explicit-only: project tier and the core-agent cost stack)
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
you asked for (the shipped `deploy/51` manifest does exactly this,
since it ships alongside the full RBAC):

```
--sources=k8s-events,object-state,rollout,saturation,degradation,expiry,capacity --storm=on --store=/var/lib/lookout/lookout.db --enrich=critical
```

`--sources=k8s-events` reproduces the pre-auto default surface
byte-for-byte. The two explicit-only sources have deployment-specific
homes: `quota` is a per-GCP-project opt-in on the `-gke` image, and
`token-burn` reads the `core-agent` daemon's cost stack (it disables
itself, loudly, under the webhook sink). The shipped manifests in
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
