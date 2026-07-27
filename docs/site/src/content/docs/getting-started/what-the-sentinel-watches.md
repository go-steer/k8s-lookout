---
title: What the sentinel watches
description: The failure classes the sentinel monitors, which signal source covers each, what is on out of the box (k8s-events only), and the flag line that turns the rest on.
sidebar:
  order: 4
---

The sentinel is one process per cluster, and what it watches is exactly
the set of signal sources you enable with `--sources`. Out of the box
only one source is on — `k8s-events`, the Kubernetes warning events —
and the other eight are opt-in.

## The coverage map

Organized by what fails, not by how the code is arranged. Every
"Example trigger" is either captured drill output or the source's own
shipped threshold.

| Watches for | Example trigger | Source name | On by default? | Extra needs |
| --- | --- | --- | --- | --- |
| Failures the control plane already reported | A pod enters `CrashLoopBackOff`; an image tag that doesn't exist (`ErrImagePull`) | `k8s-events` | **Yes** | none |
| Nodes going bad | A node's Ready condition flips to NotReady, or flaps 3 times inside 10 minutes | `object-state` | No | none |
| Services going dark, drains about to stall | A Service's ready-endpoint count drops to zero; a PodDisruptionBudget's allowed disruptions hit 0 with pods behind it | `object-state` | No | none |
| Crash loops and stuck rollouts, before the events | A pod's restart count climbs 3 in 10 minutes — ahead of the kubelet's `BackOff` events; a Deployment burns 80% of its progress deadline with unready replicas | `object-state` | No | none |
| Bad deploys, while the old version still serves | New pods crash-looping while the old version still serves: zero ready-count progress for 3 minutes (`--rollout-observe`) with the old ReplicaSet healthy | `rollout` | No | none |
| Resources trending toward exhaustion | A pod leaking ~1 MiB every 30 s, forecast to hit its 64 Mi memory limit in ~14 minutes; a PVC filling in ~3 h | `saturation` | No | metrics-server (`metrics.k8s.io`) |
| Service capacity eroding before the outage | A backend's ready endpoints declining 5/5 → 3/5 across the trend window; a readiness probe that keeps flapping below the reactive threshold | `degradation` | No | none |
| Certificates and tokens running out | A TLS certificate 13 days from expiry; a cert-manager `Certificate` whose last renewal failed | `expiry` | No | none |
| The autoscaler failing to deliver nodes | A pod Pending and unschedulable past 5 minutes; a nodegroup that asked the cloud for a node and didn't get one for 3 minutes | `capacity` | No | a running cluster-autoscaler; GCP provider (`-gke` image) for the structured whys — stockout vs quota vs IP exhaustion |
| Cloud quota exhaustion, days out | `CPUS/us-east1` at 98% of limit, exhausted in ~16 h at the current slope — drafted increase request attached | `quota` | No | GCP provider (`-gke` image); project tier — exactly one sentinel per GCP project enables it |
| Agent token spend burning out of control | One session's token rate at 4× the cross-session median, sustained two polls; a session budget projected to exhaust inside 30 minutes | `token-burn` | No | `core-agent` daemon — its cost stack is the data source |

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

## Turning it all on

The recommended full-coverage flag line, appended to the shipped
`args:`:

```
--sources=k8s-events,object-state,rollout,saturation,degradation,expiry,capacity --storm --store=/var/lib/lookout/lookout.db --enrich=critical
```

`--storm` and `--store` are part of the full experience, not garnish:
storm correlation is what turns a dead node's thirty pod incidents
into one session naming the node, and the store is what makes info
signals durable, scans aware of prior triage, and post-mortem queries
possible. The two sources left out have deployment-specific homes:
`quota` is a per-GCP-project opt-in on the `-gke` image, and
`token-burn` reads the `core-agent` daemon's cost stack (it disables
itself, loudly, under the webhook sink).

Enabling a source is loud on failure: at startup the sentinel probes
every enabled source's declared RBAC needs against its actual
ServiceAccount, and a miss is an explicit error naming the exact grant
(`source "object-state" requires permission to "list nodes
cluster-wide" …`) — never a silently empty watch. The shipped
manifests in `deploy/` carry everything every portable source needs;
see [Troubleshooting](/operations/troubleshooting/) for the
source-by-source requirements.

An honest note on the default: `k8s-events`-only is deliberately
conservative, because the sentinel is a drop-in image swap for its
predecessor — a deployment that changes nothing in its config must
keep byte-identical behavior. The default preserves compatibility;
the flag line above is the sentinel as designed.

## Where next

- [Deploy the sentinel](/getting-started/deploy/) — the manifests,
  RBAC tiers, and the rest of the flag walkthrough.
- [Signal kinds](/reference/signal-kinds/) — the exhaustive catalog of
  everything that can go on the wire.
- [`lookout watch`](/reference/watch/) — every flag, generated from
  the live flag surface.
