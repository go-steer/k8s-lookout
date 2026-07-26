---
title: What changed before the incident
description: Post-mortems with --at — blast radius and change timeline as of onset, answered offline from a copied sentinel store, 29 minutes after the cluster moved on. Real M3 drill output.
sidebar:
  order: 5
---

**The problem:** the incident is over — or at least the cluster has moved
on. The broken ReplicaSet was scaled to zero, its pods deleted, neighbors
replaced. Now comes the post-mortem, and its two central questions — *what
was the blast radius at onset?* and *what changed just before?* — are
about a topology that no longer exists.

A sentinel running with `--store` records exactly that topology: periodic
graph snapshots plus a per-delta change log in its SQLite store. The
graph-backed commands accept `--at=<RFC3339|duration-ago>` with
`--store=<file>` and answer as of that instant. All output below is from
the M3 exit drill, abridged.

The setting: a bad deploy of `drill-a/webapp` stalled at **10:54:55** (the
onset — see [Your rollout is stuck](/guides/stuck-rollout/)); it was
rolled back at 10:58; a neighbor pod was deleted and replaced at 11:15;
dozens of snapshot intervals elapsed.

## 1. Get the store

History reads are fully offline — no cluster access on the query path.
Copy the store off the node and query it anywhere:

- The store lives where the sentinel's `--store` flag points (the drill:
  `/data/lookout.db` on a hostPath; a standard deployment:
  `/var/lib/lookout/lookout.db` on the sentinel's volume).
- The distroless sentinel image has no `tar`, so `kubectl cp` cannot reach
  it. On kind: `docker cp <node>:/var/lib/lookout/lookout.db .` — on GKE,
  node-pool SSH plus `gcloud compute scp` (the drill runbooks in
  [`dev/drills/`](https://github.com/go-steer/k8s-lookout/tree/main/dev/drills)
  spell it out). SQLite's WAL absorbs copying next to the live writer.

## 2. Blast radius as of onset

At 11:23:29 — 28m34s after onset — against the copied store:

```sh
lookout triage radius webapp-55866d5cff-cwgp4 -n drill-a --at=2026-07-26T10:54:55Z --store=lookout.db
```

```txt
kind=radius.neighbor … kind_of_object=ReplicaSet name=webapp-55866d5cff direction=upstream relation=Owns hop=1
kind=radius.neighbor … name=webapp-77f8d7558c-7gvwx direction=lateral relation=shared-node hop=2 shared=Node/kl-m3-worker
kind=radius.neighbor … name=webapp-77f8d7558c-c2nmj direction=lateral relation=shared-node hop=2 shared=Node/kl-m3-worker
…(9 same-node co-tenants)…
kind=radius.neighbor … kind_of_object=Node name=kl-m3-worker direction=downstream relation=RunsOn hop=1
scanned=69 findings=13 elapsed=6ms source=history at=2026-07-26T10:54:55Z
```

The at-onset answer contains the broken-revision pod, its ReplicaSet, and
the two old-revision pods that were still serving. The same question asked
live proves the point:

```txt
$ lookout triage radius webapp-55866d5cff-cwgp4 -n drill-a          # same question, LIVE
lookout triage radius: workload Pod/drill-a/webapp-55866d5cff-cwgp4 not found in the topology
```

The live cluster no longer knows the pod existed. History and live
demonstrably differ — that difference is what the store is for.

## 3. The change timeline before onset

```sh
lookout triage changes webapp-55866d5cff-cwgp4 -n drill-a --at=2026-07-26T10:54:55Z --since=10m --store=lookout.db
```

```txt
…(abridged)…
kind=change.rollout … kind_of_object=ReplicaSet name=webapp-55866d5cff reason=Added at=2026-07-26T10:51:41Z relation=upstream origin=log
kind=change.rollout … kind_of_object=Pod name=webapp-55866d5cff-cwgp4 reason=Added at=2026-07-26T10:51:41Z relation=self origin=log
scanned=149 findings=12 … source=history at=2026-07-26T10:54:55Z window=2026-07-26T10:44:55Z..2026-07-26T10:54:55Z
```

The last change before onset is the bad-revision rollout, 3m14s before the
stall — the "what changed" answer, with provenance (`origin=log`) and the
window printed on the summary line.

## Reading the summary-line source

Always check `source=` before trusting a historical answer:

- `source=history at=…` — served from the store's snapshot + replay; the
  full recorded delta log.
- `source=live` — the current graph; no history involved.
- `source=live-approximation` — no store available: `triage changes`
  reconstructs rollouts and recent scale events from current API state and
  Events (the drill's comparison run recovered the per-revision images and
  all four scale steps this way), but cannot see un-timestamped updates —
  ConfigMap edits, label flips, old cordons. The honest degraded answer,
  marked as such.

Two current limits, found and documented by the drill: replay cannot cross
a sentinel restart (query within one incarnation), and historical targets
must be a Pod or ReplicaSet (the sentinel graph feed holds Deployments
identity-only). The M3 milestone record tracks both.

## As an agent skill

The post-hoc `--at`/`--store` procedure is taught to agents in
[`skills/k8s-triage`](https://github.com/go-steer/k8s-lookout/tree/main/skills/k8s-triage)
(the "state at onset" section); change-timeline auditing on GitOps
clusters is
[`skills/gitops-drift`](https://github.com/go-steer/k8s-lookout/tree/main/skills/gitops-drift).
