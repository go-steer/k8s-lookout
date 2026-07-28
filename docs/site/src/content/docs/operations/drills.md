---
title: Drills & verification
description: The staged-failure runbooks in dev/drills/ — what each proves, when to run them, and the captured evidence they replay.
sidebar:
  order: 3
---

[`dev/drills/`](https://github.com/go-steer/k8s-lookout/tree/main/dev/drills)
contains runbooks that replay each milestone's exit criterion against a
**real GKE staging cluster**, plus the fixtures they use:

- [`stub-daemon.py`](https://github.com/go-steer/k8s-lookout/blob/main/dev/drills/stub-daemon.py)
  — a small capture daemon implementing `POST /sessions` and
  `POST /sessions/<sid>/inject`, logging every request body.
  `kubectl logs` of the stub is the wire-level evidence capture.
- [`memory-leaker.py`](https://github.com/go-steer/k8s-lookout/blob/main/dev/drills/memory-leaker.py)
  — the tunable leak fixture for the memory-leak drill.

Every drill is **staging-only** by design — they kill nodes, ship
crashing images, and saturate quotas. Each runbook opens with its blast
warning; take it literally.

## When to run them

- **After first deploying the sentinel to a new environment** — a drill
  is the end-to-end proof that RBAC, the daemon wiring, and the enabled
  sources actually work on your cluster, with real timing (real image
  pulls, real node-monitor grace periods) rather than the kind-cluster
  originals.
- **Before turning on a new source or flag set in production** — the
  drills' flag blocks are the tested reference configurations.
- **To produce corpus records** — each drill ends with schema-stable
  `kind=resolved` outcomes; the captured store and stub log are
  harvestable labeled trajectories (`dev/tools/harvest-corpus`).

## The drills

| Runbook | What it proves | Recorded original |
| --- | --- | --- |
| [`node-failure.md`](https://github.com/go-steer/k8s-lookout/blob/main/dev/drills/node-failure.md) | Storm correlation + fix-verify: a killed node produces **1 storm session, not 30** (the kind run: 3 session creates for 33 affected objects), and recovery injects close every member without any agent polling. Includes the VM-stop-vs-drain distinction — a graceful drain exercises a different storm key and is the rehearsal, not the replay. | [`docs/milestones/M2.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/milestones/M2.md) |
| [`bad-deploy.md`](https://github.com/go-steer/k8s-lookout/blob/main/dev/drills/bad-deploy.md) | A bad rollout under `maxUnavailable=0` fires `rollout.stall` on the Deployment (~3m, ahead of `progressDeadlineSeconds` by ~5m) while users keep getting 200s from the old revision; plus the post-mortem half — copying the store off the node and answering "blast radius at onset" with `--at` after the cluster has moved on. | [`docs/milestones/M3.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/milestones/M3.md) |
| [`memory-leak.md`](https://github.com/go-steer/k8s-lookout/blob/main/dev/drills/memory-leak.md) | A slow leaker under a memory limit produces `saturation.forecast` — ETA and confidence basis attached — while the pod is still Running/Ready, minutes before the kernel OOM-kills it (kind run: critical session 14 minutes before the OOM, forecast ETA accurate to 31 seconds). Explains the window-vs-drill-time tradeoff (`--saturation-window`). | [`docs/milestones/M3.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/milestones/M3.md) |
| [`quota-exhaustion.md`](https://github.com/go-steer/k8s-lookout/blob/main/dev/drills/quota-exhaustion.md) | The full quota story against real GCP APIs: a quota driven toward its limit yields `quota.forecast` with the drafted increase request attached; the autoscaler slamming into it folds into the **same** incident; filing the draft goes through the permission gate (you run the `gcloud` command — `lookout` only reads); plus the mid-incident `health --store` triage-state check. Maps every milestone fixture to the real API it stands in for. | [`docs/milestones/M4.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/milestones/M4.md) |

The runbooks share infrastructure deliberately: the same stub daemon,
the same `deploy/` manifests applied unmodified, and flag sets that
build on each other (the bad-deploy and memory-leak drills use one
sentinel configuration). Each names the exact flags of its recorded run,
with drill-tuned values (shorter windows, faster snapshots) marked
against the production defaults.

Keep the captures. Stub logs, sentinel logs, metrics scrapes, and the
copied store are the drill record — the milestone documents linked above
are exactly that material for the original runs, and the resolved
payloads in yours are corpus records.
