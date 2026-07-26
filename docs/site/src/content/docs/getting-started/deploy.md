---
title: Deploy the sentinel
description: kubectl apply -f deploy/ — what each manifest is, the RBAC tiers, namespace-tier caveats, and a walkthrough of the flags that matter.
sidebar:
  order: 3
---

The watch-path — `lookout watch`, the resident per-cluster sentinel —
deploys from the shipped manifests:

```sh
kubectl apply -f deploy/
```

Three prerequisites a real deployment already has (the shipped manifests
reference them but do not create them): the `agent-triage` namespace, a
`k8s-event-watcher-token` Secret holding the daemon bearer token, and a
core-agent daemon answering at the `--daemon-url` — wiring the daemon is
the [next page](/getting-started/connect-core-agent/).

## What each manifest is

| Manifest | What it is |
| --- | --- |
| `11-serviceaccount-watcher.yaml` | The sentinel's ServiceAccount (`k8s-event-watcher` — resource names kept for drop-in continuity with predecessor deployments). Bound to no GCP IAM role: the sentinel talks only to the local API server and the daemon. |
| `12-clusterrole-watcher.yaml` | The minimum-necessary, **read-only** ClusterRole. No patch/update/delete on anything — the sentinel observes; mutations happen through the core-agent daemon's own permission gate. Each rule is annotated with the source that needs it; rules for disabled sources are harmless. |
| `13-clusterrolebinding-watcher.yaml` | Binds the ServiceAccount to the ClusterRole. |
| `14-role-watcher-capacity.yaml` | A `kube-system`-namespaced Role for the capacity source's one extra read: `get` on the `cluster-autoscaler-status` ConfigMap, pinned by `resourceNames` rather than widening the ClusterRole. Only needed with `--sources=…,capacity`. |
| `15-rolebinding-watcher-capacity.yaml` | Binds the ServiceAccount to the capacity Role. |
| `51-deployment-watcher.yaml` | The sentinel Deployment: one replica, distroless image, nonroot, `/healthz` liveness/readiness probes on the metrics port, and the shipped `args:`. A separate Deployment from the daemon (not a sidecar) so the two scale and restart independently; for another cluster, copy it and change `--cluster-name` + `--daemon-url`. |

One deliberate tradeoff to know about: the expiry source's `secrets`
rule is the sentinel's only read of Secret values (`tls.crt` to parse
`notAfter`; the token JWT for its `exp` claim), and it is `list` only —
no watch, no get, no informer cache of secret material. Scope it with
`--expiry-namespaces`, or remove the rule entirely if the expiry source
stays disabled.

## Deployment tiers (DESIGN.md §11)

| Tier | Unit | Mechanism |
| --- | --- | --- |
| Namespace | `lookout watch` under a `Role` | `--namespace`/`--exclude-namespace`. Cluster-scoped sources fail loudly at startup and must be disabled explicitly — never a silently empty watch. The topology graph builds a namespace-local subgraph. |
| Cluster | one sentinel per cluster (canonical) | One informer cache, one topology index, one credential boundary, one failure domain. One daemon may serve many sentinels. |
| Project | quota source only | One instance per GCP project, regardless of cluster count. Needs the `-gke` image. |
| Fleet | AX, not lookout | Sentinel-per-cluster fan-in; AX joins signals on `fingerprint` + `cluster`/`zone`/`project`. |

### Namespace-tier caveats — failures are loud

A namespace-scoped (Role-only) deployment cannot satisfy sources that
watch cluster-scoped objects (`object-state`'s nodes, `capacity`, PDB
checks, the `--storm` graph informers). The startup RBAC probe verifies
every enabled source's declared needs against the actual ServiceAccount
and refuses to start on a mismatch, naming exactly what is missing:

```
source "object-state" requires permission to "list nodes cluster-wide" (scope: Cluster)
and this ServiceAccount does not have it; grant it or disable the source —
refusing to run a silently empty watch
```

The same discipline covers scoped grants: with `--expiry-namespaces`
set, the probe verifies exactly the scoped namespaces. See
[Troubleshooting](/operations/troubleshooting/) for the source-by-source
requirements table.

## The flags that matter

The shipped `args:` in `51-deployment-watcher.yaml` are the wiring
minimum: `--daemon-url`, `--token-env`, `--mode=per-incident`, `--owner`,
`--cluster-name`, `--dedup-window=5m`, `--in-cluster`,
`--metrics-addr=:9090`, `--log-level=info`. The capability opt-ins:

- **`--sources`** — which signal sources run. The default is
  `k8s-events` only (the frozen predecessor surface). The full set:
  `k8s-events, object-state, rollout, saturation, degradation, expiry,
  capacity, quota, token-burn`. Each source's RBAC needs are probed at
  startup; each is individually disableable.
- **`--storm`** — storm correlation: incidents sharing a blast-radius
  key (nearest common topology ancestor) within `--storm-window` form
  one `kind=storm` session instead of dozens of per-pod pages. Requires
  pods/nodes/replicasets list+watch for the graph informers (in the
  shipped ClusterRole). Off by default.
- **`--store`** — the sentinel-local SQLite occurrence store: every
  emitted signal with its routing outcome, graph snapshots + change log
  (which unlock `--at` post-mortem queries), and triage-status records.
  Put it on the same volume as `--dedup-persist`. Bounded by
  `--store-ttl` (default 30 days) and `--store-max-mb` (default 512).
  See [The occurrence store](/operations/store/).
- **`--enrich`** — which severities get a pre-warmed bundle attached to
  their session's initial inject (`critical` by default; `warning`
  extends it; `off` disables). `--enrich-cap`, `--enrich-log-lines`,
  `--enrich-timeout` bound the work; failures become `enrichment_error`
  trailers, never crashes.
- **Severity and watchboard knobs** — `--severity kind=level`
  (repeatable) overrides the per-kind default routing;
  warning-class signals batch into the shared watchboard session
  (`--watchboard-batch`, `--watchboard-flush`, `--watchboard-rotate`).
  See [The watchboard](/operations/watchboard/).
- **`--dedup-persist`** and **`--recovery-stable-for`** — persist dedup
  bindings across restarts (recovery tracking resumes instead of
  re-firing), and how long a cleared symptom must stay clear before
  `kind=resolved` is injected (default 5m).

The generated [`lookout watch` reference](/reference/watch/) is the full
table of all 57 flags, derived from the live flag surface.

A battle-tested full-capability flag set — the exact args the M3 exit
drill appended to the shipped set
([`docs/milestones/M3.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/milestones/M3.md);
drill-tuned values marked):

```
--sources=k8s-events,object-state,rollout,saturation,degradation,expiry
--storm --enrich=critical
--store=/data/lookout.db
--graph-snapshot-interval=1m      # drill value; default 5m
--recovery-stable-for=60s         # drill value; default 5m
```

After `kubectl apply`, read the startup log before walking away: every
armed stage announces itself (`store: enabled …`, `graph history:
enabled …`, `storm: topology graph ready …`), and every problem is a
named, loud line — see
[Observing lookout](/operations/observability/).
