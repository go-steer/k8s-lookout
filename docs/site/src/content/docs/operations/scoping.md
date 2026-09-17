---
title: Scoping a sentinel
description: Narrowing what one sentinel watches — --exclude-namespace as a real watch scope, splitting by source, and which sources can be separated without paying for a second cache.
sidebar:
  order: 6
---

The canonical deployment is one sentinel per cluster watching
everything, and it is the right default: one informer cache, one
topology graph, one credential boundary. This page is for the cases
where it is not — where a cluster has namespaces you have no business
watching, or where you want the drift counters without also running
the event watcher.

There are two axes to cut along, and they compose: **which namespaces**
a sentinel watches, and **which sources** it runs.

## Namespaces

Two flags look alike and are not.

| Flag | What it does |
| --- | --- |
| `--namespace=a,b` | Allow-list applied to **output**. Every namespace is still listed, watched, decoded and cached; signals from namespaces outside the list are dropped before they are emitted. |
| `--exclude-namespace=x,y` | Deny-list applied to **the watch**. The namespaced informers carry a `metadata.namespace!=` field selector, so `x` and `y` are never listed and never enter the cache. |

### `--namespace` is not a security boundary

Worth saying plainly, because the flag name invites the opposite
reading: a sentinel run with `--namespace=payments` holds every other
namespace's pods in memory. Names, labels, images, owner chains and
node placement for the whole cluster are resident in its cache. (What
is *not* resident is resolved secret values — those are stripped on the
way into the cache for every namespace, watched or not.) If the
requirement is "this process must not be able to see namespace `x`",
`--namespace` does not meet it and never did. `--exclude-namespace`
does, and RBAC does it better still.

### `--exclude-namespace` shrinks the process

```
lookout watch --exclude-namespace=kube-system,gmp-system
```

The sentinel logs the selector it derived at startup:

```
watch: --exclude-namespace is scoping the watch — the namespaced
informers list and watch with field selector
"metadata.namespace!=gmp-system,metadata.namespace!=kube-system", so
excluded namespaces never enter the cache; nodes are cluster-scoped and
unaffected
```

On a busy cluster the two system namespaces above are frequently a
third to a half of all pods, and they are pods nobody is paging on.
Excluding them cuts cache size, decode work and watch traffic by
roughly their share.

Three things to know:

- **Nodes are unaffected.** They are cluster-scoped, so a namespace
  deny list has nothing to remove from them — and the API server
  *rejects* `metadata.namespace` on a cluster-scoped LIST rather than
  ignoring it, so the node informer runs on its own unfiltered
  factory. This costs no extra watch: the node informer is still one
  stream shared by every reader.
- **Correlation only sees what is watched.** Storm correlation and the
  topology graph are built from the same informers, so a node failure's
  blast radius will not include pods in an excluded namespace. That is
  usually what you want — you excluded them — but it means an excluded
  namespace cannot appear as collateral damage either.
- **An allow-list is not available.** Field selectors have no `OR`, so
  `metadata.namespace=a` can name exactly one namespace and watching
  *M* of them needs *M* informer factories — 12 namespaced streams
  each. That is tracked as
  [issue #407](https://github.com/go-steer/k8s-lookout/issues/407) and
  is deliberately not built yet; an exclusion of any length is one
  selector on one stream, which is why this direction is cheap and the
  other is not.

### Or use RBAC

The strongest version of "do not watch namespace `x`" is not to grant
it. A sentinel whose ServiceAccount cannot list pods cluster-wide fails
loudly at startup naming the source and the permission (see
[Troubleshooting](/operations/troubleshooting/)) rather than watching
an empty cache. `--exclude-namespace` is the right tool when you hold a
cluster-wide grant and want to spend less; RBAC is the right tool when
the grant itself is the problem.

## Sources

`--sources` takes a comma-separated list, and nothing requires one
sentinel to run all of them:

```
# A sentinel that only tracks topology drift.
lookout watch --sources=topology-drift

# A sentinel that only watches Events.
lookout watch --sources=k8s-events
```

An explicit list also changes failure semantics in a way that is
useful here: under `--sources=auto` a source whose grants are missing
is skipped with a log line, but a **named** source's missing required
grant is fatal. If you deployed a sentinel *for* drift, you want it to
refuse to start rather than to run as an expensive no-op.

### Which splits are free, and which cost a cache

Sources do not each own their informers — they share one factory, so
two sources reading Pods cost one pod cache between them. Splitting
them into separate deployments **un**-shares that. The question for any
proposed split is therefore only: do the two halves read the same
objects?

| Source | Objects it watches |
| --- | --- |
| `k8s-events` | Events |
| `ingress` | Events |
| `capacity` | Events, Pods, Nodes |
| `object-state` | Pods, Nodes, Deployments, EndpointSlices, PDBs |
| `rollout` | Deployments, ReplicaSets, StatefulSets, Pods |
| `degradation` | Pods, EndpointSlices |
| `topology-drift` | Pods, Nodes, ReplicaSets |
| `workload` | Jobs, CronJobs |
| `autoscaling` | HorizontalPodAutoscalers |
| `gateway` | Gateway API objects (its own factory) |
| `expiry`, `saturation` | none — polled |
| `quota`, `notifications`, `token-burn` | none — provider APIs |

So:

- **Free to separate:** `workload`, `autoscaling`, `gateway`,
  `expiry`, `saturation`, `quota`, `notifications`, `token-burn`. None
  of them shares an informer with anything else, so moving one into
  its own deployment costs only the process.
- **Expensive to separate:** anything in the Pods/Nodes/Events core —
  `object-state`, `rollout`, `degradation`, `topology-drift`,
  `capacity`, `k8s-events`, `ingress`. Pull `topology-drift` into its
  own sentinel and you now run two pod caches where you ran one, and
  the pod cache is the single largest thing the process holds. Do it
  because you want a different blast radius or a different credential,
  not to save memory — it will not.

`quota` and `notifications` are already deployed this way in a fleet:
they describe a *project*, not a cluster, so a multi-cluster sentinel
runs them once per project rather than once per cluster, and drops
them from the per-cluster source list automatically.

### What a split costs you

Signals are correlated inside one process. Split sources across
processes and you lose:

- **Cross-source follow-ups.** The dispatcher notices when one
  source's signal follows another's on the same object and counts it on
  `lookout_cross_source_followups_total`. Two processes never see each
  other's signals.
- **Storm blast radius.** `--storm` groups signals by common ancestor
  in the topology graph. A node failure that produces `object-state`
  and `rollout` signals in two different processes is two unrelated
  pages, not one incident.
- **Recovery clearance across sources.** The §7.4 tracker clears an
  incident when an observer says the condition is gone; observers come
  from the sources running in the same process.

Deduplication and the occurrence store are per-process too, so each
half needs its own `--store` path and its own sink configuration.

None of that argues against splitting — it argues for splitting along
a line where the two halves would not have correlated anyway. A
drift-only or a quota-only sentinel is a clean cut. Splitting
`object-state` from `rollout` is not.
