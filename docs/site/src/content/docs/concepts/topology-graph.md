---
title: The topology graph
description: The pod-nexus index behind radius, edges, and changes — copy-on-write snapshots live, snapshot-plus-replay for any past instant.
sidebar:
  order: 2
---

The first questions in any incident are about relationships: what does
this pod depend on, who talks to it, what else shares its node? The
Kubernetes API is flat and resource-centric — reconstructing "what
relates to this pod" costs an agent 10–15 round trips unless something
maintains the relations for it. This page explains that something: the
in-memory topology index, which has no CLI of its own but sits behind
`state edges`, `triage radius|changes|events|spec`, `bundle`, storm
correlation, and session enrichment.

## The pod-nexus model

Typed nodes and edges centered on the Pod, connecting the traffic and
policy layers above it to the infrastructure below:

```
Gateway/Ingress → Service/EndpointSlice → [NetworkPolicy, RBAC] → POD
POD → Containers | ConfigMaps/Secrets | PVCs/Volumes → Node → Zone
```

It is a directed *graph*, not a DAG — selector relationships and shared
mounts create cycles, and traversals carry visited-sets rather than assuming
acyclicity. The graph never stores secret *values*: only names, keys, and
content hashes, so a change record can say "secret db-credentials changed"
without ever holding the payload.

## One index, three questions

| Question | Command | Query shape |
| --- | --- | --- |
| Is the wiring *correct*? | [`state edges`](/reference/state-edges/) | outbound edges of a workload + per-edge validity checks (ConfigMap/Secret keys, selectors and endpoint readiness, Ingress backends, RBAC refs, TLS expiry) |
| Who is *affected*? | [`triage radius`](/reference/triage-radius/) | bounded BFS: upstream routes (Services, Ingresses — user-facing impact), lateral co-tenants (shared node/config/volume), downstream dependencies |
| What *changed*? | [`triage changes`](/reference/triage-changes/) | the graph delta log joined with the event timeline, scoped to the target's neighborhood |

`edges` and `radius` are deliberately complementary: edges verifies
correctness of dependencies; radius enumerates impact. `bundle` runs both in
one pass.

## Consistency: copy-on-write snapshots

Readers query an atomic copy-on-write snapshot and never take locks; a
single writer batches informer deltas and publishes a new snapshot at most
every few hundred milliseconds. That discipline exists for correctness, not
throughput: a blast-radius answer computed during churn is taken against
one consistent topology, never a half-applied update.

The implementation is intentionally plain (Go maps behind a compact
interface). Benchmarks at 1k and 10k pods put the graph's memory and query
costs at a small fraction of what the informer caches already spend, and the
measured thresholds that would justify a compact rewrite are recorded in
[`docs/graph-q5-gate.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/graph-q5-gate.md)
— none have tripped.

## History: `--at` and time-travel

Persistence exists for **time-travel, not recovery** (after a restart the
API server re-syncs the graph in seconds anyway). A sentinel running with
`--store` writes two things into its SQLite store:

- a compressed topology **snapshot** every `--graph-snapshot-interval`
  (default 5m), tagged with its generation;
- the continuous per-delta **change log** — which doubles as the data source
  for `triage changes`.

A query with `--at=<RFC3339|duration-ago>` resolves to the nearest earlier
snapshot and replays the change log forward to the requested instant. The
summary line always names its source: `source=history at=…` for a store
answer, `source=live` for the current graph, and
`source=live-approximation` when `triage changes` reconstructs what it can
from current API state without a store.

Two properties worth knowing:

- **History outlives the objects.** A post-mortem radius query returns pods
  and ReplicaSets the live cluster has already deleted and forgotten —
  that is the point. History stores topology, not status, so fields like pod
  readiness are omitted in history mode rather than guessed.
- **Replay does not cross a sentinel restart.** Snapshot generations are
  per-process, so a `--at` window spanning a restart is currently
  unanswerable from the store — a known, documented gap. Post-mortems
  inside one sentinel incarnation work as designed.

One-shot CLI invocations serve `--at` only when pointed at a sentinel's
store file via `--store`; history reads are fully offline — copy the SQLite
file off the node and query it with no cluster access at all. The
[what-changed guide](/guides/what-changed/) walks through exactly that.
