---
title: Sizing a sentinel
description: How much memory one sentinel needs, derived from the measured per-object cache cost — the table by cluster size, why the number varies by environment, and the levers when the limit is not enough.
sidebar:
  order: 8
---

Almost all of a sentinel's memory is one thing: the shared informer
cache. It holds every pod and node in the cluster, and every source
reads from it rather than listing the API server again. That is what
makes eleven sources cost roughly what one costs — and it is also what
makes the memory limit a function of cluster size rather than of how
many features you turned on.

So the sizing question is not "how much does lookout need", it is "how
many pods do you have".

## The measured constants

These are **retained heap** — what a cached object costs after the
ingest transform has dropped the fields nothing reads — measured by
holding deep copies and reading `HeapAlloc`, not inferred from
serialised size:

| | GKE 1.36 | kind |
| --- | --- | --- |
| Pod | **18,630 B** | **10,007 B** |
| Node | **5,308 B** | **3,026 B** |

Two things about that table matter more than the numbers in it.

**Heap is not wire.** A trimmed GKE pod is 10,788 B on the wire and
18,630 B in the cache — 1.7×. Sizing from the serialised size, which is
the number that is easy to get, is low by that factor.

**They vary ~1.9× by environment.** A managed GKE cluster annotates
heavily and runs more sidecars than a kind node does. The table below
uses the GKE column, because a default has to hold at the conservative
end. If your objects are smaller, you have headroom you can measure
rather than assume: `internal/watch/objectsize_test.go` re-runs the
measurement against any cluster you point it at.

## The table

Live set ≈ `pods × 18.2 KiB + nodes × 5.2 KiB + 100 MiB`, where the
constant covers the other eleven informer streams, the occurrence
store, the topology graph and the Go runtime. The suggested limit adds
GC headroom (~1.3×) and then sets `GOMEMLIMIT` to ~80% of it.

| Pods | Nodes | Live set | `limits.memory` | `GOMEMLIMIT` |
| --- | --- | --- | --- | --- |
| 1,000 | 100 | ~120 MiB | `256Mi` | `200MiB` |
| 5,000 | 500 | ~190 MiB | `384Mi` | `300MiB` |
| **15,000** | **1,500** | **~375 MiB** | **`768Mi` (the default)** | **`600MiB`** |
| 50,000 | 5,000 | ~1.0 GiB | `2Gi` | `1600MiB` |
| 150,000 | 5,000 | ~2.8 GiB | `5Gi` | `4000MiB` |

The shipped default is the 15,000-pod row, because that is the top of
the range lookout calls typical. It is **derived from the measured
per-object cost, not confirmed by a scale run at that size** — the
padded kwok harness exists for that and the run is tracked separately.
Treat the rows above 15,000 pods as extrapolation from a measured
slope, which is what they are.

CPU is not in the table because it does not scale with cluster size the
way memory does — it scales with *churn*. The shipped `200m` limit
covers a cluster with ordinary rollout activity; a cluster doing
sustained mass rescheduling needs more.
`rate(lookout_events_seen_total[5m])` is the churn measurement, and
`rate(process_cpu_seconds_total[5m])` against the limit is whether it
is costing you.

## The levers, when the limit is not enough

**Set `GOMEMLIMIT` first, and always.** It is a soft ceiling on the Go
heap: past it the collector runs harder rather than letting the heap
grow. The failure it prevents is the bad one — without it the kernel
OOM-kills the process, which loses the store's in-flight writes and
every established watch, and the sentinel comes back with a cold cache
during whatever incident made it busy. With it you get a slow sentinel
and a `lookout_watch_event_lag_seconds` you can alert on. The shipped
manifests set it; keep it at ~80% of the limit when you change either.

**Watch fewer namespaces.** `--exclude-namespace` is a real watch
scope, not a display filter — the excluded namespaces never enter the
cache, so it cuts memory directly and proportionally. A sentinel that
skips a handful of noisy CI namespaces on a 20k-pod cluster can be
smaller than the table says. See [Scoping a
sentinel](/operations/scoping/).

**Split by source.** Some sources can run in a second process without
paying for a second pod cache; most cannot, and splitting those buys
you two copies of the expensive thing. [Scoping a
sentinel](/operations/scoping/) says which is which.

**Raise the request too, near the top of the range.** The shipped
`requests.memory` is `128Mi`, which suits the small end. A Burstable
pod using far more than its request is evicted early under node memory
pressure, so on a cluster near or above the 15,000-pod row, set the
request to about the live-set column — otherwise the sentinel is the
first thing the kubelet reclaims on a node that is already in trouble.

## Checking it against reality

`/metrics` carries the standard Go and process collectors, so the table
is checkable against the process it describes rather than only against
cAdvisor:

| | |
| --- | --- |
| `process_resident_memory_bytes` | What the kubelet is comparing to the limit. This is the number the table's "live set" column predicts. |
| `go_memstats_heap_inuse_bytes` | How much of that is the Go heap — which is where the cache lives. |
| `go_memstats_next_gc_bytes` | The GC's next target. Sitting at `GOMEMLIMIT` means the soft ceiling is being enforced, which is the warning that you are one cluster-growth away from needing the next row. |

If the heap is much smaller than the resident set, the cache is not
what is using the memory and this page is the wrong place to look —
start at [Observing lookout](/operations/observability/).

Multiply your pod and node counts by the constants at the top to get
the prediction. A large gap in either direction is worth reporting: the
constants are a population parameter, and the only way they stay honest
is people measuring them somewhere new.
