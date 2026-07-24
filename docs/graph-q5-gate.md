# pkg/graph — §15 Q5 compaction gate

DESIGN.md §6.2 sized v1 as "interface designed for the compact
representation, implementation plain Go maps", and §15 Q5 asked M1 to *set
the gate* — the benchmarked scale at which the CSR + arena/interning rewrite
(behind the same interface) becomes justified — instead of guessing.

This file records the map-backed baseline and the trigger thresholds.
Re-run and update when the numbers move materially:

```
go test ./pkg/graph -bench . -benchmem -run '^$'
```

## Baseline (map-backed COW, v1)

Measured 2026-07-24, Intel Xeon @ 2.20GHz (4 vCPU CI-class machine), Go
1.26, synthetic cluster (`synthCluster`: deployments/statefulsets/
daemonsets/jobs with services, endpoint slices, ingresses, per-workload +
shared configmaps, secrets, PVCs, nodes/zones, namespace-wide netpols):

| Metric | 1k pods | 10k pods |
| --- | --- | --- |
| Graph size (nodes / edges) | 3.6k / 8.9k | 35.7k / 88.1k |
| Initial build (`FromObjects`, one swap) | 31 ms | 533 ms |
| Retained heap (snapshot + writer state + interner) | 1.6 MB | 17 MB |
| Radius depth-3, p50 | 112 µs | 125 µs |
| Radius depth-3, p99 | 0.70 ms | 0.47 ms |
| Delta apply, amortized (swap per 200 deltas) | 27 µs → 37k deltas/s | 48 µs → 21k deltas/s |
| Snapshot swap alone (full map clone) | 0.45 ms | 4.2 ms |

Context for reading them:

- **The informer caches still dominate** (§6.2): 17 MB of graph next to
  hundreds of MB of decoded objects in client-go caches at 10k pods.
- **Real informer delta rates are hundreds to low thousands of events/s.**
  At 21k deltas/s sustained (10k pods) the writer has ≥10× headroom.
- **The swap is the structural cost of plain-map COW**: every publish
  clones the three top-level maps (nodes, out, in) — 4.2 ms at 10k pods.
  At the default 300 ms swap interval that is ~1.4% of one core, invariant
  with delta rate (batching amortizes it).

Costs scale roughly linearly in nodes+edges. Extrapolated to the 100k-pod
ceiling (~360k nodes / ~900k edges): swap ~40 ms (13% of a core at 300 ms
cadence), heap ~170 MB, initial build ~5 s, radius p99 still ≪10 ms.

## The gate

Revisit the CSR + interning rewrite (same interface, packed adjacency
arrays, generation-sharded COW instead of full map clones) only when a
**real deployment** (not a synthetic ceiling) crosses any of:

1. **Swap cost > 25 ms** per publish (≈8% of a core at the 300 ms
   cadence) — measured by `BenchmarkSnapshotSwap` at that cluster's scale;
   linear extrapolation puts this around **60k pods**.
2. **Radius p99 > 5 ms** at depth 3 on live queries — an order of
   magnitude above today's 10k-pod tail, and the point where interactive
   `triage radius` / enrichment (§7.6) latency budgets start to notice.
3. **Graph retained heap > 150 MB** — the point where the graph stops
   being a rounding error next to the informer caches it rides on.
4. **Sustained ingest > 5k deltas/s** required with swap interval
   ≤ 300 ms — beyond any observed informer rate; seeing this means the
   delta source, not the graph, should be examined first.

Below those lines, plain maps win on code size, debuggability, and zero
unsafe/arena machinery. If a threshold trips, the rewrite is confined to
this package behind `Snapshot`/`Writer`; consumers (`state edges`,
`triage radius`, storm correlation) do not change.
