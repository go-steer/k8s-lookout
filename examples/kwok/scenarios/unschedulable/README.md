# unschedulable — three ways a pod never gets placed

Ten Pending pods from three Deployments, and the point is that the real
kube-scheduler rejects each one for a **different** reason:

| Workload | Shape | Scheduler's verdict |
| --- | --- | --- |
| `zone-orphan` (3) | `nodeSelector` on a zone that does not exist | `didn't match Pod's node affinity/selector` |
| `oversized` (2) | 64 CPU request against 32-CPU nodes | `Insufficient cpu` |
| `spread` (8, 3 placed) | required anti-affinity over a 3-node pool | `didn't match pod anti-affinity rules` |

```sh
examples/kwok/scenarios/unschedulable/inject
examples/kwok/scenarios/unschedulable/verify
examples/kwok/scenarios/unschedulable/revert
```

## Why this is the scale tier and not `examples/scenarios/`

`examples/scenarios/pending` requests 64 CPUs on a two-worker cluster
and gets one verdict: there is no room. That is the whole space a small
cluster can express. Three of the distinctions an operator actually
cares about need a fleet:

- **A capacity wall that is not a capacity shortage.** `oversized`
  cannot place a single pod while the fleet has thousands of idle
  cores, because `Insufficient cpu` is a *per-node* verdict. On two
  workers "out of CPU" and "no node big enough" are the same sentence.
- **A partially placed workload.** `spread` gets 3 of 8 pods running
  and stays there forever — degraded, not down. There is no room for
  the distinction on a cluster where one replica is the whole service.
- **Topology at all.** Anti-affinity over a bounded pool, with 97 other
  nodes visible and none of them eligible, is not a shape two workers
  can make.

The anti-affinity pool is three nodes that `inject` labels, rather than
a zone, so the arithmetic — 3 placed, 5 pending — holds at any fleet
size.

## What to expect

- `triage delta` → `kind=pod.pending severity=critical reason=Unschedulable`,
  carrying the scheduler's own message so a reader can tell a config
  error from a capacity wall from a topology limit. That distinction
  decides what you do next: fix the manifest, add a bigger node, or
  accept the ceiling.
- `triage delta` → `workload.rollout` on `spread` with `ready=3`.
- `health` → `category=pending status=degraded`.

`verify` passes `--pending-age=30s` to skip the 5-minute grace; the
grace itself is what `examples/scenarios/pending` covers.

Every pod here tolerates the fake-node taint *and* pins itself
somewhere only a fake node could satisfy — otherwise a pod could fall
back onto a real kind node and quietly succeed, which is a broken
fixture rather than a finding. `verify` asserts the count of Pending
pods before it asserts anything about lookout.

## Explore by hand

```sh
kubectl -n kwok-scenario-unschedulable get pods -o wide
lookout triage delta --namespace=kwok-scenario-unschedulable --pending-age=30s
lookout health
```

Agent-harness prompt to try:
> Nothing in kwok-scenario-unschedulable will start. The cluster has a
> hundred nodes and plenty of spare capacity. What is going on, and is
> it the same problem for all three deployments?
