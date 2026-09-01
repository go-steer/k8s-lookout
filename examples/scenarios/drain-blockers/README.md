# drain-blockers — the three things a drain destroys quietly

A **UAT fixture**, not a failure scenario. `lookout stab drain` is a
pre-maintenance check: before you cordon and drain a node, what will
*block* the drain, and what will the drain *destroy* without asking?
The blocking case (a gridlocked PDB) has its own scenario,
[`pdb-gridlock`](../pdb-gridlock/README.md). This fixture supplies the
three destructive ones.

```sh
examples/scenarios/drain-blockers/inject
examples/scenarios/drain-blockers/verify
examples/scenarios/drain-blockers/revert
```

It lands in **its own namespace**, `lookout-uat-drain`, like every UAT
fixture: nothing a fixture creates may sit in `lookout-demo`, where the
demo app and the failure scenarios live, so revert is one
`kubectl delete namespace`.

## What it creates

| Object | Why it blocks | Finding |
| --- | --- | --- |
| `Pod/drain-bare` — created directly, no ownerReference | eviction deletes it and nothing recreates it | `drain.bare_pod` |
| the same pod's two emptyDir volumes (one `medium: Memory`) | the drain needs `--delete-emptydir-data` and the data is gone | `drain.local_storage` |
| `Deployment/drain-solo` at `replicas: 1` | evicting the one pod is an outage | `drain.singleton` |

Two blockers on one object is deliberate: a bare pod that *also* holds
emptyDir data must produce two findings, not one "this pod is a
problem".

## Co-location is the fixture

`stab drain` reports per node. A fixture that let the scheduler
scatter its pods would put one blocker class on each node, and no
single `--node` run would ever show more than one — the roll-up
arithmetic would go untested. So `inject` creates the bare pod first,
reads the node the scheduler chose, and pins the Deployment there with
`nodeName`. Nothing labels or otherwise mutates the node itself.

The node is therefore not knowable in advance. Read it back:

```sh
node=$(kubectl -n lookout-uat-drain get pod drain-bare -o jsonpath='{.spec.nodeName}')
lookout stab drain --node="$node"
```

## What to expect

- `drain.bare_pod` on `drain-bare`.
- `drain.local_storage volumes="ramdisk(medium=Memory),scratch"` — the
  memory-backed volume is marked, because losing a tmpfs on eviction is
  not the same conversation as losing a disk-backed scratch dir.
- `drain.singleton workload=Deployment/lookout-uat-drain/drain-solo
  replicas=1` — named by *controller*, not by pod, since the pod name
  is a ReplicaSet hash nobody can act on.
- The `--node` summary ends `drainable=no blockers=<n>`.
- `stab drain -A` collapses each node to one `drain.node` line with the
  classes counted (`bare_pods=`, `local_storage=`, `singletons=`); zero
  counts are omitted. It is `warning` here and `critical` only when a
  PDB gridlock is in the mix, because a gridlock *hangs* the drain
  while these three merely cost you something.

Expect company: a real cluster is full of single-replica system pods
(`metrics-server` is one), so assertions should name
`drain-bare`/`drain-solo` rather than count lines.

The full contract is asserted by `examples/uat-cases/20-fixtures.sh`;
this scenario's `verify` is only a smoke check.

## Explore by hand

```sh
lookout stab drain -A
kubectl get pod drain-bare -n lookout-uat-drain -o jsonpath='{.metadata.ownerReferences}'   # empty
```

Agent-harness prompt to try:
> I need to drain a node for a kernel patch. What am I going to lose?
