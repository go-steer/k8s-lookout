# pdb-gridlock — three landmines among nine sound PDBs

Twelve Deployments, each with a PodDisruptionBudget of the identical
shape. Three of them (`stuck-*`) set `minAvailable` equal to their
replica count, so `disruptionsAllowed` is 0 and the eviction API will
refuse forever. Nine (`ok-*`) keep one replica of headroom.

```sh
examples/kwok/scenarios/pdb-gridlock/inject
examples/kwok/scenarios/pdb-gridlock/verify
examples/kwok/scenarios/pdb-gridlock/revert
```

## Why this is the scale tier and not `examples/scenarios/`

The kind scenario proves lookout notices *a* gridlocked PDB. Two
different claims need a fleet:

- **Attribution.** `stab drain -A` rolls up per node. The interesting
  output is not "there is a gridlock" but "these particular nodes are
  the ones you cannot drain", picked out of a hundred. On two workers
  every blocker lands on one of two nodes and the attribution is
  vacuous.
- **The negative.** Nine sound PDBs of the identical shape sit right
  beside the three broken ones. Reporting the needle is easy when the
  haystack has one straw in it; `verify` fails if any `ok-*` PDB is
  named. That half is the one that actually costs something to get
  right, and it is unassertable on a cluster with a single PDB.

## What to expect

- `stab drain -A` → `kind=drain.node severity=critical` with
  `pdb_gridlock=1` on each node holding a `stuck-*` pod.
- `stab drain --node=<one of them>` → `kind=drain.pdb_gridlock`
  naming the pod.
- **Nothing at all** about `ok-00` … `ok-08`.

## Explore by hand

```sh
kubectl -n kwok-scenario-pdb get pdb
lookout stab drain -A
lookout stab drain --node="$(kubectl -n kwok-scenario-pdb get pods -l app=stuck-00 \
  -o jsonpath='{.items[0].spec.nodeName}')"
```

Agent-harness prompt to try:
> We're about to roll every node in this cluster. Which nodes will hang,
> and what do I have to change first?
