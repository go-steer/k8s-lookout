# pdb-gridlock — a PodDisruptionBudget with zero headroom

Scales `api` from 2 replicas to 1. Its PDB says `minAvailable: 1`, so
`disruptionsAllowed` drops to 0: nothing is broken *today*, but the
next node drain, upgrade, or eviction will either hang or take the
service down. A classic pre-maintenance landmine.

```sh
examples/scenarios/pdb-gridlock/inject
examples/scenarios/pdb-gridlock/verify
examples/scenarios/pdb-gridlock/revert
```

## What to expect

- **Sentinel (wire)** — `kind=objectstate.pdb_gridlocked` on the
  transition to `disruptionsAllowed=0`.
- **Read-path** — `lookout stab drain -A` reports the gridlocked PDB
  as a drain blocker (`disruptions_allowed=0`, the covered pods, and
  which node is stuck behind them) — the pre-maintenance check the
  cluster-health skill runs before a drain.

## Explore by hand

```sh
lookout stab drain -A
lookout state edges --workload=Deployment/lookout-demo/api
```

Agent-harness prompt to try:
> We're about to upgrade the nodes in this cluster. Is anything going
> to block the drains?
