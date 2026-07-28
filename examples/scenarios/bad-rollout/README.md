# bad-rollout — the flagship: a user-invisible bad deploy

The examples' replay of the M3 exit criterion and the
dev/drills/bad-deploy.md runbook, automated on kind. `web` runs
`maxUnavailable=0 maxSurge=1`, so rolling it to a valid-but-broken
image (different tag AND a crashing entrypoint — the image string must
change so `triage changes` has a rollout to name) parks one crashing
surge pod NEXT TO the healthy revision:

- users never see an error (verify proves 5×200 through the Service
  mid-stall, from the in-cluster vantage pod),
- pod-level `BackOff` sessions fire at the wrong altitude within
  seconds, and
- at `--rollout-observe` (examples value: 90s) the Deployment-altitude
  `rollout.stall` fires: `new_ready=0/1 old_ready=2/2 … probable bad
  deploy`. The examples sentinel promotes it to critical
  (`--severity=rollout.stall=critical`), so it opens its own enriched
  session instead of riding the watchboard digest.

```sh
examples/scenarios/bad-rollout/inject
examples/scenarios/bad-rollout/verify
examples/scenarios/bad-rollout/revert    # rollout undo + waits for kind=resolved
```

## What to expect

- **Sentinel (wire)** — `kind=k8s-event` pod noise first, then
  `kind=rollout.stall` at ~90s; after revert, `kind=resolved`
  (`resolution=recovered`) lands in the stall's session — the closed
  loop, no agent polling.
- **Read-path** — `lookout health` scores the rollouts category
  degraded while the stall is open.

## Explore by hand

```sh
lookout bundle --workload=Deployment/lookout-demo/web    # the incident's first call
lookout triage changes --workload=Deployment/lookout-demo/web
lookout triage delta --namespace=lookout-demo
```

Agent-harness prompt to try:
> We just shipped web in lookout-demo and dashboards look fine, but
> the sentinel opened an incident. Is it real? Should we roll back?
