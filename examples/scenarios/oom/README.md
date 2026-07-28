# oom — a memory leak that hits the container limit

Deploys `leaker` running dev/drills/memory-leaker.py at drill speed
(+8 MiB every 2s against a 64Mi limit → OOMKill in ~30s, then
restarts, then restart backoff). The slow variant of this same script
is the memory-leak drill for the saturation source's linear forecast
(dev/drills/memory-leak.md) — this scenario is the fast, reactive
cousin.

```sh
examples/scenarios/oom/inject
examples/scenarios/oom/verify
examples/scenarios/oom/revert
```

## What to expect

- **Sentinel (wire)** — `kind=k8s-event` `reason=BackOff` for the
  restart loop (OOM kills look like crashes at the event altitude);
  once restarts accumulate inside the window, the leading
  `objectstate.restart_burst` joins the same dedup family.
- **Read-path** — `lookout triage events --namespace=lookout-demo`
  shows the restarts; `lookout triage spec
  --workload=Deployment/lookout-demo/leaker` flags the limit the
  container keeps dying against; `kubectl get pod` shows
  `OOMKilled` as the last termination reason.

## Explore by hand

```sh
lookout triage logs --workload=Deployment/lookout-demo/leaker   # 'leaker: holding N MiB' ramp
lookout bundle --workload=Deployment/lookout-demo/leaker
```

Want the LEADING version instead (forecast minutes before the kill)?
Run the leaker at its defaults (+1 MiB / 30s) and watch for
`kind=saturation.forecast` — that's dev/drills/memory-leak.md.

Agent-harness prompt to try:
> The leaker pod in lookout-demo keeps dying. Is it a crash or a
> resource problem, and what limit is it hitting?
