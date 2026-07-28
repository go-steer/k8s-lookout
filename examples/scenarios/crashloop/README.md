# crashloop — a container that exits on start

The classic. `worker`'s command is patched to exit(1) two seconds in;
kubelet restarts it, backoff kicks in, `BackOff` events flow.

```sh
examples/scenarios/crashloop/inject
examples/scenarios/crashloop/verify     # waits for the evidence below
examples/scenarios/crashloop/revert
```

## What to expect

- **Sentinel (wire)** — within ~1–2 minutes the stub log shows a
  per-incident session for the pod with `kind=k8s-event`
  `reason=BackOff` (kubelet's generic `BackOff` is canonicalized to
  the CrashLoopBackOff family by the message-aware classifier —
  docs/signal-schema-v1.md). Repeats within `--dedup-window` are
  deduped; a later `objectstate.restart_burst` may join the same dedup
  family rather than opening a second session.
- **Read-path** — `lookout triage events --namespace=lookout-demo`
  names the BackOff; once restarts accumulate, `lookout health` scores
  the crashloops category degraded with a `pod.restarts` finding.

## Explore by hand

```sh
lookout triage events --workload=Deployment/lookout-demo/worker
lookout bundle --workload=Deployment/lookout-demo/worker
lookout triage logs --workload=Deployment/lookout-demo/worker
```

Agent-harness prompt to try (see ../../agent-harness.md):
> Something keeps restarting in the lookout-demo namespace — find it
> and tell me why.
