# pending — a pod no node can ever schedule

Deploys `hog`, requesting 64 CPUs. It parks Pending/Unschedulable.
On kind there is no autoscaler to save it; the sentinel's capacity
source ages it (`--pending-age`, examples value 75s) and fires
`capacity.pending-aged` — the "this is not just a slow scheduler"
signal. On a GKE cluster with the `-gke` image and the quota source,
the same shape drives the quota-exhaustion drill
(dev/drills/quota-exhaustion.md).

```sh
examples/scenarios/pending/inject
examples/scenarios/pending/verify
examples/scenarios/pending/revert
```

## What to expect

- **Sentinel (wire)** — `kind=capacity.pending-aged` after
  ~75s + one capacity poll (60s), naming the pod and how long it has
  been unschedulable.
- **Read-path** — `lookout health` scores the pending category
  degraded; `lookout triage events --namespace=lookout-demo` shows
  the scheduler's `FailedScheduling` verdicts.

## Explore by hand

```sh
lookout triage delta --namespace=lookout-demo --only=pods
lookout triage spec --workload=Deployment/lookout-demo/hog   # the impossible request
```

Agent-harness prompt to try:
> Something in lookout-demo has been stuck Pending for a while. Will
> the cluster ever place it, and why not?
