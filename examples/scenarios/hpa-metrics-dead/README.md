# hpa-metrics-dead — autoscaling that silently isn't

Deploys `unscalable` with **no cpu request** and an HPA targeting
`averageUtilization: 70`. Utilization is a percentage *of the
request*, so with no request the HPA has nothing to compute against:
it reports `ScalingActive=False` with reason `FailedGetResourceMetric`
and stops making scaling decisions. The Deployment is Available, the
pod is Ready, `kubectl get hpa` shows an HPA that exists and looks
configured.

```sh
examples/scenarios/hpa-metrics-dead/inject
examples/scenarios/hpa-metrics-dead/verify
examples/scenarios/hpa-metrics-dead/revert
```

## What to expect

- **Sentinel (wire)** — `kind=autoscaling.hpa_metrics_dead`, warning,
  after `MetricsDeadSustain` (15m) of `ScalingActive=False` with a
  `FailedGet*` reason.
- **Read-path** — `lookout audit workloads` reports
  `kind=audit.hpa_cannot_scale reason=HPATargetMissingRequests` for
  the same HPA, **immediately**. Not `lookout health`: its scorecard
  is delta-backed over pods, nodes, system and quota, and a dead
  metric source is not a symptom on any pod, so `health` says nothing
  here and is right to. The pairing is the point — posture reads the
  defect off the spec in one call, the sentinel takes 15 minutes to
  observe the consequence.
- **On revert** — adding the cpu request brings `ScalingActive` back
  to True and the incident closes with `kind=resolved`. Note the
  clearance rule: it clears on `ScalingActive=True`, **not** merely on
  the reason changing away from `FailedGet*`. It also silences
  `audit workloads`: one edit fixes both altitudes.

## Why this one matters

This is the failure that costs money and shows up as an outage weeks
later, during the traffic spike the autoscaler was supposed to absorb.
Nothing is unhealthy today. There is no event, no restart, no
`NotReady`. The only observable is a status condition on an object
most dashboards do not render.

`inject` checks the reason is actually `FailedGet*` before handing off
to `verify`: `ScalingDisabled` and `InvalidSelector` are deliberately
**not** treated as metrics-dead, so a run that produced one of those
would burn the whole 15-minute window for nothing.

## Resource note

`unscalable` sets a memory request and limit but **no cpu request and
no cpu limit**. Only the dimension under test is left unset: the kind
nodes are capped at 4 cpu / 8g of docker and an unbounded-memory pod
can get the node container OOM-killed, which would surface as a fake
`objectstate.node_notready`. A cpu limit is not available as a
compromise — with no cpu request set, admission defaults the request
to the limit and the fault disappears. The workload is `sleep`, so cpu
stays bounded by being idle. See examples/README.md § The resource
budget a new scenario has to fit.

## Timing note

`MetricsDeadSustain` (15m) comes from `autoscaling.DefaultConfig()`
and is not exposed as a flag, so `verify` has a 1500s timeout and this
scenario stays **out** of `examples/e2e`'s default list. Run it
explicitly:

```sh
examples/e2e hpa-metrics-dead
```

Needs `metrics-server` (installed by `examples/kind/up`).

## Explore by hand

```sh
kubectl -n lookout-demo describe hpa unscalable
kubectl -n lookout-demo get hpa unscalable \
  -o jsonpath='{.status.conditions[?(@.type=="ScalingActive")]}'
lookout audit workloads --namespace=lookout-demo
```

Agent-harness prompt to try:
> We're about to run a load test. Is autoscaling actually working in
> this cluster, or just configured?
