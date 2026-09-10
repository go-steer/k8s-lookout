# hpa-thrash — an autoscaler fighting itself

A **UAT fixture**, not a failure scenario: nothing here breaks and
nothing is expected on the wire. It exists because `lookout triage
events` has an HPA-thrash detector that a healthy cluster can never
exercise — a stock kind cluster has no HPA at all, and an HPA that is
scaling *correctly* produces exactly the same event stream shape as one
that is oscillating, minus the direction changes.

```sh
examples/scenarios/hpa-thrash/inject
examples/scenarios/hpa-thrash/verify
examples/scenarios/hpa-thrash/revert
```

It lands in **its own namespace**, `lookout-uat-hpa`, like every UAT
fixture, so revert is one `kubectl delete namespace`.

## What it builds

| Object | Shape | Why |
| --- | --- | --- |
| `Deployment/web` + `HorizontalPodAutoscaler/web` | min 1, max 6, cpu 50% | the subject; `scaleTargetRef` is what `target=` joins against |
| 4 `SuccessfulRescale` events on `web` | sizes **3 → 1 → 3 → 1** at t−8m…t−2m | three moves, **two direction changes** — exactly `--hpa-flips=2` |
| `Deployment/ramp` + `HorizontalPodAutoscaler/ramp` | identical | the negative control |
| 3 `SuccessfulRescale` events on `ramp` | sizes **2 → 4 → 6** at t−9m…t−5m | enough points to analyze, **zero** direction changes |

Both HPAs sit at `minReplicas=1` over a one-replica Deployment, so
neither controller has any reason to move: the history below is the
only scale activity in the namespace and nothing live can perturb it.

## The synthesized history, and why

The rescale events are **written by the fixture**, not produced by a
real oscillation. This is the one place in `examples/scenarios/` where a
fixture does not reproduce the mechanism it tests, so it is worth being
explicit about the trade.

`triage events` recovers the replica sequence from the controller's
`SuccessfulRescale` messages, because the HPA object keeps no history
(`pkg/checks/events/hpa.go`). Provoking a real oscillation means
alternating a CPU burst against a **5-minute scale-down stabilization
window**, so the cheapest genuine up→down→up costs ~15 minutes of wall
clock — and still only supports a fuzzy `flips >= 2`, because the real
sizes depend on how the load lands.

What stays real: the HPA objects, their `scaleTargetRef`s (so `target=`
is a live read of a live join), the Deployments they point at, and the
message format, copied verbatim from the controller — `New size: N` is
the actual parsing contract, and a change to it would break this fixture
in exactly the way it breaks the detector.

All seven events are timestamped inside the **1h default `--since`**
lookback and the two flips inside the **30m default `--hpa-window`**, so
the defaults are what is under test.

## What to expect

```sh
lookout triage events --workload=Deployment/lookout-uat-hpa/web
```

- `kind=event.hpa_thrash severity=warning name=web` with
  `replicas=3->1->3->1 flips=2 window=30m0s target=Deployment/web`.
- Nothing at all for `ramp` — a monotonic ramp, however fast, has no
  direction change to count.
- `--hpa-flips=3` or `--hpa-window=1m` → the finding disappears. Both
  thresholds are real, and asserting only the positive direction would
  not show that.

The full contract is asserted by `examples/uat-cases/50-t1.sh`; this
scenario's `verify` is only a smoke check that the history landed.

## Not covered here

`autoscaling.hpa_pinned` (held at max for 10m+) and
`autoscaling.hpa_metrics_dead` (metrics-server broken for 15m) are
**sentinel-source** signals, not read-path findings: they need a running
sentinel and a sustained hold, so they belong to the watch-path e2e
layer rather than to this fixture.

## Explore by hand

```sh
lookout triage events --namespace=lookout-uat-hpa
lookout triage events --workload=Deployment/lookout-uat-hpa/web --hpa-flips=3
kubectl -n lookout-uat-hpa get events --field-selector reason=SuccessfulRescale
```
