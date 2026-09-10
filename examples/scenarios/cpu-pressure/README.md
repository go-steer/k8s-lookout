# cpu-pressure — containers held at a known percentage of their limits

A **UAT fixture**, not a failure scenario: nothing is expected on the
wire and no sentinel needs to be running. Tier **T1** — it needs
metrics-server, which `examples/kind/up` installs.

`lookout triage top` rates every container as a percentage *of its own
limit* and emits nothing below `--top-warn` (default 80). That makes it
unassertable on a shared cluster: whatever is hot at the moment is
whatever the last scenario left behind. This fixture stages one
namespace, `lookout-uat-top`, whose percentages are known by
construction.

```sh
examples/scenarios/cpu-pressure/inject
examples/scenarios/cpu-pressure/verify
examples/scenarios/cpu-pressure/revert
```

## What it stages

| Pod | Spec | Lands in |
| --- | --- | --- |
| `cpuhog` | `limits.cpu: 200m`, runs `yes >/dev/null` | `top.saturation` `resource=cpu` `reason=CPUNearLimit`, ~100%, **warning** |
| `memhog` | `limits.memory: 128Mi`, holds ~100Mi resident | `top.saturation` `resource=memory` `reason=MemoryNearLimit`, ~86%, warning |
| `nolimits` | no limits, no requests, idle | `top.unlimited` (and `top.unrequested`) census — never a saturation row |
| `steady` | limits set, near idle | nothing — the negative control |

The control is the point. A `top` that names two saturated containers in
a namespace containing two saturated containers is indistinguishable
from one that names everything it samples, until there is a
limited-but-quiet container sitting next to it that it leaves alone.

`nolimits` is the second control, and a different kind: it has no
denominator, so no percentage exists at any usage. It must be *counted*
by the census and never *rated*. Note that a `LimitRange` on this
namespace would defeat it — the pod would inherit defaults and stop
being unlimited — which is why the workstation-protection caps live at
the docker layer (`cap_kind_nodes` in `examples/lib.sh`) and not here.

## Why every hog is limit-bound

Because the version of this fixture that wasn't took the workstation
down. Run uncapped, `cpuhog` takes a whole core rather than 200m; with a
second cluster up from a worktree the box reached load ~28 of 32, and
what starved first was the IDE's own server — the terminal watching the
run died with it, and the inject was SIGKILLed partway through its
`kubectl apply`.

So the limits here do double duty: they are what makes the percentages
assertable *and* they are the fixture's own blast-radius control. The
node caps are the independent second layer, for the case where a future
fixture forgets.

`nolimits` is the one container with no bound, and it is idle for
exactly that reason — `nolimits` describes the spec, not the workload.

## Why memhog stops at ~86%

`judge` sends memory ≥95% to **critical**, and 95% of 128Mi leaves ~6Mi
of headroom before the kernel kills the container. Aiming there would
make this a flaky OOM scenario, which `examples/scenarios/oom/` already
owns properly. ~86% is past `--top-warn` and clear of the critical band.

CPU has no equivalent risk and needs no equivalent care: over-limit CPU
is throttled, never killed, so `cpuhog` can sit pinned at its quota
indefinitely — and `judge` caps CPU at warning for the same reason, so
even at 100% it never reports critical.

The ballast is touched page by page because the limit is enforced on
*resident* memory: an untouched `bytearray` is virtual, never faults in,
and reports ~0% however large it is.

## Why the inject waits on `kubectl top`

`triage top` reads `metrics.k8s.io`, and metrics-server serves nothing
for a pod until it has scraped it at least once (~15s resolution, longer
right after start). Waiting on pod readiness is not enough — the
assertion would run against an empty sample and fail on timing rather
than behaviour. The inject gates on the numbers the assertions use:
cpuhog past 150m of 200m, memhog past 100Mi of 128Mi.

## What to expect

```sh
lookout triage top --namespace=lookout-uat-top
lookout triage top --namespace=lookout-uat-top --show-unlimited
lookout triage top --namespace=lookout-uat-top --all
```

- Two `top.saturation` findings, both **warning**: `cpuhog` on `cpu`
  near 100%, `memhog` on `memory` near 86%. Neither is critical.
- One `top.unlimited` census line counting `nolimits`;
  `--show-unlimited` adds a `top.unlimited_container` row naming it.
- Nothing about `steady` at any point — until `--all`, which dumps every
  sampled row at info severity and should then show it near zero.

## Explore by hand

```sh
lookout triage top --namespace=lookout-uat-top --top-warn=50
kubectl -n lookout-uat-top top pod --containers
```

Agent-harness prompt to try:
> Is anything in the lookout-uat-top namespace about to hit a resource
> limit, and is anything there running without one?
