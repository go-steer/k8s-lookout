# endpoints-empty — three services that route nowhere, among nine that work

Twelve Services in front of twelve healthy two-replica Deployments.
Three of them (`unplugged-*`) select `app: <name>-v2` while their pods
are labelled `app: <name>` — one label value off, which is the shape a
rename or a copy-paste actually produces rather than an obvious typo.
Every pod is Ready; every Deployment is Available. Nothing in
`kubectl get deploy` looks wrong.

```sh
examples/kwok/scenarios/endpoints-empty/inject
examples/kwok/scenarios/endpoints-empty/verify
examples/kwok/scenarios/endpoints-empty/revert
```

## Why this is the scale tier and not `examples/scenarios/`

The failure itself is small — the *discrimination* is what needs a
fleet. `scan` is the "something is wrong and I don't know what" entry
point, and the useful claim is:

> three broken Services are named, and the nine sound ones sitting right
> beside them, built from the identical template, are not.

With one Service in the cluster a detector that reports everything
scores the same as one that reports the right thing. `verify` fails if
any `wired-*` name appears in the output.

It also exercises the seam kwok gets right for free: the **real**
EndpointSlice controller reading **real** pod IPs and Ready conditions
off fake pods. Nothing about the endpoints path is simulated, so the
empty slice is empty for the true reason. `verify` asserts that
directly — 2 addresses behind `wired-00`, 0 behind `unplugged-00` —
before it asserts anything about lookout.

## What to expect

- `scan --namespace=kwok-scenario-endpoints` → the three
  `unplugged-*` Services, `severity=critical`, with `drilldown=0`.
- `state edges --workload=Deployment/kwok-scenario-endpoints/unplugged-00`
  → `edge.selector_empty severity=critical name=unplugged-00`, and the
  label the pods actually carry.
- **Nothing at all** about `wired-00` … `wired-08`, from either.

## Two defects this scenario found, both now fixed

Both were found by running it, both were product defects rather than
fixture problems, and both were `soft` in `verify` while they were open
— a driver that goes red on a known defect stops being a signal. Both
are hard assertions now. Filed as
[#331](https://github.com/go-steer/k8s-lookout/issues/331) and
[#332](https://github.com/go-steer/k8s-lookout/issues/332).

**A bare `scan` never saw this (#331).** Stage 2 drilled only into
workloads stage 1 had already flagged, and all twelve Deployments here
are Available with every pod Running, so `drilldown=0` and `scan`
reported nothing. Three Services routing nowhere behind twelve healthy
workloads is exactly the "something is wrong and I don't know what"
case `scan` is the entry point for. It did surface under
`--include=all`, but only as a side effect: the posture checks flag
every workload at warning, and *that* is what triggered the drill-down
— so fixing the posture hid the incident. Stage 2 now ends with a
target-free sweep for the edge faults no workload owns, and runs even
when stage 1 found nothing.

**`edge.selector_empty` misattributed (#332).** The intent heuristic in
`pkg/checks/state/edges_checks.go` tested only that the selector's
label *keys* existed among the workload's pod labels — the values were
never compared. Every workload here uses the key `app`, so querying the
perfectly sound `wired-00` reported all three `unplugged-*` Services and
stamped `workload=Deployment/…/wired-00` on findings that had nothing
to do with it. With one workload per namespace — the only shape
`examples/scenarios/` can build — key-matching and
workload-identification are the same test, and the bug was invisible.
The heuristic now scores label *values* and only the best match in the
namespace claims a Service; a Service nobody can be shown to own is
reported unattributed by the `scan` sweep instead.

## Explore by hand

```sh
kubectl -n kwok-scenario-endpoints get svc,endpointslices
lookout scan --namespace=kwok-scenario-endpoints
lookout state edges --workload=Deployment/kwok-scenario-endpoints/unplugged-00
# or from the side the evidence arrives on:
lookout state edges --workload=Service/kwok-scenario-endpoints/unplugged-00
```

Agent-harness prompt to try:
> Traffic to some services in kwok-scenario-endpoints gets connection
> refused, but every deployment reports Available. Which ones are
> broken?
