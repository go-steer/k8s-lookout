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

- `state edges --workload=Deployment/kwok-scenario-endpoints/unplugged-00`
  → `edge.selector_empty severity=critical name=unplugged-00`.
- **Nothing at all** about `wired-00` … `wired-08`.

## Two gaps this scenario holds open

Both were found by running it, both are product defects rather than
fixture problems, and both are `soft` in `verify` — a driver that goes
red on a known defect stops being a signal. They become hard assertions
when the defects are fixed.

**A bare `scan` never sees this.** Stage 2 drills only into workloads
stage 1 already flagged, and all twelve Deployments here are Available
with every pod Running, so `drilldown=0` and `scan` reports nothing.
Three Services routing nowhere behind twelve healthy workloads is
exactly the "something is wrong and I don't know what" case `scan` is
the entry point for. It does surface under `--include=all`, but only as
a side effect: the posture checks flag every workload at warning, and
*that* is what triggers the drill-down.

**`edge.selector_empty` misattributes.** The kind is documented as *"a
Service selector aimed at **this workload** selects zero pods"*, and
the intent heuristic (`selectorIntends` in
`pkg/checks/state/edges_checks.go`) tests only that the selector's
label *keys* exist among the workload's pod labels — the values are
never compared. Every workload here uses the key `app`, so querying the
perfectly sound `wired-00` reports all three `unplugged-*` Services and
stamps `workload=Deployment/…/wired-00` on findings that have nothing
to do with it. With one workload per namespace — the only shape
`examples/scenarios/` can build — key-matching and
workload-identification are the same test, and the bug is invisible.

## Explore by hand

```sh
kubectl -n kwok-scenario-endpoints get svc,endpointslices
lookout scan --namespace=kwok-scenario-endpoints
lookout state edges --workload=Deployment/kwok-scenario-endpoints/unplugged-00
```

Agent-harness prompt to try:
> Traffic to some services in kwok-scenario-endpoints gets connection
> refused, but every deployment reports Available. Which ones are
> broken?
