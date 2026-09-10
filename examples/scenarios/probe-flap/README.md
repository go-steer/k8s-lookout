# probe-flap — a pod that fails without ever crashing

Deploys `flapper`, whose readiness endpoint alternates 503 and 200 on
a 20-second cycle. With `periodSeconds: 2` and both thresholds at 1,
the readiness gate flips roughly every 20s. There is deliberately **no
liveness probe**: the container never restarts, the pod is never
`NotReady` long enough for the reactive `Unhealthy` path, and the
Deployment reports `1/1` about half the time.

```sh
examples/scenarios/probe-flap/inject
examples/scenarios/probe-flap/verify
examples/scenarios/probe-flap/revert
```

## What to expect

- **Sentinel (wire)** — a `k8s-event` (reason `Unhealthy`) opens the
  incident, then `degradation.probe_flap` is reattached to it as a
  `kind=family.member` once the gate has flipped `FlapCount` (4) times
  inside `--degradation-window` (15m). At a 20s cycle that is ~80s of
  flapping. **One session, not two.**
- **On revert** — deleting the Deployment closes the incident with
  `kind=resolved`, `resolution=object_deleted` — the arm of the
  resolved contract that fix-in-place scenarios never reach.

## Why this one matters

Two reasons, and the second was a surprise.

**It never stops.** Every symptom the basic scenarios assert on
(CrashLoopBackOff, OOMKilled, ImagePullBackOff) is a container that
died. This one doesn't. `restartCount` stays 0, `kubectl get pods`
looks fine on about half of all polls, and the only externally visible
effect is the Service dropping and re-adding the endpoint every 20
seconds — intermittent 503s to callers, nothing at all to the cluster.
`verify` asserts `restartCount == 0` alongside the signal, so a
liveness probe creeping into `flapper.yaml` can't quietly turn this
into a second crashloop test.

**It is the suite's only ancestor reattachment.** This scenario was
written expecting the reactive path to stay silent, and it does not:
a probe that fails often enough to flip the gate four times also
drives the k8s Event's `Count` past `unhealthyMinCount` (3), so
`Unhealthy` opens a session before `degradation.probe_flap` is ready
to fire. What the sentinel then does is the interesting part. The
leading signal is a warning, so it would normally be buffered into a
watchboard digest entry — but its blast-radius ancestor (the Node the
pod is on) already owns a live incident, so §7.7 delivers it *there*,
as a `kind=family.member` followup, instead of paging twice. The wire
says so in as many words:

> blast-radius join: `degradation.probe_flap` … shares the ancestor
> `Node//lookout-examples-worker` with this session's incident — the
> same failure seen from a different altitude, reattached here instead
> of opening a watchboard digest entry; at most one per source family
> per incident per window

`verify` pins both halves: the reattachment, and that exactly one
session id carries both signals.

Note this is the *blast-radius* flavour of `family.member`, not the
cross-source-family flavour (`"message":"cross-source join: …"`) that
e.g. `capacity.pending-aged` joining a `FailedScheduling` event
produces. Same kind on the wire, two different seams behind it.

If you want a probe flap with genuinely no reactive event behind it,
an HTTP probe cannot get you there — the Event `Count` accumulates
across dips. It needs `spec.readinessGates` and a controller toggling
the condition, with no kubelet probe involved at all.

## If it times out looking for the family.member

Check for an open storm before suspecting the degradation source. A
storm keyed on the pod's node or namespace attaches `probe_flap` as a
`storm.member` instead — the member's session is suppressed and no
reattachment is emitted, which on the wire is indistinguishable from
the source never firing. The sentinel log says which happened:

```sh
kubectl -n agent-triage logs deploy/lookout-watch | grep -E 'storm attach|probe_flap'
```

`storm attach probe_flap … → Namespace lookout-demo` is the tell. See
examples/README.md § Storms leak across scenarios.

## Timing note

`FlapCount` (4) comes from `degradation.DefaultConfig()` and is not
flag-tunable; the counting window is `--degradation-window` and is.
Shortening the window makes the scenario *harder*, not faster — the
flips have to fit inside it.

## Explore by hand

```sh
kubectl -n lookout-demo get pods -l app.kubernetes.io/name=flapper -w
lookout health
lookout state edges --namespace=lookout-demo
```

Agent-harness prompt to try:
> Callers are seeing intermittent 503s from something in lookout-demo
> but every pod shows Running with zero restarts. What is going on?
