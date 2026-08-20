# logs — distilling a fleet's log streams

Twenty-four fake pods, two containers each, both serving a log file
kwok reads off the controller's filesystem. The app container throws a
Java exception with a `Caused by`; the gateway container serves an
access log that is half `kube-probe` noise and ends in a Go panic.

```sh
examples/kwok/scenarios/logs/inject
examples/kwok/scenarios/logs/verify
examples/kwok/scenarios/logs/revert
```

## Why this is the scale tier and not `examples/scenarios/`

`lookout triage logs` is a *reduction*: forty-eight streams in, a
handful of templates out. On the kind cluster there is no reduction to
observe — ten pods, one container each, and every finding is a pod. The
claim worth asserting only exists at fleet size:

> twenty-four pods emit the same line and it arrives as **one** finding
> that counts them (`pods=24`), not as twenty-four findings.

A two-pod cluster cannot tell those two behaviours apart.

## What to expect

Read-path only — the sentinel does not watch logs.

- `log.stacktrace` `reason=JavaException` `lang=java`, frames collapsed
  to `com.example.db.Pool.acquire < …`.
- `log.stacktrace` `reason=GoPanic` `lang=go` from the *other*
  container of the same pod — two runtimes, one workload, and the
  detector has to keep them apart.
- `log.probe_noise` counting the stripped `kube-probe` lines. Stripped,
  never silently swallowed.
- `log.overflow` under `--max-templates=2`, naming what it dropped.
- `log.template` with `pods=24`.

## How the fake logs work, and the three ways to get them wrong

kwok serves a pod's logs from a **file on the controller's
filesystem**, named by a `ClusterLogs` CR. `examples/kwok/up` mounts one
ConfigMap at `/logs`; `inject` adds a key to it and `revert` takes it
away, so publishing a stream never needs a controller rollout.

Three things are load-bearing, and each fails in a way that looks like
something else:

1. **`enableDebuggingHandlers: true`.** The kwok release config leaves
   it off, and with it off `/containerLogs/` answers *"Debug endpoints
   are disabled."* for every request — the CRDs install fine and match
   fine, and nothing serves. `up` sets it.
2. **The CRI log format**, `<RFC3339Nano> <stdout|stderr> <F|P> <msg>`.
   Anything else is rejected outright: *"unsupported log format"*. The
   `cri_log` helper in `examples/kwok/lib.sh` is the only sanctioned
   way to write a line.
3. **Fresh timestamps.** Every read path asks the kubelet for a
   `--since` window (an hour, by default), so a fixture stamped at
   authoring time is filtered out and the scenario reads as *healthy* —
   the worst possible failure, because it is green. `cri_log` takes an
   age in seconds and stamps relative to now.

And one thing outside this scenario entirely: the request only arrives
because the kwok controller runs with `hostNetwork`, which is what
makes a fake node's `InternalIP` an address the apiserver can actually
dial on port 10247. See the long comment in `examples/kwok/scale-up`.

## Explore by hand

```sh
kubectl -n kwok-scenario-logs logs deploy/noisy -c gateway | head -20
lookout triage logs --namespace=kwok-scenario-logs
lookout triage logs --namespace=kwok-scenario-logs --keep-probes
lookout triage logs --namespace=kwok-scenario-logs --max-templates=2
```

Agent-harness prompt to try:
> Something is wrong in the kwok-scenario-logs namespace. What are the
> applications actually complaining about, and how much of the log
> volume is just noise?
