# Memory-leak drill — GKE staging replay

Replays the second half of the M3 exit criterion (DESIGN.md §14: "a
staged memory leak … opens sessions before user-visible failure")
against a **real GKE cluster**: a slow leaker under a memory limit must
produce a `saturation.forecast` — ETA and confidence basis attached —
while the pod is still Running and Ready, minutes before the kernel
OOM-kills it. The kind-cluster original (forecast ETA accurate to 31
seconds, critical session 14 minutes before the OOM) is in
[`docs/milestones/M3.md`](../../docs/milestones/M3.md).

> **STAGING CLUSTERS ONLY.** The leaker is confined by its own limit and
> hurts nothing else, but the drill's point is to let a container die —
> don't colocate it with anything latency-sensitive if you plan to watch
> the OOM land.

## Prerequisites

- A GKE **staging** cluster. GKE ships metrics-server; nothing to
  install (on kind you must install metrics-server with
  `--kubelet-insecure-tls` first — the saturation source's §11 probe
  fails loudly without a metrics API, by design).
- A daemon or the capture stub ([`stub-daemon.py`](./stub-daemon.py)),
  and the sentinel deployed per
  [`bad-deploy.md`](./bad-deploy.md) step 1 — same flag set works for
  both drills; the one that matters here is the source list including
  `saturation` (and `--saturation-window`, see below).
- [`memory-leaker.py`](./memory-leaker.py) from this directory — the
  drill fixture: appends touched 1-MiB blocks forever, tunable rate.

## 1. Window vs drill time — decide before deploying

A forecast needs samples spanning **half the regression window** (§13's
insufficient-window gate). The production default `--saturation-window=90m`
therefore needs ~45 minutes of history before the first forecast can
exist — honest for a real trend, wrong for a drill. The recorded run
used `--saturation-window=10m` (first forecast possible ~5 minutes in)
and the payloads correspondingly carry `confidence_basis=linear-10m-window`;
the default emits the §8 `linear-90m-window`. Same math, parameterized
basis string — say which one you ran.

## 2. The leaker

Deploy the fixture with a limit and a rate that keep the ETA inside the
warning band ([15m, 60m)) when the forecast first fires — warning first,
then the escalation to critical as the ETA shrinks below 15m, is the
full §7.3 hysteresis story:

```
kubectl create ns leaklab
kubectl -n leaklab create configmap leaker-script --from-file=memory-leaker.py=dev/drills/memory-leaker.py
kubectl -n leaklab apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: leaker
spec:
  containers:
    - name: leaker
      image: python:3.12-alpine
      command: ["python", "/opt/leak/memory-leaker.py", "1", "30"]
      resources:
        requests: {memory: 32Mi, cpu: 10m}
        limits: {memory: 64Mi, cpu: 100m}
      volumeMounts: [{name: script, mountPath: /opt/leak}]
  volumes:
    - name: script
      configMap: {name: leaker-script}
EOF
date -u   # leak start
```

+1 MiB / 30s against a 64Mi limit ≈ 1.6% of the limit per default
sample interval: with a ~15Mi python baseline that puts the first
forecast ETA around ~22m (warning), critical ~15 minutes in, OOM ~25–30
minutes in. Scale rate and limit together if you want it faster — keep
the OOM > 15 minutes out so the "before failure" margin is unarguable.

## 3. The evidence — capture at signal time, not after

Watch the sentinel log / stub capture for `saturation.forecast`
(`reason=forecast_memory`). **The moment it fires**, capture the pod's
health — this is the exit criterion:

```
kubectl -n leaklab get pod leaker \
  -o jsonpath='{.status.phase} {.status.containerStatuses[0].ready} {.status.containerStatuses[0].restartCount}'
# expect: Running true 0
```

Expected sequence (kind timings for comparison):

- **+~6m** — warning forecast (watchboard digest by default routing);
  pod Running/Ready/0 restarts.
- **later** — ONE escalation to critical when the ETA crosses 15m: a
  per-incident session whose initial inject carries the wire attachment
  `forecast: {eta: <RFC3339>, confidence_basis: linear-<window>-window}`
  plus current/limit/slope/sample-count evidence in the message. Same
  severity never re-fires (hysteresis latch).
- **+~25m** — `OOMKilled`, restartCount 0→1: the first failure marker.
  Capture `lastState.terminated` and compare `finishedAt` against the
  forecast's `eta` — the kind run was 31 seconds apart. Letting the OOM
  land is optional once the signal-while-healthy evidence is captured,
  but it is the half that makes the record self-proving.

Recovery epilogue: after the OOM restart the leaker starts leaking
again from zero — clearance for `forecast_*` incidents is ETA recession
(beyond 2× the warn threshold) or a non-positive slope held for a full
re-observation period, deliberately NOT pod readiness (a leaking pod is
Ready right up to the kill). Delete the pod to close the incident as
`object_deleted`, or fix the "leak" (restart with a lower rate) and
watch the recede-clearance produce `kind=resolved`.

## 4. Post-mortem (optional, shares drill C)

The forecast rows land in the store with their ETA and routing outcome
(§9.1); `triage top` (live) shows the leaker's usage-vs-limit percent
climbing while it runs, and after the fact the store-copy + `--at`
procedure in [`bad-deploy.md`](./bad-deploy.md) step 5 answers "what
else shared that node at forecast time" (`triage radius leaker -n
leaklab --at=<forecast time> --store=…`).

## 5. Cleanup

```
kubectl delete ns leaklab
```

Restore `--saturation-window` to the default if you changed it, and file
the captured forecast + OOM timestamps with the drill record — the
forecast-vs-OOM delta is the number the milestone claim rests on.
