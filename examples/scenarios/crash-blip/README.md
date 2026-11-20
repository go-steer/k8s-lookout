# crash-blip — negative weekly e2e scenario for `--settle` (#834)

Verifies that `--settle=2m` holds a transient crash-loop startup blip past
`--backoff-min-count=3`, opens no `k8s-event` session, and sweeps the held key
as `self_resolved` on `lookout_watch_gate_outcomes_total{gate="settle",outcome="self_resolved"}`.

## Calibration

Live GKE `1.37.1-gke.2097000` measurement (`shipmind-stage`, 2026-11-20): real
kubelet `BackOff` events (`source.component: kubelet`) reached `count=4` at
`lastTimestamp - firstTimestamp = 99s` (`18:07:21Z` → `18:09:00Z`), well inside
`client-go`'s 25-event per-object burst bucket. A 4-exit blip (`10s + 20s + 40s`
kubelet backoffs plus container startup/exit ≈ 75–99 s) therefore crosses
`--backoff-min-count=3` into `GateSettle` and stabilizes cleanly below
`SETTLE=2m` (`120s`), while a genuine crash loop crosses `2m` on its 5th
`BackOff` (~179 s) without spam-filter throttling.

## What happens

1. `inject` redeploys `examples/sentinel/up` with `SETTLE=2m` and creates a
   1-replica `crash-blip` Deployment in `lookout-demo` backed by an `emptyDir`
   counter file (`/state/count`). The container exits `1` on starts 1–4 and
   sleeps indefinitely on start 5.
2. `verify` waits for `lookout_events_filtered_total{gate="settle"} >= 1`,
   waits for the pod to reach `Ready`, waits for the settle hold tracker's
   `settle + 5m` sweep to record
   `lookout_watch_gate_outcomes_total{gate="settle",outcome="self_resolved"} >= 1`,
   and asserts zero `k8s-event` `BackOff`/`CrashLoopBackOff` injects for
   `crash-blip-` reached `stub-daemon`.
3. `revert` deletes `deploy/crash-blip` and redeploys `examples/sentinel/up`
   without `SETTLE` (restoring the binary's `--settle=0` default).

## Run manually

```sh
examples/e2e crash-blip
```
