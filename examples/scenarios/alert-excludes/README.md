# alert-excludes — UAT fixture for `--alert-excludes` (#836)

Demonstrates that an operator-declared `--alert-excludes` rule downgrades
matching signals to `severity=info` (`RouteStore`) without blinding the
sentinel: the cluster and namespace stay watched, no incident session opens,
`lookout_watch_alert_excluded_total` increments, and an `info` row is
persisted in `--store`.

## What happens

1. `inject` redeploys the sentinel through `examples/sentinel/up` with
   `ALERT_EXCLUDES` set to:

   ```yaml
   excludes:
     - kind: Pod
       namespace: lookout-demo
       reason: CrashLoopBackOff
   ```

   `examples/sentinel/up` creates ConfigMap `lookout-alert-excludes`, mounts it
   at `/etc/lookout/alert-excludes.yaml`, passes `--alert-excludes`, and
   enables `--store=/var/lib/lookout/lookout.db`. It then patches `worker` to
   exit non-zero on start (the same break as `crashloop`).
2. `verify` waits for `lookout_watch_alert_exclude_rules >= 1`,
   `lookout_watch_alert_excluded_total >= 1`,
   `lookout_info_dropped_total{kind="k8s-event"} >= 1`, and
   `lookout_store_records_total{route="info-stored"} >= 1`, checks that
   `lookout_watch_alert_exclude_refused == 0`, confirms the sentinel logged
   `info-store k8s-event lookout-demo/worker-`, and asserts zero `k8s-event`
   `BackOff`/`CrashLoopBackOff` injects reached the stub daemon.
3. `revert` restores `worker`, redeploys `examples/sentinel/up` without
   `ALERT_EXCLUDES`, and deletes ConfigMap `lookout-alert-excludes`.

## Run manually

```sh
examples/scenarios/alert-excludes/inject
examples/scenarios/alert-excludes/verify
examples/scenarios/alert-excludes/revert
```
