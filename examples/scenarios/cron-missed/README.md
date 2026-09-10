# cron-missed — a CronJob that silently stops firing

Deploys a CronJob that runs every minute, waits for it to actually
run, then applies a `ResourceQuota` of `count/jobs.batch: "0"` so the
controller's Job creations start coming back `Forbidden`. The CronJob
object stays unsuspended, its schedule stays valid, and
`status.lastScheduleTime` simply stops advancing.

```sh
examples/scenarios/cron-missed/inject
examples/scenarios/cron-missed/verify
examples/scenarios/cron-missed/revert
```

## What to expect

- **Sentinel (wire)** — `kind=workload.cron_missed`, warning. Grace is
  5m on a 30s sweep, so the first miss lands ~5–6m after the first
  blocked activation. Three consecutive misses escalate it to
  critical: one miss is a hiccup, three is a schedule that is dead.
  Because the first miss is only a *warning*, it arrives buffered
  inside a `kind=watchboard.digest` payload rather than as its own
  per-incident inject — the digest session is then the one that
  carries the `kind=resolved`. `verify` matches the kind rather than
  the envelope, so either shape passes.
- **Read-path** — `lookout health` reports the stalled schedule, as
  `top="cron.missed lookout-demo/beat"`. Note the category it lands
  in: `category=crashloops`. That is not a typo in this README —
  `deltaCategory` (`pkg/checks/health/health.go`) has cases for
  `pod.pending`, `node.`, `addon.`, `quota.`, and
  `workload.`/`job.`, and everything else falls through to
  `crashloops`. `cron.*` has no case, so a CronJob that never fired is
  filed under crash loops while a Job that failed is filed under
  rollouts. `verify` matches on the object name rather than the
  category so it does not encode the oddity.
- **On revert** — deleting the quota lets the next minute's activation
  through; `lastScheduleTime` advances past the missed one and the
  incident closes with `kind=resolved`.

## Why this one matters

There is no failing pod to find. No Job exists, so nothing crashloops,
nothing OOMs, nothing goes Pending, and the backup/reconcile/cleanup
this CronJob was doing just quietly stops happening. It is the purest
example of a failure a pod-centric tool cannot see at all.

`verify` re-reads `lastScheduleTime` after the signal fires and fails
if it moved — otherwise a run where the quota did not actually stall
the schedule would look like a detection success.

## Timing note

`workload`'s Grace (5m) and `CriticalMisses` (3) come from
`workload.DefaultConfig()` and are **not** exposed as flags, so this
scenario cannot be sped up the way `--rollout-observe` speeds up
`bad-rollout`. Budget ~7 minutes for `verify`.

## If it does not fire

The inject depends on the CronJob controller not writing
`status.lastScheduleTime` when the Job create is rejected. If your
control-plane version writes it anyway, swap the quota for an invalid
`jobTemplate` (e.g. a container name with an underscore) — same
symptom, same signal, different rejection point.

## Explore by hand

```sh
kubectl -n lookout-demo get cronjob beat -o yaml | grep -A3 status:
kubectl -n lookout-demo get events --field-selector reason=FailedCreate
lookout health
```

Agent-harness prompt to try:
> Our nightly reconcile hasn't produced output in a while but nothing
> is alerting. Is anything scheduled not running?
