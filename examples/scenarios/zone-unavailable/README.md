# zone-unavailable — a whole zone goes; one finding, not one per workload

**kind only.** zone-b (the second worker, on `lookout-examples/zone`)
is cordoned, which leaves the zone with no node the scheduler can use.
The point is what leeway does *not* do: report the same fact once per
workload that had replicas there.

Not part of the default `examples/e2e` set; run it explicitly (the
weekly CI run does):

```sh
examples/e2e zone-unavailable
```

## Timeline

1. `zone-hard` (DoNotSchedule, ×2) and `zone-soft` (ScheduleAnyway, ×4)
   spread normally across zone-a and zone-b.
2. zone-b's only node is cordoned.

A cordon rather than `node-failure`'s `docker stop`, for two reasons.
leeway counts usable nodes (Ready *and* schedulable), so both empty the
zone. But a stopped node also opens objectstate and storm incidents.
And its domain alert would still be in the 30m resolve dwell when
`node-failure` stops the same node, so the two could not share a run.

## What to expect

- **Sentinel (wire):**
  - exactly one `leeway.domain_unavailable`, for `zone-b`, with suspected
    cause `taint_exclusion`. It is Tier B, so it arrives in the
    watchboard digest about a minute after the 60s dwell. A dead zone
    would report `domain_outage` instead.
  - no `leeway.contract_violated` or `leeway.placement_drift` for
    `zone-hard` or `zone-soft`. `verify` holds for two dwell-plus-flush
    periods before it counts.

**Run order matters.** The domain finding stays open for the 30m
resolve dwell after `revert` uncordons the node. A second run inside
that window, or a `node-failure` run *before* this one, finds zone-b's
episode still open, and nothing new reaches the wire. CI runs this
scenario before `node-failure`.

## Explore by hand

```sh
kubectl -n agent-triage port-forward deploy/lookout-watch 9090 &
curl -s localhost:9090/metrics | grep -E 'lookout_leeway_(domains_unavailable|transient_subjects)'
```

Agent-harness prompt to try:
> leeway reports zone-b unavailable. What can no longer be scheduled
> there, which workloads lost capacity, and is anything at risk if
> zone-a has a bad day too?
