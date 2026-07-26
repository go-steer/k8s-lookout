# Quota-exhaustion drill — real-GCP replay

Replays the M4 exit criterion (DESIGN.md §14: "Staged quota exhaustion
yields correlated incident + drafted increase request") against a
**real GCP project**. The recorded original is fixture-driven by
policy — no live GCP in the milestone run — and lives in two places:

- the dispatcher-level drill test
  (`internal/watch/quota_drill_test.go`, run with `go test -v -run
  TestDrill_QuotaExhaustion ./internal/watch/`) — the full pipeline
  over scripted cloud APIs;
- the kind-cluster capacity leg in
  [`docs/milestones/M4.md`](../../docs/milestones/M4.md) — the
  k8s-visible half (pending pods, the CA event grammar) live, the
  provider half faked.

This runbook exists so the same claim can be verified with the real
APIs on the wire: real Cloud Monitoring series shapes, real
`GCE_QUOTA_EXCEEDED` grammar in the visibility log, real quota
turnaround.

> **STAGING PROJECTS ONLY.** The drill deliberately drives a compute
> quota to ≥ 90% of its limit and then makes the cluster autoscaler
> slam into it. Anything else in the project that needs that quota
> will feel it. Use a dedicated staging project with nothing shared,
> and pick a quota you can afford to saturate (N2D CPUs in a region
> you otherwise don't use is a good choice — the machine family
> isolates the blast). lookout itself never mutates quota: the §10.3
> write path DRAFTS the increase request; only you (or an agent
> behind core-agent's permission gate) can file it.

## What the fixtures stand in for

The scripted/recorded fixtures used by the shipped tests map to real
API surfaces one-for-one; this drill exercises the right column:

| Test seam (`pkg/cloud` capability) | Fixture in the milestone run | Real API this drill hits |
| --- | --- | --- |
| `QuotaAPI.Quotas` (inventory) | scripted `[]QuotaUsage` (85% → 98%) | Compute `regions.get` / `projects.get` usage-limit pairs, IDs joined from Cloud Quotas `ListQuotaInfos` |
| `QuotaAPI.History` (trend) | synthetic linear daily series (§13) | Cloud Monitoring `serviceruntime.googleapis.com/quota/allocation/usage` vs `…/limit` time series |
| `CapacityAPI.ScaleDecisions` (the reactive why) | scripted `ScaleDecision{Reason: GCE_QUOTA_EXCEEDED}` | GKE `cluster-autoscaler-visibility` Cloud Logging stream (`noScaleUp` decisions with per-MIG reasons) |
| CA `NotTriggerScaleUp` event | `kubectl apply` of a hand-built Event naming the pending pod | the real cluster autoscaler's event on the pending pod |

## Prerequisites

- A GKE **staging** cluster in a dedicated staging project, cluster
  autoscaler enabled on a small node pool (`--enable-autoscaling
  --min-nodes=1 --max-nodes=20` — max high enough that quota, not the
  pool bound, is what blocks).
- A GKE-tagged lookout build: the default image is GCP-free by
  §2/§13 conformance, so the quota + decision sub-sources need
  `go build -tags gke ./cmd/lookout` (or an image built the same
  way). Verify with `lookout watch --sources=quota --dry-run …`: an
  untagged binary refuses with `source "quota" requires a cloud
  provider with the quota capability`.
- Workload Identity (or node scopes) granting the sentinel's service
  account: `compute.regions.get`, `monitoring.timeSeries.list`,
  `logging.logEntries.list`, `cloudquotas.quotaInfos.list` — all
  read-only. NO quota-mutation permission is needed or used.
- A daemon or the capture stub
  ([`stub-daemon.py`](./stub-daemon.py)) deployed per
  [`node-failure.md`](./node-failure.md); `kubectl logs` of the stub
  is the evidence capture.
- Patience budget: quota allocation series are ~daily points and the
  source's trust gates (≥ 4 points spanning ≥ half of
  `--quota-window`) are sized for that. A REAL forecast needs 3–4
  days of genuine usage growth. Fast-path alternative below.

## 1. Deploy the project-tier sentinel

The quota source runs ONCE PER PROJECT (§11) — one sentinel, not one
per cluster. Reuse the drill cluster's sentinel and add the source:

```
--sources=k8s-events,object-state,capacity,quota
--capacity-poll=60s
--quota-poll=15m            # the shipped default; keep it — quota moves slowly
--quota-window=168h         # 7d, the shipped default
--store=/data/lookout.db
```

Startup must log the provider: `cloud: provider "gke" selected`. If
the capacity line says the scale-decision sub-source is disabled, fix
credentials before proceeding — you'd be running the portable half
only, which the kind leg already covered.

## 2. Stage the exhaustion (leading half)

Two ways to make a quota approach its limit:

- **Honest (slow, best evidence):** in the staging region, run a
  workload that grows daily — e.g. a CronJob that scales a
  `n2d-standard-8` node-pool-backed deployment up by one replica per
  day. After 3–4 days the Monitoring series carries a genuine
  positive slope and `quota.forecast` fires on real math
  (`linear-7d-window`).
- **Fast (threshold-only, still real APIs):** request a quota
  DECREASE in the console (Cloud Quotas lets you lower most compute
  quotas immediately) to just above current usage — e.g. usage 46 →
  limit 48 puts you at 95.8%, warning class. The source fires on the
  usage threshold alone and the draft's suggested limit comes from
  the `limit × 1.5` term (no slope; the justification says so —
  formula in `pkg/sources/quota/draft.go`). Note the fired severity
  latches: a quota parked at 95.8% fires warning ONCE, escalation
  only on crossing 98% or an ETA under 48h.

Expected in the stub log either way: one `SESSION-CREATE` +
`INJECT kind=quota.forecast` (critical goes to its own session;
warning lands in a `watchboard.digest` entry), the payload carrying
`"quota_increase_draft":{…,"suggested_limit":…,"justification":"…"}`.

## 3. Slam the autoscaler into it (reactive half)

With the quota near its limit, force scale-up demand the quota cannot
absorb:

```
kubectl create ns quotalab
kubectl -n quotalab create deployment burst --image=registry.k8s.io/pause:3.10 --replicas=60
kubectl -n quotalab set resources deploy burst --requests=cpu=1900m
```

Watch three things converge on ONE incident:

1. `FailedScheduling` on the pending pods (reactive, k8s-events).
2. The real `NotTriggerScaleUp` events → `capacity.pending`, and the
   aging sweep → `capacity.pending-aged` — both dedup-join the
   FailedScheduling session per pod (the `FailedScheduling` family).
3. Within one `--capacity-poll` + the visibility log's ingestion lag
   (minutes): `capacity.quota_blocked` from the REAL
   `noScaleUp` decision. If its message names the quota and region
   (GCE grammar `Quota 'N2D_CPUS' exceeded … in region <r>`), the
   signal re-keys to `quota:<NAME>/<REGION>` and dedup-folds INTO the
   open quota.forecast session — `dedup quota_blocked …` in the
   sentinel log, `route=suppressed session_id=<the quota session>` in
   the store. A message that omits the scope keeps its nodegroup key
   and opens its own incident (documented conservative fallback — no
   false joins; record which you observed).

If the forecast never fired (fast path, warning-only), order
reverses: `quota_blocked` opens the session and a later escalating
forecast joins it. Both orders are the §10.3 contract.

## 4. File the draft — through the gate, not around it

The draft is paperwork, not an API call. To close the loop the way
the agent will: take `quota_increase_draft.quota_id` / `region` /
`suggested_limit` / `justification` from the captured payload and
file it as the human half of the permission gate:

```
gcloud beta quotas preferences create \
  --project=<staging-project> \
  --service=compute.googleapis.com \
  --quota-id=<quota_id tail, e.g. CPUS-per-project-region> \
  --preferred-value=<suggested_limit> \
  --dimensions=region=<region> \
  --justification="<justification string from the draft>"
```

Verify nothing in lookout did this for you: the sentinel's audit
surface for the whole drill must show reads only.

## 5. The mid-incident health scan (exit half B, real cluster)

While an incident from step 3 is open, replay the triage-state check:

```
# the real §9.4 producer surface (docs/triage-status-write-design.md;
# it replaced the M4 drill's write-triage-status stand-in fixture).
# The fingerprint is on the incident's inject payload and on its
# store occurrence row.
lookout triage status --store=<node copy of /data/lookout.db> \
  --fingerprint=<the incident's sha256:… fingerprint> \
  --resource=Pod/quotalab/<a pending pod> \
  --status=triaged --severity-override=warning \
  --root-cause="N2D_CPUS quota exhausted; increase filed" \
  --action="QuotaPreference submitted" --session=<its sid>

lookout health --store=<the store copy>
```

The pending finding must come back annotated
(`triage_status=triaged triage_root_cause=… triage_session=…`) at the
overridden severity, and the sentinel log must show
`triage-status: downgraded …` on the next followup instead of a
re-page. The store can be copied off the node live (WAL) — `gcloud
compute scp` from the node, all three of `lookout.db*`.

## 6. Cleanup

```
kubectl delete ns quotalab
```

Scale the growth workload to zero (honest path) or restore the
lowered quota (fast path — file the increase back to the original
limit), remove `quota` from `--sources` if the sentinel outlives the
drill, and keep the stub capture: the quota.forecast payload with its
draft and the joined `quota_blocked` row are the drill record.
