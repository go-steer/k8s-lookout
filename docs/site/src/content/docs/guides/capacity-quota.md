---
title: Capacity & quota ahead of time
description: The correlated quota incident with a drafted increase request attached, and the proactive cloud sweeps — stockout, quota, IP space. Real output from the M4 exit check.
sidebar:
  order: 6
---

**The problem:** capacity exhaustion has days of lead time when watched
and zero when not. A quota at 98% is invisible right up until the
autoscaler fails with `GCE_QUOTA_EXCEEDED` — and then it is an outage with
a multi-day increase-request turnaround in the middle of it.

Two halves to staying ahead of it: the resident `quota`/`capacity` sources
(watch-path, one quota source per GCP project), and the on-demand `cloud`
sweeps (read-path). Output below is from the M4 exit check — the engine
legs run the real merged pipeline over recorded cloud fixtures per the
standing drill policy, the Kubernetes legs live on kind; all abridged.

## The escalation, staged as designed

**1. The warning forecast does not page.** CPUS/us-east1 at 85%, growing
50/day → ETA ~6 days: a watchboard digest entry, not a session.

**2. The critical escalation opens the incident — draft attached.** At
98% and ETA ~16h, one inject carries the diagnosis *and* the paperwork:

```json
{"kind":"quota.forecast","reason":"quota_forecast","kind_of_object":"Quota","name":"CPUS",
 "uid":"quota:CPUS/us-east1",
 "message":"quota CPUS in us-east1 at 98.0% (usage 1960 / limit 2000), growing 60/day over the last 7d (8 points) — exhausted in ~16h0m0s at current slope; drafted increase to 3000 attached — file it via core-agent's permission gate",
 "cluster":"kl-m4-drill",
 "forecast":{"eta":"2026-07-27T04:42:06Z","confidence_basis":"linear-7d-window"},
 "quota_increase_draft":{
   "quota_id":"compute.googleapis.com/cpus","region":"us-east1",
   "current_usage":1960,"current_limit":2000,
   "suggested_limit":3000,"slope_per_day":60,
   "justification":"CPUS in us-east1 is at 1960 of 2000 (98.0%). Usage grew 60/day over the observation window; at that slope the quota is exhausted in ~16h0m0s (around 2026-07-27). Requesting an increase to 3000 to cover twice the expected request turnaround at the observed growth."}}
```

The draft is formula-pinned (suggested limit covers twice the expected
request turnaround at the observed slope, floored at 1.5× the current
limit), with a human-grade justification generated from the same numbers
the forecast fired on. **lookout drafts; it never files.** Submitting the
increase request is the agent's move, through the `core-agent` daemon's
permission gate — the one place in the suite where the managed write path
is a clean API call with paperwork attached.

**3. The reactive confirmation is the same incident.** When the
autoscaler then actually hit the wall (`Quota 'CPUS' exceeded. Limit:
2000.0 in region us-east1.`), the signal re-keyed to the same
`quota:CPUS/us-east1` identity and folded into the open session:

```txt
store: kind=capacity.quota_blocked canonical_reason=QuotaExhausted
       route=suppressed session_id=sess-xx     ← the critical forecast's session
```

Final ledger, asserted in CI forever: two injects (warning digest +
critical incident), two sessions, zero re-fires of the same critical state
— one diagnosed incident, not two alerts a human joins.

The Kubernetes-visible half ran live: a pod stuck Unschedulable produced
three observation angles — the reactive `FailedScheduling` Event, the CA's
`NotTriggerScaleUp`, and the `--pending-age` sweep — all collapsing into
one session via the dedup family:

```txt
13:14:32  k8s-event              FailedScheduling  critical  injected    stub-sess-0007
13:15:01  capacity.pending       pending           warning   suppressed  stub-sess-0007
13:15:43  capacity.pending-aged  pending-aged      warning   suppressed  stub-sess-0007
          (canonical_reason=FailedScheduling on all three)
```

## The proactive sweep

The same questions on demand, from the `cloud` group (these need the
GKE-provider build — the `:<version>-gke` image; the default binary
reports an explicit `cloud.unavailable` instead). Representative output
from the recorded-fixture suites, abridged:

**Stockouts, with reroute candidates** —
[`cloud stockout`](/reference/cloud-stockout/):

```txt
kind=stockout.zone severity=warning kind_of_object=Zone name=us-east1-b reason=ZoneResourcePoolExhausted message="GCE stockout: e2-medium exhausted in us-east1-b ×1 in the last 24h0m0s — reroute candidates (same region, no stockout for this type in window): us-east1-c" machine_type=e2-medium events=1 first_seen=2026-07-25T08:40:00Z last_seen=2026-07-25T08:40:00Z reroute=us-east1-c
scanned=6 findings=4 elapsed=100ms window=24h0m0s
```

**Quota headroom, ranked nearest-to-exhaustion** —
[`cloud quota`](/reference/cloud-quota/):

```txt
kind=quota.pressure severity=critical kind_of_object=Quota name=IN_USE_ADDRESSES reason=QuotaExhausted message="IN_USE_ADDRESSES exhausted in us-east1 (8/8) — scale-ups fail with GCE_QUOTA_EXCEEDED until an increase lands" scope=us-east1 usage=8 limit=8 pct=100
kind=quota.pressure severity=critical kind_of_object=Quota name=CPUS reason=QuotaNearLimit message="CPUS at 98% of limit in us-east1 — scale-ups fail at 100%; increases need lead time, file now (§10.3)" scope=us-east1 usage=588 limit=600 pct=98
scanned=6 findings=3 elapsed=100ms
```

**IP space, the forgotten quota** —
[`cloud ipspace`](/reference/cloud-ipspace/):

```txt
kind=ipspace.range severity=critical kind_of_object=Subnetwork name=prod-subnet reason=IPRangeNearExhaustion message="pods range at 96.9% of 10.8.0.0/14 — the next allocation fails at 100%: IP space is incompressible" cidr=10.8.0.0/14 purpose=pods used=253952 capacity=262144 pct=96.9
scanned=4 findings=4 elapsed=100ms
```

Rounding out the group, [`cloud orphans`](/reference/cloud-orphans/) sweeps
for unattached billing-active disks and load balancers targeting zero pods.

Why the stockout/quota distinction matters: the remedies are disjoint.
Stockout → reroute the node pool to a clean zone (the `reroute=` field and
the sentinel's distilled zone history inform which); quota → file the
increase, with the lead time the forecast just bought you.

## As an agent skill

The full workflow — the sweeps, reading `quota.forecast` and the draft,
the pending-pod dedup family, filing through the permission gate, and
recording the outcome with `triage status` — is taught to agents by
[`skills/k8s-capacity`](https://github.com/go-steer/k8s-lookout/tree/main/skills/k8s-capacity).
