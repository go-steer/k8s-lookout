---
name: k8s-capacity
description: Answer capacity and quota questions before they become outages — GCE stockouts, project quota headroom, IP-space utilization — with the lookout cloud sweeps; work quota.forecast / capacity.* incident sessions; review the attached QuotaPreference draft and file it through core-agent's permission gate. Use for "will we run out", pods Pending on failed scale-ups, and any capacity.stockout / quota.forecast inject.
---

# k8s-capacity — capacity & quota forecasting with lookout

Capacity exhaustion has days of lead time when watched and zero when not.
This skill covers both invocation moments:

- **Proactive** — the `lookout cloud` sweeps: stockouts, quota headroom,
  IP space, on demand.
- **Reactive/leading** — an incident session opened by the sentinel's
  `capacity` or `quota` source (`capacity.stockout`, `capacity.pending`,
  `quota.forecast`, …): read the forecast, confirm, and file the fix.

Provider note: the `cloud` group and the `quota` source need a
provider-enabled binary (the `-gke` image / `gke` build tag). The default
vanilla-k8s binary reports an explicit `cloud.unavailable` finding at exit
0 — that means "no provider configured", not "swept and clean". The
portable capacity seams (CA events, pending-pod sweeps) work everywhere.

## The two failure classes — remedies are disjoint

| Signal says | It means | The remedy |
| --- | --- | --- |
| `GCE_STOCKOUT` / `stockout.zone` | the cloud has no machines of that type in that zone — *your quota is irrelevant* | reroute: different zone (see `reroute=`) or machine type |
| `GCE_QUOTA_EXCEEDED` / `quota.pressure` / `quota.forecast` | your project's limit — *the zone has capacity you're not allowed to use* | file an increase (lead time!), or shed usage |

Never answer a stockout with a quota request or vice versa — the message
and reason name which one you have.

## The proactive sweep

```lookout
lookout cloud stockout
lookout cloud quota
lookout cloud ipspace
```

**Stockouts** (`ZONE_RESOURCE_POOL_EXHAUSTED` extraction, per
zone/machine-type over `--since`, default 24h). Abridged real output:

```lookout-golden
kind=stockout.zone severity=warning kind_of_object=Zone name=us-east1-b reason=ZoneResourcePoolExhausted message="GCE stockout: n2-standard-16 exhausted in us-east1-b ×3 in the last 24h0m0s — no same-region zone observed clean for this type in the window" machine_type=n2-standard-16 events=3 first_seen=2026-07-25T02:00:00Z last_seen=2026-07-25T11:30:00Z
kind=stockout.zone severity=warning kind_of_object=Zone name=us-east1-b reason=ZoneResourcePoolExhausted message="GCE stockout: e2-medium exhausted in us-east1-b ×1 in the last 24h0m0s — reroute candidates (same region, no stockout for this type in window): us-east1-c" machine_type=e2-medium events=1 first_seen=2026-07-25T08:40:00Z last_seen=2026-07-25T08:40:00Z reroute=us-east1-c
…
scanned=6 findings=4 elapsed=100ms window=24h0m0s
```

`reroute=` lists same-region zones with no stockout for that machine type
in the window — the concrete relocation candidates. Absence of `reroute=`
means no clean same-region zone was observed: consider a machine-type
change instead.

**Quota headroom**, ranked nearest-to-exhaustion (warning from
`--quota-warn`, default 80%; critical fixed at 95%):

```lookout-golden
kind=quota.pressure severity=critical kind_of_object=Quota name=IN_USE_ADDRESSES reason=QuotaExhausted message="IN_USE_ADDRESSES exhausted in us-east1 (8/8) — scale-ups fail with GCE_QUOTA_EXCEEDED until an increase lands" scope=us-east1 usage=8 limit=8 pct=100
kind=quota.pressure severity=critical kind_of_object=Quota name=CPUS reason=QuotaNearLimit message="CPUS at 98% of limit in us-east1 — scale-ups fail at 100%; increases need lead time, file now (§10.3)" scope=us-east1 usage=588 limit=600 pct=98
kind=quota.pressure severity=warning kind_of_object=Quota name=SSD_TOTAL_GB reason=QuotaNearLimit message="SSD_TOTAL_GB at 87.5% of limit in us-east1 — critical from 95%" scope=us-east1 usage=3500 limit=4000 pct=87.5
scanned=6 findings=3 elapsed=100ms
```

**IP space** (pod/service/node CIDR utilization per subnet — IP space is
incompressible; an exhausted range fails the next node or pod block
outright):

```lookout-golden
kind=ipspace.range severity=critical kind_of_object=Subnetwork name=prod-subnet reason=IPRangeNearExhaustion message="pods range at 96.9% of 10.8.0.0/14 — the next allocation fails at 100%: IP space is incompressible" cidr=10.8.0.0/14 purpose=pods used=253952 capacity=262144 pct=96.9
kind=ipspace.range severity=warning kind_of_object=Subnetwork name=prod-subnet reason=IPRangeHighUtilization message="nodes range at 85.7% of 10.0.0.0/24 — critical from 95%; plan the secondary range before the slope arrives" cidr=10.0.0.0/24 purpose=nodes used=216 capacity=252 pct=85.7
…
scanned=4 findings=4 elapsed=100ms
```

Exploratory variants when you want the full inventory, not just findings:

```lookout
lookout cloud quota --all --format=json
lookout cloud ipspace --all
lookout cloud stockout --since=6h
```

These are point-in-time reads. The *rate* — "exhausted in ~6 days at
current slope" — lives in the sentinel's `quota`/`capacity` sources,
which is what opens the sessions below.

## Pending pods: is it capacity?

A pod stuck Pending with `FailedScheduling` may be a capacity problem
(the autoscaler cannot add nodes) or a spec problem (nothing could ever
fit it). First look:

```lookout
lookout triage delta --only=pods
```

The sentinel observes the same incident from up to three angles —
the reactive `FailedScheduling` Event, the CA's `NotTriggerScaleUp`
(`capacity.pending`), and the aged-pending sweep (`capacity.pending-aged`)
— all deduping into ONE session (`canonical_reason=FailedScheduling`).
Expect them in the session you were handed; do not open parallel
investigations per signal. When the CA decision names the structured why
(`capacity.stockout` / `capacity.quota_blocked` / `capacity.ip_exhausted`,
GKE provider only), the failure class above is already decided for you;
otherwise run the sweep commands to determine it.

## Reading `quota.forecast` and the attached draft

A `quota.forecast` inject carries the §8 forecast attachment and — the
part that makes it actionable — a drafted increase request:

```json
{"kind":"quota.forecast","reason":"quota_forecast","kind_of_object":"Quota","name":"CPUS",
 "uid":"quota:CPUS/us-east1",
 "message":"quota CPUS in us-east1 at 98.0% (usage 1960 / limit 2000), growing 60/day over the last 7d (8 points) — exhausted in ~16h0m0s at current slope; drafted increase to 3000 attached — file it via core-agent's permission gate",
 "forecast":{"eta":"2026-07-27T04:42:06Z","confidence_basis":"linear-7d-window"},
 "quota_increase_draft":{
   "quota_id":"compute.googleapis.com/cpus","region":"us-east1",
   "current_usage":1960,"current_limit":2000,
   "suggested_limit":3000,"slope_per_day":60,
   "justification":"CPUS in us-east1 is at 1960 of 2000 (98.0%). Usage grew 60/day over the observation window; at that slope the quota is exhausted in ~16h0m0s (around 2026-07-27). Requesting an increase to 3000 to cover twice the expected request turnaround at the observed growth."}}
```

- `forecast.eta` is the projected exhaustion instant;
  `confidence_basis=linear-<window>` names the fit window. Warning-class
  forecasts (long ETA) batch into the watchboard; the critical escalation
  (short ETA) opens the per-incident session.
- `quota_increase_draft` is formula-pinned: `suggested_limit` covers twice
  the expected request turnaround at the observed slope, floored at 1.5×
  the current limit. Sanity-check `slope_per_day` against known events
  (a migration, a load test) before filing — the slope is honest but not
  psychic.
- A subsequent real scale-up failure (`capacity.quota_blocked`) for the
  same quota joins THIS session via the `QuotaExhausted` dedup family
  (keyed `quota:<NAME>/<SCOPE>`) — it is confirmation, not a new incident.

## Filing the increase — through the gate, always

lookout drafts; it never files. Submit the `QuotaPreference` increase
request through core-agent's permission gate (the daemon's managed write
path), using the draft's `quota_id`, `region`, `suggested_limit`, and
`justification` verbatim unless you have better numbers. There is no
lookout command that writes to the cloud — if you find yourself reaching
for one, you are outside the design.

## Record the conclusion

Close the loop so scans and routing reflect triaged reality (§9.4). The
fingerprint is on the inject payload and on scan findings:

```lookout
lookout triage status --store=/var/lib/lookout/lookout.db --fingerprint=sha256:5641bdee… --resource=Quota//CPUS --status=actioned --root-cause="CPUS/us-east1 at 98%, slope 60/day, ETA ~16h" --action="QuotaPreference increase to 3000 filed via permission gate" --session=stub-sess-0002
```

- `--status=actioned` once the request is filed; the sentinel flips the
  record to `resolved` itself when the pressure clears (never claim
  `resolved` yourself).
- For a stockout you rerouted around, record the reroute as the action;
  the distilled zone history ("us-east1-b: 3 stockouts this week;
  us-east1-c clean") strengthens the next recommendation.

## Per-command references

Deep docs (all flags, output-field glossaries) in `references/` —
generated from the same metadata as `--help`, so they never drift from
the binary.
