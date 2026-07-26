---
title: Catch resource exhaustion early
description: A staged memory leak, forecast by slope — warning 24 minutes before failure, a critical session 14 minutes before the OOM kill, ETA accurate to 31 seconds. Real M3 drill output.
sidebar:
  order: 3
---

**The problem:** a pod is leaking memory. Nothing is failing yet — it is
Running, Ready, zero restarts — and nothing reactive will say a word until
the kernel OOM-kills it. The first conventional failure marker *is* the
outage.

The sentinel's `saturation` source samples continuously (metrics.k8s.io +
kubelet volume stats) and fits a slope: not "at 54% of limit" but "limit
reached in ~14m at the observed trend". Deterministic arithmetic, not ML —
and it owns the time series a one-shot read never had.

All output below is from the M3 exit drill, abridged: pod `drill-b/leaker`
with a 64Mi memory limit, leaking ~1MiB every 30s, started 10:50:27. The
drill ran `--saturation-window=10m` (production default 90m).

## The timeline

```txt
10:56:25 watchboard: buffered saturation.forecast drill-b/leaker (severity=warning)   (+5m58s)
         pod at signal time: phase=Running ready=True restarts=0 (17Mi/64Mi)
11:05:56 fire forecast_memory pod=drill-b/leaker → sid=stub-sess-0010                 (+15m29s, critical escalation)
         pod at signal time: phase=Running ready=True restarts=0
11:20:04 OOMKilled (restartCount 0→1) — the FIRST failure marker of any kind
```

Three stages, by design:

1. **Warning** once enough samples span the window (severity routing sends
   it to the watchboard digest — a trend worth knowing, not a page).
2. **Critical escalation** when the ETA drops under the critical threshold
   (default 15m): a per-incident session opens while the pod is still
   healthy by every conventional measure.
3. **The failure itself**, which in this drill arrived 14m08s after the
   session opened — and 31 seconds off the forecast ETA.

The critical inject, captured verbatim on the wire:

```json
{"kind":"saturation.forecast","reason":"forecast_memory",
 "namespace":"drill-b","kind_of_object":"Pod","name":"leaker","container":"leaker",
 "message":"memory saturation forecast for leaker: current=34.8MiB limit=64.0MiB slope_per_min=2.0MiB — limit reached in ~14m39s at the observed trend (20 samples over 10m0s)",
 "cluster":"kl-m3","context":{"node":"kl-m3-worker"},
 "forecast":{"eta":"2026-07-26T11:20:35.49782084Z","confidence_basis":"linear-10m-window"}}
```

Read the `forecast` attachment: `eta` is the projected exhaustion instant;
`confidence_basis` names the fit and window (`linear-10m-window` here
because of the drill flag — `linear-90m-window` at defaults), so a
consumer knows exactly how much to trust it. A forecast is only emitted
when samples span at least half the window — insufficient data produces no
forecast, never a wild one.

## What to do with the lead time

With ~14 minutes of margin the remedies are ordinary instead of
emergency: raise the limit, roll the pod at a quiet moment, or fix the
leak. Point-in-time confirmation and neighborhood checks:

```sh
lookout triage top --namespace=drill-b        # saturation vs limits, right now
lookout triage radius Pod/drill-b/leaker      # who shares the node with it
```

The same slope → ETA machinery covers the other incompressible resources:
PVC fill (`forecast_volume`), project quota headroom
([`quota.forecast`](/guides/capacity-quota/)), IP space, and even agent
token spend (`token.burn`).

Not every saturation problem is a trend, and the drill caught one of the
other kind too: deleting a pod dropped a Service's ready ratio 2/2 → 1/2,
and the `degradation` source fired `degradation.capacity` with the step
timeline in the message (`ratio 1.00→0.50 … timeline: 2/2 → 2/3 → 2/2 →
1/2`) — then resolved it when the replacement came Ready.

## The epilogue writes itself

Whatever the fix, the loop closes as always: the source watches the ETA
recede (clearance requires it to recede well past the firing threshold —
hysteresis, no flapping), and a `kind=resolved` record lands in the
session. In the drill run the process recorded ten recoveries with zero
reverts.

## As an agent skill

Agents working a `saturation.forecast` session learn the follow-up reads —
`triage top`, `triage radius`, `bundle` — from
[`skills/k8s-triage`](https://github.com/go-steer/k8s-lookout/tree/main/skills/k8s-triage);
scheduled whole-cluster sweeps that would catch the same trend are taught
by
[`skills/cluster-health`](https://github.com/go-steer/k8s-lookout/tree/main/skills/cluster-health).
