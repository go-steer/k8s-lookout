---
title: Observing lookout
description: The Prometheus metrics surface, /healthz, startup-log verification, and which counters to alert on.
sidebar:
  order: 4
---

The watcher of the cluster needs watching too. The sentinel's own
observability surface is `--metrics-addr` (the shipped manifest sets
`:9090`), which serves Prometheus metrics on `/metrics` and a liveness
endpoint on `/healthz` — the shipped Deployment's liveness and readiness
probes both hit it.

## Metrics

Every metric carries the `lookout_` prefix. A real scrape from a live
validation drill:

```
lookout_events_seen_total{namespace="default",reason="BackOff"} 4
lookout_events_injected_total{namespace="default",reason="BackOff"} 1
lookout_events_deduped_total{namespace="default",reason="BackOff"} 3
lookout_session_creates_total{outcome="ok"} 5
lookout_active_incidents 5
```

The generated [Prometheus metrics reference](/reference/metrics/) covers
all 33 metrics — pipeline counters, recovery, storms, watchboard, store,
enrichment, distiller, and triage-status routing — with types, labels,
and meanings derived from the live collectors.

## What to alert on

Prefix `lookout_` omitted:

| Metric | Why it pages |
| --- | --- |
| `inject_errors_total` | Inject or session-create attempts returning non-2xx or a transport error. Increase means the daemon is unreachable, the token is wrong, or `--owner` fails the proxy-identity check — the sentinel is seeing incidents and failing to deliver them. |
| `session_creates_total{outcome!="ok"}` | The session-create half of the same failure surface. |
| `store_write_drops_total` | Occurrence records **lost** by the store's non-blocking write path (`buffer_full` or `write_error`). The store is telemetry, not a system of record — drops are loud, never blocking — but a sustained rate means your audit ledger and post-mortem history have holes. |
| `store_pruned_rows_total{cause="size"}` | Size-based eviction: the store hit `--store-max-mb` and is discarding oldest history. TTL pruning (`cause="ttl"`) is normal; size pruning means the bound is too small for the signal volume. |
| `enrichment_failures_total` / `enrichments_total{outcome="failed"}` | Enrichment stage failures (by stage: resolve, spec, delta, edges, radius, logs). Failures never block the inject — they surface as `enrichment_error` trailers — but a constant rate usually means an RBAC gap on the enrichment read paths. |
| `recovery_drops_total{cause="unknown_session"}` | A resolved outcome had nowhere to go — the incident binding was lost, typically a restart without `--dedup-persist`. Fix the volume; every drop is a fix-verify loop that could not close. |
| `info_dropped_total` | Info-class signals counted and discarded because no `--store` is set. Not a fault, but if you expected the store to have them, this is the tell. |
| `watchboard_buffered` (gauge) | Stuck above `--watchboard-batch` across scrapes means flushes are failing — see `inject_errors_total`. |

`active_incidents`, `storms_active`, and `recovery_tracking` are the
load gauges worth graphing rather than alerting on.

## The startup log is a checklist

Every armed stage announces itself, and every degradation is a named
line — read it once after each deploy or flag change. From recorded
drill runs:

```
storm: topology graph ready (54 nodes, 68 edges)
recovery: clearance observer backed by the object-state source's pod informer
watchboard: enabled (batch=5, flush=1m0s, rotate after 200 …)
enrichment: enabled (severities=critical … read path: live-graph (scoped-list fallback))
store: enabled (path=/data/lookout.db, ttl=720h …)
graph history: enabled (snapshot every 1m0s + per-delta change log …)
expiry: cert-manager CRD not found — Certificate renewal-state scanning disabled; TLS secrets and webhook CA bundles are still scanned
```

A missing "enabled/ready" line for a stage you configured, or any
startup RBAC-probe error, is a misconfiguration — see
[Troubleshooting](/operations/troubleshooting/).

## Traces

The sentinel exports OpenTelemetry spans with
`--otel-exporter=console|otlp`; `none` (the default) makes no outbound
tracing calls at all. Three things are worth knowing:

- **`OTEL_TRACES_EXPORTER` overrides the flag** — the OTel-standard
  env var wins, so one shared Deployment can carry per-Pod exporter
  targets without forking the manifest.
- **`otlp` is OTLP over HTTP** and reads the standard env vars:
  `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, then
  `OTEL_EXPORTER_OTLP_ENDPOINT`, then the spec default
  `http://localhost:4318`. The resolved target is logged at startup.
  Set `GOOGLE_CLOUD_PROJECT` when shipping to Cloud Trace — its OTLP
  ingress rejects batches with no `gcp.project_id` resource attribute.
- **Export failures are loud.** An unreachable collector, TLS
  mismatch, or wrong port prints `lookout: otel-export: …` on stderr
  rather than silently dropping spans; `OTEL_LOG_LEVEL=debug` raises
  SDK diagnostics too.

The W3C `traceparent` propagator is registered in every mode,
including `none`, so outbound POSTs to the daemon carry trace context
the moment an operator flips the exporter on.
