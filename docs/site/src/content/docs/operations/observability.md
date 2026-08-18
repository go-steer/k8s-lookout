---
title: Observing lookout
description: The Prometheus metrics surface, /healthz and /readyz, startup-log verification, and which counters to alert on.
sidebar:
  order: 4
---

The watcher of the cluster needs watching too. The sentinel's own
observability surface is `--metrics-addr` (the shipped manifest sets
`:9090`), which serves Prometheus metrics on `/metrics` plus two
probes.

## The two probes

They answer different questions, and the shipped Deployment points one
each at them:

- **`/healthz`** (liveness) — a static 200. The process is up. It
  deliberately does not depend on `/metrics` or on any cluster call, so
  a cluster outage does not get the sentinel killed and restarted into
  the same outage.
- **`/readyz`** (readiness) — 200 only once every source with an
  initial-LIST barrier has crossed it, for every cluster this process
  watches. A sentinel spends its first seconds listing each informer's
  world; it is running and blind in that window, so it reports `503`
  with the reason:

  ```
  not ready: informer caches syncing
  ```

  A process watching a named fleet (`--clusters`, or a single
  `--cluster-name`) names the stragglers instead, since there the
  question is *which* cluster:

  ```
  not ready: waiting on 1 of 2 cluster(s): [prod-west (syncing)]
  ```

  The poll-driven sources (`expiry`, `quota`, `saturation`,
  `notifications`, `token-burn`) have no cache to fill and never hold
  readiness. A runner that exits and is waiting on its supervisor
  backoff withdraws from readiness too — `not ready: cluster runner
  not started`, or `… (not started)` in the named-fleet form.

Readiness matters most during a rollout: the Deployment uses
`strategy: Recreate` with one replica, so the new pod must come up
before anything is watching again, and `/readyz` is what tells you when
that has happened rather than when the process merely started.

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
all 40 metrics — pipeline counters, recovery, storms, watchboard, store,
enrichment, distiller, and triage-status routing — with types, labels,
and meanings derived from the live collectors.

### One of them is about what was found

Every metric above measures the *machine*: informer lag, queue depth,
dispatch latency, store size. `lookout_findings_total{kind,severity}`
is the exception — it measures what the sentinel found.

```
lookout_findings_total{cluster="prod-us",kind="pod.crashloop",severity="critical"} 12
lookout_findings_total{cluster="prod-us",kind="objectstate.restart_burst",severity="warning"} 41
```

It counts **once per distinct finding**, at the moment a fresh dedup
window opens and before any routing decision — so an info-class signal
that is only stored, a warning batched into the watchboard, and a
critical that opens its own session all count the same. That is the
difference from `events_injected_total`, which measures delivery: a
downgraded finding is still a finding.

The `cluster` label is always present, so a multi-cluster process
produces one series per watched cluster. **Namespace is deliberately
not a label** — `kind` is bounded and `severity` is three values, but
namespace is unbounded, and that is where a findings metric turns into
an outage of the monitoring stack it feeds. For per-namespace
questions use [the occurrence store](/operations/store/) or the read
path.

`rate(lookout_findings_total{severity="critical"}[1h])` is the
cluster-health trend line; a step change in it is usually the first
graph worth looking at.

### Something to scrape

`deploy/17-service-watcher.yaml` publishes a ClusterIP Service,
`lookout-watch-metrics`, on `:9090`. Prometheus-operator users can
additionally apply the ServiceMonitor:

```sh
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy/prometheus-operator?ref=v0.21.0"
```

It ships outside the base bundle because `ServiceMonitor` is a CRD and
`kubectl apply -k deploy/` must not fail on a cluster that does not
have it. On Google Managed Prometheus the equivalent is a
`PodMonitoring`; the port name and interval carry over.

Two things to check if a scrape comes back empty: the NetworkPolicy in
`deploy/16` admits **same-namespace** scrapers only — monitoring in its
own namespace needs that `namespaceSelector` block uncommented — and
the Service selects on both `app.kubernetes.io/name` and
`app.kubernetes.io/component`, so a renamed Deployment needs both.

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
| `findings_total{severity="critical"}` | The only entry here that is about the cluster rather than the sentinel. A rate step change means something broke; a rate that goes to zero on a cluster that normally has one means the sentinel stopped seeing, which the machine metrics above will not tell you. |

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
