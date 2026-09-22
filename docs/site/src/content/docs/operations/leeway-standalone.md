---
title: The standalone leeway binary
description: Running the placement subsystem on its own as a metrics-only process — what it is for, what it gives up, and why it must never run alongside lookout watch on the same cluster.
sidebar:
  order: 7
---

`lookout watch` already runs the placement subsystem. If you are running
the sentinel, you have topology drift and compute-class ranks already,
with a store behind them and findings on the wire, and this page is not
for you.

`leeway` is a second, much smaller binary that runs the same two
sources — `topology-drift` and `compute-class` — against one cluster and
exports their metrics. Nothing else: no event watcher, no store, no
inject path, no read-path commands. It ships in the same image and the
same release as `lookout`.

## The constraint

**Do not run `leeway` against a cluster that is already running
`lookout watch`.**

Both processes build a Pod informer. On a large cluster that is the
single most expensive watch there is, and running two of them doubles
the apiserver's outbound traffic and the memory held by the caches, in
exchange for a second copy of numbers you already have. Consolidating
onto one shared informer is a thing the sentinel went out of its way to
do; standing a second process next to it undoes that in one step.

There is no interlock that stops you. Nothing in either binary can see
the other, and a cluster has no place to record "a sentinel is already
here". It is a deployment-time decision, which is why it is written
down here rather than enforced in code.

If both are deployed by accident, the symptom is not an error. It is a
doubled `apiserver_longrunning_requests`, two scrape targets reporting
almost-but-not-quite the same `lookout_leeway_*` series, and findings
that appear twice in whatever consumes them. To tell the two targets
apart, look for `lookout_leeway_standalone_info` — the standalone
exports it and the sentinel does not.

## What it is for

Three cases, and they are the only three:

**A cluster that does not run lookout.** You want the placement signal
and you are not ready to adopt the sentinel. The RBAC is small — pods,
nodes and replicasets, read-only — and the output is a scrape endpoint.

**A metrics-only posture.** The consumer is Grafana and a rotation, not
an agent. The sentinel's inject path, store and watchboard are all
things you would be running and not reading.

**Scale and soak work.** Isolating the subsystem under kwok is how its
cost is measured without the rest of the sentinel in the same RSS
number. `--exit-after` exists for exactly this: a bounded run that ends
itself rather than being killed.

## What it gives up

| | `lookout watch` | `leeway` |
|---|---|---|
| Findings | signals → inject path, watchboard, sinks | `/metrics` only |
| Store | dwell timers and baselines survive a restart | in-memory; a restart resets them |
| Other sources | all of them | these two |
| Flag surface | the full set | the knobs that change what is watched or what it costs |
| Multi-cluster | `--clusters-from`, one process, N runners | one cluster per process |

Running store-less is a supported posture, not a degraded one: current
placement is rebuilt from the informers on every start, so what a
restart costs is the dwell timers that were part-way through and the
learned baselines, not correctness. A finding that was real before the
restart becomes real again one dwell later.

## Running it

In a pod, with the same service account the sentinel would use:

```yaml
containers:
  - name: leeway
    image: ghcr.io/go-steer/lookout:latest
    # The image entrypoint is `lookout watch`; the standalone is a
    # second binary in the same image and has to be asked for.
    command: ["/leeway"]
    args:
      - "--in-cluster"
      - "--cluster-name=prod-eu-west1"
      - "--metrics-addr=:9090"
    ports:
      - name: metrics
        containerPort: 9090
    livenessProbe:
      httpGet: { path: /healthz, port: metrics }
    readinessProbe:
      httpGet: { path: /readyz, port: metrics }
```

Locally, against whatever your kubeconfig points at:

```
leeway --metrics-addr=127.0.0.1:0 --exit-after=5m
```

`--metrics-addr` accepts port `0`, which binds a free one and logs
which; that is how you run it on a machine that is already using 9090.

Set `--cluster-name` whenever more than one of these reports to one
backend. It becomes the `cluster` label on every series, and without it
two clusters are indistinguishable in the same query.

## Sources

`--sources` defaults to both. `compute-class` reads a GKE CRD, so on any
cluster that does not serve it — kind, kwok, anything not GKE — it is
**skipped with a line on stderr** and the process carries on with
`topology-drift` alone.

Naming it explicitly means something different. `--sources=compute-class`
on a cluster without the CRD is a startup failure, because an operator
who typed it is relying on it, and the likely cause is the wrong
kubeconfig rather than the wrong cluster. This is the same asymmetry the
sentinel's `--sources=auto` draws.

## Endpoints

- `/metrics` — the Prometheus endpoint. Everything documented under
  [Metrics](/reference/metrics/) for these two sources appears here
  unchanged, plus `lookout_leeway_standalone_info` and
  `lookout_leeway_standalone_signals_total`.
- `/healthz` — liveness. Up means the process is not wedged. It
  deliberately does not consult the sources: a process that cannot reach
  the apiserver is not helped by being restarted.
- `/readyz` — readiness. 503 until every source's initial LIST has
  drained, naming the one that has not. Until then the gauges are an
  undercount rather than a reading, which is not a state to route a
  rollout past.

`--otel-exporter=otlp` adds a push reader configured by the standard
`OTEL_EXPORTER_OTLP_*` environment. It does not turn `/metrics` off —
the scrape endpoint is unconditional.

## Findings

There is nowhere for a finding to go, so it goes to two places that are
not the wire: `lookout_leeway_standalone_signals_total`, labelled by
source, kind and severity, and one line per finding on stderr.

The counter is a rate, not a state. Which subjects are in breach right
now is what the sources' own gauges say; how often the subsystem decided
something was worth saying is what this says, and that is the thing no
gauge can reconstruct.
