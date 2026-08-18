---
name: cluster-health
description: Answer "any issues in this cluster?" — on demand or on a schedule — with lookout health's one-call ten-category scorecard, then drill into degraded categories with lookout triage delta, state edges, or bundle.
---

# cluster-health — assessment with lookout

For a whole-cluster health question (scheduled sweep, "how does prod
look?", pre/post-maintenance check), one call answers it:

```lookout
lookout health
```

`health` and `scan` answer the same question two ways over the same
checks: `health` scores ten categories and always emits a line per
category, so a healthy cluster is ten `status=healthy` lines; `scan`
emits findings only, so a healthy cluster is a bare summary line. Reach
for the scorecard when you owe someone a status, for `lookout scan` when
you want the raw list to work through — or when you want to pipe it into
`lookout findings diff` to see what changed since the last sweep.

## Reading the scorecard

The first block is one `health.category` line per category — **the
scorecard always answers**; healthy categories are explicit, never silent.
`status` is `healthy`, `degraded` (with `total=` findings and the worst
ones inline in `top=`, capped by `--top`), or `unavailable` with a reason.
After the scorecard block, every degraded category's full findings follow,
tagged `category=`.

Abridged real output of a degraded cluster:

```lookout-golden
kind=health.category severity=info reason=Unavailable message="requires cloud provider metrics; no cloud provider configured" category=control-plane status=unavailable
kind=health.category severity=critical category=crashloops status=degraded total=1 top="pod.crashloop prod/api-0"
kind=health.category severity=warning category=addons status=degraded total=1 top="addon.degraded kube-system/coredns"
kind=health.category severity=critical category=certs status=degraded total=1 top="cert.expired prod/old-tls"
kind=pod.crashloop severity=critical namespace=prod kind_of_object=Pod name=api-0 reason=CrashLoopBackOff fingerprint=sha256:e2957792a0b3ad9e29db2051dbc69ff01dfe3a52da8dbb6d1331aa44fe946f8b category=crashloops container=app restarts=12 last_state=Error exit_code=1
kind=cert.expired severity=critical namespace=prod kind_of_object=Secret name=old-tls reason=CertificateExpired message="certificate expired 16d ago" fingerprint=sha256:dd1c9738112cd7229a73b1920b06122bccee7bb0e7867c93752fa0b0e522557d category=certs subject=old.example.com not_after=2026-06-15T00:00:00Z days_left=-16
…
scanned=10 findings=20 elapsed=100ms
```

- A fully healthy cluster is 10 `status=healthy`/`unavailable` scorecard
  lines and a summary — nothing else.
- `status=unavailable` is *not* degraded: the category could not be
  assessed and the `message` says why. `control-plane unavailable
  ("requires cloud provider metrics")` is expected on vanilla (non-GKE)
  clusters and clusters without a cloud provider configured — report it
  as "not assessable here", not as a problem. With a metrics-capable
  provider the category instead runs the `perf probe` apiserver pack
  (p99 by verb/resource) and scores `healthy`/`degraded` like any
  other; `unavailable` there means the workspace lacks the metric
  (enable GKE control-plane metrics).
- Severity of a degraded category line is the worst severity inside it.
- Exit code is `0` whenever the scan ran — degraded is data, not an error.
  `findings=0` cannot happen for `health` (the scorecard always emits);
  a missing summary line means the invocation itself failed.

## Drilling into a degraded category

| Degraded category | Next call |
| --- | --- |
| nodes | `lookout triage delta --only=nodes` |
| crashloops, pending, rollouts | `lookout triage delta --only=pods` then `lookout bundle --workload=Deployment/prod/api` for the named workload |
| — any workload category that was healthy on the previous sweep | `lookout triage changes Deployment/prod/api --since=30m` for the named workload first — "what changed before onset" beats log spelunking on a sudden regression |
| — before acting on a degraded workload | `lookout triage radius Deployment/prod/api` — upstream Services/Ingresses (user-facing impact) and lateral co-tenants the breakage or the fix reaches |
| addons | `lookout triage delta --only=system` |
| quota | `lookout triage delta --only=quota` |
| storage | `lookout triage spec pvc/prod/data-claim` for the named PVC |
| certs | `lookout state edges --workload=Deployment/prod/api --cert-warn=720h` for the workload behind the cert, or `lookout triage spec Secret/prod/old-tls` (keys and expiry only — values never render) |
| webhooks | `lookout state webhooks` — the full audit: dead backends × failurePolicy, namespace/rule blast radius, timeout stall risk, CA-bundle expiry (health's webhooks category delegates to the same check) |
| control-plane (GKE, provider configured) | `lookout perf probe --pack=apiserver` — p99 latency by verb/resource; also `--pack=apf`, `--pack=etcd`, `--pack=startup` |

Once the drill-down names a specific broken workload, switch to the
k8s-triage skill: `bundle` first, then narrow.

## Scoping and scheduling

- `--namespace=prod` scopes the namespaced categories to one namespace
  (nodes/certs/webhooks stay cluster-wide); default is the whole cluster.
- For a scheduled sweep, prefer `--format=json` and alert on any
  `health.category` record with `status=degraded`; `--top=5` widens the
  inline naming so most pages need no second call.
- With `--store=/var/lib/lookout/lookout.db` (the sentinel's store), open
  §9.4 triage-status records merge into the scorecard: findings an agent
  already diagnosed carry `triage_status=`/`triage_root_cause=` and the
  agent's severity judgment — report the triaged reality, do not re-derive
  it. Every symptom finding also carries `fingerprint=`, the same §8 key
  the sentinel stamps on its injects, so scan results and incident
  sessions join without guessing.
- Before node maintenance, add `lookout stab drain -A` (or
  `--node=<name>`): everything that will block a drain (gridlocked PDBs)
  or be destroyed by it (bare pods, emptyDir data, singletons), before
  the eviction API hangs on it.

Per-command references (all flags, output-field glossaries) are in
`references/`, generated from the same metadata as `--help`.
