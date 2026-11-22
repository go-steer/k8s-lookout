---
title: Standalone leeway (deprecated — moved to go-steer/leeway)
description: Migration notice for k8s-lookout's deprecated in-tree leeway binary and placement-drift watch sources, superseded by github.com/go-steer/leeway.
sidebar:
  order: 7
---

:::caution[Deprecated — moved to `github.com/go-steer/leeway`]
`k8s-lookout`'s in-tree `cmd/leeway` binary, the `topology-drift` and
`compute-class` watch sources, `--topology-*` / `--compute-class-*`
flags, and `LeewayPolicy` CRDs are **deprecated**
([#842](https://github.com/go-steer/k8s-lookout/issues/842)) and
scheduled for removal after the CLI stability deprecation window. Do not
deploy `/leeway` from `ghcr.io/go-steer/lookout` or enable
`topology-drift` / `compute-class` in `lookout watch` on new clusters.
:::

## Migrate to `go-steer/leeway`

Placement skew across topology domains and GKE `ComputeClass` priority-rank
fallback depth occur while pods are `Running` and `Ready` — they are
posture checks rather than reactive per-cluster incidents. They now live in
the standalone [`github.com/go-steer/leeway`](https://github.com/go-steer/leeway)
repository:

- **Scheduled or on-demand posture sweeps (`leeway report`):** runs
  multi-cluster, paged, per-page-trimmed placement and `ComputeClass`
  priority-rank analysis with zero standing informer memory between runs,
  zero LLM tokens when clean, and ready-to-apply remediation YAML
  (`topologySpreadConstraints` patches, `platform_managed` addon guards,
  `selector_confined` attribution, Hamilton tie-break alignment, and
  `nodeTaintsPolicy: Honor` defaulting).
- **Static spec posture in `lookout` (`lookout audit computeclass`):**
  checks `ComputeClassMissing`, `ComputeClassNoMatchingNodes`,
  `SpotWithoutFallback`, `UnsatisfiableZoneWithAccelerator`, and
  `SingleZoneNodePool` (also included in `lookout audit all -A`), backed by
  `github.com/go-steer/leeway/pkg/leeway`.
- **Standalone metrics daemon:** if you specifically need resident
  Prometheus metrics (`/metrics`, `/healthz`, `/readyz`), build and deploy
  from [`github.com/go-steer/leeway/cmd/leeway`](https://github.com/go-steer/leeway)
  instead of `k8s-lookout`. Never run two resident Pod-informer processes
  against the same cluster unless you have budgeted for the second pod
  cache (see [Sizing & resource footprint](/operations/sizing/)).

## What moving from `lookout watch` to `leeway report` trades away

Replacing resident `lookout watch` leeway sources with scheduled or
on-demand `leeway report` runs intentionally trades away four stateful
watch-only behaviors:

1. **Learned per-workload EWMA baselines (`leeway.baseline_breach`)** —
   one-shot `leeway report` evaluates declared and inferred intent rather
   than maintaining SQLite history across hours.
2. **Rollout-oracle suppression** — `leeway report` does not run a live
   `rollout` informer, so sweeps taken mid-rollout observe point-in-time
   replica placement directly.
3. **Stateful breach and resolve dwell** — one-shot sweeps report current
   breaches immediately without holding 10-minute breach or 30-minute
   resolve timers in memory/SQLite.
4. **Closed-loop `kind=resolved` session injects** — posture findings flow
   through scheduled report diffs rather than per-incident `lookout watch`
   sessions.

See [Tuning placement findings (deprecated)](/operations/placement/) for
reference documentation on the legacy `--topology-*` and
`--compute-class-*` flags while existing deployments complete migration.
