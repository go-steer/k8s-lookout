# Leeway Design — Moved to `go-steer/leeway`

Status: **EXTRACTED** ([#842](https://github.com/go-steer/k8s-lookout/issues/842), 2026-11-23).

The leeway placement-skew and GKE `ComputeClass` fallback subsystem — its
normative design specification, scoring engine, standalone CLI (`leeway report`
and metrics exporter), `LeewayPolicy` / `ClusterLeewayPolicy` CRDs, Grafana
dashboard, and 100k–200.5k-pod `kwokctl` scale benchmarks — now lives in its
own repository:

- **Repository:** <https://github.com/go-steer/leeway>
- **Go package:** `github.com/go-steer/leeway/pkg/leeway`

<!-- design-baseline: LW-1 -->

## Boundary between `k8s-lookout` and `go-steer/leeway` {#LW-1}

- **`lookout watch` stays strictly reactive and leading-incident focused.** The
  in-tree `topology-drift` and `compute-class` watch sources, all 23
  `--topology-*` and `--compute-class-*` flags, in-tree `pkg/leeway`, and
  `cmd/leeway` were removed in `#842`. The eight `leeway.*` wire kinds in
  [`docs/signal-schema-v1.md`](./signal-schema-v1.md) are retired (`Retired:
  true` in `pkg/inject/schema` — name reserved, never reused).
- **Scheduled and on-demand placement/rank sweeps run via `leeway report`**
  from <https://github.com/go-steer/leeway>. Moving from resident watch mode to
  one-shot `leeway report` sweeps trades away four stateful watch behaviors:
  learned EWMA baselines (`leeway.baseline_breach`), live rollout suppression,
  stateful breach/resolve dwell timers, and closed-loop injected `resolved`
  records.
- **Static GKE `ComputeClass` spec posture (`lookout audit computeclass`) and
  cluster-autoscaler scale-up verdict classification (`pkg/sources/capacity`)
  remain in `k8s-lookout`**, importing the pure Go types and rules from
  `github.com/go-steer/leeway/pkg/leeway` with zero `client-go` or cloud SDK
  coupling.
