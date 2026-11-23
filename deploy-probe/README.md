# `deploy-probe` (Deprecated compatibility alias)

> [!WARNING]
> **Deprecated in #852:** Compose [`deploy/base/`](../deploy/base/README.md) with [`deploy/components/probe-from/`](../deploy/components/probe-from/README.md) instead (or pass `--set rbac.probeFrom=true` to [`deploy/chart/`](../deploy/chart/README.md)).

Retained during the [`docs/cli-stability-policy.md`](../docs/cli-stability-policy.md) deprecation window as a thin Kustomize alias that renders identically to `deploy/base` + `deploy/components/probe-from`.
