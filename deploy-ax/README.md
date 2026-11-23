# `deploy-ax` (Deprecated compatibility alias)

> [!WARNING]
> **Deprecated in #852:** Compose [`deploy/base/`](../deploy/base/README.md) with [`deploy/components/sink-ax/`](../deploy/components/sink-ax/README.md) instead (or pass `--set sink.type=ax` to [`deploy/chart/`](../deploy/chart/README.md)).

Retained during the [`docs/cli-stability-policy.md`](../docs/cli-stability-policy.md) deprecation window as a thin Kustomize alias that renders identically to `deploy/base` + `deploy/components/sink-ax`.
