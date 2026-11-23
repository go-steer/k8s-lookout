# `deploy-no-secrets` (Deprecated compatibility alias)

> [!WARNING]
> **Deprecated in #852:** Compose [`deploy/base/`](../deploy/base/README.md) with [`deploy/components/no-secrets/`](../deploy/components/no-secrets/README.md) instead (or pass `--set rbac.secrets=false` to [`deploy/chart/`](../deploy/chart/README.md)).

Retained during the [`docs/cli-stability-policy.md`](../docs/cli-stability-policy.md) deprecation window as a thin Kustomize alias that renders identically to `deploy/base` + `deploy/components/no-secrets`.
