# `deploy-namespaced-strict` (Deprecated compatibility alias)

> [!WARNING]
> **Deprecated in #852:** Compose [`deploy/base/`](../deploy/base/README.md) with [`deploy/components/scope-namespaced-strict/`](../deploy/components/scope-namespaced-strict/README.md) instead (or pass `--set rbac.scope=namespace --set rbac.nodes=false` to [`deploy/chart/`](../deploy/chart/README.md)).

Retained during the [`docs/cli-stability-policy.md`](../docs/cli-stability-policy.md) deprecation window as a thin Kustomize alias that renders identically to `deploy/base` + `deploy/components/scope-namespaced-strict`.
