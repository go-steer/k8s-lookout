# `deploy-namespaced` (Deprecated compatibility alias)

> [!WARNING]
> **Deprecated in #852:** Compose [`deploy/base/`](../deploy/base/README.md) with [`deploy/components/scope-namespaced/`](../deploy/components/scope-namespaced/README.md) instead (or pass `--set rbac.scope=namespace` to [`deploy/chart/`](../deploy/chart/README.md)).

Retained during the [`docs/cli-stability-policy.md`](../docs/cli-stability-policy.md) deprecation window as a thin Kustomize alias that renders identically to `deploy/base` + `deploy/components/scope-namespaced`. See [`deploy/components/scope-namespaced/README.md`](../deploy/components/scope-namespaced/README.md) for the capability table and multi-namespace configuration.
