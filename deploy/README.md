# Deploying `k8s-lookout`

Kubernetes Kustomize bundles, composable Kustomize `Component`s, and the Helm chart for running `lookout watch` (the per-cluster sentinel) and `lookout mcp` (the shared authenticated MCP endpoint).

## Layout

| Path | Kind | Purpose |
| --- | --- | --- |
| [`base/`](./base/) | Kustomize base | Cluster-wide `lookout watch` sentinel posting incidents to a `core-agent` daemon |
| [`components/sink-ax/`](./components/sink-ax/) | Kustomize `Component` | Switch `lookout watch` from `--daemon-url` to `--sink=ax` + Agent Substrate router mTLS |
| [`components/no-secrets/`](./components/no-secrets/) | Kustomize `Component` | Drop `secrets` and `serviceaccounts` `list` rules and pass `--enrich-lists=all,-secrets,-serviceaccounts` |
| [`components/scope-namespaced/`](./components/scope-namespaced/) | Kustomize `Component` | Watch listed namespaces (`Role` per namespace + minimal node/PV `ClusterRole`) |
| [`components/scope-namespaced-strict/`](./components/scope-namespaced-strict/) | Kustomize `Component` | Watch listed namespaces with `Role` grants only (zero `ClusterRole` or `kube-system` `Role`) |
| [`components/probe-from/`](./components/probe-from/) | Kustomize `Component` | Opt-in privileged `pods/ephemeralcontainers` grant + Kubernetes 1.30+ `ValidatingAdmissionPolicy` for `lookout net probe-from` |
| [`mcp/`](./mcp/) | Kustomize base | Standalone `lookout mcp` Gateway deployment with per-caller Google OAuth (`mcp/domain/` + optional `mcp/oauth/`) |
| [`prometheus-operator/`](./prometheus-operator/) | Kustomize add-on | Optional `ServiceMonitor` scraping `lookout-watch-metrics` |
| [`chart/`](./chart/) | Helm chart | Parameterizes `base/`, all `components/*`, `sink.type=webhook`, and `mode=mcp` (verified in CI by `dev/tools/verify-helm-parity`) |

`kubectl apply -k deploy/` builds [`base/`](./base/) directly for backwards compatibility. Existing top-level `deploy-*` directories remain as deprecated Kustomize aliases during the [`docs/cli-stability-policy.md`](../docs/cli-stability-policy.md) deprecation window; see [`docs/repo-map.md`](../docs/repo-map.md#deployment-variants) for the full directory table.

## Composing Kustomize components

Components under `deploy/components/` combine in a single `kustomization.yaml` — for example, running the AX sink without cluster-wide `Secret` list access:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: agent-triage
resources:
  - github.com/go-steer/k8s-lookout/deploy/base?ref=vX.Y.Z
components:
  - github.com/go-steer/k8s-lookout/deploy/components/sink-ax?ref=vX.Y.Z
  - github.com/go-steer/k8s-lookout/deploy/components/no-secrets?ref=vX.Y.Z
```

> [!IMPORTANT]
> When combining `scope-namespaced` (or `scope-namespaced-strict`) with `no-secrets`, list the scope component **before** `no-secrets` so `no-secrets` narrows the `Role` introduced by the scope component.
