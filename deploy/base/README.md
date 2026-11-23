# `deploy/base`: cluster-wide `lookout watch` sentinel

The canonical read-only sentinel bundle (`lookout watch`) delivering incidents to a `core-agent` daemon.

## Prerequisites

Create the target namespace and the daemon bearer-token `Secret` out of band (referenced, never committed):

```sh
kubectl create namespace agent-triage
kubectl -n agent-triage create secret generic lookout-watch-token \
  --from-literal=token="$WATCHER_TOKEN"
```

## Apply

```sh
# Kustomize (from a clone or remote ref):
kubectl apply -k deploy/base/
# or via Helm (parity-checked in CI):
helm install lookout-watch ./deploy/chart -n agent-triage --create-namespace
```

## Manifests

- `11-serviceaccount-watcher.yaml` — `ServiceAccount/lookout-watch`
- `12-clusterrole-watcher.yaml` — read-only `ClusterRole/lookout-watch` (guarded by `pkg/checks/state/rbac_test.go`)
- `13-clusterrolebinding-watcher.yaml` — `ClusterRoleBinding/lookout-watch`
- `14-role-watcher-capacity.yaml` & `15-rolebinding-watcher-capacity.yaml` — `kube-system` `Role` pinned to `resourceNames: ["cluster-autoscaler-status"]`
- `16-networkpolicy-watcher.yaml` — default-deny ingress except same-namespace `:9090` scrapers
- `17-service-watcher.yaml` — `Service/lookout-watch-metrics` (`:9090`)
- `51-deployment-watcher.yaml` — single-replica `Recreate` `Deployment/lookout-watch`

Layer optional capabilities from [`../components/`](../README.md#composing-kustomize-components).
