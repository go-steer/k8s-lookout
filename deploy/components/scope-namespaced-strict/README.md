# `deploy/components/scope-namespaced-strict`: strictly namespaced (`Role`-only) sentinel

Kustomize `Component` for multi-tenant clusters where the sentinel cannot hold any `ClusterRole` or cross-namespace `kube-system` `Role`.

## What it changes

Includes [`../scope-namespaced`](../scope-namespaced/README.md), deletes `ClusterRole/lookout-watch-nodes`, `ClusterRoleBinding/lookout-watch-nodes`, `Role/lookout-watch-capacity`, and `RoleBinding/lookout-watch-capacity`, and rewrites `--storm=on` to `--storm=auto` via named `replacements` (so startup skips node-dependent sources cleanly instead of failing on missing `nodes` access).

Use `scope-namespaced-strict` **instead of** `scope-namespaced`, not alongside it. See [`../scope-namespaced/README.md`](../scope-namespaced/README.md) for the full source capability table.

## Usage

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
components:
  - ../../components/scope-namespaced-strict
```

**Helm equivalent:** `helm install lookout-watch ./deploy/chart -n agent-triage --set rbac.scope=namespace --set rbac.nodes=false`
