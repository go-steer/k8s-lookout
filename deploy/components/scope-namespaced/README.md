# `deploy/components/scope-namespaced`: namespace-scoped sentinel

Kustomize `Component` scoping `lookout watch` to a list of namespaces (`--watch-scope=namespace --namespace=$(POD_NAMESPACE)`) using a per-namespace `Role` plus a minimal `ClusterRole` (`lookout-watch-nodes`) that reads only `nodes` and `persistentvolumes`.

## What each tier keeps

| Capability | `deploy/base/` (cluster) | `scope-namespaced` | `scope-namespaced-strict` |
| --- | --- | --- | --- |
| `k8s-events`, `rollout`, `workload`, `autoscaling`, `degradation`, `ingress` | yes | yes | yes |
| `gateway` (if the Gateway API CRDs are installed) | yes | yes | yes |
| `object-state`: pod, node, Deployment, EndpointSlice and PDB state | yes | yes | no: needs nodes |
| `saturation`: CPU and memory forecasts | yes | yes [^disk] | no: lists nodes |
| `capacity`: pending pods and autoscaler state | yes | yes | no: needs nodes and the `kube-system` Role |
| storm correlation: one incident for many failures with one cause | yes | yes | no: the topology graph needs nodes |
| watchboard reattachment, graph history and `--at` queries | yes | yes | no: they need the storm topology graph |
| `expiry`: certificate, token and webhook CA expiry | yes | no: reads cluster-scoped webhook configurations | no |
| incident sessions, dedup, routing, the store, recovery | yes | yes | yes |
| enrichment bundles | full | namespaced parts; storage/ingress classes and cluster roles reported in `skipped=` | same, and no node details |

[^disk]: The disk dimension also needs `get` on `nodes/proxy`. Without it, `saturation` runs with that dimension off and logs one line at startup.

## Watching multiple namespaces

By default the sentinel watches its own namespace via `POD_NAMESPACE`. To watch additional namespaces:

- **Helm:** `--set rbac.scope=namespace --set-json 'rbac.namespaces=["team-a","team-b"]'`
- **Kustomize:** create a `Role` + `RoleBinding` per watched namespace and rewrite `--namespace=` by flag match (no positional `args/<N>` index):

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
  - namespaces-config.yaml # local-config ConfigMap with data.ns: --namespace=team-a,team-b
components:
  - ../../components/scope-namespaced
replacements:
  - source:
      kind: ConfigMap
      name: namespaces-config
      fieldPath: data.ns
    targets:
      - select: {kind: Deployment, name: lookout-watch}
        fieldPaths:
          - spec.template.spec.containers.[name=watcher].args.[=--namespace=]
```

> [!IMPORTANT]
> When combining with [`no-secrets`](../no-secrets/README.md), list `scope-namespaced` **before** `no-secrets` in `components:` (reversed order fails `kustomize build` closed so `Role/lookout-watch` never silently retains `secrets: list`).
