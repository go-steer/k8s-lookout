# deploy-namespaced: a sentinel for a list of namespaces

This runs `lookout watch` for chosen namespaces instead of the whole
cluster. It watches only those namespaces, so nothing from any other
namespace is ever loaded into memory. Each namespace needs a `Role`.

There are two tiers, depending on whether your cluster admin will grant
one small `ClusterRole`:

- **`deploy-namespaced/` (the default).** Roles in your namespaces, plus
  a ClusterRole that can only read nodes and persistent volumes. Almost
  everything keeps working, including grouping a node failure into one
  incident.
- **`deploy-namespaced-strict/`.** Roles only, no cluster-wide grant at
  all. This is for a team on a shared cluster that will never be given a
  ClusterRole. It is much thinner: anything that needs to see nodes is
  turned off.

The [table below](#what-each-tier-keeps) says exactly which parts survive
each tier.

```sh
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy-namespaced?ref=vX.Y.Z"
# or, with no cluster-wide grant at all
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy-namespaced-strict?ref=vX.Y.Z"
```

Use one of them *instead of* `deploy/`, not on top of it. With the Helm
chart, set `rbac.scope=namespace` for the default tier, and add
`rbac.nodes=false` for the strict one. Each pair produces the same
objects (CI diffs them).

## What each tier keeps

| | `deploy/` (cluster) | `deploy-namespaced/` | `deploy-namespaced-strict/` |
| --- | --- | --- | --- |
| `k8s-events`, `rollout`, `workload`, `autoscaling`, `degradation`, `ingress` | yes | yes | yes |
| `gateway` (if the Gateway API CRDs are installed) | yes | yes | yes |
| `object-state`: pod, node, Deployment, EndpointSlice and PDB state | yes | yes | no: needs nodes |
| `saturation`: CPU and memory forecasts | yes | yes [^disk] | no: lists nodes |
| `capacity`: pending pods and autoscaler state | yes | yes | no: needs nodes and the `kube-system` Role |
| `topology-drift`: placement drift across zones and node groups | yes | yes | no: needs nodes and PersistentVolumes |
| storm correlation: one incident for many failures with one cause | yes | yes | no: the topology graph needs nodes |
| watchboard reattachment, graph history and `--at` queries | yes | yes | no: they need the storm topology graph |
| `compute-class`: GKE compute-class rank drift | yes | no [^cc] | no |
| `expiry`: certificate, token and webhook CA expiry | yes | no: reads cluster-scoped webhook configurations | no |
| `ClusterLeewayPolicy` overrides (namespaced `LeewayPolicy` always works) | yes | no | no |
| incident sessions, dedup, routing, the store, recovery | yes | yes | yes |
| enrichment bundles | full | namespaced parts; storage and ingress classes and cluster roles are reported as skipped | same, and no node details |

[^disk]: The disk dimension also needs `get` on `nodes/proxy`. Without
it, saturation runs with that one dimension off and says so at startup.
[^cc]: Add `list`/`watch` on `computeclasses.cloud.google.com` to the
nodes ClusterRole to bring it back.

At startup, the default `--sources=auto` skips each source a tier cannot
run and logs one line naming the missing permission. `--storm=auto` turns
off the same way. If you name a skipped source in an explicit `--sources`
list, or set `--storm=on`, the sentinel refuses to start instead. That is
the same rule as for any other missing grant. `quota`, `notifications` and
`token-burn` are not affected by either tier. They never read the cluster
and are never auto-enabled.

## Which namespaces it watches

Out of the box it watches the namespace it runs in. The watched namespace
comes from the pod's own namespace (the downward API sets `POD_NAMESPACE`).
To deploy it somewhere other than `agent-triage`, change the namespace:

```yaml
# your kustomization.yaml
namespace: team-a
resources:
  - github.com/go-steer/k8s-lookout/deploy-namespaced?ref=vX.Y.Z
```

To watch more namespaces, give each one the same Role and RoleBinding, and
list them all in `--namespace`. With the chart this is one value:

```sh
helm install lookout-watch oci://ghcr.io/go-steer/charts/lookout \
  --namespace agent-triage --set rbac.scope=namespace \
  --set-json 'rbac.namespaces=["team-a","team-b","team-c"]'
```

With kustomize, copy `role-watcher.yaml` and `rolebinding-watcher.yaml` once
per extra namespace, changing `metadata.namespace` but not the subject.
Then replace the last `--namespace` argument:

```yaml
patches:
  - target: {kind: Deployment, name: lookout-watch}
    patch: |-
      - op: replace
        path: /spec/template/spec/containers/0/args/12
        value: --namespace=team-a,team-b,team-c
```

If one listed namespace is missing its Role, the sentinel skips that
namespace and watches the rest. It logs one line naming the namespace and
the missing permission, and counts it on `lookout_namespace_errors_total`.
Alert on that counter, because a skipped namespace is a gap that otherwise
looks like a quiet one. If every listed namespace is refused, the sentinel
does not start.

The prerequisites are the same as for `deploy/`: the sentinel's namespace
and the `lookout-watch-token` Secret must already exist. You will still want
to edit `--daemon-url`, `--cluster-name` and `--owner`.

## What changes compared to `deploy/`

- The cluster-wide `ClusterRole` and its binding are gone. In their place:
  - a `Role` and `RoleBinding` in each watched namespace, carrying the
    namespaced rules of `deploy/12-clusterrole-watcher.yaml`
    (`role-watcher.yaml` says what was left out and why);
  - in the default tier, `lookout-watch-nodes`, a ClusterRole that can only
    read nodes and persistent volumes.
- The watcher gets `--watch-scope=namespace --namespace=$(POD_NAMESPACE)`.
- The strict tier also drops the `kube-system` capacity Role and changes
  `--storm=on` to `--storm=auto`.

## `--namespace` is a watch scope only with `--watch-scope=namespace`

Two different things share the `--namespace` flag:

- **With `--watch-scope=namespace`, it is the watch scope.** Only the
  listed namespaces are listed, watched and cached. This is a security
  boundary: the process cannot see other namespaces' objects, and RBAC
  backs that up.
- **Without it, it is only an output filter.** Every namespace is still
  watched and held in memory. Only reporting is limited. Names, labels,
  images, owner chains and node placement for the whole cluster stay
  resident in the process. Nobody should rely on that for isolation.

`--exclude-namespace` has nothing to remove under `--watch-scope=namespace`,
since only the listed namespaces are watched. Naming a listed namespace in
it is rejected.

## Things to know

- **One process, many namespaces.** Each namespace gets its own watch
  connection, but there is one copy of everything else: one deduplication
  cache, one store, one topology graph. A node failure that hits pods in
  three of your namespaces is still one incident.
- **Multi-cluster mode** (`--clusters` / `--clusters-from`) applies the
  scope to every cluster: each one is watched in namespaces with the same
  names.
- **Restart after changing grants.** Permissions are checked at startup.

The operations guide's [Scoping a sentinel](../docs/site/src/content/docs/operations/scoping.md)
page covers the same ground alongside the other ways to narrow a sentinel.
