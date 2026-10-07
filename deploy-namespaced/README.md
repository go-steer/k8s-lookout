# deploy-namespaced: a sentinel for one namespace

This runs `lookout watch` for a single namespace, using only a `Role` and
a `RoleBinding` in that namespace. It needs no cluster-wide permission.
It is meant for a team that owns a namespace on a shared cluster and
will not be given a `ClusterRole`.

You get a much thinner sentinel than the standard one. It watches the
Events, Deployments, Jobs, HPAs and other objects in that namespace and
opens incidents for them. It cannot see nodes, so it loses everything
that depends on them: node health, storm grouping, capacity forecasting
and topology drift. The full list is [below](#what-you-lose).

Use it *instead of* `deploy/`, not on top of it:

```sh
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy-namespaced?ref=vX.Y.Z"
# or, from a clone
kubectl apply -k deploy-namespaced/
```

With the Helm chart, set `rbac.scope=namespace`. Both produce the same
objects (CI diffs them).

## Which namespace it watches

It watches the namespace it runs in. The watched namespace comes from
the pod's own namespace (the downward API sets `POD_NAMESPACE`), so to
deploy it somewhere other than `agent-triage` you only change the
namespace:

```yaml
# your kustomization.yaml
namespace: team-a
resources:
  - github.com/go-steer/k8s-lookout/deploy-namespaced?ref=vX.Y.Z
```

The prerequisites are the same as for `deploy/`: the namespace and the
`lookout-watch-token` Secret must already exist. You will still want to
edit `--daemon-url`, `--cluster-name` and `--owner`.

## What changes compared to `deploy/`

- The `ClusterRole`, its `ClusterRoleBinding` and the `kube-system`
  capacity `Role` are gone.
- A `Role` and `RoleBinding` in the sentinel's namespace carry the
  namespaced rules of `deploy/12-clusterrole-watcher.yaml`.
  `role-watcher.yaml` says what was left out and why.
- The watcher gets `--watch-scope=namespace --namespace=$(POD_NAMESPACE)`.
  Every informer then lists and watches that one namespace, so nothing
  from other namespaces is ever loaded into memory.
- `--storm=on` becomes `--storm=auto`. Storm correlation needs nodes, and
  an explicit `--storm=on` would refuse to start without them.

## What still works

- These sources: `k8s-events`, `rollout`, `workload`, `autoscaling`,
  `degradation` and `ingress`. `gateway` also works if the Gateway API
  CRDs are installed.
- Incident sessions, deduplication, severity routing, the watchboard and
  the occurrence store (`--store`).
- Recovery: incidents in the namespace are followed to resolution.
- Enrichment bundles for incidents in the namespace. Parts that need
  cluster-scoped objects (the node, storage classes, ingress classes,
  cluster roles) are reported as skipped in the bundle head instead.

## What you lose

At startup, `--sources=auto` skips each source that needs a cluster-wide
grant and logs one line naming the missing permission. `--storm=auto`
resolves to off the same way. If you name one of these sources in an
explicit `--sources` list, or set `--storm=on`, the sentinel refuses to
start. That is the same rule as for any other missing grant.

| Lost | Why |
| --- | --- |
| `object-state`: pod, node, Deployment, EndpointSlice and PDB state signals | watches nodes |
| storm correlation: one session for many failures with a common cause | the topology graph needs nodes |
| watchboard ancestor reattachment, graph history and `--at` queries | ride the storm topology graph |
| `capacity`: pending-pod and autoscaler forecasting | watches nodes, reads `kube-system` |
| `saturation`: CPU, memory and disk exhaustion forecasts | lists nodes |
| `topology-drift`: placement drift across zones and node groups | needs nodes and PersistentVolumes |
| `compute-class`: GKE compute-class rank drift | ComputeClasses and nodes are cluster-scoped |
| `expiry`: certificate, token and webhook CA expiry | reads cluster-scoped webhook configurations |
| `ClusterLeewayPolicy` overrides | cluster-scoped; namespaced `LeewayPolicy` still works |
| node details in enrichment bundles | nodes are cluster-scoped |

`quota`, `notifications` and `token-burn` are unaffected. They never
read the cluster and are never auto-enabled.

## Getting some of it back with one ClusterRole

If your cluster admin will grant read access to nodes, which hold no
tenant data, most of the losses come back:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: lookout-watch-nodes
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: lookout-watch-nodes
subjects:
  - kind: ServiceAccount
    name: lookout-watch
    namespace: team-a
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: lookout-watch-nodes
```

With it, `object-state` and storm correlation come back, and
`--storm=auto` turns on. `saturation` also comes back; its disk
dimension additionally needs `get` on `nodes/proxy`. `capacity` also
needs `deploy/14` and `deploy/15` (a `Role` in `kube-system`), and
`topology-drift` also needs `list`/`watch` on `persistentvolumes`.
Pods are still watched in your namespace only. Node signals are not
tied to a namespace, so you also get condition signals for every node
in the cluster. Restart the sentinel after granting: the checks run at
startup.

## Things to know

- **`--namespace` on its own is not this.** Without
  `--watch-scope=namespace`, `--namespace` only filters what is reported.
  Every namespace is still watched and held in memory. Only
  `--watch-scope=namespace` limits what is watched.
- **One namespace per sentinel.** To cover several namespaces, run one
  sentinel in each.
- **`--exclude-namespace`** has nothing to exclude here, since only one
  namespace is watched. Naming the watched namespace is rejected.
- **Multi-cluster mode** (`--clusters` / `--clusters-from`) applies the
  scope to every cluster: each one is watched in a namespace with the
  same name.

The operations guide's [Scoping a sentinel](../docs/site/src/content/docs/operations/scoping.md)
page covers the same ground alongside the other ways to narrow a
sentinel.
