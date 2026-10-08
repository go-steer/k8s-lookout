---
title: Scoping a sentinel
description: Narrowing what one sentinel watches — --exclude-namespace and --watch-scope=namespace as real watch scopes, a sentinel that runs under a namespaced Role, splitting by source, and which sources can be separated without paying for a second cache.
sidebar:
  order: 6
---

The canonical deployment is one sentinel per cluster watching
everything, and it is the right default: one informer cache, one
topology graph, one credential boundary. This page is for the cases
where it is not — where a cluster has namespaces you have no business
watching, or where you want the drift counters without also running
the event watcher.

There are two axes to cut along, and they compose: **which namespaces**
a sentinel watches, and **which sources** it runs.

## Namespaces

Three flag forms look alike and are not.

| Flag | What it does |
| --- | --- |
| `--namespace=a,b` | Allow-list applied to **output** only. Every namespace is still listed, watched, decoded and cached; signals from namespaces outside the list are dropped before they are emitted. Not a security boundary. |
| `--exclude-namespace=x,y` | Deny-list applied to **the watch**. The namespaced informers carry a `metadata.namespace!=` field selector, so `x` and `y` are never listed and never enter the cache. |
| `--watch-scope=namespace --namespace=a,b` | **The watch** is exactly `a` and `b`. Each listed namespace gets its own namespaced watch, nothing else enters the cache, and a `Role` in each is enough for everything namespaced. This one *is* a security boundary. See [Watching a list of namespaces](#watching-a-list-of-namespaces). |

### `--namespace` is not a security boundary

Worth saying plainly, because the flag name invites the opposite
reading: a sentinel run with `--namespace=payments` holds every other
namespace's pods in memory. Names, labels, images, owner chains and
node placement for the whole cluster are resident in its cache. (What
is *not* resident is resolved secret values — those are stripped on the
way into the cache for every namespace, watched or not.) If the
requirement is "this process must not be able to see namespace `x`",
`--namespace` alone does not meet it and never did. `--exclude-namespace`
and `--watch-scope=namespace` do, and RBAC does it better still.

The same flag means two different things depending on
`--watch-scope`. With `--watch-scope=namespace`, `--namespace` is the
watch scope and a security boundary. Without it, `--namespace` is an
output filter and nothing more. Check which one a deployment uses before
relying on it.

### `--exclude-namespace` shrinks the process

```
lookout watch --exclude-namespace=kube-system,gmp-system
```

The sentinel logs the selector it derived at startup:

```
watch: --exclude-namespace is scoping the watch — the namespaced
informers list and watch with field selector
"metadata.namespace!=gmp-system,metadata.namespace!=kube-system", so
excluded namespaces never enter the cache; nodes are cluster-scoped and
unaffected
```

On a busy cluster the two system namespaces above are frequently a
third to a half of all pods, and they are pods nobody is paging on.
Excluding them cuts cache size, decode work and watch traffic by
roughly their share.

Three things to know:

- **Nodes are unaffected.** They are cluster-scoped, so a namespace
  deny list has nothing to remove from them — and the API server
  *rejects* `metadata.namespace` on a cluster-scoped LIST rather than
  ignoring it, so the node informer runs on its own unfiltered
  factory. This costs no extra watch: the node informer is still one
  stream shared by every reader.
- **Correlation only sees what is watched.** Storm correlation and the
  topology graph are built from the same informers, so a node failure's
  blast radius will not include pods in an excluded namespace. That is
  usually what you want — you excluded them — but it means an excluded
  namespace cannot appear as collateral damage either.
- **An allow-list costs a watch per namespace.** Field selectors have
  no `OR`, so watching *M* namespaces needs *M* namespaced watches of
  each object type. That is what `--watch-scope=namespace` does (below).
  It is the right shape when the listed namespaces are a small part of
  the cluster. An exclusion of any length is one selector on one stream,
  which is why that direction is the cheap one for trimming a few
  namespaces out of a whole cluster.
- **Under `--watch-scope=namespace` there is nothing left to exclude.**
  Only the listed namespaces are watched, and naming one of them in
  `--exclude-namespace` is a usage error.

### Or use RBAC

The strongest version of "do not watch namespace `x`" is not to grant
it. A sentinel whose ServiceAccount cannot list pods cluster-wide fails
loudly at startup naming the source and the permission (see
[Troubleshooting](/operations/troubleshooting/)) rather than watching
an empty cache. `--exclude-namespace` is the right tool when you hold a
cluster-wide grant and want to spend less; RBAC is the right tool when
the grant itself is the problem.

## Watching a list of namespaces

The sentinel can watch chosen namespaces instead of the whole cluster:

```
lookout watch --watch-scope=namespace --namespace=team-a,team-b
```

Each listed namespace gets its own namespaced watch, so nothing from any
other namespace is loaded. The startup permission check asks for each
namespaced grant in every listed namespace, so a Role in each passes it.
Everything after the watch stays single: one copy of each source, one
deduplication cache, one store, one topology graph. A node failure that
hits pods in several listed namespaces is still one storm incident. One
namespace is just a list of one.

Some sources need nodes or another cluster-scoped object, which a Role
cannot grant. So there are two supported tiers:

| Tier | Grants | What runs |
| --- | --- | --- |
| **Default** (`deploy-namespaced/`, chart `rbac.scope=namespace`) | a Role per namespace, plus a ClusterRole that can only read nodes and PersistentVolumes | everything except `compute-class` and `expiry` |
| **Strict** (`deploy-namespaced-strict/`, plus chart `rbac.nodes=false`) | Roles only | `k8s-events`, `rollout`, `workload`, `autoscaling`, `degradation`, `ingress`, and `gateway` if its CRDs are installed |

The strict tier also loses `object-state`, `saturation`, `capacity`,
`topology-drift` and storm correlation. Under the default
`--sources=auto`, each source a tier cannot run is skipped with one log
line naming the missing permission, and `--storm=auto` resolves to off
the same way. Naming a skipped source in `--sources`, or setting
`--storm=on`, makes startup fail instead. The
[deploy-namespaced README](https://github.com/go-steer/k8s-lookout/tree/main/deploy-namespaced)
has the source-by-source table.

Both overlays first ship in v0.33.0. Use one instead of `deploy/`:

```sh
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy-namespaced?ref=v0.34.0"
```

They watch the namespace the sentinel runs in. With the chart, list
more namespaces in `rbac.namespaces`; each gets its own Role and
RoleBinding.

- `--watch-scope=namespace` needs at least one `--namespace` value.
  Without one it is a usage error (exit 2).
- A listed namespace whose Role is missing is skipped, not fatal. The
  sentinel logs one line naming it and counts it on
  `lookout_namespace_errors_total{namespace,cause}`. Alert on that
  counter: a skipped namespace is a coverage gap that otherwise looks
  quiet. If every listed namespace is refused, startup fails.
- In multi-cluster mode the scope applies to every cluster: each one is
  watched in namespaces with the same names.
- Naming a listed namespace in `--exclude-namespace` is rejected.

## Without the Secret grant

The shipped ClusterRole's broadest rule is `list` on `secrets`
cluster-wide. A Secret list returns *values*, so whoever compromises
the sentinel pod can read every Secret in the cluster, however
carefully the sentinel itself masks them. If your security review
will not accept that grant, deploy the shipped variant that does not
hold it, `deploy-no-secrets/`, *instead of* `deploy/` (first shipped
in v0.29.0):

```sh
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy-no-secrets?ref=v0.34.0"
# or, from a clone
kubectl apply -k deploy-no-secrets/
```

With the chart, set `rbac.secrets=false`:

```sh
helm install lookout-watch oci://ghcr.io/go-steer/charts/lookout \
  --namespace agent-triage --set rbac.secrets=false   # plus your usual args
```

Both produce the same deployment (CI diffs them). It is `deploy/` with
two changes:

- The ClusterRole loses its `secrets: list` and `serviceaccounts: list`
  rules. Every other rule is unchanged, including the `services`,
  `ingresses` and `networkpolicies` grants that routing and the store
  read.
- The watcher gets `--enrich-lists=all,-secrets,-serviceaccounts`, so
  enrichment never requests the two lists and the apiserver audit log
  shows no 403 per incident. The chart skips this flag if your `args`
  already set `--enrich-lists`.

### What it loses

- **The `expiry` source, entirely.** Under the default
  `--sources=auto` the startup probe finds the `secrets` grant missing
  and skips the source with one log line. The `secrets` grant is a
  required one, so the skip also takes webhook CA bundle expiry,
  cert-manager renewal state and ACME stall detection with it, not
  just TLS-Secret and ServiceAccount-token expiry. If you name `expiry` in an explicit
  `--sources` list, startup fails instead, which is the §11 rule for
  named sources.
- **Secret and ServiceAccount checks in an enrichment bundle's
  `edges` section.** These are missing or wrong-typed Secret
  references (env, `envFrom`, volumes, `imagePullSecrets`, including
  those inherited from the ServiceAccount), missing Secret keys, an
  Ingress TLS secret that does not exist, the TLS certificate
  expiry/validity checks, and whether the workload's ServiceAccount
  exists. The bundle head says
  `skipped=secrets,serviceaccounts`, and those checks stay silent
  rather than reporting every reference as missing. Edges run only on
  enrichment's scoped-list path; the live `--storm` path never ran
  them.

Everything else runs exactly as it does under `deploy/`. That includes
every other source, the topology graph and storm correlation, node
incidents, capacity, ConfigMap/Service/Ingress/RBAC edge checks, and
the occurrence store.

### Bringing expiry back for chosen namespaces

To get expiry back for chosen namespaces, keep the variant's
ClusterRole. In each namespace whose certificates you care about,
create a Role granting `list` on `secrets` and `serviceaccounts`, bind
it to the `lookout-watch` ServiceAccount, and add
`--expiry-namespaces=a,b` to the watcher. The flag narrows what the
source declares: per-namespace Secret and ServiceAccount lists, plus
the webhook rules, which the variant keeps cluster-wide. So the probe
passes and expiry runs, scanning Secrets only in those namespaces.
Enrichment stays as it was, because `--enrich-lists` still deselects
both kinds.

## Read-path commands under the `view` role

Many teams give a read-only agent the built-in `view` ClusterRole.
It grants read access to the namespaced workload, networking and
configuration objects. It grants no Secrets, no
`rbac.authorization.k8s.io` objects, and none of the cluster-scoped
kinds: Nodes, PersistentVolumes, StorageClasses, IngressClasses and
admission webhook configurations. Nor does it grant `metrics.k8s.io`,
although metrics-server's own aggregated role adds that to `view` on
clusters that run metrics-server.

Every read-path command treats a refused read as information. The
part of the answer that needed it says so and everything else is still
checked. The command exits 0 with its usual summary line, on the CLI
and over MCP alike. Where the whole answer depends on the refused
kind, the answer is that one record and nothing else. Any error other
than Forbidden still fails the command, because a broken API server is
not a permission gap.

Every such line says why the read was refused and what fixes it, in
one shared wording:

```
forbidden: list nodes — cluster-scoped, not granted by the built-in view role; grant list on nodes (core) via a ClusterRole, as lookout's shipped ClusterRole does — node.* findings not checked
```

In order, the line gives:

- the refused verb and resource, with its API group;
- whether the resource is cluster-scoped (only a ClusterRole can grant
  it) or namespaced (a ClusterRole or a per-namespace Role);
- the cause. "Not granted by the built-in view role" appears only when
  the resource really is outside `view`. When `view` does grant it, the
  line says "this identity lacks it", because a custom role is to
  blame;
- the fix: the grant, plus "as lookout's shipped ClusterRole does"
  when `deploy/12-clusterrole-watcher.yaml` (and the chart) grants it;
- after the last dash, what the command could not judge without it.

| Command | Under `view` |
| --- | --- |
| `health` | `nodes`, `certs` and `webhooks` answer `status=unavailable`, with the refusal line as the message. On a cluster running cert-manager, whose own aggregated roles extend `view` to Certificates, `certs` instead scores from the Certificates and adds `unverified=` naming the TLS Secrets no Certificate manages, with its refusal line. `services` still scores and adds `unverified=` naming the Ingress class and TLS secret references it could not check, each with its refusal line. The other categories score as usual. `control-plane` needs a cloud provider either way. |
| `triage delta` | Whole cluster: one `read.unavailable` for `nodes` (`node.*` findings not checked). Every other class answers as usual. With `--namespace` the node class is off anyway. |
| `triage top` | Without `metrics.k8s.io`: one `read.unavailable` for `pods.metrics.k8s.io` and no rows. With it (metrics-server's aggregated role): under `-A`, one `read.unavailable` for `nodes` and the node view drops out. The container rows still answer. `--workload` resolves its pods from the owner tree only. |
| `triage spec` | A Node or Secret target: one `read.unavailable` for the refused `get`. |
| `state edges --workload=…` | One `read.unavailable` finding per refused list that affects the answer: `secrets`, the four RBAC kinds, `ingressclasses` and `storageclasses`, each naming the edges it could not verify. Those edges are not reported as missing. Entered as `--workload=Service/…`, only the gaps that mode reads are reported. |
| `state webhooks` | One `read.unavailable` for the webhook configurations, and no other output: every check starts from them. |
| `state volumes` | One `read.unavailable` each for `persistentvolumes`, `volumeattachments` and `nodes`. Zone conflicts and attachment errors go unchecked. RWO multi-attach and unconsumed claims still answer. An attachment whose PV or node could not be read is not called orphaned. |
| `state storage` | One `read.unavailable` each for `storageclasses` and `persistentvolumes`. Every claim judgment needs both, so nothing else is reported. |
| `stab drain` | One `read.unavailable` for `nodes`. Nodes are taken from the pods bound to them, so every blocker is still found. A node with no pods is left out of `nodes=`, since it has nothing to block a drain. |
| `stab scaledown` | One `read.unavailable` for `nodes` and no node judged: utilization is requests over each Node's allocatable. |
| `audit rbac` | One `read.unavailable` per RBAC kind, and no judgment: every claim reads a binding against its role's rules. |
| `audit workloads` | One `read.unavailable` for `nodes`. `audit.rigid_scheduling` is not judged and the `nodes=` note is left out. Every other claim answers. |
| `triage radius` | One `read.unavailable` finding each for `nodes` and `secrets`. A Secret the target mounts is listed with `observed=unknown` rather than as `radius.missing`. RBAC objects are no part of a blast radius, so their refusal is not reported. |
| `triage changes` | One `read.unavailable` finding for `nodes`: zones are read from Node labels, so neighbors reached only through a shared zone are out of scope. Every other change is reported as usual. |
| `triage events --workload=…` | Nothing is lost. The owner-reference tree is resolved from pods and workload objects only. |
| `scan` | Each stage reports its own gaps as above, as `read.unavailable` records, instead of failing as `scan.check_failed`, and the summary line's `unavailable=` names those stages. The edge drill-down still runs and names the lists it was refused in a short `drilldown_skipped=` note. `state edges --workload=…` on a flagged workload prints the full refusal line for each. |

`bundle` names the gaps in its head finding's `skipped=` note and
gives the refusal line for each in that finding's message. `triage
list` keeps its `skipped=` note (`skipped=Secret:forbidden`) and adds
one `read.unavailable` record per refused kind. `triage events`,
`triage logs`, `stab drift`, `state wi` and the other `audit` commands
read nothing `view` refuses. To get a missing check back, grant the
verb and resource the line names. A hermetic test,
`pkg/checks/all/viewrole_test.go`, runs every read-path command
against exactly `view` and fails if any of them exits non-zero.

## Custom roles that refuse a read `view` grants

A custom role can refuse something `view` does grant: pods, events,
PersistentVolumeClaims, a workload kind. The same rule applies. The
part of the answer that needed the read reports it in the shared
wording ("this identity lacks it", since `view` is not to blame) and
the rest is still checked, exit 0.

A few commands cannot give any honest answer without one particular
read, because it is their subject or the input every claim depends
on. Those still exit 1, but the diagnostic on stderr (and the MCP
tool error) is the same refusal line, ending in what was lost:

```
lookout stab drain: forbidden: list pods — namespaced, this identity lacks it; grant list on pods (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does — the command cannot answer without it
```

| Command | Exits 1 when it is refused | Everything else it reads |
| --- | --- | --- |
| `triage events` | `events`; with `--workload`, the target's own kind | A refused kind in the owner tree, or HPAs, is one `read.unavailable`; that hop's events are left out of the timeline. |
| `triage top --workload=…` | the target's kind, `pods`, and any kind between them (`replicasets` for a Deployment, `jobs` for a CronJob) | Kinds off that path change nothing. Without `--workload`, refused `pods` is one `read.unavailable`. |
| `triage logs` | the target and `pods` | |
| `triage changes`, `triage radius`, `state edges`, `bundle` | the target's own kind (the error says it could not be looked up, rather than "not found") | One `read.unavailable` per refused kind that affects the answer (`bundle`: `skipped=` plus the refusal lines in its head finding). Refused `events` drop the rescale entries from `triage changes`. |
| `stab drain`, `stab scaledown` | `pods`, `poddisruptionbudgets`, `replicasets`, `deployments`, `statefulsets` | A drain verdict that skipped a blocker would be a false "drainable". Refused `nodes` still degrade as above. |
| `audit netpol` | `networkpolicies`, `namespaces`, and every pod-template kind | Coverage compares the policies against every pod template in a namespace. A missing kind would make a live policy look like it selects nothing. |
| `audit hardening` | nothing | A refused pod-template kind is not audited. Refused `serviceaccounts` skip `audit.default_sa_automount`. Refused `namespaces` skip both namespace claims and the `namespaces=` note. |
| `audit workloads` | the `--workload` target's kind | A refused workload kind is not audited. Refused PDBs skip `audit.no_pdb`. Refused HPAs skip every replica-floor claim (`single_replica`, `no_pdb`, `no_spread`, `hpa_*`). |
| `stab drift` | the `--workload` target's kind | A refused kind is not scanned and does not count toward GitOps manager detection. |
| `triage list` | nothing | One `read.unavailable` per refused kind, plus the `skipped=` note. |

`health`, `triage delta`, `scan`, `state volumes`, `state storage`,
`state webhooks`, `triage spec` and `net probe-from` degrade under
every such role as well. `pkg/checks/all/refuse_core_test.go` refuses
each read `view` grants, one at a time, against every read-path
command. It fails if a command exits 1 where it should degrade, exits
0 without a refusal line, or fails without the shared wording.

`net probe-from` is not a read-path command, but the same guard holds
it to the same rule: under `view` it changes nothing and answers with
one `probe.refused` record in the shared wording, exit 0 (see below).

## The opt-in probe grant (`deploy-probe/`)

Every deployment above is read-only. One command, `lookout net
probe-from --pod=<namespace>/<name>`, needs a write: it runs `net probe`
from inside the named pod by adding an ephemeral container to it, so it
can see faults only that pod sees (a one-way partition from it, a DNS
failure only it gets). The container stays in the pod's spec until the
pod is replaced; the command refuses once a pod holds ten of them.

`deploy-probe/` is `deploy/` plus that grant. Use it instead of
`deploy/`:

```sh
kubectl apply -k "github.com/go-steer/k8s-lookout/deploy-probe?ref=vX.Y.Z"
```

With the chart, set `rbac.probeFrom=true`. Either way you get:

- ClusterRole `lookout-watch-probe-from`: `patch` on
  `pods/ephemeralcontainers`, plus `get`/`list` on the two admission
  policy kinds, bound to the `lookout-watch` ServiceAccount.
- A ValidatingAdmissionPolicy (Kubernetes 1.30+) that lets any
  ServiceAccount named `lookout-watch` add only the lookout probe
  container: the `ghcr.io/go-steer/lookout` image pinned by digest,
  `/lookout net probe` with probe flags only, no volume mounts, no
  `targetContainerName`, the three `LOOKOUT_PROBE_*` audit variables and
  nothing else, and a restricted security context. It matches the
  ServiceAccount by name, not namespace, so wrapping the overlay in a
  kustomization with another `namespace:` keeps it in force.

:::caution[Kubernetes 1.30+ only]
`kubectl apply -k` of `deploy-probe/` on an older cluster is
unsupported. kubectl creates the ClusterRole and binding, fails only on
the policy kinds the cluster does not serve, and leaves an unguarded
write grant. Delete the `lookout-watch-probe-from` ClusterRoleBinding if
that happened. Helm checks every kind first and fails cleanly.
:::

The policy matters. On its own, `patch pods/ephemeralcontainers` lets
the holder run any image in any pod with that pod's volumes, including
its Secrets and ServiceAccount token, mounted: about as strong as
`pods/exec`. Do not grant it without the policy.

`net probe-from` checks this itself. Before it changes anything it
looks for a policy labeled `k8s-lookout.go-steer.dev/guards=net-probe-from`
that fails closed, applies to adding ephemeral containers and covers
its own identity, and a binding that enforces it (`Deny`, not narrowed).
If it finds none it refuses with `probe.refused reason=PolicyMissing`,
says the grant is unguarded, and adds nothing. If you give the grant
to another identity (a person's own kubeconfig, say), copy the policy
and set its identity condition to `request.userInfo.username ==
'<that user>'`, a form the check understands. A mirror registry needs
the policy's image prefix changed to match.

What the policy cannot narrow: once enabled, a probe can reach whatever
the probed pod can reach, including endpoints a NetworkPolicy opens only
to that pod. It sends DNS lookups, TCP connects and `GET`s whose bodies
are never read, but a `GET` with side effects on such an endpoint is
possible. That reach is the point of the feature. Narrow it with
RoleBindings to the namespaces you want probeable.

To make only some namespaces probeable, delete the ClusterRoleBinding
and bind the same ClusterRole with a RoleBinding in each of them.

Without the grant, the command adds nothing and answers:

```
kind=probe.refused severity=info namespace=shop kind_of_object=Pod name=frontend-7d9 reason=Forbidden message="forbidden: patch pods/ephemeralcontainers — namespaced, not granted by the built-in view role; grant patch on pods/ephemeralcontainers (core) via a ClusterRole or Role (the deploy-probe/ overlay, or Helm rbac.probeFrom=true, grants it to lookout's ServiceAccount) — no probe ran and the pod was not changed" vantage=pod:shop/frontend-7d9
scanned=0 findings=1 elapsed=41ms
```

Each probe container is its own audit record: the `lookout-probe-<id>`
name, the image digest, the targets in its arguments, and
`LOOKOUT_PROBE_REQUESTED_BY` (the caller's username),
`LOOKOUT_PROBE_REQUESTED_AT` and `LOOKOUT_PROBE_CLIENT` in its
environment. The API server's audit log records the patch with the
caller's identity. Design and security review:
`docs/in-pod-probe-design.md`.

## Sources

`--sources` takes a comma-separated list, and nothing requires one
sentinel to run all of them:

```
# A sentinel that only tracks topology drift.
lookout watch --sources=topology-drift

# A sentinel that only watches Events.
lookout watch --sources=k8s-events
```

An explicit list also changes failure semantics in a way that is
useful here: under `--sources=auto` a source whose grants are missing
is skipped with a log line, but a **named** source's missing required
grant is fatal. If you deployed a sentinel *for* drift, you want it to
refuse to start rather than to run as an expensive no-op.

### Which splits are free, and which cost a cache

Sources do not each own their informers — they share one factory, so
two sources reading Pods cost one pod cache between them. Splitting
them into separate deployments **un**-shares that. The question for any
proposed split is therefore only: do the two halves read the same
objects?

| Source | Objects it watches |
| --- | --- |
| `k8s-events` | Events |
| `ingress` | Events |
| `capacity` | Events, Pods, Nodes |
| `object-state` | Pods, Nodes, Deployments, EndpointSlices, PDBs |
| `rollout` | Deployments, ReplicaSets, StatefulSets, Pods |
| `degradation` | Pods, EndpointSlices |
| `topology-drift` | Pods, Nodes, ReplicaSets |
| `workload` | Jobs, CronJobs |
| `autoscaling` | HorizontalPodAutoscalers |
| `gateway` | Gateway API objects (its own factory) |
| `expiry` | ACME Challenges and Orders when cert-manager is installed (its own factory); everything else polled |
| `saturation` | none — polled |
| `quota`, `notifications`, `token-burn` | none — provider APIs |

So:

- **Free to separate:** `workload`, `autoscaling`, `gateway`,
  `expiry`, `saturation`, `quota`, `notifications`, `token-burn`. None
  of them shares an informer with anything else, so moving one into
  its own deployment costs only the process.
- **Expensive to separate:** anything in the Pods/Nodes/Events core —
  `object-state`, `rollout`, `degradation`, `topology-drift`,
  `capacity`, `k8s-events`, `ingress`. Pull `topology-drift` into its
  own sentinel and you now run two pod caches where you ran one, and
  the pod cache is the single largest thing the process holds. Do it
  because you want a different blast radius or a different credential,
  not to save memory — it will not.

`quota` and `notifications` are already deployed this way in a fleet:
they describe a *project*, not a cluster, so a multi-cluster sentinel
runs them once per project rather than once per cluster, and drops
them from the per-cluster source list automatically.

### What a split costs you

Signals are correlated inside one process. Split sources across
processes and you lose:

- **Cross-source follow-ups.** The dispatcher notices when one
  source's signal follows another's on the same object and counts it on
  `lookout_cross_source_followups_total`. Two processes never see each
  other's signals.
- **Storm blast radius.** `--storm` groups signals by common ancestor
  in the topology graph. A node failure that produces `object-state`
  and `rollout` signals in two different processes is two unrelated
  pages, not one incident.
- **Recovery clearance across sources.** The §7.4 tracker clears an
  incident when an observer says the condition is gone; observers come
  from the sources running in the same process.

Deduplication and the occurrence store are per-process too, so each
half needs its own `--store` path and its own sink configuration.

None of that argues against splitting — it argues for splitting along
a line where the two halves would not have correlated anyway. A
drift-only or a quota-only sentinel is a clean cut. Splitting
`object-state` from `rollout` is not.
