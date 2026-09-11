# Multi-cluster from one sentinel — design note

One sentinel per cluster is a founding tenet (`pkg/sources/sources.go`:
"one resident process per cluster, never N sidecars") and it stays the
**documented default and recommendation**. This note settles how a
single sentinel process can *optionally* watch several clusters, and —
the part that makes it worth doing — how a GKE deployment does that
**without distributing a kubeconfig per cluster**, using Application
Default Credentials over each cluster's control-plane DNS endpoint.

Nothing here changes the default (untagged) build or the
one-per-cluster deployment. Multi-cluster is additive, opt-in, and —
for the kubeconfig-free path — lives entirely behind the `gke` build
tag alongside the rest of `pkg/cloud/gke`.

## Why the schema is already ready

The signal wire model already carries cluster identity. `Signal.Cluster`
/ `Project` / `Zone` (`pkg/engine/signal.go`) are documented as "stamped
by the pipeline from sentinel configuration, not by sources," sources
leave them blank, and the dispatcher stamps them
(`internal/watch/dispatch.go`). Every inject payload already has the
field. The fingerprint is already described as a cross-cluster
incident-class hash. So multi-cluster is not a schema change — it is a
change to *how many values get stamped* and *by which run of the
pipeline*.

## The one coupling that matters: instantiation, not identity

Everything from client → sources → informers → dispatcher → sink is
built exactly once, in one composition root (`internal/watch/wiring.go`,
`realMain` / `buildSources`), around a single `kubernetes.Interface`
derived from a single `kube.Options`. There is no cluster state smeared
across globals. That is the good news: multi-cluster is
*parameterizing an already-single-cluster-clean root and running it N
times*, not untangling shared state.

### Decision: a per-cluster "runner"

Factor the per-cluster half of `realMain` into a **runner**: the unit
that owns one cluster's `{clientset, dynamic client, metrics client,
source registry, shared informer factory / graph feed, dispatcher
stamped with that cluster's identity}`. The process runs one runner per
target cluster and shares only the **process-level singletons**: the
output sink, the HTTP/metrics server, signal handling, and the root
context.

```
process
├─ shared: sink, HTTP+metrics server, signal ctx
├─ runner(cluster A) ── clients ── sources ── informers ── dispatcher(A) ─┐
├─ runner(cluster B) ── clients ── sources ── informers ── dispatcher(B) ─┼─→ sink
└─ runner(cluster C) ── clients ── sources ── informers ── dispatcher(C) ─┘
```

Single-cluster is then just N=1 — the same code path, so the default
deployment carries none of the multiplexing risk in practice and all of
it in review.

### Decision: fate isolation per runner

`RunAll` currently makes the first source error fatal to the whole
process (`pkg/sources/sources.go`; one root ctx in `wiring.go`). That
fate must be scoped to a runner: cluster A's API server going away, or
its RBAC being revoked, must not tear down cluster B. Each runner gets a
child context and a supervision boundary; a runner that dies is logged,
counted, and (open question below) either retried with backoff or left
down with an explicit marker — never silent, never process-fatal.

### Decision: cluster label on metrics

Prometheus metrics carry no cluster dimension today
(`internal/watch/metrics.go`), so N runners in one process would collide
label sets. Multi-cluster mode adds a `cluster` label to the watch-path
metrics. This is the **only** place cluster identity is not already
plumbed.

## The GKE path: no kubeconfig, ADC over the DNS endpoint

For GKE we can build each cluster's `rest.Config` entirely in code —
no kubeconfig file, no `gke-gcloud-auth-plugin` exec, no per-cluster CA
cert. The operator supplies a project (or an explicit endpoint list) and
the sentinel uses its own ADC identity for every cluster.

```go
ts, _ := google.DefaultTokenSource(ctx, cloudPlatformScope)   // one identity, all clusters
cfg := &rest.Config{Host: "https://" + dnsEndpoint}           // e.g. uid.us-central1.gke.goog
cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
    return &oauth2.Transport{Source: ts, Base: rt}            // auto-refreshing bearer token
})
```

Three properties of the **control-plane DNS endpoint**
(`Cluster.controlPlaneEndpointsConfig.dnsEndpointConfig.endpoint`) make
this clean, and they are why we require it:

1. **Public TLS cert.** The `*.gke.goog` endpoint is fronted by a
   publicly-trusted certificate, so there is no per-cluster CA cert to
   fetch, carry, or pin (`CAData`). The IP endpoint would force exactly
   that per cluster.
2. **One identity for authentication.** ADC/OAuth is the same credential
   `pkg/cloud/gke` already uses for enrichment. GKE control planes accept
   Google OAuth access tokens with `cloud-platform` scope — this is what
   the auth plugin does under the hood; we just do it in-process.
3. **IAM-gated reachability.** The DNS endpoint authorizes by IAM rather
   than the authorized-networks IP allowlist, and with external traffic
   enabled it is reachable from wherever the sentinel runs — no
   VPC-peering to reach N control planes.

### We already ship the pieces

- ADC is already how `pkg/cloud/gke` authenticates (e.g.
  `iam.NewService(ctx)` / `container.NewService(ctx)` with no explicit
  credentials — `wi.go`, `ipspace.go`).
- We already talk to the Container API and hold `*container.Cluster`
  (`ipspace.go`).
- `golang.org/x/oauth2`, `google.golang.org/api`, and the metadata
  server are all already in the dependency graph.

### Discover, don't hand-maintain

Because we already have the Container API client, the operator can hand
us a **project (or list of projects/locations)** and we
`ListClusters`, reading each cluster's DNS endpoint. Support both, with
the explicit endpoint list as the dead-simple floor:

- `--clusters-from=project` — discover every cluster in the configured
  project(s)/location(s) via the Container API.
- `--clusters=<endpoint,endpoint,…>` — an explicit endpoint list, no
  discovery call.

### The provider seam

Endpoint resolution and REST-config minting live behind the `gke` build
tag, exposed through an **optional** provider surface — mirroring the
existing `cloud.Identity` pattern (a surface the sentinel type-asserts,
*not* a capability in the `Metrics()`/`Quota()` matrix, because this is
bootstrap, not a per-signal facet):

```go
// pkg/cloud (untagged boundary)
type ClusterRef struct { Name, Project, Location, Endpoint string }

type Fleet interface {
    DiscoverClusters(ctx context.Context) ([]ClusterRef, error)
    RESTConfig(ctx context.Context, ref ClusterRef) (*rest.Config, error)
}
```

`pkg/kube` stays cloud-free: it grows a `BuildClientFromConfig` that
takes a ready `*rest.Config` (from either the Fleet provider or the
existing kubeconfig/in-cluster resolution) and runs it through the same
client construction. `kube.Options` gains a third construction mode
conceptually — `in-cluster` / `kubeconfig` / **provider-supplied** —
but the GKE specifics never enter the default build.

### The kubeconfig fleet: multi-cluster without a cloud

A `cloud.Fleet` provider is one way to answer "which clusters, and with
what credentials". It should not be the only one, and it was: an
untagged build asked to watch a fleet could only refuse with *build with
`-tags gke`* (issue #388). So `resolveRunners` sits behind a small
in-package interface — `Describe` plus the Fleet pair — with a second
implementation that reads a **kubeconfig**:

```
--clusters-from=kubeconfig            # $KUBECONFIG, else ~/.kube/config
--clusters-from=kubeconfig:/etc/lookout/fleet.yaml
```

One runner per **context**, each reached with that context's own
credentials. Contexts, not cluster entries, because context names are
unique within a merged kubeconfig by construction — which is exactly
what per-cluster metric labels, dedup snapshot paths and readiness
entries need, and several contexts may share one server.

`kubeconfig` is therefore a reserved `--clusters-from` value and cannot
name a cloud project; it is checked before the `project/location` split,
since a path contains slashes.

The refs a kubeconfig yields carry **no project, location or region** —
a kubeconfig does not know them, and inventing them would put a wrong
failure domain into the §8 fingerprint. Each runner then resolves
identity the way a single-cluster sentinel does (explicit flag >
provider metadata > empty). The enumeration also never falls back to the
sentinel's own in-cluster service account: an operator who listed
contexts named their clusters, and silently adding the cluster the
process happens to run in would watch something nobody asked for.

This makes multi-cluster work against EKS, AKS, on-prem, kind, and a GKE
cluster whose credentials the operator already holds — on the default
build, with no cloud API call.

## The honest caveat: authN ≠ authZ

OAuth solves *authentication* — one Google identity to every API server
— for free. It does **not** solve *authorization*. Each target cluster
still needs an RBAC binding for the sentinel's Google identity:

```yaml
kind: ClusterRoleBinding
subjects:
- kind: User
  name: <sentinel-gsa>@<project>.iam.gserviceaccount.com   # the OAuth identity
```

Plus IAM on each project: `container.clusters.get` /
`container.clusters.list` for discovery, and the DNS-endpoint connect
permission. This is the residual per-cluster setup — but it is a
templatable RBAC manifest, not credential distribution, and materially
lighter than shipping a kubeconfig with N contexts and N credential
sets. The existing RBAC probe (`sources.Probe`) runs per runner and
already reports missing access loudly.

## Why one-per-cluster stays the default

The cost of multi-cluster is not the plumbing — it is the operational
posture, and it is real:

- **Footprint.** Each runner has its own shared informer factory and
  watches pods/nodes/etc. through it. That factory dedups *within* a
  cluster — one informer per object type, 13 LIST+WATCH streams — but
  never across clusters, because two clusters' pods are different
  objects on different API servers. Ten clusters in one pod is ~10× the
  watch/cache memory — precisely what one-per-cluster avoids.
- **Blast radius.** One process becomes one failure domain for many
  clusters; restarts, OOMs, and rollout risk all get worse.
- **Credentials/reachability.** Long-lived reach into remote control
  planes is a burden the local-only model sidesteps.

So multi-cluster is aimed at the "many small/dev clusters, one pane"
case — not large production fleets, which keep the per-cluster sentinel.

## Out of scope

- **Cross-cluster correlation.** Runners are independent; the fingerprint
  is already cross-cluster, but joining incidents *across* clusters into
  one thread is receiver-side, not sentinel-side (same posture as the
  agent-sink note's multi-sink fanout).
- **Non-GKE kubeconfig-*free* auth.** EKS/AKS analogs are future
  provider work — a second `cloud.Fleet` implementation. Watching those
  fleets is not out of scope: `--clusters-from=kubeconfig` does it
  today, with credentials the operator supplies.
- **Per-runner sink selection.** One sink per process; all runners fan
  into it. A deployment wanting per-cluster routing does it receiver-side.
- **Dynamic fleet membership.** Discovery runs at startup; clusters
  appearing/disappearing mid-run is a later iteration (re-discovery loop
  + runner add/remove), not v1.

## Resolved during implementation

1. **Runner restart policy** — bounded backoff. A runner that exits
   while the process is up is logged, counted (`lookout_runner_up`,
   `lookout_runner_restarts_total`), and restarted after a backoff;
   ctx cancellation (shutdown) is not a restart. At N=1 the single
   runner's error is returned so the kubelet still owns the restart
   (one-per-cluster behaviour is byte-identical).

   *Amended for issue #383.* The backoff was fixed at 10s and applied
   to every exit alike. Two changes: it now doubles to a 5m ceiling
   (resetting once a runner has stayed up 2m), and exits are
   classified. A **terminal** exit — today, only a settled
   authorization refusal, which the sentinel knows because it asked
   the authorizer and got a decision — ends supervision for that
   cluster instead of retrying it forever: the runner stops, the
   cluster is marked degraded on `/readyz?verbose`, and
   `lookout_runner_terminal{cluster,reason}` goes to 1. A degraded
   cluster is dropped from the readiness expectation rather than held
   against it (a process watching 24 of 26 clusters is fit to serve
   those 24); if *every* cluster goes terminal the process exits
   non-zero, because there is nothing left to be ready for.
2. **Which loops are per-runner vs. process-global** — process-global:
   the sink, the metrics registry, the signal context, and the
   `--metrics-addr` HTTP server. Per-runner: clients, sources, informers,
   dispatcher, store, recovery, and the graph feed (each wraps its
   cluster's factory). Each runner gets a child context so a feed failure
   cancels only that runner.
3. **Config shape** — flags for now. `--clusters` takes `name=endpoint`
   pairs; `--clusters-from` discovers from a `project` or
   `project/location`. The scalar `--cluster-name`/`--project`/`--zone`
   become per-runner (discovery derives them). A config file is deferred
   until the cluster list outgrows a flag.

## Per-cluster state paths

`--dedup-persist` is per-cluster state on a single path. In multi-cluster
mode `resolveRunners` treats the flag value as a **stem** and gives each
runner its own file, suffixed with that cluster's project, location and
name (issue #386):

```
--dedup-persist=/data/dedup.json
  → /data/dedup-my-proj-us-central1-a-prod-us.json
  → /data/dedup-my-proj-europe-west1-b-prod-eu.json
```

The suffix is the full triple rather than the bare cluster name because
two clusters in different locations may share a name, and two clusters
must never share a snapshot. Anything outside `[A-Za-z0-9_-]` in a
component collapses to a dash, so an operator-supplied `--clusters` name
can only ever produce one filename next to the stem. The single-cluster
default never calls this, so an existing deployment's snapshot does not
move on upgrade.

## An unresolvable cluster is skipped, not fatal

Fate isolation used to start only once the runners were running:
`resolveRunners` resolved every cluster's credentials up front and
returned the first error, so **one** deleted cluster still in a
discovery listing — or one stale kubeconfig context — left the whole
fleet unwatched (issue #388). That is the opposite of the §11 posture
the supervisor already takes at run time.

A cluster whose credentials cannot be resolved is now skipped:

- it gets no runner, and the remaining clusters start normally;
- `lookout_cluster_resolve_errors_total{cluster}` increments once;
- two log lines say it — the per-cluster error naming the cluster, then
  a fleet summary (`watching 2 of 3 cluster(s); skipped …`) spelling out
  that the sentinel reports **nothing** about the skipped clusters, so
  their silence must not be read as healthy.

The metric is process-level, not part of the per-runner bundle: every
per-runner series carries a *const* `cluster` label supplied by
`prometheus.WrapRegistererWith`, and a cluster that never got a runner
has no such wrapper. It is counted at startup only, so it moves on
process restart and on nothing else — which makes any non-zero value a
standing coverage gap, worth an alert.

If **every** cluster fails to resolve, the process exits non-zero. That
is not one cluster's bad day: it is credentials, a build tag, or a
kubeconfig naming nothing reachable, and supervising an empty fleet
would report ready while watching nothing. Same shape as the #383 rule
that a fleet gone entirely terminal exits rather than serving an empty
readiness set.

The #383 readiness interaction needs no extra code: `realMain` builds
its expected-cluster set from the runners `resolveRunners` returns, so a
skipped cluster is never expected and cannot hold `/readyz` down.

## Still deferred

- **Per-cluster occurrence stores.** `--store` is a single SQLite path
  that would collide across runners, so it is rejected in multi-cluster
  mode rather than silently shared. Unlike the dedup snapshot it also
  carries the prune loop, the size bound and the distiller's input
  window, so per-cluster stores are a larger change — run one sentinel
  per cluster when you need one.
