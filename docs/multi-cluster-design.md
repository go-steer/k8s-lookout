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

- **Footprint.** Each runner watches pods/nodes/etc. with its own
  informer factories. Storm-mode's shared factory dedups *within* a
  cluster, not across. Ten clusters in one pod is ~10× the watch/cache
  memory — precisely what one-per-cluster avoids.
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
- **Non-GKE kubeconfig-free auth.** EKS/AKS analogs are future provider
  work; the kubeconfig path already serves them.
- **Per-runner sink selection.** One sink per process; all runners fan
  into it. A deployment wanting per-cluster routing does it receiver-side.
- **Dynamic fleet membership.** Discovery runs at startup; clusters
  appearing/disappearing mid-run is a later iteration (re-discovery loop
  + runner add/remove), not v1.

## Open questions

1. **Runner restart policy** — backoff-retry a dead runner, or leave it
   down with a marker until the next process start? (Leaning: bounded
   backoff, with the down state surfaced in metrics + a startup-style
   marker.)
2. **Which loops are per-runner vs. process-global** — dedup snapshot,
   distill, watchboard are per-cluster; the HTTP/metrics server and sink
   are global. Graph feed is per-runner (it wraps a cluster's factory).
   Needs an explicit inventory before the refactor lands.
3. **Config shape** — flag surface vs. a config file once the cluster
   list grows. The scalar `--cluster-name`/`--project`/`--zone`
   (`flags.go`) become per-runner; discovery mode derives them.
