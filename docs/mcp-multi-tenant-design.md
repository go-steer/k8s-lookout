# Shared MCP endpoint: many clusters, many callers — decided

Status: decided 2026-10-14 (proposed 2026-10-13, PR #695). DESIGN.md §3
and §4.3 carry the amendments of 2026-10-14 that point here. Phase 1
(#694) shipped; the rest is not built yet. Tracked in #697.

## Why

`lookout mcp` today is single-tenant. One process reads one cluster as
one identity and advertises one tool surface. Sessions are cheap,
because no tool call keeps state: each call runs the CLI command
in-process with fresh buffers and a fresh client. But everything a
caller could vary is fixed for the whole process:

- **Identity.** Every read runs as lookout's own credentials: the
  pinned `--kubeconfig`/`--context`, or the pod's service account. The
  caller's identity never reaches the API server.
- **Cluster.** `--kubeconfig`, `--context`, `--store`, `--report` and
  `--exemptions` are pinned by the operator (`pinnedArgs`,
  `internal/mcpserver/mcpserver.go`) and left out of the tool schemas,
  so a client cannot redirect them.
- **Authorization.** On HTTP, one shared bearer token admits a caller
  to the full advertised surface (`internal/mcpserver/auth.go`). The
  access log records the remote address, not who called.
- **Fairness.** One semaphore of 8 concurrent calls is shared by every
  session. Idle HTTP sessions never expire (`SessionTimeout` is unset).

So the only way to serve two clusters or two teams is two processes.
This note designs two modes for one endpoint:

1. **Many clusters, one identity, many users.** The endpoint reads
   several clusters as lookout's own identity. Each tool call names
   one cluster.
2. **Many callers, each as themselves.** Each call reads as the
   calling user's own Google identity, so Kubernetes RBAC and Cloud
   IAM decide what that user can see.

Both modes rest on one seam: a per-call `rest.Config` carried by
`kube.Selection` (Part 1, "The seam"). Mode 1 uses it to pick the
cluster; mode 2 uses it to pick the credential. Mode 2 is built first,
on a single cluster, because its main consumer (Gemini Enterprise) is
the use case that matters now; see Phases.

## Part 1 — one endpoint, many clusters

### Shape

```
lookout mcp --listen=:8080 --clusters-from=my-project [--store=/var/lib/lookout/lookout.db] ...
lookout mcp --listen=:8080 --clusters=prod-us=abc.us-central1.gke.goog,dev=def.us-east1.gke.goog
lookout mcp --clusters-from=kubeconfig:/etc/lookout/fleet.yaml
```

`--clusters` and `--clusters-from` mean exactly what they mean on
`lookout watch` (`internal/watch/flags.go`, `docs/multi-cluster-design.md`):
GKE discovery through the `cloud.Fleet` seam, kubeconfig-free over each
cluster's DNS endpoint with one ADC identity, or one cluster per
kubeconfig context. The resolver (`resolveFleet`, `kubeconfigFleet` in
`internal/watch/fleet.go`) moves to a package both commands import, so
the two cannot drift.

Without either flag nothing changes: a single-cluster endpoint keeps its
current flags, schemas and behavior, byte for byte.

### Decision: a `cluster` argument per call, not several clusters per call

In multi-cluster mode, every tool gains one argument, `cluster`, naming a
configured cluster. It is required: with more than one cluster configured,
there is no default for a call to fall back to without saying so.

A tool call reads exactly one cluster. To cover three clusters, the client
makes three calls, and MCP clients already issue parallel tool calls. A
multi-cluster argument was rejected because:

- **The §4.2 output contract is one scan.** One payload, one summary line,
  one `--timeout` budget, one exit code. A call over three clusters would
  need new partial-failure semantics for "two answered, one timed out".
- **Failures stay separate.** A slow or unreachable API server stalls only
  its own call.
- **Merging across clusters is fleet rollup.** §3 places that in the fleet
  aggregation layer. The agent combines per-cluster answers; lookout never
  joins them.

### Decision: a validated string, not an enum

The argument is a plain string, checked on the server against the
configured set. The valid names come from a new tool, `k8s_list_clusters`
(name, project, location, reachability; with a substring filter), which is
advertised only in multi-cluster mode.

An enum was rejected on cost. Every advertised schema is paid for on every
model call: today's full surface is 37 tools and about 170 KB of schema
(`lookout mcp --list-tools`). An enum repeats the whole cluster list in
each of those 37 schemas. At 500 clusters with 15-character names, that is
roughly another 280 KB on every call, so the schema would grow with the
fleet instead of staying fixed.

An unknown name is a usage error (exit 2, JSON-RPC invalid params) that
names `k8s_list_clusters`. It never falls back to another cluster.

### The seam: `kube.Selection` carries the cluster

The per-call cluster already has a carrier. `emit.Run` puts a
`kube.Selection` in the context for each invocation
(`pkg/kube/selection.go`), and every client-construction site in
`pkg/checks` (typed, dynamic and metrics clients alike) resolves through
`kube.OptionsFrom(ctx)` → `kube.BuildConfig`.
`Selection` gains one field: a resolved `*rest.Config` supplier for the
selected cluster. `BuildConfig` uses it first, copying it with
`rest.CopyConfig` before setting QPS and burst. No check changes, and two
concurrent calls to two clusters cannot interfere, because the selection is
scoped to the call's context, not to the process.

The MCP handler resolves the `cluster` argument to a `cloud.ClusterRef`
and puts the supplier in the selection. `rest.Config`s are cached per
cluster; an ADC token source refreshes itself.

### Cloud checks need the same selection — and are wrong today

Seven read-path call sites build their provider with an empty config:
`cloud.New(ctx, cloud.Config{})`, in `state/wi.go`, `health`, `perf`,
`stab`, `audit`, `cloudcheck` and `top`. The GKE provider then resolves
project, location and cluster from env vars, then the metadata server, and
only then from the kubeconfig selection (`pkg/cloud/gke/gke.go`, `New`).

So in a pod, the metadata server's answer wins. A multi-cluster endpoint
would serve every cloud-backed answer (`cloud orphans`, `cloud ipspace`,
`audit cluster`, `stab drift --identity`, the health categories) for
**lookout's own cluster, labelled as the one asked for**. That is the worst
failure available: a confident answer about the wrong cluster.

This also happened with a single cluster: an in-cluster
`lookout mcp --context=<other>` read the other cluster's Kubernetes objects
and its own cluster's cloud state. Fixed in #694 (PR #696): when a
`kube.Selection` is present, `gke.New` no longer consults the metadata
server, because it describes where lookout runs, not what it was asked
about. A selection that names no GKE cluster leaves the identity empty, so
cluster-scoped capabilities degrade explicitly. The fix sits in `gke.New`,
which already reads the selection from the context, so the seven call sites
did not change. Part 1 extends the same rule: the per-call cluster's
`ClusterRef` supplies project, location and cluster to the provider.

### Tools that answer from where lookout runs

- **`net probe`** sends its packets from the lookout pod. For a remote
  cluster, that measures the wrong network, and an agent will read a
  healthy result as evidence against a fault (the reasoning in
  `docs/in-pod-probe-design.md`). In multi-cluster mode it is refused for
  any cluster other than the one lookout runs in, with a reason naming
  `net probe-from`.
- **`perf probe`** measures API latency along lookout's path, which for a
  remote cluster includes the WAN hop to its DNS endpoint. It still works,
  but its findings say which path they measured.

A `Command.Local` flag marks these, and the handler checks it. It is a
command attribute, so a new location-bound check opts in where it is
registered.

### Store and report

`--store` and `--report` take the same stem rule as `watch`: the value is a
stem, and each cluster reads its own suffixed file
(`lookout-prod-us.db`, `docs/multi-cluster-design.md`, "Per-cluster state
paths"). A multi-cluster endpoint next to a multi-cluster watch therefore
reads the files that watch writes.

If no store exists for a cluster (the usual case when each cluster runs its
own sentinel), the store-backed parts degrade the way an absent store
already does: `health` drops the sentinel-findings merge, `triage status`
refuses, and `--at` history is unavailable, each with the explicit §2
reason. They are never silently empty.

### Concurrency, sessions, replicas

- **Per-cluster limit inside the global one.** A wedged cluster cannot hold
  all of the global slots. The default is the global limit divided by the
  number of clusters, with a floor of 2; `--max-concurrency-per-cluster`
  overrides it.
- **Stateless HTTP.** lookout keeps no per-session state, so the endpoint
  serves streamable HTTP with the SDK's `Stateless: true`. Any replica can
  answer any request, so the endpoint scales as a Deployment behind a
  Service with no sticky sessions, and there are no idle sessions to time
  out. lookout sends no server-to-client notifications, so it loses nothing.
  stdio is unaffected.
- **Discovery while running.** `--clusters-from` re-discovers on an interval
  (`--rediscover`, default 10m), and on an unknown name at most once a
  minute, so clusters can come and go without a restart. `watch` only
  discovers at startup; for watch, adding a cluster means starting
  informers, but for the read path it is a map update.

### How far it scales

The objections to multi-cluster `watch` (`docs/multi-cluster-design.md`,
"Why one-per-cluster stays the default") mostly do not apply here:

- **API server load** follows tool calls, not clusters. Nothing is watched,
  and an idle cluster costs nothing.
- **Memory** is a cached `rest.Config` per cluster, with no informers.
- **Failure domain** is spread across stateless replicas, and per-call
  clusters keep one cluster's outage inside its own calls.

What remains is credential reach: one identity that can read N clusters
means a compromised endpoint can read all N. That depends on how sensitive
the clusters are, not how many there are, so this design sets no cluster
cap. The mitigations are a narrow role in each cluster (the shipped view
role degrades cleanly, with permission hints) and Part 2.

### Many projects

Decided 2026-10-16. One endpoint can serve clusters across many projects;
nothing below changes a single-project endpoint.

**Scope.** `--clusters-from` takes a comma-separated list of scopes,
which may be mixed:

| Scope | Discovered through |
|---|---|
| `my-project`, `my-project/us-central1` | Container API `ListClusters` (today's form) |
| `fleet:<host-project>` | GKE Hub memberships of that fleet, which can span projects; each GKE membership resolves to its cluster, and non-GKE memberships (attached clusters) are skipped and counted |
| `folders/<id>`, `organizations/<id>` | Cloud Asset Inventory `searchAllResources` for `container.googleapis.com/Cluster` under the folder or organization: one call for hundreds of projects |
| `kubeconfig[:<path>]` | unchanged, and not combinable with the cloud scopes |

When Cloud Asset Inventory cannot answer (the API is not enabled in the
scope, or the discovering identity may not search it), a folder or
organization scope falls back to Resource Manager: list the projects under
it, then `ListClusters` in each, with bounded concurrency. Slower, needs
nothing extra, and never silent: `k8s_list_clusters` reports
`discovery=asset` or `discovery=resourcemanager` (and why), and the
endpoint logs the fallback once per scope.

**Who discovers.** In Google-token mode lookout has no identity of its
own (Part 2), so discovery runs **as the caller**. `k8s_list_clusters`
lists, with the caller's token, the clusters in scope that this person can
see, cached per caller for five minutes. Two consequences, both wanted:
lookout still needs no IAM at all, and nobody learns that a cluster they
cannot access exists. A `cluster` argument is checked against the
caller's own set; an unknown name triggers at most one rediscovery per
caller per minute, then a usage error naming `k8s_list_clusters`. Without
Google-token mode, discovery runs as lookout's identity, as it does for
one project today, and the configured set is the same for every caller.
`watch` gets the same scope forms through the shared resolver, always as
its own identity.

**Names.** A cluster's name is unique only within a project and location.
A call names a cluster by its bare name when that is unique in the
caller's set, as `name@project` when it is not, and as
`name@project/location` when even that collides. The canonical
`project/location/name` is always accepted. `k8s_list_clusters` prints
the name to use, so a model copies it rather than composing it. The set a
name is unique in is the caller's, so two callers may see different short
names for the same cluster; each call is checked against its own caller's
set.

**Project-scoped answers.** `cloud quota`, `cloud orphans` and
`cloud ipspace` answer for the selected cluster's project
(`ClusterRef.Project`, as in single-cluster mode since #696). A project
with no cluster in scope is not addressable: the unit of a call stays one
cluster.

**What can still fail per cluster.** A VPC Service Controls perimeter can
refuse a cross-project call that IAM allows, and a private cluster's DNS
endpoint can be disabled. Either is that cluster's `read.unavailable`,
with the API's reason, never a silent omission and never another
cluster's answer.

## Part 2 — one endpoint, many callers

### Decision: act as the user, with their own Google token

Each call reads as the calling user's own Google identity. lookout needs
no `impersonate` grant, and lookout's own identity reads nothing on a
user's behalf.

**User identity is Google-only (decided 2026-10-13).** A person reads as
themselves only with a Google access token, which matches the reach of the
per-call credential: GKE clusters over their DNS endpoint, and Google Cloud
APIs. Callers authenticated another way (a generic OIDC token that purser
verifies, mTLS, the bearer table) are workloads and read as lookout or a
configured service account. Reading as a non-Google user would need
Workforce Identity Federation (exchanging the IdP token at Google STS). That
is out of scope; kode-gopher left it for later too.

There are two ways the user's token reaches lookout. Both end in the same
place: a `purser.Caller` plus a token source for the user, which the
per-call credential below consumes. Both live in a new go-steer module,
**steward**.

### Path A, first: Gemini Enterprise sends the user's token

The main use case is a Gemini Enterprise app calling lookout as a **Custom
MCP Server data store**. supercargo ships this pattern today
(`go-steer/supercargo`, `internal/googleauth`, #110;
`docs/guides/runbook-cloudrun.md` §6):

- **Gemini Enterprise is the OAuth client, and Google is the authorization
  server.** The admin configures the data store with lookout's own OAuth
  client (ID and secret), Google's authorize and token URLs, PKCE, and
  `access_type=offline&prompt=consent`. Gemini Enterprise runs the code
  flow for each user, holds their refresh token, and calls `/mcp` with the
  user's Google access token as a bearer token. Custody is Gemini
  Enterprise's problem; lookout stores nothing.
- **lookout verifies the token** the way supercargo does. Google's
  tokeninfo must say the token was issued to lookout's client (`aud`/`azp`),
  is unexpired, and carries the email scope with a verified email. Its
  userinfo gives the Workspace domain (`hd`), because a consumer Google
  account can carry a company address, so a domain allow-list entry needs
  `hd` to match, not the email suffix. Results are cached by token hash for
  at most 10 minutes, a refusal for 30 s, at most 8 lookups run at once, and
  a Google 429 or 5xx is a failure, never cached as a refusal. JWTs are
  refused before Google is asked. RFC 9728 Protected Resource Metadata is
  served.
- **The scopes add `cloud-platform`.** supercargo asks for `openid email`
  because it only needs to know who is calling. lookout configures the data
  store with `openid email https://www.googleapis.com/auth/cloud-platform`,
  and tokeninfo must show all three. The token that identifies the user is
  then the token lookout reads with. The client is "Internal" in the
  deploying org; in orgs that restrict Google Cloud scopes, a Workspace admin
  must mark it Trusted (kode-gopher's phase B finding).
- **Why this is not token passthrough.** The MCP authorization spec forbids
  a server from accepting tokens not issued for it and forwarding them. The
  `azp` check accepts only tokens minted for lookout's own client, with the
  scopes the user granted lookout at consent. A token Gemini Enterprise holds
  for any other app is refused, so lookout cannot be used to replay someone
  else's grant. The tension that remains is that Google access tokens carry
  no resource audience. Path B removes it for the clients that can use it.
- **Reachability.** Gemini Enterprise calls from Google's network, so the
  endpoint needs a public HTTPS address with a publicly trusted certificate
  (a Gateway with a Google-managed certificate in front of the lookout
  Service). `BindPolicy` already requires authentication and an access log
  for a routable bind; Google-token verification counts as the
  authentication. The data store's description is what the assistant uses
  to pick the connector, so the shipped manifests should carry one, and a
  `--profile` should keep the advertised surface small.

supercargo is a go-steer project, so its `internal/googleauth` moves into
steward and supercargo adopts it: one implementation, fixed in one place.

### Path B, later: lookout as its own authorization server

Generic MCP clients (Claude Code, gemini-cli, IDEs) expect to discover an
authorization server and register with it by DCR or CIMD. Google's server
supports neither, so for these clients lookout runs its own OAuth 2.1
authorization server in front of Google sign-in, the pattern kode-gopher
shipped (`gke-demos/kode-gopher`, `docs/design-in-cluster.md` §3):

- PKCE (S256 only), resource indicators (RFC 8707), registration by CIMD,
  DCR or a pre-registered client. The client receives lookout's own tokens,
  bound to its `/mcp` audience, with the user's Google access token sealed
  inside.
- Custody of the user's Google grant is the Agent Identity credential vault,
  with a sealed refresh token as the fallback, and the hardening kode-gopher
  learned: the tokeninfo `sub` check on vault tokens, canonical scope
  spelling, distrust of the vault's `expireTime`.
- Tokens are AEAD-sealed with a key-ID-rotated key, so there is nothing to
  store and any replica verifies them.
- The allow-list (domain by `hd`, Google group) is checked at sign-in and at
  every refresh.

kode-gopher lives in another org and will not adopt steward, so this half is
taken from a recorded kode-gopher commit and fixes are ported by hand.

### steward and purser

purser decides who is calling: it turns a request into a `purser.Caller`
(mTLS/SPIFFE, OIDC, bearer table) and owns the `authz` rules. It verifies
JWTs, but a Google access token is opaque and is checked by asking Google,
and purser's design rules out running the authorization code flow (purser
`docs/DESIGN.md`, `authn/oidc`). steward does both Google-specific parts and
yields a `purser.Caller` plus the user's token source, so lookout's tools,
access log and limits see one caller type whichever way the caller arrived.

### Caller kinds

| Caller | Authenticated by | Reads as |
|---|---|---|
| A person, through Gemini Enterprise | steward path A (Google access token from Gemini Enterprise) | the user's own Google identity |
| A person, through a generic MCP client | steward path B (lookout's authorization server, Google sign-in) | the user's own Google identity |
| A workload: a core-agent daemon, a watch-opened incident session | purser mTLS/SPIFFE, or OAuth client credentials through steward path B | lookout's identity, or a per-client service account through `generateAccessToken` |
| An existing `--auth-token-file` deployment | purser's bearer table, with one caller | lookout's identity, as today |

The last row is how this ships under `docs/cli-stability-policy.md`: the
existing flags keep their meaning, and everything here is additive.

### The per-call credential

The Part 1 seam carries it. The handler builds the selection's `rest.Config`
from the cluster and the caller: `restConfig(ctx, ref, tokenSource)`, with
the token source being the user's for a person and ADC otherwise. That
function already takes its token-source constructor as a parameter
(`pkg/cloud/gke/fleet.go`; production passes `google.DefaultTokenSource`).
`pkg/checks` still only sees a `rest.Config`, and the provider boundary holds, because the GCP side stays in
`pkg/cloud/gke` behind the `gke` tag.

The same token source must reach the cloud provider. `cloud.Config` gains
it, and the GKE clients are built with it. Otherwise `cloud orphans` or
`cloud quota` would answer a user with lookout's view of the project, which
is exactly the leak this mode exists to prevent.

**Never fall back silently.** A user call to a cluster where their identity
cannot be used (a kubeconfig fleet, with no Google identity to present) is
refused with that reason. It never quietly runs as lookout's identity.

### Three layers of authorization

1. **Admission:** steward's allow-list (Workspace domain by `hd`, Google
   group) decides who is let in at all, on either path.
2. **Surface:** purser `authz` rules map caller to tools and clusters. The
   one write tool, `k8s_triage_status`, needs a rule that grants it
   explicitly, matching the `ReadOnlyHint` split already on the tool
   annotations. Path B tokens also carry `lookout:read` / `lookout:write`
   scopes, so a client can ask for less; Path A tokens carry only Google
   scopes, so on Path A the rules alone decide.
3. **Data:** Kubernetes RBAC and Cloud IAM on the user's own identity. The
   view-role degrade and permission hints already turn a 403 into an
   explicit, explained gap instead of an empty answer.

### The store is not behind RBAC

Layer 3 covers only reads that go to an API. Graph history (`--at`),
`health`'s merge of open sentinel findings, `bundle`'s triage records,
`findings diff`/`ack` reports and `triage status` read or write files, as
lookout. A user who cannot read namespace X through RBAC could still read
X's history from the store.

**Decision (2026-10-13): filter each row by the user's own access.** For a
person, every row read from the store or a report is filtered by the user's
own access to that row's namespace. Use `SelfSubjectAccessReview` with the
user's own token, which needs no extra grant, cached per call. A row with no
namespace requires cluster-wide read. Deny by default. Writing a
triage-status record requires the write rule (and on Path B the
`lookout:write` scope) and `update`-equivalent access
to the workload's namespace. Workload callers are not filtered; they read as
lookout, as today.

Rejected: refusing store-backed tools and sections to users outright. It is
simpler and safe, but `health` and `bundle` lose their main value: output
that reflects triaged reality (§9.4).

### Triage-status records: lookout writes, the caller is recorded

**Decision (2026-10-13):** lookout stays the writer of record. The write goes
through the same `TriageWriter` against the same store, and §9.4's split is
unchanged (the sentinel owns record lifecycle, agents write record content).
What changes is that the record says who asked for the write: a new
`recorded_by` column holding the `purser.Caller` identity (a user's email or
a workload's identity), filled by every write and empty for records written
before it existed.

This is the "writes gain the caller identity" step that
`docs/triage-status-write-design.md` ("Out of scope") expected when writes
became mediated. Here the mediator is the MCP endpoint instead of a
core-agent daemon. It is additive: a store migration with a default of `''`,
a new last field on the read output, and no change to the
`(fingerprint, resource_key)` key or the status rules. The CLI writer fills
it with nothing, because a local CLI has no authenticated caller and the
`--store` file permission remains its write authorization.

### Everything else per caller

- **Access log** gains the caller identity as an additive field.
- **Concurrency** gains a per-caller limit inside the per-cluster and global
  ones.
- **Secrets:** the user's token lives only in memory, in the call's
  `rest.Config` and token source. A golden test plants a sentinel token and
  asserts it never appears in a tool result, a diagnostic or the access log,
  extending the sanitizer tests as AGENTS.md requires for new output paths.

## What lookout needs from steward

Written down here so the module is designed against a real consumer:

For Path A (first, from supercargo's `internal/googleauth`):

- A Google access-token verifier: tokeninfo (`aud`/`azp` is one of the
  configured clients, unexpired, required scopes present, verified email),
  userinfo for `hd`, the bounded cache and concurrency, and Google failures
  kept distinct from refusals.
- An HTTP middleware that protects a resource with it: serves Protected
  Resource Metadata naming Google as the authorization server, answers 401
  with the right `WWW-Authenticate`, and puts a `purser.Caller` plus the
  user's `oauth2.TokenSource` in the request context. The token source is
  the presented token itself, for the life of the call.
- The allow-list (domain by `hd`, group by Cloud Identity transitive
  membership), shared with Path B.
- supercargo's browser sign-in (code flow, PKCE, nonce, ID token check)
  comes along too, since supercargo uses it; lookout has no browser UI.

For Path B (later, from kode-gopher's `internal/oauth`):

- Authorization-server endpoints to mount at an issuer URL: discovery,
  registration (CIMD, DCR, static), authorize, callback, token, with the
  client-credentials grant for workloads.
- The same middleware accepting steward's sealed tokens as well as Google
  ones, yielding the same `purser.Caller` plus token source.
- Service-defined scopes, Google scopes and upstream, and a key provider
  interface. kode-gopher's Secret Manager key becomes one implementation.
- `Custody` as an interface with vault and sealed implementations.

Either way, the MCP-specific parts (go-sdk `auth`/`oauthex`) stay in a
subpackage, so a service that is not an MCP server can verify tokens and
sign people in without importing the MCP SDK.

## Phases

Ordered 2026-10-14 with Gemini Enterprise first, because it is the use
case that matters now and it does not need more than one cluster.

1. **Fix the cloud selection bug** (#694). Shipped.
2. **Spike: a Gemini Enterprise token against GKE (#698).** In the project where
   supercargo's Gemini Enterprise app runs, register a Custom MCP Server
   data store for a throwaway endpoint with scopes
   `openid email https://www.googleapis.com/auth/cloud-platform`, and
   confirm the token it sends (a) passes tokeninfo with our client as
   `azp`, (b) authenticates to a GKE API server as the user, both over the
   DNS endpoint and over the in-cluster address a single-cluster lookout
   would use, and (c) calls the Cloud APIs the cloud-backed checks use.
   The answer to (b) decides which endpoint phase 4 builds the per-call
   `rest.Config` against.
3. **steward Path A** (go-steer/steward#1), in the new public `go-steer/steward` repo:
   supercargo's `internal/googleauth` moved in with its tests, the
   protecting middleware yielding a `purser.Caller` plus the user's token
   source, the allow-list. supercargo adopts it.
4. **Callers as people, through Gemini Enterprise, on one cluster (#699):** the
   per-call `rest.Config` seam in `kube.Selection`; a Google-token auth
   mode on the HTTP endpoint through steward; the user's token for
   Kubernetes and Cloud APIs, with no silent fallback; purser `authz`
   rules for the write tool; the store filter; `recorded_by`; the caller
   in the access log and per-caller limits; a Gateway overlay with a
   managed certificate; and a runbook for the Custom MCP Server data
   store. `--auth-token-file` keeps working.
5. **Part 1, many clusters and projects (#700):** cluster flags with
   multi-project scopes (project lists, GKE fleets, folders and
   organizations), discovery as the caller in Google-token mode, the
   `cluster` argument with collision-qualified names,
   `k8s_list_clusters`, `Command.Local`, per-cluster store stems and
   concurrency, stateless HTTP, rediscovery. It reuses phase 4's seam.
6. **Callers as workloads (#701):** purser mTLS/SPIFFE in front of the HTTP
   endpoint, for core-agent daemons and watch-opened sessions; still
   reads as lookout or a configured service account.
7. **Callers as people, through generic MCP clients (#702):** steward Path B.

## Open questions

None at the moment. Settled 2026-10-13: the store rule (filter by
`SelfSubjectAccessReview`), user identity (Google only), and triage-status
attribution (lookout writes, `recorded_by` names the caller).

## Rejected

- **A multi-cluster argument on each tool:** breaks the one-scan output
  contract and is fleet rollup by another name (Part 1).
- **A cluster enum in the schema:** schema cost grows with the fleet (Part 1).
- **Discovery as lookout's identity in Google-token mode:** would give
  lookout IAM it otherwise never needs, and would show every caller the
  names of clusters they cannot read (Part 1, Many projects).
- **Always fully qualified cluster names:** never ambiguous, but long, and
  models copy long identifiers wrong more often than short ones (Part 1,
  Many projects).
- **Kubernetes impersonation:** needs the `impersonate` verb on lookout's
  identity, the largest grant lookout would hold. steward gives the user's
  own identity without it.
- **Accepting any Google token the client holds:** the MCP authorization
  spec forbids token passthrough (confused deputy). Path A accepts only
  tokens issued to lookout's own OAuth client (`azp`), with the scopes the
  user granted lookout; Path B goes further, with lookout's own
  audience-bound tokens.
- **An inbound auth proxy in front of lookout:** a second component to
  deploy, and lookout would have to trust the proxy's word for who the user
  is. (`gke-demos/mcp-auth-proxy` is the opposite direction: it adds a pod's
  ADC token to outbound calls to Google-hosted MCP servers.)
- **Putting the authorization server in purser:** purser's design excludes
  the authorization code flow; it verifies tokens and does not issue them.

## DESIGN.md amendments (made 2026-10-14)

- **§3, fleet scope:** keeps "one process watching a thousand API servers
  is wrong" and adds that on-demand, per-cluster reads through a shared
  endpoint are not watching, and that no tool merges answers across
  clusters.
- **§4.3, MCP server:** "stdio or localhost HTTP" becomes stdio, or HTTP
  (loopback by default, off-host only with authentication and an access
  log), with one endpoint able to serve several clusters and read as each
  caller's own Google identity, pointing here.
