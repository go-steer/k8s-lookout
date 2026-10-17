---
title: MCP setup
description: lookout mcp — every read command as an MCP tool over stdio or loopback HTTP, and why the HTTP transport is loopback-only.
sidebar:
  order: 7
---

Every read-path command is also exposed 1:1 as an MCP tool:

```sh
lookout mcp                        # stdio: JSON-RPC on stdin/stdout
lookout mcp --listen=127.0.0.1:8383   # streamable HTTP on a loopback address
```

This exists because of a hard-learned constraint: distroless images kill
`bash + curl`. A distroless core-agent daemon has no shell to run
`lookout triage delta` in — MCP is how it calls the same checks
natively. The server uses the same kube client bootstrap as the CLI
(kubeconfig outside a pod, the ServiceAccount inside one), and tool
results carry the exact payload the CLI prints: logfmt findings
terminated by the `scanned=/findings=/elapsed=` summary line, passed
through the same sanitizer.

## Transports

- **stdio (default)** — the transport a daemon uses when it spawns
  `lookout mcp` as a child process. Diagnostics go to stderr only.
- **`--listen=<host:port>`** — streamable HTTP, for the same-pod case
  where the daemon and `lookout` run as separate containers sharing the
  pod's network namespace. **Non-loopback binds are refused by
  default**:

  ```
  --listen="0.0.0.0:8383": refusing to bind a non-loopback address.
  To serve off-host, pass --allow-non-loopback together with an
  authentication mode (--auth-token-file=<path> or
  --google-client-id=<id>) and --access-log=<path>; without all three
  lookout mcp is loopback-only (§4.3)
  ```

  A `lookout` reachable off-host hands its cluster read access to the
  network, so it is not something to open by accident. It is, however,
  something you can open on purpose — see below.

## Serving off-host

The one deployment shape the loopback rule blocks is the useful one:
the MCP server on one host, the agent somewhere else. That is
permitted, behind three flags that must all be present:

```sh
lookout mcp \
  --listen=0.0.0.0:8383 \
  --allow-non-loopback \
  --auth-token-file=/etc/lookout/mcp-token \
  --access-log=/var/log/lookout/mcp-access.log
```

Three rather than one, because each guards a different mistake:

- **`--allow-non-loopback`** — a token supplied for a localhost bind
  must not silently change which interface gets opened.
- **`--auth-token-file`** — a bind flag must not open an
  unauthenticated cluster-read API.
- **`--access-log`** — on loopback the log is a debugging convenience;
  off-host it is the only evidence that exists of who called what.

The token is a single shared bearer token, compared in constant time
against every request's `Authorization: Bearer <token>` header; anything
else gets a bare `401` that says nothing about what is behind it. The
file may have a trailing newline, must be one line, and must be at
least 16 characters — generate one with `head -c 32 /dev/urandom |
base64`. Permissions on the file are not checked, because the obvious
way to supply it in-cluster is a Secret volume and those mount `0644`.

Startup says what it did, on stderr:

```
lookout mcp: serving MCP over HTTP on 0.0.0.0:8383 — REACHABLE OFF-HOST.
lookout mcp: bearer-token authentication is REQUIRED; every call is recorded to /var/log/lookout/mcp-access.log.
```

**What this is not.** There is no authorization: every caller
presenting the token gets the full advertised tool surface. Narrow it
with [`--profile`](#profiles-dont-advertise-what-the-agent-will-never-call)
if a caller should not reach everything. For per-workload identity and
authorization, give workloads certificates instead:
[workload callers](#workload-callers-mtls).

## Each caller reads as themselves (Google-token mode)

For a Gemini Enterprise app calling lookout as a Custom MCP Server,
`--google-client-id` replaces the shared token with each person's own
Google access token. Every tool call then reads the cluster and Google
Cloud **as the person who made it**: what they see is what their own
Kubernetes RBAC and Cloud IAM allow (on GKE, IAM roles grant Kubernetes
access too). lookout's own identity reads nothing on their behalf.
The [Gemini Enterprise](/getting-started/gemini-enterprise/) page walks
through deploying it (the `deploy-mcp-gemini/` overlay) and wiring up the
data store.

```sh
lookout mcp \
  --listen=0.0.0.0:8383 \
  --allow-non-loopback \
  --access-log=/var/log/lookout/mcp-access.log \
  --google-client-id=123-abc.apps.googleusercontent.com \
  --google-allow=@example.com \
  --google-allow-write=sre-lead@example.com \
  --resource-url=https://lookout.example.com/mcp
```

- **`--google-client-id`** — the OAuth client the Gemini Enterprise data
  store is configured with. A token issued to any other client is
  refused, as is one without the `cloud-platform` scope. Tokens are
  checked with Google ([steward](https://github.com/go-steer/steward)),
  and a result is cached for at most ten minutes.
- **`--google-allow`** — who is let in: exact emails, or `@domain` for
  every account Google says belongs to that Workspace domain. An email
  suffix alone never matches, because a personal Google account can carry
  a company address.
- **`--google-allow-write`** — who may call tools that change state: the
  triage-status write and `findings diff`/`ack` (they update the store;
  the gate is per tool, so it covers their read modes too), and
  `k8s_net_probe_from` when served. Empty means nobody.
- **`--resource-url`** — this server's MCP URL as clients reach it. It is
  named in the RFC 9728 metadata served at
  `/.well-known/oauth-protected-resource/<path>` and in every `401`.

What the mode guarantees:

- **No fallback.** A call with no verified caller is refused; it never
  runs as lookout.
- **The token stays on verified TLS.** The caller's token is sent only to
  an `https` API server whose certificate is verified, so point lookout at
  GKE (in-cluster, or a kubeconfig context over https). A plaintext or
  `insecure-skip-tls-verify` cluster is refused.
- **Stateless HTTP.** Gemini Enterprise opens a new session for every
  interaction and never closes one, so lookout keeps none.
- **Per-caller limits.** One caller may have at most four calls in
  flight, inside the server-wide limit.
- **The access log names the caller** (`caller=<email>`), never the
  token.

- **Rows lookout reads as itself are filtered by the caller's access.**
  Some tools answer from files rather than the API server: `triage status`
  and `findings diff`/`ack` (the store and report), and `triage changes`
  and `triage radius --at` (graph history). Their rows are shown only if
  the caller may list pods in the row's namespace, or in all namespaces for
  a cluster-scoped object, checked with the caller's own token
  (`SelfSubjectAccessReview`, so lookout needs no extra grant). Withheld
  rows are not silent: the summary line says `hidden=<n>`, and a failed
  check hides the row. A history target in a namespace the caller cannot
  see is refused before it is looked up. Live reads are not filtered:
  the API server already applied the caller's RBAC to them.
- **Callers cannot widen what lookout reads or writes.** `store_cluster`
  is refused (the store is the operator's choice); a caller's `findings
  diff` reports but never persists, since the stored state is the whole
  cluster's; `findings ack` works only on subjects in namespaces the
  caller can see and is recorded as the caller, whatever `by` says.
- **Triage records name who wrote them.** A caller on the write list
  writes records only for namespaces they can see, and each record carries
  `recorded_by=<email>`; lookout stays the writer of record. That column
  is store migration v11: upgrade the sentinel before (or with) the MCP
  server, because older binaries refuse a store migrated past them.

## One endpoint, many clusters

One `lookout mcp` can serve several clusters, in several projects. Name
them with `--clusters` (`name=endpoint` pairs) or discover them with
`--clusters-from`, which takes the same scopes as
[`lookout watch`](/reference/watch/): projects, GKE fleets,
folders and organizations, comma-separated and mixable, or a kubeconfig.

```sh
lookout mcp --listen=0.0.0.0:8383 --allow-non-loopback \
  --access-log=/dev/stdout --google-client-id=... --google-allow=@example.com \
  --resource-url=https://lookout.example.com/mcp \
  --clusters-from=organizations/123456789012
```

Every tool then takes a `cluster` argument, and one more tool,
`k8s_list_clusters`, lists the clusters a caller can name, and whether
they can read each one:

```
cluster=checkout project=shop-prod location=us-central1 canonical=shop-prod/us-central1/checkout access=read dns_endpoint=true
cluster=prod@shop-prod project=shop-prod location=europe-west1 canonical=shop-prod/europe-west1/prod access=read dns_endpoint=true
cluster=prod@shop-stage project=shop-stage location=us-central1 canonical=shop-stage/us-central1/prod access=limited dns_endpoint=true
scope=organizations/123456789012 via=asset clusters=3
scanned=3 findings=0 elapsed=1.2s clusters=3
```

**Seeing a cluster is not reading it.** Discovery finds the clusters a
caller may *list*, which a folder- or organization-level role grants
widely; reading inside one takes Kubernetes RBAC, or IAM such as
`roles/container.viewer`, in that cluster. So each discovery also asks
every cluster, as the caller, whether they may list pods cluster-wide:
`access=read` if so, `limited` if not, `unreachable` if the cluster
could not be asked. `limited` is not "no access": someone whose
RoleBindings cover only their team's namespaces cannot list pods
cluster-wide, and Kubernetes has no cheap way to ask which namespaces a
person may read. So limited clusters are listed after the ones readable
cluster-wide, and still chosen and suggested when there is nothing
better; on those, pass the namespace you work in. Only unreachable
clusters are never chosen.

**A call that names no cluster** reads, in order: `--default-cluster`
unless it is unreachable; the caller's only cluster in
`--default-project`; their only cluster at all, counting the clusters
they can read cluster-wide when there are any and the limited ones when
there are not. Otherwise it is answered with a short list of clusters to
choose from, and a client that supports elicitation (on a stateful
session) asks the person instead. A name that is not in the caller's set
is answered with the closest names, so a model chooses rather than
guesses; a bare name repeated across projects resolves to
`--default-project`'s, when there is exactly one there.

- **A call reads one cluster.** To compare three clusters, the agent
  makes three calls, which MCP clients run in parallel; each answer's
  summary line says which cluster it came from (`context=<canonical>`).
  lookout never merges answers across clusters.
- **Names are short where they can be:** the bare name when it is
  unique, `name@project` when it repeats across projects
  (`name@project/location` when it repeats within one), and
  `project/location/name` always works.
- **With Google-token mode, discovery runs as each caller,** with their
  own token, cached for five minutes. A caller sees only the clusters
  they can read, and lookout needs no IAM of its own. Because each
  caller's Google token goes to the cluster's API server, this mode
  serves GKE clusters only: no kubeconfig fleet, and `--clusters`
  endpoints must be control-plane DNS endpoints (`*.gke.goog`). Without it,
  lookout discovers once at startup, as itself, and again every
  `--rediscover` (default 10m); an unknown name triggers a rediscovery,
  at most once a minute. For a folder or organization, discovery uses
  Cloud Asset Inventory and falls back to a Resource Manager walk;
  `--folder-discovery=resourcemanager` never calls Cloud Asset
  Inventory, and `--folder-discovery=asset` never falls back.
- **Cloud-backed checks follow the cluster:** `audit cluster`,
  `cloud ipspace` and the rest answer for the named cluster and its
  project, never for the cluster lookout runs in.
- The access log names each call's cluster (`cluster=<canonical>`).
- **`--store` and `--report` become stems:** each call reads its
  cluster's own file, named the way a multi-cluster `lookout watch`
  names it, so an endpoint beside a multi-cluster watch reads what the
  watch writes. A cluster whose bare name repeats in the set has no file
  (watch skips such clusters too). Not in Google-token mode: file names
  are bare cluster names, which each caller's own set cannot prove
  unique.
- **`net probe` is refused for other clusters:** it sends its packets
  from lookout's own pod, so it answers only for the cluster lookout
  runs in; `net probe-from` probes from inside any cluster.
- **One cluster cannot take every slot:** each cluster's calls in flight
  are capped (the global limit divided by the number of clusters, at
  least 2; `--max-concurrency-per-cluster` overrides).
- **Stateless HTTP:** any replica answers any request, so the endpoint
  scales as a Deployment behind a Service with no sticky sessions.
- `--kubeconfig`/`--context` pin one cluster and are refused here; a
  kubeconfig fleet is `--clusters-from=kubeconfig:<path>`.

## Workload callers (mTLS)

Machine callers (core-agent daemons, watch-opened incident sessions)
authenticate with their **workload identity**, a client certificate,
instead of a shared token. `--mtls-listen` opens a second listener that
terminates TLS itself and requires a client certificate in the
handshake; the caller is the certificate's SPIFFE ID, and a rules file
says which tools (and, on a multi-cluster endpoint, which clusters)
each workload may call, and whose identity it reads as.

```sh
lookout mcp \
  --listen=0.0.0.0:8383 --allow-non-loopback \
  --access-log=/var/log/lookout/mcp-access.log \
  --google-client-id=123-abc.apps.googleusercontent.com --google-allow=@example.com \
  --resource-url=https://lookout.example.com/mcp \
  --mtls-listen=0.0.0.0:8443 \
  --spiffe-trust-domain=example-project.svc.id.goog \
  --workload-rules=/etc/lookout/workload-rules.yaml
```

**Two listeners, two kinds of caller.** People reach `--listen` through a
Gateway that terminates TLS with a public certificate and forwards
plaintext, carrying their Google token (or the shared bearer token). A
workload inside the cluster dials the lookout Service's `--mtls-listen`
port directly, and lookout terminates that TLS itself, because only the
endpoint that terminates TLS sees the client certificate. purser's mTLS
authenticators demand the certificate in the handshake, so one listener
cannot also admit people who have none; each listener has exactly one way
in, and nothing falls through from one to the other. A missing or
untrusted certificate is a refused connection, never a request served as
somebody else, and the reason is logged on stderr
(`lookout mcp: --mtls-listen: …`). Either listener may run alone.

Verify workload certificates one of two ways:

- **SPIFFE** (`--spiffe-trust-domain`, optionally
  `--spiffe-credentials-dir`): each caller presents an X509-SVID that
  must chain to the trust domain's bundle, and lookout presents its own
  SVID. The defaults read GKE managed workload identity's mount
  (`/var/run/secrets/workload-spiffe-credentials`: `certificates.pem`,
  `private_key.pem`, `ca_certificates.pem`), re-read as it rotates. On GKE
  the trust domain is `<project>.svc.id.goog` and a pod's ID is
  `spiffe://<project>.svc.id.goog/ns/<namespace>/sa/<ksa>`. Clients
  verify lookout by its SPIFFE ID, not a DNS name.
- **A standard CA** (`--tls-cert-file`, `--tls-key-file`,
  `--client-ca-file`): lookout's own certificate and key (re-read when
  they change, e.g. a cert-manager Secret), and the CA bundle client
  certificates must chain to. The caller's identity is its certificate's
  single URI SAN (a mesh certificate's SPIFFE ID, say). Restart to rotate
  the client CA bundle.

A routable `--mtls-listen` needs `--allow-non-loopback` and
`--access-log`, like `--listen`; the certificate is its authentication.

### The rules file

```yaml
rules:
  - name: incident-agents
    # exact identities (SPIFFE IDs, or URI SANs under a standard CA)
    identities:
      - spiffe://example-project.svc.id.goog/ns/agents/sa/core-agent
    tools: [triage, k8s_triage_status]   # the --tools syntax
    write: true                          # k8s_triage_status changes state
  - name: watch-sessions
    # SPIFFE only: every ID in a trust domain, under a path prefix
    trust_domain: example-project.svc.id.goog
    path_prefix: /ns/lookout
    tools: [triage]
    service_account: lookout-sessions@example-project.iam.gserviceaccount.com
```

- **Rules only grant.** A call needs one rule that matches the caller and
  grants the tool; a verified workload no rule names may call nothing.
  Order does not matter. Keys are strict: a misspelt one is a startup
  error.
- **`tools`** takes the [`--tools`](#profiles-dont-advertise-what-the-agent-will-never-call)
  syntax: tool names, profile names, `all`, and `-name`.
  `k8s_net_probe_from` is named exactly when served.
- **`write: true`** is required for the tools that change state (the
  triage-status write, `findings diff`/`ack`, `k8s_net_probe_from`):
  naming them is not enough, the same split as `--google-allow-write` for
  people.
- **`clusters`** is required on a multi-cluster endpoint and refused on a
  single-cluster one: cluster names, canonical `project/location/name`,
  or `"*"`. `k8s_list_clusters` lists only the clusters a workload may
  call.
- **`service_account`**: by default a workload reads as lookout. Name a
  service account and the call reads as it instead, with a token lookout
  mints through IAM Credentials `generateAccessToken` (a `-tags gke`
  build; lookout's own identity needs Service Account Token Creator on
  that account). If impersonation fails, the call fails with the reason;
  it never falls back to lookout's identity. On a multi-cluster endpoint
  the token goes only to GKE control-plane DNS endpoints. Two rules
  granting one call must agree on the service account.

What else applies to a workload call:

- **The access log names it** (`caller=spiffe://…`), never a token.
- **Per-caller limits**: at most four calls in flight per workload.
- **Triage records name it**: `recorded_by=<SPIFFE ID>`, and an ack is
  recorded as the workload.
- **Its store reads are not filtered.** Workloads read lookout's store
  and reports as lookout does, by design (people's rows are filtered by
  their own access); a `findings diff` it runs persists, as an
  operator's does.

## Wiring into a core-agent daemon

Register `lookout mcp` in the daemon's MCP server configuration as a
stdio server (spawned command: `lookout`, args: `["mcp"]`), or — when the
daemon image cannot exec at all — run `lookout` as a second container in
the daemon's pod with `--listen=127.0.0.1:<port>` and register the HTTP
endpoint. A daemon calling a shared lookout endpoint elsewhere in the
cluster authenticates with its workload certificate instead of a shared
token: see [workload callers](#workload-callers-mtls). In-cluster, the
pod's ServiceAccount needs read RBAC covering
the checks you expect agents to call; the sentinel's shipped ClusterRole
(`deploy/12-clusterrole-watcher.yaml`) is a working superset for the
common ones.

## Profiles: don't advertise what the agent will never call

Every advertised tool is paid for on **every** model call, whether or
not it is ever invoked. The full surface is over 130 KB of JSON schema
— roughly 35k tokens per turn — and the cost is not only tokens: an
agent choosing among thirty similar-sounding tools chooses worse than
one choosing among seven.

So the surface is selectable. The default is unchanged — every command,
for every client that asks for nothing — and the saving is opt-in:

```sh
lookout mcp --profile=triage              # the incident surface
lookout mcp --profile=audit               # the posture surface
lookout mcp --tools=all,-k8s_perf_probe   # everything but one tool
lookout mcp --profile=triage --tools=-k8s_triage_logs

lookout mcp --list-tools                  # what a selection costs, per tool
```

`--profile` and `--tools` are one left-to-right selection, `--profile`
first, in the same `all,-x` syntax as `bundle --lists`: `all` (or
`full`) adds every tool, a profile name adds its members, a tool name
adds one, and a `-` prefix removes. A selection that resolves to zero
tools is a usage error — a server with an empty tool list is
indistinguishable from a missing one.

`lookout mcp --help` prints the profiles with their sizes; each
command's Reference page names the profiles it belongs to. Membership
is declared on the command itself, so a new check joins a profile in
the same edit that creates it.

## The access log

`lookout mcp` is silent by default: it writes nothing but protocol
frames, so when an agent's tool call misbehaves there is no record it
happened at all. `--access-log` fixes that with one logfmt line per
call:

```sh
lookout mcp --access-log=/var/log/lookout/mcp-access.log
```

```
ts=2026-08-18T14:03:21Z tool=k8s_scan exit=0 dur=1.204s bytes=4096
ts=2026-08-18T14:03:29Z tool=k8s_triage_logs exit=1 dur=312ms bytes=118
ts=2026-08-18T14:03:33Z tool=k8s_triage_logs exit=2 dur=0s bytes=64
```

`exit` is the §4.2 code the tool call mapped from — `0` a payload, `1`
a tool error the model can see, `2` a rejected argument. Calls the
schema layer rejects before the command ever runs are logged too;
those are exactly the ones worth noticing.

The file is created if absent, **appended** if present (a supervisor
restart must not erase the evidence from the run that caused it), and
created mode `0600` — the tool names alone say which clusters an
operator has been reading. If the path cannot be opened, `lookout mcp`
exits 2 rather than serving without a log. It is optional on loopback
and **mandatory** for an off-host bind.

What a line deliberately does *not* carry is the arguments or the
response body. The [sanitizer](/concepts/sanitization/) guarantees
no secret value reaches an output surface; a log that copied payloads
would be a second place that guarantee has to hold, audited by nobody.
Tool, outcome, and size answer the operational questions — what was
called, did it work, what did it cost — without becoming a second data
path.

## The tools

Tool names are the commands' MCP names — `triage delta` →
`k8s_triage_delta`, `bundle` → `k8s_triage_workload` — with input
schemas mirrored from the CLI flags (plus a `target` property where a
command takes a positional argument).

Two conveniences for clients that guess: on every other tool `target`
is accepted as a synonym for `workload`, and an argument name the tool
does not know is rejected with the nearest one it does —
`unknown argument "form" for tool k8s_scan; did you mean "format"?
(accepts: …)`. Only the canonical names appear in the schemas.

The current surface:

| Tool | Command |
| --- | --- |
| `k8s_scan` | [`scan`](/reference/scan/) |
| `k8s_cluster_health` | [`health`](/reference/health/) |
| `k8s_triage_workload` | [`bundle`](/reference/bundle/) |
| `k8s_list_resources` | [`triage list`](/reference/triage-list/) |
| `k8s_triage_delta` | [`triage delta`](/reference/triage-delta/) |
| `k8s_triage_logs` | [`triage logs`](/reference/triage-logs/) |
| `k8s_event_timeline` | [`triage events`](/reference/triage-events/) |
| `k8s_resource_top` | [`triage top`](/reference/triage-top/) |
| `k8s_blast_radius` | [`triage radius`](/reference/triage-radius/) |
| `k8s_recent_changes` | [`triage changes`](/reference/triage-changes/) |
| `k8s_resource_spec` | [`triage spec`](/reference/triage-spec/) |
| `k8s_triage_status` | [`triage status`](/reference/triage-status/) |
| `k8s_findings_diff` | [`findings diff`](/reference/findings-diff/) |
| `k8s_findings_ack` | [`findings ack`](/reference/findings-ack/) |
| `k8s_state_edges` | [`state edges`](/reference/state-edges/) |
| `k8s_admission_webhooks` | [`state webhooks`](/reference/state-webhooks/) |
| `k8s_workload_identity` | [`state wi`](/reference/state-wi/) |
| `k8s_volume_conflicts` | [`state volumes`](/reference/state-volumes/) |
| `k8s_storage_binding` | [`state storage`](/reference/state-storage/) |
| `k8s_gateway_routes` | [`state gateway`](/reference/state-gateway/) |
| `k8s_keda_scalers` | [`state keda`](/reference/state-keda/) |
| `k8s_gitops_drift` | [`stab drift`](/reference/stab-drift/) |
| `k8s_drain_blockers` | [`stab drain`](/reference/stab-drain/) |
| `k8s_scaledown_blockers` | [`stab scaledown`](/reference/stab-scaledown/) |
| `k8s_perf_probe` | [`perf probe`](/reference/perf-probe/) |
| `k8s_cloud_stockout` | [`cloud stockout`](/reference/cloud-stockout/) |
| `k8s_cloud_orphans` | [`cloud orphans`](/reference/cloud-orphans/) |
| `k8s_cloud_ipspace` | [`cloud ipspace`](/reference/cloud-ipspace/) |
| `k8s_cloud_quota` | [`cloud quota`](/reference/cloud-quota/) |
| `k8s_net_probe` | [`net probe`](/reference/net-probe/) |
| `k8s_net_probe_from` | [`net probe-from`](/reference/net-probe-from/): **not served by default**, see below |
| `k8s_audit_workloads` | [`audit workloads`](/reference/audit-workloads/) |
| `k8s_audit_hardening` | [`audit hardening`](/reference/audit-hardening/) |
| `k8s_audit_netpol` | [`audit netpol`](/reference/audit-netpol/) |
| `k8s_audit_rbac` | [`audit rbac`](/reference/audit-rbac/) |
| `k8s_audit_cluster` | [`audit cluster`](/reference/audit-cluster/) |
| `k8s_audit_upgrades` | [`audit upgrades`](/reference/audit-upgrades/) |
| `k8s_audit_exemptions` | [`audit exemptions`](/reference/audit-exemptions/) |

### The one privileged tool

`k8s_net_probe_from` runs `net probe` from inside a named pod by adding
an ephemeral container to it, a change that stays in the pod's spec
until the pod is replaced. A tool that changes a workload is a
different risk class from the read surface, so the server never offers
it unless the operator starts it with an image:

```sh
lookout mcp --probe-from-image=ghcr.io/go-steer/lookout@sha256:<digest>
```

Neither `--tools=all` nor any profile adds it, and naming it in
`--tools` is a usage error. The flag fixes the image the probe
container runs (digest-pinned; tags are refused), so the tool has no
image argument and a model cannot choose what runs in the pod. When
served it is marked `readOnlyHint: false`. The identity the server runs
as also needs the `deploy-probe/` grant; without it every call answers
with a `probe.refused` record and changes nothing. See
[`net probe-from`](/reference/net-probe-from/) and
`docs/in-pod-probe-design.md`.

Clients that refuse to start when a server advertises an unclassified
write tool (k8s-sre-agent's read-only guard does) are unaffected by a
plain `lookout mcp`, which never lists it.

Commands added later become tools with no extra wiring — the list the
server serves, its schemas and its descriptions are generated from the
same command metadata as `--help` and this site's Reference section, so
`tools/list` is always the authoritative surface. The table above is a
hand-maintained copy of it, held to the registry by a test.
Tool descriptions are written as micro-skills ("when to reach for
this"), which is most of what an agent needs; the workflow-level
decision tree ships as skills in
[`skills/`](https://github.com/go-steer/k8s-lookout/tree/main/skills).

The loop is smoke-tested end to end in a live drill: `initialize` →
`tools/list`, then `tools/call k8s_cluster_health` returned
`isError:false` with the identical scorecard payload the CLI prints,
summary line included.
