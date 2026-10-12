# ax sink — incidents in Agent Executor tasks

[`agent-sink-design.md`](./agent-sink-design.md) reduced the watch path to
two verbs, open an incident and append to it, and gave lookout two `Sink`
implementations: the core-agent daemon and a generic webhook. This note adds
a third, `--sink=ax`, which runs incidents in
[Agent Executor (AX)](https://github.com/google/ax) tasks on Agent Substrate:
by default one task per incident, or one long-lived task per cluster.
Tracking issues: #567 (the sink), #590 (task identity, task scope, session
reuse), #580 (TLS for an AX in another cluster), #599 (holding reminders).

**Decision: the ax sink creates (or finds) an AX task for each incident (or
one per cluster), named from lookout's own incident identity, and speaks the
core-agent session API to the agent inside it, through Substrate's router.** Nothing about the payloads changes; like the webhook sink, this is
only about where they're delivered.

## Why a task per incident

With a long-running daemon, every incident shares one process, one set of
credentials and one blast radius, and the daemon runs whether or not anything
is happening. An AX task gives each incident its own gVisor sandbox, egress
policy and session. Substrate suspends a quiet task and wakes it when the next
request arrives (about 1.7s from suspended to answering in testing), so an
open incident with nothing happening costs storage, not compute.

lookout already decides what "one incident" is (dedup, storm correlation,
severity routing), so the mapping is direct.

## Settled decisions

### The verbs

| Verb | ax sink |
|---|---|
| `OpenIncidentKeyed(key, payload)` | Create the key's task from the template (an existing task with that name is reused), `ResumeTask`, wait for the task to be ready. If this incident was opened before and its session is known, `POST /sessions/<sid>/inject` into that session; otherwise `POST /sessions` and `POST /sessions/<sid>/inject` with the payload as the message. Returns `<task>/<sid>`. |
| `Append(id, payload)` | `POST /sessions/<sid>/inject`, routed to `<task>`. A suspended task is resumed by the router before the request is forwarded. A pure reminder (`repeat_count > 0`) to an idle task is held instead; see Holding reminders. |
| `CreateSessionKeyed(key)` | Start the cluster's watchboard task, then `POST /sessions`. Keeps the watchboard rotation's frozen wire order (empty session first). |
| `OpenIncident` / `CreateSession` | The plain `Sink` and `SessionOpener` verbs, for callers that don't pass a key. The key is rebuilt from the payload (see below); lookout's own dispatcher never uses them with this sink. |

The session calls are made by the existing core-agent `Injector`, given an
HTTP client whose transport adds `ate-target-actor: <atespace>/<task>`. The
bodies and headers are therefore byte-identical to the core-agent sink; only
the URL (the router) and that header differ.

### Task names come from lookout's incident identity

The first version named a task by hashing the payload's `fingerprint`. That
was wrong: lookout's fingerprint is the incident *class* (signal kind, reason
class, object kind, zone), deliberately not the object. A k8s-event payload
has no fingerprint, so its task fell back to `uid` + reason; a storm payload
has one, so an unrelated storm of the same class in the same zone, days later
or in another cluster, landed in the old storm's task; a watchboard digest has
neither, so every digest made a new task, and with it another running agent
and its golden snapshot (#590).

The dispatcher already knows what "the same incident" means, so it hands that
to the sink instead of the sink guessing from the payload. `pkg/inject` has an
optional capability next to `SessionOpener`:

```go
type KeyedOpener interface {
	OpenIncidentKeyed(ctx context.Context, key IncidentKey, payload any) (id string, err error)
	CreateSessionKeyed(ctx context.Context, key IncidentKey) (string, error)
}
```

When the sink has it, the dispatcher's one open helper (`openSession`) and the
watchboard use it; the core-agent and webhook sinks don't implement it and see
exactly the calls they always have. An optional interface rather than a value
on the context, because the key is required for this sink to work properly
and a type assertion makes that visible at the call site. The types live in
`pkg/inject`, so `pkg/` still never imports `internal/`
(`TestLayering_TheImportGraphMatchesTheDesign`).

| Incident | `IncidentKey` |
|---|---|
| Per-incident open (and the deferred retry) | cluster + the canonical dedup key, `TriageEvent.CanonicalKey()`: UID + canonical reason, the key the incident is bound and tracked by. kubelet's `Failed`/`BackOff` for one pull problem is one key. |
| Storm (formation and the session-less retry) | cluster + the storm's ancestor, `Kind/namespace/name`. |
| Watchboard (first session and every rotation) | cluster only: one watchboard per cluster. |

The task name is derived from the key and the task scope. AX task names are
DNS labels (63 characters of `[a-z0-9-]`):

| Scope | Key | Task |
|---|---|---|
| `incident` | incident or storm | `lookout-` + first 16 hex digits of SHA-256(cluster, kind, id) |
| `incident` | watchboard | `lookout-wb-<cluster slug>-<8 hex of the cluster name>` |
| `cluster` | any | `lookout-<cluster slug>-<8 hex of the cluster name>` |

The slug keeps the cluster readable in `ax get tasks`; the hash of the exact
name keeps two clusters that slug alike apart. A retried open, or the same
incident firing again after the dedup cooldown, reaches the same task, because
AX rejects a second task with the same name and the sink treats that as "use
it".

A caller that uses the plain `OpenIncident` (an embedder, not lookout) gets a
key rebuilt from the payload: `uid` + `reason_class` (else `reason`) for an
incident, the `ancestor_*` fields for a storm, otherwise the cluster's
watchboard. Never the fingerprint.

### Task scope

`--ax-task-scope=incident` (the default) runs each incident and each storm in
its own task, plus one watchboard task per cluster. Each incident gets its own
sandbox, egress policy and blast radius.

`--ax-task-scope=cluster` runs one long-lived task per cluster. Every incident,
storm and watchboard digest gets its own session inside it. Every incident task
is a running agent with its own memory and CPU on a Substrate worker (several
agents can share a worker, up to its resources), so this is the shape for
clusters that see many incidents; the trade-off is that every incident shares
the one agent process, its credentials and its egress policy.

The incident id is `<task>/<session>` in both scopes, so `Append` doesn't care.

### Reopening reuses the session

When an incident comes back after the dedup cooldown, it reaches the same task
and, if its earlier session is known, the payload is injected into that
session instead of a new one, so the agent picks up its own earlier diagnosis.
The sink remembers `IncidentKey -> <task>/<session>`:

- **In memory**, always (bounded at 4096 incidents; past that an arbitrary
  entry is forgotten).
- **In the store**, when `--store` is set: table `sink_sessions` (store
  migration v9), keyed by cluster and the key's text form, written on every
  open. This is what makes reuse survive a lookout restart. The sink is
  process-wide but each cluster runner has its own store, so each runner hands
  its store to the sink at start (`UseSessionStore`). Rows follow the store's
  TTL (30 days by default): an incident that hasn't come back in that long
  gets a new session.

If the agent answers the inject with 404 or 410 (the task was recreated, or
the agent lost its state), the sink opens a new session and remembers that one.
Any other failure keeps the incident bound to the remembered session and
reports the error, the same way a partial open is reported. A remembered
session in a different task (the scope changed between runs) is ignored.

Watchboard sessions are never reused: the first session and every rotation
open a new session in the watchboard task, which is what rotation is for.

### Holding reminders

A failure that stays broken repeats into its open incident's session as a
followup (#599). A followup with `repeat_count > 0` is a pure reminder: still
failing, nothing material changed. Reminders back off from `--dedup-window`
to one an hour. Sending one to an idle task keeps the task awake, and sending
one to a suspended task wakes it, only to tell the agent what it already
knows. So the ax sink holds reminders while a task is idle:

- **A pure reminder to an idle task is held, not sent.** `Append` returns
  nil. Nothing goes through the router and there is no `ResumeTask`, so
  nothing can wake the task.
- **Only the latest reminder per session is kept.** A newer one replaces it.
- **Held reminders go first, ahead of the next real inject to the task.**
  "Real" means anything that isn't a pure reminder: a material change (a
  followup without `repeat_count`), a new incident, a reopen, a storm update,
  a watchboard session, a resolution. In `cluster` scope that includes any
  other incident's inject into the shared task. Each held reminder goes to its
  own session, oldest first. A reminder that fails to deliver is logged and
  dropped, and never fails the real inject behind it.
- **A reminder to a busy task goes straight through,** as do all non-reminder
  injects.
- **The agent's first delivery for an incident is never held.** If an
  incident's last real inject failed (an open whose payload never arrived,
  or a change the agent didn't get), its next reminder goes straight
  through, however idle the task. The reminder carries the incident's full
  payload. The dispatcher also resends a failed open as an open (#607);
  this check covers the same case from the sink's side.
- **A gone task or session drops its held reminders.** A reminder whose task
  AX no longer has (`GetTask` answers NotFound) isn't held for a wake that
  will never come. The task's held reminders are dropped, and `Append`
  answers 404, the same answer the agent gives for a lost session, so the
  dispatcher un-binds the incident and reopens it. A held reminder whose
  session turns out to be gone (404 when delivered ahead of a real inject)
  is dropped as well.

**Idle detection.** The sink keeps its own record of when it last delivered a
real inject to each task. A task is busy if that was less than its idle
window ago and AX doesn't record it as suspended. The idle window is the
template's `spec.idle.suspendAfter` (#593), the same threshold AX suspends
on. Without one it is a constant, ten minutes (AX's suggested setting); there
is no flag. AX does expose a readable state: `GetTask` returns
`status.phase`, a read of AX's own record that never wakes the task. But
`Running` doesn't mean busy (an idle task is `Running` until AX suspends it,
and a reminder in that interval resets AX's idle clock), and AX's phase can
lag the actor by a reconcile pass. So the record decides. The `GetTask` made
for each reminder adds two things: whether the task still exists, and a
phase that catches a task suspended early (on completion, or by hand).
`GetTask` is bounded at three seconds, a constant, because the dispatcher
calls `Append` on the source's own goroutine and a hung AX API must not stall
the source. If `GetTask` fails or times out, the record alone decides.

A reminder sent inside the window resets AX's idle clock, which can keep a
task awake for up to one more window. That is bounded: reminders are at
least `--dedup-window` apart, and the next one usually falls outside the
window and is held.

**Resolution.** A held reminder is delivered just before the resolution, not
dropped. The task is waking for the resolution anyway, so it costs one more
inject but no extra wake. The agent then reads the incident in order:
"still failing (reminder N)", then "resolved". That tells it how long the
failure lasted, which a bare resolution doesn't.

**Bounds.** At most one held reminder per session, and at most 4096 in total.
Past that the oldest is dropped. A held reminder older than three hours is
dropped too, rather than delivered late. A still-failing incident refreshes
its reminder at least hourly, so a reminder that old means lookout stopped
sending reminders (the incident went quiet or lost its binding). The record
of real injects is bounded at 4096 tasks. A forgotten task counts as idle, so
its reminders are held rather than lost.

**Restart.** Held reminders and the record live in memory only, and a
restart loses them. That is acceptable: a held reminder only repeats what
the agent already knows, and the next reminder on the schedule takes its
place. After a restart the record is empty, so every task counts as idle
until its next real inject, and reminders are held in the meantime.

**Metrics.** `lookout_ax_reminders_total{outcome}`, per cluster:

| Outcome | Meaning |
|---|---|
| `held` | Held because the task was idle. |
| `superseded` | A held reminder replaced by a newer one for the same session. |
| `delivered` | A held reminder sent ahead of a real inject. |
| `direct` | Sent at once because the task was busy. |
| `dropped` | A held reminder expired, was evicted, or failed delivery. |

The core-agent and webhook sinks deliver every reminder at once, unchanged.

### Waiting for the agent

AX reports a task resumed once its workspace is ready, which can be a moment
before the agent's own server is listening. After `ResumeTask`, the sink polls
`GET /readyz` through the router until it answers 200 (bounded by a two-minute
start timeout). With AX's runner pass-through, the runner only reports ready
once the agent's server accepts connections.

### The task template

`--ax-task-template` is an AX `Task` manifest: image, command, egress rules,
injected credentials, `http.port`, workspaces. The sink sets
`metadata.name` per task and never changes anything else.
`metadata.atespace` defaults to `default`. The template is decoded strictly at
startup, so a typo fails `lookout watch` immediately rather than on the first
incident.

### Agents need to know which cluster

The agent in the task reads the cluster through tools that name it in full,
like the GKE MCP server's `projects/<project>/locations/<location>/clusters/<name>`.
The payload's `cluster` is only `--cluster-name`, so the sentinel must stamp
`project` and `region`/`zone` too. The GKE image flavor detects them from
metadata; the vanilla image needs `--project` and `--region` (or `--zone`). In
testing without them, the agent guessed project IDs until its budget ran out;
with them, it built the path and finished the diagnosis. The sentinel logs a
`WARNING` at startup when `--sink=ax` runs without them.

### Flags

| Flag | Meaning |
|---|---|
| `--sink=ax` | Select this sink. |
| `--ax-server` | AX API address, e.g. `ax-server.ax-system.svc:8080`. Plaintext gRPC unless `--ax-server-tls`. Required. |
| `--ax-server-tls` | Dial the AX API over TLS, verified against the system roots (augmented with `--ax-ca-file`). Default off; see Cross-cluster deployment. |
| `--ax-ca-file` | PEM CA bundle added to the system roots for verifying the AX API's certificate. Requires `--ax-server-tls`. |
| `--ax-token-env` | Optional env var name holding a bearer token (e.g. Google ID token or service JWT) attached as per-RPC `authorization: Bearer <token>` metadata on every AX gRPC call. Requires `--ax-server-tls`. |
| `--ax-task-template` | Path to the Task manifest. Required. |
| `--ax-router-url` | Substrate router, default `http://atenet-router.ate-system.svc.cluster.local`. |
| `--ax-task-scope` | `incident` (default) or `cluster`; see Task scope. |
| `--token-env`, `--owner` | Same meaning as for the core-agent sink, applied to the agent's session API inside each task. |

`--mode=shared`, `--target-session` and `--daemon-url` are rejected with this
sink. The `ax-*` flags are rejected with the other sinks.

### Cross-cluster deployment

lookout and AX don't have to share a cluster. The ax sink makes two
connections, and each can cross a network:

| Connection | Flag | Carries | Protection |
|---|---|---|---|
| AX API (gRPC) | `--ax-server` | task create, get, resume; the task template | `--ax-server-tls`, `--ax-ca-file`, `--ax-token-env` |
| Substrate router (HTTP) | `--ax-router-url` | the session calls: payloads, and the `--token-env` bearer token | an `https://` URL |

Pointing both at another cluster's endpoints — for example internal load
balancers or Gateways in a shared VPC — supports full TLS encryption and
per-RPC authentication (#580).

**The AX API.** AX serves plaintext gRPC itself (`ax-server` listens with
unencrypted HTTP/2 and no TLS options), so TLS needs a front that terminates
it: a load balancer, a Gateway, or a mesh. `--ax-server-tls` dials the front
over TLS and verifies its certificate against the system roots and the host
name in `--ax-server`; `--ax-ca-file` appends custom PEM certificates onto a
certificate pool seeded from `x509.SystemCertPool()` (falling back to a fresh
pool when unavailable), supporting private CAs without dropping public/system
roots. TLS is off by default, so an in-cluster deployment that dials
`ax-server.ax-system.svc:8080` keeps working unchanged. Passing `--ax-ca-file`
without `--ax-server-tls` is a usage error (exit 2); an unreadable or invalid
CA file fails at startup, while an untrusted certificate surfaces on the first
RPC naming the certificate problem because gRPC connects lazily.

**Per-RPC credentials (`--ax-token-env`).** When an authenticating front sits in
front of AX across clusters (such as IAP, an Envoy/Gateway JWT policy, or a
service proxy validating bearer tokens / Google ID tokens), set `--ax-token-env`
to the name of an environment variable holding the token. `axsink.BearerCredentials`
attaches `authorization: Bearer <token>` per-RPC metadata to every AX gRPC call
with `RequireTransportSecurity() == true` so tokens never travel over plaintext
gRPC. Passing `--ax-token-env` without `--ax-server-tls`, or when `--sink != ax`,
fails startup with a usage error (exit 2); if the named environment variable is
empty at startup, `dialAX` returns an error before running informers.

**The router.** An `https://` `--ax-router-url` works out of the box: session
calls use the shared sink HTTP transport verified against the system roots
(`SSL_CERT_FILE` or `SSL_CERT_DIR` add a private CA; `--ax-ca-file` applies to
the AX API only). Over plain `http://`, the `--token-env` session bearer token
crosses the network in the clear, so lookout emits a startup warning (`slog.Warn`
and stderr log) when `--ax-router-url` uses plain `http://` targeting any host
outside the local cluster. Any userinfo password in the logged URL is masked,
and bearer tokens never enter log output.

**What counts as cluster-local:** loopback (`localhost`, `*.localhost`,
`127.0.0.0/8`, `::1`) and fully qualified Service names (`*.svc`,
`*.svc.cluster.local`). Not private IPs: the cross-cluster case is often an
internal load balancer with a private IP, and nothing in an address distinguishes
a shared VPC ILB from a local ClusterIP. Not short names (`atenet-router.ate-system`)
either: they resolve through the pod's DNS search path, which can match search
domains outside the cluster. Spell in-cluster Service names out in full (for
example `http://switchboard.agent.svc:8080`) to keep plain HTTP warnings silent.

### AX API: a pinned copy, not an import

AX's API is gRPC only, and its Go module requires a newer Go toolchain than
lookout. `internal/axapi` carries a copy of AX's `ax.proto` with generated
stubs (`go generate ./internal/axapi`). AX's API is `v1alpha1`, so the copy is
pinned deliberately; re-copy it when AX changes.

The sink lives in `internal/axsink`, not `pkg/inject`, so the embeddable
`pkg/` half never depends on the AX stubs or gRPC
(`TestLayering_TheImportGraphMatchesTheDesign`). `pkg/inject` exports
`NewSinkHTTPClient` so the ax sink keeps the shared sink transport.

## Dependencies outside lookout

- **AX runner pass-through (`spec.http.port`).** Substrate's router only
  reaches the AX runner, so without a pass-through no request reaches the
  agent's session API. This is on a fork of AX
  (`mastersingh24/ax:task-idle-suspend`, built on `task-egress-credentials`)
  and will be proposed upstream. The same fork lets a task declare egress and
  injected credentials, and suspends idle or finished tasks itself
  (`spec.idle`, `spec.onCompletion`; google/ax#420), which matters because
  every open incident task otherwise keeps a running agent, with its memory
  and CPU, on a Substrate worker.
- **The agent's session API.** The agent must serve `POST /sessions` and
  `POST /sessions/<sid>/inject` (core-agent's attach API). mast answered 501
  to `POST /sessions` until go-steer/mast `feat/ax-substrate-support`.

## Out of scope

- **Lifecycle after resolution.** The sink doesn't suspend or delete a task
  when its incident resolves. Suspension on idle belongs in AX
  (google/ax#420); deletion policy is an open question on #567.
- **token-burn.** Stays core-agent-only, as for the webhook sink.
- **Reading results back.** lookout stays fire-and-forget; the agent reports
  through its own channels (switchboard).

## Limitations

- **Without `--store`, session reuse ends at a restart.** The incident still
  reaches its task, but a new session in it, so the agent starts that
  conversation cold (its earlier session is still in the task).
- **Session reuse relies on the agent answering 404 for an unknown session.**
  core-agent does. An agent that answers something else for a session it no
  longer has would keep failing that incident's reopen until the store row
  expires.
- **`cluster` scope shares one sandbox.** Every incident in the cluster runs in
  the same agent process with the same credentials and egress.
- **Static bearer token refresh.** `--ax-token-env` reads the token value from
  the process environment when opening the AX gRPC client at startup; deployments
  using short-lived tokens should mount or refresh identity tokens via their proxy
  front or workload identity sidecar.
- **Held reminders don't survive a restart.** See Holding reminders.
- **The sink can't see an agent that is busy on its own.** A task counts as
  busy only within its idle window after a real inject. An agent still
  investigating past that (AX's `spec.idle.busyPath` keeps such a task from
  suspending) gets its reminders held, not sent. This is harmless: the
  reminder said nothing new.
- **A storm reopens by ancestor.** A later storm on the same Node or workload
  goes into the earlier storm's session, by design; a different ancestor is a
  different task.

## How it was tested

- `internal/axsink`: open, append, readiness wait and id parsing; reopen
  reuses the task and the session; a 404 opens a new session; a reopen after a
  restart reaches the old session through a real store and doesn't without
  one; cluster scope shares one task; watchboard rotation stays in one task;
  task names are valid DNS labels and differ by cluster, ancestor and incident.
  Against a fake AX gRPC server and a fake router that checks the
  `ate-target-actor` header and 404s unknown sessions.
- `internal/axsink`: cross-cluster TLS + per-RPC `BearerCredentials` (`authorization: Bearer <token>`)
  against a local TLS gRPC server signed by an ephemeral CA and an `https://`
  router (`httptest.NewTLSServer`) carrying readiness, open, inject, and append.
- `internal/axsink` (`hold_test.go`), holding reminders:
  - A reminder to an idle task is held with no `ResumeTask` and no router
    request.
  - A reminder after a partial open is never held.
  - A gone task drops its held reminders and answers 404.
  - A reminder to a busy task goes through.
  - A recent task that AX records as `Suspended` holds.
  - Held reminders fold to the latest, which is delivered ahead of the next
    real inject.
  - A resolution delivers the held reminder first.
  - In cluster scope, a new incident delivers another incident's held
    reminder first.
  - The idle window comes from `spec.idle.suspendAfter`.
  - Stale reminders are dropped, the total held is bounded, and an empty
    record (after a restart) counts as idle.
  - Each outcome is counted.
- `pkg/inject`: the core-agent and webhook sinks deliver every reminder at
  once.
- `internal/watch`: the AX API dialled over TLS with CA cert + per-RPC
  `--ax-token-env` bearer token succeeds, rejects invalid/missing tokens or
  non-TLS combinations with usage errors, rejects untrusted certs without CA,
  and keeps plaintext default working; cluster-local host checks (`*.svc`,
  `*.svc.cluster.local`, loopback) suppress plain-http warnings while external
  plain-http router URLs emit structured warnings without leaking secrets.
- `internal/watch`: the dispatcher opens a `KeyedOpener` with the canonical
  key, storms by ancestor and the watchboard by cluster; flag validation
  matrix and template decoding.
- `pkg/store`: `sink_sessions` round trip, per-cluster isolation, nil store,
  TTL prune.
- End to end on a GKE cluster with Agent Substrate v0.3.0 and the AX fork:
  mast's `gke-triage` workload in a task, reached through the router with
  exactly these calls, diagnosed a crash-looping Deployment, and a request to
  the suspended task woke it and was answered in 1.7s.
