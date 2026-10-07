# ax sink — one Agent Executor task per incident

[`agent-sink-design.md`](./agent-sink-design.md) reduced the watch path to
two verbs, open an incident and append to it, and gave lookout two `Sink`
implementations: the core-agent daemon and a generic webhook. This note adds
a third, `--sink=ax`, which runs each incident in its own
[Agent Executor (AX)](https://github.com/google/ax) task on Agent Substrate.
Tracking issue: #567.

**Decision: the ax sink creates (or finds) an AX task for each incident and
speaks the core-agent session API to the agent inside it, through Substrate's
router.** Nothing about the payloads changes; like the webhook sink, this is
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
| `OpenIncident(payload)` | Create the task from the template (an existing task with that name is reused), `ResumeTask`, wait for the task to be ready, then `POST /sessions` and `POST /sessions/<sid>/inject` with the payload as the message. Returns `<task>/<sid>`. |
| `Append(id, payload)` | `POST /sessions/<sid>/inject`, routed to `<task>`. A suspended task is resumed by the router before the request is forwarded. |
| `CreateSession()` (`SessionOpener`) | Create and resume a fresh task, then `POST /sessions`. Keeps the watchboard rotation's frozen wire order. |

The session calls are made by the existing core-agent `Injector`, given an
HTTP client whose transport adds `ate-target-actor: <atespace>/<task>`. The
bodies and headers are therefore byte-identical to the core-agent sink; only
the URL (the router) and that header differ.

### Task names come from the incident

The task name is `lookout-` plus the first 16 hex digits of the SHA-256 of the
payload's `fingerprint` (falling back to `uid/reason`, then the whole
payload). A retried open, or the same incident firing again after the dedup
cooldown, reaches the same task, because AX rejects a second task with the
same name and the sink treats that as "use it". Follow-ups always reach the
agent that already has the context.

### Waiting for the agent

AX reports a task resumed once its workspace is ready, which can be a moment
before the agent's own server is listening. After `ResumeTask`, the sink polls
`GET /readyz` through the router until it answers 200 (bounded by a two-minute
start timeout). With AX's runner pass-through, the runner only reports ready
once the agent's server accepts connections.

### The task template

`--ax-task-template` is an AX `Task` manifest: image, command, egress rules,
injected credentials, `http.port`, workspaces. The sink sets
`metadata.name` per incident and never changes anything else.
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
with them, it built the path and finished the diagnosis.

### Flags

| Flag | Meaning |
|---|---|
| `--sink=ax` | Select this sink. |
| `--ax-server` | AX API address (plaintext gRPC), e.g. `ax-server.ax-system.svc:8080`. Required. |
| `--ax-task-template` | Path to the Task manifest. Required. |
| `--ax-router-url` | Substrate router, default `http://atenet-router.ate-system.svc.cluster.local`. |
| `--token-env`, `--owner` | Same meaning as for the core-agent sink, applied to the agent's session API inside each task. |

`--mode=shared`, `--target-session` and `--daemon-url` are rejected with this
sink. The `ax-*` flags are rejected with the other sinks.

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
  (`mastersingh24/ax:task-egress-credentials`) and will be proposed upstream.
  The same fork lets a task declare egress and injected credentials.
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
- **One long-lived task per cluster.** Possible later for the warning-level
  watchboard; per-incident tasks are the starting point.

## How it was tested

- `internal/axsink`: open, append, reopen-reuses-task, readiness wait and id
  parsing, against a fake AX gRPC server and a fake router that checks the
  `ate-target-actor` header.
- `internal/watch`: flag validation matrix and template decoding.
- End to end on a GKE cluster with Agent Substrate v0.3.0 and the AX fork:
  mast's `gke-triage` workload in a task, reached through the router with
  exactly these calls, diagnosed a crash-looping Deployment, and a request to
  the suspended task woke it and was answered in 1.7s.
