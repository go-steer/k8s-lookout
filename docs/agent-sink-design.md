# Agent sink — the watch-path behind two verbs, decided

DESIGN.md §3 names the core-agent daemon as the watch-path's delivery
boundary: `POST /sessions`, `POST /sessions/<sid>/inject`. That
coupling is thinner than it looks. This note settles what
`lookout watch` actually *requires* of an agent runtime, and how a
deployment without a core-agent daemon receives the sentinel's
signals.

**Decision: a `Sink` interface in `pkg/inject`, two implementations.**
The existing core-agent HTTP client becomes the default `Sink`,
wire-identical to today (the frozen pins are the regression proof); a
new webhook sink delivers the same schema-v1 payloads to any HTTP
receiver. Selection is additive: `--sink=core-agent|webhook`,
defaulting to `core-agent` — existing deployments are unchanged.

This note records the settled decisions. The payload contract stays
where it is ([`signal-schema-v1.md`](./signal-schema-v1.md), frozen);
nothing here touches the wire *shapes*, only where they are POSTed.

## Settled decisions

### The two-verb contract

Strip away everything the sentinel does internally and the watch-path
asks an agent runtime for exactly two verbs:

1. **Open an incident context** — returns an opaque id.
2. **Append to it** — followups, storm records, outcomes, digests.

Plus one *optional* third, used by a single source:

3. **A usage query** — the token-burn source (§12) reads the
   runtime's cost stack. No other source touches it.

Everything else — dedup windows, storm correlation, severity routing,
recovery tracking, enrichment — is sentinel-internal and
runtime-agnostic behind those verbs. core-agent's `POST /sessions` /
`POST /sessions/<sid>/inject` pair is one instantiation of the
contract; it is not the contract.

### `Sink` in `pkg/inject`; core-agent stays the default

`pkg/inject` grows a `Sink` interface expressing the verbs above; the
existing `Injector` (the daemon HTTP client) is the default
implementation, **wire-identical** to today: same endpoints, same
inject-message envelope, same `Authorization` / `X-Asserted-Caller`
headers. This is a seam extraction, not a rewrite — the compatibility
argument is that the frozen pins already prove it:

- `TestDispatcher_ExactInjectPayloadWireShape` (`internal/watch`)
  pins the M0 `k8s-event` / `k8s-event-followup` bodies
  byte-for-byte;
- `pkg/inject/schema_freeze_test.go` pins every payload struct's
  ordered field list and round-trips it losslessly;
- `TestFlagSurfaceFrozen` (`internal/watch/flags_contract_test.go`)
  pins the flag surface; the new flags are additive and get their own
  pin tests, per the standing convention.

A refactor that moved a byte on the core-agent wire fails CI before
review sees it.

Flag surface (additive):

| Flag | Default | Meaning |
| --- | --- | --- |
| `--sink` | `core-agent` | Which sink delivers signals: `core-agent` (the daemon client, today's behavior) or `webhook`. |
| `--sink-url` | (none) | Webhook receiver base URL, no trailing slash; required with `--sink=webhook`. |
| `--sink-token-env` | (none) | Name of the env var holding the Bearer token the webhook sink sends. |

`--daemon-url` / `--token-env` / `--owner` remain the core-agent
sink's configuration, untouched.

**Migration: none needed.** The default is `core-agent`; a deployment
that never passes `--sink` runs the exact code path and wire bytes it
runs today.

### The webhook wire contract

Two endpoints, mirroring the two verbs:

- **Open**: `POST <url>/incidents` with a schema-v1 payload as the
  JSON body (the opening signal rides the open) → any 2xx with
  `{"id":"<opaque>"}`.
- **Append**: `POST <url>/incidents/<id>/events` for followups,
  outcomes, and digests — same body shape, any 2xx.

The body is the schema-v1 payload itself — the same JSON the
core-agent sink stringifies into its `{"message":"…"}` inject
envelope, unwrapped. Receivers parse `signal-schema-v1.md` payloads
directly; nothing webhook-specific is added to or removed from them.

- **Auth**: `Authorization: Bearer <value of --sink-token-env>` on
  every request.
- **Retry/backoff: identical to the core-agent sink.** Delivery
  failure posture is a property of the dispatcher, not the sink — an
  error is logged, counted in `inject_errors_total`, and the signal
  re-fires through dedup's retry cooldown, exactly as core-agent
  delivery failures do today. The webhook sink adds no retry loop of
  its own.
- **Receivers MAY be stateless.** A receiver is free to ignore the
  ids it hands out (return a constant, log the body, move on) —
  lookout still sequences correctly: opens precede appends, and every
  payload carries its own identity (`fingerprint`, object
  coordinates, kind). Correlation into per-incident threads is the
  receiver's *opportunity*, not its obligation.
  [`dev/drills/stub-daemon.py`](../dev/drills/stub-daemon.py) is the
  existence proof that a useful receiver fits in a page of stdlib
  Python.

### token-burn requires the core-agent sink

The third verb — the usage query — is core-agent's cost-stack API
(§3, §12); the webhook contract deliberately omits it (a generic
receiver has no usage to report). With `--sink=webhook`, a
`--sources=…,token-burn` deployment starts, and the source **idles
loudly**: the existing §11-style startup message pattern, naming the
source and the reason it will emit nothing — never a silent empty
watch.

## Out of scope

- **Per-framework adapters** (a LangChain sink, a CrewAI sink, …).
  Same reasoning as §15 Q4's Prometheus-backend deferral: the
  no-speculative-surface rule — deferred until a concrete consumer
  exists. The webhook contract is deliberately the smallest thing any
  framework can terminate; an adapter is a receiver-side afternoon,
  not a lookout release.
- **Acknowledgement semantics beyond HTTP status.** 2xx is delivered,
  anything else is not; no receipts, no consumer offsets, no
  read-back of agent state through the sink.
- **Multi-sink fanout.** One sink per sentinel. A deployment that
  wants both a daemon and a capture stream puts a fanout behind one
  webhook URL — receiver-side composition, not sentinel
  configuration.
