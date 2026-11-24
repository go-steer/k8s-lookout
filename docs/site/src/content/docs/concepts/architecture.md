---
title: "Architecture: two paths, one binary"
description: The read-path (one-shot diagnostic commands) and the watch-path (the resident sentinel) share one binary, one output contract, and one check implementation.
sidebar:
  order: 1
---

This page explains how `lookout` is put together: two ways in — one-shot
commands you run mid-investigation, and a resident watcher that lives in
the cluster — and why both share a single binary. If you have wondered
why the output is so terse, or why a healthy resource prints nothing,
the answer is one of a handful of principles everything else follows
from:

- **Token density.** Raw telemetry costs money, evicts context, and slows
  the loop. Deterministic pre-compression — dedup by template, strip nominal
  state — sits between every high-volume source and the context window.
- **Determinism.** A compiled graph traversal does not hallucinate an
  EndpointSlice. Checks are exact; the model reasons over their output, never
  re-derives it.
- **Fewer round trips.** One dense, correlated payload beats five sequential
  tool calls. This pushes toward fewer, wider tools (`lookout bundle`,
  `lookout health`) and pre-warmed incident sessions.
- **Leading indicators over autopsies.** A Kubernetes Event is the autopsy.
  State transitions, trend slopes, and countdowns run ahead of it — the
  watch-path exists to surface them first.
- **Zero nominal state, never ambiguous silence.** Healthy resources are
  omitted; every invocation ends with an explicit summary line
  (`scanned=412 findings=0 elapsed=1.2s`) so "cluster healthy" is
  distinguishable from "broken invocation".
- **Read-only in the cluster.** Tools inspect and diagnose. The only
  sanctioned write actions — GitOps PRs and quota-increase requests — route
  through the `core-agent` daemon's permission gate, never raw write
  authority.

## The read-path

One-shot diagnostic commands an agent (or a human) runs mid-investigation
or on a standing schedule:
`triage`, `state`, `stab`, `perf`, `cloud`, `net`, `audit`, `findings`,
plus the composed `scan`, `bundle`, and `health`. All of them share a
single check implementation in `pkg/checks` and one `pkg/emit.Writer`
pipeline, consumed by three invocation surfaces:

```mermaid
flowchart TB
  subgraph Surfaces["Invocation Surfaces"]
    direction LR
    CLI["CLI (cmd/lookout)<br/>lookout &lt;group&gt; &lt;cmd&gt;<br/>--kubeconfig / --context / --contexts<br/>(or in-cluster SA autodetect)"]
    MCP["MCP (internal/mcpserver — lookout mcp)<br/>stdio | loopback HTTP (--listen) |<br/>shared bearer (--auth-token-file) |<br/>per-caller Google OAuth (--google-client-id) |<br/>embedded OAuth 2.1 AS (--oauth-issuer) |<br/>workload mTLS (--mtls-listen + --workload-rules)"]
    Enrich["In-Process Enrichment (internal/watch)<br/>--enrich=critical|warning<br/>live pkg/graph + scoped-list fallback"]
  end

  subgraph Core["Shared Check Execution (pkg/checks)"]
    direction LR
    Checks["pkg/checks/*<br/>scan, health, bundle, triage, state,<br/>stab, perf, cloud, net, audit, findings"]
    Deps["Cluster, History &amp; Cloud Seams<br/>pkg/kube (paged + trimmed reads)<br/>pkg/graph + pkg/store (--at, changes, triage_status)<br/>pkg/cloud.Provider (-gke build tag; *.unavailable when absent)"]
  end

  subgraph Emit["Output Pipeline (pkg/emit.Writer + MCP Fitting)"]
    direction TB
    Sanitize["1. Sanitize (SanitizeFinding)<br/>Mask secrets, tokens, keys &amp; PEM blocks on every surface"]
    Vis["2. Visibility (WithVisibility)<br/>Per-caller SSAR filter on store/history rows → hidden=&lt;n&gt;"]
    Stamp["3. Stamp, Fold &amp; Validate<br/>check=&lt;cmd&gt; composition stamp + autopilot_idle_folded"]
    Pol["4. Policy (WithPolicy)<br/>Check enablement + severity override → policy=&lt;name&gt; suppressed=&lt;n&gt;"]
    Ex["5. Exemptions (WithExemptions — pkg/exempt)<br/>Annotate exempt=true + exempt_reason + exempt_expires (never drops) → exempt=&lt;n&gt;"]
    Sum["6. Encode &amp; Summary Line (Writer.Summary)<br/>--format=logfmt|json|findings + mandatory scanned=&lt;n&gt; findings=&lt;n&gt; elapsed=&lt;d&gt;"]
    Fit["7. MCP Response Fitting (fitResponse, --max-response-bytes)<br/>Drop whole lines (tail, or head when NewestLast) + rewrite summary response_* notes"]
  end

  Out["Output Destinations<br/>CLI stdout (exit 0) | MCP tool result (+ --access-log) | Sentinel inject attachment"]

  CLI --> Checks
  MCP --> Checks
  Enrich --> Checks
  Checks <--> Deps
  Checks --> Sanitize --> Vis --> Stamp --> Pol --> Ex --> Sum
  Sum -->|"CLI &amp; enrichment"| Out
  Sum -->|"lookout mcp"| Fit --> Out
```

- **CLI** — for deployments where the agent or operator has a shell.
- **MCP** — [`lookout mcp`](/getting-started/mcp/) serves every registered
  command 1:1 as an MCP tool (selectable via `--profile` and `--tools`)
  over `stdio`, loopback HTTP, authenticated off-host HTTP (shared bearer
  token, per-caller Google OAuth, or `lookout`'s embedded OAuth 2.1
  authorization server), and workload mTLS (`--mtls-listen`).
- **In-process enrichment** — the sentinel calls the same `bundle` check
  directly to pre-warm incident sessions; no fork/exec, shared informer
  cache.

Whatever the surface, the output contract is identical: one finding per line
(logfmt by default, `--format=json` for JSON-per-line, or
`--format=findings` for a JSON array of findings), keys in fixed order,
healthy resources silent, a mandatory final summary line. Exit `0` is pure
payload on stdout; exit `1` puts diagnostics on stderr only, so a captured
stream never corrupts a context window; exit `2` is a usage error.

## The watch-path

`lookout watch` is a resident per-cluster **sentinel**: pluggable signal
sources feeding one pipeline (`internal/watch`), ending in injects to an
agent sink (`core-agent`, `webhook`, or `ax`) and closing the loop on
recovery.

```mermaid
flowchart TB
  subgraph Srcs["Signal Sources (pkg/sources/*)"]
    direction LR
    Auto["Auto-Probed Portable Set (--sources=auto)<br/>k8s-events, object-state, rollout, workload,<br/>autoscaling, saturation, degradation, expiry,<br/>capacity, ingress, gateway"]
    Explicit["Explicit-Only (--sources=...)<br/>quota (project tier),<br/>notifications (Pub/Sub),<br/>token-burn (daemon cost stack)"]
  end

  Classify["1. Classify (classifySignal)<br/>CanonicalKey + PullClassMemo + engine.Fingerprint<br/>RoutingPolicy (--severity) → triage_status override → --alert-excludes"]
  Filter["2. Filter (pkg/engine.Filter)<br/>Namespace/reason gates, count debounces,<br/>FailedScheduling scale-up verdict gate, --settle hold"]
  Dedup["3. Dedup (pkg/engine.DedupCache, --dedup-window)<br/>Suppress duplicates | doubling-schedule repeat followups<br/>Persisted via --dedup-persist"]

  subgraph Route["4. Per-Incident Waterfall: Info → Storm → Warning / Critical (--mode=per-incident)"]
    direction TB
    Info["4a. severity=info (RouteStore)<br/>Stored to occurrences only"]
    Storm["4b. Storm Correlation (engine.StormCorrelator)<br/>--storm=auto|on via pkg/graph blast-radius<br/>+ --storm-mine → kind=storm session"]
    Board["4c. severity=warning (RouteWatchboard)<br/>Ancestor reattach (family.member) or<br/>watchboard.digest (--watchboard-batch / --watchboard-flush / --watchboard-rotate)"]
    Crit["4d. severity=critical<br/>Sibling fold + dedicated incident session"]
  end

  EnrichW["5. In-Process Enrichment<br/>pkg/checks/bundle (--enrich=critical|warning)<br/>for new storm &amp; per-incident opens"]
  Guard["6. Wire Fit &amp; Sanitization<br/>FitTo (--inject-max-bytes) + MaskingSink<br/>on every outbound inject"]

  subgraph Sinks["7. Agent Sinks (pkg/inject.Sink: OpenIncident + Append)"]
    direction LR
    CA["core-agent (--sink=core-agent)<br/>POST /sessions + /sessions/{id}/inject<br/>(--daemon-url)"]
    WH["webhook (--sink=webhook)<br/>POST /incidents + /incidents/{id}/events<br/>(--sink-url, schema-v1 body)"]
    AX["ax (--sink=ax, internal/axsink)<br/>gRPC Task (--ax-server, --ax-task-template)<br/>+ router mTLS (--ax-router-url)"]
  end

  Recov["8. Recovery Loop (engine.RecoveryTracker, --recovery-stable-for)<br/>Per-source ClearanceObserver + Pod/ReplicaSet/Deployment informers<br/>Emits resolved / resolved.reverted → Guard → Append &amp; flips triage_status"]

  DB[("SQLite Store (pkg/store, --store)<br/>occurrences, graph_snapshots, graph_changes,<br/>triage_status, memory_facts, sink_sessions")]

  Auto --> Classify
  Explicit --> Classify
  Classify --> Filter --> Dedup
  Dedup -->|"DedupNew (info)"| Info
  Dedup -->|"DedupNew (warning / critical)"| Storm
  Storm -->|"StormNone (warning)"| Board
  Storm -->|"StormNone (critical)"| Crit
  Dedup -->|"DedupRepeat"| Guard
  Info --> DB
  Storm -->|"StormFormed"| EnrichW
  Storm -->|"StormAttached"| Guard
  Crit --> EnrichW
  Board --> Guard
  EnrichW --> Guard --> Sinks
  Sinks --> Recov
  Recov -->|"DispatchSignal (resolved)"| Guard
  Recov -->|"ResolveTriageStatus"| DB
  DB -.->|"open triage_status"| Classify
  AX <-->|"sink_sessions"| DB
```

*(In `--mode=shared`, watchboard and per-incident enrichment are disabled and every post-dedup signal appends directly to `--target-session`.)*

The sources (enabled per deployment via `--sources`) cover reactive and
leading classes: `k8s-events` (the reactive baseline), `object-state`
(transitions: node flaps, emptied endpoints, gridlocked PDBs), `rollout`
(stalls caught while the old revision still serves), `workload` (failed
Jobs and CronJob schedules that stop producing runs), `autoscaling` (HPA
thrash and ceiling pinning), `saturation` (slope → ETA forecasts),
`degradation` (ready-ratio trends), `expiry` (certificate countdowns),
`capacity` and `quota` (cluster-autoscaler and project-quota seams),
`ingress` and `gateway` (controller/GCLB and Gateway API path failures),
`notifications` (the provider's own upgrade events and security
bulletins), and `token-burn` (agent spend as a saturation dimension).
Placement-skew and GKE `ComputeClass` fallback sweeps live in standalone
[`go-steer/leeway`](https://github.com/go-steer/leeway).
The full flag surface is in
[Reference → `lookout watch`](/reference/watch/); every signal kind is
cataloged in [Reference → Signal kinds](/reference/signal-kinds/).

A sentinel with `--store` also keeps a bounded, TTL'd SQLite file: every
emitted signal with its routing outcome, plus topology snapshots and the
per-delta change log. That store is what powers point-in-time queries
(`--at`) and the triaged-reality merge described in
[The closed loop](/concepts/closed-loop/).

## Why one binary

Earlier designs specced a matrix of per-check binaries; each would have
statically linked client-go and cloud SDKs into a multi-gigabyte image with
dozens of drifting flag surfaces. Instead there is one multicall binary,
`lookout`, busybox-style: one release, one image, one client bootstrap, one
output-envelope implementation — and an agent discovers the entire surface
from one `--help`.

The same discipline applies to documentation: command metadata (name, flags,
when-to-use line, output-field glossary) is declared once and generates
`--help`, the MCP schemas, the skill reference docs in
[`skills/`](https://github.com/go-steer/k8s-lookout/tree/main/skills), and
the [Reference](/reference/) section of this site. Drift tests fail CI when
any generated surface goes stale.

## The contract with core-agent

Three boundaries, all pre-existing: the sentinel posts sessions and injects
to the daemon's HTTP API; the `token-burn` source reads the daemon's cost
stack; and fleet-level tooling consumes the
[frozen signal schema](/concepts/signals-and-fingerprints/). Fleet scope is
explicitly out of scope here — `lookout` deploys per cluster (or per project,
for the quota source), and cross-cluster rollup joins signals, not graphs.
