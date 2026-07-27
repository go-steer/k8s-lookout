---
title: "Architecture: two paths, one binary"
description: The read-path (one-shot diagnostic commands) and the watch-path (the resident sentinel) share one binary, one output contract, and one check implementation.
sidebar:
  order: 1
---

This page explains how lookout is put together: two ways in — one-shot
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

One-shot diagnostic commands an agent (or a human) runs mid-investigation:
`triage`, `state`, `stab`, `perf`, `cloud`, `net`, plus the composed
`bundle` and `health`. All of them share a single check implementation,
consumed by three invocation surfaces:

```
             CLI (lookout <command>) ──┐
             MCP  (lookout mcp) ───────┼──→ shared checks ──→ sanitizer + output envelope ──→ stdout / MCP response
  enrichment (inside lookout watch) ───┘         │                                             / inject attachment
                                                 └──→ topology graph (radius, edges, changes, --at)
```

- **CLI** — for deployments where the agent has a shell.
- **MCP** — `lookout mcp` serves every registered command 1:1 as an MCP tool
  (stdio or loopback HTTP), for distroless daemons that cannot shell out.
- **In-process enrichment** — the sentinel calls the same checks directly to
  pre-warm incident sessions; no fork/exec, shared informer cache.

Whatever the surface, the output contract is identical: one finding per line
(logfmt by default, `--format=json` for JSON-per-line), keys in fixed order,
healthy resources silent, a mandatory final summary line. Exit `0` is pure
payload on stdout; exit `1` puts diagnostics on stderr only, so a captured
stream never corrupts a context window; exit `2` is a usage error.

## The watch-path

`lookout watch` is a resident per-cluster **sentinel**: pluggable signal
sources feeding one pipeline, ending in injects to `core-agent` sessions.

```
sources ──→ filter ──→ dedup ──→ storm correlation ──→ severity routing ──→ enrichment ──→ inject ──→ core-agent sessions
```

The sources (enabled per deployment via `--sources`) cover reactive and
leading classes: `k8s-events` (the reactive baseline), `object-state`
(transitions: node flaps, emptied endpoints, gridlocked PDBs), `rollout`
(stalls caught while the old revision still serves), `saturation` (slope →
ETA forecasts), `degradation` (ready-ratio trends), `expiry` (certificate
countdowns), `capacity` and `quota` (cluster-autoscaler and project-quota
seams), and `token-burn` (agent spend as a saturation dimension). The full
flag surface is in [Reference → lookout watch](/reference/watch/); every
signal kind is cataloged in
[Reference → Signal kinds](/reference/signal-kinds/).

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
explicitly out of scope here — lookout deploys per cluster (or per project,
for the quota source), and cross-cluster rollup joins signals, not graphs.
