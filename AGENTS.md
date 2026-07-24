# AGENTS.md — k8s-lookout

Instructions for AI agents working in this repository.

## Start here

Read [`docs/DESIGN.md`](./docs/DESIGN.md) before proposing or changing
anything. It is the complete, settled specification (tool matrix, sentinel
architecture, signal schema, storage, phase plan) and it records *rejected*
alternatives with reasons (§2, §6.2, §6.6, §9.4). Do not re-propose a rejected
design without new evidence. `docs/appendix-v2-dataplane-intelligence.md` is
historical only — nothing in it is normative.

## What this project is

One multicall Go binary, `lookout`, giving `core-agent`-driven troubleshooting
agents deterministic, token-dense reads of Kubernetes/GKE clusters
(read-path) plus a resident per-cluster signal engine, `lookout watch`, that
opens agent incident sessions from leading indicators (watch-path).

## Hard rules (from the design; violations are bugs)

- **Output contract (DESIGN.md §4.2):** stdout is pure payload + a final
  summary line (`scanned=N findings=N elapsed=D`); diagnostics go to stderr
  only; exit 0 data / 1 runtime / 2 usage; every command honors `--timeout`.
- **Sanitization:** every output surface passes the `pkg/emit` sanitizer; no
  secret value may ever reach stdout, an MCP response, or an inject. Golden
  tests enforce this — extend them when adding output paths.
- **Provider boundary:** `pkg/checks` and `pkg/sources` never import GCP SDKs;
  cloud functionality goes through `pkg/cloud.Provider`. Absent provider →
  explicit `unavailable` finding, never silence.
- **Back-compat:** inject kinds `k8s-event` / `k8s-event-followup` and the
  shipped `InjectPayload` field names are frozen (playbooks pattern-match
  them). New kinds are `source.event`-namespaced.
- **Scope boundaries:** fleet-level coordination is AX's job; `core-agent`
  stays k8s-agnostic and never imports this module. Signals carry
  `fingerprint`/`cluster`/`project`/`zone` so AX can roll up without parsing.
- **Skills/playbooks live here** (`skills/`), never in core-agent — they
  version with tool flags and output formats. Skills map to workflows, not to
  commands (DESIGN.md §4.4).

## Conventions

- Mirror `core-agent` conventions (package layout, `dev/` tooling, milestone
  discipline); a maintainer of one repo should recognize the other. Fixes
  should be one-line ports between them.
- Testing per DESIGN.md §13: `fake.Clientset` fixtures for k8s, recorded
  fixtures behind small interfaces for GCP, contract tests on every command's
  output schema. No live-cluster or live-project tests in CI.
- Apache 2.0 license headers on Go source files (match the files migrated
  from core-agent).
- Run the presubmit scripts (once `dev/ci/presubmits/` lands, mirroring
  core-agent) before pushing; they are the same checks CI runs.

## Current state

Design complete (DESIGN.md v3.5). Implementation not started. Next milestone:
**M0** (DESIGN.md §14) — move `cmd/k8s-event-watcher` from `core-agent`
verbatim as `lookout watch` (flags preserved, behavior-identical), extract
`pkg/{kube,engine,inject,emit}`. The core-agent side (deleting the watcher,
dropping `k8s.io/*` deps) is a separate PR in that repo.
