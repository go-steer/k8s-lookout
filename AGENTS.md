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

**M1 complete** (see `docs/milestones/M1.md` for the exit-check evidence;
`docs/milestones/M0.md` for M0's): the read-path core is in — `triage
delta|logs|spec`, `state edges`, `bundle`, `health`, all registered through
the `pkg/checks` metadata registry (one declaration generates `--help`, the
MCP schema, and the skill reference stubs), executed under the §4.2
envelope with the §6.5 sanitizer on every surface, served over MCP by
`lookout mcp`, and consuming the `pkg/graph` COW topology index (Q5
compaction gate recorded in `docs/graph-q5-gate.md`). The first skills ship
in `skills/` (`k8s-triage`, `cluster-health`, `playbooks/`); their
`references/` are generated — run `dev/tools/gen-skill-refs` after touching
command metadata, and keep `internal/skilldoc`'s contract tests green (they
parse-validate every documented `lookout` command line). From M0: `lookout
watch` is the moved `k8s-event-watcher` (flags frozen by
`internal/watch/flags_contract_test.go`, inject wire shape pinned
byte-for-byte by `TestDispatcher_ExactInjectPayloadWireShape`), and
`ghcr.io/go-steer/lookout` images publish on `v*` tags.

Next milestone: **M2 — closed loop** (DESIGN.md §14): recovery injects
(§7.4), storm correlation via graph blast-radius keys (§7.5), severity
routing (§7.7), enrichment via the in-process bundle (§7.6), and the
`object-state` source. Deferred from M1 scope: `triage
events|radius|changes`, `net probe` (M3); the control-plane health
category and the store/memory-merged `health` (M4); `--incident`
enrichment pre-warming (M2).
