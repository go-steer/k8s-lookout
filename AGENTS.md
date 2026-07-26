# AGENTS.md — k8s-lookout

Instructions for AI agents working in this repository.

## Start here

Two documents, in order:

1. [`docs/DESIGN.md`](./docs/DESIGN.md) — the complete, settled
   specification (tool matrix, sentinel architecture, signal schema,
   storage, phase plan). It records *rejected* alternatives with reasons
   (§2, §6.2, §6.6, §9.4); do not re-propose a rejected design without
   new evidence. `docs/appendix-v2-dataplane-intelligence.md` is
   historical only — nothing in it is normative.
2. [`docs/repo-map.md`](./docs/repo-map.md) — the repo architecture
   map: where everything lives, the two data paths, the frozen
   contracts and their guardian tests, the build-tag scheme, the
   generated-doc machinery, the testing-conventions map, and the known
   design-vs-implementation divergences. Read DESIGN.md for the spec,
   repo-map.md for the tree.

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
  from core-agent); `dev/tools/add-license-headers` for new shell/YAML/Python
  files. Markdown files carry no header.
- Run `dev/tools/ci` before pushing — the same checks CI runs
  (`dev/ci/presubmits/` holds them individually).

## Current state

**Implementation complete through M5 (v0.6.0)** — the §14 phase plan
is finished; post-M5 work is the review backlog in
`docs/milestones/M5.md` §Remaining known gaps, not new milestones.
Every §4.1 command and all nine §7.2 sources ship; the signal schema
is FROZEN as v1 (`docs/signal-schema-v1.md` — removing/renaming a
frozen field or touching the fingerprint recipe is a v2 negotiation
with AX, never a test update; additions are additive-only and extend
the ledger + doc in the same change). The full frozen-contract →
guardian-test table is in `docs/repo-map.md`; per-milestone evidence
and archaeology live in `docs/milestones/M0–M5.md`.

Key open items (full list in M5.md): the real-incident trajectory +
real AX integration + real-GCP drills (human steps; runbooks in
`dev/drills/`), core-agent's shared Memory surface and
CostCeiling-on-/usage TODOs, the `triage status` §4.1 extension
awaiting review, `health`'s control-plane category not yet delegating
to the perf packs, the `k8s-capacity` skill, and the M3 graph-history
observations (`--storm` default stays OFF pending the restart fix).
