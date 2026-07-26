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

**M3 complete** (see `docs/milestones/M3.md` for the drill evidence;
M2/M1/M0 records sit alongside): the sentinel runs all six §7.2 sources
(`k8s-events`, `object-state`, `rollout`, `saturation`, `degradation`,
`expiry` — each behind a §11 loud RBAC probe), writes every post-dedup
signal to the §9.1 store (`pkg/store`, `--store`, pure-Go SQLite) and the
§6.6 graph history (snapshots + delta log, `--graph-snapshot-interval`),
and the read path answers history: `triage events|top|radius|changes` +
`net probe`, with `--at`/`--store` on every graph-backed command
(`checks.Command.GraphBacked`). Trend payloads carry the §8 `forecast`
attachment (ADDITIVE, wire-pinned); all M2 invariants stay frozen (storm/
watchboard/resolved pins, the M0 `k8s-event` payload and flag surface,
`flags_contract_test.go`). From M1: the read-path core (`triage
delta|logs|spec`, `state edges`, `bundle`, `health`) under the §4.2
envelope + §6.5 sanitizer, `lookout mcp`, `pkg/graph`, and the generated
skills (`dev/tools/gen-skill-refs` after touching command metadata; keep
`internal/skilldoc` contract tests green). Known M3 gaps to pick up
(recorded with evidence in `docs/milestones/M3.md` §Observations):
`store.GraphAt` cannot replay across a sentinel restart; store-restored
snapshots lose the unknown-vs-missing radius fix (`Snapshot.Watches` not
serialized); historical queries cannot target a Deployment (graph holds
them identity-only); `rollout_stall` resolved records bypass the
stability window with inverted duration fields; informer-sync Adds
masquerade as changes. `--storm` default stays OFF pending the restart
fix. GKE replay runbooks: `dev/drills/node-failure.md`,
`dev/drills/bad-deploy.md`, `dev/drills/memory-leak.md`.

Next milestone: **M4 — capacity & quota** (DESIGN.md §14): `capacity` +
`quota` sources (§10); `cloud stockout|orphans|ipspace|quota`; distilled
memories (§9.2); triage-status records (§9.4) + store/memory-merged
`health`; the quota write path behind the §12 permission gate. Deferred
further out: `state wi|webhooks|volumes`, `stab drift|drain`,
`perf probe`, `token-burn`, corpus harvester (M5).
