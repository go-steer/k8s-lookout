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

**M4 complete** (see `docs/milestones/M4.md` for the drill evidence;
M3/M2/M1/M0 records sit alongside): all eight §7.2 sources ship —
M3's six plus `capacity` (§10.1: CA events / status-ConfigMap gap /
provider scale decisions, portable halves degrade LOUDLY without a
provider) and the per-PROJECT `quota` source (§10.2: slope-fitted
`quota.forecast` with the §10.3 `quota_increase_draft` attached —
lookout drafts, the agent files through core-agent's permission gate;
a quota-capable provider is a hard startup requirement). §10.3
correlation is APPEND-ONLY dedup family `QuotaExhausted` keyed on
`quota:<NAME>/<SCOPE>` (leading `quota_forecast` + reactive
`quota_blocked` = one session; `capacity.pending`/`pending-aged` join
`FailedScheduling` the same way). Read path adds `cloud
stockout|orphans|ipspace|quota` behind `pkg/cloud` capabilities (the
default build is GCP-free, nm-conformance-tested; gke lives behind
`-tags gke/allproviders`). §9.2 distilled memories and §9.4
triage-status records live in the sentinel store via `pkg/memory`
(migrations v3/v4): routing honors `severity_override`/`escalated`,
recovery flips records to resolved, `health`/`bundle --store` merge
open records into findings. All prior invariants stay frozen (M2
storm/watchboard/resolved pins, M0 payload + flags, §8 additive
attachments). The five M4 drill observations (evidence in
`docs/milestones/M4.md` §Observations) are FIXED post-M4: the §9.4
producer surface is `lookout triage status` / MCP `k8s_triage_status`
(the §4.1 addition decided in `docs/triage-status-write-design.md`;
the `dev/drills/write-triage-status` stand-in is deleted); the
shipped ClusterRole now covers BOTH enrichment read paths (a test in
`pkg/checks/state` parses `deploy/12` against
`LoadClusterListRequirements()`); downgraded incidents that regress
inside a never-expiring dedup window get ONE pinned
`kind=triage.regressed` evidence followup (`--triage-regress-factor`,
default 3x — evidence only, never an auto-re-page); cross-source
dedup joins inject a compact followup into the bound session
(route=followup, max 1 per source family per incident per window;
k8s-event joiners keep the frozen `k8s-event-followup` kind); GHCR
publishes the `-tags allproviders` flavor as
`lookout:<version>-gke` / `latest-gke` for project-tier quota
deployments. M3 observations remain open (`GraphAt` restart replay,
`Snapshot.Watches` round-trip, Deployment-targeted history,
`rollout_stall` resolved fields, sync-Adds); `--storm` default stays
OFF pending the restart fix. GKE replay runbooks:
`dev/drills/node-failure.md`, `dev/drills/bad-deploy.md`,
`dev/drills/memory-leak.md`, `dev/drills/quota-exhaustion.md`.

Next milestone: **M5 — fleet & corpus** (DESIGN.md §14): remaining
reads (`state wi|webhooks|volumes`, `stab drift|drain`, `perf probe`);
fingerprint schema finalized with AX; `token-burn` source; corpus
harvester contract validated end-to-end (§9.3). Exit: AX can rollup a
multi-cluster staged stockout; one harvested labeled trajectory from a
real incident.
