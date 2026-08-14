# Repo architecture map

The first document a new maintainer or agent should read for *where
things live*. [`DESIGN.md`](./DESIGN.md) is the normative
specification — when this map and DESIGN.md disagree, DESIGN.md wins
on intent and this map wins on what is actually in the tree (see
[Design vs implementation](#design-vs-implementation-divergences) for
the known gaps). Hard rules for contributors are in
[`AGENTS.md`](../AGENTS.md).

## Layout

```
k8s-lookout/
├── cmd/lookout/          # multicall entrypoint: subcommand dispatch, --help, provider
│                         #   blank-imports behind build tags, nm-based isolation test
├── internal/
│   ├── watch/            # the sentinel wiring (`lookout watch`): flag surface, dispatcher,
│   │                     #   watchboard, enrichment, recovery/storm/triage dispatch, distiller,
│   │                     #   graph feed — plus the scripted end-to-end drill tests
│   ├── mcpserver/        # `lookout mcp`: MCP tool schemas generated from command metadata
│   └── skilldoc/         # skill references/ generator + ```lookout doc-example contract tests
├── pkg/
│   ├── checks/           # read-path check implementations, one package per group
│   │   │                 #   (delta, logs, events, top, triage, state, stab, perf,
│   │   │                 #   cloudcheck, netprobe, bundle, health) — shared by CLI, MCP,
│   │   │                 #   and in-process enrichment
│   │   ├── command.go    # Command metadata: flags, when-to-use line, output-field glossary
│   │   ├── registry.go   # the metadata registry all generated surfaces read from
│   │   └── checktest/    # §13 contract-test scaffold every command suite runs
│   ├── cloud/            # Provider interface + capability registry; pkg/cloud/gke is the
│   │                     #   only implementation, compiled in via build tags only
│   ├── corpus/           # §9.3 harvester: labeled trajectories from inject captures, no NLP
│   ├── emit/             # output envelope: logfmt/JSON findings, summary line, SANITIZER
│   ├── engine/           # source-agnostic pipeline pieces: filter, dedup, storm, severity
│   │                     #   routing, recovery, fingerprint recipe, quota-draft
│   ├── graph/            # in-memory topology index (COW snapshots) + LKGH history/replay
│   ├── inject/           # agent sinks behind the two-verb Sink interface (core-agent
│   │                     #   daemon client + generic webhook) + FROZEN wire payload types
│   ├── kube/             # client bootstrap (kubeconfig/in-cluster) + informer source glue
│   ├── memory/           # §9.2/§9.4 record types (distilled facts, triage-status) bound
│   │                     #   to the sentinel store until core-agent ships a Memory surface
│   ├── sources/          # §7.2 signal sources, one package each: k8sevents, objectstate,
│   │                     #   rollout, workload, saturation, degradation, expiry, capacity,
│   │                     #   ingress, quota, notifications, tokenburn; rbac.go declares
│   │                     #   per-source RBAC requirements
│   └── store/            # sentinel-local SQLite: occurrences, graph history, records
├── skills/               # workflow skills + playbooks; references/ are GENERATED
├── deploy/               # sentinel manifests: SA, read-only ClusterRole(+Binding),
│                         #   capacity Role(+Binding), Deployment (image-swap compatible)
├── dev/
│   ├── ci/presubmits/    # the exact checks CI runs, individually invokable
│   ├── drills/           # GKE/kind replay runbooks (node-failure, bad-deploy, memory-leak,
│   │                     #   quota-exhaustion) + stub-daemon.py capture sink
│   └── tools/            # ci (runs all presubmits), build, lint-go, test-unit,
│                         #   gen-skill-refs, harvest-corpus, add-license-headers, …
├── examples/             # runnable e2e: kind recipe, sentinel + capture stub, demo
│                         #   workloads, inject/verify/revert failure scenarios, e2e driver,
│                         #   agent-harness (skills/MCP) testing guide
└── docs/                 # DESIGN.md (normative), design notes, milestones/, this map
```

## The two data paths

Read-path — one implementation, three invocation surfaces (§4.3):

```
            CLI (cmd/lookout) ─┐
     MCP (internal/mcpserver) ─┼─→ pkg/checks ─→ pkg/emit envelope + sanitizer ─→ stdout / MCP response
enrichment (internal/watch) ───┘        │              (summary line, exit 0)      / inject attachment
                                        ├── pkg/graph (radius, edges, changes, --at history)
                                        └── pkg/sources/saturation (checks/top only: the shared
                                            metrics fetcher/sample seam — deliberate §10.2 crossing
                                            so the metrics-client join exists exactly once)
```

Watch-path — the sentinel pipeline (§7.1), wired in `internal/watch`:

```
pkg/sources (11) ─→ filter ─→ dedup ─→ storm ─→ severity ─→ enrichment ─→ pkg/inject ─→ sink:
                  └────────────── pkg/engine ──────────┘   (pkg/checks,     (Sink)     daemon sessions
                                     │                      in-process)                | webhook /incidents
        pkg/store (occurrences, graph history, §9.2/§9.4 records via pkg/memory)
        pkg/graph (blast-radius keys for storm correlation; snapshots into the store)
```

Recovery closes the loop: the dispatcher watches bound incidents'
symptoms clear and injects `resolved`/`resolved.reverted` into the same
session — which is what makes the §9.3 corpus (`pkg/corpus`)
harvestable with ground-truth labels.

## Frozen contracts and their guardian tests

Violating any of these is a bug, not a test update
(AGENTS.md hard rules):

| Contract | Lives in | Guardian test |
| --- | --- | --- |
| `lookout watch` flag surface (M0 image-swap compat) | `internal/watch/main.go` | `internal/watch/flags_contract_test.go` (`TestFlagSurfaceFrozen`; additive flags get their own pin tests) |
| Inject wire payloads, incl. byte-frozen M0 `k8s-event`/`k8s-event-followup` pair (re-baselined once 2026-07-27: + `type` field — dated pre-consumer amendment, `signal-schema-v1.md` §Amendments) | `pkg/inject/injector.go` | `pkg/inject/schema_freeze_test.go` (field-set ledger, 32-kind inventory, byte-exact round-trip) |
| Webhook sink wire (`POST /incidents` + `/incidents/<id>/events`, schema-v1 payload as the body — never the daemon envelope; re-pinned 2026-07-27 with the `type` amendment) | `pkg/inject/webhook.go` | `internal/watch/webhook_dispatch_test.go` (byte-exact open/append pins: k8s-event, resolved, storm, watchboard.digest, watchboard.rotated) |
| Signal-schema v1 ledger (the fleet-rollup contract) | [`signal-schema-v1.md`](./signal-schema-v1.md) | same freeze tests; removal/rename is a v2 negotiation with fleet consumers, additions extend ledger + doc together |
| engine↔inject kind constants stay one value | `pkg/engine/signal.go`, `pkg/inject` | `internal/watch/signal_contract_test.go` |
| Fingerprint recipe (the §8/§9.4 join key; recipe untouched — the reason-class INPUT for pull-shaped `BackOff`/`Failed` events was corrected 2026-07-27, `signal-schema-v1.md` §Amendments) | `pkg/engine/fingerprint.go` | `pkg/engine/fingerprint_test.go` + `TestFingerprintParity_PushAndScan` (`internal/watch/m5_corpus_rollup_test.go`) |
| LKGH graph-snapshot format (magic + version byte + gzip body) | `pkg/graph/history.go` | `pkg/graph/history_test.go` (golden v1 blob, `TestRestore_V1Compat`) |
| Sanitizer: no secret value on any surface | `pkg/emit/sanitize.go` | `pkg/emit/sanitize_test.go` golden fixtures (`pkg/emit/testdata/sanitize/`) — an unmasked credential fixture fails CI |
| Shipped ClusterRole covers the enrichment read paths | `deploy/12-clusterrole-watcher.yaml` | `pkg/checks/state/rbac_test.go` parses the manifest against `LoadClusterListRequirements()` |
| Command output schema ↔ declared metadata | each `pkg/checks/*` | `pkg/checks/checktest.VerifyContract` in every command's suite |
| Bundle section identifiers/order across CLI and enrichment ("the enrichment bundle IS a bundle") | `pkg/checks/bundle/bundle.go` + `internal/watch/enrich.go` | `internal/watch/enrich_bundle_contract_test.go` (`TestBundleSectionContractFrozen`; pins both heads' `sections=` joins and both bodies' emission order) |

## Provider boundary and build tags

`pkg/checks` and `pkg/sources` never import GCP SDKs; cloud-touching
functionality asks `pkg/cloud` for capabilities (metrics, quota,
workload-identity, stockout, orphans, ipspace, …).

- **default build (no tags)** — vanilla Kubernetes, ZERO cloud SDK
  linkage. Provider-gated commands/sources emit an explicit
  `unavailable` finding (never silence, never a crash);
  `--sources=…,quota` refuses loudly at startup.
- **`-tags gke` / `-tags allproviders`** — compiles in `pkg/cloud/gke`
  via the blank import in `cmd/lookout/providers_gke.go`. Release
  images: `ghcr.io/go-steer/lookout:<v>` (default) and `:<v>-gke`
  (`allproviders`), same Dockerfile, only `BUILD_TAGS` differs.
- The isolation is *tested*, not aspirational:
  `cmd/lookout/nogcp_default_test.go` builds the default binary and
  scans its symbol table (`go tool nm`) for GCP package markers;
  `pkg/cloud/conformance_default_test.go` and the
  `providers_{default,tagged}_test.go` pair pin registration behavior
  on both sides of the tag.

## Generated outward — one source of truth

Command metadata declared in `pkg/checks` (`Command`: name, flags,
when-to-use line, output-field glossary) generates three surfaces:

1. `--help` — `pkg/checks/help.go`, rendered by the multicall binary;
2. MCP tool schemas — `internal/mcpserver/schema.go`, every command
   1:1 as an MCP tool;
3. skill `references/` — `internal/skilldoc` via
   `dev/tools/gen-skill-refs` (committed output; a drift test in
   `internal/skilldoc` fails CI when metadata changes without a
   re-run).

Drift gates on the docs themselves: every fenced ```` ```lookout ````
block under `skills/` must parse against the real registry
(`internal/skilldoc/examples_test.go`), and every ```` ```lookout-golden ````
snippet must appear verbatim in a checktest golden fixture — skill docs
cannot silently drift from tool output.

## Testing conventions map (DESIGN.md §13)

| §13 convention | Where it lives |
| --- | --- |
| `fake.Clientset` seeded fixtures, exact findings + summary line | every `pkg/checks/*` and `pkg/sources/*` suite |
| command contract tests (schema round-trip both formats) | `pkg/checks/checktest` (`VerifyContract`), used by every command |
| sanitizer golden files (secrets in every known position) | `pkg/emit/testdata/sanitize/*.golden` |
| topology index: synthetic-cluster generator, property tests, bench gates | `pkg/graph/{synthetic,property,bench}_test.go`; gate criteria in [`graph-q5-gate.md`](./graph-q5-gate.md) |
| graph history round-trip (snapshot + replay ≡ live) | `pkg/graph/history_test.go` |
| GCP checks: recorded fixtures behind small client interfaces | `pkg/cloud/gke/*_test.go`; no live-project tests in CI |
| trend sources: synthetic time series, ETA math, no-forecast case | `pkg/sources/{saturation,degradation,quota,tokenburn}` suites |
| pipeline drills: scripted signal streams through the REAL dispatcher | `internal/watch/*_dispatch_test.go`, `m5_corpus_rollup_test.go` (storms, recovery, routing, quota, corpus, rollup) |
| doc-example validation | `internal/skilldoc/examples_test.go` |
| live-cluster replay drills (human-run, never CI) | `dev/drills/*.md` runbooks + `stub-daemon.py` |
| live-cluster e2e scenarios (local kind / GKE staging; in CI only non-blocking — post-merge smoke + weekly full via `e2e-kind.yml`) | `examples/e2e` + `examples/scenarios/*` (inject/verify/revert against the wire capture + read-path) |

CI: `dev/tools/ci` runs the same steps as `.github/workflows/ci.yml`
(format → vet → build → lint → mod-tidy → test → vuln), individually
invokable via `dev/ci/presubmits/`. `ci-docs.yml` green-stubs the
required checks for docs-only changes.

## Design vs implementation divergences

Where the tree deliberately differs from DESIGN.md, in one place:

- **Layout:** the §2 tree predates M1+. Not in it but real:
  `internal/{watch,mcpserver,skilldoc}` (the sentinel wiring and
  serving layers live under `internal/`, not `cmd/lookout/`),
  `pkg/memory`, `pkg/corpus`, `pkg/checks/checktest`.
- **Build tags:** §2 sketched an optional `nogcp` tag on a
  GCP-linked default. Implementation inverted it: the default build
  is GCP-free and providers are opt-in (`gke`/`allproviders`).
- **§9.2 Memory interface:** DESIGN says records travel "through the
  core-agent Memory interface" — that surface does not exist in
  core-agent v2.7.0, so `pkg/memory` binds the record types to the
  sentinel's SQLite store in-tree (`TODO(core-agent)` marks the
  migration point).
- **`triage status`** is a §4.1 *extension* (the §9.4 producer
  surface) decided in
  [`triage-status-write-design.md`](./triage-status-write-design.md),
  not in the original command table; still awaiting maintainer
  sign-off.
- **`--storm` default** — design ships storm correlation as standard
  behavior; it shipped default-OFF through 0.8.0 (M3 `GraphAt`
  restart-replay observations). Divergence closed by the auto-defaults
  change: `--storm=auto` (with `--sources=auto`) now resolves ON
  whenever the graph grants are present; the milestone notes' "stays
  OFF" lines are point-in-time. The `GraphAt` restart-replay item was
  FIXED by #55 (store epoch ids, migration v5; regression tests in
  `pkg/store/history_test.go`) two days before the default flip — the
  M3/M5 milestone "remains open / stays OFF pending" lines were
  written stale the same morning #55 merged and are point-in-time,
  not current state.
- **`k8s-capacity` skill** (§4.4 tree): shipped post-M5 alongside the
  docs-site Guides section — divergence closed (the M5 record's
  "never shipped" note is point-in-time).
- **Watchboard rotation** (§15 Q2) settled as size-based —
  [`watchboard-rotation-design.md`](./watchboard-rotation-design.md).
- **`perf probe` portability** (§15 Q4): only the Cloud Monitoring
  backend exists, so the packs are GKE-only in practice.
- **Scan-side fingerprints are zone-less by design**: the sentinel
  stamps §8 zone/project (explicit `--zone`/`--project` flag >
  provider metadata via `cloud.Identity` > empty) and hashes the
  zone into fingerprints, but a point-in-time scan carries no
  deployment identity, so `ScanFingerprint` callers pass `zone=""`.
  Deployments that stamp nothing keep zone-less fingerprints — still
  stable, and push/scan self-consistent; the §9.4 join tolerates the
  mismatch via its resource-key pin (`pkg/memory/join.go`).

## Further pointers

- [`DESIGN.md`](./DESIGN.md) — normative spec; records *rejected*
  alternatives with reasons. Do not re-propose without new evidence.
- Design notes: [`signal-schema-v1.md`](./signal-schema-v1.md),
  [`triage-status-write-design.md`](./triage-status-write-design.md),
  [`watchboard-rotation-design.md`](./watchboard-rotation-design.md),
  [`agent-sink-design.md`](./agent-sink-design.md),
  [`graph-q5-gate.md`](./graph-q5-gate.md),
  [`fleet-audit-detectors-design.md`](./fleet-audit-detectors-design.md)
  (proposal — re-basing the fleet audit onto deterministic `checks`),
  [`audit-ingestion-contract.md`](./audit-ingestion-contract.md)
  (its consumer seam: `emit.Finding` → an audit ledger).
- [`milestones/`](./milestones/) — M0–M5 completion records with
  exit-check evidence and the post-M5 review backlog (M5.md).
- [`appendix-v2-dataplane-intelligence.md`](./appendix-v2-dataplane-intelligence.md)
  — historical only; nothing in it is normative.
