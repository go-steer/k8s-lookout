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
│   ├── axsink/           # AX agent sink implementation (task lifecycle + fast-path delivery)
│   ├── axapi/            # Generated AX wire types
│   ├── watch/            # the sentinel wiring (`lookout watch`): flag surface, dispatcher,
│   │                     #   watchboard, enrichment, recovery/storm/triage dispatch, distiller,
│   │                     #   graph feed — plus the scripted end-to-end drill tests
│   ├── fleet/            # multi-cluster resolver shared by watch and mcp: --clusters,
│   │                     #   --clusters-from scopes (projects, fleets, folders, orgs), kubeconfig
│   ├── mcpserver/        # `lookout mcp`: MCP tool schemas generated from command metadata
│   ├── telemetry/        # OTel bootstrap for --otel-exporter (propagator always, exporter
│   │                     #   on opt-in); in-tree so no agent framework rides in for it
│   ├── sitedoc/          # Starlight reference docs generator + drift test
│   └── skilldoc/         # skill references/ generator + ```lookout doc-example contract tests
├── pkg/
│   ├── checks/           # read-path check implementations, one package per group
│   │   │                 #   (scan, delta, logs, events, top, triage, state, stab, perf,
│   │   │                 #   cloudcheck, netprobe, bundle, health, findings, audit)
│   │   │                 #   — shared by CLI, MCP, and in-process enrichment
│   │   ├── command.go    # Command metadata: flags, when-to-use line, output-field glossary,
│   │   │                 #   and the ledger of finding kinds the command may emit
│   │   ├── registry.go   # the metadata registry all generated surfaces read from; also
│   │   │                 #   KindGlossary(), the finding-kind union across every command
│   │   ├── crd/          # discovery-gated unstructured reader seam (state gateway, keda, kcc)
│   │   └── checktest/    # §13 contract-test scaffold every command suite runs
│   ├── cloud/            # Provider interface + capability registry; pkg/cloud/gke is the
│   │                     #   only implementation, compiled in via build tags only
│   ├── corpus/           # §9.3 harvester: labeled trajectories from inject captures, no NLP
│   ├── emit/             # output envelope: logfmt/JSON findings, summary line, SANITIZER
│   ├── exempt/           # --exemptions loader: reviewed, expiring opt-outs that ANNOTATE
│   │                     #   findings (via the emit Writer seam) rather than drop them
│   ├── engine/           # source-agnostic pipeline pieces: filter, dedup, storm, severity
│   │                     #   routing, recovery, fingerprint recipe, quota-draft
│   ├── graph/            # in-memory topology index (COW snapshots) + LKGH history/replay
│   ├── inject/           # agent sinks behind the two-verb Sink interface (core-agent
│   │                     #   daemon client + generic webhook) + FROZEN wire payload types
│   ├── kube/             # client bootstrap (kubeconfig/in-cluster), informer trimming + paging
│   ├── memory/           # §9.2/§9.4 record types (distilled facts, triage-status) bound
│   │                     #   to the sentinel store until core-agent ships a Memory surface
│   ├── policy/           # four-layer posture policy engine + built-in profiles
│   │                     #   (default, strict, onboarding, gke-autopilot) for `lookout audit`
│   ├── sources/          # §7.2 signal sources, one package each: k8sevents, objectstate,
│   │                     #   rollout, workload, autoscaling, saturation, degradation, expiry,
│   │                     #   capacity, ingress, gateway, quota, notifications, tokenburn;
│   │                     #   rbac.go declares per-source RBAC rules
│   └── store/            # sentinel-local SQLite: occurrences, graph history, records
├── skills/               # workflow skills + playbooks; references/ are GENERATED
├── deploy/               # canonical deployments: base/ sentinel, composable components/*
│                         #   (sink-ax, no-secrets, scope-namespaced, scope-namespaced-strict,
│                         #   probe-from), mcp/ Gateway server, prometheus-operator/, and chart/
├── deploy-*/             # DEPRECATED (#852): thin Kustomize compatibility aliases to deploy/*
├── dev/
│   ├── ci/presubmits/    # the exact checks CI runs, individually invokable
│   ├── drills/           # GKE/kind replay runbooks (node-failure, bad-deploy, memory-leak,
│   │                     #   quota-exhaustion) + stub-daemon.py capture sink
│   └── tools/            # ci (runs all presubmits), build, lint-go, test-unit, docs-lint,
│                         #   gen-skill-refs, gen-site-docs, new-check, verify-helm-parity, …
├── scripts/              # verify-internal-links.py (built Starlight HTML + repo Markdown links)
├── examples/             # runnable e2e: kind recipe, sentinel + capture stub, demo
│                         #   workloads, inject/verify/revert failure scenarios, e2e driver,
│                         #   agent-harness (skills/MCP) testing guide
└── docs/                 # DESIGN.md (normative), design notes, milestones/, this map
```

## The two data paths

End-to-end Mermaid architecture and data-flow diagrams live on the
documentation site (verified against live source/sink/table/flag
ledgers in CI):

- **Read-path (`cmd/lookout`, `internal/mcpserver`, `internal/watch` enrichment → `pkg/checks` → `pkg/emit.Writer`):**
  [Architecture → The read-path](https://go-steer.github.io/k8s-lookout/concepts/architecture/#the-read-path)
  and [MCP setup → Request & identity flow](https://go-steer.github.io/k8s-lookout/getting-started/mcp/#request--identity-flow).
  One deliberate §10.2 package crossing on the read path: `pkg/checks/top`
  imports `pkg/sources/saturation`'s metrics fetcher/sample seam so the
  `metrics.k8s.io` client join exists in one place.
- **Watch-path (`pkg/sources` → `pkg/engine` → `internal/watch` → `pkg/inject` / `internal/axsink` + recovery loop):**
  [Architecture → The watch-path](https://go-steer.github.io/k8s-lookout/concepts/architecture/#the-watch-path).
  Recovery closes the loop: `engine.RecoveryTracker` watches bound
  incidents' symptoms clear and injects `resolved` / `resolved.reverted`
  into the same session — making the §9.3 corpus (`pkg/corpus`)
  harvestable with ground-truth labels.
- **Posture (`audit.Snapshot`, detectors, four-layer `pkg/policy`, `pkg/exempt`, `PostureFingerprint`, `findings diff` / `ack`):**
  [Audit cluster posture & track drift → Posture pipeline](https://go-steer.github.io/k8s-lookout/guides/posture-audit/#posture-pipeline).
- **SQLite store (`pkg/store` writers, readers, and pruned vs. durable tables):**
  [The occurrence store → Who writes and reads each table](https://go-steer.github.io/k8s-lookout/operations/store/#who-writes-and-reads-each-table).
- **Deployment (`deploy/base/`, composable `deploy/components/*`, `deploy/mcp/` Gateway, and `deploy/chart/`):**
  [Deploy the sentinel → Deployment architecture, RBAC tiers & sinks](https://go-steer.github.io/k8s-lookout/getting-started/deploy/#deployment-architecture-rbac-tiers--sinks).

## Frozen contracts and their guardian tests

Violating any of these is a bug, not a test update
(AGENTS.md hard rules):

| Contract | Lives in | Guardian test |
| --- | --- | --- |
| `lookout watch` flag surface (M0 image-swap compat) | `internal/watch/main.go` | `internal/watch/flags_contract_test.go` (`TestFlagSurfaceFrozen`; additive flags get their own pin tests) |
| Inject wire payloads, incl. byte-frozen M0 `k8s-event`/`k8s-event-followup` pair (re-baselined once 2026-07-27: + `type` field — dated pre-consumer amendment, `signal-schema-v1.md` §Amendments) | `pkg/inject/injector.go` | `pkg/inject/schema_freeze_test.go` (field-set ledger, 59-kind inventory: 51 active + 8 retired in `pkg/inject/schema/schema.go`, byte-exact round-trip) |
| Webhook sink wire (`POST /incidents` + `/incidents/<id>/events`, schema-v1 payload as the body — never the daemon envelope; re-pinned 2026-07-27 with the `type` amendment) | `pkg/inject/webhook.go` | `internal/watch/webhook_dispatch_test.go` (byte-exact open/append pins: k8s-event, resolved, storm, watchboard.digest, watchboard.rotated) |
| Signal-schema v1 ledger (the fleet-rollup contract) | [`signal-schema-v1.md`](./signal-schema-v1.md) | same freeze tests; removal/rename is a v2 negotiation with fleet consumers, additions extend ledger + doc together |
| engine↔inject kind constants stay one value | `pkg/engine/signal.go`, `pkg/inject` | `internal/watch/signal_contract_test.go` |
| Fingerprint recipe (the §8/§9.4 join key; recipe untouched — the reason-class INPUT for pull-shaped `BackOff`/`Failed` events was corrected 2026-07-27, `signal-schema-v1.md` §Amendments) | `pkg/engine/fingerprint.go` | `pkg/engine/fingerprint_test.go` + `TestFingerprintParity_PushAndScan` (`internal/watch/m5_corpus_rollup_test.go`) |
| Posture fingerprint recipe (the `audit` group's class key: detector kind, uncanonicalized reason, empty zone — an ADDITION, `ScanFingerprint` unchanged) | `pkg/engine/fingerprint.go` | `pkg/engine/fingerprint_test.go` (`TestPostureFingerprint_PinnedVectors`, `TestScanFingerprint_Unchanged`) |
| LKGH graph-snapshot format (magic + version byte + gzip body) | `pkg/graph/history.go` | `pkg/graph/history_test.go` (golden v1 blob, `TestRestore_V1Compat`) |
| Sanitizer: no secret value on any surface | `pkg/emit/sanitize.go` | `pkg/emit/sanitize_test.go` golden fixtures (`pkg/emit/testdata/sanitize/`) — an unmasked credential fixture fails CI |
| Exempt annotates, never drops: a covered finding is still emitted and counted, and the summary reports `exempt=<n>` | `pkg/exempt/`, `pkg/emit/writer.go` | `pkg/exempt/exempt_test.go`, `pkg/emit/exempt_test.go` (`TestExemptAnnotatesNeverDrops`) |
| Shipped ClusterRole covers the enrichment read paths | `deploy/base/12-clusterrole-watcher.yaml` | `pkg/checks/state/rbac_test.go` parses the manifest against `LoadClusterListRequirements()` |
| Command output schema ↔ declared metadata | each `pkg/checks/*` | `pkg/checks/checktest.VerifyContract` in every command's suite |
| MCP response limit (`lookout mcp --max-response-bytes`, #747): a cut tool result loses whole finding lines only, keeps its summary line with `findings=` = lines returned, and says so in the envelope-owned `response_*` notes (`emit.ResponseNoteFields`); head kept unless the command declares `NewestLast`; the hint offers `since` only when the command declares `UsesSince` (#756); `k8s_list_clusters` is cut too, keeping scope lines and summary (#755) | `internal/mcpserver/response.go` | `internal/mcpserver/response_test.go` (`checktest.Verify` on every cut, both formats) + `pkg/checks/all/since_test.go` (`TestUsesSinceMatchesTheSource`: every `Scope.Since` read in `pkg/checks/**` maps to a command declaring `UsesSince`, and vice versa) |
| Finding kinds ↔ the ledger each command declares (an emitted `kind=` must be declared, at a severity the declaration lists) | `Command.Kinds` in each `pkg/checks/*`; the glossary union in `pkg/checks/registry.go` (`KindGlossary`) | `pkg/checks/checktest.Verify` (every kind a test run produces) + `pkg/checks/all/kinds_test.go` (`TestEveryEmittedKindIsDeclared` resolves the kind at every emit site in `pkg/checks/**`, so a branch no fixture reaches cannot slip through) |
| Every registered check reaches the binary and both doc generators | `pkg/checks/all/all.go` (the one blank-import list; the binary, `internal/sitedoc/gen`, `internal/skilldoc/gen`, and their drift tests import only it) | `pkg/checks/all/all_test.go` (`TestAllImportsEveryRegisteringPackage` walks `pkg/checks/**` for `checks.Register` calls) |
| Bundle section identifiers/order across CLI and enrichment ("the enrichment bundle IS a bundle") | `pkg/checks/bundle/bundle.go` + `internal/watch/enrich.go` | `internal/watch/enrich_bundle_contract_test.go` (`TestBundleSectionContractFrozen`; pins both heads' `sections=` joins and both bodies' emission order) |

## Deployment variants

Every Kustomize bundle, `Component`, Helm chart, and deprecated compatibility alias carries its own `README.md` and appears in the directory ledger below (enforced against the live `deploy*` tree by `internal/sitedoc/deploy_variants_test.go` and `dev/tools/docs-lint`, and parity-checked by `dev/tools/verify-helm-parity`):

| Directory | Kind | Helm equivalent | Purpose |
| --- | --- | --- | --- |
| [`deploy/`](../deploy/README.md) | Kustomize alias | default values | Backwards-compatible alias building `deploy/base/` |
| [`deploy/base/`](../deploy/base/README.md) | Kustomize base | default values (`mode=watch`, `sink.type=core-agent`) | Cluster-wide `lookout watch` sentinel posting incidents to `core-agent` |
| [`deploy/chart/`](../deploy/chart/README.md) | Helm chart | — | Parameterizes `deploy/base/`, all `deploy/components/*`, `sink.type=webhook`, and `mode=mcp` |
| [`deploy/components/no-secrets/`](../deploy/components/no-secrets/README.md) | Kustomize `Component` | `rbac.secrets=false` | Withhold `secrets` & `serviceaccounts` `list` rules + `--enrich-lists=all,-secrets,-serviceaccounts` |
| [`deploy/components/probe-from/`](../deploy/components/probe-from/README.md) | Kustomize `Component` | `rbac.probeFrom=true` | Opt-in privileged `pods/ephemeralcontainers` + `ValidatingAdmissionPolicy` for `net probe-from` |
| [`deploy/components/scope-namespaced/`](../deploy/components/scope-namespaced/README.md) | Kustomize `Component` | `rbac.scope=namespace` | Namespace-scoped sentinel (`Role` per namespace + minimal node/PV `ClusterRole`) |
| [`deploy/components/scope-namespaced-strict/`](../deploy/components/scope-namespaced-strict/README.md) | Kustomize `Component` | `rbac.scope=namespace` + `rbac.nodes=false` | Strictly namespaced `Role`-only sentinel (zero cluster-scoped or `kube-system` grants) |
| [`deploy/components/sink-ax/`](../deploy/components/sink-ax/README.md) | Kustomize `Component` | `sink.type=ax` | Switch `lookout watch` to `--sink=ax` + task template `ConfigMap` + Substrate router mTLS |
| [`deploy/mcp/`](../deploy/mcp/README.md) | Kustomize base | `mode=mcp` | Standalone `lookout mcp` Gateway deployment with per-caller Google OAuth |
| [`deploy/mcp/domain/`](../deploy/mcp/domain/README.md) | Kustomize `Component` | `mcp.domain` | Propagate `ConfigMap/lookout-mcp` `data.DOMAIN` into `HTTPRoute` hostname |
| [`deploy/mcp/oauth/`](../deploy/mcp/oauth/README.md) | Kustomize `Component` | `mode=mcp` + `mcp.oauth.enabled=true` | Enable `--oauth-issuer` and OAuth client/keyring `Secret` mounts for generic MCP clients |
| [`deploy/prometheus-operator/`](../deploy/prometheus-operator/README.md) | Kustomize add-on | `serviceMonitor.enabled=true` | Optional `ServiceMonitor` scraping `lookout-watch-metrics` |
| [`deploy-ax/`](../deploy-ax/README.md) | Deprecated alias | `sink.type=ax` | Compatibility alias composing `deploy/base` + `deploy/components/sink-ax` |
| [`deploy-mcp-gemini/`](../deploy-mcp-gemini/README.md) | Deprecated alias | `mode=mcp` | Compatibility alias building `deploy/mcp/` (plus `domain/` and `oauth/` component aliases) |
| [`deploy-mcp-gemini-gateway/`](../deploy-mcp-gemini-gateway/README.md) | Deprecated alias | `mode=mcp` | Compatibility alias building `deploy/mcp/` |
| [`deploy-namespaced/`](../deploy-namespaced/README.md) | Deprecated alias | `rbac.scope=namespace` | Compatibility alias composing `deploy/base` + `deploy/components/scope-namespaced` |
| [`deploy-namespaced-strict/`](../deploy-namespaced-strict/README.md) | Deprecated alias | `rbac.scope=namespace` + `rbac.nodes=false` | Compatibility alias composing `deploy/base` + `deploy/components/scope-namespaced-strict` |
| [`deploy-no-secrets/`](../deploy-no-secrets/README.md) | Deprecated alias | `rbac.secrets=false` | Compatibility alias composing `deploy/base` + `deploy/components/no-secrets` |
| [`deploy-probe/`](../deploy-probe/README.md) | Deprecated alias | `rbac.probeFrom=true` | Compatibility alias composing `deploy/base` + `deploy/components/probe-from` |

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
when-to-use line, output-field glossary, and finding-kind ledger)
generates four surfaces:

1. `--help` — `pkg/checks/help.go`, rendered by the multicall binary;
2. MCP tool schemas — `internal/mcpserver/schema.go`, every command
   1:1 as an MCP tool;
3. skill `references/` — `internal/skilldoc` via
   `dev/tools/gen-skill-refs` (committed output; a drift test in
   `internal/skilldoc` fails CI when metadata changes without a
   re-run);
4. Starlight docs-site `reference/` and `detect/{index,scan,audit}.md`
   — `internal/sitedoc` via `dev/tools/gen-site-docs` (committed
   output; a drift test in `internal/sitedoc` fails CI on drift).

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
| Gemini Enterprise token spike (human-run, never CI; standalone Go module for isolated Cloud Build deployment to Cloud Run — pre-#699 spike; `lookout`'s root `go.mod` now requires `steward` and `purser` directly for `lookout mcp` and `internal/axsink`) | `dev/drills/ge-token-spike/` (#698) |
| live-cluster e2e scenarios (local kind / GKE staging; in CI only non-blocking — post-merge smoke + weekly full via `e2e-kind.yml`) | `examples/e2e` + `examples/scenarios/*` (inject/verify/revert against the wire capture + read-path) |

CI: `dev/tools/ci` runs the same steps as `.github/workflows/ci.yml`
(format → vet → build → lint → mod-tidy → test → vuln), individually
invokable via `dev/ci/presubmits/`. `ci-docs.yml` green-stubs the
required checks for docs-only changes.

## Design vs implementation divergences

Where the tree deliberately differs from DESIGN.md, in one place:

- **Layout:** the §2 tree predates M1+. Not in it but real:
  `internal/{watch,fleet,mcpserver,skilldoc,telemetry,version}` (the
  sentinel wiring and serving layers live under `internal/`, not
  `cmd/lookout/`), `pkg/memory`, `pkg/corpus`,
  `pkg/checks/checktest`.
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
- **Scan-side fingerprints are domain-less by design**: the sentinel
  stamps §8 project/region/zone (explicit `--project` /
  `--region`/`--zone` flags > provider metadata via `cloud.Identity`
  > empty; region and zone resolve as a *pair*, so setting either
  flag stops metadata detection for both) and hashes the failure
  domain — `engine.FailureDomain` = zone if the cluster has one, else
  region — into fingerprints. A point-in-time scan carries no
  deployment identity, so `ScanFingerprint` callers pass `zone=""`.
  Deployments that stamp nothing keep domain-less fingerprints —
  still stable, and push/scan self-consistent; the §9.4 join
  tolerates the mismatch via its resource-key pin
  (`pkg/memory/join.go`).
- **Leeway extracted to standalone module (#842):** DESIGN.md §4.1 ("The
  one exception: `cmd/leeway`") and §7.2 (`topology-drift`,
  `compute-class`) predate #842, which extracted placement-skew and GKE
  `ComputeClass` fallback tracking into `github.com/go-steer/leeway`
  (`leeway report`) and restored `k8s-lookout` to a single multicall
  binary (`cmd/lookout`). Read-path `audit computeclass` and `capacity`
  scale-up verdict classification remain in-tree, importing
  `github.com/go-steer/leeway/pkg/leeway`.

## Further pointers

- **Normative specifications & contracts:**
  - [`DESIGN.md`](./DESIGN.md) — normative spec; records *rejected*
    alternatives with reasons. Do not re-propose without new evidence.
  - [`signal-schema-v1.md`](./signal-schema-v1.md) — frozen v1 wire
    contract and kind ledger for fleet consumers.
  - [`cli-stability-policy.md`](./cli-stability-policy.md) — CLI flag,
    syntax, default, and exit-code stability rules.
  - [`adding-a-check.md`](./adding-a-check.md) — step-by-step guide and
    touchpoint checklist for adding a read-path command.
  - [`release-process.md`](./release-process.md) — release tagging,
    multi-arch image signing, and SBOM attestation pipeline.
- **Subsystem design notes (shipped):**
  - [`fleet-audit-detectors-design.md`](./fleet-audit-detectors-design.md)
    and [`audit-ingestion-contract.md`](./audit-ingestion-contract.md) —
    deterministic posture detectors (`lookout audit`), policy profiles,
    and the `emit.Finding` (`--format=findings`) audit ledger seam.
  - [`findings-diff-design.md`](./findings-diff-design.md) — stateful
    finding transition stream (`lookout findings diff` and `findings ack`).
  - [`leeway-design.md`](./leeway-design.md) — pointer to
    `github.com/go-steer/leeway` after #842 extracted placement-skew and
    GKE `ComputeClass` fallback tracking out of `lookout watch`
    (read-path `audit computeclass` stays).
  - [`in-pod-probe-design.md`](./in-pod-probe-design.md) — `net probe-from`,
    the opt-in privileged ephemeral-container probe (`deploy-probe/`).
  - [`multi-cluster-design.md`](./multi-cluster-design.md) and
    [`mcp-multi-tenant-design.md`](./mcp-multi-tenant-design.md) —
    multi-cluster resolution (`internal/fleet`, `--contexts`,
    `--kubeconfig-dir`) and per-caller Google OAuth identity on `lookout mcp`.
  - [`agent-sink-design.md`](./agent-sink-design.md) and
    [`ax-sink-design.md`](./ax-sink-design.md) — the two-verb `Sink`
    interface, generic webhook sink, and AX task lifecycle sink.
  - [`triage-status-write-design.md`](./triage-status-write-design.md),
    [`watchboard-rotation-design.md`](./watchboard-rotation-design.md),
    and [`graph-q5-gate.md`](./graph-q5-gate.md).
- **Historical & milestone records:**
  - [`milestones/`](./milestones/) — M0–M5 completion records with
    exit-check evidence.
  - [`assessments/`](./assessments/) — competitive & detection-coverage
    assessments (k8sgpt, LangChain SRE agent, kube-agents event-watcher
    noise controls).
  - [`appendix-v2-dataplane-intelligence.md`](./appendix-v2-dataplane-intelligence.md)
    — historical only; nothing in it is normative.
