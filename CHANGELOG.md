# Changelog

All notable, user-visible changes to k8s-lookout.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

**Default-pin note (2026-07-27, zero-deployed-users policy):** the two
flag-default changes below amend post-M0 flag-default pins CLEANLY
ONCE, with the pin tests updated deliberately (dated policy comments in
`internal/watch/sources_flag_test.go` / `storm_dispatch_test.go`). The
M0 frozen flag surface (`TestFlagSurfaceFrozen`, the 19 predecessor
flags) is untouched — `--sources` and `--storm` postdate it. All frozen
wire pins are byte-identical.

- `--sources` now defaults to `auto` (was `k8s-events` only):
  probe-and-enable across the seven portable sources (k8s-events,
  object-state, rollout, saturation, degradation, expiry, capacity) —
  each candidate's §11 RBAC probe passing enables it, and saturation
  additionally requires the metrics.k8s.io API in discovery. Misses are
  skipped with one explicit startup line naming the source, the missing
  grant/API, and the fix; the summary block always prints (enabled
  lines included). `k8s-events` failing its probe is fatal — a sentinel
  that cannot watch events is misdeployed. `quota` (project tier) and
  `token-burn` (core-agent cost stack) are never auto-enabled. Explicit
  lists keep the frozen semantics exactly: a named source's probe
  failure is fatal, and `--sources=k8s-events` reproduces the old
  default byte-for-byte.
- `--storm` now defaults to `auto` (was off) **and changes syntax**:
  the flag is a string taking `auto|on|off` (`true`/`false` accepted as
  aliases). auto probes the graph informer grants
  (pods/nodes/replicasets list+watch) — present resolves on, missing
  resolves off with a loud line; `--storm=on` keeps the old fatality.
  The resolution is independent of object-state (the graph feed runs
  its own informers; factory sharing is an optimization). BREAKING
  SYNTAX: bare `--storm` (valid bool syntax before) now errors — write
  `--storm=on`; drills and docs updated.
- `deploy/51-deployment-watcher.yaml` now ships the full capability
  surface explicitly: all seven portable sources plus `token-burn`,
  `--storm=on`, and `--store=/var/lib/lookout/lookout.db` on an
  emptyDir volume (commented PVC alternative for durable post-mortems).
  Explicit on purpose — the manifest ships alongside the full RBAC, so
  a grant gap should fail loudly, not skip. The image-swap upgrade path
  for predecessor deployments is unchanged.

- Docs site: plain-language rewrite of the entry points for first-time
  readers — new landing page (problem, what lookout does, one real
  output sample, audience router), a "How lookout thinks" overview
  replacing the Concepts section stub, orienting introductions on the
  Getting started / Guides / Operations indexes (plus a symptom-based
  "which guide do I need" table), and plain-register opening paragraphs
  on every concepts detail page. Writing only: no command, flag, or
  reference-content changes; all sample output is pre-existing captured
  output.

- Docs: removed references to the external fleet project by name —
  fleet-rollup rationale now uses generic phrasing ("the fleet layer",
  "a fleet-level consumer") throughout docs, comments, and test text.
  No functional change; all frozen pins and goldens are byte-identical.

## [0.8.0] - 2026-07-27

Adopts the improvements kube-agents made to their in-tree copy of the
k8s-event-watcher (their #329/#382/#406), re-baselining the affected
frozen pins once under the zero-deployed-users policy: message-aware
reason canonicalization, a --dry-run that actually watches, the `type`
payload field, ErrorHandlers hygiene, and persist-corruption tolerance.

Adopted four fixes and one wire-contract addition from kube-agents'
in-tree copy of the k8s-event-watcher (their #329/#382/#406), with a
one-time re-baseline of the affected frozen pins.

**Re-baseline note (2026-07-27, standing policy for this change):**
there are zero deployed consumers of lookout today (only kube-agents'
watcher fork and the core-agent demos), so the byte-frozen M0 inject
pins, the webhook wire pins, and the schema-v1 `Payload` field ledger
were amended CLEANLY ONCE — no migration machinery, no compat shims.
Each amendment is dated and recorded in
`docs/signal-schema-v1.md` §Amendments; the `Fingerprint` recipe and
its pinned cross-cluster vectors are untouched.

### Added

- `type` field on the wire (`inject.Payload.Type`, json `type`): the
  k8s `Event.Type` (`Normal`/`Warning`), populated by the k8s-events
  source via the new `engine.TriageEvent.Type`. Positioned after
  `context` and NOT omitempty, matching kube-agents' watcher wire
  byte-for-byte; empty for synthetic source signals (they observe
  transitions and forecasts, not Events). The M0 byte-exact pins and
  the webhook wire pins were re-baselined once for it (see the note
  above). Scan-side `emit.Finding` deliberately does not gain the
  field — scan findings aren't Events.

### Fixed

- Message-aware reason canonicalization
  (`engine.CanonicalReasonForEvent`): kubelet emits the generic
  `BackOff`/`Failed` reasons for both crash-loop and image-pull
  cycles, and only the event message disambiguates them. Pull-shaped
  messages (`Back-off pulling image …`, `Failed to pull image …`,
  `Error: ErrImagePull`, `Error: ImagePullBackOff`) now classify as
  `ImagePullBackOff` — one image-pull failure is ONE incident (one
  dedup slot, one session, one fingerprint class), not a parallel
  session per reason variant. The dispatcher computes the canonical
  pipeline key once per signal (`TriageEvent.CanonicalKey`) and
  threads it through dedup, storm bookkeeping, triage regression
  state, watchboard binding, and recovery tracking; `lookout triage
  events` (pull path) and the store's `canonical_reason` column use
  the same message-aware class. Wire payloads keep the original event
  reason, and the messageless `engine.CanonicalReason` mapping is
  unchanged for reason-only callers.
- `--dry-run` actually watches: the sentinel now builds the kube
  client and runs the full pipeline (informers, sources,
  filter/dedup/routing, recovery) in dry-run, printing inject
  payloads to stdout instead of calling the daemon/sink — previously
  it skipped the kube client entirely and validated flags against an
  idle process. Dry-run therefore now requires cluster access, like a
  normal run (help text, reference page, and getting-started docs
  updated).
- `runtime.ErrorHandlers` in the k8s-events source is now APPENDED
  to instead of replaced: the old assignment clobbered client-go's
  package-global handler list for every other informer consumer in
  the process — a real multi-source bug now that one binary runs many
  informer-backed sources.
- Corrupt `--dedup-persist` snapshots no longer refuse to boot the
  sentinel: an unparseable file is logged and the cache starts fresh
  (the code previously returned the unmarshal error out of
  `NewDedupCache` while its comment promised "start fresh" — a
  crash-loop on corruption). The version-tolerant loader behavior is
  unchanged; a corrupt-file fixture test pins the new path.

## [0.7.0] - 2026-07-26

Pluggable agent sinks: the watch-path's two-verb runtime contract is now
explicit (docs/agent-sink-design.md). core-agent remains the wire-identical
default; `--sink=webhook` delivers the same frozen schema-v1 payloads to any
HTTP receiver. Plus the docs-site Integrations page.

### Added

- `docs/agent-sink-design.md` — design note settling the agent-sink
  contract: the two verbs the watch-path requires of any agent runtime
  (open an incident context, append to it; plus an optional usage query
  for token-burn only), the `pkg/inject` Sink interface with the
  core-agent HTTP client as the wire-identical default (frozen pins as
  the regression proof; additive `--sink`/`--sink-url`/
  `--sink-token-env` flags), the webhook wire contract
  (`POST /incidents`, `POST /incidents/<id>/events`, Bearer auth,
  stateless receivers legal), and the out-of-scope list (per-framework
  adapters per the no-speculative-surface rule, acknowledgement
  semantics beyond HTTP status, multi-sink fanout). Linked from the
  repo-map design-notes list.
- Docs site: Integrations page (Getting started, step 6) — consuming
  the read path from any MCP client (generic config shape + Claude Code
  one-liner), from shell-capable agents via the CLI contract, and
  skills portability; receiving the watch path anywhere via the webhook
  sink, with a curl-able example exchange built from captured milestone
  payloads and `dev/drills/stub-daemon.py` as the runnable reference
  receiver. core-agent remains the first-class default.
- Pluggable agent sinks: `pkg/inject` now exposes the two-verb
  `Sink` interface (`OpenIncident` + `Append`) every sentinel inject
  routes through, with two implementations selected by the new
  additive `--sink` flag (default `core-agent` — byte-identical wire
  and unchanged flag semantics for every existing deployment; all
  frozen wire pins pass untouched). `--sink=webhook` delivers to any
  generic HTTP receiver at `--sink-url`: `POST <url>/incidents` opens
  an incident with the signal-schema v1 payload JSON as the request
  body (the exact bytes that ride inside the core-agent envelope's
  `message` — never wrapped), `POST <url>/incidents/<id>/events`
  appends follow-ups, optional Bearer auth via `--sink-token-env`.
  Receivers answer 2xx with `{"id":"<opaque>"}`; stateless receivers
  that return no id get locally generated ids (logged once). https is
  strongly recommended; plain http is allowed with a loud startup
  warning. The webhook wire is pinned byte-exact
  (`internal/watch/webhook_dispatch_test.go`) and frozen like the
  daemon envelope. Transport posture (10s timeout, otelhttp-wrapped,
  no retries) is shared with — and identical to — the core-agent
  client.
- `k8s_event_watcher_sink_info{sink}` gauge (value 1 on the active
  sink). Metrics decision: the frozen operation counters
  (`session_creates_total`, `inject_errors_total`, …) stay
  sink-agnostic with their exact names and label sets — the sink is
  process-level config, so a per-request `sink` label would add a
  constant-valued dimension and change existing series identities;
  the info gauge carries it instead.

### Changed

- `--sink=webhook` validation: `--mode`, `--target-session`, and
  `--owner` are core-agent session concepts and are rejected with the
  webhook sink; `--daemon-url`/`--token-env` are no longer required
  when it is selected. The token-burn source requires the core-agent
  sink (its §12 cost stack is the daemon's attach API): with
  `--sink=webhook` it idles with a loud startup message instead of
  running.
- Watchboard rotation on a stateless sink (webhook) opens the
  successor incident WITH its first digest and appends the
  `watchboard.rotated` lineage pointer to the closed incident
  afterwards — a generic receiver has no create-empty verb. The
  core-agent sink keeps the frozen §15 Q2 wire order (successor
  session first, lineage pointer before the successor's first digest)
  via its `inject.SessionOpener` capability; the existing rotation
  pins pass unchanged.

## [0.6.1] - 2026-07-26

End-user documentation site (Astro Starlight, mirroring core-agent's
stack) with a generated command reference, scenario guides authored
from the milestone drill evidence, and operations documentation; plus
two post-M5 fixes surfaced by the repo-map verification pass.

### Added

- Docs site: Getting started and Operations sections (replacing the
  scaffold placeholders). Getting started: install (image flavors +
  cosign + `go install` + the entrypoint/image-swap contract), first
  reads against a kubeconfig (`health` / `triage delta` with the
  captured kind outputs), deploying the sentinel (per-manifest table,
  §11 tier table, namespace-tier loud-failure semantics, the
  capability flag walkthrough), connecting to core-agent (session/
  inject contract, per-incident vs shared, the M0 wire capture), and
  MCP setup (stdio + loopback HTTP, the non-loopback refusal, the full
  tool↔command table). Operations: the occurrence store (contents,
  bounds, distroless store-copy procedure, `--at` queries, epoch
  semantics across restarts), the watchboard (digest cadence, size-
  based rotation lifecycle, lineage), drills & verification (per-
  runbook map into `dev/drills/`), observing lookout (alerting table
  over the metrics surface, startup-log checklist), and
  troubleshooting (RBAC probe failures, source-by-source requirements
  table, startup errors verbatim with fixes, the `unavailable`
  markers). All command/flag/metric detail links into the generated
  Reference section; every quoted output is a captured one from the
  milestone records or README.
- `docs/repo-map.md` — contributor/maintainer architecture map: repo
  layout with per-directory responsibilities, the read-path and
  watch-path data flows, the frozen-contract → guardian-test table,
  the provider-boundary/build-tag scheme, the generated-outward doc
  machinery, the §13 testing-conventions map, and the known
  design-vs-implementation divergences in one place. Linked from
  AGENTS.md "Start here" (read DESIGN.md for the spec, repo-map.md
  for the tree) alongside a trim of the AGENTS.md milestone
  archaeology now covered by `docs/milestones/`.
- Docs site Concepts and Guides sections filled in (the scaffold PR's
  placeholders): six concept pages authored from DESIGN.md and the
  design notes (architecture, topology graph, signals & fingerprints,
  the closed loop, sanitization guarantees, portability & providers)
  and six problem-first scenario guides built from the milestone
  exit-drill evidence with real captured output, abridged and never
  invented (broken workload — M1; stuck rollout — M3 drill A; resource
  exhaustion — M3 drill B; node failure — M2; what-changed post-mortem
  — M3 drill C; capacity & quota — M4), each guide cross-pointing its
  matching agent skill.
- `skills/k8s-capacity` — the §4.4 capacity/quota workflow skill that
  never shipped with M4/M5 (tracked as a repo-map divergence, now
  closed): the cloud stockout/quota/ipspace sweep, the
  stockout-vs-quota remedy split, pending-pod dedup families, reading
  `quota.forecast` + the attached `quota_increase_draft`, filing
  through the daemon's permission gate, and recording the conclusion
  via `triage status`. References generated (`dev/tools/gen-skill-refs`
  now registers the `cloudcheck` command group), skilldoc contract
  tests cover the new doc's command lines and golden snippets.

### Changed

- README rewritten user-facing: quickstart for both halves (a real
  abridged `lookout health` run against a kind cluster with only a
  kubeconfig; `kubectl apply -f deploy/` for the sentinel), the full
  per-command surface table with provider-gated entries marked and
  the vanilla-Kubernetes degradation shown verbatim, image flavors +
  cosign verification, and documentation pointers — milestone
  narration moved out to `docs/milestones/`.
- End-user docs site under `docs/site/` (Astro Starlight, stack mirrored
  from core-agent: light-only theme, remark-prepend-base, GitHub Pages
  deploy via the new `Docs` workflow). The Reference section is generated
  by `dev/tools/gen-site-docs` (`internal/sitedoc`) from the same
  declarations that produce `--help`, the MCP schemas, and the skill
  reference stubs: one page per registered command, the full
  `lookout watch` flag table (derived from the live flag surface via the
  new `internal/watch.FlagInventory`), the signal-kind catalog (from the
  v1 ledger, now exported as data in `pkg/inject/schema` and shared with
  the freeze tests), and the Prometheus metrics table (names + help
  derived from the live collectors via `internal/watch.MetricsInventory`).
  A drift test in `internal/sitedoc` fails CI when committed pages differ
  from regeneration. `dev/tools/docs-lint` (dropped at M0) is restored —
  mirrored from core-agent, adapted to this tree — and wired into
  `dev/tools/ci`, `dev/ci/presubmits/verify-docs-lint`, and the new
  `Docs lint` workflow; `ci.yml`/`ci-docs.yml` path lists now let
  docs-site-only changes skip Go CI with required checks staying green.

### Fixed

- `lookout health`'s control-plane category now delegates to the
  shipped `perf probe` packs (the §5 "control-plane latency (perf
  probe packs)" row) instead of always reporting the M4-era
  `unavailable`: with a Metrics-capable provider it runs the
  apiserver pack (p99 by verb/resource — the cheapest meaningful
  control-plane read), scores `degraded` with the breaching series
  in `perf probe`'s exact finding shape, and `healthy` when queries
  succeed with no breach. The honest `unavailable reason="…"`
  remains only when the capability is genuinely absent — no
  provider / no metrics capability (reason text drops the stale
  "(M4)"), or the metric positively missing from the workspace (the
  pack_unavailable case: GKE control-plane metrics not enabled).
  Scorecard line shape unchanged; a real backend failure fails the
  scan like any other category's read error.
- `lookout watch` now stamps the §8 zone/project deployment
  identity: additive `--project`/`--zone` flags for vanilla
  clusters, blanks filled from a compiled-in provider's metadata
  (the new optional `cloud.Identity` surface — the gke provider
  resolves config pins, well-known env vars, then the GCE metadata
  server). Precedence: explicit flag > provider metadata > empty.
  Source-namespaced payloads and signal fingerprints carry real
  zones in-cluster, completing the (fingerprint,
  cluster/project/zone) fleet-rollup join. Zone participates in the
  fingerprint hash, so a deployment that starts stamping a zone
  re-keys its incident classes once; deployments that stamp nothing
  keep their zone-less fingerprints byte-identical (still stable —
  but cross-cluster joins within a failure domain need zones
  stamped, which is the point of the wiring). Frozen contracts
  untouched: the fingerprint recipe's pinned vectors are inputs→hash
  and unchanged, the M0 `k8s-event`/`k8s-event-followup` wire
  payloads never carry identity fields, and `zone`/`project` ride
  other kinds via the existing omitempty ledger entries.

## [0.6.0] - 2026-07-26

M5 — fleet & corpus — closes out the §14 milestone plan (all six
milestones complete): the signal schema is FROZEN as v1
(`docs/signal-schema-v1.md`), the §9.3 corpus harvester contract is
validated end-to-end, and the multi-cluster rollup is demonstrated as
the fleet-side fingerprint join — exit evidence and the post-M5 review
backlog in `docs/milestones/M5.md`. Plus all five M4-drill
observations fixed (`docs/milestones/M4.md` §Observations).

### Added

- `skills/gitops-drift/` (the last §4.4 tree entry alongside the
  still-backlogged `k8s-capacity`): the divergence-auditing workflow —
  `stab drift` answers "who diverged" (managedFields ownership, spec
  paths, manager strings never user identities), `triage changes`
  answers "what changed" (chronological, provenance-tagged, `--store`
  for the full delta log) — plus the remediation boundary (lookout
  only reads; sync-vs-commit is a GitOps decision) and the §9.4
  record-the-conclusion step. `k8s-triage` gained the M5 verification
  rows (`state webhooks|volumes|wi`) and the `fingerprint=` reading
  note; `cluster-health` drills webhooks into the real
  `state webhooks`, control-plane into `perf probe`, adds the
  pre-maintenance `stab drain` sweep, and replaces the stale
  M1-era store-merge caveat with the shipped §9.4 behavior.
  References regenerated for all three skills (the generator and
  skilldoc tests now register the `perf`/`stab` groups).

- **Signal-schema v1 freeze** (`docs/signal-schema-v1.md`, DESIGN.md
  §8/§14 M5): the fleet-rollup wire contract fleet consumers take
  as-is, frozen in-repo per the standing decision (no external
  filing).
  Machine-readable ledger in `pkg/inject/schema_freeze_test.go`: all
  32 shipped kinds enumerated and mapped to their wire structs, every
  struct's ordered json field list pinned (removal/rename fails the
  ledger — a v2 negotiation with fleet consumers, never a test update; additions
  are additive-only and must extend the ledger consciously), and
  every payload round-trips marshal→unmarshal→marshal byte-exact so
  schema-walking consumers re-serialize losslessly.
- **§8 identity fields on source-namespaced payloads**: kinds other
  than the frozen `k8s-event`/`k8s-event-followup` pair now carry
  `source`, `severity`, `fingerprint`, and (when known) `project`/
  `zone` on the wire — fleet rollup becomes a fleet-layer join on
  (fingerprint, cluster/project/zone) instead of a parsing project.
  The M0 pair stays byte-identical (frozen wire pins unchanged); the
  `quota.forecast`/`saturation.forecast` pins were extended to the
  completed v1 shape in the same change.
- **Scan-side fingerprints** (§8 "one schema for push and pull"):
  `lookout health` and `lookout triage delta` stamp every
  symptom-class finding with the new `fingerprint` envelope field,
  computed by the frozen scan-source mapping
  `engine.ScanFingerprint(reason, objectClass, zone)` ≡
  `Fingerprint("k8s-event", CanonicalReason(reason), …)` — the exact
  recipe the §9.4 join has used since M4 (the joiner now calls the
  shared helper). `lookout triage status` moved the record key to the
  same envelope field. Push/pull parity is pinned by
  `TestFingerprintParity_PushAndScan`: the dispatcher-stamped
  occurrence is found in the §9.1 store under the scan-computed key.
- **§9.3 corpus harvester** (`pkg/corpus`; CLI
  `go run ./dev/tools/harvest-corpus`): extracts labeled trajectories
  — symptom → diagnosis (enrichment bundle, §9.4 records) → action
  (`status=actioned|escalated`) → externally verified outcome
  (`resolved`/`resolved.reverted` with its structured `resolution`) —
  from a captured inject stream in `dev/drills/stub-daemon.py`'s log
  format (optionally interleaved with exported triage-status record
  JSON lines), by PURE schema walks over the frozen payloads: no NLP,
  which is the §9.3 contract this validates. Labels come only from
  structured fields (`recovered`/`object_deleted`/`reverted`; a
  reverted resolve wins over the resolve it reverts). End-to-end exit
  check `TestDrill_CorpusHarvest_EndToEnd`: a scripted full lifecycle
  through the REAL dispatcher against a stub-format capture yields
  exactly one COMPLETE labeled trajectory; the sibling
  `TestDrill_MultiClusterRollup_Stockout` runs TWO dispatcher
  instances (prod-east/prod-west) over the same staged zonal stockout
  (+ `quota_blocked`, + storm-fingerprint kind coverage) and asserts
  the fleet-level join: identical fingerprints, differing cluster
  identity, one group per failure class.

- `lookout triage status` (MCP `k8s_triage_status`) — the §9.4
  triage-status PRODUCER surface (M4 observation 1), the §4.1
  addition decided in `docs/triage-status-write-design.md` (new).
  Incident playbooks write the record at each material transition:
  `--fingerprint` + `--resource` + `--status
  investigating|triaged|actioned|escalated` with `--root-cause`,
  `--severity-override`, `--action`, `--session`; without `--status`
  the same command reads the current record(s) back. Writes go
  through `pkg/memory`'s `TriageWriter` against `--store` (required —
  the usage error names the design note), validated per the §9.4
  schema; `--status=resolved` is refused (the sentinel's §7.4
  recovery flip owns that transition). The `crashloopbackoff` and
  `hpa-thrash` playbooks gained the "write your triage status" step
  with real command lines; the `dev/drills/write-triage-status`
  drill stand-in is deleted as its header promised, and the
  quota-exhaustion runbook now uses the real command. Daemon-mediated
  writes stay out of scope until core-agent ships the shared Memory
  surface (`pkg/memory`'s TODO).
- `kind=triage.regressed` evidence followups (M4 observation 3): a
  steady symptom stream never exits the dedup window, so a
  §9.4-downgraded incident could regress hard with no visible routing
  change until the loop paused. When a downgraded incident's window
  count reaches `--triage-regress-factor` (default 3, >= 2, 0
  disables) times its count at downgrade time, ONE schema-stable
  followup (payload pinned byte-exact; kind APPEND-ONLY in
  engine/inject) lands in the bound session with baseline_count /
  count / factor and the open record's status, override, and session.
  Deliberately evidence-only — no auto-re-page and no record rewrite
  (rationale in `docs/triage-status-write-design.md` §out-of-scope);
  metric `k8s_event_watcher_triage_regressed_total`.
- Cross-source dedup joins are now session-visible (M4
  observation 4): a dedup-window duplicate whose SOURCE family
  differs from the one that opened the incident (leading↔reactive —
  the drill's exact case: capacity's `quota_blocked` folding into the
  quota source's forecast session) injects a compact followup into
  the bound session instead of vanishing as `route=suppressed`.
  Frozen contract respected: k8s-event joiners carry
  `k8s-event-followup`; other kinds keep their own source kind.
  Bounded to ONE per source family per incident per window
  (`followup_sources` on the persisted dedup entry); recorded as the
  new APPEND-ONLY `route=followup` outcome; metric
  `k8s_event_watcher_cross_source_followups_total{source}`. Shared
  mode keeps its frozen suppress-everything contract.
- GHCR publishes a GKE-flavored image (M4 observation 5):
  `release-images.yml` builds `ghcr.io/go-steer/lookout:<version>-gke`
  (plus the `-gke`-suffixed tag pyramid and `latest-gke`) with
  `-tags allproviders` via the Dockerfile's new `BUILD_TAGS` arg —
  same Dockerfile, same Sigstore signing. The default `:<version>`
  image stays GCP-free per §2 (conformance-tested);
  `deploy/51-deployment-watcher.yaml` documents that project-tier
  quota deployments (`--sources=…,quota`, §11) must pin the `-gke`
  flavor.

### Fixed

- The shipped ClusterRole now covers BOTH §7.6 enrichment read paths
  (M4 observation 2 — every enrichment in the drill failed at
  resolve on the scoped-list path): `deploy/12-clusterrole-watcher.yaml`
  gains `list` on daemonsets, jobs, cronjobs, services, configmaps,
  ingresses, and the RBAC kinds the namespace-scoped
  `state.LoadCluster` pass reads, plus `get` on the workload kinds
  the live path's top-owner GET can target. The requirement list is
  now exported (`state.LoadClusterListRequirements`) and a test in
  `pkg/checks/state` parses the shipped YAML against it — the role
  and the code cannot drift again, and a second test pins the role
  read-only (no write verbs; secrets stay `list`-only).

## [0.5.0] - 2026-07-26

M4 — capacity & quota (DESIGN.md §14). Exit criterion verified in
`docs/milestones/M4.md`: staged quota exhaustion (scripted cloud APIs per
the §13 fixture policy; the Kubernetes-visible half live in kind)
produced ONE correlated incident — warning forecast to the watchboard,
critical escalation opening the session with the formula-exact
`quota_increase_draft` attached, the reactive `GCE_QUOTA_EXCEEDED`
scaleup failure folded into the same session by the `QuotaExhausted`
dedup family — and a `lookout health --store` scan run mid-crashloop
reported `triage_status=triaged` with the agent's root cause, action,
session pointer, and downgraded severity instead of a fresh critical
unknown, while the sentinel routed the incident's next dedup cycle to
the watchboard instead of re-paging. The v0.4.0 tag predates PRs #50–#51,
so the distilled-memories and triage-status entries below record features
first released in THIS version (they were filed under 0.4.0 in error
until this release cut).

### Added

- The `token-burn` source (M5, DESIGN.md §7.2 row 9, §12) — token
  spend as a first-class saturation dimension: a runaway agent loop
  is an OOM in the currency that matters. Enabled via
  `--sources=…,token-burn`; each poll (`--token-poll`, default 60s)
  reads the core-agent v2.7.0 cost stack — the REAL, shipped attach
  surface (`GET /sessions` + `GET /sessions/{app}/{sid}/usage`, the
  #222 UsageMetadata schema) on the SAME daemon the injector talks
  to (same `--daemon-url`/`--token-env`; `--token-endpoint`
  overrides for split deployments) — and applies the saturation
  source's regression (`saturation.LeastSquaresSlope`) to each
  active session's cumulative billed tokens over a 15m ring buffer.
  Emits `token.burn` (§7.3, APPEND-ONLY) at warning when a session's
  rate sustains ≥ `--burn-multiple` (default 4×) the cross-session
  trailing-MEDIAN baseline for 2 polls, and at critical — with
  `Forecast{ETA, "linear-15m-window"}` — when a known per-session
  budget projects exhaustion inside `--burn-eta` (default 30m) or is
  already exhausted; evidence carries session id, rate, baseline,
  multiple, and budget fraction. Nothing ever fires on the first
  poll (cold start), and the saturation-style hysteresis latch never
  re-fires the same severity (escalation once; release after 2 calm
  polls → §7.4 clearance reports recovered; a session the daemon
  stops listing clears as object_deleted). Budget caveat, verified
  against core-agent v2.7.0: the daemon tracks per-session spend
  caps in-process (`agent.CostCeiling`) but exposes NO budget over
  the attach API, so `--token-budget-usd` (default 0 = unknown →
  budget trigger disarmed, loudly logged at startup) supplies it
  lookout-side until core-agent ships the surface —
  `TODO(core-agent)` in `pkg/sources/tokenburn`, same posture as
  `pkg/memory`. The source needs no Kubernetes RBAC at all
  (`Scope()=Namespace`), and its client-interface fixtures record
  the v2.7.0 wire shapes so the adapter is pinned to the real
  contract.
- `state wi` (M5, DESIGN.md §5; MCP `k8s_workload_identity`) — GKE
  Workload Identity KSA↔GSA binding verification through the
  `pkg/cloud` provider boundary (§2): for every ServiceAccount in
  scope carrying the `iam.gke.io/gcp-service-account` annotation,
  the provider verifies the claimed GSA via the IAM API
  (`serviceAccounts.getIamPolicy`, `roles/iam.workloadIdentityUser`
  membership for `serviceAccount:<project>.svc.id.goog[<ns>/<ksa>]`).
  Findings: `wi.gsa_missing` (critical — the annotated GSA does not
  exist), `wi.unbound` (critical — annotation present, IAM binding
  absent), `wi.unannotated_use` (info — pod points
  `GOOGLE_APPLICATION_CREDENTIALS` at a mounted key file instead of
  WI); bound identities are silent. Scopes with `--namespace`/`-A`/
  `--workload`. Vanilla clusters get the standard §2 explicit
  degradation (`cloud.unavailable` finding + `unavailable reason=…`
  summary marker). `cloud.WorkloadIdentityAPI.VerifyBinding` grew a
  `cloudIdentity` parameter (the annotation's claim — verification
  must be anchored on it) plus machine-matchable problem codes; the
  GKE implementation is tag-guarded (`gke || allproviders`) behind a
  small client interface with doc-authored IAM fixtures.
- `state webhooks` (M5, DESIGN.md §5; MCP `k8s_admission_webhooks`)
  — the FULL failing-closed admission-webhook audit over every
  Validating/MutatingWebhookConfiguration: backend service exists /
  has ready endpoints / serves the referenced port, judged against
  the effective failurePolicy — `webhook.failing_closed` (critical:
  Fail + dead backend, with compact blast-radius details `gates`
  from namespaceSelector, `rules`, `object_selector`),
  `webhook.dead_backend` (warning: Ignore + dead — the policy is
  silently not enforced), `webhook.slow_risk` (info: timeout ≥ 10s
  with Fail), and caBundle expiry as `webhook.ca_expired`/
  `webhook.ca_expiring` (`--cert-warn`, default 720h). `health`'s
  webhooks category now delegates to this check's core
  (`state.LoadWebhookInputs` + `state.CheckWebhooks`) — the
  scorecard line shape is unchanged; the minimal
  `webhook.backend_missing` kind is superseded by the full
  inventory above.
- `state volumes` (M5, DESIGN.md §5; MCP `k8s_volume_conflicts`) —
  VolumeAttachment + PV/PVC/pod join naming stuck-mount causes
  before the Multi-Attach events do: `volume.multi_attach`
  (critical — RWO/RWOP claim wanted by scheduled pods on ≥2 nodes,
  with both pod and node sets), `volume.attach_error` (warning,
  critical once the attach/detach error is ≥ 10m old),
  `volume.zone_conflict` (critical — PV nodeAffinity zone set does
  not admit the scheduled pod's node zone), and
  `volume.orphaned_attachment` (info — attachment referencing a
  deleted PV or node).
- Milestone drill surfaces (M4 close-out): the dispatcher-level
  quota-exhaustion drill test
  (`internal/watch/quota_drill_test.go`) pinning the correlated
  incident + draft end-to-end (real sources, real pipeline, scripted
  cloud seams); `dev/drills/write-triage-status` — the documented
  STAND-IN for the not-yet-existent agent write path for §9.4
  records (drill fixture, not a product surface; see
  `docs/milestones/M4.md` observation 1); and the real-GCP replay
  runbook `dev/drills/quota-exhaustion.md`.
- Triage-status records (M4, DESIGN.md §9.4) — scans report triaged
  reality. `pkg/memory` gains `TriageStatusRecord` exactly per the
  §9.4 schema (fingerprint, resource_key, session,
  status=investigating|triaged|actioned|escalated, plus the
  sentinel-written lifecycle terminal `resolved`;
  root_cause_hypothesis, severity_override, action, updated;
  wire shape golden-pinned), keyed by the (fingerprint,
  resource_key) pair and stored in the sentinel store (migration
  v4; same in-tree binding decision as §9.2 — core-agent v2.7.0
  ships no Memory interface, `pkg/memory` documents the TODO).
  Consumers: (1) the sentinel's severity routing honors open
  records — an agent's `severity_override` re-routes followups and
  re-pages (downgraded → watchboard/store), `status=escalated` pins
  critical and bypasses the watchboard; matching requires the
  resource pin (object or ControllerRef key) because the §8
  fingerprint is class-level; cached, refreshed every 30s, metrics
  `k8s_event_watcher_triage_overrides_total{action}` /
  `…_triage_resolved_flips_total`. (2) Lifecycle is automatic: a
  §7.4 `kind=resolved` recovery inject flips the record to resolved
  (write-through — routing stops honoring it immediately; reverted
  does NOT restore it, a failed fix pages again). (3) Memory-merged
  `health` and `bundle`: with `--store=<sentinel store>`, findings
  join open records — matched findings gain
  `triage_status/root_cause/action/session/age` and severity
  reflects the agent's judgment (the §14 M4 exit: a health scan run
  mid-incident reports the triage state, not a fresh unknown);
  unmatched findings and runs without `--store` are unchanged.
- Distilled memories (M4, DESIGN.md §9.2): a scheduled distiller pass
  in the sentinel (`--distill-interval`, default 6h, requires
  `--store`) converts recurring raw occurrences into durable,
  agent-queryable `DistilledFact` records (schema-stable JSON:
  class, scope keys, statement, evidence window, occurrence counts,
  source fingerprints). Three predicates ship first, each documented
  in `pkg/memory/distill`: repeated `capacity.stockout` per
  (cluster, zone, nodegroup) — the design's "us-east1-b n2d pool: 3
  stockouts this week"; repeated crashloop/OOM incidents per
  workload (≥2 fresh incidents and ≥5 occurrences in 7d); repeated
  cert-renewal failures per issuer (≥3 failures across ≥2
  certificates). Facts dedupe on (class, scope): re-distilling
  updates the existing fact's window/counts instead of duplicating.
  BINDING NOTE: DESIGN.md §9.2 routes these records through
  core-agent's shared Memory interface, but core-agent v2.7.0 (the
  pinned dep) does not ship it — `pkg/memory` defines lookout's own
  minimal FactWriter/FactReader interface, implemented in-tree over
  the sentinel store (migration v3, `memory_facts`; exempt from the
  §9.1 TTL/size prune), with a documented TODO naming exactly what
  core-agent must expose for the adapter swap. New metrics:
  `k8s_event_watcher_memory_facts_total` (by class),
  `k8s_event_watcher_distill_errors_total`.
- The `quota` source (M4, DESIGN.md §7.2 row 8, §10.2/§10.3) — the
  per-PROJECT leading countdown over cloud quota exhaustion, enabled
  via `--sources=…,quota` on exactly ONE sentinel per GCP project
  (§11 Project tier; fifty clusters must not each poll the quota
  APIs; `Scope()=Project`). Each poll (`--quota-poll`, default 15m)
  reads the provider quota inventory and, for the watched set only
  (top-10 nearest exhaustion by usage/limit ratio plus everything at
  or above `--quota-warn`, default 0.80), fetches usage-vs-limit
  history over `--quota-window` (default 7d) and applies the
  saturation source's regression (exported as
  `saturation.LeastSquaresSlope` — the §10.2 "slope math applies
  directly" seam): the signal says "exhausted in ~6d at current
  slope", never just "at 87%". Emits `quota.forecast` (§7.3,
  APPEND-ONLY) with `Forecast{ETA, "linear-7d-window"}` when a
  positive-slope projection exists; severities are design-fixed
  (warning at ETA < 7d or usage ≥ 90%; critical at ETA < 48h or
  ≥ 98%), with a per-quota hysteresis latch (same severity never
  re-fires across 15m polls; escalation fires once; release only
  after receding below 85% with no urgent ETA). A provider without
  the quota capability is a LOUD startup error naming the source —
  a project-tier deployment without a cloud makes no sense (§11);
  there is no degraded quota mode.
- The §10.3 quota write path, lookout's half: every `quota.forecast`
  carries a DRAFTED `QuotaPreference`-shaped increase request in the
  new additive `quota_increase_draft` payload field (omitempty — all
  other payloads stay byte-identical): canonical quota id
  (`<service>/<quotaId>` from Cloud Quotas metadata, name fallback),
  region, current usage/limit, `suggested_limit =
  ceil(max(limit×1.5, usage + 2×slope/day×7d leadtime))` (formula
  documented at `pkg/sources/quota.Draft`), the fitted slope, and a
  slope-derived justification string — human-grade paperwork the
  agent files through core-agent's PERMISSION GATE. lookout only
  drafts: no `QuotaPreference` create (or any quota mutation) exists
  anywhere in this repository. CLI exposure of the draft (`cloud
  quota --draft`) is a possible follow-up; the §4.1 command surface
  is unchanged here.
- §10.3 correlation: "scaleup failed (`GCE_QUOTA_EXCEEDED`)" +
  "CPUS at 98%, exhausted in ~6 days" is now ONE diagnosed incident.
  APPEND-ONLY `reasonCanonical` additions collapse `quota_forecast`
  (leading) and `quota_blocked` (reactive) into the `QuotaExhausted`
  dedup family, keyed by the canonical quota UID
  `quota:<NAME>/<SCOPE>` — the quota, not the nodegroup, is the
  incident at project scope. The capacity source re-keys
  `capacity.quota_blocked` decisions to that UID when the provider's
  decision message names the quota and its scope (GCE "Quota 'CPUS'
  exceeded … in region us-east1" grammar; conservative
  nodegroup-key fallback otherwise — no false joins), so whichever
  side fires first opens the session and the other attaches as a
  followup via the existing dedup collapse.
- GKE provider: `Quota().History` is live — Cloud Monitoring
  `serviceruntime.googleapis.com/quota/allocation/usage` vs
  `…/quota/limit` series behind a §13 small client interface with
  recorded doc-authored fixtures — and the inventory rows gain a
  canonical increase-request ID (`<service>/<quotaId>`) joined
  best-effort from Cloud Quotas metadata (read-only
  `ListQuotaInfos`; a metadata outage degrades IDs to empty, never
  fails the inventory). New dependency
  `cloud.google.com/go/cloudquotas` (tag-guarded like all GCP SDKs;
  the default build stays GCP-free — nm-verified): the pinned
  `google.golang.org/api` release ships no cloudquotas discovery
  client, and the GAPIC speaks to the same
  `cloudquotas.googleapis.com` surface §10.2 names.
- `stab drift` (M5, DESIGN.md §5; MCP `k8s_gitops_drift`): out-of-band
  drift vs the GitOps manager via `managedFields`, over
  Deployments/StatefulSets/DaemonSets scoped by `--namespace`/`-A`/
  `--workload`. One `drift.manual_edit` finding per (object, foreign
  manager) owning spec fields — warning, critical when a drifted path
  is image/replicas/env — with the manager string, operation, compact
  field paths (capped at 8), and the edit's age; `kubectl-edit`/
  `kubectl-patch`/`kubectl-client-side-apply` managers are recognized
  specially (`tool=kubectl`, reason `KubectlManualEdit`). The declared
  manager comes from `--manager`, defaulting to auto-detect: the
  manager owning the most spec leaf fields across the scope
  (deterministic tie-break; `detection=declared|majority|none` summary
  note). Per the §5 respec, this is MANAGER-level detection only —
  `managedFields` never carries a user identity; identity enrichment
  is a later Cloud Audit Logs query pack, and the command's help says
  so.
- `stab drain` (M5, DESIGN.md §5; MCP `k8s_drain_blockers`): everything
  that will block a node drain or be destroyed by it. `--node` details
  one node — `drain.pdb_gridlock` (critical, one per PDB at
  `disruptionsAllowed=0` covering pods on the node: a gridlocked PDB
  IS a drain blocker), `drain.bare_pod` (warning, no ownerReferences —
  eviction deletes it permanently), `drain.local_storage` (warning,
  emptyDir data a drain destroys), `drain.singleton` (warning,
  single-replica Deployment/StatefulSet/ReplicaSet losing its only
  replica) — plus `drainable=yes|no` and `blockers=<n>` summary notes;
  `-A` is the all-nodes summary (one `drain.node` rollup per blocked
  node, worst-class severity, per-class counts). Mirror pods,
  DaemonSet pods, and completed pods are skipped like a standard
  drain.
- `perf probe --pack=apiserver|apf|etcd|startup` (M5, DESIGN.md §5;
  MCP `k8s_perf_probe`): control-plane and startup performance as
  data-driven metrics query packs executed through the provider
  metrics backend (§2) — apiserver p99 by verb/resource
  (`perf.apiserver_p99`, WATCH/CONNECT/PROXY excluded), APF queue
  saturation + 429 rejects (`perf.apf_saturation`,
  `perf.apf_rejects`), etcd WAL fsync p99 + storage DB size
  (`perf.etcd_fsync`, `perf.etcd_db_size`), pod-first-ready p95 with
  window trend (`perf.startup_p95`). Findings fire per threshold
  breach (max point in the window vs data-declared warn/crit). Packs
  needing GKE control-plane metrics degrade EXPLICITLY when the
  workspace lacks the metric: a `perf.pack_unavailable` warning naming
  the metric and the enable-control-plane-metrics remedy, never
  silence; no provider → the standard `cloud.unavailable` finding +
  summary marker.
- `cloud.SeriesQuery` grows three backend-neutral fields for the packs
  (§15 Q4: every construct has an obvious PromQL equivalent):
  `GroupBy` (cross-series reduction keeping named labels),
  `Percentile` (histogram/distribution quantile), `Rate` (per-second
  counter rate) — plus the `cloud.ErrMetricAbsent` sentinel a backend
  returns only when it can POSITIVELY determine the metric is absent.
- GKE provider: the Cloud Monitoring `MetricsBackend` is live
  (CapabilityMetrics now project-scoped, no longer deferred) — a §13
  small client interface over `timeSeries.list` +
  `metricDescriptors.get` with doc-authored recorded fixtures, and a
  translation table from backend-neutral metric names to Monitoring
  types (control-plane `prometheus.googleapis.com/...` metrics, GKE
  system metrics, and `triage top --history`'s container CPU/memory
  names, so `--history` now works on GKE). Absence detection probes
  the metric descriptor after an empty control-plane read: 404 →
  `ErrMetricAbsent`. Doc-driven mapping note recorded in `mtTable`:
  GKE ships no etcd metrics package today — DB size maps to
  `apiserver_storage_size_bytes` (what the ~6GiB etcd quota is
  enforced against) and the fsync query reports `pack_unavailable`
  until Google exposes the metric.

### Fixed

- The five M3-drill observations (`docs/milestones/M3.md`
  §Observations):
  1. `store.GraphAt` now replays across sentinel restarts. Every
     graph snapshot and change-log row is stamped with a per-process
     EPOCH id (store migration v5, additive columns; pre-migration
     rows backfill to `''` and are treated as one epoch), and
     resolution is epoch-scoped: nearest snapshot ≤ t (any epoch),
     then only THAT epoch's changes replay — one process's delta log
     is never decoded against another process's interner (the drill's
     `graph: bad change effect` failure is gone). Boundary semantics,
     documented on `GraphAt`: a `--at` falling in the gap between
     epochs (sentinel down, or the new process's pre-baseline window)
     resolves to the prior epoch's last known state — nothing was
     observing the cluster then, so the last observed state is the
     honest answer; times from the new epoch's first snapshot onward
     resolve within the new epoch.
  2. `Snapshot.Watches` survives serialization: LKGH format v2
     appends the watched-kind set, and restore/replay carry it
     through the store — history-mode `triage radius` keeps the #46
     unknown-vs-missing honesty (`observed=unknown` for identity-only
     neighbors of unwatched kinds, never `ReferencedNotFound`). v1
     files remain restorable forever (frozen contract, pinned by a
     golden-blob test) and read as watch-everything, exactly the
     posture they were written under.
  3. Historical queries can target Deployments: `triage radius` and
     `triage changes` with `--at` resolve identity-only targets of
     unwatched kinds through the owner chain present in the
     historical graph (Deployment identity node → Owns edges → its
     RS/pods). A target genuinely absent at t now errors with "was
     not in the watched topology as of <t>" plus what to try — not a
     bare "not found".
  4. `rollout_stall` resolved records honor the §7.4 stability window
     and carry correct durations: the clearance observer stamps
     `StableSince` at the sweep that OBSERVES the not-complete →
     complete transition (never a completion from before the
     incident), so a rollback debounces `--recovery-stable-for` like
     every other observer and the §9.3 corpus fields come out right
     (`cleared_after` = fire → clearance, `observed_stable_for` ≥ the
     window; no longer inverted or zeroed). Clearance for every other
     kind is untouched.
  5. Informer-sync Adds no longer masquerade as changes: the graph
     feed's initial-LIST replay applies through the new
     `graph.Writer.ApplyInitial` (folds state and seeds change
     tracking, emits no ChangeRecords) and the §6.6 delta log arms
     only after initial sync — the signal sources' arm-after-sync
     discipline — so a `triage changes` window spanning a sentinel
     restart no longer reports every pre-existing object as `Added`
     at the sync instant.

## [0.4.0] - 2026-07-26

M3 — leading indicators + history (DESIGN.md §14). Exit criterion verified
in `docs/milestones/M3.md`: a staged bad deploy opened a session 3m10s
after the rollout while every user request kept returning 200 (old
revision serving under `maxUnavailable=0`); a staged memory leak opened a
critical session 14 minutes before the OOM kill, with the forecast ETA
accurate to 31 seconds; "blast radius at onset" was answered 28m34s after
the fact — offline, from a copied sentinel store — naming the bad-image
rollout as the last change before onset.

### Fixed

- The shipped ClusterRole now grants `get` on `apps/deployments`,
  `apps/replicasets`, and `pods/log` — the M2 drill showed enrichment's
  spec and logs sections failing in-cluster without them (5 of 8 runs
  `outcome=partial`; see `docs/milestones/M2.md` §Observations).
- Node-scoped incidents now resolve (M2 drill §Observations item 2):
  the object-state source's node informer feeds a §7.4 `NodeClearance`
  observer — node Ready=True stable for `--recovery-stable-for` →
  `kind=resolved` (resolution=recovered; a node deleted with no
  same-name replacement closes as object_deleted). Consequence: a
  node-anchored storm can now FULLY resolve — the storm's aggregate
  `kind=resolved` fires when all members including the node incident
  clear (drill A ended 32/33 with the storm session left to the idle
  TTL).
- Live-graph enrichment no longer mislabels existing-but-unwatched
  objects as dangling (M2 drill §Observations item 3): the topology
  graph now declares which kinds its ingest watches
  (`graph.Options.WatchedKinds` / `Snapshot.Watches`), and the radius
  section claims `radius.missing reason=ReferencedNotFound` only for
  watched kinds — a mount-referenced ConfigMap/Secret/PVC the live
  informer set deliberately holds identity-only renders as
  `radius.neighbor` with `observed=unknown` instead. One-shot
  full-List bundles (CLI) watch everything and are byte-unchanged.
- Storm sessions now carry size freshness (M2 drill §Observations
  item 4): the formation `kind=storm` payload stays byte-frozen
  (schema stability), and a NEW schema-stable `kind=storm.update`
  followup (`affected_count`, `namespaces_count`,
  `new_members_since_last` — pinned byte-exact) is injected into the
  storm session when membership doubles or grows by 10 since the last
  size report, rate-limited to one per minute
  (`k8s_event_watcher_storm_updates_total` counts them).

### Added

- The `cloud` command group (M4, DESIGN.md §5): `cloud
  stockout|orphans|ipspace|quota` — the GCP-side point-in-time reads,
  exposed over MCP as `k8s_cloud_stockout` / `k8s_cloud_orphans` /
  `k8s_cloud_ipspace` / `k8s_cloud_quota`. `stockout` extracts
  ZONE_RESOURCE_POOL_EXHAUSTED per zone/machine-type over `--since`
  (default 24h) with an event-derived reroute suggestion (same-region
  zones active in the window with no stockout for that type — never
  claims knowledge outside the log window). `orphans`
  (`--only=disks,lbs`, `--min-age` default 24h) reports unattached
  billing-active disks (size/type/idle-age; undatable disks reported
  with age unknown, never dropped) and forwarding rules resolving to
  zero endpoints across backend-service, URL-map, and target-pool
  shapes. `ipspace` judges pod/node range utilization at the fixed
  §5 lines (warning ≥80%, critical ≥95%); the GKE services range is
  reported as explicitly not-cloud-visible rather than a fake 0%.
  `quota` ranks per-project usage/limit nearest-to-exhaustion with
  findings from `--quota-warn` (default 80%, critical fixed at 95%,
  `QuotaExhausted` at 100%); the summary counts every quota scanned.
  All four go through pkg/cloud capabilities only: no provider (or
  missing identity) yields the §2 explicit `cloud.unavailable`
  finding + `unavailable reason="…"` summary marker at exit 0. The
  gke provider implements the four capabilities against
  google.golang.org/api (compute/logging/container) behind small
  per-call client interfaces with doc-authored recorded fixtures
  (§13); all GCP imports stay behind the `gke`/`allproviders` tags,
  and a new `go tool nm`-based conformance test in cmd/lookout fails
  if the default binary ever links a GCP symbol.

- MOVED TO 0.5.0 (release-cut correction): the triage-status records
  (#51) and distilled memories (#50) entries originally appended here
  landed after the v0.4.0 tag was cut at #49; they are recorded under
  0.5.0, their first released version.

- The `degradation` and `expiry` sources (M3, DESIGN.md §7.2 rows 5–6)
  — two leading indicators, enabled via `--sources=…,degradation,expiry`
  (ADDITIVE; the default stays k8s-events only).
  `degradation.capacity` (§7.3) is a TREND on per-Service
  EndpointSlice ready RATIOS — "payment-backend capacity 5/5 → 3/5
  over 10 min" — distinct from `objectstate.endpoints_empty` (the
  >0→0 transition) by construction: it NEVER fires at ratio 0. Exact
  predicate (documented in the package): ratio samples per Service
  (coalesced to 10s so one sharded controller write is one step, plus
  a low-frequency tick), fire iff the ratio dropped ≥
  `--degradation-drop` (default 0.3) from the window start
  (`--degradation-window`, default 15m) AND ≥ 2 distinct downward
  steps — no single blip fires, however deep. Warning; critical at
  surviving ratio ≤ 0.5; single-fire per decline episode with
  evidence carrying from/to/desired counts, window, and the compact
  step timeline. `degradation.probe_flap` catches pods whose
  readiness gate flipped ≥ 4 times in-window but never sustained
  failure long enough for the reactive `Unhealthy` path (flip
  counting encodes "below the threshold" structurally: down-and-
  stays-down is one transition). Pod + EndpointSlice informers are
  shared with the §6.3 factory under `--storm`; arm-after-sync
  restart discipline as object-state; both kinds carry §7.4
  clearance (ratio back at the fire-time baseline or 1.0; pod Ready
  with no in-window flips), registered AHEAD of the generic pod
  observer so flap incidents aren't resolved by "pod reads Ready
  between flaps".
  `expiry.warning` (§7.3) is the leading COUNTDOWN over TLS secret
  certs, Validating/MutatingWebhookConfiguration caBundles,
  ServiceAccount token JWT `exp` claims where detectable, and —
  discovery-gated — cert-manager Certificate CRs (absent CRD = one
  loud startup log line, never a silent skip; present CRD attributes
  cert-manager-owned Secrets to their Certificate so one failing
  renewal is one incident). Warning at `--expiry-warn` (default
  336h/14d), critical at the design-fixed 72h or when the last
  renewal FAILED (regardless of window, per the §7.2 example); fires
  once per (object, threshold crossing), re-checked every
  `--expiry-interval` (default 1h) scan; renewal (notAfter moved back
  out) resets the latch and is the §7.4 clearance. Every signal
  carries the §8 `Forecast{ETA: notAfter,
  "certificate-notAfter"}`, now serialized as the ADDITIVE
  `forecast` payload field (omitempty — reactive payloads stay
  byte-identical). Scan model: periodic PAGED LISTs, deliberately NO
  Secret informer — an informer would hold every Secret in scope
  resident in the sentinel's heap; the scan holds one page
  transiently and retains only (identity, notAfter, subject CN,
  issuer CN). RBAC: the secrets `list` is the sentinel's first
  secret-value read and is declared + probed (§11) and flagged as
  the tradeoff in `deploy/12-clusterrole-watcher.yaml`; scope it
  with `--expiry-namespaces` (default all — the declared probe
  requirements narrow with the flag). New grants: secrets/
  serviceaccounts list, webhook configurations list, cert-manager
  certificates list (inert without the CRD). The degradation source
  adds NO new RBAC.

- The sentinel-local raw-occurrence store (M3, DESIGN.md §9.1) —
  `pkg/store`, ONE bounded, TTL'd embedded SQLite database holding every
  emitted signal (info severity included) together with the routing
  outcome it received: the lookback substrate for storm correlation,
  §7.4 resolved stability windows, digests, and recommendation history.
  Opt-in via the new ADDITIVE `--store=<path>` flag on `lookout watch`
  (default empty = disabled, M2 behavior byte-for-byte; the path is
  always explicit — put it on the `--dedup-persist` volume). The
  dispatcher records EVERY post-dedup signal with its outcome
  (`injected`|`suppressed`|`storm`|`storm-member`|`watchboard`|
  `info-stored`|`resolved`), and the §7.7 info class is now PERSISTED
  instead of counted-and-dropped when the store is enabled (the frozen
  `info_dropped_total` metric keeps counting the class either way).
  Each `occurrences` row carries kind/source/severity, the §8
  fingerprint, object identity, raw + canonical reason, a size-capped
  message, count and first/last seen, nullable session / storm
  fingerprint / forecast ETA, and the compact raw Signal JSON
  (enrichment stripped) for later distillation (§9.2) — indexed on
  (fingerprint, emitted_at), (uid, emitted_at), (severity, emitted_at),
  behind forward-only `schema_version` migrations (§6.6 graph snapshots
  + the delta log land later as separate tables). The store is
  telemetry, not a system of record: writes go through a non-blocking
  buffered writer goroutine that drops loudly on overflow
  (`store_write_drops_total{cause}`) rather than ever stalling the
  inject pipeline; WAL mode + busy timeout keep concurrent reads safe.
  Retention: `--store-ttl` (default 720h — the §9.1 30 days) and
  `--store-max-mb` (default 512) with a prune loop every
  min(1h, ttl/24) deleting expired rows first, then oldest-first when
  the size bound is exceeded — both loud in logs and
  `store_pruned_rows_total{cause}`. Minimal M3 query surface:
  `RecentByFingerprint`, `RecentByObject`, `CountsBySeverity`, and a
  limit-bounded newest-first iterator for digests. New dependency:
  `modernc.org/sqlite` v1.54.0 — the pure-Go, CGO-free SQLite required
  by the distroless static release image (built with `CGO_ENABLED=0`).
  New metrics: `store_records_total{route}`,
  `store_write_drops_total{cause}`, `store_pruned_rows_total{cause}`.

- Graph history (M3, DESIGN.md §6.6) — point-in-time topology:
  persistence for time-travel, not crash recovery. `pkg/graph` snapshots
  now serialize to a versioned compressed binary (`Snapshot.Encode` /
  `graph.Restore`: 4-byte `LKGH` magic + format-version byte +
  gzip-compressed node/edge/interner arrays, stdlib gzip — no new
  dependency at a 5-minute cadence; every structural invariant is
  re-validated on Restore, so corrupt or truncated blobs error, never
  panic or misread). With the new `graph.Options.OnChange` hook armed,
  every applied `Writer.Apply` delta emits one `ChangeRecord{At,
  Generation, Op, Kind/Namespace/Name/UID, FieldChanges, Effect}` — one
  log, two consumers (§6.6): `FieldChanges` is the `triage changes`
  summary, derived in the graph ingest where the typed objects are
  visible, carrying NAMES/HASHES/COUNTS only, never values — container
  image changes (`container/<name>/image`), `replicas`, label changes as
  CHANGED KEYS with 8-hex value hashes (`label/<key>`),
  ConfigMap/Secret mount-reference changes (`mount/<Kind>/<name>`),
  Node `unschedulable`, and ConfigMap/Secret CONTENT hashes (`data`,
  16-hex sha256 over sorted keys+values — the §6.5 "names, keys, and
  content hashes" half; secret values pass through the hash and are
  dropped, and the shipped sentinel's informers don't watch
  ConfigMaps/Secrets yet, so content-hash tracking stays dormant until
  the informer set grows — it lights up automatically for objects
  routed through `Writer.Apply`). `Effect` is the delta's graph
  mutations recorded at the primitive level (node new/observed/
  unobserved/collected, edge add/remove — a decision beyond the doc's
  record sketch: a names/hashes summary alone cannot rebuild edges), so
  `graph.NewReplayer(base).Apply(...)` reproduces the live graph BY
  CONSTRUCTION — the §13 round-trip invariant (snapshot + replayed
  deltas ≡ live graph at the same generation, NodeIDs included) holds
  as deep equality over a synthetic-cluster churn test.
  `pkg/store` migration v2 adds `graph_snapshots` (taken_at,
  resource_version — carrying the graph's monotonic snapshot
  generation, since no single cluster-wide k8s resourceVersion exists
  across informers — format_version, size_bytes, compressed blob) and
  `graph_changes` (at, generation, op, identity, `changes` JSON
  summary, `effect` replay blob) to the SAME sentinel SQLite file:
  change rows ride the existing non-blocking buffered writer (a
  dropped row degrades `--at` precision inside one snapshot interval,
  never later correctness); snapshots insert synchronously from the
  5-minute loop. Same TTL/size pruning, with one carve-out: the NEWEST
  snapshot survives both bounds even past TTL (zero snapshots would
  make every logged change unreplayable; a snapshot describes current
  state, so keeping it leaks no expired history). `store.GraphAt(t)`
  resolves nearest snapshot ≤ t + replays logged changes forward
  (boundaries inclusive: a change stamped exactly at t is part of the
  answer), `store.GraphChanges(from, to]` is the `triage changes`
  feed, and `store.OpenRead` opens a sentinel's store read-only for
  one-shot CLI use. `lookout watch` wires it up when BOTH `--store`
  and the graph feed (`--storm`) are on: per-delta change logging plus
  a snapshot loop — new ADDITIVE flag `--graph-snapshot-interval`
  (default 5m, §6.6), with a fast-poll bootstrap so the baseline
  snapshot lands right after initial sync and generation-deduped ticks
  when topology hasn't changed. CLI contract (§4.2): `checks.Command`
  gains `GraphBacked`; graph-backed commands (and only they — live-only
  commands reject the flag as unknown) accept `--at=<RFC3339|dur-ago>`
  + `--store=<path>` on every surface (--help, MCP schema, and flag
  parsing all generate from the one declaration); `--at` without
  `--store` is a usage error naming the requirement, because history
  is a watch-path feature and one-shot invocations answer live-only
  without a sentinel's store. No production command consumes `--at`
  yet — `triage radius --at` and `triage changes` come next; a hidden
  graph-backed probe (`checktest.GraphAtCommand`) pins the plumbing
  end-to-end (flag gating → Scope → read-only open → GraphAt).

- `lookout triage events` (M3, DESIGN.md §5 — absorbs v2's ev-sifter +
  hpa-loop-catcher): the deduped chronological event timeline. One
  paged List of core/v1 Events, collapsed by the SAME identity the
  `lookout watch` sentinel dedups on — (involvedObject.uid, canonical
  reason via `engine.CanonicalReason`), so ErrImagePull/ImagePullBackOff
  and BackOff/CrashLoopBackOff families merge here exactly as they
  merge into one incident session (pull mode of the same filter/dedup;
  `engine.DedupCache` itself is deliberately NOT reused — its rolling
  window/TTL, LRU, and session bindings are session-routing semantics,
  wrong for a one-shot bounded window, documented in the package).
  Targeting: `--workload=<Kind>/<ns>/<name>` scopes to the target's
  whole owner-reference tree — climb to the root owner, include every
  descendant, siblings too — resolved from the same one-List-pass
  `pkg/graph` snapshot `state edges`/`bundle` build; `--namespace`/`-A`
  give the untreed namespace timeline. Entries order by newest activity
  (`lastTimestamp`), Warning events emit `kind=event.warning`
  severity=warning, Normal emit `kind=event.normal` severity=info, each
  with summed repeat counts, first/last seen, collapsed reason
  `variants`, and the reporting `source`; `--since` honored (default
  1h) and the summary line counts events scanned. HPA thrash detection
  is an analysis mode here because the HPA object keeps NO replica
  history: the replica sequence is recovered from SuccessfulRescale
  events ("New size: N", including each aggregated event's first/last
  envelope), and ≥`--hpa-flips` (default 2, i.e. up→down→up)
  scale-direction changes inside one `--hpa-window` (default 30m) emit
  `kind=event.hpa_thrash` (warning) with the replica sequence, flip
  count, window, and — in workload mode, where HPAs targeting into the
  tree are resolved via an autoscaling/v2 List — the scaleTargetRef; a
  monotonic ramp never fires (zero direction changes). MCP:
  `k8s_event_timeline`.

- `lookout net probe` (M3, DESIGN.md §5): active DNS/TCP/HTTP checks
  for hypothesis CONFIRMATION — `--dns=<name,...>`, `--tcp=<host:port,...>`,
  `--http=<url,...>` (at least one required), `--probe-timeout` (default
  5s per probe). Zero cluster mutation and zero Kubernetes API use; the
  vantage point is WHEREVER lookout runs — in a pod you get the
  in-cluster view (cluster DNS, Service VIPs, NetworkPolicies as that
  pod experiences them), on a laptop you get the laptop's network; no
  pod is ever spawned. Because targets are not Kubernetes objects, the
  §4.2 scoping flags (`--workload`, `--namespace`/`-A`, `--since`) are
  REJECTED as usage errors instead of silently ignored. Findings:
  `probe.dns` (sorted resolved IPs + latency), `probe.tcp` (connect
  latency), `probe.http` (GET only, redirects reported as their 3xx and
  not followed, bodies never read — status + latency + declared
  Content-Length). Failures carry a machine-matchable `error_class`
  (`nxdomain|timeout|refused|unreachable|reset|cert|http_4xx|http_5xx|
  error`; TLS verification failures are their own `cert` class) with a
  uniform severity policy: definitive negatives (nxdomain, refused,
  unreachable, reset, cert, 5xx) are critical; indeterminate outcomes
  (timeout — could be policy, load, or vantage; 4xx — reachable and
  serving, request turned away) are warning. MCP: `k8s_net_probe`.

- `triage radius` and `triage changes` (M3, DESIGN.md §5) — the two
  history-consuming read-path commands, MCP tools `k8s_blast_radius`
  and `k8s_recent_changes` (§4.3). `triage radius <Kind>/[<ns>/]<name>`
  enumerates *impact* (complementing `state edges`, which verifies
  *correctness*): every neighbor of a pod/workload with
  `direction=upstream|lateral|downstream`, `relation` (the attaching
  edge kind — RoutesTo, Owns, Selects, Governs, RunsOn, Mounts — or
  `shared-node|zone|config|secret|pvc` with a `shared=<Kind>/<name>`
  anchor for co-tenants), `hop` count, pod `ready` where the live List
  pass knows it, and `radius.missing` warnings for dangling references;
  `--depth` defaults to 3. Live mode reuses the `state.LoadCluster` one
  List pass; `--at=<t> --store=<path>` answers "what was the blast
  radius at onset" entirely from the sentinel store (`store.GraphAt`,
  no cluster access — post-mortems work offline), and readiness is
  deliberately not claimed from history (topology is stored, status is
  not). `triage changes` answers "what changed in the N minutes before
  onset" (`--since`, default 30m), scoped to the target's graph
  neighborhood (`--depth`, default 2): chronological
  `change.rollout|config|secret|scale|label|node|topology` findings
  with field changes rendered compactly (`path=from→to`, hashes
  shortened, never values), each tagged with its neighborhood
  `relation` and provenance `origin=log|event|api`. With `--store` the
  §6.6 delta log is the source; `--at` shifts the window to end at
  onset and takes the neighborhood from the point-in-time graph. HPA
  `SuccessfulRescale` events join from the live timeline (the HPA
  keeps no replica history, §5). WITHOUT a store the command degrades
  honestly to what the API can still tell now — ReplicaSet revision
  annotations + creation timestamps for rollouts, recent scaling
  events — and cannot see un-timestamped updates (ConfigMap/Secret
  edits, label flips, old cordons); the summary says
  `source=live-approximation`. Which topology answered is always on
  the summary line: `emit.Writer` gains `Note(key, value)` — ordered
  annotations after the mandatory `scanned=/findings=/elapsed=` keys
  (`source=live|history|live-approximation`, resolved `at=`,
  `window=`), byte-identical output when unused, note keys
  contract-checked against the command glossary like any Details key.
  Plumbing shared, not duplicated: `bundle`'s radius merge is exported
  as `bundle.RadiusNeighbors` (bundle output unchanged), the
  `graph.Hit` traversal result now carries `Via` (first-reaching edge
  kind) and `Anchor` (lateral shared node), and `state.Cluster` gains
  `Pod`/`Objects` accessors for readiness and typed-object reads.
  Delta-log records skipped on purpose in `triage changes`:
  EndpointSlice churn (pure pod-IP noise — the pod-level change is the
  signal) and updates whose tracked fields did not change (zero
  nominal state).

- The `rollout` and `saturation` signal sources (M3, DESIGN.md §7.2
  rows 3–4) — the first as-it-happens and trend leading indicators.
  `rollout` (`--sources=...,rollout`) watches Deployments and
  StatefulSets with in-progress rollouts through shared-factory
  informers (Deployments + ReplicaSets + StatefulSets + pods; rides
  the storm graph's factory when `--storm` is on) and emits
  `rollout.stall` (warning) when the new revision has made ZERO
  ready-count progress for `--rollout-observe` (default 3m) while the
  old revision stays ≥90% ready — the §7.2 "new RS 0/1 ready for
  4 min, old RS healthy — probable bad deploy", fired well before
  `progressDeadlineSeconds` and EVIDENCE-based, distinct from
  `objectstate.progress_deadline`'s deadline clock. Evidence rides the
  message (`new_ready=/old_ready=/elapsed=/top_waiting_reason=`);
  fires once per revision (dedup uid = workload UID, reason
  `rollout_stall`); any new-pod readiness increase resets the window,
  so slow-but-progressing rollouts never fire, and initial deploys
  (no old-healthy baseline) are out of scope by design. `saturation`
  (`--sources=...,saturation`) is v2 top-analyzer's regression math
  resident: every `--saturation-interval` (default 30s) it samples
  container CPU/memory from `metrics.k8s.io` against LIMITS and PVC
  usage from each kubelet's stats summary (`nodes/proxy` GET;
  unreachable → ONE loud log, PVC dimension skipped, auto-resumes —
  portable per §2; an unavailable metrics API is instead a loud
  startup error per §11), fits a least-squares line per (object,
  resource) over `--saturation-window` (default 90m) and emits
  `saturation.forecast` with the §8
  `forecast{eta, confidence_basis:"linear-90m-window"}` attachment
  (also on the wire: `inject.Payload` gains the ADDITIVE omitempty
  `forecast` key, wire-pinned; non-trend payloads stay byte-identical
  to the frozen shapes) — warning below `--saturation-warn` (default
  60m to exhaustion),
  critical below 15m; NO forecast on <8 samples, span under half the
  window, non-positive slope, or no limit. Hysteresis: the fired
  severity latches per target (same severity never re-fires;
  escalation warning→critical fires once), releasing when the ETA
  recedes beyond 2× the warn threshold or the slope stays
  non-positive for a full re-observation period (window/2). Both
  sources implement §7.4 ClearanceObservers wired into recovery:
  rollout stalls clear on completion OR rollback (`recovered`) and on
  workload deletion (`object_deleted`); saturation clears on the same
  recede/non-positive-slope rules (the observer deliberately CLAIMS
  every `forecast_*` incident and is registered ahead of pod-scoped
  observers — a leaking pod is Ready right up to the OOM kill, so
  pod-readiness must never judge these), and the fallback pod
  observer's missing RBAC no longer disables recovery outright when
  trend observers exist. New dependency: `k8s.io/metrics` v0.36.3
  (same version as the existing k8s.io/* pins). RBAC:
  `sources.Requirement` gains `Subresource`; the shipped ClusterRole
  adds `apps/statefulsets` list/watch, `metrics.k8s.io pods` get/list,
  and `nodes/proxy` get — all harmless while the sources stay
  disabled (the `--sources` default is unchanged: `k8s-events` only).

- The `capacity` signal source (M4, DESIGN.md §7.2 row 7, §10.1) —
  cluster-autoscaler signals from STRUCTURED sources, never the CA
  text log. Enabled via `--sources=…,capacity` (ADDITIVE; the default
  stays k8s-events only). Four sub-sources, one source:
  (1) CA Kubernetes Events, watched by the capacity source's OWN
  event-informer filter — the k8s-events `--reason` default is
  untouched; the capacity source owns these reasons —
  `NotTriggerScaleUp` → `capacity.pending` (warning, per-nodegroup
  rejection reasons parsed from the real message shapes incl.
  multi-nodegroup lists and comma-bearing taint reasons),
  `TriggeredScaleUp` → `capacity.scaleup` (info), the ScaleDown
  family → `capacity.scaledown` (info; ScaleDownFailed warning).
  Event edges arm only after cache sync (the initial LIST is stale
  history; the polled sub-sources own current state).
  (2) The `cluster-autoscaler-status` ConfigMap in kube-system,
  polled every `--capacity-poll` (default 60s), BOTH formats — the
  legacy text block and the CA ≥ 1.30 yaml document (detected by the
  `autoscalerStatus:` key): a nodegroup whose `cloudProviderTarget`
  exceeds `ready` sustained > 3m fires `capacity.scaleup_gap`
  ("asked for a node, didn't get one") — warning, critical when the
  nodegroup's scale-up is in Backoff WITH a recorded error (yaml
  `backoffInfo`); per-episode latch, nodegroup evidence
  (target/registered/ready, health, backoff detail) in the message.
  (3) Provider scale decisions through the §2 boundary
  (`cloud.CapacityAPI` — on GKE the `cluster-autoscaler-visibility`
  Cloud Logging stream): `GCE_STOCKOUT` → `capacity.stockout`,
  quota reasons → `capacity.quota_blocked`, IP exhaustion →
  `capacity.ip_exhausted` (all critical — the §10.1 remedy-disjoint
  trio); no provider → the sub-source is OFF with the standard §2
  `unavailable reason="…"` startup log, and the portable sub-sources
  still fire on every scaleup failure.
  (4) Pending-pod aging, the resident TRENDING version of `triage
  delta`'s point-in-time scan: pods Pending+Unschedulable (the
  scheduler's own PodScheduled=False/Unschedulable verdict) longer
  than `--pending-age` (default 5m) fire `capacity.pending-aged`
  (warning; critical at the design-fixed 15m), carrying the
  scheduler's message as evidence.
  Dedup families (M2 pattern, APPEND-ONLY): `pending` and
  `pending-aged` join the scheduler's `FailedScheduling` family on
  the Pod UID — one unplaceable pod is ONE session across all three
  observers. `pkg/cloud/gke` implements `CapacityAPI` against Cloud
  Logging (logadmin), parsing documented noScaleUp/scaleUp/
  eventResult visibility records (per-MIG reasons; error messageIds
  normalized: `scale.up.error.out.of.resources` → `GCE_STOCKOUT`,
  `…quota.exceeded` → `GCE_QUOTA_EXCEEDED`, `…ip.space.exhausted` →
  `IP_SPACE_EXHAUSTED`) behind a small `EntryLister` interface with
  authored-from-docs JSON fixtures (§13; no live-project tests). New
  dependency: `cloud.google.com/go/logging` (+transitives), imported
  ONLY under the `gke`/`allproviders` build tags — `go tool nm` on
  the default binary shows zero `cloud.google.com/go/logging` and
  zero `pkg/cloud/gke` symbols (the #19 conformance tests still pin
  this). RBAC: `sources.Requirement` gains `Name` (SSAR
  ResourceAttributes.Name) so the source's one extra read — `get` on
  the `cluster-autoscaler-status` ConfigMap — is declared
  name-scoped and satisfied by the new kube-system Role pinned with
  `resourceNames` (`deploy/14-role-watcher-capacity.yaml` +
  `15-rolebinding-watcher-capacity.yaml`) instead of widening the
  ClusterRole; events/pods informers ride the existing grants,
  verified loudly at startup (§11).
### Docs

- Skills teach the M3 command surface (DESIGN.md §4.4): `k8s-triage`
  replaces its "M3 gaps" fallback notes with the real workflow —
  `triage changes` as the first question on sudden regressions,
  event timeline vs logs, `triage radius` for impact, `net probe`
  for hypothesis confirmation, and `--at` + `--store` post-hoc
  analysis; `cluster-health` gains changes/radius drill-down rows;
  the CrashLoopBackOff and FailedMount playbooks gain changes-first
  and event-timeline steps, plus a new `playbooks/hpa-thrash.md`;
  reference stubs regenerated for the four new commands.

- `lookout triage top` (M3, DESIGN.md §5 — v2 top-analyzer, point-in-
  time half): CPU/memory saturation vs LIMITS, right now, from ONE
  metrics.k8s.io read over the scope (`--namespace` | `-A` |
  `--workload`, which resolves member pods through the same
  one-List-pass graph owner-chain `triage events`/`bundle` use). The
  metrics-client join is NOT duplicated: the saturation source's
  fetcher seam is reused via the new
  `saturation.NewScopedMetricsPodFetcher` (same code path the sentinel
  runs, narrowed to the asked-about namespace). Findings only at/above
  `--top-warn` (default 80%, zero nominal state); `--all` dumps every
  sampled row (info below the threshold) sorted by pct descending and
  capped at `--limit` (default 50). The severity asymmetry is the
  point and is documented in command help and package doc: MEMORY
  ≥95% of limit is CRITICAL (incompressible — the kernel kills at the
  limit), while CPU CAPS AT WARNING at any percentage (compressible —
  over-limit throttles, never kills, and a single sample cannot prove
  the sustained starvation a critical would claim; that proof lives in
  `--history` or the sentinel's saturation source, which keeps the
  slope→ETA math per the §5 respec). Containers with no cpu/memory
  limit are invisible to a usage-vs-limit judgment, so they surface as
  ONE aggregate info `top.unlimited` finding counting pods+containers
  (`--show-unlimited` lists each as `top.unlimited_container` with the
  `missing` dimensions). With `-A`, the node dimension is added:
  `top.node` usage vs allocatable per node (`NodeMemoryPressure` /
  `NodeCPUPressure` precursors, same asymmetry). `--history=<dur>`
  goes through the `pkg/cloud` provider boundary (Metrics capability):
  max/avg/p95 usage-vs-limit percent per container finding over the
  window, queried in the backend-neutral `cloud.SeriesQuery` shape
  (`container/cpu/used_millicores`, `container/memory/used_bytes` —
  the GKE translation lands with the M4 backend); with no provider the
  command emits the §2-mandated EXPLICIT degradation — a
  `cloud.unavailable` finding plus the
  `unavailable="no cloud provider configured"` summary marker — and
  the point-in-time findings are unaffected. MCP: `k8s_resource_top`.

## [0.3.0] - 2026-07-25

M2 — closed loop (DESIGN.md §14). Exit criterion verified in
`docs/milestones/M2.md`: the node-failure drill produced 1 storm session
(3 session creates for 33 affected objects), not 30; fix-verify
round-tripped without agent polling — a config fix became a `kind=resolved`
record in the incident's own session 76 seconds later.

### Added

- The Signal engine foundation (M2, DESIGN.md §7.2/§7.3/§8): `TriageEvent`
  generalizes to `engine.Signal` — kind, source (`sentinel`|`scan`),
  severity (`critical`|`warning`|`info`), cluster/project/zone, optional
  forecast, and the frozen cross-cluster `fingerprint`
  (`sha256(kind ⊕ reason-class ⊕ object-class ⊕ zone)`, NUL-separated —
  the failure *class*, never the object name — pinned by contract-test
  vectors so fleet rollup joins stay stable across versions).
  `pkg/sources` carries the §7.2 `Source` interface
  (`Name`/`Scope`/`Run(ctx, emit)`), a source registry, and the §11
  startup capability probe: sources declare required RBAC and the
  sentinel SelfSubjectAccessReview-checks each one, failing loudly with
  the source name and missing permission instead of running a silently
  empty watch. The M0 event watcher is refactored into
  `pkg/sources/k8sevents`, the first Source, and `lookout watch` consumes
  it through the interface — flags, defaults, metrics names, and the
  `k8s-event` inject payload remain byte-identical (the frozen wire-shape
  and flag-surface tests pass unchanged, plus a new pin proving the
  Signal path emits the same bytes).
- Recovery injects — the fix-verify closed loop (M2, DESIGN.md §7.4/§9.3):
  the sentinel now watches every bound incident for the *absence* of its
  symptom. A new `engine.RecoveryTracker` runs the per-incident state
  machine (symptomatic → clearing → resolved → reverted) against
  clearance predicates provided by observers, with a stability window
  set by the new `--recovery-stable-for` flag (default 5m; 0 disables).
  This PR's observer covers pod-scoped incidents via a minimal pod
  informer: cleared when the pod is Ready and restart-stable for the
  window; a deleted pod whose controller has a Ready replacement counts
  as recovered (owner-based); deleted with the owner gone closes the
  incident as `resolution=object_deleted` — explicitly distinct from
  fixed. Outcomes are injected into the incident's own session as
  schema-stable structured payloads (`kind=resolved` /
  `resolved.reverted` with `cleared_after`, `observed_stable_for`,
  `resolution`, `reverted_after`, and the ORIGINAL incident's
  fingerprint; byte-exact wire pin), never prose — the §9.3 corpus
  contract. Session bindings now persist incident identity through
  `--dedup-persist` (version-tolerant: old snapshots load unchanged), so
  recovery tracking survives a sentinel restart; a resolved signal whose
  binding is lost is logged, counted, and dropped. New metrics:
  `recoveries_observed_total{resolution}`, `recoveries_reverted_total`,
  `recovery_tracking`, `recovery_drops_total{cause}`. The shipped
  ClusterRole gains pods `list`/`watch`; deployments still running the
  M0 role keep working with recovery disabled (loud startup log, no
  crash-loop).
- The `object-state` source (M2, DESIGN.md §7.2 row 2) — leading
  indicators from state transitions, watched by shared informers on
  Pods, Nodes, Deployments, EndpointSlices, and PodDisruptionBudgets.
  New source-namespaced signal kinds (append-only; the dedup reason is
  the kind suffix): `objectstate.node_notready` (Ready True→False,
  Unknown counts as down; critical), `objectstate.node_flapping` (N
  Ready transitions within a window, default 3/10m; warning),
  `objectstate.progress_deadline` (rollout stalled past 80% of
  `progressDeadlineSeconds` with unready replicas — fired BEFORE the
  control plane's ProgressDeadlineExceeded event; warning),
  `objectstate.endpoints_empty` (a Service's ready-endpoint count
  across its EndpointSlices transitions >0 → 0; created-empty never
  fires; critical), `objectstate.pdb_gridlocked` (`disruptionsAllowed`
  transitions >0 → 0 while pods exist behind the PDB — the transition
  counterpart of `triage delta`'s scan; warning), and
  `objectstate.restart_burst` (restart-count growth ≥3 within 10m —
  the leading edge of a crash loop, ahead of kubelet's BackOff;
  warning). Transition memory is in-memory with TTL, rebuilt from the
  informer cache on restart; the source arms only after every cache
  syncs, so an initial LIST never fires transition signals.
  `node_notready` and `restart_burst` join their reactive k8s-event
  dedup families (`NodeNotReady`, `CrashLoopBackOff`) via append-only
  `CanonicalReason` entries, so the leading signal opens the session
  and the later event attaches as a followup. Enabled by the new
  ADDITIVE `--sources` flag (default `k8s-events` — existing
  deployments unchanged; `--sources=k8s-events,object-state` opts in);
  the source declares its RBAC (nodes, deployments, endpointslices,
  poddisruptionbudgets list/watch — ClusterRole updated) and the §11
  probe fails loudly when it's missing. The source's pod informer
  absorbs the recovery pod observer: when enabled, §7.4 clearance
  tracking runs off the source's informer (one pod watch total); the
  standalone observer remains the zero-config fallback with behavior
  unchanged.
- Storm correlation via graph blast-radius keys (M2, DESIGN.md §7.5) —
  the second-level correlation stage between dedup and severity
  routing, and the first wiring of the `pkg/graph` topology index into
  the sentinel. When enabled, a graph ingest loop shares ONE informer
  set with the signal sources (§6.3: the object-state source and the
  graph register on the same shared factory; the graph adds only a
  ReplicaSet watch — pods/nodes are already watched,
  ConfigMap/Secret/PVC ancestors come free as identities referenced by
  pod specs, and Zone is excluded because zone-tier grouping is the
  fleet layer's join). New incidents enter a rolling window (default 60s);
  when `--storm-min` (default 3) of them share a blast-radius key —
  the nearest common topology ancestor, priority node > owner chain
  (nearest first: ReplicaSet, then Deployment) > shared
  ConfigMap/Secret/PVC > namespace — they collapse into ONE
  `kind=storm` incident/session carrying the ancestor identity,
  affected/namespace counts, the first 3 representative incidents, ALL
  member fingerprints, and severity = max member severity with a size
  escalator (≥10 members bumps warning→critical). Member signals are
  recorded but open no sessions; members that fired per-incident
  before the storm formed (inherent for a burst's first arrivals) get
  a `kind=storm.member_superseded` pointer in their own session and
  their dedup binding is rebound to the storm session (the entry also
  records the storm fingerprint and persists via `--dedup-persist`),
  so followups and §7.4 outcomes route to the storm. Late arrivals
  attach as `kind=storm.member` followups while the storm is
  unresolved; member clearance feeds the storm's recovery — when ALL
  members clear, the storm session receives its own `kind=resolved`
  record (reason `storm`, uid `storm:<ancestor key>`). All three storm
  payloads are schema-stable with byte-exact wire pins; the storm
  fingerprint is `sha256(storm ⊕ reason-class ⊕ ancestor-kind ⊕ zone)`
  so the fleet layer joins the same node-failure storm across
  clusters. ADDITIVE
  flags, default OFF: `--storm` (opt-in — the graph informers need
  pods/nodes/replicasets list+watch; ClusterRole updated, §11 probe
  fails loudly when the grant is missing; flipping the default is an
  M2-exit decision), `--storm-window` (60s; 0 disables), `--storm-min`
  (3). New metrics: `storms_formed_total`, `storms_resolved_total`,
  `storms_active`, `storm_members_total{kind}`.
- Severity routing + the shared watchboard session (M2, DESIGN.md §7.7;
  §15 Q2 decided — see
  [docs/watchboard-rotation-design.md](docs/watchboard-rotation-design.md)):
  in per-incident mode the dispatcher now routes each NEW incident by its
  effective severity — `critical` opens a per-incident session exactly as
  before (full §7.6 enrichment lands next), `warning` batches into a
  managed shared watchboard session as a rolling
  `kind=watchboard.digest` inject (flushed at `--watchboard-batch`
  entries, default 5, or `--watchboard-flush` age, default 60s, whichever
  first; entries carry kind, fingerprint, object ref, count, first/last
  seen; byte-exact wire pin), and `info` is counted
  (`info_dropped_total{kind}`) and dropped with a debug log — never
  silently — until the M3 raw store (§9.1) persists it. Severity defaults
  stay stamped by the sources; the new repeatable `--severity`
  flag (`kind=level[,kind=level...]`, validated) overrides any kind. The
  watchboard session is created lazily (POST /sessions, owner =
  `--owner`) at the first digest flush and rotates SIZE-BASED per the Q2
  decision: after `--watchboard-rotate` digest injects (default 200) the
  next flush opens a fresh session and closes the old one with a
  schema-stable `kind=watchboard.rotated` lineage record
  (`successor_session_id`, `injects_count`, `rotated_at`; byte-exact wire
  pin). Dedup bindings survive rotation unchanged — followups and §7.4
  outcomes keep routing to the watchboard generation their incident is
  bound to; only NEW warnings flow to the successor. Storms bypass
  warning routing (§7.5 always opens ONE aggregate session, even for a
  warning-class storm), and a storm claims the bindings of members
  sitting in the watchboard buffer. `--mode=shared` is untouched: ALL
  severities keep routing to `--target-session` and the watchboard
  machinery is disabled (the watchboard is the per-incident-mode answer
  to warning noise). All new flags ADDITIVE with behavior-preserving
  defaults for the M0 surface (k8s-events are critical). New metrics:
  `watchboard_entries_total{kind}`, `watchboard_digests_total`,
  `watchboard_rotations_total`, `watchboard_buffered`,
  `info_dropped_total{kind}`.
- Enrichment — warm incident sessions via the in-process bundle (M2,
  DESIGN.md §7.6; §15 Q3 decided: fixed byte budget now, revisit with
  this PR's telemetry): before the INITIAL inject of a per-incident
  session, the sentinel runs the `lookout bundle` composition
  in-process (§4.3 surface 3 — the same `pkg/checks` code as the CLI,
  no fork/exec) scoped to the affected object, and attaches the result
  to the payload as the additive `enrichment: {bundle: "…"}` field
  (omitempty — un-enriched payloads stay byte-identical to M0; the
  frozen wire pins pass unchanged, a new pin covers the enriched
  shape). The bundle is `bundle`-shaped logfmt: a `bundle.target`
  head, then spec / delta / edges / radius / logs sections, every line
  emitted through the §6.5 sanitizer. Two read paths: with `--storm`
  on, enrichment reuses the LIVE topology snapshot + shared informer
  caches (owner chain, pods, radius) and pays one API GET for the
  workload object — the edges section, whose checks need the
  Service/RBAC index the live informer set doesn't carry, ships as an
  overflow trailer naming `lookout state edges` instead; with the feed
  off (or the object not in the topology yet) a namespace-scoped
  `state.LoadCluster` pass feeds all five sections. Size cap
  `--enrich-cap` (default 16384 bytes) truncates ONLY at section
  boundaries (prefix cut, never mid-line); every dropped or uncomputed
  section becomes a schema-stable `overflow section=<s> cmd="lookout
  …"` trailer with real arguments, so the inject itself teaches the
  next move (§4.4.4). Failure honesty: errors never block the inject —
  each failed stage attaches an `enrichment_error stage=<s>
  error="…"` trailer, whatever succeeded still ships, and the whole
  run is hard-capped by `--enrich-timeout` (default 5s) via context.
  Severity-gated by `--enrich=critical|warning|off` (default critical,
  per §7.6/§7.7); `--enrich-log-lines` (default 200) tail-limits the
  logs section per container stream (template cap 10, fixed). Storm
  sessions are enriched too — the storm ancestor's blast radius only
  (no logs, no per-member reads: a storm exists to collapse cost,
  §7.5). Shared mode is untouched (frozen contract; no session to
  warm). Exported seams added to `pkg/checks/bundle` for the sentinel
  (thin veneers over the CLI internals, no second implementation):
  `ResolveIncidentTarget`, `RadiusFindings`, `DeltaObjectsFor`. New
  metrics: `enrichments_total{outcome}`, `enrichment_bytes` histogram,
  `enrichment_truncated_total`, `enrichment_failures_total{stage}`.

## [0.2.0] - 2026-07-24

M1 — read-path core (DESIGN.md §14). Exit criterion verified in
`docs/milestones/M1.md`: an incident session can be fully investigated with
lookout tools alone; `lookout health` answers "any issues in this cluster?"
in one call; no secret value reached any output surface.

### Added

- First skills (DESIGN.md §4.4): `skills/k8s-triage` (incident
  investigation — bundle first, then the decision tree across triage
  logs/delta, state edges, and triage spec) and `skills/cluster-health`
  (scorecard assessment and per-category drill-down), each with
  three-level progressive disclosure; `skills/playbooks/` seeds the
  per-symptom convention with `crashloopbackoff.md` and `failedmount.md`
  keyed to the frozen `k8s-event` inject payloads. Per-command
  `references/*.md` are **generated** from the pkg/checks metadata by
  `dev/tools/gen-skill-refs` (internal/skilldoc); §4.4.3 contract tests
  enforce freshness, parse-validate every documented `lookout` command
  line against the registry, and pin quoted output snippets to the
  checktest golden fixtures. `skills/README.md` carries the
  `.agents/skills/` install recipe.
- `lookout triage delta` (§5) — one scan, every abnormal object; the first
  read-path command. Five finding classes, toggled with
  `--only=pods,nodes,pdb,system,quota`: broken workloads (crashloop /
  image-pull / OOM history / restart churn / aged-Pending with the
  scheduler's verdict / not-ready containers / failed pods and Jobs /
  rollout mismatch with stalled-progress detection), node problems
  (NotReady, Memory/Disk/PID pressure, NPD-style conditions, cordons still
  holding pods, spot/autoscaler reclaim taints), gridlocked
  PodDisruptionBudgets, degraded kube-system add-ons (CoreDNS / kube-proxy /
  CNI / CSI by well-known names and labels), and ResourceQuotas at or near
  their hard limits. Thresholds: `--restarts=5`, `--pending-age=5m`,
  `--quota-warn=90`. One paged List pass per resource kind; findings
  ordered critical-first; healthy objects emit nothing. Registered as MCP
  tool `k8s_triage_delta`.
- `lookout triage logs` (`k8s_triage_logs`) — the first read-path command
  (§5, "highest-value tool in the suite"): distills raw container logs into
  Drain-clustered templates. Hand-rolled fixed-depth parse tree over
  tokenized lines with variable-token pre-masking (numbers, hex, uuids,
  IPs, timestamps, durations); health/readiness probe noise stripped and
  reported (`log.probe_noise`, `--keep-probes` to disable); Go/Java/Python
  stack traces collapsed to their top-5 frames and clustered by frames
  (`log.stacktrace`); per-cluster count, pod spread, level guess,
  first/last seen, and one sanitized representative line. Scope by
  `--workload` (selector-resolved pods for Deployment/StatefulSet/
  DaemonSet/ReplicaSet/Job), `--namespace`/`-A`, or `--pod`; `--since`,
  `--previous`, `--container`, `--tail`, `--max-templates` (explicit
  `log.overflow` record when capped). Measured on the synthetic 10k-line
  corpus: ~900 KB of raw logs distill to ~7.5 KB (≈120x).

- `pkg/emit` — the §4.2 output envelope: findings as flat ordered key=value
  records (logfmt default, `--format=json` for one JSON object per line),
  the mandatory `scanned=<n> findings=<n> elapsed=<d>` summary line, exit
  codes 0 data / 1 runtime / 2 usage with stderr-only diagnostics, common
  flags (`--namespace|-A`, `--workload`, `--since`, `--format`, `--timeout`)
  parsed once into a typed Scope, and the sanitizer seam every finding
  passes through.
- The §6.5 sanitizer in `pkg/emit`, wired in as the default on every emit
  surface (not opt-in): finding-level masking of credential-shaped strings
  in messages, reasons, and detail values, plus `SanitizeObject` /
  `SanitizeUnstructured` for Kubernetes specs — system metadata stripped
  (`managedFields`, `resourceVersion`, `uid`, `creationTimestamp`,
  `ownerReferences[].uid`, the last-applied annotation, noisy status),
  `Secret.data`/`stringData` values masked to length-only redactions,
  credential env vars and annotations masked by name, and value-shape
  heuristics (JWTs, PEM private keys, AWS key IDs, GCP service-account
  `private_key`, Bearer/Basic tokens, URL userinfo passwords, `--password=`
  flags, key-anchored base64/hex) applied to every string. Golden-file
  tests plant a unique marker in every known secret position; a CI
  tripwire fails naming the position if any marker survives.
- `pkg/checks` — the command-metadata registry (§4.4.3, one source of truth
  generated outward): name/MCP-name/when-to-use/flag-spec/output-glossary
  declarations that generate agent-readable `--help` today and the MCP JSON
  schemas next; `pkg/checks/checktest` is the §13 contract-test scaffold
  that round-trips every command's emitted fields against its declared
  glossary. `cmd/lookout` mounts registered command groups automatically;
  no read-path commands are registered yet.
- `pkg/graph` — the topology index (M1, DESIGN.md §6): map-backed,
  copy-on-write, live-only pod-nexus graph with a single-writer batched
  ingest path (`FromObjects` initial sync + neutral `Delta` type shaped for
  informer wiring in M2/M3) and the §6.4 query surface (`Radius`,
  `OwnerChain`, `CommonAncestors`, `WorkloadEdges`, `PodsUnder`). Secret
  values are never stored — Secret ingestion reads `ObjectMeta` only.
  Benchmarks set the §15 Q5 compaction gate, recorded in
  `docs/graph-q5-gate.md`.
- `lookout triage spec` — the first registered read-path command (M1, §5):
  the sanitized, token-dense spec of ONE resource, "kubectl describe, but
  for agents" (MCP tool `k8s_resource_spec`). Takes
  `<Kind>/[<namespace>/]<name>` positionally (case-insensitive kinds with a
  small kubectl-alias table: po, deploy, rs, sts, ds, svc, cm, pvc, ing,
  netpol, no) or `--workload=`; the §6.1 pod-nexus kinds fetch through
  typed clients, everything else resolves via API discovery + the dynamic
  client. Output is the §6.5-sanitized object flattened into the finding
  model — metadata essentials, per-container image/resources/probes/env
  (credential values `[REDACTED]`), workload replicas/strategy/selector,
  service ports/selector, ConfigMap/Secret data KEYS only (values never
  rendered) — with healthy conditions and defaulted fields elided; abnormal
  conditions emit as warnings. `--diff` is a registered surface that
  returns an honest usage error until graph history lands (M3, §6.6). To
  carry it, the §4.2 runner gained positional-argument support
  (interspersed with flags, kubectl-style) and check-raised usage errors
  (`emit.UsageErrorf` → exit 2), and `pkg/kube` gained
  `BuildConfig`/`BuildDynamicClient`.
- `lookout state edges` (M1, DESIGN.md §5) — dependency-graph verification
  for one workload, the first pkg/graph consumer: one-shot paged Lists
  build a topology snapshot, the graph resolves the workload's dependency
  edges (§6.4), and each edge is validity-checked — ConfigMap/Secret
  existence *and* key-level references (env, envFrom, volume items),
  Service selectors (empty-selection and unready pods),
  Service→EndpointSlice→Pod health (missing slices, orphaned endpoints,
  ready-count mismatches), Ingress backends (service and port), RBAC
  reference integrity (ServiceAccount, dangling (Cluster)RoleBindings), and
  TLS certificate expiry for reachable `kubernetes.io/tls` Secrets
  (`--cert-warn`, default 720h; findings carry only subject/notAfter/days
  left — never certificate or key bytes). Healthy edges emit nothing.
  Registered as MCP tool `k8s_state_edges`.
- `lookout mcp` (M1, §4.3) — every registered read-path check served as an
  MCP tool, picked up automatically from the `pkg/checks` registry: tool
  name = the command's MCP name (`triage delta` → `k8s_triage_delta`),
  description = the §4.4.1 when-to-use line + output contract + field
  glossary, and the input schema derived mechanically from the same
  FlagSpecs that generate `--help` (string/bool/integer; durations as
  strings with a Go-duration pattern; `-A` surfaces as `all_namespaces`;
  positional arguments as a `target` property). Tool calls run the command
  in-process through the same `emit.Run` envelope as the CLI — identical
  flag parsing, `--timeout`, summary line, and §6.5 sanitizer — with exit
  codes mapped per §4.2: 0 → payload text, 1 → MCP tool error carrying the
  stderr diagnostics, 2 → JSON-RPC invalid-params. Transports: stdio by
  default; `--listen=<host:port>` for streamable HTTP restricted to
  loopback addresses (no auth story yet, so non-loopback binds are refused
  loudly). New dependency: `modelcontextprotocol/go-sdk` v1.4.1 (pinned to
  core-agent's version).
- `lookout bundle` (M1, DESIGN.md §5) — the correlated incident snapshot
  and the first composition command: given `--workload=<Kind>/<ns>/<name>`
  (or `--incident=<inject payload JSON>`, whose object reference resolves
  to the owning workload via the graph's owner chain, falling back to the
  payload's `controller_ref` when the object is already gone), one List
  pass feeds every section of a single payload — sanitized `spec`,
  target-scoped `delta`, dependency-edge validity (`edges`), blast
  `radius` (upstream/lateral/downstream neighbors with `relation`/`hop`
  fields, dangling references flagged), and Drain-distilled `logs` for
  the workload's pods (tighter `--max-templates=15` budget). Findings
  carry a `section` field so the stream reads as one document; a `triage
  events` section joins in M3 when that command lands. The `state edges`
  List pass now also ingests Nodes into the graph so placement neighbors
  resolve as observed instead of dangling. Registered as MCP tool
  `k8s_triage_workload`.
- `lookout health` (M1, DESIGN.md §5) — the "any issues with this
  cluster?" scorecard: ten categories (control-plane latency, node
  conditions, crash loops, aged Pending, rollout stalls, PVC/storage
  health, system add-ons, ResourceQuotas, cert expiry, webhook backend
  health), each answering `healthy|degraded|unavailable` via one
  `health.category` finding — the scorecard always answers, healthy
  included — with degraded categories naming their worst findings inline
  (`--top`) and emitting full details after the scorecard block. Six
  categories delegate to the `triage delta` scan; storage (Pending/Lost
  PVCs), certs (cluster-wide `kubernetes.io/tls` expiry, `--cert-warn`),
  and webhooks (validating/mutating configurations whose service backend
  does not resolve — the minimal subset of M5's `state webhooks`) are new
  lightweight checks; control-plane latency reports
  `unavailable ("requires cloud provider metrics (M4)")` through the
  pkg/cloud provider boundary. M1 is live checks only: the merge with
  open sentinel findings (§9.1) and triage-status records (§9.4) lands in
  M4. Registered as MCP tool `k8s_cluster_health`.

## [0.1.0] - 2026-07-24

M0 — bootstrap (DESIGN.md §14). Exit criterion verified in
`docs/milestones/M0.md`: an existing `k8s-event-watcher` deployment swaps
images with zero config change.

### Added

- Repository bootstrap (M0): Go module, `lookout` multicall binary skeleton
  with `version` subcommand, `dev/` presubmit tooling mirrored from
  core-agent, and CI (test / lint / mod-tidy / govulncheck) building every
  provider build-tag variant (vanilla, `gke`, `allproviders`).
- `lookout watch` — the `k8s-event-watcher` sidecar moved verbatim from
  core-agent (#14). Flags, behavior, exit codes, and the inject payload wire
  shape are identical; the flag surface is frozen by a contract test and the
  wire shape is pinned byte-for-byte by
  `TestDispatcher_ExactInjectPayloadWireShape`.
- `pkg/kube` (client construction), `pkg/engine` (filter + dedup), and
  `pkg/inject` (daemon HTTP client + frozen wire types), extracted from the
  watch command for reuse by the M1+ read path (#15).
- Container image at `ghcr.io/go-steer/lookout` (distroless, nonroot,
  multi-arch), published + cosign-signed on `v*` tags, with `deploy/`
  manifests carried over from core-agent (#16). The image's ENTRYPOINT is
  `["/lookout", "watch"]`, so predecessor deployments keep passing bare
  watcher flags via `args:` unchanged.

### Migration from `ghcr.io/go-steer/k8s-event-watcher`

Change the image line to `ghcr.io/go-steer/lookout:v0.1.0`. That is the only
change: flags, RBAC, probes, metrics names (`k8s_event_watcher_*`), ports,
and payload wire shape are all identical.
