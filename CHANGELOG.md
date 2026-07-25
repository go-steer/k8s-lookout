# Changelog

All notable, user-visible changes to k8s-lookout.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- The shipped ClusterRole now grants `get` on `apps/deployments`,
  `apps/replicasets`, and `pods/log` — the M2 drill showed enrichment's
  spec and logs sections failing in-cluster without them (5 of 8 runs
  `outcome=partial`; see `docs/milestones/M2.md` §Observations).

### Added

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
  vectors so AX fleet rollup joins stay stable across versions).
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
  pod specs, and Zone is excluded because zone-tier grouping is AX's
  fleet join). New incidents enter a rolling window (default 60s);
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
  so AX joins the same node-failure storm across clusters. ADDITIVE
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
