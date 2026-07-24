# Changelog

All notable, user-visible changes to k8s-lookout.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
