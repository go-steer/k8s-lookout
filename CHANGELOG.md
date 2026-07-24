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
