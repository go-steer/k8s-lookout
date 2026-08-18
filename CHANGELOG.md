# Changelog

All notable, user-visible changes to k8s-lookout.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- New command `scan` (MCP `k8s_scan`): the zero-argument entry point.
  "Something is wrong with this cluster" is the question people and
  agents actually arrive with, and until now answering it required
  already knowing which of thirty-odd commands to reach for. `lookout
  scan` needs nothing but a kubeconfig.

  Stage 1 runs every target-free incident check — `triage delta`,
  `state webhooks`, `state volumes`, `state storage`, `state gateway`,
  `state wi`, `stab drift` — into one stream under one envelope, each
  finding stamped `check=<command>`, which is also the command to run
  for the detail behind it. Stage 2 then drills into the dependency
  edges of every workload stage 1 flagged at warning or above: one
  cluster List pass and N in-memory `state edges` evaluations, with
  each finding rolled up to its outermost controller so twenty
  crashlooping pods of one Deployment are one drill-down, not twenty.
  `--max-drilldown` bounds it (default 20) and reports what it
  dropped. The output is the standard §4.2 envelope, so `lookout scan
  | lookout findings diff --store=...` works with no glue.

  `scan` reports **incidents** — things that are broken now and that
  clear themselves when fixed. The posture groups are one flag away
  (`--include=audit`, `cloud`, `perf`, or `all`) and the groups left
  out are named in the summary's `skipped=` note so they stay
  discoverable while off. They are off by default because posture
  findings never self-clear: defaulting them on would both flood a
  healthy cluster's first run and swamp the `findings diff` transition
  stream with a flat backlog.

  A scan that could not run something says so rather than reporting a
  clean bill of health it did not earn: a check that declines the
  invocation becomes an info `scan.check_skipped`, one that errors
  becomes a warning `scan.check_failed` without voiding the other
  twelve's findings, a timeout emits `scan.incomplete` naming the
  checks left unrun, and stages that degraded (no cloud provider, no
  Gateway API) are rolled up into the `unavailable=` summary note.

  Unlike `bundle` and `health`, which compose a hand-written list of
  Go calls and have therefore never picked up a check added after they
  were written, `scan` composes the command **registry**. A contract
  test requires every visible command to be either in scan's default
  set or in its exclusion table with a stated reason, so adding a
  check without deciding whether a bare scan should run it fails CI.

- New command `state gateway` (MCP `k8s_gateway_routes`): the Gateway
  API path end to end — GatewayClass → Gateway → listener → HTTPRoute
  → Service — reporting every hop that is rejected, unprogrammed, or
  points at something that is not there. The sentinel has watched
  `gateways` and `httproutes` since v0.16.0; there was no way to ask
  the same question from the CLI or from an agent.

  Nine claims: a Gateway naming a GatewayClass that does not exist or
  whose controller rejected it, a Gateway its controller refused or
  accepted-but-never-programmed, a single unusable listener on an
  otherwise working Gateway, an HTTPRoute attached to an absent
  Gateway or refused by a present one, and backendRefs naming a
  Service that does not exist or a port it does not expose.

  Where the Gateway API defines a status condition, that condition is
  the answer — route attachment is read from what the controller wrote
  per parent rather than by re-implementing `AllowedRoutes` matching
  in this process. The backendRef checks are the exception and are
  recomputed, because "a reference did not resolve" is not an answer
  without "which Service, and which port".

- New package `pkg/checks/crd`: the read path's shared seam for
  detectors over API groups that may or may not be installed.
  Discovery decides, so `state gateway` auto-enables where the CRDs
  are present; a cluster without them gets one explicit
  `crd.unavailable` info finding, an `unavailable=` summary note, and
  exit 0 with `scanned=0` — the same degradation shape `cloud.*`
  already uses for an unavailable provider capability, rather than a
  clean bill of health it did not earn. A partially served group
  reports what it could not read as `not_served=`.

  Objects are read dynamically as unstructured: taking a build-time
  dependency on the Gateway API, OLM, KEDA and Kyverno Go modules to
  read a handful of status conditions is not a trade worth making.
  Optional CRD reads deliberately stay out of
  `state.LoadClusterListRequirements()` and therefore out of the
  shipped watcher ClusterRole — a CRD detector does its own List pass,
  so an optional read never becomes an unconditional grant.

- New command `state storage` (MCP `k8s_storage_binding`): why a
  PersistentVolumeClaim will never bind. `state volumes` answers "the
  volume bound, so why won't it attach"; this answers "why is the
  claim still Pending" — a StorageClass that does not exist, no class
  named and no cluster default, a static-only class with nothing
  pre-provisioned — plus the two states behind those: more than one
  StorageClass annotated as the default, and volumes stranded in
  `Released` or `Failed`.

  Every claim rule is gated on evidence rather than shape, so the
  normal ways a Pending claim is healthy stay silent:
  `WaitForFirstConsumer`, an explicit `storageClassName: ""`, and a
  matching volume already sitting Available.

- `state edges` verifies three more references, all of which fail
  silently today:
  - **imagePullSecrets.** `triage delta` reports `pod.imagepull` — the
    symptom. This names the cause: the Secret does not exist, or
    exists and is not a registry credential type the kubelet will use.
    Both the pod spec's and the ServiceAccount's are checked, and the
    ServiceAccount's are the trap — the kubelet merges them in at pull
    time, so they appear nowhere in `kubectl get pod -o yaml`.
  - **StatefulSet governing Service and volumeClaimTemplate class.** A
    `serviceName` that resolves to nothing means the per-pod DNS names
    never resolve, which is the entire reason to run a StatefulSet. A
    `volumeClaimTemplates` entry naming a StorageClass that no longer
    exists leaves replica 0 healthy and every replica after it Pending
    forever.
  - **IngressClass.** An Ingress naming a class that does not exist,
    or naming none where nothing declares itself the cluster default,
    is accepted by the API server and then served by nothing, with no
    event and no condition to find. The second case is a warning, not
    a critical: a controller may still claim an unclassed Ingress by
    convention, as GKE's does.

  `state.LoadCluster` now also lists `ingressclasses` and
  `storageclasses` — both cluster-scoped, name-only reads — and
  `deploy/12-clusterrole-watcher.yaml` grants them.

- `triage delta` reports `workload.replicafailure`: a Deployment whose
  pods were never created at all, because a ResourceQuota denied them,
  PodSecurity admission rejected them, or the ServiceAccount they name
  does not exist. This is the one abnormality with no pod to find, so
  every pod-level check was silent on it and the scan showed
  `desired=3 ready=0` with no explanation anywhere. The finding
  carries the admission error verbatim, which is the whole answer.

- Every read-path command now declares the finding kinds it can emit,
  and the severities it carries each one at. Until now the `kind=`
  field was the most load-bearing string in the output and the only
  one with no contract behind it: `Finding.validate` asked that it be
  non-empty and nothing more, so the vocabulary lived in whichever
  emit site happened to write it and appeared in no help text, no
  schema, and no page. A new `Kinds` ledger on `checks.Command` sits
  beside the `Output` glossary that has always worked this way, and
  the [Finding kinds](https://k8s-lookout.dev/reference/finding-kinds/)
  page renders the whole vocabulary — kind, severities, the one-line
  claim, and which commands emit it — from those declarations. Each
  command's own reference page and skill stub gained the same table
  for its slice of it.

  Two tests hold the ledger to the code. `checktest.Verify` rejects a
  finding whose kind is undeclared, or whose severity the declaration
  does not list, so every test run checks the emitters it exercises; a
  source sweep over `pkg/checks/**` then resolves the kind at every
  emit site — through constants, struct fields, and helper returns —
  so a branch no fixture reaches cannot slip through either. The
  ledger caught four real drifts while it was being filled: `state
  storage` never declared `storage.pv_failed` or `storage.pv_released`
  at all, `pod.pending` and `health.category` were emitted at
  severities above the ones documented, and `cloud.unavailable` was
  described two different ways by two commands.

- A contributor onramp: `docs/adding-a-check.md`, a matching page on
  the docs site, and `dev/tools/new-check`. There was no document
  saying how to add a read-path command, and behind the missing
  document sat mechanical friction that was accidental rather than
  contractual.

  The doc walks `audit netpol` end to end — the rationale comment, the
  kind and reason constants, the output glossary, the usage-vs-runtime
  exit split, the shape of `Run`, and the five kinds of test a check's
  suite carries — and then lists the nine touchpoints, marking which
  are generated and which are decisions.

  `dev/tools/new-check --group=<g> --check=<c> --summary=...` writes
  the command, its suite and its first golden, registers it, and runs
  the generated golden test so the scaffold is proven to compile
  before it is handed over. It reads the target group's `Deps` first,
  so a group with a Kubernetes client gets the client guard and a
  fake-clientset fixture and a group with only a clock gets neither, and
  it refuses a group that does not exist rather than inventing one — a
  group is a claim about a class of question, which is not a template
  decision. What it deliberately does not generate, it prints as a
  checklist naming the test that fails until each item is answered:
  the claim, scan membership, RBAC, and skills.

  Goldens also now have one update mechanism instead of two.
  `checktest.Golden(t, path, got)` replaces the per-package copies
  across fourteen packages — four of which had drifted onto a
  `-update` flag, so "how do I refresh this golden" had two answers
  depending on which file you were in. `UPDATE_GOLDEN=1 go test ./...`
  is now the only one, and a failure prints an aligned line diff
  rather than two blobs. `pkg/emit` keeps a local copy with a comment
  saying why: `checktest` is built on it, so it cannot import it.

### Fixed

- A malformed invocation now exits 2 (usage) instead of 1 (runtime),
  as §4.2 has always specified. `state edges`, `state wi`, `triage
  delta`, `triage logs`, and the `triage` positional-target parser
  reported bad flag values, unsupported workload kinds, contradictory
  `--namespace`/`--workload` pairs, and missing scopes as runtime
  failures, so a caller could not tell "the cluster is unreachable"
  (retry) from "you typed it wrong" (do not retry). The distinction is
  preserved where it is real: a well-formed target naming an object
  that does not exist is still a runtime error.

## [0.21.0] - 2026-08-16

The `audit` group arrives in full. v0.20.0 shipped the posture claim
with a single detector; this release adds four more, so "what has no
safety net while it is still healthy" now spans workload security,
network isolation, cluster configuration and upgrade readiness:
`audit hardening` (privileged containers, host namespaces, hostPath
mounts, default-ServiceAccount tokens something actually uses,
namespaces with no Pod Security Admission), `audit netpol`
(namespaces nothing isolates, and the individual workload that fell
through the selectors covering its neighbours), `audit cluster`
(Workload Identity off or bypassed, legacy metadata endpoints, a
control plane the internet can reach) and `audit upgrades` (how far
behind the control plane and its node pools are, and whether
anything is set up to close that gap on its own).

The last two read the cloud provider's cluster record rather than
Kubernetes objects, and they set the convention every cloud-backed
detector after them follows: a tri-state the provider leaves unset
makes no claim at all, an unavailable capability emits one explicit
`cloud.unavailable` and exits 0 rather than reporting a clean
cluster, and a version is judged against the cluster's own release
channel — including the `-gke.N` build suffix, where most GKE
security patches land, and without which a months-behind cluster
reads as current.

`triage list` closes a hole in the read surface. Every
detail-returning command takes a `<Kind>/<namespace>/<name>` target
and nothing produced one: the health scans report only what is
abnormal and correctly name nothing when a namespace is clean, so an
agent dropped into an unfamiliar namespace had to guess object names.
It is `kubectl get` across every kind an incident normally involves,
in one pass, one line per object, each leading with a paste-ready
target — an inventory and not a diagnosis, which is why a kind's
fields are the columns `kubectl get <kind>` prints in its own default
table and nothing else.

Finally, the sentinel stops linking an agent framework. OTel setup
was this module's one library import of `core-agent`, and it reached
`google.golang.org/adk` underneath — an agent framework's model,
session and tool packages, plus `google.golang.org/genai`, in every
shipped binary in service of about a hundred lines of standard SDK
wiring. That wiring now lives in `internal/telemetry`; the build
closure drops from 1074 packages to 966. The daemon relationship is
unchanged, because it was always HTTP. One fix rides along:
`--otel-exporter=otlp` previously built no exporter at all unless an
endpoint env var was set, while still printing one at startup — an
operator who asked for OTLP and set no endpoint got a boot line and
zero spans.

### Added

- **`lookout triage list`** (MCP `k8s_list_resources`) — the discovery
  call the read surface was missing: `kubectl get` across every kind
  an incident normally involves, in one pass, one line per object
  (#252). Every other detail-returning command takes a
  `<Kind>/<namespace>/<name>` target and nothing produced one — the
  health scans report only what is *abnormal* and correctly name
  nothing when a namespace is clean, so an agent dropped into an
  unfamiliar namespace had to guess object names. Every line now leads
  with a paste-ready target. Defaults to 18 namespaced kinds ordered
  workloads → routing → configuration; `--kinds` takes kubectl
  spellings (`pods`, `deploy`, `certificates.cert-manager.io`) and
  resolves anything outside the built-in table through discovery.
- `triage list` is an inventory, not a check: a kind's fields are the
  columns `kubectl get <kind>` prints in its own default table and
  nothing else, every finding is `severity=info` with no reason and no
  message. An Endpoints object's address count is in; a Service's
  selector is out, because "this selector matches no pods" is
  `state edges`' answer and it is a better one (#252).
- `triage list` treats a refusal as a result: a kind the caller may
  not list is reported as `skipped=Secret:forbidden` on the summary
  line rather than failing the run, `--max` truncation reports
  `truncated=<n>`, and an empty listing for a namespace that does not
  exist is marked `namespace_absent=true` — an empty listing alone
  cannot tell that from an empty namespace (#252).
- Secrets are counted, never read: a Secret's line is its type and its
  key count (#252).
- **`lookout audit hardening`** — workload security posture (#183).
  Five claims from one pass over every pod template in scope
  (Deployments, StatefulSets, DaemonSets, CronJobs, Jobs, unowned
  Pods) plus the namespaces holding them:
  `audit.privileged_container`, `audit.host_namespace`,
  `audit.hostpath_mount`, `audit.default_sa_automount` and
  `audit.podsecurity_gaps`. Init containers are judged alongside
  regular ones. A Job or Pod with an `ownerReference` is judged at its
  owner, so a namespace with 200 completed Jobs reports the one
  template that is wrong rather than 200 copies of it. No new RBAC.
  Scope with `--namespace` or `-A`; `--workload` is rejected, because
  two of the five claims are about a namespace, not a workload.
- `audit.privileged_container` matches `capabilities.add: ALL` or
  `SYS_ADMIN` (reason `DangerousCapability`) as well as
  `securityContext.privileged` (reason `PrivilegedContainer`). Neither
  form is `privileged: true` and both are equivalent to it, so a check
  reading only that flag reports a `CAP_SYS_ADMIN` container as clean
  (#183).
- `audit.default_sa_automount` fires only where the `default`
  ServiceAccount's token is both offered and taken — some workload
  actually runs as `default` and does not set
  `automountServiceAccountToken: false` itself. The subject is the
  ServiceAccount, since disabling automount there is the single edit
  that fixes every workload listed in `mounting_workload_names` (#183).
- `audit.hostpath_mount` counts only volumes a container actually
  mounts, and splits `WritableHostPath` (warning) from
  `ReadOnlyHostPath` (info). `audit.host_namespace` reports
  `HostNetwork`, `HostPID` and `HostIPC` as separate findings, since
  each grants a different thing and each is separately removable
  (#183).
- `audit.podsecurity_gaps` reports a namespace with no
  `pod-security.kubernetes.io/enforce` label (`NoPodSecurityEnforce`)
  or one set to `privileged` (`PodSecurityEnforcePrivileged`), with
  the count of workloads it covers. PSA's cluster-level default lives
  in the API server's admission config and is unreadable from the API,
  so on a cluster that sets one this over-reports; a reviewed
  cluster-wide `--exemptions` entry is the way to record that (#183).

- **`lookout audit netpol`** — NetworkPolicy coverage posture (#185).
  One kind, `audit.netpol_missing`, with three reasons per direction:
  a namespace where no policy restricts ingress (`NoIngressPolicies`,
  warning) or egress (`NoEgressPolicies`, info); a namespace whose
  policies exist and select none of its workloads
  (`IngressPoliciesSelectNothing` / `EgressPoliciesSelectNothing`,
  warning in both directions, since a selector that matches nothing is
  a mistake and not a posture); and an individual workload that fell
  out of the selectors covering its neighbours (`UnselectedIngress` /
  `UnselectedEgress`). The subject follows the remedy — the Namespace
  where nothing is covered, the workload where the namespace is
  policed and one object is the hole in it.
- Coverage means **isolation**: some policy selects the pod and names
  the direction. A namespace with one default-deny (`podSelector: {}`,
  which selects every pod in it) is fully covered and reports nothing.
  A policy whose rule is `ingress: [{}]` isolates the pod and then
  allows the cluster back in; `audit netpol` counts that as coverage
  rather than calling a deliberate, reviewed policy absent. `policyTypes`
  is treated as the API server defaults it — Ingress always, Egress
  only where egress rules exist — so a policy read from a manifest with
  the field unset does not read as covering nothing (#185).
- hostNetwork pod templates are excluded from `audit netpol`'s
  arithmetic, since NetworkPolicy selects pods and cannot constrain a
  pod on the node's network stack. The exclusion is visible in
  `host_network_workloads` rather than silent, and `audit hardening`
  reports those templates as `audit.host_namespace` (#185).

- **`lookout audit cluster`** — GKE cluster security configuration
  (#186). Three kinds read from the provider rather than from the
  Kubernetes API: `audit.workload_identity_off` (the cluster has no
  workload-identity pool, `WorkloadIdentityDisabled`; or a node pool
  runs the node-identity metadata server and so bypasses a pool the
  cluster does have, `NodePoolMetadataServerOff`),
  `audit.legacy_metadata` (a node pool that does not set
  `disable-legacy-endpoints=true`, `LegacyMetadataEndpoints`) and
  `audit.public_control_plane` (`PublicEndpointUnrestricted`,
  `AuthorizedNetworksAllowAll`, `AuthorizedNetworksAllowProviderCIDRs`).
  The subject is the `Cluster` for the cluster-wide claims and the
  `NodePool` for the per-pool ones — the object an operator edits to
  fix it. `--namespace`, `-A` and `--workload` are rejected: nothing
  here is namespaced.
- New provider capability `cluster-config`, read over the same
  `clusters.get` call `state ipspace` already makes. On a build without
  the provider — or a cluster whose provider identity is not fully
  resolved — `audit cluster` emits one `cloud.unavailable` finding and
  exits 0, the §2 degradation contract, rather than reporting a clean
  cluster (#186).
- `audit cluster` preserves the provider's tri-states instead of
  flattening them. A node pool whose metadata mode is unset or unknown
  makes no claim at all, since the default varies by cluster version
  and node image; an absent `disable-legacy-endpoints` key is reported
  as `unset` and still fires, because the legacy endpoints are on until
  something turns them off. A public endpoint restricted to a
  non-trivial allow-list is silent — the claim is exposure, not the
  existence of a public endpoint — but an allow-list containing
  `0.0.0.0/0` is a warning, the same defect shape as a NetworkPolicy
  that selects nothing (#186).

- **`lookout audit upgrades`** — upgrade and patch readiness (#187).
  Four kinds, grouped by what an operator does about them:
  `audit.version_behind` (the control plane is a release line behind
  what its channel publishes, `ControlPlaneMinorBehind`, or a patch
  behind, `ControlPlanePatchBehind`; a node pool at the 2-minor
  supported skew, `NodePoolVersionSkew`), `audit.upgrade_unmanaged`
  (`NoReleaseChannel`, `NodeAutoUpgradeOff`, `NodeAutoRepairOff`),
  `audit.upgrade_blocked` (a maintenance exclusion in force,
  `MaintenanceExclusionBlocksPatches` / `MaintenanceExclusionActive`;
  a node image built around the removed Docker runtime,
  `StaleNodeImageType`) and `audit.upgrade_unattended`
  (`NoMaintenanceWindow`, `NoUpgradeNotifications`). The subject is the
  `Cluster` or the `NodePool`, whichever an operator edits.
  `--namespace`, `-A` and `--workload` are rejected.
- The `cluster-config` capability now also carries the cluster's
  version, release channel, maintenance policy and upgrade
  notifications, and each node pool's version, image type and
  management block — plus `UpgradeTargets`, which reads the provider's
  published versions for a given channel. `audit upgrades` compares
  against the cluster's **own** channel, so a stable-channel cluster is
  not reported as behind for running exactly what stable publishes, and
  against that channel's rolling upgrade target where it has one. The
  `-gke.N` build suffix is part of the comparison: most GKE security
  patches land there, and ignoring it reports a months-behind cluster
  as current (#187).
- Node pools are judged only at the supported 2-minor skew, not at any
  difference from the control plane — one minor behind is what a
  rolling upgrade looks like from outside. An unset auto-upgrade or
  auto-repair toggle makes no claim, since the provider's default
  depends on how the pool was created; both being off is two findings,
  because they have two remedies. Only a maintenance exclusion in force
  right now is reported, and one scoped to all upgrades is a warning
  while one scoped to minors is info, because patches still flow
  through it (#187).
- Cluster-wide version consistency across a cohort (`fleet-spread`) is
  not part of this command: it is a question about a fleet, not about
  one cluster, and arrives with the fan-out layer (#189).

### Changed

- **The sentinel no longer depends on `core-agent` (or on an agent
  framework) at build time.** OTel setup was the module's one library
  import of `github.com/go-steer/core-agent/v2`, and it reached
  `google.golang.org/adk` underneath — so every `lookout` binary
  linked in an agent framework's model, session, and tool packages,
  plus `google.golang.org/genai`, for about a hundred lines of
  standard OpenTelemetry SDK wiring. That wiring now lives in
  `internal/telemetry`, and the module requires neither. The daemon
  relationship is unchanged: it was always HTTP (`pkg/inject`, the
  token-burn cost stack), which is a wire contract, not a build one
  (#255).
- `--otel-exporter=otlp` now always exports. It previously built no
  tracer provider at all unless `OTEL_EXPORTER_OTLP_ENDPOINT` or
  `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` was set — while still logging
  an endpoint at startup, so an operator who asked for OTLP and set no
  endpoint got a boot line claiming `localhost:4318` and zero spans.
  The exporter now honors the OTel spec's own default
  (`http://localhost:4318`), and an unreachable collector reports
  itself as `lookout: otel-export: …` on stderr (#255).
- Spans now carry `service.name=lookout` and `service.version=<build
  semver>` instead of the SDK's `unknown_service:lookout` placeholder.
  `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES` still win when set.
  `gcp.project_id` is stamped from `GOOGLE_CLOUD_PROJECT` as before,
  but only when that variable is non-empty (#255).
- `--otel-exporter`'s help text pointed at `docs/otel.md`, which does
  not exist; it now names the `OTEL_TRACES_EXPORTER` override, which
  is what an operator reaching for that doc was likely after (#255).
- The two hand-written command tables — README's "Command surface" and
  the docs site's MCP tool list — were nine commands behind the
  registry (the whole `audit` group, both `findings` commands and
  `triage list`). Both now list the full surface, README's compressed
  `cloud` row is one row per command, and a test holds them to the
  registry so the next command cannot land without a row. Everything
  else that enumerates commands is generated; these two were the only
  places the surface could silently fall behind.
- Findings now sort by reason as the final key, after namespace, kind
  of object, name, severity and kind. One subject can carry several
  findings that tie on all of the earlier keys — a pod template in all
  three host namespaces, an HPA that is both pinned and unmeasurable —
  and without it their order was left to the sort's internals (#183).

## [0.20.0] - 2026-08-15

One theme: lookout starts answering a second question. Every group
until now reported what is broken right now. The new `audit` group
reports what has no safety net while it is still healthy — a claim
with a different truth condition, a different fingerprint recipe and
a different consumer, which is why it is a group rather than more
kinds bolted onto an existing one.

`audit workloads` is the first detector, and makes seven claims from
a single API pass: no PodDisruptionBudget, a single replica, no
spread, no readiness or liveness probe, placement pinned to too few
nodes, and autoscalers that structurally cannot scale. This is the
standing question `stab drain` cannot answer — `drain.singleton`
fires only for a workload whose pod happens to sit on the node being
drained, so the same workload is invisible from every other node.
Nothing here needs new RBAC.

Posture findings are only worth reading if the opt-outs are
auditable, so exemptions shipped with the first detector rather than
after the sixtieth. `--exemptions` is a common flag across the whole
tree: a git-reviewable file with a mandatory reason and expiry on
every entry, where an exempt finding is annotated and counted, never
dropped. `audit exemptions` then audits the file itself, so it cannot
quietly become a permanent list of things nobody re-reads.

Also here: `triage top` censuses missing requests beside missing
limits, both now LimitRange-aware, closing the scheduler-side half of
a gap that previously only covered saturation. And a Go toolchain
bump to 1.26.6 clears six standard-library advisories the published
images were still being built against — hidden until now because CI
resolved a floating Go version while the release images read
`go.mod`. Those two now agree by construction.

### Added

- **`lookout audit workloads`** — the first posture detectors (#190).
  Seven claims about workloads that are healthy right now, from one API
  pass over Deployments, StatefulSets, DaemonSets, PDBs, HPAs and
  Nodes: `audit.no_pdb`, `audit.single_replica`, `audit.no_spread`
  (warning / warning / info), `audit.no_readiness_probe` /
  `audit.no_liveness_probe`, `audit.rigid_scheduling` and
  `audit.hpa_cannot_scale`. This is the standing question `stab drain`
  cannot answer: `drain.singleton` fires only for a workload whose pod
  happens to sit on the node being drained, so the same workload is
  invisible from every other node. No new RBAC — every resource it
  lists was already granted. Scope with `--namespace`, `-A`, or
  `--workload`.
- `audit.rigid_scheduling` resolves a workload's required placement
  (`nodeSelector` ANDed with required `nodeAffinity`) against the live
  node labels, which is what separates `pool=gpu` over a 40-node pool
  from the same three lines of YAML over one surviving node:
  `SingleEligibleNode` and `FewerEligibleNodesThanReplicas` at warning,
  `NoEligibleNodes` at info, because a node pool scaled to zero by the
  cluster autoscaler reports exactly that and it is a normal resting
  state. Taints and cordons are not subtracted, so `eligible_nodes` is
  an upper bound and the check under-reports rather than inventing
  findings (#190).
- `audit.hpa_cannot_scale` reports autoscalers that structurally never
  can: `HPAMinEqualsMax`, `HPATargetMissingRequests` (a utilization
  target is a percentage OF a request, so one without a request is
  arithmetic the controller can never do), and `HPATargetMissing`. It
  does not overlap the `autoscaling.hpa_pinned` /
  `autoscaling.hpa_metrics_dead` sentinel kinds, which are sustained
  states needing a resident watch — in fact the sentinel deliberately
  declines the `min == max` case, so until now nothing reported it. The
  subject is the HPA: these findings carry
  `kind_of_object=HorizontalPodAutoscaler` and the autoscaler's own
  name (#190).
- `triage top` now censuses missing **requests** alongside missing
  limits: `top.unrequested` (aggregate) and `top.unrequested_container`
  (per container, behind `--show-unrequested`), mirroring the existing
  `top.unlimited` pair. A missing limit hides a container from
  saturation analysis; a missing request is the scheduler-side half —
  the pod is bin-packed as zero, which is what sits behind
  `FailedScheduling` churn, noisy-neighbour eviction and bad packing.
  The request is read from the same pod-spec pass the limit already
  came from, so the census costs no extra API calls. Because the
  apiserver copies an unset request down from the limit, the
  unrequested census is always a subset of the unlimited one (#235).
- Both censuses are now LimitRange-aware. A container missing a
  dimension that its namespace's LimitRange defaults carries
  `limitrange=<names>`, and the aggregate line reports
  `limitrange_defaulted=<n>`. These pods predate the LimitRange —
  LimitRanger mutates at admission and never touches running pods — so
  the finding stays, correctly, with the note that recreating the pod
  picks the value up (#235).
- **Reviewed, expiring exemptions on every command (`--exemptions`).** A
  git-reviewable YAML file marks a finding as intentional, with a mandatory
  reason and expiry per entry. Exempt does **not** mean absent: a covered
  finding is still emitted and still counted, annotated with
  `exempt_reason=`/`exempt_expires=`, and the summary line reports
  `exempt=<n>` — including `exempt=0`, which says the file was in effect and
  nothing matched. Filtering is the consumer's job; an opt-out that hid
  findings would reintroduce exactly the unverifiable coverage the audit
  surface exists to eliminate. This is a common flag, so it applies to every
  command in the tree, not just the new group (#234).
- **`lookout audit exemptions`** — the first command in the new `audit`
  posture group. It audits the exemption file itself, reporting lapsed entries
  (`audit.exemption_expired`) and ones about to lapse
  (`audit.exemption_expiring`, `--within`, default 14 days), so a file cannot
  quietly become a permanent list of unexamined opt-outs (#234).
- **The `audit` command group.** Best-practice posture — the absence of a
  safety net around something currently healthy — as a distinct claim from the
  incident groups, shipped in this binary and separated by group (#182).
- **`engine.PostureFingerprint`** — the incident-class hash for posture
  findings: the detector's own kind, an uncanonicalized reason, and no zone.
  An addition to the §8 contract; `engine.ScanFingerprint` is unchanged
  (docs/signal-schema-v1.md § "Posture-source mapping").

### Security

- **Go toolchain 1.26.6**, up from the 1.26.3 language pin and the 1.26.5
  toolchain directive, clearing six standard-library advisories that release
  images were still being built against: GO-2026-6218 (`net/url`),
  GO-2026-6091 (`html/template`), GO-2026-6090 (`crypto/tls`), GO-2026-6089
  (`net/http`), GO-2026-5972 (`encoding/asn1`) and GO-2026-5026 (`x/net/idna`
  via `net/http`). No source change — the reachable paths are the HTTP server
  behind `serve`, the inject client, corpus scanning, and x509 parsing in
  `state edges`. `govulncheck` now reports zero.

### Changed

- **CI now takes its Go version from `go.mod`** (`go-version-file`) in every
  `setup-go` step, instead of a floating `go-version: '1.26'` with
  `check-latest`. The float is what hid the advisories above: CI resolved to
  whatever the newest 1.26.x was that day and its `govulncheck` passed, while
  the local runner and the release image — which reads `go.mod` — built
  against an affected toolchain. CI, `dev/tools/ci`, and the published images
  now agree by construction, restoring the invariant `.github/workflows/ci.yml`
  already claimed: a green local run is the same green run as remote CI.

### Fixed

- **`dev/tools/verify-mod-tidy` no longer reports uncommitted-but-tidy `go.mod`
  edits as untidy**, and no longer rewrites the working tree as a side effect
  of a check. It tidied in place and then ran `git diff`, which compares
  against HEAD rather than against tidy form — so any in-progress `go.mod` edit
  failed the check with a misleading message, and a failing run left the tree
  mutated. It now tidies a copy via `go mod tidy -modfile` and diffs that
  against the working tree, which is the question actually being asked.

## [0.19.0] - 2026-08-13

Two themes: correlation that no longer needs a code change per fault
class, and a run-to-run view of what actually changed.

Storm correlation used to answer "are these one incident?" from the
topology graph alone, so every blast radius that lives outside the
cluster — a registry, a cloud API, a DNS resolver — arrived either as a
hardcoded special case or not at all. A correlation key now comes from
one of three sources and every storm records which: the graph
(`topology`), a named external dependency (`registry-host`, now a
documented extension point), or an attribute discovered in the window
itself (`mined:<attribute>`, behind `--storm-mine` and off by default,
since a mined key is circumstantial where a modelled one is causal).
The same release fixes the reason the registry key kept missing its
evidence: kubelet names a pull failure's cause once per object and then
repeats itself causelessly, so a replayed region-wide Artifact Registry
429 across seven workloads carried a usable cause on only two of them —
below the formation threshold, and seven separate root causes to dig
(#225).

On the reporting side, `lookout findings diff` turns a scheduled scan
into a transition surface — each subject `new`, `ongoing`, `escalated`,
`resolved`, or `suppressed` — instead of the same open findings
re-listed in full every run, and `lookout findings ack` time-boxes one
an operator has already picked up (#212). Finding state is durable and
cluster-scoped in the sentinel's existing `--store` file (schema v6),
so several clusters can share one store.

### Added

- `--storm-mine` correlates on *discovered* keys, for blast radii nobody
  modelled (#225). Storm correlation has always grouped incidents by
  something declared in advance: a topology ancestor from the graph, or
  — new in this release — a named external dependency such as the
  registry host. Both mean a code change before a new kind of fault can
  be seen as one incident. With `--storm-mine`, incidents in the window
  that share an exact image reference, node or container group into one
  storm even when nothing connects them in the cluster: one bad digest
  rolled out to five unrelated Deployments is one session, not five.

  Off by default, and deliberately so — a mined key is circumstantial
  where a modelled one is causal. It needs more members than a declared
  key to form (`--storm-mine-min`, which can never be set below
  `--storm-min`), it matches exact values only, and every mined storm
  names the attribute it grouped on, so the payload can always say *why*
  these N are one incident. Absent attributes never correlate: incidents
  with no node recorded do not become "the same node".

- Blast-radius keys for dependencies outside the cluster are now a
  documented extension point rather than a special case in the
  correlator (#225). A registry host, a cloud API endpoint, a DNS
  resolver — none has a vertex in the topology graph, and the registry
  key added in 0.18.0 was hardcoded because of it. Storms now record
  which source produced their key (`topology`, `registry-host`, or
  `mined:<attribute>`), so a grouping can be explained rather than just
  asserted.

- `lookout findings diff` — a run-to-run transition surface, so a
  scheduled scan reports what CHANGED instead of re-listing every open
  finding every time (#212). Pipe a report in
  (`lookout health | lookout findings diff --report=- --store=… --cluster=…`)
  and each subject comes back classified `new`, `ongoing`,
  `escalated`, `resolved`, or `suppressed`, with the previous
  severity and a `first_seen` that survives across runs. Add
  `--transitions=new,escalated,resolved` for the digest view: the
  summary line then reads `scanned=40 findings=3`. Either wire format
  is accepted on stdin, so the upstream command does not need
  `--format=json`. `--dry-run` classifies without consuming the report.

  A rescheduled pod stays one ongoing finding: subjects are keyed on
  `(cluster, namespace, kind, normalized-name, canonical-reason)`,
  with Kubernetes' generated name suffixes stripped. This is a second,
  instance-grain key — the §8 class fingerprint is unchanged and still
  drives fleet rollup.

- `lookout findings ack <subject-key> --for 4h --by …` — suppress one
  finding for a window after an operator has taken it (#212). Later
  diffs report it `suppressed` rather than re-raising it, and it comes
  back on its own when the window expires; a severity bump that
  happened inside the window fires as an `escalated` transition at
  expiry rather than being swallowed. `--clear` ends a window early.
  Distinct from a §9.4 `severity_override`, which is a standing
  judgment backed by a diagnosis; an ack is time-boxed and asserts
  nothing. Lookout is the store of record for the ack; the caller
  owns identity — `--by` is recorded verbatim, not authenticated.

  Finding state is durable, in the sentinel's existing `--store` file
  (schema v6, table `finding_state`): a diff with nowhere to persist
  would report everything `new` forever, so `--store` is required.
  State is cluster-scoped, so several clusters can share one store
  file: each run diffs and rewrites only the cluster named by
  `--cluster`. The multi-cluster watch path (#208) keeps its own
  in-memory state and does not feed this table yet. Design note:
  `docs/findings-diff-design.md`, summarized as DESIGN.md §9.5.

### Fixed

- A registry-wide fault now correlates into one incident even when most
  of its events never say what went wrong (#225). The registry-scoped
  storm added in 0.18.0 (#213) keyed only off failures whose message
  named a retryable cause — but kubelet states the cause once per object
  and then repeats itself causelessly. Replaying a real region-wide
  Artifact Registry 429 that hit seven workloads across two namespaces,
  only *two* of the seven carry a registry key. Two is below the
  formation threshold, so no storm formed and each would dig its own
  root cause. Cause evidence is now remembered per
  registry host as well as per object: a causeless `Back-off pulling
  image` inherits the host's known-retryable state, and the seven arrive
  as one incident.

  Only retryable causes propagate host-wide, and on a much shorter clock
  than per-object evidence — a bad tag is a statement about one image
  reference, not about the registry serving it, so it stays with the
  object that reported it. Objects keep their own evidence ahead of the
  host's, so a pod failing terminally during a rate limit still fires
  immediately.

## [0.18.0] - 2026-08-13

A correlation release, with one breaking change for anyone scraping the
sentinel's metrics. Three of the changes below attack the same failure
mode from different angles: one root cause producing more than one thing
to triage. A missing secret opened an enriched per-incident session for
the pod *and* a separate watchboard session for its Deployment — a
buffered warning now reattaches to a live incident sharing its
blast-radius ancestor instead (#220). A registry rate limit opened a
session per affected pod across unrelated namespaces, for a failure
kubelet clears by itself on its next retry — retryable pull failures are
now recognized as such, debounced rather than fired on sight, and
correlated into a single registry-scoped storm (#213, #216). Each is
strictly subtractive on the wire: no signal is lost, it lands somewhere
that already has the context.

Separately, one `lookout watch` process can now optionally watch several
clusters (#208) — `--clusters` or `--clusters-from`, each cluster an
independently supervised runner whose failure restarts only itself. One
sentinel per cluster remains the default and the recommendation; leave
both flags unset and nothing changes.

**Breaking (metrics):** every `lookout_*` series now carries a `cluster`
label. Dashboards and alerts that match those series by an exact label
set must add or ignore it — see the migration note under *Changed*.

### Added

- A watchboard warning now reattaches to a live incident sharing its
  blast-radius ancestor, instead of becoming a second session (#220).
  A missing secret on a live cluster produced exactly two things to
  triage for one root cause: a critical `FailedMount` on the pod, which
  opened an enriched per-incident session naming the missing secret,
  and an `objectstate.progress_deadline` on its Deployment — same
  failure, one level up — which §7.7 routed to the watchboard, where it
  became a digest entry in a *different* session with no pointer back
  to the diagnosis. `rollout.stall` followed and made it three signals
  across two sessions.

  At flush time the sentinel now asks whether a buffered warning's
  blast-radius ancestor (§7.5 classes 0–2: placement, owner chain,
  shared config/PVC) already owns a live per-incident session. If it
  does, the warning is delivered there as a `kind=family.member`
  followup — the §10.3 shape a cross-source dedup join already uses —
  and never becomes a digest entry. A flush whose every entry
  reattached creates no watchboard session at all. Strictly subtractive
  on the wire: one `Append` replaces a session create plus a digest
  inject.

  Flush time rather than route time is the whole trick. In the trace
  above the warning was buffered 60ms *before* the critical event
  opened its session, so a check at buffering would have found nothing;
  the batching delay the watchboard already imposes is what makes the
  correlation possible.

  Bounded and conservative by construction: at most one reattachment
  per source family per target incident per dedup window (the existing
  cross-source join budget), namespace-class ancestors never match (every
  incident in a namespace shares one), and storm-claimed sessions are
  never targets (§7.5 exists to collapse fan-out, not receive it). The
  reattached incident is bound to the session, so its §7.4 recovery
  outcome closes where its evidence landed. A failed followup inject
  falls back to the digest — never to silence.

  The stage rides the `--storm` topology graph and is inert without it:
  with `--storm-window=0` every warning digests exactly as it did
  before, and startup says which of the two is in effect. New metric
  `lookout_watchboard_reattached_total{kind}`.

- Image-pull failures are classified by error class, and the retryable
  ones are debounced (#213). 0.16.0's crash-loop gate deliberately
  exempted the whole image-pull family on the grounds that "a bad tag
  is persistent" — true of a bad tag, but the same family also carries
  registry rate limits (`429 toomanyrequests`, Artifact Registry
  per-region quota), 5xx, and connection timeouts, which kubelet
  clears by itself on its next retry (10s → 20s → 40s → …). Those were
  opening a session and firing an alert for an incident that no longer
  existed by the time anyone looked. The sentinel now reads the failure
  cause out of the message and sorts it into **terminal** (bad tag,
  `manifest unknown`, denied/unauthorized, `no space left on device`),
  **retryable** (429/quota, 5xx, i/o and TLS timeouts, connection
  reset), or **unknown**, and applies a new `--imagepull-transient-min-count`
  (default 3) to the retryable class only. Terminal and unknown causes
  still fire on the first event, so the gate can only ever delay a
  failure it positively recognizes as self-clearing. Set
  `--imagepull-transient-min-count=1` to restore 0.16.0 behavior.

  Because kubelet splits the incident across two events — the cause
  rides `reason=Failed`, which the default `--reason` allow-list drops,
  while the allow-listed `reason=BackOff` message carries no cause at
  all — the class is resolved per object ahead of the allow-list and
  remembered for 10 minutes, so the causeless back-off inherits the
  cause the `Failed` event named. This carry-forward is what makes the
  gate real rather than decorative; it applies to terminal causes too,
  which is why a bad tag keeps firing on its first back-off.

- Registry-wide pull failures correlate into one storm (#213). A
  retryable pull failure now contributes a synthetic `Registry/<host>`
  ancestor (e.g. `Registry/us-east1-artifactregistry.gcr.io`) to storm
  correlation, ranked ahead of the owner chain, so a quota exhaustion
  that hits pods across unrelated Deployments and namespaces forms a
  single registry-scoped storm instead of N unrelated sessions.
  Topology-only correlation could not group these: the members share
  nothing but the registry they pull from. Terminal failures do not get
  the key — two workloads with independently bad tags are two
  incidents.

- `lookout_events_filtered_total{gate}` counts every signal the filter
  drops, labelled by which rule dropped it (`reason_not_allowed`,
  `namespace_excluded`, `namespace_not_allowed`, `unhealthy_debounce`,
  `crashloop_debounce`, `imagepull_transient_debounce`) — so the new
  gates, and the pre-existing ones, are observable instead of silent.

- **Optional multi-cluster watch (issue #208).** One `lookout watch`
  process can now watch several clusters. Two mutually-exclusive flags
  select the fleet: `--clusters` takes comma-separated `name=endpoint`
  pairs (a bare endpoint derives a short name from its first DNS label),
  and `--clusters-from` discovers them from a `project` or
  `project/location` via the cloud provider's cluster API. Both need a
  Fleet-capable provider build (`-tags gke`); on GKE they authenticate
  kubeconfig-free with Application Default Credentials over each
  cluster's DNS control-plane endpoint (per-cluster RBAC still required).
  Each cluster runs as an isolated *runner* — its own clients, informers,
  sources, and store — supervised so one cluster's failure restarts only
  that runner (bounded backoff) and never ends the process; the new
  `lookout_runner_up` and `lookout_runner_restarts_total` metrics expose
  this. Project-tier sources (quota, notifications) run once per distinct
  project rather than once per cluster. **One sentinel per cluster
  remains the default and recommended deployment** — leave both flags
  unset and nothing changes. `--cluster-name`, `--kubeconfig`,
  `--in-cluster`, `--store`, and `--dedup-persist` are rejected in
  multi-cluster mode (they are inherently single-cluster).

### Changed

- **Breaking (metrics):** every `lookout_*` series now carries a
  `cluster` label whose value is `--cluster-name` (empty string when
  unset). This is groundwork for optional multi-cluster support (one
  sentinel watching N clusters, issue #208), but it lands for
  single-cluster deployments too — several sentinels scraped into one
  Prometheus are now filterable by cluster. **Migration:** dashboards
  and alerts that match `lookout_*` series by an exact label set must
  add/ignore the `cluster` label; set `--cluster-name` so the value is
  meaningful. One-sentinel-per-cluster remains the default and
  recommended deployment.

### Fixed

- The image-pull classifier could read a failure class out of the
  image reference instead of out of the error (#216, follow-up to
  #213). Two ways: `429` was matched as a bare marker, so any digits
  in a sha256 digest (about a 1.5% chance per pull), a tag like
  `:v429`, or an Artifact Registry `project_number` classified the
  failure retryable — holding a real, non-transient failure for three
  events; and a repository path such as `registry/denied-team/app`
  matched the terminal `denied` marker, firing a registry timeout
  immediately and denying it a `Registry/<host>` storm ancestor. The
  classifier now blanks the reference and all of its parts (path,
  tag, digest, and the copies kubelet repeats in `failed to resolve
  reference` and in the registry URL) before matching, and every
  retryable marker is a phrase rather than a status number — the
  registries that rate-limit say `429 Too Many Requests` or
  `toomanyrequests:` in words. A naked `: 429` with no reason phrase
  now classifies unknown and fires immediately, which is the safe
  direction. Blanking covers the percent-encoded copy too: a registry
  that authenticates per repository puts the path in a token-URL query
  (`?scope=repository%3Aproj%2Fteam%2Fapp%3Apull`), where only the
  delimiters are escaped and the path segments survive verbatim — so
  an Artifact Registry repository named `denied-team` still leaked the
  marker until the encoded form was blanked as well. Four verbatim GKE
  1.36 messages are pinned as tests, two of them captured from a live
  drill that also confirmed the end-to-end behaviour: a repository
  named `denied-team` behind an unreachable registry is now held by
  `--imagepull-transient-min-count` instead of firing on the first
  event, while a missing repository still fires immediately.

- Every row of the docs-site metrics reference rendered with a
  trailing `", unit: "` glued to its description. The generator
  derives names and help text by parsing `prometheus.Desc.String()`
  (its fields are unexported and it exposes no accessors), and
  client_golang v1.24 added a `unit` field between `help` and
  `constLabels` — which the greedy help match swallowed. The parse now
  matches each field exactly and treats `unit` as optional, so it
  survives both client_golang layouts. Docs-only; the metrics
  themselves were never affected. The bug was invisible because every
  existing guard compares generated output against generator output,
  making a parse bug self-consistent, so the parser now has a
  round-trip test against a real collector.

### Docs

- **Assessment of LangChain's `sre-agent` sample against k8s-lookout**
  (`docs/assessments/langchain-sre-agent.md`, a new `docs/assessments/`
  home for external comparisons). That project's Python utility layer
  serves roughly the purpose lookout serves for core-agent, so it is a
  useful mirror. Covers its agent topology (nine subagents over 56
  tools, dispatched only by LLM `task` calls), the finding that its
  production monitoring path bypasses those subagents entirely for a
  30-minute poll plus one Haiku call, a capability-by-capability
  head-to-head, and the architectural trade. Conclusion: lookout is a
  strict superset on incident detection; six capabilities are worth
  adopting (a missing-`Resources.Requests` census with LimitRange
  awareness ranks first), two are posture checks parked behind
  `fleet-audit-detectors-design.md` Open Question 1, and its 19
  mutating tools are a counterexample rather than a candidate — the
  sample's writer ClusterRole is effectively cluster-admin, gated only
  by an approval prompt.

- **`docs/roadmap-post-m5-sensors.md` status refreshed** — it was stale
  as a gap list and had been cited as a live one. Every Tier A–C item
  now carries ✅ shipped / ◐ partial / ○ open, verified against the
  tree: ten of twenty have shipped (#128, #129, #130, #131, #132, #134,
  plus §7.7 severity routing and the `--cluster-name` half of A.4).
  The shortlist table gains a status column; C.5 is marked event-half
  only, and C.2's audit-log attribution is noted as now unblocked by
  A.1 having landed. Tier D is unchanged as policy, with a
  clarification that the Gateway API *source* (#168) shipping does not
  touch its "no" on Gateway-API **graph kinds** — `pkg/graph` still has
  no gateway node or edge type.

## [0.17.0] - 2026-08-11

A naming release: the transition scaffolding is gone. The watch-path
sentinel began as core-agent's `k8s-event-watcher`, moved verbatim so an
existing deployment could swap the image with zero config change, and
kept that project's names — metric prefix, Kubernetes resource names,
log prefix — frozen for the duration. That transition is complete, so
everything now reads as `lookout`. This is a **breaking** change for
anyone running the sentinel: Prometheus dashboards/alerts on the old
`k8s_event_watcher_*` prefix and any tooling that addresses the
`k8s-event-watcher` Kubernetes resources by name must be updated — see
the migration note below. The flag surface, exit codes, RBAC rules,
ports, and the `k8s-event` inject wire shape are unchanged.

### Changed

- **BREAKING: retired the `k8s-event-watcher` transition naming.** The
  sentinel was moved verbatim from core-agent's `k8s-event-watcher` and
  kept that project's names for a drop-in image-swap during the
  transition; that period is over, so the names now read as `lookout`:
  - **Prometheus metric prefix `k8s_event_watcher_*` → `lookout_*`.**
    Every series is renamed (e.g. `k8s_event_watcher_events_seen_total`
    → `lookout_events_seen_total`). Dashboards and alerts referencing
    the old prefix must be updated — a scrape after upgrade shows only
    the new names.
  - **Kubernetes resource names `k8s-event-watcher` → `lookout-watch`**
    (Deployment, ServiceAccount, ClusterRole, ClusterRoleBinding, the
    `-capacity` Role/RoleBinding, NetworkPolicy) and the token Secret
    `k8s-event-watcher-token` → `lookout-watch-token`. A `kubectl apply
    -k` upgrade of an existing deployment creates the new-named
    resources alongside the old ones — delete the old set after
    cutover (`kubectl -n agent-triage delete deploy/k8s-event-watcher
    sa/k8s-event-watcher …`), or redeploy fresh.
  - The stderr log prefix is now `lookout watch:` (was
    `k8s-event-watcher:`) and the inject truncation marker is now
    `[truncated by lookout]`. The `--flag` surface, exit codes, RBAC
    rules, ports, and the `k8s-event`/`k8s-event-followup` inject wire
    shape are unchanged.

## [0.16.0] - 2026-08-11

A noise-reduction release for the watch path. A pod that lost a node
scale-up race would flap `BackOff` for a minute or two and self-heal —
but the sentinel canonicalized that first `BackOff` to
`CrashLoopBackOff` and opened an incident before the recovery tracker
could resolve it, firing an alert for a transient that no longer
existed. `lookout watch` now debounces the crash-loop family on the
leading edge, requiring the canonical `CrashLoopBackOff` reason to
reach `--backoff-min-count` (default 3) before opening a session; a
genuine crash loop climbs past it within seconds. The gate is
message-aware, so the image-pull family (`ImagePullBackOff` /
`ErrImagePull`) is deliberately not debounced — a bad tag is
persistent and still fires on the first event.

### Added

- Leading-edge debounce for the crash-loop family (#197): a transient
  `BackOff` — e.g. a pod losing a node scale-up race that self-heals in
  ~2 minutes — no longer opens a `CrashLoopBackOff` session and fires a
  noise alert before the recovery tracker can resolve it. A new
  `--backoff-min-count` (default 3) requires the crash-loop family
  (canonical `CrashLoopBackOff` — kubelet's repeating `BackOff` cycle,
  and `CrashLoopBackOff` itself) to reach that `Event.Count` before
  firing; a genuine crash loop climbs past it within seconds. The gate
  keys on the message-aware canonical reason, so the image-pull family
  (`ImagePullBackOff`/`ErrImagePull`, and `BackOff` on a bad image) is
  deliberately NOT debounced — a bad tag is persistent and should fire
  fast. Mirrors the existing `--unhealthy-min-count` probe-flap gate.
  Set `--backoff-min-count=1` to restore firing on the first event.

## [0.15.0] - 2026-08-10

A reliability release for the incident-open path. A new incident whose
enrichment bundle pushed the inject body past the core-agent daemon's
8192-byte per-inject ceiling was rejected `400 request body too large`,
leaving a bound-but-empty session with the loss visible only in
`inject_errors_total`. The dispatcher now measures every payload at
wire size and fits it under the ceiling before delivery — shedding the
enrichment bundle first, then truncating the message, never touching
identity or routing — so an oversized incident still opens with
(trimmed) context instead of nothing. The `--enrich-cap` default drops
16384 → 4096 to leave headroom under the double-JSON envelope, and a
new `--inject-max-bytes` lets operators align the ceiling with their
daemon without a rebuild.

### Fixed

- Oversized incident injects no longer silently lose their opening
  context (#198): a new incident whose enrichment bundle pushed the
  inject body past the core-agent daemon's 8192-byte per-inject limit
  was rejected `400 request body too large`, leaving a bound-but-empty
  session. The dispatcher now fits every payload under the ceiling
  before delivery, shedding least-signal-first — enrichment bundle
  dropped, then message truncated — while never touching identity, so a
  shrunk incident still routes, dedups, and closes. Shrinks are logged
  and counted (`k8s_event_watcher_inject_shrinks_total`).

### Changed

- `--enrich-cap` default lowered 16384 → 4096 so the enrichment bundle
  leaves headroom under the inject ceiling for the rest of the payload
  and the double-JSON envelope (#198).

### Added

- `--inject-max-bytes` (default 8192): the per-inject wire-body ceiling
  the dispatcher fits payloads to, so operators can align it with their
  daemon's limit without a rebuild (#198).

## [0.14.0] - 2026-08-10

A signal-coverage and security-posture release. The sentinel gains new
leading-indicator sources — Gateway API health (#168), GCLB/Ingress
health (#135), HPA autoscaling (#131), and a cluster bin-packing
capacity forecast — plus node-pressure/eviction watches in
object-state and a `family.member` followup on cross-source dedup
joins (#132); the signal schema grows 46 → 48 kinds, append-only. On
the security side, bundles now degrade to a documented, secret-free
partial under a narrowed role (#192): the one List pass behind
`bundle`, the `k8s_triage_workload` MCP tool, and enrichment's
scoped-list fallback tolerate a per-resource `Forbidden`/`NotFound`,
report what they dropped as a `skipped=` note, and expose
`--lists`/`--enrich-lists` knobs so an operator can withhold the broad
`secrets: list` grant and still get a useful bundle.

### Added

- Partial bundles under least-privilege RBAC (#192): `state.LoadCluster`
  — the one List pass behind `bundle` / the `k8s_triage_workload` MCP
  tool and enrichment's scoped-list fallback — no longer fails
  all-or-nothing when a single resource is denied. A per-resource
  `Forbidden` or `NotFound` now skips that resource and continues, so a
  watcher/daemon ServiceAccount can withhold the broad `secrets: list`
  grant (which returns full Secret values at the API level) and still
  get a useful, secret-free bundle instead of nothing. The dropped
  resources are reported honestly as a `skipped=` note on the
  `bundle.target` head finding (both `bundle` and enrichment), never a
  silent gap. Two new flags select coverage explicitly: `--lists` on
  `bundle` and `--enrich-lists` on `watch` take `all` (default), a
  comma-separated allowlist (`pods,deployments`), or subtractions
  (`all,-secrets`); an optional `--lists-preflight` /
  `--enrich-lists-preflight` runs a `SelfSubjectAccessReview` per
  resource to drop denied lists proactively (fewer 403s), falling back
  to the reactive skip if SSAR itself is not permitted. Non-Forbidden,
  non-NotFound errors still abort, and callers that pass no options
  (every non-bundle `state`/`triage` command) keep the strict
  all-or-nothing behavior unchanged.
- `gateway` signal source (#168 — Gateway API health, status
  conditions): the Gateway-API sibling of the `ingress` source. GKE
  steers most users to the Gateway API, whose programming failures
  never surface as the ingress-gce `Sync`/`Translate` events the
  `ingress` source keys on — they surface as `Programmed` /
  `Accepted` / `ResolvedRefs` status conditions reading `False` on
  `Gateway` and `HTTPRoute` objects. Two new kinds (signal-schema v1
  grows 46 → 48, append-only): `gateway.programming_failed` (a
  Gateway or listener held `Programmed=False` past the grace window —
  the load balancer/data plane is not being programmed; the analog of
  `ingress.sync_failed`) and `gateway.route_rejected` (a
  Gateway/listener or HTTPRoute parent held
  `Accepted=False`/`ResolvedRefs=False` — the route config never
  became routable; the analog of `ingress.translate_failed`). Fires
  only on SUSTAINED failure (`reason != Pending`, condition
  `observedGeneration` current, held past `--gateway-grace`, default
  5m) so a Gateway that is merely mid-provisioning stays quiet. Reads
  the CRs unstructured through a dynamic informer (no new dependency,
  the expiry-source precedent) and gates on the Gateway API CRDs via
  discovery: under `--sources=auto` it enables only where the CRDs are
  served, skipping with one loud line elsewhere (the grant is inert on
  clusters without the group). Shares the NEG half with `ingress`
  (`ingress.neg_failed` already fires for Gateway-backed Services —
  same NEG controller). New read-only `gateway.networking.k8s.io`
  list+watch grant in `deploy/12-clusterrole-watcher.yaml`; registers
  a §7.4 clearance observer (condition recovered / object deleted →
  cleared).
- `ingress` signal source (#135, half 1 — GCLB/Ingress health, event
  reasons): the ingress-gce controller's failure events, today the
  only in-cluster evidence that GCLB programming is failing while the
  Ingress object looks fine. Three new kinds (signal-schema v1 grows
  37 → 40, append-only): `ingress.sync_failed` /
  `ingress.translate_failed` (Warning `Sync` / `Translate` on an
  Ingress) and `ingress.neg_failed` (NEG sync/attach/detach/retry
  failures on a Service). The source owns these reasons with its own
  Warning-only Event informer (the capacity-source precedent) — a
  default k8s-events allow-list entry was disqualified because
  ingress-gce reuses reason `Sync` for Normal housekeeping events and
  the reactive path carries no event type. Rides the existing events
  grant (no new RBAC) and auto-enables under `--sources=auto`. The
  metrics half of #135 (LB backend unhealthy ratios) stays open.
- `kind=family.member` followup on cross-source dedup joins (#132):
  when a signal from a different source family attaches to an
  incident another source opened (e.g. capacity's `quota_blocked`
  folding into a `quota.forecast` session), the bound session now
  hears about it — a schema-stable followup carrying the joining
  signal's kind/reason/severity, the canonical family, the source
  family that opened the incident, and a `design_ref` for the join
  contract. At most one per source family per incident per dedup
  window; storm-claimed members never fan these out into the storm
  session. Additive schema-v1 change (v1 grows 40 → 41); previously
  the join followup reused the joining signal's own kind.
- Object-state now watches node pressure and eviction activity
  (#134): `objectstate.node_pressure` fires when a Node's
  MemoryPressure/DiskPressure/PIDPressure condition flips False→True
  (warning; one escalation to critical per episode when pressure
  sustains past 5m or an eviction burst hits the same node), and
  `objectstate.eviction_burst` folds 3+ pod evictions on one node
  within 10m into a single node-scoped signal instead of N pod-scoped
  ones. Both clear through the §7.4 recovery tracker — pressure when
  every pressure condition reads False, the burst when the eviction
  window drains.
- `capacity.cluster_forecast`: the capacity source now forecasts
  schedulable headroom per scheduling domain (GKE nodepool label,
  else zone, else the whole cluster) — the slope of summed pod
  requests over summed node allocatable, per CPU and memory
  independently, projected to "full in ~N hours" with the same
  linear-window machinery as `saturation.forecast` (warning at ETA
  ≤ 4h, critical at ≤ 1h, 3h regression window). Clears via the §7.4
  recovery loop when headroom recovers past hysteresis or the domain
  disappears. The source now also lists/watches nodes (already
  granted in the shipped ClusterRole). Part of #131.
- New `autoscaling` watch source (#131): HPA leading indicators read
  from `autoscaling/v2` status conditions, no events RBAC needed —
  `autoscaling.hpa_pinned` when an HPA has sat at maxReplicas with its
  metric still over target for 10 minutes (the autoscaler is out of
  headroom; escalates to critical at 30 minutes), and
  `autoscaling.hpa_metrics_dead` when the HPA's metrics pipeline
  reports `FailedGet*` for 15 minutes (autoscaling silently dead;
  warning-only). Both clear through the recovery loop when the HPA
  scales back inside its range / the pipeline computes replicas again.
  HPA *thrash* detection stays on the read path (`lookout triage
  events`). Auto-enabled under `--sources=auto` when HPA list/watch is
  granted; `deploy/12-clusterrole-watcher.yaml` grows the
  `autoscaling`-group grant.

### Fixed

- The failed-mount example scenario's verify no longer flakes when the
  FailedMount incident is absorbed into a node storm (#154): crashloop
  runs just before it on the same node, so inside the storm window the
  incident arrives as a `kind=storm` representative instead of a fresh
  `k8s-event`. The verify now accepts both routings, the same guard the
  image-pull scenario already carries.

## [0.13.0] - 2026-07-31

The adoption release, driven by feedback from the first outside users:
the docs now read as user docs — the three surfaces (the `lookout`
CLI, the MCP server, the in-cluster sentinel) are named up front, and
internal milestone references are gone from prose and runtime strings
alike; the sentinel installs with one `kubectl apply -k`, no clone;
the site publishes an agent-optimized surface (`llms.txt`, a curated
`/agents/` guide, and a 20-minute tutorial whose every output block
was captured live); and releases now ship prebuilt workstation
binaries for Linux, macOS, and Windows in both flavors, with
keyless-signed checksums.

### Added

- Prebuilt release binaries: every release now attaches workstation
  binaries for linux amd64/arm64, macOS amd64/arm64, and windows
  amd64 — in both flavors (`lookout_*` GCP-free, `lookout-gke_*`
  with the GKE provider) — with a keyless-signed SHA256SUMS.
  `gh release download -R go-steer/k8s-lookout -p 'lookout_*_<os>_<arch>.tar.gz'`.

- Agent-facing docs surface: the site now publishes `llms.txt`,
  `llms-full.txt`, and `llms-small.txt` (llmstxt.org convention, via
  starlight-llms-txt), a curated "Using k8s-lookout from an AI agent"
  entry page (`/agents/`), and a ~20-minute end-to-end tutorial
  (`/getting-started/tutorial/`) built on `examples/` with real
  captured output — kind cluster, staged crashloop, user-invisible bad
  deploy, and the verified `resolved` closing the loop.

- `deploy/kustomization.yaml`: the sentinel now installs without a
  clone — `kubectl apply -k "github.com/go-steer/k8s-lookout/deploy?ref=vX.Y.Z"`.
  From a clone, use `kubectl apply -k deploy/` — `apply -f` on the
  directory no longer works (it chokes on the kustomization file);
  `examples/sentinel/up` updated accordingly.

### Changed

- The docs site now reads as user docs: the landing and getting-started
  pages name the three surfaces (CLI, MCP server, sentinel) explicitly,
  install leads with the quickest path per audience, and internal
  milestone/project references were swept from prose and runtime strings
  (metric help text, `--enrich-cap`/`--diff` help, the signal-kind
  ledger). Captured drill output is unchanged.
- `lookout triage spec --diff` now says "not yet implemented" instead of
  citing an internal milestone; behavior is unchanged (it was never
  implemented).

## [0.12.0] - 2026-07-30

Two new watch sources and the fixes from the first real-cluster
evaluation (GKE Autopilot, issues #144-#146): the `workload` source
(failed Jobs, dead CronJob schedules) and the `notifications` source
(GKE upgrade events and security bulletins); the RBAC probe now
repeats the authorizer's own reason instead of coaching RBAC widening
for platform denials, and saturation degrades its PVC dimension on
Autopilot instead of dying; build identity (version+commit+date) is
stamped everywhere including the sentinel's first log line; the
deploy manifest is current and auto-sourced; releases now publish
GitHub Release objects and the workflow guards the manifest pin and
the flavor tags (the v0.11.0 `:latest` mispointing cannot recur).

### Fixed

- The startup RBAC probe no longer misdiagnoses platform denials as
  missing grants (#145): SelfSubjectAccessReview reasons now surface
  verbatim in probe failures — on GKE Autopilot, Warden's
  "managed-namespaces-limitation" denial of `nodes/proxy` reads as
  the platform policy it is, instead of an instruction to widen RBAC
  that no grant can satisfy. The saturation source's `nodes/proxy`
  requirement is now optional: on Autopilot the source runs with
  CPU/memory forecasting (`metrics.k8s.io`) and reports the PVC
  dimension degraded, rather than being disabled (or, under an
  explicit `--sources` list, crash-looping the sentinel).

### Added

- Build identity, core-agent style (#146): `lookout version` and the
  new `--version` flag report `lookout vX.Y.Z (commit <sha8>, built
  <date>)`; release images stamp version + commit + date;
  `go install` builds fall back to Go's embedded module/VCS metadata
  instead of the literal `dev`; and the sentinel logs its build
  identity as its first startup line, so a running pod is
  identifiable from `kubectl logs` alone.

### Changed

- `deploy/51-deployment-watcher.yaml` now pins the current release
  (it sat at v0.1.0 for eleven releases, #146) — the pin is bumped by
  each release-cut commit and enforced by the release workflow's
  preflight guard — and rides the binary's `--sources=auto` default
  instead of an explicit source list: the Autopilot crash-loop in
  #145 was auto-skippable. NOTE: auto never enables `token-burn`, so
  the shipped manifest no longer watches the daemon cost stack by
  default — re-pin the explicit list (documented in-file) to keep it;
  strict fail-fast deployments re-pin for §11 semantics anyway.

- New `notifications` watch source (issue #130, explicit-only like
  `quota`): reads the provider's cluster-notification stream — GKE's
  notificationConfig Pub/Sub topic via a new `notifications` provider
  capability and `--notifications-subscription`. Upgrade events are
  store-recorded (info) so incident windows correlate with upgrade
  windows; `UpgradeAvailableEvent` is recorded for pre-warning;
  security bulletins land on the watchboard (warning). Backlog older
  than 1h at receipt is dropped loudly, never replayed as live
  signal.

### Fixed

- Release pipeline: the `-gke` matrix leg no longer claims the bare
  `:latest` tag. docker/metadata-action's implicit `latest=auto`
  added `:latest` on any release tag in BOTH flavor legs, so
  whichever leg pushed last owned it — on v0.11.0 that left
  `:latest` pointing at the gke-flavor digest (breaking the README's
  ":latest = GCP-free default" contract) and failed the default
  leg's signature verify. `flavor: latest=false` makes the explicit
  `latest` / `latest-gke` entries the only source of those tags.

## [0.11.0] - 2026-07-30

An interop-fix release. The injector's bodyless `POST /sessions`
now declares `Content-Type: application/json`, which core-agent's
browser-CSRF guard (shipped in its 2.8.0-dev.1) requires on every
state-changing attach request — without it, every per-incident
session open against a current daemon failed at dispatch with 415
(#139). `v0.11.0` is therefore the watcher floor for core-agent ≥
2.8.0-dev.1. Also ships `stab drift --identity` audit attribution
behind the new `audit` provider capability (#128).

### Fixed

- Session create against core-agent ≥ 2.8.0-dev.1 no longer fails
  with 415. The daemon's browser-CSRF guard (core-agent #431)
  requires `Content-Type: application/json` on every state-changing
  request — including the bodyless `POST /sessions` — and the
  injector's `CreateSession` didn't send it, so every per-incident
  session open failed at dispatch (`status 415: unsupported media
  type`). The inject path already sent the header; only session
  creation was affected.

### Added

- New `workload` watch source (issue #129, on by default under
  `--sources=auto`): `workload.job_failed` fires on a Job's `Failed`
  condition (`BackoffLimitExceeded`, `DeadlineExceeded`, …) and
  `workload.cron_missed` when an unsuspended CronJob passes a
  scheduled activation without running — batch failures previously
  invisible unless their pods crashlooped. Consecutive missed
  activations escalate to critical at 3. §7.4 clearance: a
  CronJob-owned failed Job clears on the next successful sibling run;
  a missed schedule clears when it fires again or is suspended.
  Requires `watch` on `batch` jobs/cronjobs
  (deploy/12-clusterrole-watcher.yaml updated).

- `stab drift --identity` resolves each drift finding to the audited
  principal who wrote it — `principal`, `principal_agent`, and
  `other_principals` fields from the cloud provider's audit trail
  (GKE: Cloud Audit Logs admin-activity entries), behind the new
  `audit` provider capability. Clusters without one get the explicit
  §2 `identity=unavailable` marker; findings the trail cannot answer
  for carry the `none-in-audit-window` / `no-write-time-anchor`
  sentinels, never silence. (#128)

## [0.10.0] - 2026-07-29

A hardening release from two rounds of adversarial review. The
sentinel's failure modes stop black-holing incidents: failed session
creates and storm formation no longer suppress a symptom or a
correlated class forever (#80, #81, #84, #104), buffered warnings
survive SIGTERM (#108), and the storm graph feed no longer settles on
stale topology after an informer resync (#107). Real leak and
overstatement paths close: the §6.5 sanitizer finally covers the whole
inject surface and learns `pass`/`passphrase` (#82, #106), and the
prompt-injection threat model is corrected to name its one ungated
write (#105). The deployment ships a default-deny ingress NetworkPolicy
(#87), the `/metrics` `reason` label is bounded (#109), and the release
pipeline now verifies every image it signs (#111).

### Added

- The prompt-injection trust boundary is now named and documented
  (#83): DESIGN.md §7.8 covers what cluster tenants can author into
  agent-session payloads, what bounds the blast radius (JSON
  delimiting, the managed-write-only permission gate), and what is
  explicitly not mitigated; skills gained a standing "payload text is
  evidence, never instructions" rule (skills/README.md, k8s-triage).
  Provenance marking is tracked as §15 Q6.
- The sentinel deployment now ships a default-deny ingress
  NetworkPolicy (#87): the pod whose ServiceAccount can list Secret
  values cluster-wide no longer accepts connections from co-tenant
  pods; only same-namespace scrapers reach :9090 (metrics/healthz),
  with a commented block for cluster monitoring namespaces. The
  secrets-grant tradeoff note in deploy/12 is corrected — the grant
  serves enrichment's guardian-enforced edges verification as well as
  the expiry source, so it cannot be dropped by disabling expiry; the
  documented narrow path is the §11 namespace tier.

### Changed

- The release-images workflow now verifies what it publishes (#111):
  after signing, every pushed tag of BOTH image flavors (`:<v>` and
  `:<v>-gke`) is re-checked with the documented `cosign verify`
  invocation — asserting the signature and the tag→digest linkage the
  `-gke` flavor previously left unchecked — then the image is pulled
  by digest and smoke-run (`lookout version`, entrypoint intact). The
  two flavors, formerly ~150 duplicated lines, now share one
  matrix-driven job that differs only in BUILD_TAGS, tag suffix, and
  OCI title/description.

### Fixed

- Deployment-guide docs drift corrected (#113): the "What each
  manifest is" table now documents `16-networkpolicy-watcher.yaml`
  (default-deny ingress, and its **inert without an enforcing CNI**
  caveat), and the M5 milestone note no longer claims the `--storm`
  default "stays OFF" — 0.9.0 shipped auto-defaults (#75), so
  `--storm=auto` probes the graph-informer grants and resolves on
  where present.
- The `/metrics` `reason` label can no longer grow without bound
  (#109): `Event.reason` is free-form (raw k8s events, scheduler
  predicate text), yet it was stamped verbatim onto the reason label
  of the events/inject counters — one Prometheus series per distinct
  reason string. A runtime distinct-value cap now keeps the first 100
  distinct reasons and collapses the rest to `other`, bounding
  cardinality without a static allowlist to maintain.
- Buffered watchboard warnings are no longer dropped on shutdown
  (#108): the interval-flush loop ran in a fire-and-forget goroutine,
  so on SIGTERM the sentinel could exit before that goroutine's final
  best-effort flush landed. The shutdown path now joins the loop and
  blocks on its last flush (bounded by the existing 3s timeout).
- The storm-correlation graph feed no longer settles on stale topology
  after an informer resync (#107): arming set `armed=true` and dropped
  the lock before replaying the pre-arm buffer, so a live delta racing
  that window was queued ahead of the buffered initial-sync deltas and
  a stale buffered placement could clobber a newer live change for the
  same object. Arm-and-replay now runs under a single critical section,
  guaranteeing buffered deltas always precede live ones.
- The prompt-injection threat model no longer overstates its
  guarantees (#105): DESIGN.md §7.8 and principle 6 claimed *every*
  agent write — triage-status records included — routes through the
  daemon's permission gate. `lookout triage status` actually writes the
  sentinel's `--store` directly and ungated, and a hostile
  `--severity-override` can suppress paging for the affected
  incident/object until the §7.4 recovery flip. The docs now name this
  ungated third write and its scoped paging-suppression effect, and the
  MCP surface stops advertising the write tool `k8s_triage_status` as
  `ReadOnlyHint:true` (new `checks.Command.Writes` marker) so
  convention-following clients no longer auto-approve it as a read.
- The secret sanitizer now masks `pass`/`passphrase` env names and
  flags (#106): env vars like `DB_PASS`, `PG_PASS`, `REDIS_PASS`, or
  `SSL_PASSPHRASE` holding a low-entropy human password (below the
  shape/entropy heuristic) previously leaked verbatim through `triage
  spec`, enrichment specs, and MCP output because the credential word
  list only had `password`/`passwd`/`pwd`. Both the env-name path and
  the `--db-pass=`/`--ssl-passphrase` flag path now cover them, with
  whole-word/segment anchoring so `BYPASS`, `COMPASS`, and `--bypass`
  are untouched.
- Storm suppression now holds when a storm's session create fails
  (#104): the #94 storm retry and the #84/#96 unbound-retry did not
  compose — a session-less storm member's next duplicate passed the
  unbound-retry guard and opened a competing per-incident session,
  the §7.5 N-session fan-out. The dedup result now carries the storm
  fingerprint so the guard excludes storm-claimed members (recording
  them as `storm-member`, session-less until the storm's retry-on-
  attach reopens it), the failed-formation path now marks the storm
  trigger's dedup entry so its duplicates are recognized too, and the
  §9.1 store record on that path is restored.
- A failed session create for a new per-incident symptom no longer
  suppresses it forever (#84): the unbound dedup entry left behind by
  the failed open black-holed every later event for the key, because
  the case-3 LastSeen refresh kept a steady symptom stream inside the
  dedup window indefinitely (and `--dedup-persist` carried the unbound
  entry across restarts). The dispatcher now retries the open on the
  next duplicate — the per-incident mirror of #81's storm-path retry —
  binding, tracking, and recording exactly as the original open would
  have.
- Saturation, quota, and token-burn forecasts no longer fire false
  CRITICALs on flat or near-flat series (#80): the ETA conversion
  overflowed `time.Duration` when headroom/slope exceeded ~292 years —
  on amd64 the float→int64 conversion wraps to a *negative* ETA that
  read as "already breached", so the epsilon slope least-squares
  leaves on an idle pod's constant memory could open a critical
  incident session (and draft a quota increase). Projections beyond
  the representable horizon now read as no projection, via one shared
  clamp (`saturation.ETAFromSeconds`) at all three sites.
- A failed session create at storm formation no longer black-holes
  the correlated incident class (#81): the correlator commits the
  storm before the open, so a transient daemon outage at exactly that
  moment left a session-less storm that suppressed every later
  correlated incident (each attach refreshed the idle TTL, so the
  suppression outlived any ongoing burst). The dispatcher now retries
  the open on each subsequent attach and, on success, completes the
  interrupted formation — binds the session, rebinds and re-tracks
  every member, and delivers the owed supersede pointers. No Append
  is ever issued against an empty session id.
- The §6.5 sanitizer now really covers the inject surface (#82):
  incident, followup, triage-regressed, resolved, and storm payloads
  masked nothing — an event message carrying a URL password, JWT, or
  auth header reached the per-incident agent session verbatim, despite
  the "every payload on every surface" design claim (only the
  enrichment bundle was sanitized). Cluster-sourced free text (event
  messages, label values) is now masked at dispatcher payload
  assembly; innocent content passes through byte-identical, so the
  frozen wire shape is unchanged.

## [0.9.0] - 2026-07-28

The sentinel now configures itself: `--sources` and `--storm` default
to `auto`, probing what the deployment's RBAC actually grants. And the
repo grows a runnable e2e layer — `examples/` deploys the sentinel
against real workloads on kind, drives ten real failures, and asserts
on the wire; CI runs a smoke subset on every merge to main and the
full set weekly, non-blocking.

### Added

- `examples/` — a runnable e2e layer between the unit/contract suites
  and the human-run `dev/drills/` runbooks (#76): a kind recipe
  (metrics-server, image pre-pull, `--build` for a from-HEAD image), a
  sentinel + capture-stub deploy that applies the shipped `deploy/`
  manifests unmodified, the `lookout-demo` victim workloads, and ten
  inject/verify/revert failure scenarios (crashloop, image-pull,
  failed-mount, oom, pending, cert-expiry, pdb-gridlock,
  endpoints-empty, bad-rollout, node-failure) — each verified on BOTH
  surfaces: the schema-v1 inject wire via the stub capture, and the
  read-path CLI. Plus `examples/e2e` (the driver), an agent-harness
  guide (skills / `lookout mcp` / plain CLI, with per-scenario
  prompts), and GKE staging deltas in `examples/gke/`. Every script
  refuses to run unless the kubectl context is the examples cluster.
- Non-blocking kind e2e in CI (#77, #78):
  `.github/workflows/e2e-kind.yml` builds the image from HEAD and runs
  a smoke subset (crashloop, failed-mount, bad-rollout) on every push
  to main plus the full set weekly; failures upload the wire capture +
  sentinel log as artifacts and open an issue. PR presubmits stay
  hermetic — a live cluster never gates a PR (AGENTS.md testing
  convention amended to say exactly that).

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
