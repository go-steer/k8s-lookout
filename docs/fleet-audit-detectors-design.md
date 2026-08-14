# Fleet-audit detectors — deterministic posture checks for audit consumers

DESIGN.md §5 (Read-Path Tool Matrix) enumerates what `k8s-lookout` detects
today; §8 declares the signal schema "fleet-rollup ready" and freezes a
`Fingerprint` built to join the same incident class across clusters; §14 M5
names "a fleet-level consumer can rollup a multi-cluster staged stockout" as an
exit criterion. This note asks a downstream question those three leave open:
**can the periodic _fleet audit_ — today an LLM reading a Markdown SOP and
running `kubectl … | jq` by hand — be re-based onto deterministic `checks`
instead?**

The audit consumer in question is `kube-agents`' `fleet-audit` skill. It runs
five cron-scheduled streams (security/RBAC posture, upgrade readiness, workload
reliability, cost/waste, and config-consistency drift). The scanning is done by
a model interpreting shell recipes in `*_sop.md`; only the _publishing_ side (a
Python harness that validates findings, derives stable ids, computes run-over-run
deltas, and opens remediation PRs) is deterministic. That split leaves the part
that decides _what is true about the cluster_ unverifiable: the harness can
attest that a command was published, never that it ran or returned what the
model said it did. The 2026-08-03 rename-as-resolution incidents live on this
seam.

**Proposal: move the scanning half onto `checks.Command`s.** Where a stream's
questions are already answered by an existing check, the consumer calls it. Where
they are not — cluster-level GKE _config_ posture and _cross-cluster_ consistency
— add new `checks` that read `*container.Cluster` through the existing
`pkg/cloud` `Provider` and emit findings carrying `ScanFingerprint` values, so
the fleet layer joins on `(fingerprint, project/cluster)` exactly as §8 intends.
This note is a proposal, not a settled decision; the open questions at the end
are the ones that decide whether all five streams belong here.

## Why deterministic

A compiled detector makes three failure modes of the SOP approach
_structurally_ impossible rather than caught-if-lucky:

- **Fabricated all-clears.** An LLM that skips a check can still report "no
  findings." A `checks.Command` that runs emits its `scanned=`/`findings=`
  summary line (the `pkg/emit` contract); zero findings is a fact the process
  produced, not a claim.
- **Rename-as-resolution.** The audit ledger's delta is a set difference over
  ids; when the model spells the same finding differently run-to-run, the
  harness reports the rename as a fix (the compliance ledger did exactly this on
  2026-08-03). A finding whose `Fingerprint` is derived from the incident class
  (`engine.ScanFingerprint`, not object identity) is stable across runs by
  construction.
- **Unverifiable coverage.** "Did we check every namespace?" is answerable from
  a detector's scan bounds; from an SOP it is answerable only by trusting the
  transcript.

The cost is authoring velocity: an SOP check is a one-line `jq` edit; a `checks`
detector is Go plus a contract test plus a golden fixture (§13). That trade is
worth making for a fixed, long-lived roster of audit questions — which is what
these five streams are — and not worth making for a one-off investigation. This
note is about the roster, not the one-offs.

## What `k8s-lookout` already covers

Two of the five streams are substantially answered by checks that ship today.
Mapping each SOP check slug to the check/finding-kind that already produces the
same signal:

### Workload reliability (`obtainability-audit`)

| SOP check                        | Existing signal                                                  |
| -------------------------------- | --------------------------------------------------------------- |
| `single-replica`                 | `stab drain` → `drain.singleton`                                 |
| `blocking-pdb`                   | `stab drain` → `drain.pdb_gridlock` / `pdb.gridlocked`          |
| `no-memory-limit`                | `triage top` → `top.unlimited` / `top.unlimited_container`       |
| `overrequest`-adjacent headroom  | `triage top` → `top.saturation`                                 |
| `no-hpa` / `hpa-cannot-scale`    | partial — `triage events` → `event.hpa_thrash` (thrash, not absence) |

### Cost / waste (`fleet-wide-cost-analysis`)

| SOP check         | Existing signal                          |
| ----------------- | ---------------------------------------- |
| `unattached-disk` | `cloud orphans` → `orphan.disk`          |
| `orphan-lb`       | `cloud orphans` → `orphan.lb`            |
| `idle-address`    | partial — `cloud orphans` family         |
| `scaledown-blocked` | partial — `stab drain` blockers        |
| `overrequest`     | partial — `triage top` → `top.saturation` |

The remainder of these two streams (`no-requests`, `no-pdb`, `probes-*`,
`rigid-scheduling`, `no-spread`; `orphan-pv`, `unconsumed-pvc`, `idle-nodepool`,
`idle-namespace`, `terminal-pods`) is net-new but small, and lives in the same
packages (`pkg/checks/top`, `stab`, `cloudcheck`) alongside the checks it
extends. The mapping table above is the starting point, not the finished audit.

**A charter caveat worth stating plainly.** `k8s-lookout`'s existing detectors
find what is _abnormal now_ — a workload that is crashing, a disk that is
orphaned, a PDB that is gridlocked. Many audit checks instead find the _absence
of a safety net_ around a workload that is currently healthy (`no-pdb`,
`no-hpa`, `no-requests`, `probes-*`). That is a different detection philosophy —
posture, not incident. It is a deliberate design decision whether such
best-practice-posture checks belong in `k8s-lookout` proper or in a sibling
surface that imports the same `emit`/`engine`/`cloud` packages. The two config
streams below do not have this tension; the reliability and cost streams
partially do. Decision 1 below resolves it: posture ships here, in its own
`audit` command group, so the two philosophies stay separable by which group a
consumer calls. One slug named above moved as a result — `no-requests` was
classified as incident work, not posture, and belongs beside the `top.unlimited`
census in `triage top`.

## The two gaps

### Gap 1 — GKE cluster-config posture (net-new detectors)

Three streams are almost entirely reads of the GKE control-plane/node-pool
_config_, not of live workload state:

- **`compliance-audit`** — the flagship security stream. `exec-spy` and
  security-posture detection were explicitly cut from v2 as "security-detection
  scope creep" (DESIGN.md §5, "Cut from v2:", lines 351–359). Re-opening that
  decision for _config_ posture (not runtime exec spying) is the crux of this
  proposal. Its 11 checks: `privileged-container`, `host-namespace`,
  `hostpath-mount`, `cluster-admin-binding`, `wildcard-rbac`, `netpol-missing`,
  `default-sa-automount`, `workload-identity-off`, `legacy-metadata`,
  `public-control-plane`, `podsecurity-gaps`. Note `workload-identity-off` is
  the cluster-config sibling of the existing `state wi` (`k8s_workload_identity`)
  binding-chain check.
- **`security-patch-orchestrator`** — upgrade/patch readiness, 10 checks:
  `master-behind`, `pool-skew`, `fleet-spread`, `no-channel`, `no-autoupgrade`,
  `no-autorepair`, `no-maintenance-window`, `blocking-exclusion`,
  `stale-image-type`, `no-notifications`. Every one is a field of
  `*container.Cluster` / its node pools.
- **`fleet-consistency-drift`** — 19 config facets compared across a cohort
  (`release-channel`, `shielded-nodes`, `secure-boot`, `integrity-monitoring`,
  `network-policy`, `private-nodes`, `private-endpoint`, `authorized-networks`,
  `logging-components`, `monitoring-components`, `managed-prometheus`,
  `binary-authorization`, `node-autoprovisioning`, `pool-autoscaling`,
  `intra-node-visibility`, `datapath-provider`, `label-keys`, `image-type`,
  `database-encryption`) plus a derived `uncohorted` outlier guard.

The seam already exists: `pkg/cloud/gke` fetches the whole
`*container.Cluster` today via `GetCluster` (used for pod-CIDR sizing in
`cloud ipspace`), and the `Provider` capability model keeps `pkg/checks` free of
cloud SDK imports (AGENTS.md hard rule). A new `checks` group — call it `audit`
or `posture` — reads that object and emits findings; no new cloud plumbing, only
new field reads and a group added to `groupDocs` (`pkg/checks/command.go:136`).

The workload-level compliance checks (`privileged-container`, `host-namespace`,
`hostpath-mount`, RBAC, `netpol-missing`, `default-sa-automount`,
`podsecurity-gaps`) read from the Kubernetes API, not the cloud provider, and so
resemble the existing `state edges` detectors in shape.

### Gap 2 — fleet fan-out & rollup

`k8s-lookout` is one-process-per-cluster by design: `lookout watch` is "the
resident per-cluster sentinel," and `health` calls its own output "the
per-cluster answer a fleet-level consumer aggregates." The consumer is
explicitly _not_ in this repo (§14 M5). `fleet-consistency-drift` is inherently
cross-cluster — a facet is "drift" only relative to a cohort — so it cannot be
answered by a single-cluster process at all. Something has to run the per-cluster
detector across the fleet and roll findings up on `(Fingerprint, project/cluster)`.

Whether that fan-out lives here (a `lookout fleet` subcommand / thin aggregator)
or in the consumer (`kube-agents` / `core-agent`) is open question 2. The
`Fingerprint` contract is already designed for it either way — it hashes the
incident _class_, deliberately not the object, so identical postures across
clusters collapse to one rolled-up finding with a per-cluster breakdown.

## Consumer contract

The audit consumer replaces "model runs `jq`, model writes findings.json" with
"consumer runs `checks`, checks _are_ the findings":

1. The consumer invokes each stream's `checks.Command`(s) — via CLI (logfmt/json
   + terminating `scanned= findings= elapsed=` summary) or MCP (each command is
   already a tool named by its `MCPName`, no new wiring).
2. Each `emit.Finding` maps to a ledger finding: `Fingerprint` → stable id (no
   more `derive_finding_id` string-shape guessing), `Kind`/`Reason` → check
   slug, `Severity` → severity, `Details` → the evidence the SOP used to paste
   by hand.
3. The deterministic _publishing_ harness stays — delta, remediation PRs,
   stale-close semantics are genuinely good and orthogonal to how findings are
   produced. It simply ingests `emit` envelopes instead of prompting a model.

The prize on the consumer side is deleting the machinery that only exists to
police an LLM: `FINDING_ID_RE`, `derive_finding_id`, `ID_SCHEME` versioning, the
attestation-not-verification command validators. A detector's `Fingerprint` is
its id; a detector that ran is its own coverage proof.

## Non-goals & trade-offs

- **Not** runtime security spying. `exec-spy` stays cut (DESIGN.md §5); this is
  static config/posture, an audit-log or `GetCluster` read, not a syscall watch.
- **Not** a replacement for the publishing harness. Delta, PR authoring, and
  remediation-state tracking remain the consumer's job.
- **Authoring velocity regresses** from a `jq` edit to Go + contract test +
  golden fixture. Accepted for a fixed roster; see the intro.
- **Provider lock-in per detector.** Config-posture detectors read
  `*container.Cluster`; they are GKE-shaped. The `Provider` capability gate
  (`available bool`) is the honest way to express "this posture check has no
  answer on provider X."

## Decisions

The four questions this note left open were decided on 2026-08-14. Each is
recorded here with the reasoning, because in every case the reasoning constrains
the implementation more than the verdict does.

### 1. Charter — an `audit` command group, in this binary

Posture checks ship inside `k8s-lookout`, separated from incident checks by
**command group** rather than by binary. `triage`/`health`/`state` keep today's
semantics; `audit …` opts into the other kind of claim. The mechanism already
exists — a group entry in `groupDocs` (`pkg/checks/command.go`) — and this note
anticipated the shape when it suggested "a new `checks` group — call it `audit`
or `posture`."

The sibling binary was rejected on recurring cost: a second release artifact,
image, RBAC surface, deploy and version-fallback test, plus a fork of the three
metadata-driven doc generators. The isolation ladder also runs one way. Group →
build tag → separate binary can be climbed later if group separation proves too
weak; starting at "separate binary" cannot be reversed cheaply.

This resolves the "abnormality vs. posture" tension above by scoping it. Silence
still means something precise, but what it means now depends on which group you
called — and every command's terminating `scanned=`/`findings=` line already
says which one ran.

**All five streams ship here**, including the workload-level security detectors.
This reverses DESIGN.md §5's "security-detection scope creep" cut for _config_
posture; runtime exec spying (`exec-spy`) stays cut. It also supersedes
`docs/assessments/langchain-sre-agent.md` §6.3, which argued for taking
cloud-config posture only and leaving workload hardening, RBAC rules and
NetworkPolicy coverage to Kyverno/Gatekeeper/PSA. That recommendation is
recorded as considered and not taken; §6.3 should not be read as live guidance
on those rows.

### 2. Fan-out home — the consumer orchestrates, lookout derives

The original question bundled three things that do not share an answer:
fan-out (running a check against N clusters), rollup (reducing findings onto
`(Fingerprint, cluster)`), and cross-cluster detection (a finding that exists
only relative to a cohort).

**Fan-out belongs to the consumer**, as §14 M5 implies — it already owns cron,
credentials and scheduling. The `cloud.Fleet` seam that shipped with
multi-cluster watch is weaker evidence for "here" than it looks:
`docs/multi-cluster-design.md` scopes that work to "many small/dev clusters, one
pane — not large production fleets, which keep the per-cluster sentinel," and
audit is the production-fleet case.

**Rollup and cross-cluster derivation belong here**, as a read-path command
whose input is finding streams rather than a cluster. `lookout findings diff`
established that shape after this note was written: a `checks.Command` taking
`--report=<scan output>` and reducing it against stored state. A fleet rollup is
the same shape with a different reducer — no credentials, no cluster access,
testable from fixtures, and the comparison logic stays deterministic Go instead
of migrating into consumer Python, which is the outcome this whole note argues
against.

Consistency drift then decomposes cleanly: per-cluster detectors emit their
facet values as **inventory records** — `emit.Finding` with no `Fingerprint`, a
shape §8 already reserves for "scorecard lines, inventory records, probe
results" — and the rollup layer derives `drift` findings by comparing the set.

One seam is created and should be closed by the consumer contract: "a detector
that ran is its own coverage proof" becomes two-stage, so a rollup must assert
how many reports it expected against how many it received, or a short rollup
looks identical to a clean one.

### 3. Cohort model — invocation-scoped

A cohort is **the set of reports passed to the rollup**. No roster, no label
convention, no discovery, and no cohort state that can drift out of sync with
the real fleet — a roster that lags reality produces confidently wrong drift
findings, which is worse than none. The consumer knows its own topology;
"compare these five" is a shell loop, not a data model.

This costs nothing, because `uncohorted` is an outlier guard derived from the
comparison itself (a cluster differing on ≥6 facets is a different _kind_ of
cluster), not a roster-membership test. A roster would only answer a different
question — "which clusters were never scanned at all" — which is the consumer's
to answer.

### 4. RBAC checks — `audit rbac`, sharing the loader

`cluster-admin-binding` and `wildcard-rbac` land in the `audit` group, not in
`state edges` and not in a separate `sec`/`rbac` group.

The existing RBAC walk in `state edges` is a _correctness_ check:
`edge.rbac_dangling` fires when a binding points at a missing role — something
is broken now. These two slugs are the opposite shape: bindings that work
perfectly and grant too much. The deciding axis is therefore the posture/incident
split from decision 1, not which package happens to already list the objects.
Adding posture kinds to `state edges` would mean a consumer calling it for
incident triage starts receiving posture findings — precisely the leak group
separation exists to prevent.

The RBAC index builder in `pkg/checks/state/edges.go` should be **reused** by
`audit rbac` rather than duplicated. Share the loader, not the group.

A distinct `sec` group was rejected because it splits posture across two groups
— `audit` for cluster config, `sec` for workload and RBAC — forcing users to
learn which family a check belongs to. One `audit` group with roughly five
commands is the same order as `state`'s four.

## Exemptions

Posture checks are unusable without an opt-out: `no-pdb` on a deliberately
single-replica batch worker, a privileged container that is the CNI. But the
mechanism is constrained by the argument this note opens with. An opt-out that
makes a finding _disappear_ reintroduces unverifiable coverage through the front
door — "the audit found nothing" becomes unfalsifiable again, with the omission
now in a YAML file nobody reads instead of a model's transcript.

So **exempt must not mean absent**:

- **Annotate, never drop.** An exempted finding is still emitted, carrying its
  reason and expiry, and the terminating summary line gains an exemption count
  through the existing §6.6 `Writer.Note` seam — "the one place that cannot be
  missed." Consumers filter; lookout does not hide.
- **A git-reviewable config file**, not an in-cluster ConfigMap (edits bypass
  review) and not object annotations (they let the team requesting an exemption
  grant it to itself). An exemption should be reviewable by someone other than
  its beneficiary.
- **Mandatory reason and expiry.** A permanent exemption is indistinguishable
  from a check nobody wrote. Expired and expiring exemptions are themselves
  findings.

This is a third suppression axis and must stay distinct from the two that
exist: `findings ack` is operator-owned, transient and expiring ("known, I'm on
it"), and §9.4's `severity_override` is agent-owned and standing (it asserts a
diagnosis). A config exemption is owner-owned, durable and reviewed in git ("by
design, here").

An exemption is also **not** the remedy for a LimitRange false positive. An
exemption says "we accept this finding"; a defaulting LimitRange means the
finding is factually wrong, because the namespace supplied the request. That is
a correctness prerequisite for any missing-requests or missing-limits census,
not something the opt-out absorbs.

Because exemption state rides on `emit.Finding`, it is an additive §8 schema
change and must land with the first `audit` detector rather than after the
stream — retrofitting it across a built-out roster means re-cutting every golden
fixture.

## Tracking

Delivery is tracked in the GitHub issues linked from the epic
_"deterministic fleet-audit detectors for audit consumers"_ (label `tracking`).
