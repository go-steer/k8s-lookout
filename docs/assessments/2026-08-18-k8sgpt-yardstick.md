# What k8sgpt does well — a yardstick for measuring ourselves

**Date:** 2026-08-18 · **Subjects:** k8sgpt @ `b4a86de` (0.4.36 era) ·
k8s-lookout `v0.22.0-dev`

Companion to [the executive summary](2026-08-17-k8sgpt-assessment-summary.md) and
[the gap list](2026-08-18-gap-list-and-decisions.md). Those two documents ask
*"what do they detect that we don't"* and *"what should we build."* This one asks
a different question: **what practices does k8sgpt have that we should be judged
against, independent of feature count?**

Detection gaps close in a week (gap list §1). These don't. Most of them are
project-health and product-surface practices that compound, and several of them
are the reason k8sgpt is in the CNCF Sandbox and we are not.

**This is a yardstick, not a self-flagellation.** §14 lists the six axes where we
already beat them, verified the same way. Read the whole thing before quoting any
row of it.

---

## How to read the scorecard

Each practice gets: what k8sgpt does (with `file:line`), where we stand today
(verified 2026-08-18), and a **target** — the measurable bar, not a vague
aspiration. Targets are deliberately modest; this is a measuring stick, not a
roadmap. Where a target is genuinely not worth hitting, it says so.

| # | Practice | Them | Us | Priority |
|---|---|---|---|---|
| 1 | Zero-arg time-to-value | ✅ | ❌ | **High** |
| 2 | Distribution breadth | ✅ | ⚠️ | **High** |
| 3 | Contributor onramp (analyzer = 1 struct + 1 map line) | ✅ | ❌ | **High** |
| 4 | External bug-fix velocity as real-cluster exposure | ✅ | ❌ | Structural |
| 5 | Findings as Prometheus series | ✅ | ❌ | **High** |
| 6 | False-positive suppression as an explicit practice | ✅ | ⚠️ | **High** |
| 7 | In-cluster opt-out (annotations/labels) | ✅ | ❌ | Medium |
| 8 | Rule engine separated from the LLM | ✅ | ✅✅ | Done, better |
| 9 | Extensibility without forking | ✅ | ❌ | Medium |
| 10 | Governance artifacts | ✅ | ❌ | Medium |
| 11 | Branch protection as code | ✅ | ❌ | Low, cheap |
| 12 | Public roadmap | ✅ | ⚠️ | Low, cheap |
| 13 | Candid self-assessment | ✅ | ⚠️ | Medium |
| 14 | Release hygiene (SBOM, pinning, cadence) | ✅ | ⚠️ | Medium |
| 15 | Unit-test discipline on the detection layer | ✅ | ? | Medium |

---

## 1. Zero-argument time-to-value

**Them.** `k8sgpt analyze` with no arguments runs the 14 analyzers in
`coreAnalyzerMap` (`pkg/analyzer/analyzer.go:34-48`) across every namespace. No
config file, no target, no flags. `brew install k8sgpt && k8sgpt analyze` is
under a minute from cold to a list of what is broken.

**Us.** No run-everything entry point. Five commands — including three we lead
with in the README — hard-error without a named workload. A first-time user has
to already know which thing is broken before we will tell them what is wrong
with it.

**Target.** `lookout scan` with no arguments returns findings on an unfamiliar
cluster in under 60s. Designed in [gap list §2](2026-08-18-gap-list-and-decisions.md);
this row is the reason that section exists. **This is the single highest-value
item in this document** — every other row is downstream of a user getting a
result on their first command.

**RECOMMENDATION.** Ship `lookout scan`. Then measure literally: time from
`docker pull` to first finding, on a cluster the operator has never seen.

## 2. Distribution breadth

**Them.** Verified in `README.md`: Homebrew core *and* a tap (`:47-57`),
RPM (`:61-76`), DEB (`:82`), APK (`:109`), a Helm chart in-repo
(`charts/k8sgpt/Chart.yaml` — 8 templates including `serviceMonitor.yaml`), a
separate `k8sgpt-operator` repo for in-cluster install (`:154-156`), multi-arch
images, and 19 AI backends so the tool works with whatever model you already pay
for.

*Correction to an earlier draft:* k8sgpt does **not** ship a krew plugin. There
is no krew manifest in the repo and no `kubectl gpt` install path in the README.
Do not cite one.

**Us.** GHCR images (both flavors, multi-arch, distroless) + release tarballs +
`go install`. No Helm chart, no package-manager path, no in-cluster operator.
Our README's install section is a `gh release download` and a `tar -xzf`.

**Target.** A Helm chart in-repo covering the sentinel Deployment we already
ship in `deploy/`, with a `serviceMonitor.yaml`. That is a mechanical conversion
of existing manifests and closes most of the gap. Homebrew is a tap away and
GoReleaser can publish it from the release we already build.

**RECOMMENDATION.** Helm chart first — it is the one that unblocks anyone
evaluating the sentinel. Skip RPM/DEB/APK; the audience installs containers.

## 3. Contributor onramp

**Them.** Adding an analyzer is: implement one method —
`Analyze(analysis Analyzer) ([]Result, error)` (`pkg/common/types.go:35-37`) —
and add one line to a map (`pkg/analyzer/analyzer.go:34-68`). That is the entire
contract. Two files touched, no registration ceremony, no cross-cutting
knowledge required.

The result is measurable: **144 distinct author emails, 1,450 commits, 116
releases.**

**Us.** 165 commits by one author. Our `checks.Command` registry is genuinely
good — 31–32 commands generate 31–32 MCP tools from one declaration — but a new
check must understand the emit contract, the finding-kind taxonomy, the
sanitizer, severity assignment and fingerprinting before it can be written.
Richer output is not free; it raises the floor for a contributor.

**Target.** A `docs/adding-a-check.md` that takes a competent Go developer from
zero to a merged trivial check, and one worked example in the tree. Then measure
the honest metric: **can someone outside the team land a check without a
synchronous conversation?**

**RECOMMENDATION.** Write the doc. Do not restructure the registry to match
theirs — our richer contract is a deliberate trade and it is the right one.

## 4. External bug-fix velocity as real-cluster exposure

**Them.** Over 12 months, `pkg/analyzer/` took **+2,201/−81 across 33 files from
12 external contributors** — and the character of those changes is the point.
Recent examples: #1728 (panic on an Ingress path with a resource backend), #1725
(false failure report for jobs that succeeded after retries), #1737 (Gateway
conditions inspected by type), #1716, #1722, #1599, #1474, #1705. That is roughly
**one nil-dereference or false-positive per month**, each found by a real cluster
that a maintainer did not have.

**Us.** No external contributors. This is not a practice we can adopt — it is a
consequence of having users.

**Target.** None, honestly. But the *inference* matters and belongs in every
planning conversation: **k8sgpt's external bug rate is the best available
estimate of the bug backlog we have not found yet.** Our detection surface is
comparable in size and has had a fraction of the cluster-hours. Assume a similar
density of latent panics and false positives in `pkg/checks/`.

**RECOMMENDATION.** Treat this as a prior, not a task. Two concrete responses:
fuzz or table-test the field-traversal paths that mirror the ones they keep
patching (resource backends, optional status conditions, retry-completed jobs);
and get the tool onto clusters we don't own sooner than feels comfortable.

## 5. Findings as Prometheus series

**Them.** Every analyzer emits `analyzer_errors{analyzer_name, object_name,
namespace}` (`pkg/analyzer/analyzer.go:28-32`) and — the part that makes it
correct rather than merely present — clears stale series with
`DeletePartialMatch` at the top of each run (10+ call sites:
`deployment.go:45`, `hpa.go:49`, `job.go:42`, `storage.go:30`, `rs.go:30`,
`pdb.go:40`, `netpol.go:40`, `daemonset.go:28`, `validating_webhook.go:41`,
`clustercatalog.go:34`, …).

This is a small amount of code that buys an enormous amount of product for free:
timestamps, retention, deduplication, `increase()` over a window, `absent()` for
"the thing stopped being broken", `for:` durations in alert rules, federation,
and Grafana. They did not build any of that.

**Us.** `internal/watch/metrics.go` has **40+ metrics** — `lookout_events_seen_total`,
`lookout_storms_active`, `lookout_recoveries_observed_total`,
`lookout_watchboard_reattached_total`, and so on. It is a far richer registry than
theirs. But every one of them is *operational telemetry about the sentinel*, and
grep confirms **zero findings-shaped series**: no `lookout_findings{kind,
namespace, severity}` gauge anywhere in `internal/` or `pkg/`. The handler is
registered at `internal/watch/metrics.go:356` and only there — **the CLI exports
no metrics at all.**

So we can tell you how many events the sentinel deduped, and we cannot tell you
what is currently wrong with the cluster.

**Target.** A `lookout_findings{kind, namespace, severity}` gauge in the sentinel,
with stale-series clearing on each evaluation — we already have the right key for
it in `findings.SubjectKey`, and our findings carry severity and stable
fingerprints, which theirs do not. Their gauge has no severity label; ours can.

**RECOMMENDATION.** Do this. It is small, it is squarely in an area where our
data model is *better* than theirs, and it converts the sentinel from a thing
that emits to a sink into a thing that is queryable. Consider a `--metrics-addr`
on `lookout scan` too, for cron-style scrapes.

## 6. False-positive suppression as an explicit practice

**Them.** Suppression is treated as first-class analyzer work, not cleanup.
Verified examples: HPA does not flag `ScalingLimited` when the deployment is
legitimately sitting at `minReplicas` (`hpa.go:74-79`); Ingress exempts the
`gce` and `gce-internal` classes from a check that does not apply to them
(`ingress.go:192-194`); the validating-webhook analyzer defers to the service
analyzer rather than double-reporting the same broken backend
(`validating_webhook.go:84-87`); the ConfigMap usage check knows four sidecar
loader label conventions — `grafana_dashboard`, `grafana_datasource`,
`prometheus_rule`, `fluentd_config` — that mean "referenced dynamically, not in
a Pod spec" (`configmap.go:157-172`).

Every one of those is a bug report someone filed, turned into a permanent rule.

**Us.** We have exemptions, but as a *user-supplied YAML file* — the burden is on
the operator to discover the false positive and then silence it. We have very
little of the "we already know this pattern is fine" logic baked in.
`stab drift`'s missing majority-manager threshold
(`pkg/checks/stab/drift.go:158-166`, fix-list item 6) is exactly the class of
defect this practice prevents.

**Target.** Before shipping any new check, name the three most likely legitimate
configurations that would trip it, and encode the exemptions. Adopt their
double-reporting discipline: when two checks can flag the same root cause, one
defers.

**RECOMMENDATION.** Adopt the practice, and make it a review question. Cheap,
and it is the difference between a tool people trust and a tool people mute.

## 7. In-cluster opt-out

**Them.** Users silence a finding from inside the cluster, on the object itself:
`k8sgpt.ai/skip-usage-check: "true"` as an annotation (`configmap.go:180-182`)
and `k8sgpt.ai/dynamically-loaded: "true"` as a label (`configmap.go:172-175`).
The suppression lives with the workload, travels with it through GitOps, and is
owned by the team that owns the object — not by whoever holds the scanner's
config file.

**Us.** A YAML exemption file supplied to the CLI. No annotation path. On a
multi-team cluster, that centralises suppression in the wrong place: the platform
team ends up curating exemptions for workloads they don't own.

**Target.** Support `lookout.dev/skip: "<kind>"` (or similar) on objects,
honoured by every check, and surface it in output as a suppressed finding rather
than silence — we already have a `suppressed` state in `pkg/findings` diff, so
the vocabulary exists.

**RECOMMENDATION.** Worth doing, after `lookout scan`. Note the security caveat
and design for it: an in-cluster annotation is a suppression mechanism writable
by anyone with edit on the namespace. Emitting suppressed findings rather than
dropping them keeps it auditable.

## 8. Rule engine cleanly separated from the LLM

**Them.** All 30 analyzers run and produce structured `Result`s with no model
involved; `--explain` gates the entire AI path. The deterministic layer is
independently useful and independently testable.

**Us.** Better — we have no LLM at all. Every finding is deterministic
arithmetic, which is the property that makes lookout safe to put in an agent's
tool loop.

**Target.** Hold the line. See [gap list §5](2026-08-18-gap-list-and-decisions.md)
for the decision on whether to add an optional LLM narration layer for standalone
users; the recommendation there is to keep the core clean regardless.

**RECOMMENDATION.** No action. Listed because it is worth knowing we're ahead
here, and worth *staying* ahead when the pressure to add narration arrives.

## 9. Extensibility without forking

**Them.** `k8sgpt custom-analyzer add/remove/list`
(`cmd/customAnalyzer/customAnalyzer.go:24-42`), backed by a `custom_analyzers`
config key (`pkg/server/config/config.go:29`) and a gRPC contract
(`pkg/custom/client.go`). A user can add a detection to their own deployment
without touching the codebase or waiting for a release.

**Us.** Nothing. Every check is compiled in.

**Target.** Not a near-term build — but the shape matters. Our `checks.Command`
registry is already the right abstraction to expose, and our MCP surface is
already generated from it.

**RECOMMENDATION.** Defer, but do not architect it away. When we need it, the
cheapest version is a declarative check (CEL over a resource, emitting a standard
finding) rather than a gRPC plugin protocol.

## 10. Governance artifacts

**Them.** `ADOPTERS.md`, `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`,
`GOVERNANCE.md`, `MAINTAINERS.md`, `SECURITY.md`, `RELEASE.md`, `ROADMAP.md`,
`SECURITY_SELF_ASSESSMENT.md`, `GENERAL_TECHNICAL_REVIEW.md` — all at the repo
root. `.github/` holds `CODEOWNERS`, `ISSUE_TEMPLATE/`,
`pull_request_template.md`, `settings.yml`. GOVERNANCE.md is 108 real lines:
consensus-seeking, 48h + simple majority for votes, 50% quorum, project lead
breaks ties, 2/3 supermajority for governance changes, explicit vendor-neutrality.

**Us.** Root holds `AGENTS.md`, `CHANGELOG.md`, `CLAUDE.md`, `LICENSE`,
`README.md`. `.github/` contains **only** `workflows`. No CONTRIBUTING, no
SECURITY, no CODEOWNERS, no issue or PR templates.

**Target.** Three files, in order of return: `CONTRIBUTING.md` (how to build,
test, and add a check — pairs with row 3), `SECURITY.md` (where to report a
vulnerability in a tool that reads Secrets), `CODEOWNERS`. The rest are
CNCF-application artifacts and can wait until there is an application.

**RECOMMENDATION.** Do those three. SECURITY.md is arguably not optional for a
tool whose ClusterRole includes `secrets: list`. Skip ADOPTERS.md until there are
adopters — and see the summary's note on why citing *their* empty one is a
mistake.

## 11. Branch protection as code

**Them.** `.github/settings.yml:24-46` — required reviews with
`require_code_owner_reviews` and `dismiss_stale_reviews`,
`required_conversation_resolution`, DCO as a required status check, **`enforce_admins:
true`**, `required_linear_history: true`, team-to-permission mapping declared in
the same file.

The value is not the specific policy — it is that the policy is reviewable,
diffable, and applies to maintainers too.

**Us.** Nothing declared. Whatever protection exists is clicked into the GitHub
UI and invisible to anyone reading the repo.

**Target.** A `.github/settings.yml`. An hour of work.

**RECOMMENDATION.** Do it. Cheapest row in the document. `enforce_admins: true`
is the part worth copying deliberately.

## 12. Public roadmap

**Them.** `ROADMAP.md` with a vision, five current focus areas, and explicit
Near Term (3–6mo) / Medium Term (6–12mo) / Long Term (12mo+) buckets, plus a
"How to Contribute" section that ties the roadmap to open issues.

**Us.** `docs/roadmap-post-m5-sensors.md` — real content, but internally framed
and not discoverable as *the* roadmap.

**Target.** Promote it to `ROADMAP.md` at root with the same three time buckets.
Mostly a rewrite of framing.

**RECOMMENDATION.** Do it when there is an external audience. Low value while
the repo has one contributor, but near-zero cost.

## 13. Candid self-assessment

**Them.** `SECURITY_SELF_ASSESSMENT.md` and `GENERAL_TECHNICAL_REVIEW.md` name
their own gaps in public: anonymization is incomplete (with the issue number,
#560), API keys are stored in plaintext, there is no remediation capability,
tracing is declared but unimplemented. They also hold an OpenSSF Best Practices
badge (#7272).

Naming your own weaknesses before a reviewer finds them is a maturity signal, and
it is cheap to do and expensive to fake.

**Caveat, already established in Q1 and worth repeating so we don't over-admire
this:** several GTR claims are contradicted by the repo — it cites FOSSA scanning,
Kind/Minikube CI, and a Kubernetes version matrix, none of which are present in
the workflows. So the practice is good and the execution is partly aspirational.
Copy the practice; verify your own claims before publishing them.

**Us.** This assessment corpus is exactly this artifact — including
[the summary's §"A note on this document's own reliability"](2026-08-17-k8sgpt-assessment-summary.md),
which counts the eleven claims our own drafts got wrong in both directions. It is
internal. That is fine for now.

**Target.** When there is an external audience, publish a known-limitations
section. Every claim in it verified against the tree first — the GTR's failure
mode is the one to avoid.

**RECOMMENDATION.** No action now; the artifact already exists. Keep it honest.

## 14. Release hygiene

**Them.** ~Monthly releases (116 total), release-please + GoReleaser, SHA-pinned
GitHub Actions, a **Syft SBOM on every release**, a semantic-PR check, and
`only-new-issues: true` on the linter so a stricter config can be adopted without
a thousand-line cleanup PR.

**Us.** Mixed, and the mix is not what you'd guess:

- **We win on signing.** `.github/workflows/release-images.yml:79` declares
  `id-token: write   # cosign keyless via GitHub OIDC`; `:298-309` installs
  `sigstore/cosign-installer@v3` and runs `cosign sign --yes "{}@${DIGEST}"`;
  `:333` runs the documented `cosign verify` **in CI**, so the instructions in
  our README are continuously proven; `:370` sign-blobs the release archives.
  **k8sgpt has no cosign at all** — and its self-assessment's "Container Image
  Signing" line points at the Syft SBOM step, which is not signing. Verifying the
  published verification command in CI is a practice *they* should copy from us.
- **We lose on SBOM.** Grep for `syft|sbom|spdx|cyclonedx|attest|provenance` in
  `release-images.yml` returns **nothing**. No SBOM, no provenance attestation.
- Cadence and pinning: not audited here.

**Target.** Add SBOM generation to the release workflow — with cosign already
wired, `cosign attest` on the SBOM is a few lines and lands us ahead of them on
the whole supply-chain axis rather than split.

**RECOMMENDATION.** Add the SBOM. Small, and it removes the only supply-chain
row where we trail.

## 15. Unit-test discipline on the detection layer

**Them.** **87.4% coverage on `pkg/analyzer`** across 33 test files, table-driven
against fake clientsets, with several tests written explicitly as regression
guards for past panics. Given row 4 — an external bug a month — that test suite
is the mechanism that keeps fixes fixed.

**Us.** Not audited in this pass. We have `e2e-kind.yml` (real-cluster CI, which
they do not have) and `coverage.out` in the tree, but no measured figure for
`pkg/checks/` specifically.

**Target.** Measure `pkg/checks/` coverage; then treat 85% as the bar for the
detection layer specifically, and require a regression test for every
false-positive or panic fix.

**RECOMMENDATION.** Measure first — this row has a `?` in the scorecard and
should not stay that way. The regression-test rule is worth adopting regardless
of the number.

---

## Where we already beat them

Stated with the same evidence standard, so this document reads as a benchmark
rather than a confession:

1. **Sigstore keyless signing, with the verification command proven in CI**
   (`release-images.yml:79, :298-309, :333, :370`). They have none.
2. **Real-cluster CI** — `e2e-kind.yml`. Their GTR claims Kind/Minikube CI; the
   workflows do not have it.
3. **Continuous operation.** The sentinel is a category k8sgpt does not compete
   in and structurally cannot grow into.
4. **Always-on two-layer sanitization** with an entropy gate. Their anonymization
   is opt-in and self-declared incomplete (#560).
5. **Severity, stable fingerprints, and a finding-diff state machine**
   (new/ongoing/escalated/resolved/suppressed). Their `Result` has no severity
   and no identity across runs — which is precisely why their Prometheus gauge in
   row 5 has to be labelled by object name.
6. **A published documentation site with an agent-oriented entry point** —
   `go-steer.github.io/k8s-lookout`, including `llms.txt` / `llms-full.txt` and a
   dedicated agent guide, built and linted by three separate workflows
   (`docs.yml`, `ci-docs.yml`, `docs-lint.yml`).

## The short version

Ranked by return, ignoring effort:

1. **`lookout scan`** (row 1) — nothing else matters until a first command
   returns something.
2. **Findings as Prometheus series** (row 5) — small, and our data model is
   better than theirs here.
3. **False-positive suppression as a review practice** (row 6) — free, and it is
   what separates a trusted tool from a muted one.
4. **Helm chart** (row 2) — mechanical, unblocks sentinel evaluation.
5. **CONTRIBUTING + SECURITY + CODEOWNERS + `settings.yml`** (rows 10, 11) — an
   afternoon, together.
6. **SBOM** (row 14) — completes a supply-chain story we otherwise lead.
7. **Measure `pkg/checks/` coverage** (row 15) — turn the `?` into a number.

Rows 3, 7, 9, 12 are worth doing and are not urgent. Row 4 is not a task; it is
a prior about how many bugs we have not found. Rows 8 and 13 need no action.
