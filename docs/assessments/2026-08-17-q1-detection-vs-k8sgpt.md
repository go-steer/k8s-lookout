# Q1 — Detection and analysis: k8s-lookout vs k8sgpt

Part 1 of 3. ← [Executive summary](2026-08-17-k8sgpt-assessment-summary.md) ·
[Q2](2026-08-17-q2-core-agent-lookout.md) · [Q3](2026-08-17-q3-mast-lookout.md)

**Question:** how does k8s-lookout compare to k8sgpt on assessment, analysis and
problem detection — in both CLI/MCP mode and sentinel (continuous watch) mode?

**Status:** complete. Drafted from three code-level inventories, then revised
twice under adversarial review — a fact-checker with no brief, and a red team
briefed to argue k8sgpt is stronger. Every verdict-level attack was independently
re-verified against the source before being accepted; one was rejected on
re-verification (see 1.7).

**Subjects:** k8sgpt @ `b4a86de` (0.4.36 era) · k8s-lookout `v0.22.0-dev`
(latest release 0.21.0, 2026-08-16).

**Reading note:** this document deliberately preserves its own corrections rather
than silently fixing them. Several first-draft claims were wrong in our own
favour, and the pattern of *how* they were wrong is more useful than the corrected
numbers.

---

### 1.1 The honest headline

These are not the same kind of tool, and most of the interesting differences are
architectural rather than a matter of check count.

**k8sgpt is a breadth-first snapshot rule engine with an LLM narration layer
bolted on.** Its differentiator is that it *explains* findings in natural
language, across 19 model backends, with zero setup.

**k8s-lookout is a deterministic instrument with no LLM at all**, designed so that
the intelligence lives in a consuming agent. Its differentiator is the
continuously-running sentinel and the machine-consumption contract.

The two projects disagree about what the product *is*, so the comparison that
matters is: for an agent-driven SRE system, which substrate is better?

The answer this document arrives at, after two rounds of adversarial review, is
that **lookout has the better architecture and the worse product** — and that on
the straight "who detects more" question we lose, which is the opposite of what
the first draft claimed.

### 1.2 Detection breadth — we are behind, not ahead

This section has been rewritten twice. Both earlier versions were wrong in our
own favour, and the correction matters more than the original claim.

| | k8sgpt | k8s-lookout (CLI) |
|---|---|---|
| Units | 30 analyzers (+4 integration) | 31 commands |
| Raw emit sites / declared kinds | **95** emit sites (84 analyzer + 11 integration) | **113** declared kinds |
| …after removing non-detections | **~90–95** (no info tier to remove) | **~70** |
| …available without GCP | **~90–95** | **~48** |
| Severity on findings | **none** | 3 levels (info/warning/critical) |
| Stable finding ID | **none** | fingerprint + subject key |
| Timestamp on a finding | **none** | **none** (summary carries `elapsed=`, a duration) |

**The headline number in the first two drafts was rigged on both sides.** An
early draft said "~60–70 k8sgpt vs ~105 lookout." The first correction fixed the
k8sgpt undercount but left lookout's total intact and concluded "roughly at
parity." Counted consistently, that is still wrong.

*Deflating our 113.* Roughly **33 of the declared kinds are not detections of a
cluster problem**: `inventory.object` (existence only, "deliberately no
judgment"), `health.category` (one line per category, emitted for healthy ones
too), `bundle.target` and `spec.resource`/`spec.container` (renderers),
`event.normal`, `cloud.unavailable`, `perf.pack_unavailable`,
`log.fetch_error`/`overflow`/`probe_noise` (tool self-reports),
`triage.status`/`findings.ack`/`findings.transition` (read-backs of our own
SQLite), `radius.neighbor`, `top.unlimited`/`unrequested`, the seven
`change.*` info kinds, `audit.exemption_expired`/`_expiring` (housekeeping on our
own YAML), `drain.node`, `demo.finding`. Real detections: **~70**.

*k8sgpt has nothing equivalent to deflate.* Every `common.Failure` is a claimed
problem — no info tier, no inventory output, no self-reports. Recounted at the
same family granularity as ours it is **~90**; `grep -h 'Text:' pkg/analyzer/*.go`
gives 84 and the integrations another 11. Two integrations are *unbounded*:
Kyverno surfaces every failing policy, EKS whatever AWS reports.

**So on a consistent basis k8sgpt detects more distinct problem conditions than
we do, and roughly twice as many that a non-GCP user can actually reach.** We
should stop making breadth claims.

**The GCP gate is worse than "we are GCP-only."** The *default published image
ships without the GKE provider* — `ghcr.io/go-steer/lookout:latest` is
"GCP-free"; the provider is behind `-tags allproviders` in a separate `-gke` tag
(`README.md:51-52`, `//go:build gke || allproviders` throughout `pkg/cloud/gke/`).
Gated: 5 `cloud.*` + 3 `audit cluster` + 4 `audit upgrades` + 2 `state wi` + all
7 `perf.*` (which additionally require an opt-in, extra-cost GKE metrics feature),
plus partials in `stab drift --identity`, `triage top --history`, and health's
`control-plane` category. That is **~22 kinds — a third of our real detections —
and it is exactly the set this section used to lead with.**

**The "runs by default" comparison is inverted from what we claimed.** There is
no `lookout analyze`: the registry has `All()`, `Groups()`, `TopLevel()` and no
run-everything entry point. Our zero-argument answer is `lookout health` — ten
fixed categories, one of them GCP-gated, so nine for a non-GKE user. And five
commands, including the three this document leans on as depth differentiators,
**hard-error without a target the user must already have identified**:
`state edges` (`pkg/checks/state/edges.go:151`), `bundle`
(`bundle/bundle.go:356`), `triage radius`/`triage` (`triage/triage.go:133`),
`triage events` (`events/events.go:128`), `spec` (`checks/spec.go:299`).

`k8sgpt analyze` with zero arguments runs 14 analyzers cluster-wide — including
exactly the referential-integrity class we gate behind `--workload`:

| Failure class | k8sgpt, zero args | lookout, zero args |
|---|---|---|
| Ingress backend Service / TLS Secret missing | default (`ingress.go:55-159`) | needs `--workload` |
| Service selects nothing / endpoints not ready | default (`service.go:59-131`) | needs `--workload` |
| StatefulSet governing Service / StorageClass missing | default (`statefulset.go:55-131`) | not covered |
| ConfigMap unreferenced / empty / >1MB | opt-in (`configmap.go:41-150`) | not covered |
| Webhook → Service → Pod traversal | **default** (`validating_webhook.go:54-137`) | separate invocation |

The honest framing: *one command, no arguments, 14 analyzers, cross-resource,
cluster-wide* versus *one command, no arguments, a nine-to-ten-category
scorecard, plus twenty-odd further invocations, five of which require you to
already know which workload is broken.* k8sgpt's `--filter` default gap is real
but it is a much smaller problem than ours.

**Prescription:** this section diagnoses the reachability loss and stops. The
design for the missing entry point — `lookout scan`, whose second stage fans
`state edges` out automatically onto whatever stage one flagged, so the caller
never has to name the broken workload — is in
[the 2026-08-18 follow-up §2](2026-08-18-gap-list-and-decisions.md#2-lookout-scan--the-one-shot-entry-point).

**Coverage k8sgpt has that we do not** — the two lists below are summaries. The
itemised, grep-verified gap table (twelve conditions, ranked, with the six worth
fixing called out) is in
[the 2026-08-18 follow-up §1](2026-08-18-gap-list-and-decisions.md#1-what-k8sgpt-detects-that-lookout-does-not):
- **Gateway API v1 in the CLI** — GatewayClass, Gateway, and an HTTPRoute analyzer
  that validates `AllowedRoutes` namespace policy *and* backend port matching
  (`pkg/analyzer/httproute.go:55-214`). This is genuinely good. **Correction to an
  earlier draft: it is not true that "we have nothing."** `pkg/sources/gateway/`
  watches Gateway and HTTPRoute `Programmed`/`Accepted`/`ResolvedRefs` with a
  sustained-failure gate — but only in the **sentinel**. The gap is real and
  CLI-shaped, not total.
- **Operator lifecycle, both generations** — OLMv1 (ClusterCatalog,
  ClusterExtension) and OLMv0 (CSV, Subscription, InstallPlan, CatalogSource,
  OperatorGroup). Few tools cover OLMv1 at all.
- **Policy engines** — Kyverno PolicyReport/ClusterPolicyReport.
- **KEDA** ScaledObject validation.
- **Prometheus config validation** — parses and validates scrape configs.
- **AWS/EKS** cluster health issues. We are GCP-only.
- **19 LLM backends** including local/offline models.

**Coverage we have that k8sgpt does not:**
- **Cloud/GKE posture** — Workload Identity, upgrade governance, release channels,
  maintenance windows, legacy metadata, public control plane, stockouts, quota
  pressure, IP-space exhaustion, orphaned disks/LBs.
- **Control-plane performance packs** — apiserver p99, APF saturation/rejects,
  etcd fsync and DB size, pod startup p95.
- **Log understanding** — a hand-rolled Drain implementation plus a
  per-stream stack-trace state machine (Go/Java/Python). k8sgpt's log analyzer is
  `regexp.MustCompile("(error|exception|fail)")` over 100 lines, returning **the
  first matching line only** (`pkg/analyzer/log.go:28-30,138-149`) — it fires on
  "failover" and on `error_rate: 0`.
- **Blast radius** over a typed topology graph.
- **Active network probing** — DNS/TCP/HTTP from inside the cluster.
- **Drain blockers, GitOps drift, volume multi-attach/zone conflicts.**
- **Run-to-run finding transitions** (`findings diff`) and exemptions.
- **Continuous operation** — see 1.4.

### 1.3 Detection depth — we are less far ahead than our docs imply

This is the finding most likely to be uncomfortable, and it should survive into
any external positioning.

**Both tools are majority single-object field assertions.** Our
`pod.crashloop` is literally `waiting.Reason == "CrashLoopBackOff"`
(`pkg/checks/delta/pods.go:157`). Their Deployment analyzer is the single
comparison `spec.replicas != status.readyReplicas`
(`pkg/analyzer/deployment.go:57`). Ours is better instrumented, not categorically
deeper, across most of the surface.

Where we are genuinely deeper:
- **`pkg/graph` is a real typed topology graph** — 18 node kinds, 7 edge kinds,
  and critically `Ref.Observed`, making referenced-but-absent objects first-class
  nodes. That is what makes `radius.missing` possible.
- **`state edges` names the specific broken hop** — workload → volume/env ref →
  ConfigMap/Secret *key*, Service selector → Pods → EndpointSlice targetRefs.
- **One-List-pass loading with SSAR preflight** and graceful partial-RBAC
  degradation, rather than failure.

**But the graph is a drill-down advantage, not a detection advantage.** Only 5 of
31 commands import `pkg/graph`, and of those, `state edges`, `bundle` and
`triage radius` all require a pre-named target; `triage events` and
`triage changes` use it for owner-tree scoping and neighbourhood rendering. There
is **zero cluster-wide graph-based detection**. You must already suspect the
workload before the graph does anything for you.

k8sgpt's HTTPRoute analyzer (`httproute.go:55-214`) is genuine three-object
reasoning — HTTPRoute → parent Gateway → listener `AllowedRoutes` → backend
Service → port presence — cluster-wide with one `--filter`, and the webhook
analyzers traverse webhook → Service → Pods → Running **by default**. That is
comparable in kind to `state edges` and strictly better in reachability.

Two things neither tool does: **no root-cause ranking, and no cross-check
correlation.** Nothing on our side joins `node.pressure` to `pod.oomkilled` on the
same node.

**And the aggregation jab in an earlier draft was backwards.** k8sgpt aggregates
per pod — `pod.go:93-99` collects every container failure into one `PreAnalysis`,
one `Result`, one LLM call. Our CLI does *not* aggregate: `checkContainer` runs
per init container and per container, calling `s.add(f)` each time
(`delta/pods.go:95-99`), so ten crashing two-container pods produce **twenty**
findings with no owner collapse. Storm correlation lives only in
`pkg/engine/storm.go` and is never reached by `triage delta`. On the run-once path
being compared, **k8sgpt collapses better than we do.** k8sgpt's real limitation
is different: it computes ownership via `util.GetParent` but uses it
presentationally only, so it never collapses across pods of one Deployment.

**Neither tool remediates.** Ours has no remediation field, no suggested fix, and
no confidence score on a finding (`pkg/emit/finding.go:51-93`). Theirs has zero
patch/apply code paths, declared an explicit non-goal.

### 1.4 Continuous operation — the decisive gap, in our favour

**k8sgpt has no continuous mode at all.** No ticker, informer, watch, or reconcile
anywhere in `pkg/` or `cmd/`. Every run is an independent snapshot with no state,
no dedup, no diffing, no history. Their findings carry no severity, no timestamp,
and no stable ID. History, trends, and multi-cluster all sit in the 6–24 month
buckets of a roadmap where **every checklist item is unchecked**.

**One important qualification, against our interest:** "nothing downstream can
track them" is too strong. `pkg/analyzer/analyzer.go:27-32` exports an
`analyzer_errors{analyzer_name, object_name, namespace}` gauge — a stable identity
tuple — and analyzers `DeletePartialMatch` on entry and repopulate, so series go
absent when a problem clears. Served on `--metrics-port 8081`, which is how the
operator runs it. **Prometheus then supplies timestamps, retention, dedup by label
set, `increase()`/`absent()` for recurrence and resolution, `for:` stability
windows, and federation — for free, on infrastructure that is already HA'd and
monitored.** That is a meaningful fraction of what our `recovery.go` + `dedup.go`
+ `storm.go` do, on a substrate far more operationally mature than a
single-replica pod with an `emptyDir`. What it cannot give them is
graph-keyed storm correlation and late causal attachment, which remain genuinely
ours.

The k8sgpt-operator lives in a separate repository that is **not checked out
here**, so we have not verified how it schedules. What we *can* verify is that it
drives this gRPC API, and that this core is stateless — so whatever the operator
does, it re-scans a tool with no memory. Do not assert "it is cron" externally
without reading that repo.

Sentinel does four things that a re-run scanner structurally cannot:

1. **Arm-after-cache-sync** (`pkg/sources/objectstate/objectstate.go:650-653`) —
   a cold start on an already-broken cluster does not page for everything. This is
   the property a scanner cannot fake — **but see 1.4.1: it is missing from the
   one source that opens every incident.**
2. **Recovery as a verified outcome** (`pkg/engine/recovery.go:265-380`) —
   `symptomatic → clearing → resolved` over a 5-minute stability window, flap
   resets the window, resolution lands on the *original* fingerprint. A scan says
   "green now"; only this says "closed, and it held".
3. **Graph-topology storm correlation** (`pkg/engine/storm.go:189-223`) — 200 pod
   symptoms collapse to one incident with three representatives, keyed on
   blast-radius ancestry.
4. **Late causal attachment** (`internal/watch/reattach.go:27-60`) — a symptom at
   T gets its cause attached at T+30s, at flush time. Impossible single-pass.

Plus forecasting (least-squares → ETA across four sources), 7-day recurrence
distillation, and a closed triage loop where an agent's verdict changes routing
within 30 seconds.

#### 1.4.1 The arm gate is missing from `k8s-events` — a real bug, found during review

Eight of nine informer-backed sources carry an `armed` flag that suppresses
emission until `WaitForCacheSync` returns: `objectstate`, `workload`, `capacity`,
`ingress`, `autoscaling`, `gateway`, `rollout`, `degradation`. **`k8s-events` does
not.** Its `AddFunc` calls `emit(toSignal(ev))` unconditionally
(`pkg/sources/k8sevents/k8sevents.go:92-99`), and the handler is registered at
line 91 — *before* `factory.Start` (133) and `WaitForCacheSync` (139). client-go
delivers the initial LIST as `Add` events, so **everything already in the API
server is emitted as a fresh signal on every start.** There is no downstream
age gate either (no `event-age`/`maxAge` anywhere in `internal/watch`,
`pkg/engine`, `pkg/inject`).

The `WaitForCacheSync` call at :139 is present, and its comment explains it is
there so events arrive with their prior `Count`/`LastTimestamp` for dedup — the
ordering was thought about, the arm gate simply was not added. This reads as an
oversight, not a decision.

`k8s-event` is the source our own frozen schema describes as "the opening inject
of a per-incident session," which makes this the worst source to have missed.

**Failure sequence on the shipped manifest** (`deploy/51-deployment-watcher.yaml`):
the pod restarts → the `emptyDir` at :158 is gone and `--dedup-persist` is not in
the args → the informer LISTs and the API server's default one-hour `--event-ttl`
window replays → the allow-list in `pkg/engine/filter.go` bounds this to
genuinely-broken objects, but the `Unhealthy` debounce reads the Event's
*accumulated* `Count`, so it offers no protection on replay → with an empty dedup
cache, **every replayed event opens a new incident.** Fifty crashlooping pods,
fifty re-pages. And they cannot be closed: `internal/watch/dispatch.go:823` logs
`"recovery: no bound session … dropping … (restart without --dedup-persist…)"`.

**Compounding it:** `replicas: 1` with no `strategy` block means the default
RollingUpdate, where `maxSurge: 25%` rounds *up* to 1 and `maxUnavailable: 25%`
rounds *down* to 0 — so every image bump briefly runs **two sentinels** with
independent dedup state and independent `emptyDir`s. The sink has one attempt and
no retries. And nothing detects any of it, because `/healthz` is an unconditional
200 wired to both probes.

This is the sharpest single finding in Q1 and it points at us. Section 1.4's
framing that "the gap is operational hardening, not capability" does not survive
it: a watcher that re-pages on restart, double-pages on upgrade, cannot close the
incidents it opened, and cannot be probed is not a capability with an ops gap
beside it.

**However, the sentinel's production-service story is weak**, and this must not be
oversold:
- `replicas: 1`, zero leader-election code — every restart is a coverage hole.
- **`/healthz` is a static 200** wired to both probes
  (`internal/watch/metrics.go:356-363`). A wedged informer, dead source, or
  failing sink still returns healthy. Kubernetes will never restart a sentinel
  that has stopped watching.
- **At-most-once sink delivery, no retries by design** (`pkg/inject/sink.go:78-80`).
  A 30-second daemon restart drops every critical in the window.
- The manifest **does** set `--store=/var/lib/lookout/lookout.db`
  (`deploy/51-deployment-watcher.yaml:114`) — but backs it with an `emptyDir`
  (PVC present but commented out), and sets no `--dedup-persist`. With no
  event-age gate, a restart produces a duplicate storm.
- **Multi-cluster mode rejects `--store` and `--dedup-persist`**
  (`internal/watch/flags.go:446,449`), so N-cluster loses the durable half: no
  triage overrides, no history, no distillation. This is a **loud startup error
  with an explanation**, and is documented at `docs/multi-cluster-design.md:231`
  — it is a known tradeoff, not a silent trap.
- The `new/ongoing/resolved` lifecycle is shipped **CLI-only**; `internal/watch`
  never imports `pkg/findings`.

### 1.5 Agent-facing design — our clearest structural win

| | k8sgpt | k8s-lookout |
|---|---|---|
| LLM in the tool | 19 backends, `GetCompletion` only | **none — deliberate** |
| Agentic loop | **none.** One prompt → one completion per finding | n/a (lives in consumer) |
| Tool/function calling | **zero constructs in tree** | n/a |
| Context sent to model | joined failure sentences only | n/a |
| MCP tools | 12 | 31, generated from one registry |
| Mutating MCP tools | filter tools rewrite user's YAML; `get-resource` returns **Secrets unredacted** | 3 of 31, all write only lookout's SQLite; **none touch the cluster** |
| MCP transport | **stdio by default** — no socket at all | loopback TCP |
| `serve` API security | gRPC on 8080 with **no TLS, no auth**, reflection on, full request bodies logged | n/a |
| Output contract | text/json, no key-order guarantee | identical key order, mandatory summary, glossary-enforced keys, frozen exit codes |
| Redaction | see below — **`--anonymize` only ever affects the LLM prompt** | two-layer sanitizer, always on, entropy-gated |

k8sgpt's AI layer is the weakest part of their system and the part their
marketing leans on hardest. The prompt asks for a fix "in no more than 280
characters" and sends the model *only* joined failure sentences — no YAML, no
resource identity, no owner chain, no other findings. `amazonsagemaker.go:88`
ships the literal unresolved string `"DEFAULT_PROMPT"` as its system message.
The cache key omits the model (acknowledged TODO), so switching models silently
reuses stale answers.

**On `--anonymize`, the accurate criticism is sharper than "it's incomplete."**
It only ever affects the LLM prompt: it requires `--explain`, mutates a
loop-local copy, and *un-masks* the answer on the way back
(`pkg/analysis/analysis.go:546-552`). **All output is unmasked in every mode.**
31 of 74 emit sites pass an empty `Sensitive` slice, event messages and log lines
are never masked at all, and `-o json` serialises the `Failure`/`Sensitive`
structs — which carry no json tags whatsoever — so the unmasked values ship in
the JSON output. The flag protects the model provider, not the operator's
terminal or their log pipeline.

**Correction to an earlier draft on transport:** it claimed k8sgpt's MCP server
listens on 8089 without TLS. It does not. `--mcp` and `--mcp-http` both default
to **false** (`cmd/serve/serve.go:285-287`), and `pkg/server/mcp.go:95` builds a
`NewStdioServer` — **no network socket**, which is strictly safer than our
loopback TCP listener. Port 8089 exists only under an explicit `--mcp-http`. The
plaintext, unauthenticated, reflection-enabled criticism is valid and stands, but
it belongs to `k8sgpt serve`'s gRPC API on 8080, not to MCP.

The unredacted-Secret finding in `get-resource` is correct. But note the
symmetry before using it externally: **our shipped sentinel ClusterRole grants
`list` on Secrets cluster-wide**, which returns values, and the manifest's own
comment concedes it. Their exposure requires a human to opt in with their own
kubeconfig; ours is a resident pod, 24/7, by default.

Our "one metadata declaration generates CLI help, MCP JSON schema, and skill
reference docs, with CI drift tests" is the best structural idea in either repo.

### 1.6 Maturity — where we are behind, and it is not close

The gap is not "three years versus four weeks." It is **144 contributors versus
one person.**

| | k8sgpt | k8s-lookout |
|---|---|---|
| Age | ~3 years, CNCF Sandbox | **~4 weeks** (first commit 2026-07-24) |
| Commits / authors | 1450 commits, **144 distinct author emails** | 165 commits, **1 author** |
| Releases | **116 tags**, 0.4.28→0.4.36 ~monthly | 22 tags in 4 weeks — churn on a 0.x line, not maturity |
| Distribution | krew (`kubectl gpt`), Helm chart, separate operator repo, 19 backends | one GHCR repo + release tarballs |
| Governance | 7 maintainers, GOVERNANCE.md, DCO, CODEOWNERS | single-team, **bus factor 1** |
| Live-cluster CI | **none** | **kind e2e** — post-merge smoke + weekly full, 10 scenarios |
| envtest | none | none |
| Test coverage | 87% `pkg/analyzer`, 16.8% `pkg/server`, 0% for KEDA/custom/interactive — **as reported by their tooling; not independently re-derived** | strong contract/golden tests |

Two corrections to earlier drafts, both against the direction the draft was
leaning:

- **"Live-cluster CI: none | none" understated us.** `.github/workflows/e2e-kind.yml`
  runs a post-merge smoke tier and a weekly full tier over ten scenarios in
  `examples/scenarios/`, building from HEAD, with forensics upload and auto-filed
  tracking issues. It is kind, not a cloud cluster — cloud paths remain
  fixture-only — but it is not nothing, and k8sgpt has no equivalent.
- **"ADOPTERS.md lists zero" is not evidence of no adoption** and should be struck.
  `git log --diff-filter=A` shows it was added on **2026-04-24** in
  `docs: prepare governance docs for CNCF incubation (#1642)`, and its own text
  says "For incubation, the TOC requires at least 3 independent adopters." It is a
  four-month-old application template, not a register that came up empty. Citing
  it as an adoption metric would discredit the rest of this document to any reader
  who checks.

**Their low velocity is not the mitigation it looks like.** "One new analyzer in
six months" is true and misleading. `git log --since='12 months ago' --numstat`
over `pkg/analyzer/` gives **+2201/−81 across 33 files from 12 external
contributors** — and what those commits *are* is the point: panic on an Ingress
resource backend (#1728); false failures on retried Jobs (#1725); Gateway
conditions read by index instead of by type (#1737); an HPA `ScalingLimited` false
positive at minReplicas (#1716); init containers missed in ConfigMap usage
(#1722); GKE-specific ingress classes (#1599); SchedulingGated pods missed
(#1474); a concurrent map write (#1705); nil dereferences in Service TargetRef,
Deployment replicas, and HTTPRoute port.

A three-year-old project with 144 contributors still finds roughly one
nil-deref-or-false-positive per month across ~30 analyzers. **That is the best
available estimate of the bug backlog we have not found yet** — ours is not
absent, it is undiscovered, because we have one author and no external users
running us against clusters we did not imagine. Two candidates found by
inspection during this review are in 1.7.

Their own `SECURITY_SELF_ASSESSMENT.md` is marked **"Incomplete"**, and their
technical review makes claims the repo contradicts (FOSSA scanning that was
removed; "CI tests against multiple Kubernetes versions" when the only test
workflow is `go test ./...` on one runner, one Go version).

But we cannot claim maturity. Four weeks, one author, no envtest, cloud paths
validated only against recorded JSON fixtures, and self-declared unautomated
read-path UAT.

### 1.7 Things we should fix, ordered by how much they undercut the pitch

Genuine defects:

1. **`k8s-events` has no arm gate** (1.4.1) — the one source that opens every
   incident replays the API server's whole event TTL window as new incidents on
   every restart, and with an `emptyDir` store cannot then close them. Eight of
   nine sibling sources already have the gate; this is a one-source oversight with
   an outsized blast radius. **Fix this first.**
2. **`/healthz` static 200** — the sentinel cannot be monitored. Cheap to fix,
   embarrassing if found externally.
3. **No leader election / `replicas: 1`, and no `strategy` block** — blocks any
   "continuously watching production" claim, and the default RollingUpdate runs
   two uncoordinated sentinels during every image bump.
4. **Gateway API coverage in the CLI** — the sentinel watches it; `lookout` the
   command-line tool does not, and k8sgpt beats us there outright.
5. **`stab drift` can fire critical on well-managed clusters.** With `--manager`
   unset the GitOps manager auto-resolves to whichever manager owns the most spec
   leaf fields, ties broken lexicographically, with **no minimum-share threshold**
   (`pkg/checks/stab/drift.go:158-166`). On a Helm-platform + Argo-apps cluster,
   whichever wins makes every object of the other one `drift.manual_edit` —
   escalated to critical on any image/replicas/env leaf. One `kubectl rollout
   restart` leaves a permanent `kubectl-rollout` managedFields entry. Not
   catchable by golden files, which encode the author's model of the cluster.

Neither of these last two was found by a test. They were found by reading the
code during this review, which is the point of 1.6.

*A third candidate did not survive checking.* It was proposed that
`edge.selector_unready` fires critical on a Deployment scaled to zero
(`state/edges_checks.go:527-534`). It does not: with zero replicas nothing is
selected, so `readyPods < len(c.selected)` is `0 < 0`, false, and no finding is
emitted. The narrower real exposure is a transient critical during a
single-replica rolling update, while the only pod is not yet ready.

Known, documented tradeoffs — worth revisiting, but they are decisions with
written rationale in-repo, not things we failed to notice. Presenting them as
discovered defects would misrepresent our own engineering:

4. **Sentinel `secrets: ["list"]`** — returns Secret *contents* cluster-wide.
   Read-only, but a compromised sentinel pod reads every Secret. Carries a
   30-line `§11 TRADEOFF` block in-repo.
5. **`emptyDir` under `--store`, no `--dedup-persist`** — the manifest ships a
   commented-out PVC alternative. The default discards exactly the state the
   design story depends on.
6. **At-most-once sink delivery** — explicitly "no retries, by design"
   (`pkg/inject/sink.go:78-80`); redundancy comes from dedup-cooldown re-fire.
   Still the one place where "we can lose findings" is true.
7. **Multi-cluster disables persistence** — a loud startup error, documented in
   the design doc. Surprising, but not a trap.

Deleted from an earlier draft: a claim that posture findings carry no
fingerprint. **False** — every `audit` subcommand stamps
`engine.PostureFingerprint` (`pkg/engine/fingerprint.go:138`), landed
2026-08-14. `audit *` *can* participate in fleet rollup.

### 1.8 Q1 verdict

*This verdict is narrower than the one in the first draft. The earlier version
rested on a breadth claim and a continuous-operation claim that did not survive
adversarial review.*

**Where we genuinely win, and it is not close:** k8sgpt has no continuous mode
anywhere in its tree — no informer, ticker, watch or reconcile — so recovery as a
verified outcome over a stability window, graph-keyed storm correlation, and late
causal reattachment at flush time are things it structurally cannot do. The
machine-consumption contract (one metadata declaration generating CLI help, MCP
schema and docs, with CI drift tests; frozen exit codes; guaranteed key order;
always-on sanitization) is the best structural idea in either repository, and
their AI layer — 280-character prompts fed only joined failure sentences, a cache
key that omits the model, a literal `"DEFAULT_PROMPT"` shipped as a system message
— is the weakest part of theirs.

**Where we lose today:**

- **Breadth.** Counted consistently, ~90 distinct problem conditions to our ~70,
  and roughly twice as many available to a user without GCP — where our default
  published image ships with no cloud provider at all.
- **Reachability.** `k8sgpt analyze` with no arguments does cross-resource Ingress,
  Service, StatefulSet and webhook traversal cluster-wide. Our no-argument answer
  is a scorecard, and five commands — including the three we present as our depth
  story — refuse to run until you name the workload. **The graph is a second-call
  advantage, not a detection advantage.**
- **Aggregation on the run-once path**, where they collapse per pod and we do not.
- **Maturity**, by more than the age gap suggests: 144 contributors, 116 releases,
  krew, a Helm chart, an operator and 19 backends, against 165 commits by one
  person with no package-manager presence.
- **The flagship component's operational story.** Our resident sentinel cannot be
  monitored, re-pages on restart, double-runs on upgrade, cannot close what it
  opened, and drops deliveries by design — while their `analyzer_errors` gauge
  gets timestamps, retention, recurrence and resolution free from Prometheus.

**The defensible claim** is that k8s-lookout has the better *architecture* for an
agent-driven SRE system, and two capabilities k8sgpt cannot reach from where it
stands. It is not yet the better *product*: today it is a narrower detector, a
GCP-dependent one, a drill-down instrument that must be told where to look, and a
single-author project whose always-on component is not yet operable.

"Different and correct architecture" is fair, and we should keep saying it. It is
a claim about the design, and we should stop letting it be heard as a claim about
the product. The gap between those two is roughly the 1.7 list, and most of it is
weeks of work, not quarters.

