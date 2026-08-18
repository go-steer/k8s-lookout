# The k8sgpt gap list, and four product decisions

Follow-up to the 2026-08-17 assessment.
← [Executive summary](2026-08-17-k8sgpt-assessment-summary.md) ·
[Q1](2026-08-17-q1-detection-vs-k8sgpt.md) ·
[Q2](2026-08-17-q2-core-agent-lookout.md) ·
[Q3](2026-08-17-q3-mast-lookout.md)

**Date:** 2026-08-18 · **Subjects:** k8sgpt @ `b4a86de` · k8s-lookout `v0.22.0-dev` ·
mast v0.4.0 · core-agent @ HEAD · core-sre-agent @ HEAD

Six questions the original assessment either answered with prose where a table was
wanted, or diagnosed without prescribing. Sections 1 and 2 are the artifacts Q1
should have contained. Sections 3–6 are decisions, and each ends with an explicit
**RECOMMENDATION** line.

Every claim in section 1 was re-verified against `pkg/checks/` on 2026-08-18; the
grep results are quoted where the answer is "nothing".

**Contents**

1. [What k8sgpt detects that lookout does not](#1-what-k8sgpt-detects-that-lookout-does-not)
2. [`lookout scan` — the one-shot entry point](#2-lookout-scan--the-one-shot-entry-point)
3. [mast: library or configured harness?](#3-mast-library-or-configured-harness)
4. [Is core-agent good enough?](#4-is-core-agent-good-enough)
5. [A standalone lookout with an LLM?](#5-a-standalone-lookout-with-an-llm)
6. [Should we consume k8sgpt as an MCP server?](#6-should-we-consume-k8sgpt-as-an-mcp-server)
7. [Consolidated action list](#7-consolidated-action-list)

---

## 1. What k8sgpt detects that lookout does not

Q1 §1.2 gave two prose bullet lists and no itemisation. This is the itemisation.

### 1.1 Real gaps inside coverage we already claim

Ordered by how much they matter, not by effort.

| # | Condition | k8sgpt | lookout today | Verified |
|---|---|---|---|---|
| 1 | **More than one default StorageClass**; PVC with no StorageClass; PV `Released`/`Failed`; `no-provisioner` StorageClass | `pkg/analyzer/storage.go:60-216` | **Nothing** | `grep -rn StorageClass pkg/checks/` → 0 hits |
| 2 | **imagePullSecret does not exist** | `pkg/analyzer/daemonset.go:41-79` | **Nothing** | `grep -rn ImagePullSecret pkg/checks/` → 0 hits |
| 3 | **ReplicaSet `ReplicaFailure` / `FailedCreate`** | `pkg/analyzer/rs.go:46-58` | **Nothing** | only a comment at `spec_render.go:552`; the `FailedCreate` string at `state/edges_checks.go:943` is the missing-ServiceAccount case, not the RS condition |
| 4 | **StatefulSet governing Service missing**; `volumeClaimTemplates` StorageClass missing | `pkg/analyzer/statefulset.go:55-131` | **Nothing** | `grep -rn ServiceName pkg/checks/` → 0 hits (only licence-header "governing permissions") |
| 5 | **IngressClass** unset, or the named IngressClass resource is absent | `pkg/analyzer/ingress.go:55-159`, exemptions `:192-194` | **Nothing** | `grep -rn IngressClass pkg/checks/` → 0 hits |
| 6 | **CronJob**: `spec.suspend`, invalid cron expression, negative `startingDeadlineSeconds` | `pkg/analyzer/cronjob.go:55-115`, `:141-148` | **Nothing.** CronJob appears in `pkg/checks/` only as an inventory / drift / hardening / netpol *template* kind — never as a diagnostic | file list from `grep -rln CronJob pkg/checks/` |
| 7 | **HPA runtime conditions** — `AbleToScale=False`, `ScalingActive=False` | `pkg/analyzer/hpa.go:64-162` | Posture only. `audit.hpa_cannot_scale` catches min==max, missing `scaleTargetRef`, and a utilization metric with no matching container request — it never reads HPA `status.conditions` | `grep -rn "AbleToScale\|ScalingActive" pkg/checks/` → 0 hits |
| 8 | **Wildcard RBAC** — a RoleBinding onto a Role carrying `*` verbs or resources | `pkg/analyzer/security.go:58-201` | `edge.rbac_dangling` covers a *dangling* roleRef. Nothing scores permission scope | `PolicyRule` appears only in `state/rbac_test.go` |
| 9 | **Gateway API in the CLI** — GatewayClass, Gateway, and HTTPRoute `AllowedRoutes` + backend-port validation | `gatewayclass.go`, `gateway.go`, `httproute.go:55-214` | **Sentinel only** (`pkg/sources/gateway/`). The CLI has nothing | Q1 §1.2, re-confirmed |
| 10 | NetworkPolicy selecting zero pods; empty podSelector | `pkg/analyzer/netpol.go:57-87` | `audit.netpol_missing` is the *inverse* — absence of coverage, not dead policies | — |
| 11 | ConfigMap unreferenced / empty / >1MB | `pkg/analyzer/configmap.go:41-150` | Nothing. Low value; listed for completeness | — |
| 12 | Job `spec.suspend`; `status.failed>0` while still retrying | `pkg/analyzer/job.go:55-103` | `job.failed` requires the terminal `JobFailed` condition | — |

**Items 1–6 are the ones worth fixing.** All six are shallow single- or two-object
field assertions — the kind of check `pkg/checks/` is built for — and three of them
are *cause-versus-symptom* gaps on the exact axis we claim to win:

- **#2** — we emit `pod.imagepull` (the symptom) and never name the missing
  imagePullSecret (the cause). This is one of the most common real-world
  ImagePullBackOff root causes.
- **#3** — quota or PodSecurity admission denial creates **zero pods**, so every
  pod-level check in `triage delta` is silent and `workload.rollout` reports only
  "RolloutIncomplete". The `ReplicaFailure` condition is where the reason lives.
- **#5** — an Ingress with an unresolvable class is inert; we check its backend
  Service and TLS Secret and never check whether anything will serve it.

**Item 1** deserves its own note: more than one default StorageClass is silent,
cluster-wide, and breaks every PVC that omits `storageClassName`. We have no
StorageClass logic of any kind.

### 1.2 Whole categories we have scoped out

Not bugs — deliberate scope. But they are most of why the breadth count in Q1 §1.2
goes against us, and they should be named rather than left implicit.

| Category | k8sgpt | Our position |
|---|---|---|
| **OLM v1** — ClusterCatalog, ClusterExtension | 2 analyzers | none. Few tools cover OLMv1 at all |
| **OLM v0** — CSV, Subscription, InstallPlan, CatalogSource, OperatorGroup | 5 analyzers | none |
| **Policy engines** — Kyverno PolicyReport / ClusterPolicyReport | integration, unbounded finding count | none |
| **KEDA** ScaledObject validation | integration | none (we cover HPA only) |
| **Prometheus** scrape-config parse + validate | integration | none |
| **AWS / EKS** cluster health issues | integration | none — `pkg/cloud/` is GCP-only, `//go:build gke \|\| allproviders` |

### 1.3 Where "k8sgpt has it" is misleading

The table in 1.1 should not be read as 12–nil. On the shared surface we are
comfortably ahead, and several of these are not close:

| Condition | k8sgpt | lookout |
|---|---|---|
| Deployment health | one comparison, `spec.replicas != status.readyReplicas` (`deployment.go:57`) | `workload.stalled` reads `Progressing=False` and **beats** `workload.rollout`; covers Deployment, StatefulSet **and** DaemonSet (`delta/pods.go:260-345`), with scale-to-zero excluded |
| Node conditions | condition-to-text mapper | `node.notready` / `pressure` / `condition` / `cordoned` / `preempt`, with a reclaim-taint table |
| Admission webhooks | webhook → Service → Pod phase | plus `failurePolicy` awareness (`failing_closed` vs `dead_backend`), timeout risk, and **caBundle X.509 expiry** |
| PodDisruptionBudget | `Conditions[0]` only (`pdb.go:58`) | `pdb.gridlocked` reads the condition properly, and `drain.pdb_gridlock` scopes it per node |
| Logs | `regexp.MustCompile("(error\|exception\|fail)")` over 100 lines, **first matching line only** | hand-rolled Drain clustering + a per-stream Go/Java/Python stack-trace state machine |
| Service endpoints | endpoints with no subsets | `edge.selector_empty` / `endpoints_missing` / `_orphaned` / `_unready`, naming the specific broken hop |
| Pod waiting reasons | allowlist → text | same allowlist, split into `crashloop` / `imagepull` / `waiting` with distinct severities |

Three of their analyzers read `Conditions[0]` only and silently miss the real
condition if ordering differs (`pdb.go:58`, `gatewayclass.go:52`,
`instalplan.go:66`); two truncate to `failures[:1]` (`storage.go:209`,
`security.go:185`).

**RECOMMENDATION.** Implement 1.1 items 1–6 plus item 9 (Gateway API in the CLI).
Roughly one week. Leave 1.2 alone — those are scope decisions, and adopting any of
them should be driven by a user asking, not by a competitor's analyzer count.

---

## 2. `lookout scan` — the one-shot entry point

Q1 §1.8 identified reachability as a loss (*"the graph is a second-call advantage,
not a detection advantage"*) and stopped there. This is the prescription.

### 2.1 The problem, restated precisely

`k8sgpt analyze` with zero arguments runs 14 analyzers cluster-wide, including
cross-resource Ingress, Service, StatefulSet and webhook traversal. Our
zero-argument answer is `lookout health` — a nine-to-ten-category scorecard — and
**five commands hard-error until the user names the workload that is already
broken**: `state edges`, `bundle`, `triage radius`, `triage events`, `spec`.

There is no run-everything entry point: `pkg/checks/registry.go` exposes `All()`,
`Groups()` and `TopLevel()` and nothing that executes a set.

### 2.2 The design

```
lookout scan [-n <ns> | -A]        # -A is the default
```

**Stage 1 — every target-free check, cluster-wide.** `triage delta` (all five
subsystems), the `health` categories, `state webhooks`, `state volumes`,
`audit workloads`, `audit netpol`, `audit hardening`. All of these already run
without a positional argument today; stage 1 requires no new check code.

**Stage 2 — automatic drill-down.** For every workload stage 1 flagged at warning
or above, run `state edges --workload=<that>` and merge the findings.

Stage 2 is the whole point of the command. It converts the graph from a
second-call advantage into a first-call one, and it removes the "you must already
know which workload is broken" problem **without changing any of the five
commands** — the user no longer names the workload, stage 1 names it.

**Stage 3 — emit the standard contract.** Same `emit.Finding` stream, same
mandatory summary line, same exit codes, fingerprints stamped. So

```
lookout scan | lookout findings diff --store=/var/lib/lookout/lookout.db
```

works on day one, and the sentinel's dedup grain (`engine.Fingerprint` /
`findings.SubjectKey`) applies unchanged.

**Excluded by default; opt in with `--include=cloud,perf,net`:** the four
`cloud *` commands (need a provider), `perf probe` (needs an explicit `--pack` and
bills against GKE metrics), `net probe` (actively sends traffic from inside the
cluster). Existing fail-soft `cloud.unavailable` behaviour is unchanged.

### 2.3 Two things that will bite if not handled up front

1. **The 10-second default timeout.** `pkg/emit/flags.go:64` sets `--timeout` to
   10s. A real cluster-wide scan will exceed it, and a timeout is exit 1 with **no
   summary line** — which the contract defines as a void run. `scan` needs its own
   default of 60s.
2. **Stage 2 is a multiplier.** One `state edges` invocation per flagged workload
   is unbounded on a badly broken cluster. Cap it — `--max-drilldown`, default ~20,
   ordered by severity — and emit an overflow note naming what was skipped, the way
   `log.overflow` already does rather than dropping silently.

### 2.4 Scope boundaries

- **Leave `health` alone for now.** `scan` is the detail stream; `health` is the
  scorecard. Converging them — `health` becomes `scan --summary` over a single pass
  — is the right end state but it is a second change, not a prerequisite.
- **MCP name: `k8s_scan`**, and it should sort first in the tool listing. An agent
  facing 31 tools with no obvious entry point currently has to guess, and the
  generated descriptions do not tell it where to start.
- Bare `lookout` with no subcommand should keep printing help. Parity with k8sgpt
  is `lookout scan` against `k8sgpt analyze` — theirs is a subcommand too.

**RECOMMENDATION.** Build `lookout scan` as specified. It is the single highest-
leverage item in this document: it closes the reachability loss outright, it makes
the graph a first-call feature, and stage 1 is composition of code that already
exists. Estimate roughly one week including the fan-out cap and the timeout
default.

---

## 3. mast: library or configured harness?

**Configured harness. mast was built to be one, and core-sre-agent took the library
path anyway.**

### 3.1 The harness surface is complete

mast's README states the intent as pillar 2 — *"Library-embedded. A Go library
first (`mast.RunWorkload(ctx, ...)` from your own service ...) and a standalone
binary second (`mast serve` ...). Same subsystems in both shapes."* Both halves
exist: `mast.go:173` is `RunWorkload`, and `cmd/mast/` carries `main.go`,
`oneshot.go`, `attach.go`, `sessions.go`, `autoresume.go`, `pausesched.go`,
`a2a.go`, `agui.go`, `shutdown.go`.

The declarative surface:

```
examples/workloads/gke-triage/
  workload.yaml        roster, tool catalog w/ per-tool `mutating:` flags,
                       budget, hitl policy, dispatch shape, edge_trigger
  mcp.json             MCP servers — supports transport http AND stdio
                       (pkg/mcp/toolset.go:51-58, newStdioToolset :119-125)
  specialists/*.tmpl   14 of them; YAML frontmatter (description,
                       output_schema, budget, per-server tool allowlist)
                       followed by the prompt body
  schemas/finding.json typed output contract
```

That bundle is **already reason-keyed SRE triage**: specialists named
`CrashLoopBackOff`, `ImagePullBackOff`, `ErrImagePull`, `OOMKilled`, `FailedMount`,
`FailedScheduling`, `BackOff`, `Unhealthy`, `NetworkNotReady`, `NodeNotReady`,
`Evicted`, plus `triage-classifier`, `_fallback`, and a single `change-executor`
that is the only member holding write tools — with
`internal/compose.CheckCapabilitySplit` **refusing to start a roster** in which a
read-only diagnoser can reach a write tool. It declares
`edge_trigger.http.path: /inject` with bearer auth, which is the sentinel's target.

It points at the hosted Google GKE MCP server rather than lookout. But `pkg/mcp`
supports `TransportStdio`, so swapping in `lookout mcp` is an `mcp.json` edit plus a
`tool_catalog.tools` block — **not** the 557 lines core-sre-agent wrote in
`internal/lookout`.

### 3.2 How core-sre-agent's 11,526 internal lines split

| Bucket | Packages | Lines |
|---|---|---|
| **Reinvented — mast already has it** | `internal/lookout` (557), `internal/approval` (373, *zero importers*), the classification half of `internal/kubewrite` | ~1,100+ |
| **Genuinely not expressible in a bundle today** | `internal/scheduler` (651), `internal/notify` (724), `internal/bounded` (443) | ~1,800 |
| **Test harness, not product** | `internal/faults`, `internal/kindcluster`, `internal/evals` | ~3,000 |

The middle bucket is the real finding. `workload.Bundle` has `edge_trigger` but
**no schedule or cron field at all** (`grep -n "Schedule\|[Cc]ron" pkg/workload/*.go`
→ 0 hits), so the three-clock scheduler cannot be declared. There is no notification
egress block, so the switchboard path cannot be declared. There is no cheap-tier
one-shot mode, so the `$0.0046` bounded pass cannot be declared.

Those three are the case for either extending the bundle schema or staying on the
library — and they are a much narrower case than 11,526 lines suggests.

### 3.3 The blocker to know about

`ModeMultiSession` is in the vocabulary but **not honoured**: `pkg/workload/bundle.go:37`
still reads *"will be honored once the multi-session substrate lands"* — in v0.4.
`loader.go:69` accepts the value and nothing downstream implements it.

So harness mode today is **one long-lived session per workload**. For per-incident
sessions you are on the library path until that substrate lands. This is Q3's item 1
(`runner.NewInMemory` is the only runner) seen from the configuration side — the
same gap, two symptoms.

**RECOMMENDATION.** Treat harness mode as the target and the library as the escape
hatch. Concretely: fork `examples/workloads/gke-triage` into a `lookout-triage`
bundle, point `mcp.json` at `lookout mcp` over stdio, and see what survives contact.
Expect the specialists, tool catalog, write gate and inject trigger all to go
declarative. Expect to be left holding the scheduler and the notify path — and those
two should then move **into mast as bundle fields** (`schedule:` and `notify:`)
rather than staying as bespoke Go in the consumer. Do this as a timeboxed spike
before any further investment in `internal/`.

---

## 4. Is core-agent good enough?

Q2's verdict — "superseded" — reads harsher than the evidence supports. Two claims
got tangled and should be separated.

### 4.1 Is the runtime good? Yes.

59,846 non-test LoC in `pkg/`, 2,506 tests, near-universal issue-numbered rationale
comments, crash-durable tail repair of orphaned tool calls, guardrail trip-state
restore, compaction boundary events, auto-continue with a boot-count circuit
breaker, and a watchdog with three signals (not one — an earlier draft had that
wrong, and `pkg/watchdog/cycle.go:17-24` was purpose-built for the alternating-loop
case). Nothing in Q2 disputes any of this, and nothing here does either.

### 4.2 Is it the right substrate for the SRE line? No — and the reason is not quality.

The deciding fact is `google.golang.org/adk v1.2.0` (`core-agent/go.mod:32`) against
mast's `v2.2.0`. One major version behind. Everything mast built on ADK v2 — durable
approvals, the effect-keyed write gate, the specialist/workload split — would have
to be rebuilt on v1, or core-agent would have to take the v2 migration. The org has
already answered this: `core-sre-agent` imports **zero** core-agent packages, and
every scored eval result we possess was produced on mast.

core-agent's own README agrees, and is worth quoting because it settles the framing:
mast is *"not the successor to core-agent. Sibling products with different jobs:
mast is the platform-agent runtime; core-agent stays the experimentation +
integration substrate. Both are maintained."*

### 4.3 The pushback: core-agent has one thing mast does not, and it is the SRE one

**`pkg/tools/wait_and_verify.go`** — a bounded poll-until-condition that will only
poll tools the runtime itself classifies read-only (`:358`), returning per-attempt
evidence, so "RESOLVED" means an observed convergence rather than a model assertion.

**mast has no equivalent.** `grep -rln "wait_and_verify\|WaitAndVerify"` across mast
returns nothing, and mast ships **no `pkg/tools/` directory at all**.

That is the *verify* half of diagnose → fix → verify. The moment writes are enabled
— the parked `internal/kubewrite` from §3.2 and Q3 — mast will have a durable,
effect-keyed gate to **approve** a change and nothing at all to **confirm it
worked**. For an unattended agent that is the more dangerous half to be missing:
an approved-but-ineffective remediation closes the incident in the agent's mind
while the cluster stays broken.

### 4.4 The standing cost of two frameworks

Q3 §3.3 found both repositories independently working around the same ADK
`readOnlyHint` bug — core-sre-agent at `internal/lookout/lookout.go:144-186`, mast in
`pkg/mcp`. That is the recurring tax of a two-framework org, and it will keep
recurring.

**RECOMMENDATION.** Do not invest further in core-agent for the SRE line; that ship
has sailed and sailing it back would cost an ADK major-version migration. But the
actionable item is not "fix core-agent" — it is **port `wait_and_verify` into mast**,
before rather than after the write question is decided. It is a small port and it is
on the critical path for any remediation story.

---

## 5. A standalone lookout with an LLM?

**Mostly no — but the instinct behind the question is right, and there is a much
cheaper way to satisfy it.**

### 5.1 The case against building it in

Release 0.21.0 deliberately dropped core-agent, ADK and `google.golang.org/genai`,
cutting the build closure from 1,074 packages to 966. "The instrument never
hallucinates, and every output is golden-testable" is a real property and it is
load-bearing for the agent-substrate pitch that Q1 §1.8 calls our defensible claim.
Putting a model back into the binary reverses a decision made on purpose.

It also puts us on k8sgpt's treadmill. Their CHANGELOG for Feb–Jul 2026 is the
warning: nine releases, **8 feature entries, exactly one new analyzer**, and the rest
dominated by AI-provider breadth and provider quirks — Bedrock Converse, Groq, Azure
API type, a Vertex model-id typo, Anthropic temperature/top_p exclusivity, OpenAI
`MaxCompletionTokens`. Nineteen backends is where their engineering went instead of
detection.

### 5.2 We already ship the standalone LLM experience and do not say so

`lookout mcp` plus the four `skills/` and three playbooks, pointed at Claude Code,
Claude Desktop or any MCP client, **is** the narrated-answer experience — and it is
categorically better than `--explain`: real tool calling, follow-up questions, 31
tools, against a single-shot 280-character narration fed only joined failure
sentences (`pkg/ai/prompts.go:4-8`, `analysis.go:564`).

What is missing is not a model. It is five paragraphs of README and a copy-pasteable
`mcp.json`.

### 5.3 Ranked options

1. **Do now, nearly free.** A "Using lookout with an LLM" README section with the MCP
   config block for Claude Code / Claude Desktop / generic MCP clients. Zero new code,
   zero new dependencies, and it answers the objection directly.
2. **Do if you want the k8sgpt-shaped answer.** A `lookout explain` that reads a
   finding stream on **stdin** — `pkg/findings/report.go ParseReport` already parses
   our own output, auto-detecting logfmt vs JSON per line — and calls one provider.
   Ship it as a **separate Go module in the same repo**, so the core binary's
   dependency closure and the no-LLM property both survive intact. One backend, not
   nineteen.
3. **Do not** put a model behind the check commands themselves.

If option 2 happens, note the asymmetry in our favour: our findings carry severity,
fingerprints, and glossary-declared structured detail keys, so the prompt can include
real context. k8sgpt's structurally cannot — `analysis.go:564` sends the joined
failure sentences and nothing else, no manifest, no resource identity, no owner
chain, no other findings. A thin `explain` over our data would be straightforwardly
better than theirs, which is the honest reason to consider building it.

**RECOMMENDATION.** Do option 1 this week. Hold option 2 until someone asks for it;
if built, it goes in a separate module with a single backend. Never option 3.

---

## 6. Should we consume k8sgpt as an MCP server?

**No.** It takes the wrapper and leaves the value.

### 6.1 What the server actually offers

Twelve tools (`pkg/server/mcp.go:132-341`), and eleven are things we already do
better or do not want: `list-resources`, `get-resource`, `list-namespaces`,
`list-events`, `get-logs`, `cluster-info`, `list-integrations`, plus four that
manage k8sgpt's own configuration. The only tool of interest is `analyze`.

Hosting it drags in:

- **`get-resource` includes `secret` in its resource registry and returns it
  unredacted** (`pkg/server/mcp_handlers.go:130-137`). Our sanitizer is always-on at
  the source, two layers; theirs does not exist — `--anonymize` only ever mutates the
  LLM prompt and un-masks the reply on the way back.
- **`add-filters` / `remove-filters` write the user's `k8sgpt.yaml`** via
  `viper.WriteConfig()` (`mcp_handlers.go:491-494`, `:527-530`). Mutating MCP tools,
  inside a stack whose safety story is "3 of 31 tools mutate and none of them touch
  the cluster."
- **`analyze`'s handler binds nine parameters beyond its declared schema**
  (`mcp.go:344-358`).
- **Their findings have no severity, no timestamp and no stable ID**
  (`pkg/common/types.go:77-98`). So they cannot join `findings diff`, cannot dedup,
  cannot be fingerprinted, and cannot participate in the recovery state machine. You
  would run two finding streams with incompatible identity models and no way to
  assert that a k8sgpt finding and a lookout finding are the same incident.

### 6.2 If what you want is the coverage — and per §1 you should want about six items of it

**(a) Implement the six.** They are shallow field assertions in a codebase built
for exactly that. Everything lands inside the contract, with severity, fingerprints,
sanitization, exemptions and run-to-run diff for free.

**(b) Shell out to `k8sgpt analyze -o json --filter=Storage,Ingress,...` and adapt
into `emit.Finding`.** Faster to a demo, but it inherits a binary dependency, the
`Conditions[0]`-only defects in three of their analyzers, and a mapping layer that
has to invent a severity that does not exist upstream. Worth it **only** as a
throwaway measurement — run it against a real cluster to find out how much the
missing coverage actually surfaces before committing engineering to (a).

**(c) Register lookout as a k8sgpt custom analyzer.** The reverse direction, and the
one that would put us in front of their users. Not viable today: the `RunRequest` is
**empty** (`pkg/custom/client.go:33-58`) so we would receive no namespace, no
filters and no context, the transport is plaintext gRPC with `insecure.NewCredentials()`
and no auth (`client.go:18-31`), and a TODO at `:47` concedes sensitive-data masking
is unsupported. Revisit only if that protocol grows a real request.

**RECOMMENDATION.** Do not adopt their MCP server. Spend one day on (b) purely as
measurement, then do (a). Keep (c) on the watch list as a distribution play if their
custom-analyzer protocol ever carries a populated request.

---

## 7. Consolidated action list

Merged with the eight-item list in the
[executive summary](2026-08-17-k8sgpt-assessment-summary.md#what-to-fix-in-order),
which stands unchanged. New items from this document are marked **NEW**.

| Effort | Item | Source |
|---|---|---|
| Hours | **NEW** README: "Using lookout with an LLM" via `lookout mcp` + an MCP client | §5, option 1 |
| Hours | Real `/healthz` — currently a static 200 on both probes | summary #2 |
| ~1 day | Add the arm gate to `pkg/sources/k8sevents` | summary #1 |
| ~1 day | Leader election, or `strategy` + `replicas`, in `deploy/51-deployment-watcher.yaml` | summary #4 |
| ~1 day | `stab drift` majority-manager minimum-share threshold | summary #6 |
| ~1 day | **NEW** Measurement spike: `k8sgpt analyze -o json` against a real cluster, to size the gap | §6, option (b) |
| Small | **NEW** Port `wait_and_verify` from core-agent into mast | §4.3 |
| ~1 week | **NEW** `lookout scan` — stage 1 + automatic `state edges` fan-out + 60s default timeout + `--max-drilldown` | §2 |
| ~1 week | **NEW** Six missing checks: StorageClass, imagePullSecret, ReplicaSet `ReplicaFailure`, StatefulSet governing Service, IngressClass, CronJob | §1.1 items 1–6 |
| ~1 week | Gateway API in the CLI | summary #5, §1.1 item 9 |
| Spike | **NEW** Fork `gke-triage` → `lookout-triage` bundle; measure what stays in `internal/` | §3 |
| Project | Wire the durable session path in core-sre-agent (`runner.NewInMemory` is the only runner) | summary #3 |
| Decision | State the trigger for enabling writes — two finished subsystems, ~4,000 lines, parked | summary #8 |
| Decision | Give mast's permission layer Kubernetes shape (`permissions.FromConfig` has no non-test callers) | summary #7 |

If only three things happen: **`lookout scan`**, **the six missing checks**, and the
**`lookout-triage` bundle spike**. The first two close the two losses Q1 §1.8 named
— breadth and reachability — and the third tells you whether ~1,100 lines of
core-sre-agent should exist at all.
