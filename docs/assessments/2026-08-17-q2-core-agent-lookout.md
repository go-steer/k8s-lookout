# Q2 — Is core-agent + k8s-lookout up to snuff?

Part 2 of 3. ← [Executive summary](2026-08-17-k8sgpt-assessment-summary.md) ·
[Q1](2026-08-17-q1-detection-vs-k8sgpt.md) · [Q3](2026-08-17-q3-mast-lookout.md)

**Question:** given what we have built in core-agent, `core-agent-demo-3` and the
earlier `gke-troubleshoot-agent` example, is core-agent up to snuff when combined
with k8s-lookout — measured against k8sgpt?

**Status:** complete, revised under adversarial review. The red team landed four
verdict-level hits, each independently re-verified before acceptance; one of them
falsified this section's original headline claim. Corrections are preserved
inline.

**Subjects:** core-agent @ HEAD (ADK v1.2.0) · `core-agent-demo-3` ·
`core-agent/examples/gke-troubleshoot-agent` · k8s-lookout `v0.22.0-dev`.

---

### 2.0 The framing problem: the question is already partly answered by our own org

**`core-sre-agent` — the flagship SRE application — imports zero core-agent
packages.** It is built on `go-steer/mast v0.4.0` (self-described as "a lean fork
of go-steer/core-agent, native to ADK v2") plus `google.golang.org/adk/v2 v2.2.0`
(`core-sre-agent/go.mod:6,8`). core-agent is pinned to `google.golang.org/adk
v1.2.0` (`core-agent/go.mod:32`) — one ADK major behind.

So Q2 and Q3 are not independent. The SRE line has already migrated. Every scored
eval result we possess was produced on mast, not core-agent.

### 2.1 The runtime is genuinely strong

59,846 non-test LoC in `pkg/`, 2,506 tests, near-universal issue-numbered
rationale comments. Almost every design doc is shipped — only **code mode**,
**bidirectional MCP (server mode)**, **tiered tools**, and **scheduled cron ops**
are design-only.

The two primitives that matter most for SRE both ship and are **runtime-enforced,
not prompt-held**:
- **Plan-first** (`pkg/permissions/gate.go:558-590`) — mutating calls are denied
  until `record_plan` fires. A gate, not a convention. **But see 2.1.1: in the
  shipped Kubernetes recipe it gates nothing.**
- **`wait_and_verify`** (`pkg/tools/wait_and_verify.go`) — bounded
  poll-until-condition that will only poll tools the runtime itself classifies
  read-only (`:358`), returning per-attempt evidence. This lets "RESOLVED" mean an
  observed convergence rather than a model assertion.

Plus real crash-durability: tail repair of orphaned tool calls, guardrail
trip-state restore, compaction boundary events, auto-continue with a boot-count
circuit breaker.

#### 2.1.1 The plan gate is weaker than 2.1 claims, twice over

**First: `record_plan` is a model self-service unlock.** It is always allowed,
never gated, and its own doc comment calls it "the escape valve from plan-first
gating — it does not call the gate" (`pkg/tools/record_plan.go:108-121`). There is
no schema beyond non-empty-after-trim, and **nothing ever compares a later
mutation against the recorded plan.** So the sequence is: model writes any
non-empty string, model is now unlocked. It raises the cost of an unplanned
mutation by one tool call. That is not nothing — it forces an auditable statement
of intent before a write — but "denied until" overstates a gate the caller opens
itself.

**Second, and more serious: the shipped Kubernetes recipe turns the gate off
entirely.** `examples/gke-troubleshoot-agent/deploy/base/config/config.json:8`
sets `"mode": "yolo"`, and `recipe_test.go:103-105` *asserts* it must be yolo,
with the rationale that "a no-TTY daemon cannot answer a prompt." The reasoning is
sound — an unattended daemon has nobody to ask — but the consequence is that **in
the only shipped k8s deployment, the permissions gate is bypassed wholesale**, and
the plan-first machinery in front of it sits ahead of a read-only toolset.

The gate has therefore never gated a cluster mutation in any shipped
configuration. This does not make it bad engineering — the runtime primitive is
correct and will matter the moment a durable approval path exists. But 2.1 should
not have described it as the SRE safety story without saying that the deployed
recipe disables it, and that the gap it leaves is exactly what mast's durable
approvals are built to fill.

### 2.2 The integration gap — real, but narrower than an earlier draft claimed

An earlier draft said flatly that "core-agent does not use lookout's read path."
**That is false, and it was the largest error in this section.**

`pkg/inject/payload.go:94-105` defines a `PayloadEnrichment` envelope carrying a
`lookout bundle`-shaped payload — one finding per line, each tagged with a
`section` key of `spec|delta|edges|radius|logs`. That is the output of
`k8s_triage_workload` and `k8s_blast_radius`, two of the three tools the draft
called unused. It is **on by default** (`--enrich=critical`,
`internal/watch/flags.go:294`), demo-3 does not disable it, and it degrades
honestly: overflow trailers name the exact `lookout` command that reproduces a
dropped section.

So lookout's read path *is* reaching the agent — **pushed, not pulled.** The
accurate gap is three specific things:

1. **No pull-side lookout tooling.** The agent cannot ask a follow-up question. It
   gets one bundle at inject time and everything after that goes through the
   Google GKE MCP's 14 generic full-YAML tools.
2. **A 4096-byte cap** (`--enrich-cap`), which is a small fraction of what the
   MCP server would answer with.
3. **Initial inject only** — no enrichment on subsequent turns of the same
   incident.

**And the fix is not "roughly an `mcp.json` change."** That estimate was wrong by
an order of magnitude. `lookout mcp` binds stdio or loopback only
(`cmd/lookout/mcp.go:51-58`); the daemon image is distroless-static with no
lookout binary in it; and the `core-agent-daemon` ServiceAccount holds **zero
Kubernetes RBAC**. Wiring the MCP server into the agent pod means giving the agent
process cluster read — up to and including the sentinel's `secrets: list`. The
current split, where the privileged reader is a separate pod that pushes a capped
sanitized bundle to an unprivileged agent, is **a deliberate security boundary,
not an oversight.** Any proposal to close this gap has to say what happens to that
boundary.

Only the **mast-based** `core-sre-agent` wires `lookout mcp` as a pull-side
toolset — and it does so via ADK's mcptoolset directly, bypassing `mast/pkg/mcp`
(see Q3).

It does so over stdio with a `tools/list` handshake purely to recover
`ReadOnlyHint`, which ADK's mcptoolset discards
(`core-sre-agent/internal/lookout/lookout.go:144-186`).

### 2.3 What has actually been demonstrated

`core-agent-demo-3` is a real UAT harness with six injected faults
(`bad-secret`, `bad-image`, `oom`, `unschedulable`, `bad-probe`, `restore`),
each mapped to specific lookout sources and a specific expected reasoning branch.
The instruction design is good — 12 per-reason playbooks, and an explicit
pass condition that the report must contain the subagent's *actual evidence and
proposed patch*, because "a content-free summary is the failure this demo exists
to detect."

**But it is propose-only, and no end-to-end remediation has ever been
demonstrated under the current safety posture.** The only autonomous fix
transcripts in the entire corpus come from the *superseded* demo-1, whose agent
still had mutating GKE tools and no plan gate. The diagnose→**fix**→verify loop is
architecturally supported and has never been run.

Recorded results in demo-3: none. Three `"model": "echo"` `"ping"` stubs. The
*failures* are better documented than the successes — 37 tool calls / 2.1M input
tokens diagnosing its own pod until an operator added `--exclude-namespace`.

Fault classes demo-3 does **not** cover: saturation, degradation, storage/PVC,
connectivity/DNS/NetworkPolicy, security posture, HPA/VPA, cert/secret expiry,
ingress, Job/CronJob, StatefulSet/DaemonSet rollouts, all node/infra faults,
control-plane/RBAC, multi-cluster, GitOps, and application-level silent failures.

### 2.4 No evals in core-agent — at all

`docs/eval*.md` are a framework *selection* memo (which recommended Hermes, not
core-agent) and a rebuttal scorecard. **No `evals/` dir, no runner, no dataset, no
judge, no committed results.** The only executable gates are 11 shell smoke
scripts and per-recipe structural `recipe_test.go` assertions — which catch config
drift but say nothing about answer quality.

There is currently no way to answer "did upgrading the model, editing AGENTS.md,
or bumping lookout make triage better or worse."

### 2.5 The cost number that should change our roadmap — with three caveats

From the mast-based `core-sre-agent` tier-2 suite (11 kind-cluster fixtures,
10 scored, 1 healthy fixture skipped):

| metric | bounded one-shot Haiku | full agentic |
|---|---|---|
| fault_recall | 0.517 | 1.000 *(best run; see below)* |
| hallucinated_fault | 1.000 | 1.000 |
| cost / fixture | **$0.0046** | **$0.2374 (51.6×)** |
| latency | 1.1–6.6 s | 16 s – 2 m 44 s |
| model calls | 1 | 7 (mean) |

Projected: **$1.32/day vs ~$683/day.** The orchestrator alone burns 88.9–93.4% of
input tokens. **The cost ratio is the durable finding and it is large enough that
the caveats below do not threaten it.**

The quality numbers are much softer than three decimal places imply:

1. **`1.000` is the best run, and the source says so.** `AGENTS.md:490` labels it
   "the best figures the suite has produced." Across the recorded runs
   full-agentic `fault_recall` is **1.000, 0.944, 0.889, 0.833** — quoting the
   top of that range against bounded mode's 0.517 overstates the gap. `root_cause
   1.000` is worse: it is **1 fixture scored, 10 skipped.**
2. **No committed artifacts.** `cmd/sre-eval-live/main.go` is a real 22 KB
   harness — this is not a paper exercise — but there is no results directory and
   no scored output in git. Every number above exists only as a prose table in
   `AGENTS.md`. They cannot be diffed across commits, which is precisely the
   capability 2.4 says the org lacks.
3. **n=10 hand-written fixtures**, authored by the same people as the evaluators.

**And the closing claim in the earlier draft was false.** It said "nothing in
either runtime routes a cheap deterministic pass first and escalates only on
change; neither has it." `core-sre-agent/internal/scheduler/scheduler.go` **is
exactly that**, and in a better form than the draft proposed — three clocks, not
two:

- every cycle, the bounded pass plus a diff of its scan against the last;
- on a new or escalated transition, the full agent scoped to that namespace;
- every `Floor` interval, the full agent regardless of the diff.

The third clock exists because the team *measured* the failure mode the draft's
two-clock proposal would have shipped with: the bounded pass cannot see a fault
that is an absence, and on two of the eleven fixtures it returned `ok` with no
findings, so a change-trigger alone would never revisit them — as happened to a
tier-3 namespace broken the same way for twelve days. It keys on
`lookout findings diff` rather than a home-grown differ.

This was the most embarrassing error in the section: the draft mined this very
repository for its cost figures and missed the component built in response to
them, which is quoted in that component's own package documentation.

Tier-1 severity accuracy sits at **0.387–0.548 across five runs, with every miss
"too hot" and zero "too cold"** — the agent systematically over-escalates.
Grounding is 1.000 and hallucination 1.000, so it does not invent; it
mis-prioritizes.

### 2.6 Gaps that would bite in production

1. **No sandbox for `bash`** — the daemon runs shell in its own process and pod,
   which holds MCP OAuth tokens, attach bearer secrets, and the session DB. Every
   recipe copes by disabling bash entirely. The package doc admits the denylist
   "is not a security boundary."
2. **No Kubernetes-aware policy layer.** The gate reasons about tool names, paths,
   URLs, and shell verbs — it has no concept of a k8s verb, resource, namespace,
   or blast radius. `kubectl delete ns prod` and `kubectl get pods` are the same
   shape to it. The only k8s-aware guard in the corpus lives in
   `core-sre-agent/internal/kubewrite/guard.go` — which is mast, not core-agent.
3. **HITL approvals are not durable** (`pkg/attach/prompter.go:45` is an in-memory
   map). A pod eviction during an approval loses the request and fails the tool
   call mid-remediation. mast treats exactly this as its flagship feature.
4. **Watchdog.** An earlier draft said it has one signal — N consecutive
   *identical* calls — "trivially evaded by an alternating loop." **Wrong on
   both counts:** it has three signals, and `pkg/watchdog/cycle.go:17-24` was
   purpose-built to catch exactly the alternating-loop case. What remains true,
   and is the point worth keeping, is that the observed real runaways were
   stopped by the **cost ceiling**, not by the watchdog.
5. **Provider lock-in** to Google + Anthropic. No OpenAI-compatible endpoint or
   LiteLLM, which was an explicit criterion in the team's own framework eval.
6. **Doc rot** — a design doc header says "DESIGN" for something its own body
   records as shipped. (An earlier draft also cited stale `TODO.md` entries;
   that file is git-ignored private notes and should not have been used as
   evidence.)

### 2.7 Q2 verdict

As a **runtime**, core-agent is up to snuff and then some — the plan-gate plus
`wait_and_verify` pair is the correct SRE primitive set, and the durability work
is real.

As the **chosen substrate for the SRE line**, it has already been superseded in
practice by mast. It has never been measured, has never completed a fix→verify
loop under its own current constraints, and does not consume the read path of the
tool it is being paired with.

"core-agent + k8s-lookout" as a *combination* is **under-built but not unwired.**
Lookout pushes a real, sanitized, capped triage bundle into every critical
incident session by default. What is missing is the *pull* side — the agent cannot
ask a second question of lookout — and closing that means deciding whether the
agent pod may hold cluster read, which is currently and deliberately denied.

**Against k8sgpt specifically** — the comparison this section was supposed to
make and did not, in any of its drafts:

k8sgpt has no agentic loop, no tool calling, no plan or approval concept, and no
remediation path; it emits a 280-character narration per finding. So the
comparison is not close on capability, and it is not really the same product
category. But two things temper that:

- k8sgpt's `analyze → explain` needs one binary and an API key. "core-agent +
  lookout" needs a daemon, a watcher, a session DB, an MCP config, a
  ServiceAccount split and a model budget. **For "tell me what is wrong with my
  cluster right now," they win on time-to-value by a wide margin**, and that is
  the evaluation most first-time users actually run.
- Everything our combination does beyond that is **currently propose-only and
  unmeasured against theirs.** We have no head-to-head. Any external claim of
  superiority is at present an architectural argument, not a demonstrated one.

