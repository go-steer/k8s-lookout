# Q3 — Is mast v0.4.0 + k8s-lookout up to snuff?

Part 3 of 3. ← [Executive summary](2026-08-17-k8sgpt-assessment-summary.md) ·
[Q1](2026-08-17-q1-detection-vs-k8sgpt.md) · [Q2](2026-08-17-q2-core-agent-lookout.md)

**Question:** the same judgement as Q2, but for mast v0.4.0 combined with
k8s-lookout — measured against k8sgpt.

**Status:** complete, and **substantially revised** after adversarial review. The
red team landed two verdict-level hits, and unusually **both were cases where the
draft was unfair to mast**: it condemned an eval rig it had not found, and
declared a notification path missing that ships in the consumer. §3.4 is retracted
in full. Both retractions are preserved in place rather than deleted, because the
failure mode that produced them — generalising from the first directory that
matched a search — is worth recording.

**Subjects:** mast v0.4.0 (ADK v2.2.0) · `core-sre-agent` @ HEAD, the only real
mast consumer · k8s-lookout `v0.22.0-dev`.

---

### 3.0 Why this is the question that actually matters

Q2 established that the SRE line already migrated: `core-sre-agent` is built on
mast v0.4.0 + ADK v2.2.0 and imports zero core-agent packages. So Q3 is not a
hypothetical alternative to Q2 — **it is the assessment of what we actually
ship.** Everything scored, every eval number in 2.5, and the only pull-side
`lookout mcp` integration in the org all live here.

### 3.1 mast is the better framework for this job, and the reasons are concrete

Three things mast has that core-agent does not, all of which matter for an
unattended SRE agent:

1. **Durable approvals, and they are *proven*, not asserted.** An earlier draft of
   this section sourced the claim to a code comment, which was weak evidence for
   a flagship feature. The real proof is a test:
   `mast/scripts/uat-v0.3.sh:756` — *"U-gate-crash: a parked call survives kill -9
   and is answered by the next process."* It genuinely `kill -9`s the daemon, the
   parked approval survives into a fresh process, and **the call then runs exactly
   once across the crash** — the hard part, since naive replay would double-execute
   a mutation. Reported harness result: PASS=132, FAIL=0. This is precisely the
   defect in core-agent's `pkg/attach/prompter.go:45` in-memory map (2.6 item 3),
   and mast has closed it properly.
2. **A mutation-triggered write gate** with a declarative policy —
   `hitl.on_mutation: require_approval | apply | dry_run`
   (`pkg/workload/loader.go:77-80`) — rather than core-agent's plan-first unlock.
   Because it keys on *effects* rather than on a self-declared plan, it does not
   have 2.1.1's self-service problem.
3. **ADK v2**, where core-agent is a major version behind on v1.2.0.

If the question is "which framework should the SRE line be on," mast is the right
answer and the migration was correct.

### 3.2 But the only consumer barely uses it

`core-sre-agent` imports **five** mast packages, and this is the whole list:

| package | import sites |
|---|---|
| `pkg/pricing` | 7 |
| `pkg/agent` | 6 |
| `pkg/budget` | 5 |
| `pkg/specialists` | 3 |
| `pkg/providers/anthropic` | 1 |

(Six if you count `pkg/taskclass`, reached transitively.) It imports **none** of
`eventlog`, `approval`, `permissions`, `effects`, `workload`, `mcp`, `attach`,
`inject`, `watchdog`, `transcript`, `graph`, `router`, `planner`, `a2a`, or
`observability`.

Read that against 3.1: all three of mast's differentiators are in packages the
product does not import. **But the reason is not neglect — see 3.2.1, and read
that before drawing any conclusion from this table.**

**And there are no durable sessions at all.** The only runner construction in the
repository is `runner.NewInMemory` at `internal/evals/runner.go:73` — and both
production binaries route through it: `cmd/sre-agent/main.go:285` and
`cmd/sre-monitor/main.go:336` each build an `&evals.Runner{}`. The production
agent runs on the eval harness's runner. Sessions are in-memory, so a pod
restart loses the incident. This is not a misconfiguration; the durable path was
never wired.

#### 3.2.1 The product is deliberately read-only, and that reframes everything above

**Both production binaries structurally exclude writes.** `sre.Config.Writes` is
never populated by either one, and both say so at length:
`cmd/sre-monitor/main.go:328` — *"No Writes. The whole reason this command is safe
to leave running."* `cmd/sre-agent/main.go:273` notes the change-executor is
therefore never constructed. There is an entire package, `internal/readonly`,
whose job is to **assert** that no write tools are built (`readonly.go:30`).
`internal/approval` (1,156 lines) has zero importers. The three mutating lookout
tools are *withheld from the toolset*, not gated at call time.

This is a coherent and defensible posture: **the product does not write to
clusters yet, on purpose.** Read-only is the safety mechanism, and it is a
stronger one than any gate.

It also means the criticism in 3.2 is aimed at the wrong target. mast's write gate
is unused by the product not because the integration was neglected, but because
**the capability it guards is intentionally absent.** The honest version of the
finding is narrower and more useful:

> The org has built two substantial write-path subsystems — mast's durable
> approval gate and core-sre-agent's 2,722-line `internal/kubewrite` — and wired
> neither into a shipping binary. That is a large amount of finished work parked
> behind a decision to stay read-only. The decision is right for now; the question
> for the roadmap is what evidence would change it.

The Q2/Q3 comparison should therefore not be read as "mast has the safety story
core-agent lacks." Neither stack writes to a cluster in any shipped
configuration. core-agent's recipe is `yolo` over a read-only toolset; mast's
product withholds the write tools entirely. mast's approach is better, but the
*deployed* difference in blast radius today is zero.

### 3.3 The lookout integration is real, and it routes around mast

`core-sre-agent/internal/lookout/lookout.go:88-133` wires `lookout mcp` as a stdio
subprocess — the only genuine pull-side lookout integration anywhere in the org,
and the thing 2.2 says core-agent lacks. It works, and it is careful: a
`tools/list` handshake recovers `ReadOnlyHint`, which ADK's mcptoolset discards,
and 3 of 24 tools are correctly declared as writers (`:200-206`).

But it does all of this **via ADK's mcptoolset directly, bypassing
`mast/pkg/mcp`**, and in doing so independently reinvents mast's `named` wrapper,
clean-env handling, allowlist filtering, approval gate, and scheduler. The file's
own comment says the fix belongs "here rather than in mast" (`:41-46`). Both
repositories separately work around the same ADK `readOnlyHint` bug.

So the framework/product split is not paying for itself: the consumer is
reimplementing the framework's subsystems next door to them.

### 3.4 RETRACTED — mast's write gate *is* properly evaluated

**An earlier draft of this section called it "the sharpest finding in Q3." It was
false, and it is retracted in full.** The claim was that mast validates its write
gate against a tool surface with no mutating tools, so the gate could never fire
in its own evals.

That was based on reading `mast/internal/evals/judge/`, where `readOnlyPolicies`
(`rig.go:296-308`) does declare every lookout tool non-mutating. **But that is not
the write-gate rig.** `mast/internal/evals/differentiators/` is, and it contains
exactly what the draft said was missing:

- genuinely mutating tools — `scale_deployment` and `rollout_restart`
  (`rig.go:66-68`), described in their own registrations as "Mutating";
- a real SQLite session store (`rig.go:201`), not an in-memory fake;
- the daemon's own `compose.WriteGate`;
- an out-of-band operator answering parked calls from the durable log.

Reported result on running it: **5/5 PASS in 0.81s**, exercising
`awaiting_approval`, `denied_by_operator`, `edit_applied`, `apply`, and outbox
replay.

The error was one of coverage, not of citation: every file the draft cited said
what it claimed, but the draft generalised from one eval package to "mast's
evals." This is the standard failure mode of an inventory pass that stops at the
first directory matching the search, and it is worth noting that it produced the
section's most confident claim.

*What survives, as a footnote rather than a finding:* mast ships no `pkg/tools/`
directory, so there is no `record_plan` tool, and the plan-first machinery its
`permissions.Gate` inherited from core-agent — `planExemptTools`,
`MarkPlanRecorded`, `record_plan` at `pkg/permissions/gate.go:176` — is inert
migration residue. Relatedly, `permissions.FromConfig` has **no non-test callers**,
so mast's config-driven permission surface is unused: policy in practice is the
single tri-state `hitl.on_mutation` enum, with no Kubernetes-shaped granularity
(no notion of verb, resource, namespace, or blast radius). That is a real gap, and
it is the same one 2.6 item 2 identifies in core-agent.

### 3.5 Production gaps that block an always-on SRE product

Shipped and genuinely good: rate limiting (`pkg/serverauth/ratelimit.go`,
`pkg/attach/rate_limit.go`), budgets, and an audit log.

Four blockers:

1. **No durable ingress.** `pkg/inject/server.go:15-23` describes itself as a
   "single-session and single-bearer" spike. Lookout's sentinel is at-most-once on
   send (1.7 item 6); if the receiver is also non-durable, a critical finding has
   two independent ways to vanish and no retry on either side.
2. **No incident dedup.** Per-incident session IDs isolate incidents but never
   suppress duplicates — so the sentinel's storm correlation is the only thing
   standing between a 200-pod failure and 200 sessions.
3. ~~**No notification egress of any kind.**~~ **Retracted — this was wrong.** An
   earlier draft said the approval gate "has no doorbell" because `mast/pkg/`
   contains no PagerDuty, Slack or webhook code. It contains none because **egress
   is a deliberate, written-down delegation to switchboard**, and the product
   implements it: `core-sre-agent/internal/notify` is 1,325 lines, wired into the
   monitor at `cmd/sre-monitor/main.go:249`, sending through switchboard's
   outbound ingress to Slack. The draft searched the framework, concluded the
   capability was absent, and missed it sitting in the consumer.

   This also closes the loop on the original three-component framing: **switchboard
   is the notification egress for the SRE line.** It is not the lookout→agent
   transport (Q2), but it is the agent→human path, and it is real.
4. **No tool-output redaction into a verbatim on-disk event log.** For an agent
   whose tools read Kubernetes, that is a secret-exfiltration path. Note the
   contrast: lookout sanitizes at the source, always on, two layers — so
   *lookout's* output is safe, but the GKE MCP's is not.

Multi-tenancy is documented as deferred (`pkg/serverauth/auth.go:42-47`).
`pkg/digest` and `pkg/providers/vertexcache` have zero importers; prompt caching
defaults off in mast and is enabled only by core-sre-agent; the planner returns
`not_implemented` for two of four run shapes.

### 3.6 Q3 verdict

**mast + k8s-lookout is the strongest combination in the org, and it is still not
a product.**

Against Q2: it is clearly ahead. It has the right safety primitive — effect-keyed,
durable, and proven across a real `kill -9` with exactly-once execution — instead
of a self-service plan unlock; it is on the current ADK; it has the only real
pull-side lookout integration; and it has the three-clock scheduler (2.5) that
solves the cost problem both other stacks have. The `$0.0046` bounded pass wired
to `lookout findings diff` is the best idea in the corpus.

*Not, however, an unprecedented one.* An earlier draft called the scheduler
"genuinely novel." The Floor clock is structurally controller-runtime's
`SyncPeriod`, whose documentation gives substantially the same rationale — a
periodic full reconcile exists because edge-triggered watches miss the
steady-state failure. The good judgement here is in recognising that an LLM agent
needs the same thing and in measuring the cost of not having it; that is worth
crediting without overclaiming.

Against k8sgpt: the same asymmetry as Q2, more sharply. k8sgpt cannot do any of
this — no loop, no tools, no approvals, no continuous operation. But it also
cannot lose an incident to a pod restart, because it has no incidents; and its
value does not depend on an approval reaching a human, because it never proposes a
change. **We are being beaten on the axes where having no state is an advantage,
and we win on every axis that requires state — while our state is currently
in-memory on both sides of the wire.**

What would close it, after the retractions above:

1. **Wire the durable session path.** `runner.NewInMemory` is the only runner in
   the repository, and both production binaries route through it, so a pod restart
   loses the incident. This is the one unambiguous gap left in Q3.
2. **Decide the write question explicitly.** Two finished write-path subsystems —
   mast's durable approval gate and core-sre-agent's 2,722-line `internal/kubewrite`
   — are parked behind a read-only posture. Staying read-only is defensible;
   leaving that much built work in limbo without a stated trigger for enabling it
   is not.
3. **Give the permission layer Kubernetes shape.** `permissions.FromConfig` has no
   non-test callers; effective policy is one tri-state enum that cannot express
   "read anything, scale within this namespace, never touch prod."

*An earlier draft added "fix the eval rig" as item 3 and claimed all the fixes
were "wiring, not architecture." Both are withdrawn.* The eval rig is fine (3.4).
And the wiring claim understates the work badly: the durable path in mast cost
roughly 4,852 lines. Item 1 is a project, not an afternoon.
