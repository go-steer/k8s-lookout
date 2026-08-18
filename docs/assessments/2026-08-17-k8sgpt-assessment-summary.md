# Where we stand against k8sgpt — executive summary

**Date:** 2026-08-18 · **Subjects:** k8sgpt @ `b4a86de` (0.4.36 era) · k8s-lookout
`v0.22.0-dev` · core-agent @ HEAD (ADK v1.2.0) · mast v0.4.0 (ADK v2.2.0) ·
core-sre-agent @ HEAD

**Detail documents:**
- [Q1 — detection and analysis: k8s-lookout vs k8sgpt](2026-08-17-q1-detection-vs-k8sgpt.md)
- [Q2 — core-agent + k8s-lookout](2026-08-17-q2-core-agent-lookout.md)
- [Q3 — mast v0.4.0 + k8s-lookout](2026-08-17-q3-mast-lookout.md)

**Follow-up (2026-08-18):**
[The k8sgpt gap list, and four product decisions](2026-08-18-gap-list-and-decisions.md)
— the itemised list of what k8sgpt detects and we do not, a design for the
missing `lookout scan` entry point, and decisions on mast harness-vs-library,
core-agent, an LLM in lookout, and consuming k8sgpt over MCP. Its §7 merges the
fix table below with the new items.

[What k8sgpt does well — a yardstick for measuring ourselves](2026-08-18-k8sgpt-yardstick.md)
— the 15 practices behind their maturity, scored against where we stand today:
zero-arg time-to-value, distribution, contributor onramp, findings-as-Prometheus,
false-positive suppression, governance and release hygiene. Includes the six axes
where we already beat them (signing, kind CI, sanitization, finding identity).

Method: six parallel code-level inventories, then three adversarial passes — a
fact-checker with no brief, and two red teams briefed to argue the opposing case.
Every verdict-level attack was independently re-verified against the source before
being accepted; two were rejected on re-verification. Claims carry `file:line`.

---

## The one-paragraph answer

**We have the better architecture and the worse product.** k8sgpt is a stateless
snapshot scanner with an LLM narration layer; it cannot watch, cannot remember,
cannot act, and structurally cannot grow into any of those from where it stands.
Everything we have built — continuous observation, verified recovery, storm
correlation, an agent that proposes fixes — is a category it does not compete in.
But on the question a first-time user actually asks, *"what is wrong with my
cluster right now,"* k8sgpt today detects more conditions than we do, reaches them
with zero arguments and zero setup, and does it on infrastructure that has three
years and 144 contributors behind it. Our always-on component, meanwhile, cannot
be health-checked, re-pages its whole backlog on restart, and runs two copies of
itself during an upgrade.

**The gap is not architectural. It is roughly six weeks of wiring and hardening,
and it is all on our list below.**

---

## Verdicts

| | Verdict |
|---|---|
| **Q1** — lookout vs k8sgpt on detection | **Split.** We lose on breadth, reachability and maturity; we win, uncontested, on continuous operation and machine-consumption design. |
| **Q2** — core-agent + lookout | **Superseded.** Strong runtime, but the SRE line already migrated off it, its shipped k8s recipe disables the permissions gate, and it has never completed a fix→verify loop. |
| **Q3** — mast v0.4.0 + lookout | **Best of the three, still not a product.** Right primitives, properly tested; but the product runs on in-memory sessions in both binaries, so a pod restart loses the incident. |

## The five things worth knowing

**1. Our detection breadth claim was wrong, twice.** Counted on a consistent
basis — after removing ~33 of our 113 declared kinds that are inventory rows,
scorecard lines, tool self-reports and read-backs of our own database — it is
**~90 conditions for k8sgpt against ~70 for us**, and roughly **~90 vs ~48** for a
user without GCP, because our default published image ships with no cloud provider
compiled in. We should stop making breadth claims.

**2. `k8sgpt analyze` with no arguments beats `lookout` with no arguments.** They
run 14 analyzers cluster-wide including cross-resource Ingress, Service,
StatefulSet and webhook traversal. We have no run-everything entry point, and five
commands — including the three we lead with — refuse to run until you name the
broken workload. **Our graph is a second-call advantage, not a detection
advantage.**

**3. There is a real bug in the sentinel, and it is in the worst possible place.**
`pkg/sources/k8sevents` has no arm-after-cache-sync gate; eight of its nine
sibling sources do. Its handler emits unconditionally and is registered before
`WaitForCacheSync`, so client-go's initial LIST replays the API server's entire
event TTL window as fresh signals. With the shipped manifest's `emptyDir` and no
`--dedup-persist`, a restart re-pages everything *and* loses the session bindings
needed to close any of it. `k8s-event` is the source that opens every incident.

**4. Neither stack writes to a cluster in any shipped configuration — and that is
the safety story.** core-agent's plan-first gate is unlocked by the model calling
`record_plan` with any non-empty string, nothing compares later mutations to the
plan, and the shipped GKE recipe sets `permissions.mode: yolo`, asserted by its own
test, because an unattended daemon has nobody to prompt. mast fixes this properly
with a durable, effect-keyed write gate proven across a real `kill -9` with
exactly-once execution. But the SRE product imports none of it, because **it is
deliberately read-only**: `Config.Writes` is never granted, and a whole package
exists to assert that no write tools are built. So core-agent is `yolo` over a
read-only toolset and mast withholds the write tools entirely — different
philosophies, identical deployed blast radius of zero. What's actually parked is
~4,000 lines of finished write-path work with no stated trigger for enabling it.

**5. The cheapest big win is already built and nearly unused.** A bounded one-shot
pass costs **$0.0046/fixture against $0.2374 full-agentic** — $1.32/day versus
~$683/day. `core-sre-agent/internal/scheduler` already implements the
cheap-pass-first architecture, in a well-reasoned three-clock form keyed on
`lookout findings diff`. It is the best idea in the corpus.

## What to fix, in order

| # | Fix | Where | Why now |
|---|---|---|---|
| 1 | Add the arm gate to `k8s-events` | `pkg/sources/k8sevents` | One-source oversight, outsized blast radius; breaks the core "we don't page on cold start" claim |
| 2 | Real `/healthz` | `internal/watch/metrics.go:356-363` | Static 200 on both probes; a wedged sentinel is never restarted |
| 3 | Wire the durable session path | core-sre-agent | `runner.NewInMemory` is the only runner, and both production binaries route through it — a pod restart loses the incident |
| 4 | Leader election, or `strategy` + `replicas` | `deploy/51-deployment-watcher.yaml` | Default RollingUpdate runs two uncoordinated sentinels per image bump |
| 5 | Gateway API in the CLI | `pkg/checks/` | The sentinel watches it; the CLI doesn't, and k8sgpt beats us there outright |
| 6 | `stab drift` majority-manager threshold | `pkg/checks/stab/drift.go:158-166` | No minimum share — flags every object of the losing manager on a mixed Helm+Argo cluster |
| 7 | Kubernetes-shaped permission policy | mast `pkg/permissions` | `FromConfig` has no non-test callers; policy is one tri-state enum that cannot express "scale in staging, never touch prod" |
| 8 | State the trigger for enabling writes | mast + core-sre-agent | Two finished write subsystems (~4,000 lines) are parked behind a read-only posture with no stated criteria for turning them on |

Items 1, 2, 4, 5 and 6 are lookout; 3, 7 and 8 are the agent side. Only item 3 is
large — the durable path cost mast roughly 4,852 lines, so this is a project, not
an afternoon.

## What not to say externally

- **Do not claim detection breadth.** See above.
- **Do not cite k8sgpt's empty `ADOPTERS.md`.** It was added 2026-04-24 as part of
  a CNCF incubation application and is a template, not a register that came up
  empty. Any reader who checks will discount everything else we said.
- **Do not claim their MCP server is insecure.** It defaults to stdio with no
  socket — safer than our loopback TCP. The plaintext/no-auth criticism is valid
  but attaches to `k8sgpt serve`'s gRPC API on 8080.
- **Do not use their Secret handling against them** without noting that our
  sentinel's ClusterRole grants `secrets: list` cluster-wide, 24/7, by default,
  while theirs requires a human to opt in with their own kubeconfig.
- **Do not claim "three years vs four weeks" as the maturity gap.** It is 144
  contributors and 116 releases against 165 commits by one author. Framing it as
  age invites the correction.

## A note on this document's own reliability

Across three adversarial passes, **eleven claims in the drafts were wrong**, and
they did not all lean the same way:

*Errors that flattered us* (Q1, mostly): a competitor undercounted by a third; our
own detection count inflated by a third; three documented in-repo tradeoffs
presented as newly discovered defects; a claim that core-agent ignores lookout's
read path when it ships a bundle on every critical inject; an integration fix
estimated at "an `mcp.json` change" when it actually requires dismantling a
security boundary; a best-case eval run quoted as typical.

*Errors that maligned our own work* (Q3, mostly): we understated our CI and our
Gateway API coverage; we condemned a mast eval rig we had not found (§3.4,
retracted in full); we declared a notification path missing that ships in the
consumer; we called the watchdog single-signal when it has three and was
purpose-built for the evasion we described.

**The single most instructive one:** §2.5 asserted that neither runtime routes a
cheap deterministic pass before escalating — while quoting cost figures from the
very repository whose scheduler does exactly that, in the same section.

Two adversary claims were themselves rejected on re-verification, so the red teams
are not authoritative either. Everything above was checked against source before
being accepted.

Corrections are preserved inline in the detail documents rather than silently
applied. The pattern of how a self-assessment drifts — and it drifts in *both*
directions, not just toward flattery — is more useful than the corrected numbers.
