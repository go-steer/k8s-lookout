# Assessment — LangChain `sre-agent` vs `k8s-lookout`

An architectural read of LangChain's sample Kubernetes SRE agent and a
capability-by-capability comparison against this repo, undertaken because that
project's Python utility layer serves roughly the purpose `k8s-lookout` serves
for `core-agent`: give an LLM a usable view of a cluster.

**Assessed:** the `sre-agent` sample from `langchain-samples`, at the revision
cloned locally on 2026-08-12. Read-only; nothing in that tree was modified.
**Against:** this repo at `main` (v0.17.0, `v0.18.0-dev`).

Citations of the Python side are prefixed `sre-agent/`; citations without a
prefix are repo-relative to `k8s-lookout`. This is an assessment, not a design
note — the recommendations in §6 are proposals, and the ones tagged
**[posture]** are blocked on an open question this repo has already framed for
itself (`docs/fleet-audit-detectors-design.md:194`).

---

## 1. Executive summary

Five findings, in the order they matter.

1. **On incident detection, `k8s-lookout` is a strict superset.** Nearly every
   Python read tool maps to an equal or richer lookout check. Their pod triage
   is one tool returning formatted text; lookout's is eleven typed finding kinds
   with a 5-reason image-pull map and a 6-reason waiting map. Their "traffic
   blackhole" heuristic is three `edge.endpoints_*` kinds here. Their static
   70%-full PVC alert reads the *same* kubelet Summary API that
   `saturation.forecast` samples into a slope→ETA.

2. **The comparison people expect to be interesting — nine subagents vs a Go
   sentinel — is the wrong comparison.** Their subagent fan-out plays no part in
   monitoring. `MonitoringScheduler.__init__` states the agent "is kept for API
   compatibility but is NOT used for scheduled checks"
   (`sre-agent/scheduler.py:831`). Production detection runs through a
   zero-token Python collection loop plus exactly one Haiku call. That loop, not
   the agent graph, is the true analogue of `lookout watch` — and it is a
   30-minute poll where lookout is an informer.

3. **Six capabilities are genuinely absent here and worth taking**, detailed in
   §6.1: a missing-*requests* census (with LimitRange awareness as its
   correctness prerequisite), stuck-Job detection, PV `Released`/`Failed`
   orphans, Helm/release awareness, and CR inventory.

4. **Two further capabilities are posture, not incident** — missing-probe audit
   and a fleet-wide no-PDB/no-HA sweep — and land exactly on the tension
   `docs/fleet-audit-detectors-design.md:89-98` already named. They should not
   ship until Open Question 1 is answered.

5. **Their write surface is a cautionary tale that validates this repo's
   read-only stance rather than challenging it.** `sre-agent/k8s/role-writer.yaml`
   is, despite its filename and despite a comment in the sibling file claiming
   namespace scoping, a *ClusterRole* closing with
   `apiGroups:["*"] resources:["*"] verbs:[create,patch,update,delete]` (L48-50),
   bound cluster-wide. Their own `kubectl_get_rbac_summary` would flag it. Every
   real restriction on that agent is application-level and one approval click deep.

---

## 2. How the LangChain agent is built

### 2.1 Topology

The whole construction is fifteen lines (`sre-agent/agent.py:165-175`):

```python
agent = create_deep_agent(
    name="sre-agent", model=MODEL, tools=tools,
    system_prompt=SYSTEM_PROMPT, subagents=ALL_SUBAGENTS,
    backend=FilesystemBackend(root_dir=".", virtual_mode=True),
    middleware=_build_middleware(), checkpointer=checkpointer, store=store,
)
```

The primitive is `deepagents.create_deep_agent`, not `create_react_agent`.
Orchestrator is Sonnet (`sre-agent/config.py:6`); the eight analyst subagents
are Haiku (`:7`); the one writer subagent is Sonnet.

The orchestrator holds **44 read tools and zero write tools**
(`sre-agent/agent.py:163`) — the primary structural safety property of the
design. Subagents are plain dicts of
`name`/`model`/`description`/`system_prompt`/`tools`
(`sre-agent/subagents/__init__.py:11-21`), each a **separate LLM loop** with a
disjoint tool list; a subagent does not inherit the orchestrator's tools.

Dispatch is deepagents' built-in `task(agent=…, description=…)` tool, driven by
the orchestrator's system prompt (`sre-agent/agent.py:89-97`). **Nothing in
Python ever calls a subagent** — delegation is entirely an LLM decision. Context
in is the `description` string only; context out is free assistant text. **No
subagent declares an output schema.** The single Pydantic contract in the
codebase, `HealthReport` (`sre-agent/schemas.py:62`), belongs to the scheduler
path and is never used by a subagent.

### 2.2 The nine subagents

All in `sre-agent/subagents/`. All triggered the same way — the orchestrator LLM
emits a `task` call.

| Subagent | Tools bound | Purpose |
|---|---|---|
| `pod-inspector` (`pod_inspector.py:34`) | 5 | crashes, OOM, image-pull, pending |
| `scaling-analyzer` (`scaling_analyzer.py:40`) | 7 | HPA pinned at max, min==max, quota headroom |
| `performance-analyzer` (`performance_analyzer.py:44`) | 8 | right-sizing: `top` vs configured requests/limits |
| `log-analyzer` (`log_analyzer.py:41`) | 5 | error mining, `previous=True` after restarts |
| `security-auditor` (`security_auditor.py:34`) | 5 | RBAC, pod security, NetworkPolicy coverage, image tags |
| `reliability-auditor` (`reliability_auditor.py:35`) | 6 | missing PDBs/probes, zero-ready endpoints, SPOFs |
| `job-inspector` (`job_inspector.py:33`) | 5 | failed Jobs, Jobs stuck >1h, lagging CronJobs |
| `config-auditor` (`config_auditor.py:38`) | 6 | missing limits, orphaned PVs, selector mismatch |
| `change-executor` (`change_executor.py:44`) | 6 read + **13 write** | the only writer; BEFORE→EXECUTE→AFTER→REPORT |

The change-executor's prompt is where the operational learning lives: batch bulk
operations into "ONE tool call and ONE approval" (`change_executor.py:57`),
delete the owning CR rather than children that will be recreated (`:72-73`),
never auto-retry a failed change.

### 2.3 Detection: a poll, not a watch

There is no `Watch()`, no informer, and no `resourceVersion` bookkeeping
anywhere in the tree. Every Kubernetes interaction is a one-shot `list_*` or
`read_*`, with a fresh API object constructed per call
(`sre-agent/tools/k8s_client.py:19-59`).

`MonitoringScheduler` (`sre-agent/scheduler.py:829-947`) is a single asyncio
task: sleep 30 s, then loop `_run_check()` / `sleep(interval*60)`. One job — no
per-check-type cadence; every tick runs the same full collection. Default
30 minutes, overridden to 60 in the shipped manifest.

`_collect_cluster_data()` (`:145-363`) gathers, each block independently
try/excepted so no single API failure aborts the tick: nodes, all pods, warning
events (≤20, age- and deleted-pod-filtered at `:230`), HPAs, deployments
(skipping `kube-*`), node and pod metrics, and **PVC usage scraped from the
kubelet Summary API** per node —
`connect_get_node_proxy_with_path(node, "stats/summary")` at `:336`, alerting at
70% (`:368`).

`_classify_pod` (`:75-142`) is five ordered rules over
`UNHEALTHY_WAITING_REASONS` (`:41`) and `FAILURE_TERMINATION_REASONS` (`:48`),
with a 60-minute recency window and a 10-minute startup grace. Notably it
**deliberately refuses to treat `restart_count` as a fault signal** (comment at
`:79-83`); high restarts surface only as "churny" context.

`_format_snapshot` (`:500-642`) renders a capped text snapshot (top-10 CPU pods,
top-10 churny pods, 10 events) and `_analyse_with_haiku` (`:710-793`) makes
**exactly one** Haiku call with `tool_choice` forced to `report_health`. The
module docstring records the motivation: "~20 Sonnet calls per check" → "~1
Haiku call — roughly a 95-99% cost reduction" (`:1-7`).

### 2.4 Dedup and suppression

`sre-agent/monitor_state.py` is the most directly comparable piece of
engineering to `pkg/engine`, and it is all pure functions.

- **Fingerprint** (`:77-93`): `f"{ns}/{kind}/{normalized_name}:{slug(reason)}"`.
- **Name normalization** (`:42-69`) strips generated pod suffixes using the real
  vowel-free Kubernetes alphabet, `_RAND = "[bcdfghjklmnpqrstvwxz2456789]"`
  (`:34`), while deliberately preserving StatefulSet ordinals — `web-0` and
  `web-1` are distinct incidents.
- **Diff** (`diff_report:197-274`) classifies each finding as
  new / escalated / ongoing / suppressed and derives resolved from
  disappearance.
- **Notification gate** (`ReportDiff.should_notify:162-166`): notify only on new
  or escalated. An unchanged ongoing incident produces no repeat message. A full
  digest is forced every 12 checks regardless.
- **Fails open**: the final gate is
  `notify = (not db.available) or diff.should_notify(...) or digest_due`. With
  no Postgres there is no state, so no ability to suppress safely — it alerts
  every tick.

Persistence (`sre-agent/persistence.py:30-96`) is five Postgres tables:
`sessions`, an append-only `hitl_audit`, `finding_state`, `monitor_reports`
(exists because a Slack button `value` caps at 2000 chars, so the Ack button
carries an id), and `monitor_meta` for the digest counter.

### 2.5 Call graph

**Scheduled scan (the production path — no subagents, no deep agent):**

```
_loop → _run_check → _do_check                            [python]
  ├─ _collect_cluster_data()      ~8 API groups, 0 tokens [python]
  ├─ _classify_pod per pod                                [python]
  ├─ _format_snapshot()           capped text             [python]
  ├─ _analyse_with_haiku()        EXACTLY ONE LLM call    [llm]
  ├─ diff_report() → apply_diff() one transaction         [python]
  └─ notifier.send_structured_report()                    [python]
```

No tool in `sre-agent/tools/` is invoked on this path at all; the collector
calls the Kubernetes client directly.

**Slack question (the agentic path):** a regex on the mention text
(`sre-agent/api.py:42-48`, applied at `:610`) routes to either the fast path
above or the orchestrator. Everything from there down — `write_todos`,
`get_cluster_summary`, each `task(...)`, each tool inside each subagent — is
LLM-chosen. The only deterministic decisions on this path are the regex and the
session bookkeeping.

**Write path:** orchestrator delegates to change-executor, which reads BEFORE
state, then calls a write tool; `interrupt_on` fires *before* execution, the
graph raises `__interrupt__` and checkpoints, `api.py:252-312` posts
Approve/Reject buttons to Slack, and a click resumes with
`[{"type":"approve"}] * pending_decisions`. That one-click-clears-the-batch
behaviour is why the change-executor prompt insists on one bulk call per
request.

### 2.6 Safety surface, and where it leaks

The good parts: the orchestrator holds no write tools; subagent tool lists are
disjoint; every mutation therefore funnels through one interrupt-guarded loop.
The checkpointer is load-bearing and the code says so — "without it, a subagent
interrupt has nowhere to persist, and with only an in-memory one a restart
strands every pending approval" (`sre-agent/agent.py:152-154`). Cost controls
are layered: `recursion_limit` 60, 40 model calls per run, 80 tool calls per
run, per-tool caps of 25 on the filesystem tools, prompt caching, model tiering.

The leaks, in descending severity:

1. **RBAC is effectively cluster-admin.** `sre-agent/k8s/clusterrole.yaml:69-71`
   ends in a `["*"]/["*"]` get/list/watch catch-all that subsumes `secrets`.
   `role-writer.yaml` is a ClusterRole, not a Role, closing at L48-50 with a
   wildcard create/patch/update/delete. Both bindings are ClusterRoleBindings.
2. **`POST /api/approve` is unauthenticated** (`sre-agent/api.py:987`) and
   resumes any interrupted run, bypassing the Slack approver allowlist entirely.
   The code concedes the CORS default "blocks the browser path only" (`:847-856`).
3. **`SLACK_APPROVER_IDS` empty means everyone**, and empty is the shipped
   default.
4. **`kubectl_scale_bulk` (`kubernetes_write.py:611`) has no cap and no
   protected-namespace check**, unlike its delete sibling — one approval can
   scale all of `kube-system` to zero, which is availability-equivalent to
   deletion.
5. **`kubectl_get_configmap` (`kubernetes_read.py:515`) dumps values verbatim**
   into model context.
6. Six write tools are exported but bound to no agent; `WRITE_TOOL_NAMES`
   (`tools/__init__.py:160`) computes exactly the map HITL needs but is dead
   code, and the hand-maintained `interrupt_on` list has already drifted from it
   (19 vs 13).
7. The shipped manifests define no `DATABASE_URL`, so `kubectl apply -k k8s/`
   yields in-memory state: approvals lost on restart, no audit, no dedupe, and
   alerts on every tick.

Their real guardrails — the ones worth respecting — are concentrated in
`kubernetes_write.py`: a bulk-delete cap of 25, a protected-namespace check that
**refuses the entire batch** rather than partially applying it ("A
partially-applied destructive batch is worse than none"), and a PVC resize that
refuses shrink, refuses >10× growth, and verifies `allowVolumeExpansion` on the
StorageClass. `tests/test_bulk_delete_guardrails.py` pins all of it, including
that `sre-agent`'s own namespace is protected so the agent cannot delete itself.

---

## 3. Side-by-side inventory

### 3.1 Python surface

56 `@tool` functions — 37 read, 19 write (`sre-agent/tools/__init__.py:95,140`)
— across nine modules:

| Module | Count | Content |
|---|---|---|
| `kubernetes_read.py` | 23 read | namespaces, nodes, pods, describe pod/deployment, logs, deployments, HPA, events, services, ingress, PVC, quotas, cluster summary, top pods/nodes, configmaps, statefulsets, daemonsets, CRDs, custom resources, rollout history |
| `kubernetes_write.py` | 16 write | scale, patch limits, patch HPA, delete pod, resize PVC, apply manifest, cordon/uncordon, rollout restart, patch configmap, rollback, apply/delete CR, delete resource, scale bulk, delete bulk |
| `kubernetes_reliability.py` | 4 read | PDB status **and coverage**, probe audit, endpoints blackholes, single-replica audit |
| `kubernetes_security.py` | 4 read | RBAC summary, pod security audit, NetworkPolicy coverage, image-tag audit |
| `kubernetes_hygiene.py` | 4 read | missing limits, PVs incl. orphans, LimitRanges, selector mismatch |
| `kubernetes_batch.py` | 2 read | Jobs (incl. running >1h), CronJobs (last-success lag >300 s) |
| `helm.py` | 7 read, 3 write | list/values/manifest/versions/repos/updates/history; upgrade/rollback/add-repo |
| `slack.py` | 1 | `send_slack_notification` factory |
| `k8s_client.py` | — | nine client factories, no caching |

Every tool returns a **formatted string**, never structured data.

### 3.2 k8s-lookout surface

Two paths meeting at one shared dedup key, `engine.ScanFingerprint`
(`pkg/engine/fingerprint.go:97`).

**Watch-path** — 14 sources under `pkg/sources/` (k8sevents, degradation,
rollout, workload, saturation, capacity, autoscaling, expiry, objectstate,
quota, tokenburn, ingress, gateway, notifications), a failed source being fatal
rather than a silent coverage gap. Pipeline is filter → dedup → storm-correlate
→ route → inject with out-of-band recovery tracking
(`internal/watch/dispatch.go:32-33`): reason allow-list plus leading-edge
debounce (`engine/filter.go:21-33`), 5-minute rolling window with a 10 000-entry
LRU persisted across restarts (`engine/dedup.go:120-138`), blast-radius storm
collapse (`engine/storm.go:185-216`), severity routing that fails toward paging
(`engine/routing.go:29-34`), and a recovery state machine distinguishing
`recovered` from `object_deleted` so a deletion is never counted as a fix
(`engine/recovery.go:29-39`).

**Read-path** — 24 registered commands, 22 public MCP tools, all generated from
one `Command` metadata struct (`pkg/checks/command.go:163-211`). Finding
families: pods/workloads (`delta/pods.go`), nodes (`delta/nodes.go`), addons
(`delta/system.go:194-214`), budget (`delta/budget.go`), edges
(`state/edges_checks.go`), webhooks (`state/webhooks.go`), volumes
(`state/volumes.go`), drain (`stab/drain.go`), drift (`stab/drift.go`), top
(`top/top.go`), logs (`logs/logs.go`), health (`health/health.go`), cloud
(`cloudcheck/`), perf packs (`perf/packs.go`), net probe (`netprobe/`).

**Write audit, definitive.** Grepping non-test Go under `pkg/`, `internal/`, and
`cmd/` for mutating verbs on a typed clientset or the dynamic client
(`(CoreV1|AppsV1|BatchV1|PolicyV1|NetworkingV1|RbacV1|AutoscalingV[12]|StorageV1|AuthorizationV1)().…(Create|Update|Patch|Delete|DeleteCollection|Apply|UpdateStatus)`)
yields exactly **two** hits, and the dynamic-client search yields none:

| Hit | Verdict |
|---|---|
| `pkg/sources/rbac.go:137` `SelfSubjectAccessReviews().Create` | not a mutation — a read-only authz query |
| `pkg/checks/state/cluster.go:250` `SelfSubjectAccessReviews().Create` | same, used by `Preflight()` for least-privilege partial loads |

Three further constructs look mutating to a naive grep and are not:
`pkg/checks/spec_render.go:262,264` renders a probe's `exec.command` *spec
field* (nothing is executed); `pkg/sources/objectstate/objectstate.go:834,1286`
call `Delete` on the in-process §7.4 pod/node clearance state machines
(`:371-378`), not on the API; and `pkg/checks/triage/status.go:63-70` is the one
command with `Writes: true`, which upserts into lookout's own SQLite, never the
cluster, and advertises `ReadOnlyHint:false` so MCP clients cannot auto-approve
it.

No exec, no port-forward, no ephemeral containers. `net probe` sends packets but
touches no Kubernetes API and spawns no pod (`netprobe/netprobe.go:15-35`).

---

## 4. Head-to-head

✅ covered · 🟡 partial · ❌ absent

| Python capability | k8s-lookout | |
|---|---|---|
| pod health / describe | `pod.crashloop\|imagepull\|oomkilled\|restarts\|pending\|failed\|notready\|waiting` (`delta/pods.go:76-371`), `bundle`, `triage spec` | ✅ superset |
| pod logs (+`previous`) | `triage logs` with Drain templating, stack-trace collapse, probe-noise stripping | ✅ superset |
| nodes | `node.notready\|pressure\|condition\|cordoned\|preempt` (`delta/nodes.go`) | ✅ superset |
| events | `triage events` with owner-tree walk + reason collapse | ✅ |
| top pods/nodes | `top.saturation:454`, `top.node:490` | ✅ |
| deployments / rollout history | `workload.stalled:273`, `workload.rollout:291`, `triage changes` | ✅ superset |
| services + selector mismatch | `edge.selector_empty:504`, `edge.selector_unready:534` | ✅ |
| endpoints "blackholes" | `edge.endpoints_missing:576`, `_unready:649`, `_orphaned:604` | ✅ superset |
| ingress | `edge.backend_missing:726`, `edge.cert_*:838-895`, `pkg/sources/ingress` | ✅ |
| PVC | `pvc.pending`/`pvc.lost` (`health.go:512-548`), `state volumes` | ✅ |
| PVC **usage %** via kubelet Summary API | `saturation.forecast` PVC dimension, same API, slope→ETA (`sources/saturation/`) | ✅ superset |
| quotas | `quota.near`/`quota.exhausted`, `quota.pressure` | ✅ |
| HPA | `pkg/sources/autoscaling`, `event.hpa_thrash` (`events/hpa.go:188`) | ✅ |
| PDB *status* | `pdb.gridlocked`, `drain.pdb_gridlock:333` | ✅ |
| PDB *coverage* ("no PDB") | — | ❌ posture |
| single-replica audit | `drain.singleton:390`, but node-drain-scoped only | 🟡 |
| Jobs / CronJobs | `workload.job_failed`, `workload.cron_missed`, `job.failed` | ✅ — except "running >1h" |
| statefulsets / daemonsets | delta + `addon.degraded:194-214` | ✅ |
| configmap **keys** | `triage spec` renders keys + byte sizes | ✅ |
| configmap **values** | deliberately impossible (`emit/sanitize.go`) | ❌ by design |
| CRDs / custom resources | `triage spec` dynamic + discovery fallback | 🟡 no CR listing |
| cluster summary | `health` 10-category verdict | ✅ superset |
| namespace inventory | — | 🟡 inherent to zero-nominal-state |
| **probe audit** | probes rendered, never judged | ❌ posture |
| **missing limits** | `top.unlimited:529`, `top.unlimited_container:558` — limits only | 🟡 **requests never checked** |
| **LimitRanges** | — | ❌ |
| **PV Released/Failed orphans** | — (cloud `orphan.disk` is a GCE-disk check) | ❌ |
| pod security audit | — `securityContext` never read anywhere in `pkg/` | ❌ scope |
| RBAC summary | RBAC objects listed; only `edge.rbac_dangling:955` | ❌ scope |
| NetworkPolicy posture | renderable + netprobe input only | ❌ scope |
| image-tag audit | images are opaque strings | ❌ scope |
| **Helm (10 tools)** | zero awareness — only `"helm-legacy"` as a test fixture string | ❌ |
| 19 write tools | none | ❌ by design |
| Slack egress | daemon's job | ❌ by design |
| new/escalated/ongoing/resolved diff | `engine.DedupCache` + `engine/recovery.go:135-149` | ✅ superset |
| `ack_until` suppression | §9.4 triage-status records incl. `severity_override` | ✅ |
| `HealthReport` schema | `emit.Finding` + frozen inject schema v1 | ✅ |

**Lookout capabilities with no Python analogue at all:** resident informer
sentinel with push injection; frozen 48-kind schema with class-level
fingerprints; storm/blast-radius correlation; cost-aware dedup with restart
persistence; the recovery state machine; topology graph with LKGH history and
`--at` replay; slope→ETA forecasting and expiry countdowns; GitOps drift from
`managedFields`; admission-webhook failure analysis; volume attach/zone-conflict
analysis; drain-blocker analysis; control-plane perf packs; cloud stockout /
orphans / quota / IP-space sensors; active network probing without spawning a
pod; log distillation; the universal sanitizer as an enforced invariant; the
composed `bundle` incident call; SSAR preflight for least-privilege partial
loads; the GCP-free default build enforced by an `nm` symbol-table test;
metadata-driven generation of help + MCP schema + skill docs; the token-burn
sensor; and agent-writable triage status feeding sentinel routing.

---

## 5. Architectural difference

| Axis | `sre-agent` | `k8s-lookout` |
|---|---|---|
| Trigger | pull only: LLM tool choice, or a 30-min tick | push sentinel (informers, per-source cadences 30 s–1 h) **plus** an on-demand read path |
| Freshness | bounded by the tick; anything that starts and self-heals within it is invisible | event-driven — the sentinel sees the transition |
| API access | fresh client per tool call; an 8-tool turn is 8+ round trips | informer cache on the watch path; one paged `LoadCluster` feeding five sections on the read path |
| Who decides what matters | the LLM, at inference time, from a 56-tool menu; then a second LLM call classifies | deterministic Go; the agent receives pre-computed, fingerprinted, deduped signal |
| Output | prose blobs, plus one Pydantic report | machine records — logfmt/JSON, one per line, terminating summary, typed exit codes, glossary-enforced detail keys |
| Nominal state | always narrated | zero nominal state, but silence is never ambiguous (`unavailable`, `skipped=`) |
| Dedup key | `ns/kind/name:reason` string, diffed report-to-report | SHA-256 over (kind, reason-class, object-class, zone) — object identity deliberately excluded |
| Cost control | report diffing, digest cadence, ack window, one Haiku call per tick | eight layered mechanisms incl. debounce, storm collapse, enrichment caps, token budget |
| Mutation | 19 write tools behind one approval click; RBAC is cluster-admin | one `Writes: true` command, and it writes SQLite |
| Secrets | ConfigMap values dumped; cluster-wide Secret read grant | universal sanitizer; keys and byte sizes only |
| Temporal reasoning | point-in-time snapshot, plus report-to-report diff | LKGH history, `--at` replay, forecasts, countdowns, delta log |

**The trade, stated plainly.** Their design's strength is breadth and
composability: it can answer questions nobody anticipated — including posture
questions — and it can act. Its costs are latency proportional to tool count,
non-determinism (two runs may inspect different things), token burn proportional
to cluster size, blindness between ticks, and an LLM holding
cluster-admin-equivalent credentials. Lookout buys bounded cost, reproducible
output, sub-tick detection, cross-signal correlation, and a credential blast
radius small enough to audit — at the price of only answering questions someone
anticipated and encoded, and never being able to fix anything.

One point of **convergent design** worth recording: both projects independently
concluded that raw restart count is not a fault signal. Their `_classify_pod`
says so in a comment (`sre-agent/scheduler.py:79-83`); lookout gates
`pod.restarts` behind an explicit `--restarts` threshold and debounces
crash-loops at `--backoff-min-count`. No change needed on either side — but the
agreement is evidence the heuristic is right.

---

## 6. Recommendations

### 6.1 Adopt — [incident], fits the current charter

Ordered by value-to-effort.

**R1. Missing-*requests* census.** `top.unlimited` and
`top.unlimited_container` (`top/top.go:529,558`) census *limits* only. The sole
non-test occurrence of `Requests` anywhere under `pkg/checks/` is the unrelated
`APFRequestsRejected` reason string (`perf/packs.go:135`) — no check reads
`Resources.Requests`. A container with no CPU or
memory **request** is the direct cause of `FailedScheduling` chaos,
noisy-neighbour eviction, and bad bin-packing — all of which lookout already
reports as symptoms without ever naming the cause. Add `top.unrequested` /
`top.unrequested_container` alongside the existing census; the resource walk at
`sources/saturation/fetchers.go:79-82` already reads `Resources.Limits` and can
read `Requests` in the same pass. Smallest change here, largest explanatory
payoff. Listed as `no-requests` in
`docs/fleet-audit-detectors-design.md:83-87`.

**R2. LimitRange awareness — ship with R1, not after.** Without it, any
"no limits"/"no requests" finding is a false positive in a namespace with a
defaulting LimitRange, which makes R1 a noise generator rather than a signal.
Load LimitRanges in the existing `state/cluster.go:67-88` pass and
suppress-or-annotate `top.unlimited*` accordingly. This is a correctness
prerequisite, not an enhancement.

**R3. Stuck-Job detection.** `sre-agent/tools/kubernetes_batch.py:72` flags Jobs
*running* longer than an hour. Lookout has `workload.job_failed` and
`workload.cron_missed` (`pkg/sources/workload/workload.go:98-99`) but nothing
for the Job that never fails and never finishes — the classic silent batch hang.
Extend `pkg/sources/workload/` with a duration threshold against
`activeDeadlineSeconds` or an expected runtime; §7.4 clearance semantics are
natural here since completion clears the signal.

**R4. PV `Released`/`Failed` orphans.** Lookout's only orphan checks are
cloud-side (`orphan.disk`, `orphan.lb`) plus the correctness join
`volume.orphaned_attachment:487`. A `Released` PV with a `Retain` policy is both
a cost leak and a re-bind failure waiting to happen. The PV list is already
loaded in `pkg/checks/state/volumes.go`. This follows the `cloud orphans`
cost-sweep precedent exactly, on the Kubernetes side. Named `orphan-pv` /
`unconsumed-pvc` in `docs/fleet-audit-detectors-design.md:83-87`.

**R5. Helm/release awareness — two cheap slices only.** Ten tools on their side,
zero here; the only occurrence of the string in this repo is `"helm-legacy"` as
a fake managedFields manager in `stab/drift_test.go`. Helm-managed clusters are
the majority case, and `stab drift` currently treats a Helm manager as just
another opaque manager string. Take: (a) surface
`meta.helm.sh/release-name` / `app.kubernetes.io/managed-by` as an object
attribute in `pkg/graph/derive.go` so every finding can carry a release label
and `triage changes` can say "this broke at release rev 42"; (b) teach
`stab/drift.go` that a Helm manager means "expected owner". Do **not** take
chart resolution, values diffing, or upgrade-availability checks — that is a
different product and pulls a `helm` dependency into a binary that currently has
none.

**R6. CR inventory.** `kubectl_get_custom_resources` lists any CR by GVR;
`triage spec` only renders one object via the dynamic + discovery path
(`spec.go:365,401`). Operator-managed clusters keep their real state in CRs.
Extend the dynamic path, or add condition-based health reading for CRs carrying
standard `status.conditions`.

**R7 (marginal). Narrow inventory read.** Agents occasionally need "what exists"
before "what's broken", and zero-nominal-state means they cannot get it here. If
taken, a `k8s_inventory` MCP tool must emit records with no `Fingerprint` — the
schema already reserves that shape for scorecard/inventory lines
(`pkg/emit/finding.go:68-75`). Low value; listed for completeness.

### 6.2 Defer — [posture], blocked on Open Question 1

`docs/fleet-audit-detectors-design.md:89-98` already names the exact tension
these sit on: lookout finds "what is *abnormal now*", while audit checks find
"the *absence of a safety net* around a workload that is currently healthy…
posture, not incident". Open Question 1 (`:194`) leaves unresolved whether
posture belongs in lookout proper or a sibling `audit` binary. **Neither item
below should ship before that is answered**, and when it is, the mapping at
`:65-87` is the design of record.

**R8. Missing-probe audit.** Lookout renders probes
(`spec_render.go:224-225`) but never judges them. A workload with no readiness
probe is the root cause of `edge.endpoints_unready` false negatives and of
rollouts that "succeed" into a black hole. Would fit as `stab probes`.

**R9. Fleet-wide no-PDB / no-HA sweep.** The highest-signal availability posture
check there is, and mostly a re-scoping of code that already exists:
`drain.singleton:390` and `drain.pdb_gridlock:333` compute exactly these facts
but only under `stab drain`, node-scoped. Cheapest path is allowing `-A`
workload scope on the existing drain logic.

### 6.3 Reject, with reasons

| Capability | Why not |
|---|---|
| All 19 mutating tools | `deploy/12-clusterrole-watcher.yaml:139-143` — "the sentinel is read-only"; `DESIGN.md:609` — "No raw write authority over the cluster"; §1's "deterministic read-path, managed write-path". Mutation belongs behind core-agent's permission gate. Adopting even one write breaks the property that makes this ClusterRole auditable, and §1 finding 5 is the counterexample. |
| Pod-security and image-tag audits | `DESIGN.md` §5 cut `exec-spy` as "security-detection scope creep… Deferred indefinitely". Security posture is a compliance product (Kyverno, Gatekeeper, PSA), not an incident product. There is no `securityContext` read anywhere in `pkg/`; keep it that way. |
| RBAC rule analysis | Same reasoning. RBAC objects are already listed (`state/cluster.go:67-88`) with reference integrity only (`edge.rbac_dangling:955`). Inspecting `rules[]` for wildcards turns a triage tool into a half-hearted RBAC scanner. |
| NetworkPolicy posture | Same category. NetworkPolicy stays a `triage spec` renderable and a netprobe input. |
| ConfigMap value dumping | Directly violates the no-secrets invariant (`emit/sanitize.go:68,346,377`; `state/edges_checks.go:39-41`). Non-negotiable. |
| Generic hygiene sweeps (unused ConfigMaps, evicted-pod cleanup, zombie ReplicaSets) | `DESIGN.md` §5 cut `stale-object-sweeper` as "a false-positive machine, and housekeeping rather than triage". The `cloud orphans` sweep survives only because cloud resources have unambiguous billing-active/unattached state — which is also why R4 qualifies and these do not. |
| Slack/chat egress | Wrong layer. Lookout's sink is the core-agent daemon; notification routing is the daemon's job, and `AGENTS.md` keeps lookout notification-agnostic. |

### 6.4 Documentation correction (applied)

`docs/roadmap-post-m5-sensors.md` was **stale as a gap list** — it named
Job/CronJob (B.1), HPA saturation (B.2), cluster bin-packing (B.3), node
pressure (B.4), and GCLB/Ingress (C.5) as gaps, all of which had shipped. It was
cited as a live gap list during this assessment and produced wrong answers,
which is what surfaced the problem.

It has been refreshed in the same change that added this document: every Tier
A–C item now carries ✅ shipped / ◐ partial / ○ open, verified against the tree
on 2026-08-12. Ten of twenty had shipped. The residual halves worth knowing
about are A.4's zone stamping, C.2's audit-log preemption attribution (now
unblocked, since A.1 landed the capability it waited on), and C.5's LB metrics.

Its **Tier D standing "no"** remains current policy and is consistent with §6.3
above — with one clarification added there: a Gateway API *source* shipped
(#168), but the Tier D entry is about mesh/Gateway-API **graph kinds**, and
`pkg/graph` still has no gateway node or edge type. Watching those objects and
modelling them in the topology graph are separate decisions.

### 6.5 Suggested sequencing

1. **R1 + R2 together**, as one change — highest value, no charter decision
   needed, and R2 is what keeps R1 from being noise.
2. **R3**, then **R4** — both are self-contained new findings on data already
   loaded.
3. **R5(a)** — the release label on graph objects — before R5(b); (a) is useful
   on its own and (b) depends on it.
4. **R6**, if operator-managed clusters are in the target profile.
5. Answer Open Question 1. If posture lands in scope, **R9 before R8** (R9 is a
   re-scope, R8 is net-new logic).
