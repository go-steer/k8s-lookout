# `k8s-lookout` — Data-Plane Intelligence for `core-agent`

**Document Version:** 3.5 (supersedes the v2.0 spec, preserved at
`docs/appendix-v2-dataplane-intelligence.md`;
v3.1 added §6, the topology index, absorbing the `k8s-graph` / `kube-distill`
proposal; v3.2 added `lookout health` (§5) and triage-status records (§9.4),
absorbing the cluster-health-checker / fleet-assessment-store proposal;
v3.3 added §4.4, agent education & discovery; v3.4 settled the repo name as
`k8s-lookout` and added the cloud-provider boundary in §2; v3.5 is the move
into this repository)

**Status:** design (2026-07-24), authored in `core-agent` and moved here at
repo bootstrap. Implementation starts at §14 M0 (the `k8s-event-watcher` code
move from `core-agent` is the part of M0 still outstanding).

**Objective:** Give a `core-agent`-driven troubleshooting agent deterministic,
token-dense eyes on Kubernetes/GKE data planes — both **read-path** (one-shot
diagnostic reads invoked mid-investigation) and **watch-path** (resident
subscribers that detect issues before or as they happen and open agent sessions
with warm context). Not predictive/ML: every leading indicator here is
deterministic arithmetic (state transitions, slopes, countdowns).

---

## 1. Mission & Principles

Revised from v2. The v2 doc assumed a cheap-intent-router world ("format for
Gemini Flash", "the LLM must never perform arithmetic"). With a capable model
driving, the rationale changes and so does what we build:

1. **Token density.** Not because the model can't read 150k tokens of logs, but
   because those tokens cost money, evict other context, and slow the loop.
   Deterministic pre-compression (dedup by template, strip nominal state) sits
   between every high-volume source and the context window.
2. **Determinism.** A compiled graph traversal doesn't hallucinate an
   EndpointSlice. Read-path checks are exact; the model reasons over their
   output, never re-derives it.
3. **Fewer round trips.** A powerful model reasons better over one dense,
   correlated payload than over five sequential tool calls it stitches together
   across turns. This pushes toward *fewer, wider* tools — the opposite of
   v2's 25-binary matrix — and toward pre-warmed sessions (§7.6). The K8s API
   is flat and resource-centric; reconstructing "what relates to this pod"
   costs an agent 10–15 round trips unless something maintains the relations
   for it — that something is the topology index (§6).
4. **Leading indicators over autopsies.** A k8s Event is the autopsy. The
   signals that run ahead of Events are state transitions, trends (slope →
   ETA), and countdowns (expiries, quota headroom). The watch-path exists to
   surface these before the human operator knows.
5. **Zero nominal state — but never ambiguous silence.** Healthy resources are
   omitted from output, and every invocation ends with an explicit summary line
   (`scanned=412 findings=0 elapsed=1.2s`) so an agent can distinguish
   "cluster healthy" from "wrong flag / broken tool".
6. **Deterministic read-path, managed write-path.** Tools inspect and diagnose.
   The two *cluster-facing* write actions — GitOps PRs for cluster changes, and
   `QuotaPreference` requests for quota increases (§10.3) — route through the
   daemon's permission gate, never raw write authority. A third write exists and
   is **not** cluster-facing and **not** yet daemon-gated: `lookout triage
   status` (§9.4) upserts a triage-status record into the sentinel's `--store`
   SQLite file directly, with `--store` access control as its sole authorization
   until core-agent's memory surface ships the gate
   (docs/triage-status-write-design.md, "Out of scope"). It cannot change the
   cluster, but it can steer the sentinel's own routing — see §7.8.
7. **Closed loops.** An agent that acts must learn from the world whether the
   action worked. The sentinel that watched a symptom appear also watches it
   clear and injects the resolution (§7.4). Every incident therefore produces a
   trajectory with a verified outcome (§9.3).
8. **No secrets in context.** Every payload that can carry a spec passes
   through the sanitizer (§6.5): secret values, `env` credentials, and
   system metadata (`managedFields`, `resourceVersion`, `uid`) are
   stripped/masked before anything reaches stdout, an MCP response, or an
   inject. Defense in depth, applied in `pkg/emit`, not per-tool.

Token-reduction figures from v2 are treated as directional motivation, not
acceptance criteria.

---

## 2. Repository

**Name:** `go-steer/k8s-lookout`. A lookout is the crew member posted to spot
trouble before it reaches the ship — precisely the watch-path mission, and it
fits the `go-steer` nautical theme. The `k8s-` prefix scopes the repo honestly
(every tool here presumes a Kubernetes data plane) and establishes the
`*-lookout` pattern for future domains (a hypothetical `gcp-lookout` for
non-k8s cloud estates, etc.) sharing the same signal-engine philosophy. The
**binary stays `lookout`** — short commands matter for agents and operators
alike, and only one lookout is ever installed in a given environment.
(Rejected: `dataplane` — collides with GKE Dataplane V2 in exactly the
conversations this tool lives in; `sentinel` — Redis Sentinel et al.; `dpi` as
a repo name — deep-packet-inspection connotation.)

**One repo, not three.** A parallel proposal split this into sibling repos
(`k8s-graph` for the topology engine, `kube-distill` for distillation). We
reject the split: the graph is substrate consumed by half the tool matrix
(§6.4), and "distillation" *is* this suite — log clustering is `triage logs`,
timeline synthesis is `triage events`/`triage changes`, `TriagePod` is
`bundle`, MCP exposure is `lookout mcp`. Separate repos would reintroduce
exactly the contract-drift problem the multicall consolidation solves. (That
proposal also referenced ecosystem components `mast` and `vessel`. `mast` is
real but pre-fork — a planned lean fork of `core-agent` for unattended,
library-embedded workloads; it shares `core-agent`'s daemon surface, so
lookout's contract (§3) is unchanged whichever runtime a deployment uses.
`vessel` does not exist; the embedded store is SQLite (§9).)

**Module:** `github.com/go-steer/k8s-lookout`

**Layout:**

```
k8s-lookout/
├── cmd/lookout/         # single multicall binary (see §4)
├── pkg/
│   ├── kube/            # client bootstrap (extracted from k8s-event-watcher buildKubeClient)
│   ├── emit/            # logfmt/json envelope + summary line + sanitizer (§4.2, §6.5)
│   ├── graph/           # in-memory topology index + history (§6)
│   ├── engine/          # filter → dedup → storm → severity pipeline (extracted from watcher)
│   ├── sources/         # signal sources (§7.2), one package per source
│   ├── checks/          # read-path check implementations, shared by CLI / MCP / enrichment
│   ├── cloud/           # Provider interface + pkg/cloud/gke implementation (see below)
│   ├── inject/          # daemon HTTP client (extracted from injector.go)
│   └── store/           # sentinel-local SQLite: occurrences + graph history (§9.1)
├── skills/              # workflow skills + per-symptom playbooks (§4.4); version with output formats
├── deploy/              # k8s manifests + RBAC per scope tier (§11)
├── docs/
│   └── DESIGN.md        # this document
└── go.mod
```

**Dependencies:** `k8s.io/client-go`, GCP SDKs (Logging, Monitoring, Compute,
Cloud Quotas), and `github.com/go-steer/core-agent/v2` as an ordinary library
dependency (telemetry setup; inject payload types if we promote them to a
shared contract package). `core-agent` never imports `lookout`.

**What moves here from `core-agent`:** `cmd/k8s-event-watcher` in its entirety
(it becomes `lookout watch`, §7), plus `docs/dataplane-intelligence-tools.md`
(as historical appendix) and this document. After the move, `core-agent` drops
all `k8s.io/*` dependencies from its `go.mod`.

**Why a separate repo** (recap of the settled decision):

- `core-agent`'s DESIGN.md and the k8s-event-agent design both state the
  policy: the daemon stays k8s-agnostic; no k8s bloat in core.
- Release cadence: this suite chases k8s minors and GCP SDK bumps; `core-agent`
  has GA discipline.
- Playbooks/skills must version in lockstep with tool flags and output formats,
  not with the daemon — they ship in this repo's `skills/`.

### Portability: the cloud-provider boundary

~80% of the suite is pure client-go and works on **any conformant Kubernetes
cluster** — the entire `triage` group, `state edges|webhooks|volumes`,
`stab drift|drain`, `bundle`, `health`, `net probe`, the topology index, and
the sentinel sources `k8s-events`, `object-state`, `rollout`, `saturation`
(metrics.k8s.io + kubelet stats), `degradation`, `expiry`, `ingress` (pure
client-go — it reads the Events ingress-gce writes), and `token-burn`.
The GKE/GCP surface is small and enumerable: the `cloud` command group,
`state wi`, the `perf probe` backend, the `quota` source, and one of the three
cluster-autoscaler sub-sources (§10.1).

We keep one repo (a `gke-lookout` split would fork the engine, the schema, and
the skills for ~20% of the surface) and enforce the boundary architecturally:

- **`pkg/checks` and `pkg/sources` never import GCP SDKs.** Cloud-touching
  functionality goes through a `pkg/cloud.Provider` interface (capacity
  explanations, quota inventory, orphan sweeps, metrics queries, workload
  identity verification); `pkg/cloud/gke` is the first implementation.
- **No provider configured → explicit, not broken.** Cloud-dependent commands
  and sources report `unavailable reason="no cloud provider configured"` in
  their summary line / startup check, consistent with the fail-loudly
  principle (§11). `lookout mcp` and `--help` omit or mark them. Everything
  else runs untouched — a vanilla-k8s user gets a fully functional lookout.
- **Graceful degradation inside features:** the capacity source runs on the
  two upstream-portable CA sub-sources (Events + status ConfigMap) everywhere;
  the GKE visibility logs sub-source lights up only with the `gke` provider
  (§10.1). `perf probe` packs are metrics queries behind the provider's
  metrics backend — Cloud Monitoring today, a Prometheus backend when a
  non-GKE consumer materializes (§15 Q4).
- A `nogcp` build tag can produce a slimmer binary without GCP SDK linkage for
  pure-k8s deployments; optional, decided at M1 by measured binary-size delta.

A future EKS/AKS provider is a new `pkg/cloud/<impl>` (IRSA is the `state wi`
analog, capacity-insufficiency events the stockout analog) — no engine,
schema, or skill changes.

---

## 3. Contract Boundaries

Three interfaces, all pre-existing, none new:

| Boundary | Contract | Direction |
| --- | --- | --- |
| `core-agent` daemon | `POST /sessions`, `POST /sessions/<sid>/inject` HTTP API | lookout → daemon |
| `core-agent` cost stack | cost/usage query API (token-burn source, §12) | lookout → daemon |
| Fleet aggregation layer (external) | rollup-ready signal schema (§8); per-cluster deployment | the fleet layer consumes lookout signals |

**Fleet scope is explicitly out of scope for this repo.** One process watching
a thousand API servers is wrong on every axis (API server load, credential
blast radius, failure domain). lookout deploys per cluster (watch-path) or per
project (quota source); cross-cluster rollup and coordination belong to a
fleet aggregation layer, out of scope for this repo. This includes a
*federated central graph*: the topology index (§6) is strictly per-cluster,
and the fleet layer joins signals — not graphs — across clusters. What
lookout owes the fleet tier is a signal schema a fleet-level consumer can
aggregate without parsing prose — §8.

---

## 4. Packaging & Invocation

### 4.1 One multicall binary

v2 specced 25 binaries under `/app/bin/<category>/`. Each would statically link
client-go and/or GCP SDKs (40–70 MB apiece → multi-GB image, 25 release
artifacts, 25 copies of the same flag/timeout/client scaffolding). Instead:
**one binary, `lookout`**, busybox-style. Symlinks can reproduce a
`/app/bin/<category>/<name>` layout if a deployment wants it, but the canonical
surface is subcommands:

```
lookout watch        # resident sentinel (§7) — the evolved k8s-event-watcher
lookout mcp          # serve read-path checks as MCP tools (§4.3)
lookout bundle       # correlated incident snapshot (§5, first call of every incident)
lookout health       # composed cluster scorecard: live checks + open findings + triage state (§5)

lookout triage  delta|logs|events|top|radius|changes|spec
lookout state   edges|webhooks|wi|volumes
lookout stab    drift|drain
lookout perf    probe --pack=apiserver|apf|etcd|startup
lookout cloud   stockout|orphans|ipspace|quota
lookout net     probe --dns|--tcp|--http     # active checks (§5, phase M3)
```

One release, one image, shared informer/client bootstrap, one output-envelope
implementation, and the agent discovers the whole surface from one `--help`.

### 4.2 CLI contract

- **Common flags:** `--namespace=<ns>|-A`, `--workload=<Kind>/<ns>/<name>`,
  `--since=<dur>`, `--format=logfmt|json` (default logfmt), `--timeout=10s`
  (every command wraps `context.WithTimeout`). Graph-backed commands
  additionally accept `--at=<RFC3339|dur-ago>` for point-in-time queries
  (§6.6).
- **Exit 0:** pure token-dense payload on stdout — no banners, no progress —
  **always terminated by a summary line**: `scanned=<n> findings=<n> elapsed=<d>`.
  Empty findings are explicit, never implicit.
- **Exit 1+:** structured diagnostics on stderr only; stdout stays clean so a
  captured stream never corrupts the context window. Exit 2 for usage errors.
- **Sanitization:** all output passes the §6.5 sanitizer in `pkg/emit`.

### 4.3 Three invocation surfaces, one implementation

All read-path logic lives in `pkg/checks`, consumed by:

1. **CLI** — for deployments where the agent has a shell (`bash` tool).
2. **MCP server** (`lookout mcp`, stdio or localhost HTTP) — the v2.6
   retrospective showed distroless images kill `bash + curl`; MCP is how a
   distroless `core-agent` daemon calls these checks natively. Each subcommand
   maps 1:1 to an MCP tool with a JSON schema derived from its flags (e.g.
   `bundle` → `k8s_triage_workload`, `triage radius` → `k8s_blast_radius`).
3. **In-process enrichment** — `lookout watch` calls `pkg/checks` directly to
   pre-warm incident sessions (§7.6). No fork/exec; shares the informer cache
   and the live topology index.

### 4.4 Teaching the agent the surface

A tool the model doesn't know to reach for is shelfware. Education is layered
by invocation surface, cheapest-first:

1. **Self-describing surfaces carry their own docs.** MCP tool descriptions
   (generated per §4.3) are written as micro-skills — one sentence of *when to
   reach for this*, not just what it does — and anchor to concepts the model
   already knows ("`triage spec`: kubectl describe, but token-dense and
   secret-safe"). `--help` output follows the same discipline: terse,
   exhaustive, example-bearing, written for an agent reader. One `lookout
   --help` discovers the whole surface.

2. **Skills map to workflows, never to commands.** The agent is never in a
   "`state volumes`-shaped task"; it is in an incident investigation, a health
   assessment, a capacity question. `skills/` therefore contains a small
   number of task-triggered skills, each teaching the *decision tree across
   commands* — which is the real knowledge:

   ```
   skills/
   ├── k8s-triage/        # incident investigation: bundle first; logs vs events vs radius; when to net-probe
   ├── cluster-health/    # on-demand & scheduled assessment: health, reading the scorecard, when to drill down
   ├── k8s-capacity/      # stockout/quota/ipspace forecasting; drafting the QuotaPreference request
   ├── gitops-drift/      # stab drift + triage changes: separating "who diverged" from "what changed"
   └── playbooks/         # per-symptom (CrashLoopBackOff, FailedMount, …) — the v2.6 convention, now
                          #   naming exact lookout commands per step
   ```

   Three-level progressive disclosure matches the token budget: frontmatter
   (~50 tokens, always in context) answers "lookout exists, is it relevant
   now"; the SKILL.md body (on trigger) teaches the workflow; per-command
   deep docs live in each skill's `references/` (flag detail, output-field
   glossary, shared envelope semantics) and load only when a command is
   actually being run. One skill per command is explicitly rejected: ~20
   competing frontmatter entries, none matching how tasks arrive, none
   teaching command sequencing.

3. **One source of truth, generated outward.** Command metadata in
   `pkg/checks` (name, flags, when-to-use line, output fields) generates
   `--help`, the MCP schemas, and the skill `references/` stubs. The §13
   contract tests validate skill-doc examples against golden outputs, so the
   three surfaces cannot drift apart silently.

4. **In-context education at the moment of need.** The enrichment bundle's
   overflow keys already name the follow-up `lookout` commands the agent can
   run (§7.6) — the inject itself teaches by example, exactly when it
   matters. Playbooks do the same per symptom. Skills teach ambient
   capability; injects and playbooks teach the next move.

Distribution: skills ship in this repo (they version with tool flags and
output formats — §2) and install into the consuming deployment's
`.agents/skills/` (project scope) or `$HOME/.agents/skills/` (user scope) via
the deploy recipes.

---

## 5. Read-Path Tool Matrix

Consolidated from v2's 25 binaries. Disposition summary: 8 kept, 11 merged into
5, 4 respecced (they were designed against APIs that don't behave as v2
assumed), 2 cut, 7 added.

| Command | Absorbs (v2 names) | Mission | Notes |
| --- | --- | --- | --- |
| `triage delta` | `workload-delta`, `node-pressure-sifter`, `disruption-budget-analyzer`, `kernel-sentry`, `spot-countdown` | One scan → every abnormal object: broken workloads, aged Pending pods, node pressure/NPD conditions, gridlocked PDBs, imminent spot reclaims, **degraded system add-ons** (CoreDNS / CNI / kube-proxy / CSI DaemonSets desired-vs-ready in `kube-system`), **namespaces at ResourceQuota limits**. | The agent's first move is "show me everything abnormal" — one call, not five. Resource-class toggles (`--only=pods,nodes,pdb,system,quota`). The system-addon and k8s-ResourceQuota classes came from the health-check review: CoreDNS/CNI degradation means the cluster is down while every workload status claims green, and a hit quota silently blocks scaling — both were missing from v2 and v3.0/3.1. |
| `triage logs` | `log-condenser` | Template-fingerprint log dedup, probe-noise strip, top-5 stack frames. | **Highest-value tool in the suite; build first.** ~150k tokens → ~350. Implementation: Drain-style template tree (fixed-depth parse tree over tokenized lines) rather than v2's flat regex+SHA256 — better clustering of near-identical templates at the same cost. |
| `triage events` | `ev-sifter`, `hpa-loop-catcher` | Deduped chronological event timeline over the owner-reference tree. | Shares `pkg/engine` with `lookout watch` — pull mode of the same filter/dedup; owner tree resolved via the topology index (§6). HPA thrash detection is an analysis mode here: the HPA object keeps no replica history; oscillations are recovered from `SuccessfulRescale` events. |
| `triage top` | `top-analyzer` (point-in-time only) | CPU/mem saturation vs limits, right now. | **Respecced:** v2's slope math needed a time series a one-shot binary doesn't have. Slope → ETA lives in the saturation source of `lookout watch` (§7.2), which does. `triage top` reports the instant; `--history` variants query Cloud Monitoring. |
| `triage radius` | *(new)* | Blast radius for a pod/workload: upstream routes (Gateway/Ingress → Service), lateral neighbors (same node, shared PVC/ConfigMap), downstream dependents. `--at` answers "what was the blast radius when the incident started". | Pure topology-index query (§6). Complements `state edges`: edges verifies *correctness* of dependencies; radius enumerates *impact*. |
| `triage changes` | *(new — closes a v3.0 omission)* | "What changed in the N minutes before onset": rollouts, ConfigMap/Secret updates, HPA rescales, node-pool ops, scoped to the target's graph neighborhood. | The #1 SRE question; identified in the original review but missing from the v3.0 matrix. Powered by the graph delta history (§6.6) joined with the event timeline. |
| `triage spec` | *(new)* | Sanitized, token-dense spec for one resource: system metadata stripped, secrets masked, defaults elided; `--diff` against the previous graph-history revision. | Surfaces the §6.5 sanitizer as a standalone read; the "kubectl describe, but for agents" tool. |
| `state edges` | `edge-tracer`, `endpoint-resolver`, *(new)* TLS-expiry checks | Dependency-graph verification: ConfigMap/Secret keys, RBAC, Service selectors, Service→EndpointSlice→Pod health (orphaned/unready endpoints), TLS secret expiry. | Graph-backed (§6): traversal comes from the index; per-edge validity checks live here. Endpoint resolution is just another dependency edge; cert expiry was missing from v2 entirely. |
| `state webhooks` | `webhook-inspector` | Failing-closed admission webhooks with dead backends. | Kept as specced. |
| `state wi` | `wi-scout` | GKE Workload Identity KSA↔GSA binding verification via IAM API. | Kept as specced; top GKE support driver. |
| `state volumes` | `volume-binder` | RWO multi-attach / cross-zone PV locks via `VolumeAttachment`. | Kept as specced. |
| `stab drift` | `field-sentinel` | Out-of-band drift vs GitOps manager via `managedFields`. | **Respecced:** `managedFields` yields the *manager string* (`kubectl-edit`), never a user identity — v2's `user=` field requires audit logs. Ships as manager-level detection; `--identity` resolves the principal through the §2 audit capability (GKE Cloud Audit Logs; issue #128), explicit-unavailable elsewhere. |
| `stab drain` | `drain-blocker` (+ PDB gridlock analysis) | Everything that will block a node drain: PDBs at `disruptionsAllowed=0`, bare pods, emptyDir without tolerations. | A gridlocked PDB *is* a drain blocker; merged. |
| `perf probe --pack=…` | `api-latency-sifter`, `apf-inspector`, `etcd-sentry`, `startup-profiler` | Cloud Monitoring query packs: apiserver P99 by verb/resource, APF queue saturation + 429s, etcd fsync/db-size, pod-startup P95 trend. | Four v2 binaries that were each "query Monitoring, threshold, emit" → one command, data-driven packs. etcd/APF packs require GKE control-plane metrics enabled — detect and degrade with an explicit `pack_unavailable` finding, not silence. |
| `cloud stockout` | `stockout-sentry` | `ZONE_RESOURCE_POOL_EXHAUSTED` extraction from Cloud Logging, with reroute suggestions. | Point-in-time read; the resident version with history lives in the capacity source (§10). |
| `cloud orphans` | `disk-orphan-scout`, `lb-ghost-buster` | Unattached billing-active GCE disks; forwarding rules / LBs targeting zero pods. | Same sweep shape, one command, `--only=disks,lbs`. |
| `cloud ipspace` | `ip-space-monitor` | Pod/Service CIDR utilization per subnet. | Point-in-time; consumption *rate* lives in the capacity source (§10). |
| `cloud quota` | *(new)* | Per-project quota usage/limit snapshot with nearest-to-exhaustion ranking. | Read companion of the quota source (§10). |
| `bundle` | *(new)* | Given `--workload=…` (or `--incident=<inject payload>`): run delta + events + edges + logs + radius scoped to the target, emit one correlated payload including the sanitized spec. | The first tool call of every incident, and the enrichment payload for §7.6. Converts 4–5 agent round trips into one. Exposed over MCP as `k8s_triage_workload`. |
| `health` | *(new)* | The "are there issues with this cluster?" scorecard: one composed pass over ~10 check categories — control-plane latency (`perf probe` packs), node conditions, crash loops, aged Pending, rollout stalls, PVC/storage health, system add-ons, ResourceQuotas, cert expiry, webhook health — each category reporting `healthy` or findings, merged with open sentinel findings (§9.1) and triage-status records (§9.4). | Composition, not new checks: every category delegates to an existing `pkg/checks` implementation. The merge is the point — output reflects *triaged reality* ("payment-service CrashLoop: triaged 10 min ago, root cause DB pool exhaustion, PR open, downgraded to warning"), not raw telemetry. Exposed over MCP as `k8s_cluster_health`; the per-cluster answer a fleet-level consumer aggregates for the fleet-wide question. |
| `net probe` | *(new)* | Active DNS/TCP/HTTP check from inside the cluster. | Hypothesis confirmation, not inference. Bends read-only in letter, not spirit; still zero cluster mutation. Phase M3. |

**Cut from v2:**

- `stale-object-sweeper` — determining "unreferenced" requires modeling every
  consumer (pod specs, ingress TLS, CSI, cert-manager, external-secrets, …);
  false-positive machine, and housekeeping rather than triage.
- `exec-spy` — specced against an API that doesn't exist: `kubectl exec` emits
  no `core/v1` Event with `Subresource == "exec"`; that's Cloud Audit Log
  territory. Also security-detection scope creep. If ever wanted, it's an
  audit-log query pack, not a tool. Deferred indefinitely.

---

## 6. Topology Index (`pkg/graph`)

The relational substrate under the suite. Adopted from the `k8s-graph`
proposal, right-sized, and demoted from product to package: it has no CLI of
its own; it is consumed by `state edges`, `triage radius|changes|events|spec`,
`bundle`, storm correlation (§7.5), and enrichment (§7.6).

### 6.1 Pod-nexus model

Typed nodes and edges centered on the Pod, connecting the traffic/policy
layers above it to the infrastructure below:

```
Gateway/Ingress → Service/EndpointSlice → [NetworkPolicy, RBAC] → POD
POD → Containers | ConfigMaps/Secrets | PVCs/Volumes → Node → Zone
```

```go
type NodeID uint32          // interned identity; names live in the interner
type NodeKind uint8         // Gateway, Service, Policy, Pod, Container, Config, Secret, PVC, Node, Zone, …
type EdgeKind uint8         // RoutesTo, Selects, Governs, RunsOn, Contains, Mounts, …
```

It is a directed *graph*, not a DAG — selector relationships and shared mounts
create cycles across layers; traversals carry visited-sets, and nothing may
assume acyclicity.

### 6.2 Right-sizing: interface first, heroics later

The originating proposal specced CSR adjacency arrays, string interning,
zero-GC arenas, and lock-free everything for 100k pods. Two facts size this
correctly:

- **The informer caches dominate memory.** client-go keeps every watched
  object as a full decoded struct; at any cluster size, the graph's overhead is
  a fraction of what the informers already cost. Optimizing the small term
  first is backwards.
- **Realistic targets:** typical single clusters are 1–15k pods; 100k is the
  ceiling, not the design point. Real informer delta rates are hundreds to low
  thousands of events/sec even on large clusters — the proposal's 50k events/s
  target is fiction, and we drop it.

So: the **interface** is designed for the compact representation (uint32
`NodeID`s, typed edge kinds, readers query an `atomic.Pointer[Graph]` COW
snapshot and never take locks), but the **initial implementation** is plain
Go maps behind that interface. CSR packing and arena allocation are
profile-triggered optimizations (§15 Q5), not v1 work. The COW/single-writer
discipline is kept from day one — not for throughput, but because it makes
"readers see a consistent topology" trivially true, which matters for
correctness of blast-radius answers during churn.

### 6.3 Ingestion

- **Initial sync:** paged `List` (limit ~5000, ResourceVersion bookmarks) per
  resource kind, built off to the side, then one atomic pointer swap. The
  read-path serves nothing until the first swap.
- **Steady state:** the same shared informer factory the sentinel already runs
  feeds `ADD/UPDATE/DELETE` deltas through a single-writer loop into the next
  COW snapshot (batched; swap at most every few hundred ms — readers don't
  need per-event freshness).
- One informer set serves the sentinel sources *and* the graph; the graph adds
  watches only for kinds no source needs (Gateways, NetworkPolicies).

### 6.4 Consumers

| Consumer | Query shape |
| --- | --- |
| `state edges` | outbound edges of a workload + per-edge validity checks |
| `triage radius` | BFS up (routes), lateral (shared node/volume/config), down (dependents) with depth limit |
| `triage events` / `triage changes` | owner-chain resolution; neighborhood scoping for change filters |
| `bundle` / enrichment (§7.6) | radius + edges + owner chain in one pass |
| storm correlation (§7.5) | blast-radius key: given N incident objects, find the shared ancestor (node, owner, config) |
| `triage spec --diff` | previous revision lookup from history (§6.6) |

### 6.5 Sanitizer

Lives in `pkg/emit`, applied to every payload on every surface (CLI, MCP,
inject) — not opt-in per tool. On the inject surface the mask runs at
dispatcher payload assembly (`internal/watch`), since `pkg/inject` is a
dependency-free leaf that cannot import the sanitizer. Strips system metadata (`managedFields`,
`resourceVersion`, `uid`, noisy status), elides defaulted fields, and masks
secret material: `Secret.data` values, env vars sourced from Secrets, and
value-shaped strings matching credential heuristics → `[REDACTED]`. The graph
never stores secret *values* at all — only names, keys, and content hashes
(so `triage changes` can say "secret db-credentials changed" without ever
holding the payload).

### 6.6 History: persistence is for time-travel, not recovery

The proposal justified an embedded WAL (PebbleDB) with <200ms crash recovery.
Rejected on two grounds: **(a)** recovery is free — the K8s API server is the
source of truth and informers must re-list/re-sync after any restart long
enough to fall out of the watch cache window anyway; a paged rebuild is
seconds, once per restart of a resident daemon. **(b)** A second embedded
storage engine next to the sentinel's SQLite store is complexity without a
customer.

What persistence *does* buy is **point-in-time topology**: "what did the blast
radius look like 20 minutes ago when the incident started." So:

- Periodic graph snapshots (every ~5 min, compressed binary of the node/edge
  arrays, tagged with ResourceVersion) + the per-delta change log, both in the
  existing sentinel-local SQLite store (§9.1), same TTL policy.
- `--at=<time>` on graph-backed commands resolves to nearest-snapshot +
  replay-forward.
- The delta log doubles as the data source for `triage changes` — one
  mechanism, two features.

History is a watch-path feature: one-shot CLI invocations without a resident
sentinel serve `--at` queries only if pointed at a sentinel's store
(`--store=`); otherwise they answer live-only and say so in the summary line.

---

## 7. Watch-Path: `lookout watch` (the sentinel)

The existing `k8s-event-watcher` — informer → filter → dedup → per-incident
session inject — is the seed. Its pipeline is already source-agnostic
(`TriageEvent` deliberately carries no k8s types), so it generalizes to a
**signal engine**: one resident process per cluster, pluggable signal sources
feeding a shared pipeline. We do not build N sidecars.

### 7.1 Pipeline

```
sources (N) → filter → dedup → storm correlation → severity routing → enrichment → inject
                                     (§7.5)            (§7.7)           (§7.6)
```

Filter, dedup, and the injector are extractions of the shipped
`filter.go` / `dedup.go` / `injector.go` into `pkg/engine` and `pkg/inject`.
Storm correlation, severity routing, and enrichment are new stages.

### 7.2 Signal sources

Each source is a package under `pkg/sources` implementing:

```go
type Source interface {
    Name() string                          // stable, used in signal schema + config
    Scope() Scope                          // Namespace | Cluster | Project (§11)
    Run(ctx context.Context, emit func(Signal)) error
}
```

| Source | Class | Watches | Emits (examples) |
| --- | --- | --- | --- |
| `k8s-events` | reactive | `core/v1 Event` informer (today's watcher, unchanged semantics) | `CrashLoopBackOff`, `FailedMount`, … per existing allow-list |
| `object-state` | leading (transitions) | Pod/Node/Deployment/EndpointSlice informers | node `Ready→NotReady` flap, node Memory/Disk/PID pressure onset (sustained → critical), per-node eviction burst, Deployment progress-deadline approaching, endpoints count → 0, PDB → `disruptionsAllowed=0` |
| `rollout` | leading (as-it-happens) | Deployments/StatefulSets with in-progress rollouts | "new RS 0/1 ready for 4 min, old RS healthy — probable bad deploy", fired well before `progressDeadlineSeconds` |
| `workload` | reactive + leading (schedule) | `batch/v1` Job/CronJob informers (post-M5 #129) | Job `Failed` condition (`BackoffLimitExceeded`, `DeadlineExceeded`); CronJob activation passed with no run — batch failures leave no crashlooping pod for `k8s-events` to catch |
| `autoscaling` | leading (sustained state) | `autoscaling/v2` HPA informer, status conditions (post-M5 #131) | HPA pinned at maxReplicas with its metric still over target (`autoscaling.hpa_pinned` — out of headroom, escalates when sustained); metrics pipeline dead, `ScalingActive=False`/`FailedGet*` sustained (`autoscaling.hpa_metrics_dead`). HPA *thrash* stays a read-path check (`triage events`). |
| `saturation` | leading (trend) | `metrics.k8s.io` + kubelet volume stats, continuously sampled | "pod hits memory limit in ~14 min" (slope → ETA), "PVC full in ~3 h". This is where v2 `top-analyzer`'s regression math lives — a resident process owns the time series a one-shot never had. |
| `degradation` | leading (trend) | EndpointSlice ready ratios, probe flaps below the `Unhealthy` threshold | "payment-backend capacity 5/5 → 3/5 over 10 min" |
| `expiry` | leading (countdown) | TLS secrets, webhook CA bundles, SA key ages, cert-manager status | "cert expires in 72 h and last renewal failed" |
| `capacity` | leading + reactive | CA events, `cluster-autoscaler-status` ConfigMap, GKE CA visibility logs (§10.1), pod-requests/node-allocatable ratio per scheduling domain | scaleup failed (`GCE_STOCKOUT` / `GCE_QUOTA_EXCEEDED`), pending-pod aging, headroom trend, "domain full in ~N h" bin-packing forecast (`capacity.cluster_forecast`) |
| `ingress` | reactive | ingress-gce controller events, own Warning-only Event informer filter (post-M5 #135; the capacity-source reason-ownership precedent — ingress-gce reuses reason `Sync` for Normal housekeeping, so the k8s-events allow-list can never carry it) | GCLB programming failures while the Ingress object looks fine: `Sync`/`Translate` errors on an Ingress, NEG sync/attach failures on a Service |
| `quota` | leading (countdown) | Cloud Quotas + Monitoring quota metrics, per **project** (§10.2) | "CPUS us-east1 at 91%, exhausted in ~6 days at current slope" |
| `notifications` | reactive (provider announcements) | provider cluster-notification stream, per **project** (GKE notificationConfig Pub/Sub; post-M5 #130) | upgrade started on a node pool (info → store, correlates incident windows), security bulletin (warning → watchboard) |
| `token-burn` | leading (trend) | `core-agent` cost-stack API (§12) | "session X burning 4× baseline; budget exhausted in ~20 min" |

Sources are individually enabled in config. A deployment whose RBAC can't
support a source's scope gets an explicit startup error naming the source and
the missing permission — never a silent empty watch (§11).

### 7.3 Signal, not TriageEvent

`TriageEvent` generalizes to `Signal` (schema in §8). The existing inject kinds
`k8s-event` / `k8s-event-followup` are preserved verbatim for playbook
back-compat; new kinds are namespaced by source:
`rollout.stall`, `saturation.forecast`, `degradation.capacity`,
`expiry.warning`, `capacity.stockout`, `capacity.pending-aged`,
`quota.forecast`, `token.burn`, plus the cross-cutting `resolved` and
`storm` kinds below.

### 7.4 Recovery injects — the closed loop

The dedup cache already binds incident keys to sessions (`BindSession`). Each
source that can observe a symptom can observe its absence: pod Ready and
restart-stable for N minutes, rollout completed, endpoints back to full ratio,
cert renewed. When a bound incident's symptom clears, the sentinel injects
`kind=resolved` (with `cleared_after`, `observed_stable_for`) into the same
session; if it recurs within the stability window, `kind=resolved.reverted`.

This is the fix-and-verify loop closed from the signal side: the agent no
longer polls to confirm its fix stuck, sessions can auto-conclude, and every
incident record gains a ground-truth outcome (§9.3). **Highest value per line
of code in this document; ships in M2 before any new source.**

### 7.5 Storm correlation

Today's dedup key is `(uid, reason)`: a node failure opens ~30 sessions for 30
evicted pods. A second-level correlation window (default 60 s) groups new
incidents sharing a blast-radius key — resolved via the topology index: the
nearest common ancestor of the affected objects (node, owner chain, shared
ConfigMap/PVC, namespace; at fleet tier, zone) — into one `kind=storm`
incident: *"Node X NotReady; 30 pods affected across 6 namespaces; 3
representative incidents attached."* Member signals are recorded but don't
open sessions. Severity of the storm is the max of members + a size escalator.

### 7.6 Enrichment (warm sessions)

The first 2–3 tool calls of an incident session are predictable. Before
injecting, the sentinel calls `pkg/checks` in-process — the same code as
`lookout bundle`, sharing the live topology index — scoped to the affected
object, and attaches the bundle to the initial inject payload (size-capped;
overflow keys reference `lookout` commands the agent can run itself). The
session starts warm: seconds, not turns. Enrichment is per-severity
configurable (always for `critical`, off for `info`).

### 7.7 Severity routing

Leading indicators must not each open a per-incident session at page priority.
Severity is assigned per signal kind (overridable in config), and routing is
per-severity policy — the existing `per-incident` / `shared` process-level
modes become per-class:

| Severity | Default routing |
| --- | --- |
| `critical` | per-incident session (today's behavior), full enrichment |
| `warning` | shared watchboard session, batched (rolling digest inject) |
| `info` | stored only (§9.1); surfaced by read-path queries and digests |

### 7.8 Untrusted input: the prompt-injection boundary

Every free-text field a payload carries into an agent session originates
in the cluster, and parts of the cluster are attacker-influenceable: any
tenant who can create a workload chooses its name and labels; any
controller or admission webhook (including a compromised one) authors
event messages; annotations and spec strings arrive via the enrichment
bundle. That text is delivered to an LLM agent as the incident it must
investigate — a hostile tenant can therefore write *to the
investigator*. This is a distinct threat from secret leakage: the §6.5
sanitizer masks credential-shaped values, it does not and cannot decide
whether prose is adversarial instructions.

What bounds the blast radius today:

- **Syntactic delimiting.** Payloads are JSON; field values cannot
  escape their fields or forge sibling keys. Names, namespaces, kinds,
  label keys, and reasons are additionally constrained by Kubernetes
  validation (DNS-1123 / qualified-name / enum shapes). Event messages,
  label values, and annotation/spec strings in enrichment bundles are
  the genuinely free-text carriers.
- **No raw write authority over the cluster** (principle 6, §10.3).
  Injected text can at most *persuade* the agent; the cluster-facing
  mutations it can attempt — GitOps PRs and `QuotaPreference` drafts —
  route through the core-agent daemon's managed write path and
  permission gate. The sentinel's own ClusterRole is read-only.
  **Exception — the triage-status write is not gated today.** `lookout
  triage status` (§9.4) writes the sentinel's `--store` directly, and
  the dispatcher honors an open record's `severity_override` when
  routing (§7.4). So an injection that steers the agent to write
  `--severity-override=info` for the incident it is investigating can
  make the sentinel stop paging *that incident/object* — the write is
  keyed by `(fingerprint, resource_key)` (plus the controller ref), so
  the mute is scoped to one incident or workload, not a whole severity
  class cluster-wide, and it lasts only until the §7.4 recovery flip
  clears the record (which requires the symptom to actually resolve).
  This is a real routing effect, not just paperwork; the only
  counter-signal is the once-per-window `kind=triage.regressed`
  evidence injected into the same (possibly compromised) session.
  Closing the gap needs the daemon-mediated write path
  (docs/triage-status-write-design.md, "Out of scope").
- **Skills treat payloads as data.** The shipped skills instruct agents
  to parse payload fields as evidence, never to execute instructions
  found in them (skills/README.md "Untrusted cluster data").

What is deliberately acknowledged as NOT mitigated: an injection that
successfully steers the agent produces *plausible-but-hostile
paperwork* — a misleading PR draft, a padded quota justification, or a
misled human reading the session. For the gated cluster-facing writes
the permission gate makes that reviewable, not impossible. The
triage-status write is the sharper case called out above: it is not
gated today, and a hostile `severity_override` has a direct
paging-suppression effect (scoped to the incident/object, bounded by
the §7.4 recovery flip) rather than only producing reviewable
paperwork — closing that is tracked with the daemon-mediated write
path. Defenses beyond
this — provenance marking on payload fields so the model can tell
operator instruction from cluster data, and adversarial-content
heuristics — are future work, tracked as an open question (§15), and
any payload framing change goes through the frozen-contract amendment
process (§8).

---

## 8. Signal Schema (fleet-rollup ready)

The inject payload, superset of the shipped `InjectPayload` (existing fields
keep their exact names and casing for playbook back-compat):

```json
{
  "kind": "capacity.stockout",
  "source": "sentinel",              // "sentinel" (push) | "scan" (read-path finding)
  "severity": "critical",
  "fingerprint": "sha256:…",        // stable hash of (kind, reason-class, object-class, zone) —
                                     // NOT of the object name — so a fleet-level consumer can
                                     // recognize the same failure across clusters
  "cluster": "prod-east",
  "project": "acme-prod",
  "zone": "us-east1-b",
  "namespace": "…", "kind_of_object": "…", "name": "…", "uid": "…",
  "reason": "…", "message": "…",
  "count": 3, "first_seen": "…", "last_seen": "…",
  "forecast": { "eta": "…", "confidence_basis": "linear-90m-window" },   // trend sources only
  "context": { "controller_ref": "…", "node": "…", "labels": {} },
  "enrichment": { "bundle": "…" }    // §7.6, optional
}
```

`fingerprint` + `cluster`/`project`/`zone` are what make fleet rollup a join
instead of a parsing project: the same stockout hitting 40 clusters in a zone
carries 40 identical fingerprints. lookout ships the schema; the fleet layer
ships the rollup.

**One schema for push and pull.** Read-path findings (from `health`, `triage
delta`, etc.) are Signals too, with `source: scan`. Point-in-time scans miss
transients; the push stream lacks stateful context — merging them is only
cheap if they dedupe on the same `fingerprint`. That's how `lookout health`
rolls "the sentinel paged on this 20 minutes ago" and "the scan still sees it"
into one finding instead of two, and how a fleet-level consumer avoids
double-counting a symptom reported by both paths.

---

## 9. Storage

One embedded engine (SQLite), two data classes with different lifetimes;
neither turns the memory store into a metrics database. (A second embedded
engine for graph persistence was considered and rejected — §6.6.)

### 9.1 Raw occurrences + graph history — sentinel-local

`pkg/store`: bounded, TTL'd SQLite alongside the existing dedup-persist file
(same volume). Holds:

- every emitted signal (including `info` severity that never injects) —
  powers storm-correlation lookback, `resolved` stability windows, digests,
  and recommendation history;
- graph snapshots + the topology delta log (§6.6) — powers `--at`
  point-in-time queries and `triage changes`.

Default TTL 30 days; it's telemetry, not a system of record.

### 9.2 Distilled facts — shared memory store

Low-volume, durable, agent-queryable records written through the `core-agent`
Memory interface (`docs/shared-memory-design.md`). A distiller pass (in the
sentinel, on a schedule) converts recurring raw occurrences into facts:

> *"us-east1-b n2d-standard-48: 3 stockouts this week; us-east1-c clean over
> same window."*

This is the shared-memory design's audit-derived-memory thesis with its first
non-conversational producer. These facts are what upgrade alerts into
recommendations ("reroute to us-east1-c") — §10.3.

### 9.3 Verified-fix corpus

With recovery injects (§7.4), every incident session is a trajectory with a
ground-truth label: **symptom → diagnosis → action → externally verified
outcome**. That corpus is valuable (evals, playbook improvement, and — for any
model developer — scarce training-grade data) but only if harvestable. The
contract: outcome records are schema-stable structured injects
(`kind=resolved` / `resolved.reverted` / escalation payloads), never prose, so
a harvester can extract labeled trajectories from the eventlog without NLP.
This costs nothing beyond discipline once §7.4 exists; it is a hard
requirement on those payload schemas.

### 9.4 Triage-status records — scans report triaged reality

The problem: an incident agent triages a CrashLoop at 08:00 and determines
it's a bad connection string; a health scan at 08:15 must not report the same
symptom as a fresh unknown, re-page, or re-burn tokens re-deriving the
diagnosis. The raw telemetry says "broken"; the *triaged reality* says
"diagnosed, PR open, downgraded."

The mechanism: incident playbooks instruct the agent to write a compact
**triage-status record** at each material transition (diagnosed, action taken,
escalated) through the shared Memory interface (§9.2), keyed by the incident's
`fingerprint` + `resource_key`:

```json
{
  "fingerprint": "sha256:…",
  "resource_key": "apps/v1/Deployment/prod/payment-service",
  "session": "sid-…",
  "status": "triaged",               // investigating | triaged | actioned | escalated
  "root_cause_hypothesis": "DB connection pool exhausted (max_connections 100/100)",
  "severity_override": "warning",    // agent judgment: traffic on backup, not page-worthy
  "action": "PR #402 opened; escalated to #platform-db",
  "updated": "…"
}
```

Consumers:

- **`lookout health` / `bundle`** join open findings against these records by
  fingerprint, so scan output carries the diagnosis and the paper trail, not
  just the symptom.
- **The sentinel's severity routing (§7.7)** honors `severity_override`: an
  incident the agent downgraded stops re-paging on followups; an
  `escalated` status keeps it hot.
- **Lifecycle is automatic:** recovery injects (§7.4) close the loop — when
  the symptom clears, the record flips to resolved and joins the corpus
  (§9.3). No manual TTL bookkeeping; the stability window is the decay.

What we deliberately **don't** build (the parallel proposal specced Redis
Redlock leases, `assigned_agent` claiming, pgvector/Mongo tiers): distributed
locking solves a race lookout doesn't have. Per cluster, the sentinel is the
single writer — an event for an already-bound incident becomes a followup
inject to the existing session (today's shipped dedup behavior), which *is*
the "check for active finding, attach, don't re-triage" flow, minus the lock
infrastructure. Cross-agent contention only exists at fleet tier, which is
the fleet layer's coordination problem by the settled boundary (§3). And storage tiering is
already decided by the shared-memory design (FTS5-over-eventlog in-tree, Redis
AMS extras adapter) — we add a record type, not a database.

---

## 10. Capacity & Quota Subsystem

The deepest GKE-specific investment, because capacity exhaustion has days of
lead time when watched and zero when not.

### 10.1 Cluster-autoscaler signals — never the text log

Three structured sources replace CA log parsing, in ascending quality:

1. **Kubernetes Events:** `NotTriggerScaleUp` (on the pending pod, with
   per-nodegroup rejection reasons), `TriggeredScaleUp`, scale-down events.
   Allow-list additions to the `k8s-events` source; the real-time trigger.
2. **`cluster-autoscaler-status` ConfigMap** (`kube-system`): per-nodegroup
   health and backoff, `cloudProviderTarget` vs `registered` vs `ready` — the
   target/ready gap is "asked for a node, didn't get one". Polled.
3. **GKE CA visibility logs** (Cloud Logging,
   `cluster-autoscaler-visibility`): structured decision records —
   `noScaleUp` with per-MIG reasons including `GCE_STOCKOUT` and
   `GCE_QUOTA_EXCEEDED`, plus IP-exhaustion reasons. The authoritative *why*,
   in JSON.

The stockout/quota distinction matters because remedies are disjoint: stockout
→ reroute zone/machine-type (informed by §9.2 history); quota → file an
increase (§10.3).

Portability: sources 1 and 2 are upstream cluster-autoscaler behavior and work
on any CA-running cluster; source 3 is GKE-only and lights up via the `gke`
cloud provider (§2). On non-GKE clusters the capacity source still fires on
scaleup failures — it just can't always name the structured *why*.

### 10.2 Quota source (per-project)

Deployed once per GCP project — fifty clusters in a project must not each poll
quota APIs. Sources:

- **Compute regional quotas:** `regions.get` usage/limit pairs (CPUs, GPUs,
  IPs, disks) — the cheap 80%.
- **Cloud Quotas API** (`cloudquotas.googleapis.com`): quota metadata across
  services; `QuotaPreference` for programmatic increase requests.
- **Cloud Monitoring:** `serviceruntime.googleapis.com/quota/allocation/usage`
  vs `…/limit` time series → the saturation-source slope math applies
  directly: not "at 87%" but "exhausted in ~6 days at current slope".

### 10.3 Correlation and the quota write path

- **Correlation:** "scaleup failed (`GCE_QUOTA_EXCEEDED`)" + "CPUS_PER_REGION
  at 98%" arrives as *one* diagnosed incident, not two alerts a human joins.
- **Recommendations:** distilled history (§9.2) turns "quota nearly exhausted"
  into "request +2000 CPUs in us-east1; note us-east1-b is stockout-prone,
  spread the pool to -c/-d".
- **Write path:** the agent drafts a `QuotaPreference` increase request with
  justification generated from observed slopes, and files it **behind the
  daemon's permission gate**. First place in the suite where the managed write
  path is a clean API call with human-grade paperwork — and quota increases
  have exactly the lead time that makes "know before the operator" pay off.

---

## 11. Scoping & Deployment Tiers

| Tier | Unit | Mechanism |
| --- | --- | --- |
| Namespace | `lookout watch` under a `Role` | `--namespace`/`--exclude-namespace` (existing flags). Cluster-scoped sources (`object-state` nodes, `capacity`, PDB checks) **fail loudly at startup** — "source X requires cluster RBAC" — and are disabled explicitly, never silently empty. The topology index builds a namespace-local subgraph (no Node/Zone layer). |
| Cluster | one sentinel per cluster (canonical) | one informer cache, one topology index, one credential boundary, one failure domain. One daemon may serve many sentinels (unchanged from v2.6 design). |
| Project | quota source only | one instance per GCP project, regardless of cluster count. |
| Fleet (1000s) | **the fleet layer, not lookout** | sentinel-per-cluster fan-in; the fleet layer joins on `fingerprint` + `cluster`/`zone`/`project` (§8). No federated central graph (§3). lookout's storm correlation (§7.5) is the single-cluster instance of the same idea. |

Read-path commands are stateless and scope by flags (`--namespace`,
`--workload`, `-A`); granularity is free there.

---

## 12. Token-Burn Source

The suite treats CPU, memory, disk, and IP space as saturation dimensions; for
agent fleets, **token spend is a first-class saturation metric** — a runaway
agent loop is an OOM in the currency that matters. The `token-burn` source
polls the `core-agent` cost stack (shipped v2.7) per session/fleet, applies the
same slope → ETA math as `saturation`, and emits `token.burn` signals
("session X at 4× baseline; budget exhausted in ~20 min"; severity from
budget-fraction thresholds). It is deliberately just another source: no special
casing, and it's the one signal here no conventional SRE stack will ever
provide.

---

## 13. Testing Conventions

Carried from v2 and the shipped watcher, normalized:

- **k8s sources/checks:** `client-go/kubernetes/fake` seeded fixtures; assert
  exact findings and the summary line. Informer-driven sources use the shipped
  watcher's test harness pattern (fake clientset + synthetic event stream).
- **Topology index:** synthetic-cluster generator (10k pods + services +
  configs) for correctness (property tests: every informer delta is reflected
  in the next snapshot; radius answers are stable across COW swaps) and for
  benchmark gates that trigger the §15 Q5 optimization decision. History
  round-trip: snapshot + replayed deltas ≡ live graph at same
  ResourceVersion.
- **Sanitizer:** golden-file tests over specs containing secrets in every
  position we know of (env, envFrom, volumes, annotations); a payload
  containing an unmasked credential fixture fails CI.
- **GCP checks:** recorded API fixtures (Logging/Monitoring/Compute/Quotas
  responses) behind small client interfaces; no live-project tests in CI.
- **Trend sources:** synthetic time series with known slopes; assert ETA math
  and threshold crossings, including the no-forecast case (insufficient
  window).
- **Pipeline:** storm correlation and severity routing tested at the engine
  level with scripted signal streams (N pod failures on one node → exactly one
  storm inject).
- **Contract tests:** every command's logfmt/json output round-trips through a
  schema check so playbooks/skills can't drift silently from tool output.

---

## 14. Phased Delivery

| Phase | Contents | Exit criterion |
| --- | --- | --- |
| **M0 — bootstrap** | Repo + CI + module. Move `k8s-event-watcher` verbatim as `lookout watch` (behavior-identical, flags preserved); extract `pkg/{kube,engine,inject,emit}`. `core-agent` drops k8s deps; CHANGELOG + deprecation pointer. | Existing watcher deployment swaps images with zero config change. |
| **M1 — read-path core** | `pkg/graph` v1 (map-backed, COW, live-only); `triage logs` (Drain), `triage delta` (incl. system-addon + ResourceQuota classes), `state edges`, `triage spec` + sanitizer, `bundle`, `health` (live checks only — store/memory merge lands M4); §4.2 output contract; `lookout mcp`; first skills. | An incident session can be fully investigated with lookout tools alone; `lookout health` answers "any issues in this cluster?" in one call; no secret value can reach any output surface. |
| **M2 — closed loop** | Recovery injects (§7.4), storm correlation via graph blast-radius keys (§7.5), severity routing (§7.7), enrichment via in-process bundle (§7.6); `object-state` source. | Node-failure drill produces 1 storm session, not 30; fix-verify round-trips without agent polling. |
| **M3 — leading indicators + history** | `rollout`, `saturation`, `degradation`, `expiry` sources; raw store (§9.1) incl. graph snapshots/delta log; `triage events`, `triage top`, `triage radius --at`, `triage changes`, `net probe`. | A staged bad deploy and a staged memory leak both open sessions before user-visible failure; "blast radius at onset" answerable 30 min after the fact. |
| **M4 — capacity & quota** | `capacity` + `quota` sources (§10); `cloud stockout|orphans|ipspace|quota`; distilled memories (§9.2); triage-status records (§9.4) + memory-merged `health`; quota write path behind permission gate. | Staged quota exhaustion yields correlated incident + drafted increase request; a health scan run mid-incident reports the triage state, not a fresh unknown. |
| **M5 — fleet & corpus** | Remaining reads (`state wi|webhooks|volumes`, `stab drift|drain`, `perf probe`); fingerprint schema finalized for fleet rollup; `token-burn` source; corpus harvester contract validated end-to-end (§9.3). | a fleet-level consumer can rollup a multi-cluster staged stockout; one harvested labeled trajectory from a real incident. |

---

## 15. Open Questions

1. **Inject payload types as a shared contract package** — promote
   `InjectPayload`/`Signal` into `core-agent` (or a tiny third module) so
   daemon-side playbook tooling and lookout share one definition, or keep
   lookout as schema owner with contract tests on both sides?
2. **Watchboard session lifecycle** — the shared `warning` session (§7.7) grows
   unboundedly; rotation policy (daily? size-based?) and its interaction with
   session resume need a small design note in M2.
3. **Enrichment size cap** — fixed byte budget vs model-aware (the daemon knows
   the session's model/context); start fixed, revisit after M2 telemetry.
4. **Prometheus metrics backend** — the `pkg/cloud` provider boundary (§2)
   makes `perf probe` packs and `triage top --history` portable in principle;
   a Prometheus-backed provider implementation is the missing piece. Deferred
   until a non-GKE consumer exists (respecting the no-speculative-surface
   rule), but the pack query format should avoid Cloud-Monitoring-only
   constructs where a PromQL equivalent is obvious.
5. **Graph compaction trigger** — at what benchmarked scale (nodes, delta
   rate, p99 radius latency) does the map-backed graph justify the CSR +
   interning rewrite behind the same interface? Set the gate in M1 benchmarks
   rather than guessing now.
6. **Provenance marking for untrusted payload text (§7.8)** — should inject
   payloads structurally distinguish cluster-sourced free text (event
   messages, label values) from lookout-composed framing, so the consuming
   agent can weight instruction-like content accordingly? Any such framing
   change amends the frozen schema (§8), so it needs the daemon-side story
   at the same time; until then the mitigation is skill-level guidance plus
   the permission gate.
6. **Graph kind coverage** — v1 covers core + apps + discovery + storage +
   networking.k8s.io. Gateway API (`gateway.networking.k8s.io`) and mesh CRDs
   (Istio) widen the northbound layer; add when a consuming deployment runs
   them, detected via discovery at startup.
