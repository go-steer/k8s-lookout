# leeway — Placement Drift Detection

**Status:** Draft v0.6 — proposal. Both maintainer decisions taken (§15 Q3, Q4):
the higher scale targets stand and DESIGN §6.2 is edited to match; `topology-drift`
ships default-on. Spikes S9, S10 and S11 resolved against this repo; **S1, S2 and S3
resolved against a live GKE cluster** — S1 rewrote the §7.7.2 annotation model and
deleted `RankTracker.Move`, S2 confirmed §7.7.1 unchanged and added a second fallback
shape to §7.7.3, S3 measured every remaining extractor key and turned
`NodeProfile.Reservation` into a (project, name) pair. **S4 and S5 are now resolved
too** — the scheduler's `defaultConstraints` are confirmed unreadable on managed GKE,
so FR-9 is a manual sync whose mitigation caps default-derived intent below Tier A,
and §8.4's export defaults are measured rather than guessed (GMP bills per sample, the
GKE managed collector is the OTLP endpoint, temporality is cumulative). **S7 is
deferred by maintainer decision, and S6 closed without being run** — its question
turned out to be answerable from the field-selector grammar rather than from a
namespace count, and its method would have generalised an estate to a population.
**S8 resolved 2026-09-21, and every spike is now closed** — it found the pod memory
model out by 12× (§6.1, §6.6) and shipped the kwok padding it existed to size.
No spike gates a data model, and §7.7.2 carries no `UNVERIFIED` marker.
**Tracking:** [#416](https://github.com/go-steer/k8s-lookout/issues/416)
**Date:** 2026-09-16
**Home:** `k8s-lookout` — `pkg/leeway` plus two watch sources
**Language / stack:** Go, `k8s.io/client-go` shared informers

---

## 1. Problem statement

Kubernetes clusters are expected to distribute nodes and workloads across failure
domains (zones, regions, racks, node pools). That expectation is expressed
inconsistently — sometimes explicitly (`topologySpreadConstraints`), sometimes
implicitly (pod anti-affinity, node pool configuration), and very often only in
someone's head.

Over time, actual placement *drifts* away from the intent:

- a zone runs out of instance capacity, so the autoscaler backfills elsewhere
- a rollout lands unevenly and nobody notices because the pods are all `Running`
- node consolidation packs a workload into one zone
- a zone incident evicts pods that never rebalance after recovery
- a `topologySpreadConstraint` is `ScheduleAnyway`, so it is silently ignored
- StatefulSet pods are pinned by zonal PVs and can never rebalance

None of these produce an error condition. Every pod is `Ready`, every Deployment
is at full replica count, and the cluster looks healthy — right up until a zone
fails and takes 80% of a service with it.

**The name.** *Leeway* is the nautical term for a vessel's sideways drift off its
intended course, and colloquially the tolerance you are allowed. Both senses are
the thesis: the hard problem is not measuring deviation, it is deciding how much
deviation is normal before it counts as drift.

### 1.1 Why this is hard

The naive version — "count pods per zone, alert if uneven" — produces so many
false positives it gets muted within a week. The real difficulty is in four
places:

1. **Perfect balance is usually impossible.** 5 replicas across 3 zones cannot be
   even. The floor is a skew of 1, and alerting on it is noise.
2. **Not every domain is eligible.** A workload with a `nodeSelector` for GPU
   nodes should only be compared against zones that *have* GPU nodes. Comparing
   against all zones guarantees a permanent false alarm.
3. **Domains are not equal.** Three zones with 40/40/20 of the cluster's capacity
   should not be expected to hold 33/33/33 of the pods.
4. **Transient skew is normal.** Rollouts, HPA scaling, node drains and spot
   reclaims all produce short-lived imbalance that self-corrects.

If we get the "what is normal" question wrong, the rest of the tool does not
matter. §7 is therefore the substance of this document; almost everything else is
plumbing that lookout already owns.

### 1.2 Two axes, one idea

Leeway measures drift from declared intent along two different kinds of axis:

- **Topology axes** — zones, regions, racks. Domains are **peers**; the intended
  distribution is even (or capacity-weighted), and deviation is measured as
  imbalance.
- **Preference axes** — ordered fallback lists, initially GKE custom compute class
  priorities. Tiers are **ranked**; the intended distribution is all mass at
  rank 0, and deviation is measured as depth.

They share the intent model, the delta engine, the baseline machinery, the dwell
and hysteresis state machine, and the false-positive discipline. They do *not*
share a scoring function, and §7.7 is explicit about why reusing the topology
score on a preference axis would be a category error.

---

## 2. Where this fits in `k8s-lookout`

### 2.1 Why lookout and not a new repo

Most of the substrate this design needs already exists in lookout, built and
tested:

| What leeway needs | What lookout already has |
|---|---|
| Cluster-wide Pod + Node informers | `object-state` runs them; DESIGN §6.3 — "one informer set serves the sentinel sources *and* the graph" |
| Pod → node → zone relation | `pkg/graph` implements `Node → RunsOn(Zone)` (`derive.go:350`), with the beta-label fallback, interned `NodeID uint32`, COW snapshots |
| Alert state machine, dedup, fingerprinting | `pkg/engine` |
| Durable state, pruning, history | `pkg/store` |
| Prometheus registry, `/metrics`, cardinality capping | `internal/watch/metrics.go` (note `reasonLabelCap` — the same label-explosion problem, already solved once) |
| OTEL bootstrap | `internal/telemetry/otel.go` (traces today; metrics is an extension, §8.4) |
| GKE API access behind a provider boundary | `pkg/cloud/gke` |
| Read-only RBAC with a loud startup probe | `pkg/sources/rbac.go`, `sources.AccessDeclarer` |
| Release, signing, SBOM, Helm, docs site | already shipping |

And `pkg/sources/sources.go` provides the exact seam this plugs into:
`Name() / Scope() / Run(ctx, emit func(Signal)) error`.

There is also a natural pairing on the read path. `pkg/checks/audit/workloads.go:502`
already emits `NoTopologySpread` — *"you declared no spread intent."* Leeway is the
runtime complement — *"you declared spread intent and it isn't holding."* Those two
belong in one repo, one docs site, one finding-kind ledger.

**The decisive argument is the informer.** lookout DESIGN §11 makes one sentinel
per cluster canonical: *"one informer cache, one topology index, one credential
boundary, one failure domain."* A separate resident binary means every cluster runs
a **second cluster-wide Pod informer**. This design spends its entire cost model
(§6) minimising watch-path expense — protobuf, field selectors, object trimming, no
polling. Running a second process pays that whole budget again to decode the
identical bytes, for zero additional information. That is the design's own cost
model arguing against shipping it standalone-by-default.

### 2.2 Package layout

```
pkg/leeway/                     the scoring engine — pure, no client-go
  intent.go                     normalised Intent, precedence resolution
  eligible.go                   eligible-domain computation (§7.1)
  apportion.go                  Hamilton apportionment, water-filling caps (§7.2)
  score.go                      skew, ρ, R, Herfindahl, χ² (§7.3)
  baseline.go                   EWMA/EWMAD, freeze, maturity, invalidation (§7.5)
  rank.go                       preference axes, assignRanks, pod-seconds (§7.7)

pkg/sources/topology-drift/     resident source: topology axes
pkg/sources/compute-class/      resident source: preference axes

cmd/leeway/                     optional standalone binary (§2.4)
```

`pkg/leeway` is deliberately shaped like `pkg/graph`, which DESIGN §6 describes as
*"adopted from the k8s-graph proposal, right-sized, and demoted from product to
package: it has no CLI of its own."* Same story here, and for the same reason: three
callers justify the boundary — the two watch sources, a future read-path
`lookout state drift` one-shot, and the existing `audit` check, which can gain a
"you have intent and it is currently violated" variant once the scoring is a
library.

### 2.3 Source names, finding kinds, metric names

Three different naming systems, and they should not all get the same answer.

- **Source names stay descriptive: `topology-drift` and `compute-class`.** These
  are user-facing config (`--sources=`) and every one of the fourteen siblings is
  self-describing — `object-state`, `saturation`, `token-burn`. `--sources=leeway`
  tells an operator, or an agent reading the flag list, nothing.
- **Finding kinds are `leeway.*`.** `docs/signal-schema-v1.md` is explicit that
  *"`kind` is the detector's own (`audit.no_pdb`)"*, so the prefix tracks the
  detector, not the source; `audit.*` is already a subsystem prefix spanning
  several commands. The settled set is below.
- **Metrics are `lookout_leeway_*`**, matching the existing `lookout_*` prefix.

> **The kinds are the expensive part.** `TestSchemaV1_KindInventory` pins the
> inventory and v1 is frozen, so kind names are a durable commitment in a way that
> source names, package names and metric names are not. They should be settled
> before the first source lands, not during.

**Settled 2026-09-17.** Eight kinds, taking the v1 inventory from 49 to 57. The Go
constants land with their sources in phases 3 and 7; this table is the commitment.

| Kind | Source | Tier | Fires when |
|---|---|---|---|
| `leeway.contract_violated` | `topology-drift` | A | A declared `DoNotSchedule` TSC or required anti-affinity is being violated |
| `leeway.placement_drift` | `topology-drift` | B | Inferred intent deviation — `ρ` over threshold (§7.3) |
| `leeway.baseline_breach` | `topology-drift` | C | Deviation from the subject's own history (§7.5) |
| `leeway.domain_unavailable` | `topology-drift` | B | A domain the cluster has, or recently had, now has no *usable* node |
| `leeway.rank_wedged` | `compute-class` | A | Pods Pending against a `DoNotScaleUp` class (§7.7.4) |
| `leeway.rank_degraded` | `compute-class` | B/C | Rank-0 share below threshold, sustained time at the last rank, or mean achieved rank rising against its own baseline |
| `leeway.rank_no_migration` | `compute-class` | B | No `from_rank > to_rank` transitions after capacity returns |
| `leeway.rank_tier_unused` | `compute-class` | info | An entire preference tier unused in 30 d |

Five of these differ from the names this section first proposed, and the reasons are
worth keeping, because each one is an argument about what a kind is *for*.

- **`leeway.skew` became `leeway.placement_drift`.** `skew` put the vaguest name on
  the most-emitted kind; `observed_skew` and `excess_skew` are already metric names,
  so a finding called `skew` reads as though it were about that one metric rather
  than about `ρ`; and it is simply wrong for `ModeColocate`, where the complaint is
  that pods spread out when they should not have.
- **`leeway.pinned_skew` was dropped**, and pinning became a `suspectedCause` on
  `leeway.placement_drift` (§8.5). The payload already carries that field, pinning is
  one cause among several, and promoting exactly one of them to its own kind is
  inconsistent. It also costs nothing in dedup: the §8 fingerprint hashes
  `reasonClass` alongside `kind`, so a distinct cause already gets a distinct
  identity without spending a frozen name on it.
- **`leeway.rank_depth` split into `leeway.rank_degraded` and
  `leeway.rank_tier_unused`.** One kind was carrying four triggers across three
  tiers. The split is not symmetry for its own sake — a dead preference tier is a
  *cost* finding whose subject is the compute class and whose remedy is deleting the
  tier or the reservation, while the rest are availability findings about workloads.
  Different reader, different action. Mean-rank-against-baseline stays inside
  `rank_degraded` rather than routing to `leeway.baseline_breach`, because the rank
  payload is shaped around a rank histogram and sharing the Tier C kind would force
  two payload shapes through one name.
- **`leeway.domain_outage` became `leeway.domain_unavailable`.** We cannot observe an
  outage. We observe that a domain's eligible node count reached zero, which a
  cordon, a taint or a `nodeSelector` edit produces just as readily as a zone
  failure. The name should not assert a cause we did not measure.

  > **Shipped 2026-09-21, and three things about the rule are worth stating here
  > rather than only in the code.** *It counts **usable** nodes — Ready **and**
  > schedulable — not Ready ones.* That single choice is what makes one rule cover
  > both halves of Phase 7's exit criterion: a zone whose nodes are all NotReady and
  > a zone whose nodes are all cordoned are the same finding with different
  > `suspectedCause` values (`domain_outage` and `taint_exclusion`), which is
  > precisely what the rename exists to express. *It is `Known && usable == 0`, not
  > "fewer than the peak inside the window".* The ready series is retained for hours
  > and no longer, so a window rule would have resolved the finding on its own
  > evidence ageing out while the zone was still down; the source instead latches
  > which domains the cluster has, and forgets a domain only when it has neither
  > nodes nor history — at which point we can no longer tell a deliberately deleted
  > zone from a failed one, and asserting the second would be inventing the
  > distinction. *And the axis set is configuration with a narrow default
  > (`--topology-domain-unavailable-keys`, zone and region).* `kubernetes.io/hostname`
  > is a topology key on which every node is its own domain; judged there, this kind
  > would raise a finding per NotReady node, which is `objectstate`'s observation
  > with `objectstate`'s subject.
- **`leeway.baseline_breach` is new: Tier C had no kind at all.** §8.3 routes Tier C
  to metrics by default *but allows a Signal at `info`, opt-in per policy*. A kind
  that is reachable by configuration has to exist in the frozen v1 inventory before
  the source ships, or the opt-in cannot be honoured later without a schema revision.

**`leeway.placement_drift` and `audit.no_spread` are not duplicates and must not be
merged.** `audit.no_spread` is a static claim about a template — this workload
declares no spread constraint — answered from the spec at audit time. `leeway.*` is
the runtime observation that pods are not where the declared or inferred intent puts
them, which fires precisely on the workloads whose templates look correct. The same
distinction holds between `audit.rigid_scheduling` and `leeway.domain_unavailable`.

> **All eight are in the v1 ledger as of 2026-09-21.** `pkg/inject/schema` lists
> what CAN be emitted, not what has been named, and the freeze test counts it — so
> a kind enters the ledger in the change that gives it a producer.
> `leeway.contract_violated` and `leeway.placement_drift` arrived in Phase 4,
> `leeway.baseline_breach` in Phase 5 when Tier C's learned baselines turned on,
> the four `leeway.rank_*` kinds in Phase 6, and `leeway.domain_unavailable` last,
> in Phase 7, because it is raised by the source rather than by a verdict. That the
> names were settled here is what made each of those additions mechanical — the
> commitment was made once, in this table, and the ledger caught up to it eight
> times without a rename.

### 2.4 Standalone binary

`cmd/leeway` builds the same two sources against their own informer set, its own
`MeterProvider`, and an `emit` that writes metrics instead of injecting agent
sessions. This is cheap to keep working, but only if four things hold from day one —
retrofitting any of them is expensive:

1. **`pkg/leeway` never imports client-go.** Pure functions over plain structs.
   This is what makes it embeddable in something that is not lookout at all.
2. **Sources take their informers as a parameter.** Lookout injects the shared set;
   `cmd/leeway` builds its own. This is the discipline most likely to be violated
   by accident, because calling `NewSharedInformerFactory` inline is the natural
   thing to write — and the repo already has the right habit: eight sources expose
   `WithFactory(informers.SharedInformerFactory)` and fall back to constructing
   their own only when none is injected (`objectstate.go:439`, `rollout.go:247`,
   and six more). Leeway follows that shape exactly, with one deviation: it should
   accept a *narrow* interface (register handler, get lister) rather than the
   concrete client-go factory, since the concrete type is what would drag client-go
   back into anything embedding this.
3. **The meter and the store are injected too.** Standalone may run store-less —
   §9.1 establishes that current state is rebuildable, so the cost is losing dwell
   timers across a restart, not losing correctness.
4. **Sources import `pkg/*` only, never `internal/watch`.** They are in the same
   module, so nothing in the compiler stops them. This is the trap that would
   quietly weld the sources to the sentinel.

**What it is for:** clusters not running lookout; a metrics-only deployment whose
consumer is Grafana and a rotation rather than an agent; isolating the subsystem
under kwok for scale runs; vendoring `pkg/leeway` elsewhere.

**What it must not become:** a second process on a cluster already running
`lookout watch`. That reintroduces the duplicate Pod informer §2.1 exists to avoid.
This belongs in the deployment docs as an explicit constraint, because otherwise
someone will do it.

**Two honest caveats.** First, DESIGN §4.1 is "one multicall binary"; a second
`cmd/` is a narrow exception (an optional artifact, not a second user-facing CLI
surface) and folding this in should say so rather than quietly adding it. Second,
disciplines 1 and 4 are conventions unless enforced. lookout documents the
analogous provider boundary in four places — `AGENTS.md:37`, `DESIGN.md:171`,
`repo-map.md:124`, `CONTRIBUTING.md:82` — and enforces the cloud half structurally
with build tags plus `pkg/cloud/noprovider.go`, but there is no general
import-graph test. One is needed here, and `cmd/leeway` must be built in CI: a
binary nobody builds is a binary that does not work.

### 2.5 What folding in changes about this design

Four things, all load-bearing:

1. **We do not own the informers, so `trimPod` becomes a negotiation.** See §6.1 —
   this is the one place where the fold-in has a real cost, and it invalidates part
   of the original memory model.
2. ~~**The scale posture needs reconciling.**~~ **Resolved (2026-09-15): the higher
   targets win, and DESIGN §6.2 was edited to match.** That paragraph previously
   read *"100k is the ceiling, not the design point"* against this document's 200k
   pods at 500 pods/sec. The two were never really in conflict — they were one
   sentence covering two different questions. §6.2 now states them separately:
   pods are a **memory** question (200k target, no cliff before ~500k, dominated by
   the informer caches), events/sec are a **CPU** question (500 pods/sec ≈ 5,000
   events/s ≈ a quarter core across the whole watch path). The 50k events/s target
   stays dropped as fiction — nothing about a bigger cluster gets you there. A
   200k-pod cluster is ten times the memory of a 20k-pod one and roughly the same
   CPU.
3. **Output shape.** Lookout's watch path turns signals into agent sessions with
   warm context. Drift is a slow-moving condition, not an incident. §8 handles this
   by routing most leeway findings to the store and metrics rather than to inject,
   using the existing severity routing (DESIGN §7.7) — the same treatment
   `notifications` info-severity signals already get.
4. **Two metric APIs in one binary, deliberately.** Spike S11 established that
   `otelprom` bridges leeway's OTEL-native instruments into the registry the
   sentinel already serves, so `/metrics` stays one scrape and no migration is
   needed. The accepted asymmetry is on the push side: an OTLP backend sees
   leeway's metrics and none of the sentinel's 43 until someone files that refactor
   on its own merits. §8.4 and §13/S11 carry the numbers and the two regressions
   that argue against doing it on leeway's schedule.

---

## 3. Goals and non-goals

### 3.1 Goals

| # | Goal |
|---|------|
| G1 | Infer desired topology distribution from declarative signals (TSC, affinity, node selectors, tolerations, volume topology) |
| G2 | Track actual distribution of pods and nodes across configurable topology keys |
| G3 | Distinguish structurally-unavoidable skew and transient churn from genuine drift |
| G4 | Support explicit, user-declared topology policy where inference is insufficient |
| G5 | Learn per-subject behavioural baselines so drift is detectable with no declared intent |
| G6 | Alert with configurable, layered thresholds and dwell times |
| G7 | Update state incrementally from watch deltas — no periodic full recomputation on the hot path |
| G8 | Survive process restarts without losing dwell timers, alert state, or learned baselines, and without an alert storm on startup |
| G9 | Explain every finding: which domains, by how much, and the most likely cause |
| G10 | Track preference-rank axes — ordered fallback lists such as GKE compute class priorities — measuring which priority is used *over time*, not just which is occupied now (§7.7) |
| G11 | Add no new cluster-wide watch streams; run inside the existing sentinel (§2.1) |
| G12 | Keep the scoring engine free of client-go so it is usable one-shot, resident, and embedded (§2.4) |

### 3.2 Non-goals (v1)

- **Automatic remediation.** No eviction, no rebalancing, no descheduler
  behaviour. We emit signal.
- **Cloud provider API access for node groups.** Node-group intent comes from node
  labels and in-cluster CRs, not ASG/MIG APIs. (The GKE `ComputeClass` CR is
  in-cluster, so §7.7 is not an exception to this.)
- **Multi-cluster aggregation.** Per lookout DESIGN §11, fan-in is the fleet
  layer's job. Signals carry `fingerprint`/`cluster`/`zone` so a fleet layer can
  roll up; leeway does not become that layer.
- **Scheduler simulation.** We report deviation from expectation, not
  schedulability.
- **Network-topology or latency-based drift.** Label-based domains only.

---

## 4. Requirements

### 4.1 Functional

**Discovery & modelling**

- **FR-1** Discover topology domains from node labels for a configurable set of
  topology keys. Defaults: `topology.kubernetes.io/zone`,
  `topology.kubernetes.io/region`, `kubernetes.io/hostname`.
- **FR-2** Group pods into **subjects** by owner chain (Pod → ReplicaSet →
  Deployment; StatefulSet; DaemonSet; Job → CronJob; unrecognised CRD owners
  handled generically via `ownerReferences`), and into policy-defined subjects via
  label selectors.
- **FR-3** Track nodes as subjects, grouped by inferred node group, with a
  configurable label-key precedence list (`--topology-node-group-keys`). Compute
  class outranks node pool because auto-created pools are per-machine-type and
  ephemeral (§11). Three rules follow from what a node group *is*, and phase 7
  settled all three:
  - **No intent, ever.** §3.2 rules out the ASG/MIG APIs, so a node group declares
    nothing this process can read, and §8.1 reads a non-nil intent as the difference
    between Tier C and Tier B. Handing these subjects a synthetic intent — to carry a
    weighting, or anything else — would silently promote every pool in an estate to a
    tier that emits by default. They are Tier C, which means metrics-only unless
    `--topology-tier-c-signals`.
  - **Eligibility is cluster-wide.** A pool's *configured* zones are not in the
    labels, so the only statement the evidence supports is "this pool sits in one of
    the N zones this cluster spans". A deliberately zonal pool therefore reads as
    drifting; that is the second reason these subjects stay Tier C, and why the
    precedence list is configurable down to nothing.
  - **A cardinality bound, `--topology-max-node-groups` (default 200).** Past it,
    *no* groups are tracked rather than an arbitrary map-order subset, and
    `lookout_leeway_node_groups_discovered` keeps reporting the true count — the one
    reading that sends an operator to the precedence list. A negative bound is the
    off switch. An estate with auto-created pools and a misconfigured list degrades
    visibly instead of emitting thousands of series.

**Intent inference**

- **FR-4** Parse `topologySpreadConstraints` including `maxSkew`, `topologyKey`,
  `whenUnsatisfiable`, `minDomains`, `labelSelector`, `matchLabelKeys`,
  `nodeAffinityPolicy`, `nodeTaintsPolicy`.
- **FR-5** Derive spread intent from `podAntiAffinity`, required and preferred.
- **FR-6** Derive *co-location* intent from `podAffinity` — for those subjects
  concentration is desired and spread is the anomaly.
- **FR-7** Compute the **eligible domain set** per subject from `nodeSelector`,
  required `nodeAffinity`, tolerations vs. node taints, and node
  readiness/schedulability. Expectations are computed over eligible domains only.
- **FR-8** Account for volume topology: pods bound to PVCs whose PVs carry
  `nodeAffinity` are pinned and cannot rebalance. Report as *pinned skew* — a
  distinct, lower-severity category, because alerting a human about drift they
  cannot fix is a bug.
  > **Shipped 2026-09-19, and the rule is narrower than "carries
  > `nodeAffinity`".** A volume constrained to `kubernetes.io/os=linux` carries
  > node affinity and pins nothing, because every domain on every scored axis
  > still holds a linux node. What the predicate asks instead is whether the
  > volume's required affinity constrains an axis *we are about to report drift
  > on* — the configured `--topology-keys`, plus `kubernetes.io/hostname`
  > always, since one node is one domain on every axis there is and local
  > volumes pin the zone transitively. Node-selector terms are ORed, so a
  > volume pins only when **every** term does: one unconstrained term is an
  > escape route. `Exists` on a scored key is not a constraint (every node in
  > every domain carries the label — that is what makes it an axis).
  >
  > Two imprecisions are deliberate and recorded on `pvPins`. A volume
  > constrained on an unscored key (a rack label) reads unpinned, which
  > under-reports into the *visible* direction — a finding a human dismisses,
  > not one they never see. A volume enumerating every domain on a scored axis
  > reads pinned, which is the silent direction, tolerated only because no
  > provisioner emits one: a zonal disk names its zone, a regional disk names
  > its two.
  >
  > There is no PVC or PV event handler. A pod whose claim has not bound is a
  > pod that has not been scheduled, so it holds no domain and contributes to
  > no distribution; the event that schedules it is a pod event the source
  > already acts on. Watching the volumes would make `Pinned` depend on a
  > second informer's arrival order — the trap that ruled out a subject-keyed
  > pod index in §6.2.
  >
  > "Report as *pinned skew*" above is superseded: `leeway.pinned_skew` was
  > dropped as a kind, and pinning is a `suspectedCause: volume_pinning` on
  > `leeway.placement_drift` (§8.2).

- **FR-9** Support the scheduler's `PodTopologySpread` `defaultConstraints` as
  static configuration, since they are not readable from the API server (spike S4,
  confirmed on managed GKE). The configuration is three-state — *unset*, *declared
  empty*, *declared* — and intent derived from a cluster default that was assumed
  rather than declared is marked as such and can never raise a Tier A finding
  (§13 S4).

  > **Shipped 2026-09-19 as `--topology-cluster-defaults`.** Three states on a CLI
  > flag need one of them to have a spelling, and "absent" is the one that cannot:
  > unset is the flag's empty default (assume the upstream set), the literal `none`
  > is declared-empty, and `key=maxSkew[:action]` comma-separated is declared. An
  > omitted action parses as `ScheduleAnyway`, which is deliberately *not* the API's
  > default for a pod's own constraint — the API reads an unset
  > `whenUnsatisfiable` as `DoNotSchedule`, and here that would let a typo promote
  > a cluster-wide assumption into something that can raise Tier A. The flag is
  > parsed twice, once in `validate()` and once at construction, so a malformed
  > value stops startup instead of falling through to the assumed set, which would
  > be the S4 failure wearing a different hat.
  >
  > **One thing the design did not anticipate: cluster defaults change who has an
  > intent at all.** Every other source only fires where a human wrote something,
  > so `intent_info` was sparse. An assumed default fires on *every* subject, which
  > at §8.4's 20k-subject baseline takes the metric from ~3.5k series to ~40k for
  > the two-axis upstream set. Cluster-default intents are therefore the one source
  > filtered to the *counted* axes (`onlyTrackedAxes`): nobody declared them, an
  > untracked axis will never be scored, and being the lowest-precedence candidate
  > they cannot even survive as demoted evidence. The tracked-axis rows stay,
  > because those are exactly what S4's "how much of this fleet's intent rests on a
  > guess?" needs to be answerable.

- **FR-10** Allow explicit declaration of intent via CRD, overriding inference.

  > **Shipped 2026-09-19 as `LeewayPolicy` / `ClusterLeewayPolicy`** (§10.1),
  > `deploy/crds/leewaypolicies.yaml`, `helm --set leewayPolicyCRD.install=true`.
  > Four decisions worth keeping:
  >
  > **Optional, which inverts the gateway source's precedent.** That source fails to
  > start when its CRD is absent, because there the CRD is the whole subject. Here
  > the CRD is an override on a default-on source that works completely without it,
  > so an absent CRD logs one line and continues. The discovery gate is evaluated
  > once at startup: watching for the CRD itself to appear would cost a watch on
  > `apiextensions` — a grant every deployment would then carry — to save a restart
  > on a one-off installation step.
  >
  > **Only the honoured subset is in the schema.** §10.1 also sketches `thresholds`,
  > `baseline` and `exclusions`. A structural schema prunes what it does not declare,
  > so shipping them ahead of the code that reads them would let an operator write a
  > threshold, see it accepted, and believe it was in force. They are absent instead,
  > which fails visibly; adding them later is additive.
  >
  > **Ambiguity is reported, not resolved.** A namespaced policy beats a
  > cluster-scoped one — it names a smaller set of subjects outright — but between
  > two policies at the same scope there is no honest ordering: "more specific
  > selector" has no definition spanning a `matchLabels` set and an `Exists`
  > expression. The store picks lexicographically, applies the winner *whole* with no
  > per-key merging, and logs the tie once per competing set naming both policies.
  >
  > **The allowlist is applied before precedence.** A source excluded by
  > `inference.sources` must not survive as demoted evidence either, because evidence
  > is what a finding quotes to justify itself and quoting a term the policy said to
  > disregard makes the finding unanswerable. The policy's own intent is never
  > filtered by its own allowlist.
  >
  > **A defect found on the way in.** Precedence decides whose *description* of
  > intent wins, but a `DoNotSchedule` maxSkew is not a description — it is a promise
  > Kubernetes made at admission and kept regardless of who outranked it. A
  > higher-precedence source expressing no bound was erasing it, silently downgrading
  > a Tier A violation to an ordinary distributional observation; a policy outranks
  > everything and carries no maxSkew, so FR-10 would have hit this on every subject
  > that had both. The carry is narrow: a required podAntiAffinity's `MaxPerDomain`
  > is an equally real promise and deliberately does *not* carry, because
  > `HardContract()`'s only consumer pairs it with `ExcessSkew`, which is derived
  > from `MaxSkew` alone — carrying the ceiling would report Tier A about a
  > `ScheduleAnyway` bound Kubernetes never refused to exceed. Reporting it honestly
  > needs `Breach` to compare it against the observed per-domain maximum, which is a
  > scoring rule and belongs in Phase 4.

**Measurement**

- **FR-11** Compute per (subject × topology key): per-domain counts, observed
  max-min skew, minimum achievable skew, excess skew, expected distribution,
  relocation distance, normalised drift (§7.3).
- **FR-12** Support capacity weighting: expectations proportional to per-domain
  allocatable CPU/memory or schedulable node count.
- **FR-13** Count pod states separately: `Running`/`Ready`, `Pending`
  (schedulable vs. unschedulable), `Terminating`. Unschedulable pods in a
  constrained domain are a leading indicator, not noise.
- **FR-14** Maintain per-subject historical baselines of domain share.

**Preference-rank tracking**

- **FR-15** Discover ordered preference axes from cluster resources — initially GKE
  `ComputeClass` CRs — via a dynamic informer, degrading to disabled (not failing)
  when the CRD is absent. This is the `gateway` source's discovery-gated
  dynamic-informer precedent, reused.
- **FR-16** Resolve each pod's achieved rank from its node, using a configurable
  label-extraction table rather than hard-coded platform label keys.
- **FR-17** Accumulate **time-weighted** occupancy (pod-seconds) per rank, so
  "which priority is used most of the time" is answerable over arbitrary windows.
  Instantaneous counts do not answer it.
- **FR-18** Report rank attribution quality (unmatched, ambiguous, disagreement) as
  tool-health signals, distinct from cluster findings.
- **FR-19** Detect and surface changes to an axis's rule list, since a rank's
  meaning is not stable across edits.

**Alerting**

- **FR-20** Three-tier classification by confidence (§8.1).
- **FR-21** Thresholds layered global → policy → per-workload annotation.
- **FR-22** Dwell before firing and before resolving, independently configurable;
  timers survive restart.
- **FR-23** Suppress or relax during known-transient states: in-progress rollout,
  recent HPA scale event, node drain, cluster warmup.
- **FR-24** Emit `Signal`s through the existing pipeline, and metrics through the
  sentinel's `MeterProvider` — exportable by Prometheus pull and OTLP push
  simultaneously from one set of instruments (§8.4).
- **FR-25** Every finding carries structured evidence: intent source, expected vs.
  actual per domain, contributing factors, suspected cause.

**Operations**

- **FR-26** Incremental, delta-driven state updates from informer events.
- **FR-27** Durable alert state, dwell timers, baselines and episode history via
  `pkg/store`.
- **FR-28** Warmup suppression on startup, aligned with the sentinel's existing
  `SyncReporter` "armed" flip.
- **FR-29** Self-verification: periodically rebuild counters from cache and compare
  against the incrementally-maintained ones (§6.5).
- **FR-30** Read-only RBAC, declared through `sources.AccessDeclarer` so a missing
  permission fails loudly at startup rather than producing an empty watch.

### 4.2 Non-functional

| # | Requirement | Target |
|---|---|---|
| NFR-1 | Scale | 200,000 pods at 500 pods/sec sustained scheduling. Headroom tiers in §6.6. Nothing is bounded by node count — memory is linear in **pods**, CPU in **event rate** |
| NFR-2 | Marginal memory | ≤ 120 MiB above the sentinel's existing footprint at the design target (leeway's own indexes; the object caches are already paid for) |
| NFR-3 | Detection latency | p95 < 15 s from pod event to metric update at steady state; < 90 s while absorbing a full-domain reschedule (§6.6.2) |
| NFR-4 | Marginal CPU | < 0.15 core at 500 pods/sec. The decode is already paid for by the shared informer; leeway pays only relevance-check, delta-apply and evaluation (§6.6.1) |
| NFR-5 | API server load | **Zero new cluster-wide watch streams.** One additional discovery-gated CRD watch for `ComputeClass`, which is a handful of objects |
| NFR-6 | Burst tolerance | Absorb a full-domain reschedule (~66k pods) with bounded memory and degraded-but-recovering latency, never OOM (§6.6.2) |
| NFR-7 | Availability | Inherits the sentinel's. State is rebuildable; dwell timers are durable |
| NFR-8 | Security | Read-only. No secret values in process memory (§6.1). No egress except the sentinel's configured sinks and OTLP endpoint |
| NFR-9 | Export isolation | An unreachable OTLP collector degrades to dropped points, never unbounded queueing; Prometheus pull unaffected (§8.4) |
| NFR-10 | Embeddability | `pkg/leeway` has no client-go dependency; `cmd/leeway` builds and is exercised in CI (§2.4) |

NFR-2 and NFR-4 are stated as *marginal* cost, which is the whole point of §2.1 —
the expensive parts (watch, decode, object cache) are already in the process.

---

## 5. Domain model

```go
// TopologyKey is a node label key defining a domain axis.
type TopologyKey string

// Domain is a value of that label. Empty normalises to DomainUnknown so
// unlabelled nodes are visible rather than silently dropped.
type Domain string

const DomainUnknown Domain = "__unknown__"

// SubjectRef identifies the thing whose distribution we track.
type SubjectRef struct {
    Kind      SubjectKind // Deployment, StatefulSet, DaemonSet, Job, NodeGroup, Custom
    Namespace string      // empty for cluster-scoped subjects
    Name      string
}

// Placement is the per-object index entry that makes delta updates possible:
// it records where we last counted this object, so a delete or a move can be
// applied without consulting the previous version of the object.
//
// One of these exists per pod, so its layout dominates leeway's marginal memory
// (NFR-2): at 200k pods every 32 bytes here costs another 6.4 MiB. Domains is a
// slice indexed by topology-key ordinal (the key set is fixed at startup) rather
// than a map — a Go map per pod would cost ~250 B against ~50 B for the slice.
// NodeName and Domain values are interned; where pkg/graph already interns the
// same strings, share its interner rather than building a second one.
type Placement struct {
    NodeName string     // interned
    Domains  []Domain   // indexed by topology-key ordinal; interned values
    State    CountState // Running, Pending, Unschedulable, Terminating
    Pinned   bool       // bound to zonal volume(s)
}

type Distribution struct {
    ByDomain map[Domain]*DomainCount
    Total    int64
}

type DomainCount struct {
    Running, Pending, Unschedulable, Terminating, Pinned int64
}
```

### 5.1 Intent

Intent is resolved into one normalised structure regardless of source, so the
scoring engine has a single input shape — and so `pkg/leeway` can be tested with
no cluster at all:

```go
type Intent struct {
    TopologyKey TopologyKey
    Mode        IntentMode // Spread, Colocate, Ignore
    Source      IntentSource

    // Hard contract, when one exists (from TSC).
    MaxSkew           *int32
    MinDomains        *int32
    WhenUnsatisfiable v1.UnsatisfiableConstraintAction
    MaxPerDomain      *int64 // required podAntiAffinity's ceiling; see below

    // Expectation shaping.
    Weighting       Weighting // Equal, NodeCount, AllocatableCPU, AllocatableMemory
    EligibleDomains sets.Set[Domain]
    DomainCaps      map[Domain]int64
    Policies        NodeInclusionPolicies // read via EligibilityPolicies(), never directly

    Confidence Confidence // Declared, Inferred, Assumed, Learned
    Evidence   []EvidenceItem
}
```

> `MaxPerDomain` was added during Phase 3, also a delta. It is the contract a
> *required* `podAntiAffinity` expresses — at most one of the subject's pods per
> domain on the term's topology key — and it is deliberately not modelled as a
> `MaxSkew` of one. A skew bound of one is satisfied by two pods in every domain,
> which the anti-affinity forbids outright; and in the other direction a required
> anti-affinity holds the observed skew at one by construction, so a skew check
> against it could never fire. What it does catch is the `IgnoredDuringExecution`
> half: a pod placed legally, then a node relabelled so two of them share a domain
> after the fact. Callers turn it into the per-domain caps `Apportion` already takes
> once §7.1 has decided how many domains there are; inference runs before that.
>
> A required `podAffinity` gets no ceiling and is not a hard contract, even though
> the scheduler enforces it just as hard. §8.1's Tier A is about placement Kubernetes
> promised and did not deliver, and an affinity that was satisfied at every placement
> broke no promise — "further apart than the affinity would have put them" is a
> deviation to explain, which is Tier B's job.

> `Policies` was added during Phase 3 and is a delta from this section as first
> written. `nodeAffinityPolicy` and `nodeTaintsPolicy` shape the eligible set
> (§7.1) and only a TopologySpreadConstraint can express them, so every other
> source leaves them empty — but the zero value of `NodeInclusionPolicy` is the
> empty string, and §7.1 reads anything that is not `Honor` as "do not apply".
> An unset field travelling as itself would therefore stop `nodeSelector` being
> honoured, widen the eligible set to the whole cluster, and make every pinned
> workload look like it was drifting. `EligibilityPolicies()` substitutes
> kube-scheduler's defaults at the point of use and answers on a nil receiver,
> because a subject with no intent at all still has an eligible set to compute.

**Precedence** (highest first): `SourcePolicyCRD` → `SourceWorkloadAnnotation` →
`SourceTopologySpreadConstraint` → `SourcePodAntiAffinityRequired` →
`SourcePodAffinityRequired` → `SourceClusterDefaultDeclared` →
`SourcePodAntiAffinityPreferred` → `SourcePodAffinityPreferred` →
`SourceLearnedBaseline` → `SourceClusterDefaultAssumed`. Multiple intents on
*different* topology keys coexist; on the same key the highest-precedence source wins
and the others are retained as evidence.

> **`SourceClusterDefaultAssumed` moved to the bottom of this list in Phase 5,
> 2026-09-20.** It used to sit sixth, above both preferred affinity sources and
> above the learned baseline. That ordering made §7.5 unreachable: the assumed
> defaults cover the zone axis and apply to exactly the population baselines are
> learned for — pods that declare no `topologySpreadConstraints` — so on any
> cluster that had left `--topology-cluster-defaults` unset, which is the default
> and the majority, the guess won every axis a baseline would have spoken on.
> Tier C would have shipped and never once scored a subject against a learned
> intent.
>
> It is the better order on its own terms too, independently of §7.5. Every other
> entry here is something *somebody said*: an operator, a workload author, or the
> workload's own observed behaviour. `Assumed` alone is our reconstruction of a
> `kube-scheduler` configuration S4 confirmed we cannot read, and a guess belongs
> below every answer — including a soft `preferredDuringScheduling` term, which is
> at least the workload author expressing a wish. §8.1 sharpens the point: an
> assumed intent is capped at Tier B, which signals, while a learned baseline is
> Tier C, which is metrics-only. Under the old order a workload nobody had declared
> anything about got *louder* treatment for being unmeasured than for being
> measured.
>
> The enum is ordinal, so this renumbers the constants — but nothing persists the
> number. The kebab-case `String()` spellings are what reach the `intent_info`
> label and the CRD's `inference.sources` enum, and those are unchanged; only the
> order the CRD lists them in moved, which is documentation rather than schema.

> The two `PodAffinity` sources were added in Phase 3 and are a delta from this list
> as first written, which had only the anti-affinity halves. FR-6 requires colocation
> intent from `podAffinity` and that intent has to be able to say where it came from
> — reusing an anti-affinity source would put `source="pod-anti-affinity-required"`
> on the `intent_info` label of something that is not one. Each sits immediately
> below its anti-affinity counterpart, so a subject declaring both on one key — a
> contradiction the scheduler resolves by refusing to place the pod at all —
> resolves here to the spread reading with the colocation kept as evidence. The
> CRD's `inference.sources` list (§10.1) takes the same two additions.

The cluster-default source is split in two because S4 confirmed we cannot read the
scheduler's configuration on managed GKE, so "the cluster default is X" is sometimes
an operator's assertion and sometimes our assumption — and those must not be the same
value in the model. `Assumed` is a distinct `Confidence` for the same reason. §8.1
caps an `Assumed` intent at Tier B and §13 S4 has the rest.

> `Intent` uses `v1.UnsatisfiableConstraintAction` from `k8s.io/api`, which is a
> types-only dependency, not client-go — NFR-10 is about the client, not the API
> types. If even that proves awkward for embedding, mirror the two constants
> locally; it is a two-value enum.

---

## 6. Delta-driven state maintenance

A pod event must cost O(topology keys), not O(pods). This section is what NFR-3
and NFR-4 rest on.

### 6.1 What we need from the shared informer set

This is the one place where folding in has a real cost, and it is worth being
precise about rather than discovering during implementation.

**Fields leeway reads.** Pods: `metadata` (uid, name, namespace, labels,
ownerRefs, deletionTimestamp, creationTimestamp), `spec.nodeName`,
`spec.nodeSelector`, `spec.affinity`, `spec.tolerations`,
`spec.topologySpreadConstraints`, `spec.volumes[].persistentVolumeClaim`,
`status.phase`, `status.conditions[PodScheduled]`. Nodes: `metadata.labels`,
selected `metadata.annotations`, `spec.taints`, `spec.unschedulable`,
`status.allocatable`, the `Ready` condition.

**Two client settings are load-bearing at 500 pods/sec** and should be verified on
the sentinel's existing client rather than assumed:

```go
// Protobuf, not JSON. client-go defaults to JSON, which costs roughly 4–5× more
// CPU per decoded pod — at the design event rate, the difference between ~0.2 and
// ~1.0 cores spent purely on deserialisation. CRDs are JSON-only, so the dynamic
// ComputeClass client keeps the default; it is low-volume and does not matter.
cfg.AcceptContentTypes = "application/vnd.kubernetes.protobuf,application/json"
cfg.ContentType = "application/vnd.kubernetes.protobuf"

// Drop COMPLETED pods server-side. A Succeeded pod occupies no domain. When a pod
// reaches a terminal phase the API server's watch cache sees it stop matching and
// emits a Deleted event — exactly the decrement we want. On batch-heavy clusters
// this removes completed-pod accumulation from both memory and the event stream at
// zero cost to correctness.
//
// Failed pods are NOT excluded, though the standalone design excluded both.
// objectstate's eviction-burst detector reads exactly that phase:
//
//     func podEvicted(p *corev1.Pod) bool {
//         return p.Status.Phase == corev1.PodFailed && p.Status.Reason == "Evicted"
//     }
//                                   — objectstate.go:1220, resolved by spike S9
//
// Excluding Failed server-side would silently delete eviction detection from the
// sentinel: no error, no empty watch, just a detector that never fires again. This
// is the field-selector instance of the same hazard the transform has.
podSelector := fields.OneTermNotEqualSelector(
    "status.phase", string(v1.PodSucceeded),
).String()
```

> **There is no transform on the shared factory today.** `internal/watch/wiring.go:963`
> builds a bare `informers.NewSharedInformerFactory(client, 0)` feeding all eight
> sources and the graph feed, with no `WithTransform` and no tweaked list options.
> Everything above is therefore a *new* process-wide behaviour introduced by leeway,
> not an adjustment to an existing one — which is the strongest argument for the
> preserved-field registry below, and for landing it before the transform rather
> than after.

**The transform is the sharp edge.** The standalone design attached a `TransformFunc`
that stripped container `Env`/`EnvFrom`, `ManagedFields`, and all container
statuses, cutting pods from 15–25 KiB to ~1.5 KiB. Under a shared informer set that
transform mutates objects *every other consumer sees*, and three existing consumers
read fields it would have destroyed:

| Consumer | Reads | Verified at |
|---|---|---|
| `pkg/graph` | `Env[].ValueFrom`, `EnvFrom[].ConfigMapRef/SecretRef` — this is how ConfigMap and Secret edges are built | `derive.go:159–173`, `changes.go:272–286` |
| `pkg/checks/state` | `Env[].ValueFrom` + `Env[].Name` (edge checks); `Env[].Name` alone (Workload Identity) | `edges_checks.go:390–409`, `wi.go:330` |
| `objectstate` | `pod.Status.ContainerStatuses` | `podclearance.go:349` |
| `rollout` | `Status.InitContainerStatuses`, `Status.ContainerStatuses` | `rollout.go:501` |
| `pkg/checks/delta` | `Status.InitContainerStatuses`, `Status.ContainerStatuses` | `pods.go:95–99` |
| `pkg/checks/logs` | `Spec.EphemeralContainers[].Name` | `fetch.go:183` |

`pkg/checks/state` is in this table because it is *not* only read-path code:
`internal/watch/enrich.go:78–80` imports it, and the enricher's `livePod` reads
straight from the shared pod lister (`enrich.go:129–131`). Anything the transform
does reaches the enrichment payload too.

**The last two rows were added when the registry was implemented, and they are
the argument for the registry in miniature.** The same import block that makes
`pkg/checks/state` informer-reachable also pulls in `pkg/checks/delta` and
`pkg/checks/logs`, so all three are reachable and only one was listed. Neither
new row changes the transform — `ContainerStatuses` was already preserved, and
`EphemeralContainers[].Name` survives a strip aimed at the container's other
fields — but both were discovered by enumerating readers a second time, not by
anything failing. A third enumeration would be a third chance to miss one, which
is why the mechanism is now a test rather than a table in a document.

Applying the original `trimPod` would have silently emptied the graph's
config/secret edges and broken two shipped sources — with no error, because a
nil slice is a valid empty slice.

**Spike S9 resolved the narrowing question in our favour: not one of those four
readers touches `Env[].Value`.** Every site reads `Name`, `ValueFrom`, or both —
the *reference*, never the resolved literal. So the transform can be narrowed to
exactly the field that carries secret material:

```go
// The security goal is that secret VALUES never enter process memory or a heap
// dump. That is satisfied by nulling Env[i].Value while preserving Name and
// ValueFrom, which is precisely what pkg/graph needs to build its edges — the
// reference, not the resolved secret. Container statuses stay, because two
// sources read them.
func trimPod(obj any) (any, error) {
    pod, ok := obj.(*v1.Pod)
    if !ok {
        return obj, nil // tombstones and other types pass through
    }
    pod.ManagedFields = nil
    trimContainers(pod.Spec.Containers)
    trimContainers(pod.Spec.InitContainers)

    // Ephemeral containers share EphemeralContainerCommon with a regular
    // container, so `kubectl debug --env` writes literal secret values into
    // the cache by exactly the route this transform exists to close. Name is
    // preserved — pkg/checks/logs enumerates it, and that check is
    // informer-reachable through the enricher.
    for i := range pod.Spec.EphemeralContainers {
        trimContainerCommon(&pod.Spec.EphemeralContainers[i].EphemeralContainerCommon)
    }
    return pod, nil
}
```

**Ephemeral containers were missing from the original sketch, and that is a
security hole rather than an omission.** The stated goal is that secret values
never enter process memory; a `kubectl debug --env` container defeats it while
every strip above still passes. The two container slices the sketch covered are
the common case, not the boundary.

**Consequence: the pod memory model in §6.6 was out by an order of magnitude, and
spike S8 has now remeasured it (2026-09-21).** Retaining `ContainerStatuses` and the
`Env` name/ref structure puts a trimmed pod at **18,630 B of retained heap** on
`std-simian-test` and 10,007 B on a kind node — not the ~1.5 KiB above. The
transform survives the measurement, but not the reason it was sold: it is a *wire*
optimisation far more than a memory one, removing **40.6% of a pod's bytes and only
25.7% of its heap**. §6.6 carries the corrected figures; §13 S8 carries the method
and the two-cluster spread.

The ~1.5 KiB was never reachable, either. It is quoted above as the standalone
design stated it, and the strip it describes is *larger* than the one we ship — yet
`ManagedFields` is already nil in the 18,630 B, so deleting the container statuses
and the whole `Env` structure on top would have to find another 17 KB in a pod that
does not contain it. The figure was an estimate presented as a measurement, which is
the specific failure S8 exists to stop repeating.

Nodes are simpler — nothing in the repo reads `status.images`, and it is the single
largest node-side line item (10–40 KiB/node, ~2 GiB at 50k nodes):

```go
func trimNode(obj any) (any, error) {
    node, ok := obj.(*v1.Node)
    if !ok {
        return obj, nil
    }
    node.ManagedFields = nil
    // retainedNodeAnnotations is load-bearing, not cosmetic: GKE records the
    // provisioned compute-class priority in the `ccc_priority_index` annotation
    // (§7.7.2), so dropping it here would silently disable preference-rank
    // tracking with no error anywhere. Anything §7.7 depends on must be listed.
    node.Annotations = filterKeys(node.Annotations, retainedNodeAnnotations)
    node.Status.Images = nil
    node.Status.VolumesInUse, node.Status.VolumesAttached = nil, nil
    node.Status.NodeInfo = v1.NodeSystemInfo{}
    node.Status.Conditions = retainConditions(node.Status.Conditions, v1.NodeReady)
    return node, nil
}
```

Node heartbeats live in `coordination.k8s.io/v1` `Lease` objects, which the
sentinel does not watch, so `Node` update rate is governed by
`nodeStatusReportFrequency` (5 min default) rather than the 10 s heartbeat —
roughly 170 events/s even at 50k nodes.

> **A transform is shared state.** Any future source that needs a field a transform
> strips will fail silently, because absent and empty are indistinguishable
> downstream. This wants a single documented registry of "fields the shared
> transform preserves and why", and a test that fails when a field is added to the
> strip list without an entry. That is a small piece of work with a large blast
> radius, and it should land with the first transform, not after the second
> incident.
>
> **Done — `internal/watch/transform_registry.go`.** Every field the transform
> touches has an entry justifying it; every field it deliberately preserves names
> the informer-reachable reader that requires it, with the `file:line` of the
> read. The guard is deliberately *not* a declared strip list diffed against the
> registry, because that only moves the drift up one level — edit `trimPod`
> directly and the declaration becomes a lie that still passes. Instead the test
> fills a `Pod` and a `Node` so every field is non-zero, runs the transform, and
> reflectively collects the JSON path of everything that actually changed. That
> set must equal the registry's stripped set **exactly**, so a strip with no entry
> fails and an entry for a strip that no longer happens fails too. Verified by
> mutation in both directions.
>
> **Attached in Phase 2.** `newSharedFactory` in `internal/watch/transform.go` is
> now the single place the shared factory is constructed, with
> `informers.WithTransform(sharedTransform)` on it, and `wiring.go` calls that
> rather than building its own — a test that constructs a matching factory of its
> own would stay green on the day someone drops the option. The dispatch lives in
> one `TransformFunc` because a factory takes one for all its informers: `Pod` and
> `Node` are trimmed, everything else passes through by identity.
>
> This closes the "source on, transform off" half of the default-on decision
> (§15 Q4). Two client-go contract points constrain it: the transform must be
> **idempotent**, because cached objects can be handed back to `Replace()`
> (`delta_fifo.go:501-506`), and it never sees a `DeletedFinalStateUnknown` or a
> `Sync`, both of which `DeltaFIFO` skips because the object has already been
> through it (`delta_fifo.go:507-516`). Both are pinned by tests; the tombstone
> case is tested as an unreachable-by-contract path rather than assumed.
>
> The per-source fallback factories (`pkg/sources/*/…`, built only when
> `WithFactory` was not called) do **not** carry the transform. In the shipped
> binary that path is dead — `wiring.go` injects the shared factory into every
> source — so it is reachable only from tests and from embedding lookout as a
> library. Left as-is deliberately: those callers get untrimmed objects, which is
> a memory cost and not a correctness one, and duplicating the option across eight
> constructors would create eight places for it to drift.

### 6.2 Indexes

| Index | Purpose | Maintained on |
|---|---|---|
| `podUID → Placement` | Apply deletes/moves without the old object | Pod events |
| `nodeName → set(podUID)` | Re-map all pods when a node's topology labels or schedulability change | Pod + Node events |
| `subjectKey → map[TopologyKey]*Distribution` | The counts themselves | Derived |
| `podUID → subjectKey` | Owner resolution result, cached | Pod add, owner events |
| `DomainInventory[TopologyKey][Domain]` | Node count, allocatable, taint sets, label sets | Node events |
| `templateHash → Intent` | Avoid re-deriving intent per pod | Owner events, inventory generation bump |

**Leeway owns all six. Spike S10 settled this, and the reason is not the one
expected.** The open question was whether `nodeName → set(podUID)` and the pod →
zone relation are really *queries against `pkg/graph`* rather than new indexes,
since the graph already holds Pod nodes, Node nodes, `RunsOn` edges and a Zone
layer, interned, under a COW snapshot. Three findings close it:

1. **The graph does not always run.** `internal/watch/wiring.go` builds the graph
   feed inside `if f.stormEnabled()`. Storm correlation is an unrelated feature
   with its own flag, and with `--storm=off` there is no graph at all. A drift
   detector whose counters vanish when an operator turns off correlation is not a
   detector. This alone is decisive.
2. **The graph has no counting API.** Its surface is traversal — `Radius`,
   `OwnerChain`, `CommonAncestors`, `PodsUnder`, `Lookup`, `Out`/`In`. Getting
   per-zone totals means BFS per subject per evaluation, which is the O(pods)
   recomputation §6 exists to avoid.
3. **The swap cadence is wrong for counting**, as suspected — batched at "at most
   every few hundred ms", which is right for evaluation and wrong for a counter
   that must be correct at the instant a delta lands.

The graph stays genuinely useful in two narrower places, and leeway should use it
where present rather than ignore it: `PodsUnder(subjectID)` is a ready-made
*rebuild* path for the §6.5 verifier, and `OwnerChain` is the owner resolution
behind `podUID → subjectKey`. Both are off the hot path, both tolerate the swap
cadence, and both must degrade to leeway's own walk when storm is off.

> **`DomainInventory` landed 2026-09-17** as `Inventory` in
> `pkg/sources/topologydrift`, with `Placement` in `pkg/leeway`. Three things the
> implementation settled that this section left open:
>
> **The relevance comparison is against retained facts, not against objects.**
> `Upsert` extracts the handful of fields the inventory reads and compares those,
> so a node status update whose only difference is a condition heartbeat or a
> kubelet version produces a zero `Change` — no pods re-mapped, no generation
> bump. Comparing objects, or even comparing `oldObj` to `newObj`, would make
> every heartbeat look like an event. Taint comparison ignores `TimeAdded` for
> the same reason: a node controller re-applying an identical taint is not a
> change.
>
> **Eligibility is compared on the full label set, not just the topology
> labels**, which is a deliberate widening of §6.3's five-field list. A
> non-topology label is exactly what a subject's `nodeSelector` matches on, so
> changing one moves the eligible domain set without moving any domain. Node
> label churn is rare and the map compare is over a few dozen entries, so the
> cost of being right here is nil. Topology labels alone still drive
> `DomainsChanged`, which is the narrow, expensive half.
>
> **Zone and region read the deprecated beta labels as a fallback.** A node
> carrying only `failure-domain.beta.kubernetes.io/zone` would otherwise resolve
> to `DomainUnknown`, and because that is a *visible* domain rather than a drop,
> the result would not be a missing node — it would be a confident, wrong
> distribution placing part of the fleet in a nonexistent zone. Three other
> places in the repo already read both spellings.

> **The `templateHash → Intent` row is not what shipped (2026-09-18).** It cannot
> be: S3 established that intent is read from an *admitted pod* and never from a
> controller's `PodTemplateSpec`, so there is no template to hash. Two
> subject-keyed maps took its place, both in `State` because both need exactly the
> eviction rule `State` already runs — a subject is forgotten the moment its last
> pod stops counting:
>
> | Index | Purpose | Maintained on |
> |---|---|---|
> | `subjectKey → representative{uid,name}` | Get from a subject back to a pod to infer from | Pod events |
> | `subjectKey → map[TopologyKey]*Intent` | Last evaluation's resolved intent; what `intent_info` scrapes | Evaluation |
>
> The representative is **a hint, not an index**: one entry per subject, holding a
> name rather than a pod (§6.6.2 budgets this process to absorb a ~66k-pod
> reschedule, and retaining pod objects would put the whole cache in a second
> place), overwritten by every counted pod event so it converges on the most
> recently admitted spec. A reader that misses re-elects by listing the namespace.
> That the fallback is un-indexed is the decision and not an omission: a
> subject-keyed *pod* index has to run owner resolution to place a pod, which
> reads the ReplicaSet cache — so an entry would depend on a different informer's
> state, and an informer re-indexes an object only when *that object* changes.
> With the resync period at zero, an entry computed before the ReplicaSet landed
> is never repaired. A miss is cheap and self-correcting; a wrong index is
> neither. Misses that cluster mean pods are turning over faster than the cache,
> which is the §7.6 domain-outage row — the case where workload findings are
> suppressed anyway.
>
> **Eviction belongs to the caller that knows its decrement is final.** A one-pod
> subject's counts pass through zero on every in-place re-count — the move path in
> `OnPodUpdate` and `remapNode` both take the old placement out before putting the
> new one in — so hanging eviction off "the counts row emptied" clears the
> representative and the resolved intents of a healthy workload whose node was
> merely relabelled.

### 6.3 Delta rules

```go
func (s *State) OnPodAdd(pod *v1.Pod) {
    p := s.resolvePlacement(pod)  // node → domains, state, pinned
    sub := s.resolveSubject(pod)  // owner chain, cached
    s.placements[pod.UID] = p
    s.byNode[p.NodeName].Insert(pod.UID)
    s.apply(sub, p, +1)
    s.enqueue(sub)
}

func (s *State) OnPodUpdate(_, cur *v1.Pod) {
    old, known := s.placements[cur.UID]
    nw := s.resolvePlacement(cur)
    if known && old.Equal(nw) {
        return // no topologically-relevant change; do not enqueue
    }
    sub := s.resolveSubject(cur)
    if known {
        s.apply(sub, old, -1)
    }
    s.placements[cur.UID] = nw
    s.apply(sub, nw, +1)
    s.enqueue(sub)
}

func (s *State) OnPodDelete(obj any) {
    pod, ok := obj.(*v1.Pod)
    if !ok {
        tomb, ok := obj.(cache.DeletedFinalStateUnknown)
        if !ok {
            return
        }
        if pod, ok = tomb.Obj.(*v1.Pod); !ok {
            return
        }
    }
    if p, known := s.placements[pod.UID]; known {
        s.apply(s.resolveSubject(pod), p, -1)
        delete(s.placements, pod.UID)
        s.byNode[p.NodeName].Delete(pod.UID)
    }
}
```

Two decisions worth calling out:

1. **Decrement from the stored `Placement`, never from `oldObj`.** After a resync or
   a missed event, `oldObj` may not reflect what we actually counted. Our own index
   does, by construction. This makes the counters self-consistent even when the
   event stream is not.
2. **Early return when the placement is unchanged.** Most pod updates are status
   churn and topologically inert. Filtering here is what keeps the workqueue quiet
   at scale — and under a shared informer it matters more, not less, because leeway
   sees every event every other source sees.

> **Landed 2026-09-17** as `State` in `pkg/sources/topologydrift`. Two places the
> implementation is stricter than the sketch above, both for the same reason the
> sketch gives for decrementing from the stored `Placement`.
>
> **The subject is cached, and a delete uses the cached one rather than
> re-resolving.** `OnPodDelete` above calls `resolveSubject(pod)`, which asks the
> owner chain a question it may no longer be able to answer: deleting a
> Deployment takes its ReplicaSets and pods with it, and whether the ReplicaSet
> lookup still succeeds depends on which delete arrives first. A miss would
> decrement nothing and leave that subject's counts permanently high. It is the
> stored-placement argument applied to the other half of the key.
>
> **`OnPodAdd` routes through the update path.** An informer add is not a
> guarantee of novelty — after a watch break the relist replays the whole cache
> as adds — so a handler that incremented unconditionally would double every
> count in the cluster on a dropped connection.
>
> Two things the sketch leaves open are settled the strict way. A pod whose owner
> chain does not end in a Deployment, StatefulSet, DaemonSet or Job is not
> counted at all: leeway scores a distribution against an *intent*, and a pod
> nobody declared has none. And `Succeeded` **and `Failed`** pods are both
> uncounted, which is narrower than §6.1's field selector — the selector keeps
> Failed pods in the shared cache because objectstate's eviction-burst detector
> reads exactly that phase, whereas leeway is asking where a workload's replicas
> are, and a failed pod is not a replica. Counting them would make a zone that
> has just evicted five hundred pods look like the most populated in the cluster.

**Node events** are lower-volume but higher-fanout, and get the same relevance
filter — we compare only topology labels, taints, `unschedulable`, `Ready` and
allocatable against the inventory entry:

- *Topology label change* → re-map every pod in `byNode[node]`, enqueue affected
  subjects.
- *Ready / schedulable / taint change* → mutates `DomainInventory`, which changes
  the **eligible domain set** for potentially every subject. Bump
  `inventoryGeneration` and mark the intent cache stale rather than enqueuing
  20,000 subjects. Subjects re-evaluate lazily on their next event, and a
  rate-limited sweep (default: full sweep over 5 minutes) guarantees eventual
  re-evaluation. This bounds the cost of a zone-wide node event.
- *Node add/delete* → inventory update as above, plus `byNode` maintenance.

> One correction from the implementation: **a node *add* must re-map too.** A new
> node is not an empty one as far as `byNode` is concerned — pod events routinely
> arrive before their node's, and a node deleted and re-created keeps its name —
> so skipping the re-map on add strands those pods in `DomainUnknown` until they
> next change, which on a stable workload is never. A node *delete* re-maps its
> pods to `DomainUnknown` rather than dropping them: their own deletions are
> coming but are not here yet, and in the meantime they genuinely exist and are
> genuinely nowhere, which is a true statement about a cluster that just lost a
> node. The re-map reads no pod objects at all — the stored placement holds
> everything that is not changing and the inventory supplies the new tuple, so
> the re-map cannot disagree with what was counted, because it is derived from
> it.

### 6.4 Coalescing

The workqueue is keyed by subject, so a rollout churning 200 pods for one
Deployment collapses into a few evaluations. `AddAfter(key, coalesceWindow)`
(default 2 s) turns a burst into one evaluation at the end; during an active
rollout the window widens to `rolloutCoalesceWindow` (default 15 s).

> **Landed 2026-09-17** as `coalescer` in `pkg/sources/topologydrift`. Three
> corrections to the sketch above, all forced by building it.
>
> **It is not client-go's workqueue.** `AddAfter` on the delaying queue keeps
> the *earliest* `readyAt` when a waiting key is re-added, so it can shorten a
> pending delay but never widen one — and widening is the entire behaviour this
> section asks for. The queue here is ~150 lines with the same dedup-by-key
> property and the widening it actually needs.
>
> **Nothing has to tell it a rollout is happening.** The sketch implies an
> external signal for "during an active rollout". There is none to read:
> re-enqueue *is* the signal, because a Deployment replacing 200 pods produces
> 200 enqueues. The first enqueue of a burst waits `coalesceWindow`; any
> subsequent one while the subject is still pending widens to
> `rolloutCoalesceWindow`.
>
> **Widening needs a cap, and the cap is not a detail.** A subject whose pods
> never settle — a crash-restart loop, a Job queue with constant turnover —
> would have its evaluation pushed out forever, and that is precisely the
> subject most worth evaluating. `maxCoalesceDelay` (default 60 s), measured
> from the first enqueue of the burst, bounds it.
>
> Ordering is a lazily-updated heap rather than a scan of the pending set:
> after a relist, pending is *every* subject, and an O(pending) scan per
> evaluation turns 20k subjects into 400M map iterations.

### 6.5 Self-verification

Incremental counters are the kind of code that is correct in tests and subtly wrong
in production 60 days in. Treat that as a certainty, not a risk:

```go
// Every verifyInterval, rebuild counters for 1/verifyShards of subjects directly
// from the informer cache and compare.
func (v *Verifier) verifyShard(shard int) {
    for _, sub := range v.state.SubjectsInShard(shard) {
        want := v.rebuildFromCache(sub)
        got := v.state.Snapshot(sub)
        if diff := want.Diff(got); !diff.Empty() {
            metricCounterMismatch.Add(1, attribute.String("subject_kind", sub.Kind.String()))
            klog.ErrorS(nil, "counter drift detected; repairing", "subject", sub, "diff", diff)
            v.state.Replace(sub, want)
        }
    }
}
```

`lookout_leeway_counter_mismatch_total` is an SLI for the tool itself and should be
alerted on at any non-zero rate.

> **Implemented 2026-09-17** in `pkg/sources/topologydrift/verify.go`, with four
> departures from the sketch above. Each was forced by writing it.
>
> 1. **The rebuild is per shard, not per subject.** `rebuildFromCache(sub)` reads
>    like a helper and is a full pod-cache walk; calling it once per subject makes
>    a pass O(pods × subjects-in-shard) — at the §6.6 baseline row, 150,000 pods
>    times 1,600 subjects. `State.RebuildShard` walks the cache **once** and
>    groups by subject, discarding pods outside the shard. Sharding was never
>    about making the walk cheaper; it bounds the *repair*, so that a systematic
>    bug found at 20,000 subjects rebuilds a twelfth of them per tick instead of
>    stalling the evaluation queue behind all of them at once.
> 2. **The compared set is the union of both sides**, not `SubjectsInShard` alone.
>    A subject the incremental path still holds and the pod cache no longer
>    supports is the leak this component exists to catch, and it appears in the
>    rebuild only as an absence — so it has to be looked for from the other
>    direction.
> 3. **`Replace(sub, want)` became `Repair(sub, pods)`.** Overwriting the counts
>    alone passes the very next verification and re-corrupts on the first real
>    event: `placements` is what a delta decrements *from*, so a pod left at its
>    stale placement subtracts from a domain it is no longer counted in. The
>    repair re-seats `placements`, `subjects` and `byNode` from the rebuilt pods.
> 4. **The rebuild takes the subject from the cache** (`State.subjectOf`) rather
>    than re-resolving the owner chain. This looks like it weakens the check, and
>    the alternative is worse: a pod whose ReplicaSet has left the informer cache
>    is *supposed* to keep counting under its first-assigned subject (§6.3), so a
>    fresh resolution would report correct behaviour as drift — on a metric whose
>    entire value is that any non-zero rate means a bug. What the pass verifies is
>    the arithmetic, which is where drift actually comes from.
>
> The log line also reports per-domain `counted→rebuilt` rather than per-axis
> totals. The commonest drift is a pod that moved and was never re-counted, and
> its totals match on both sides.

### 6.6 Scale envelope

Node count is close to free here. Nodes contribute a trimmed cache entry, a
`byNode` bucket and an inventory update; they do not multiply anything. **Pods**
drive memory, **event rate** drives CPU. A 50,000-node cluster running 200,000 pods
is cheaper for us than a 2,000-node cluster running 400,000 pods, which is the
opposite of how cluster scale is usually quoted.

| Tier | Nodes | Pods | Subjects | leeway indexes | Marginal heap |
|---|---|---|---|---|---|
| Typical | 1,500 | 15,000 | 2,000 | 5 MiB | ~10 MiB |
| Baseline | 5,000 | 150,000 | 20,000 | 45 MiB | ~70 MiB |
| Large | 15,000 | 500,000 | 60,000 | 150 MiB | ~230 MiB |
| Very large | 50,000 | 1,000,000 | 120,000 | 300 MiB | ~450 MiB |

These are leeway's **marginal** cost. Object caches are the sentinel's and are
already paid for — which is the entire quantitative case for §2.1. The marginal
columns stand; what spike **S8** corrected is the number they are marginal *to*.

**Measured per-object cost (S8, 2026-09-21).** Arrows are before → after the §6.1
transform. Retained heap is measured by holding 5,000 deep copies and reading
`HeapAlloc`, not inferred from the wire size — `internal/watch/objectsize_test.go`
is the measurement and re-runs against any cluster you point it at.

| | `std-simian-test` (GKE 1.36) | kind |
|---|---|---|
| Pod, wire p50 | 18,164 → **10,788 B** | 7,854 → **4,284 B** |
| Pod, retained heap | 25,068 → **18,630 B** | 13,393 → **10,007 B** |
| Node, wire p50 | 25,163 → **3,381 B** | 6,888 → **1,823 B** |
| Node, retained heap | 17,399 → **5,308 B** | 6,325 → **3,026 B** |

Three things in there outlive the numbers:

1. **Heap is not wire.** A trimmed GKE pod is 10,788 B on the wire and 18,630 B in
   the cache — 1.7×. Every memory estimate in this document before S8 was derived
   from serialised size, and that is where the order of magnitude went.
2. **The pod transform is a ~25% heap win in both environments** (25.7% and 25.3%)
   against a 40–46% wire win. Holding across two very unlike clusters is what makes
   the *ratio* a mechanical one in the §13 S6 sense, quotable anywhere. The absolute
   sizes are not: they are a population parameter, and they vary 1.9×.
3. **The node transform is the large one** — 69.5% of heap on GKE, because
   `status.images` is 25 entries there and nothing in the repo reads it.

Extrapolating the shared object cache — lookout's number, not leeway's — from the
GKE column, which is the conservative end:

| Tier | Pods | Nodes | Shared object cache |
|---|---|---|---|
| Typical | 15,000 | 1,500 | ~275 MiB |
| Baseline | 150,000 | 5,000 | ~2.6 GiB |
| Large | 500,000 | 15,000 | ~8.8 GiB |
| Very large | 1,000,000 | 50,000 | ~17.6 GiB |

This is **extrapolation from measured per-object cost**, reported as such exactly as
§12.1 requires of the 1M tier, and it counts only cached objects — no index
overhead, no delta queue, nothing leeway adds.

> **It also says the shipped default is too small, and that is the most actionable
> thing S8 found.** `deploy/51-deployment-watcher.yaml` sets `memory: 256Mi`, which
> the pod cache alone exhausts at **~14,000 GKE-sized pods** (~27,000 kind-sized
> ones) — inside the 1–15k range lookout DESIGN §6.2 calls typical, and reached
> before leeway allocates anything. This predates leeway and is not leeway's to fix,
> but it is now measured rather than suspected, so it is filed against Phase 8 as
> [#480](https://github.com/go-steer/k8s-lookout/issues/480) rather than left in a
> design document.

The "Typical" row is added deliberately: lookout DESIGN §6.2 puts real clusters at
1–15k pods, and at that size leeway costs single-digit MiB. The larger rows exist
because the design has no cliff in it, not because we expect them.

Set `GOMEMLIMIT` to ~80% of the container limit so the GC becomes aggressive under
pressure instead of the kernel OOM-killing us.

#### 6.6.1 Event-rate cost model

This, not memory, is what the design target stresses. At 500 pods/sec sustained
scheduling, steady state implies ~500 pods/sec terminating too, and each pod
lifecycle produces roughly eight watch events — a **modelled** figure, not a measured
one, since S7 was deferred (§13). Everything below scales linearly off it, so read the
totals as an estimate and re-derive them before Phase 8's scale gate is judged:

| | |
|---|---|
| Pod lifecycle rate | 500/s created, ~500/s removed |
| Watch events | ~4,000/s pods + ~200/s nodes and owners ≈ **5,000/s** |
| Topologically relevant | ~1,000/s (bind, move, terminate) — the other ~80% are inert status churn |

Per-event cost:

| Stage | Cost | Paid by |
|---|---|---|
| Protobuf decode + allocate | ~40 µs | **The sentinel, already** — shared, not leeway's marginal cost. ~200 µs if left on JSON |
| Transform | ~5 µs | Shared |
| Relevance check | ~2 µs | leeway. ~80% of events return here |
| Delta apply | ~2 µs | leeway; O(topology keys); only the relevant ~20% |
| Enqueue | ~0.5 µs | leeway; deduped by subject, amortises to near zero |
| **leeway marginal** | **~2.5 µs** | → 5,000/s × 2.5 µs ≈ **0.013 core** |
| **Whole watch path** | **~48 µs** | → ~0.24 core, whoever pays it |

This is the number that makes the fold-in obviously correct: **running leeway inside
the sentinel costs ~0.013 core; running it as a second process costs ~0.24 core**,
almost all of it re-decoding bytes the sentinel already decoded. NFR-4's 0.15-core
budget is mostly evaluation and scoring, not ingest.

The relevance early-return (§6.3) is what keeps it at 2.5 µs. Note what it does
*not* buy: the decode has already happened by the time we can tell an event is
inert. Decode cost is a property of the raw stream and is reducible only
server-side.

#### 6.6.2 Burst behaviour

The design load is not steady state, it is a domain failure: a zone outage
reschedules ~1/3 of 200k pods — ~66k pod moves arriving as fast as the scheduler
will bind them, a ~130 s burst at 500/s.

What saves us is that the workqueue is **keyed by subject**, so 66k pod events
collapse into at most ~20k evaluations, and coalescing collapses them further. The
queue is bounded by subject count, not event count, however violent the burst.
Expected behaviour: evaluation lag rises from ~2 s to ~60–90 s and recovers within
minutes of the burst ending. That is the correct degradation — the tool stays up
and merely reports late, during precisely the incident it exists to describe.

The hazard is upstream of the queue. `sharedIndexInformer` gives each listener an
**unbounded** pending-notification ring buffer, so a slow handler converts a burst
directly into heap growth and then an OOM. Under a *shared* informer this is worse
than in the standalone design, because a slow leeway handler now degrades every
other source on the same informer:

> **Invariant.** The informer event handler does index maintenance and an enqueue,
> nothing else. It never blocks, never does I/O, never takes a contended lock, never
> calls the evaluator. All real work happens in workqueue workers, which apply
> backpressure by falling behind.

Workqueue depth and `lookout_leeway_last_event_timestamp_seconds` detect a
violation in production.

**What binds, in the order the limits are reached:**

1. **Watch decode throughput** — first to bind, ~0.24 core at 500 pods/sec,
   saturating a core near 2,000 pods/sec. Shared with the sentinel; reducible only
   server-side.
2. **Metric cardinality** — comfortable at ~20k subjects; the wall above ~50k,
   where §8.4 per-domain gating stops being an optimisation and becomes mandatory.
3. **Initial sync peak RSS** — peak memory is at startup, not steady state.
4. **API server watch capacity** — shared with every controller in the cluster.
5. **Steady-state memory** — last, and at 200k pods never reached.

### 6.7 Sharding and watch scope

The standalone design carried a full horizontal-sharding section. Folded in, it is
**out of scope**: sharding is a property of the sentinel, not of leeway, and
lookout DESIGN §11 already answers the fleet question by putting fan-in in the
fleet layer.

Two things carry over as cheap seams:

- **Store keys are subject-keyed, not shard-keyed**, so baselines survive any future
  resharding of the sentinel.
- **Subjects never span namespaces**, so leeway's state partitions cleanly on
  namespace if the sentinel is ever scoped or sharded that way. A namespace-scoped
  sentinel gets *complete* distributions for the subjects it can see — fewer
  subjects, never partial ones — which is why scoping degrades leeway gracefully
  and losing nodes does not (see below).

#### The selector grammar decides this, not the namespace count

Spike S6 was going to measure namespace counts to find out whether server-side
scoping was viable. It was the wrong instrument, and §13 records why it was closed
without being run. The answer is a property of the field-selector grammar, and it
is the same on a cluster with 12 namespaces and one with 1,200:

- **Exclusion is one stream at any length.** Field-selector requirements are ANDed
  and `!=` is supported, so `metadata.namespace!=a,metadata.namespace!=b,…` is a
  single selector on a single informer. This shipped:
  `--exclude-namespace` scopes the watch rather than filtering the output
  ([#431](https://github.com/go-steer/k8s-lookout/pull/431)), and it costs leeway
  nothing — the excluded namespaces simply contribute no subjects.
- **Inclusion is M factories.** There is no `OR`, so `metadata.namespace=a` names
  exactly one namespace. Watching *M* of them means *M* informer factories, and the
  shared factory carries ten namespaced informers, so the stream count is
  `10M + 1` — 51 at M=5, 201 at M=20 — against a flat 11 for cluster-wide. The
  memory win exists only when the *M* namespaces are a real subset of the cluster;
  above roughly a tenth of it, scoping costs more than it saves. Tracked as
  [#407](https://github.com/go-steer/k8s-lookout/issues/407), deliberately unbuilt.

Neither of those crosses over at a namespace count, which is what makes the
measurement uninformative: no value S6 could have returned would have changed the
design. What *does* vary per deployment is how much of the cluster the operator
wants to watch, and that is an operator's decision to state, not ours to infer.

#### Nodes are the carve-out

`metadata.namespace` is not a selectable field on a cluster-scoped resource, and the
API server refuses the LIST rather than ignoring the term. A factory-wide selector
would therefore not filter the node watch, it would break it — as a reflector that
never syncs, which is worse than an error. Nodes run on a second, unfiltered factory.

That matters more for leeway than for anything else that reads nodes. §7.1 derives
*eligible* domains from the node inventory, so without a cluster-wide node read
`topology-drift` cannot tell "the workload avoided this zone" from "no node exists
there" — it does not degrade, it becomes wrong. The documented shape for a scoped
deployment is therefore a **hybrid grant**: namespaced `Role`s for the workload
objects plus a minimal node-only `ClusterRole`. Nodes carry no tenant data and §6.6
prices them as nearly free, so this is cheap to grant. A strictly namespaced
deployment stays supported and needs no new code — the §11 probe denies the node
requirement and disables the source loudly — but it is a deployment without leeway,
and should be chosen knowing that.

For the record, on client-side sharding, in case the question returns: it divides
memory by N but *multiplies* aggregate decode cost and watch fanout by N, because
every shard still receives every event. Only server-side scoping reduces decode.

---

## 7. Scoring: separating drift from normal variance

This is the part of the design that is genuinely new work, and it lives in
`pkg/leeway` with no client-go dependency.

### 7.1 Eligible domains

For subject `S` on topology key `K`:

```
Eligible(S, K) = { d ∈ Domains(K) : ∃ node n in d such that
                     n matches S.nodeSelector ∧ S.nodeAffinity.required
                   ∧ S tolerates n.taints          (if nodeTaintsPolicy=Honor)
                   ∧ n is Ready ∧ schedulable      (if nodeAffinityPolicy=Honor) }
```

If `Intent.MinDomains` is set and `|Eligible| < minDomains`, the missing domains are
treated as present-with-zero, matching kube-scheduler semantics.

Everything downstream is computed over `Eligible` only. **This single rule removes
the largest class of false positives**, and §7.7.6 shows a real cluster where
skipping it would have produced a guaranteed false alarm.

### 7.2 Expected distribution

Given `n` replicas, eligible domains `d₁..d_m` with weights `w_i` and optional caps
`c_i`:

1. Normalise weights: `p_i = w_i / Σw`.
2. Apportion `n` by **largest remainder (Hamilton)**: `e_i = floor(n·p_i)`, then
   distribute the `n − Σe_i` remaining units to the largest fractional remainders.
   Integer apportionment matters — `n·p_i` is not achievable, and comparing against
   it manufactures fractional drift out of nothing.
3. Apply caps by water-filling: clamp any `e_i > c_i` to `c_i`, remove those
   domains, re-apportion the overflow, iterate to a fixed point.

`Weighting: Equal` is the default for workloads. `AllocatableCPU` is the default for
workloads whose eligible domains have materially unequal capacity (triggered when
max/min domain capacity ratio > 1.25, configurable with `--topology-capacity-ratio`).

The trigger is a **default, not a rule**, and four carve-outs make that precise:

- **Node-group subjects are pinned to `Equal`** and take neither the trigger nor a
  declaration. *(Amended in phase 7. This section previously made `AllocatableCPU`
  their default, on the reasoning that a pool's zones are unequal by design often
  enough that an even expectation would be the surprising one. Working §14's phase 7
  exit criterion through showed that backwards.)* Capacity weighting apportions a
  subject's objects over the cluster's allocatable CPU per domain — it answers *where
  can this go*, which is a question about nodes. A node group's objects **are** that
  capacity, so the expectation contains the thing it is meant to predict, and the
  contamination runs both ways. Three zones of two nodes each, plus a six-node pool
  wholly in zone-a, weights `[32 8 8]`: the lopsided pool is apportioned `[4 1 1]`,
  partly excusing itself with the concentration being measured, and the pool that is
  *evenly spread* is apportioned `[4 1 1]` too and reported as drifting because a
  different pool is lopsided. Equal is the only non-circular expectation for a node
  group. A group with genuinely unequal machine sizes per zone is the cost, and it is
  the smaller error. Implemented as `EligibilityOptions.UndeclaredWeighting`, not as a
  synthetic intent — see FR-3 for why these subjects must stay intent-free.

- **A declared weighting wins**, including a declared `Equal`. This is why
  `Weighting` has an `Auto` zero value: "nobody said" and "somebody said Equal" have
  to be different values, or an operator who declared `Equal` on a heterogeneous
  cluster would be silently overruled by the trigger.
- **An intent that bounds object counts keeps the even expectation.** A `maxSkew` or
  a per-domain ceiling is a contract over *pods*, and §7.3 measures `E` from
  `max(S*, maxSkew)` where `S*` is read off the expectation — so a capacity-weighted
  expectation of `[8 1 1]` raises the floor to 7 and a declared `maxSkew: 1` stops
  being violable. A workload whose zones are unequal is exactly the one whose
  `DoNotSchedule` constraint the scheduler is about to violate; re-deriving the
  operator's contract from the shape of their cluster would delete the finding.
- **Synthetic `minDomains` padding is excluded from the ratio.** The padding carries
  zero capacity by construction, a zero makes the ratio infinite, and capacity
  weighting would then apportion zero objects to the padding — so the missing domain
  the padding exists to expose would score as perfectly balanced.

Where the trigger does fire, the resolved weighting is written back onto the intent
during §5.1 resolution, with an evidence line quoting the ratio and the trigger it
crossed. One fact, one value: `intent_info`'s `weighting` label and §8.5's payload
both report what was actually apportioned.

### 7.3 Metrics

Let `a_i` be actual count in domain `i`, `e_i` expected.

| Metric | Definition | Use |
|---|---|---|
| **Observed skew** | `S = max(a) − min(a)` over eligible domains | Direct comparison to TSC `maxSkew` |
| **Min achievable skew** | `S* = max(e) − min(e)`, read off the expectation | The floor imposed by arithmetic |
| **Excess skew** | `E = max(0, S − max(S*, maxSkew))` | Tier A/B primary signal |
| **Relocation distance** | `R = Σ_i max(0, a_i − e_i)` | "How many pods must move" — the number humans act on |
| **Normalised drift** | `ρ = R / n ∈ [0,1]` | Scale-free threshold; equals total variation distance |
| **Concentration** | `H = Σ (a_i/n)²` (Herfindahl) | Blast-radius framing |
| **Max domain share** | `max(a_i)/n` | The number that matters for zone-failure risk |
| **Goodness of fit** | `χ² = Σ (a_i − e_i)²/e_i`, df = m−1 | Significance gate for weighted expectations, only where all `e_i ≥ 5` |

**Primary signal:** `ρ` for threshold evaluation, `R` for the human-readable body,
`max domain share` for severity escalation. `E` supersedes both where a hard
contract (`maxSkew`) exists.

> **Amended in Phase 4 — every metric above assumes the subject wanted to be
> spread.** FR-6 infers `Colocate` intent from `podAffinity`, and under this table
> a workload doing exactly what it asked for scores the worst ρ available
> (`1 − 1/m`), permanently. Colocation is therefore judged on two counterparts,
> against the same thresholds and the same §7.4 small-n floor:
>
> | Metric | Definition | Spread counterpart |
> |---|---|---|
> | **Scattered** | `Sc = n − max(a)` | `R` |
> | **Dispersion** | `Sc / n = 1 − max domain share` | `ρ` |
>
> Max domain share does not escalate a colocation finding: concentration is the
> goal, so escalating on it would raise severity on the subjects behaving best.
> `deliberate colocation: a podAffinity workload in one zone` is the §12 fixture,
> and it carries the counterfactual — the same placement under the spread rule,
> asserted to breach — so the inversion cannot quietly stop being what saves it.

`S*` is read off the expectation rather than computed as `n mod m`. The two agree
in the equal-weight uncapped case, but only the former stays correct when weights
are unequal or a cap has saturated — a domain clamped to 1 of an expected 4 makes a
wide spread the *best available* placement, and charging the difference as excess
skew would report drift for a cluster doing the only thing it can.

Rationale for `R`/`ρ` over raw skew: skew is a max-min statistic and therefore blind
to the shape of the distribution. `[10,0,0,0]` and `[10,0,5,5]` both have skew 10
across four domains but represent very different risks — the first has everything
in one zone, the second has a zone-failure blast radius of half. `ρ` is 0.70 vs.
0.25, which matches intuition.

Note that both `ρ` figures are computed against the *integer* expectation
(`[3,3,2,2]` and `[5,5,5,5]` respectively), not against `n/m`. Using the
fractional expectation here would give 0.75 for the first case, and that
0.05 discrepancy is the §7.2 error in miniature: it charges the workload for a
remainder no placement could have avoided.

### 7.4 Small-n handling

Below `minReplicasForScoring` (default 3), skew is meaningless — record distribution
and `max domain share`, do not evaluate drift. Between 3 and `smallNThreshold`
(default 10), require `ρ` over threshold **and** `R ≥ 2`, so a single misplaced pod
out of 4 never pages anyone.

### 7.5 Behavioural baselines (Tier C)

For subjects with no declared or inferrable intent, "normal" is empirical. Per
(subject, topology key, domain) maintain an EWMA of domain share plus an EWMA of
absolute deviation as a robust dispersion estimate:

```go
type Baseline struct {
    Share     float64 // EWMA of a_i/n
    Deviation float64 // EWMA of |s_i − Share|
    Samples   uint64
    UpdatedAt time.Time
    Frozen    bool // true while a finding is firing for this subject
}

func (b *Baseline) Observe(share float64, halfLife, dt time.Duration) {
    if b.Frozen {
        return
    }
    alpha := 1 - math.Exp2(-float64(dt)/float64(halfLife))
    dev := math.Abs(share - b.Share)
    b.Share += alpha * (share - b.Share)
    b.Deviation += alpha * (dev - b.Deviation)
    b.Samples++
}
```

Drift is flagged when `|s_i − Share| > k · max(Deviation, floorDeviation)` for a
sustained dwell, with `k` default 4 and `floorDeviation` default 0.05.

Three details that matter more than the formula:

- **Freeze the baseline while a finding is firing.** Otherwise the drift is absorbed
  into "normal" over a few hours and the finding self-resolves while the problem
  persists. This is the classic failure mode of learned baselines; it must be
  designed out, not patched later.
- **Require maturity.** No Tier C findings until `Samples ≥ minSamples` (default
  200) *and* baseline age ≥ `minBaselineAge` (default 6 h).
- **Invalidate on structural change.** If the eligible domain set changes, reset the
  baseline — the old shares describe a different cluster.

Default half-life 12 h, configurable per policy.

> **The estimator shipped 2026-09-20** as `pkg/leeway.BaselineSet`, pure and
> clock-injected. Six things about it are not in the sketch above, and five of
> them are the difference between a learned baseline that works and the usual
> one that does not.
>
> **`Samples`, `UpdatedAt` and `Frozen` live on the set, not on each domain.**
> Every domain of one (subject, topology key) is observed in the same pass from
> the same distribution, so per-domain copies would cost m× the memory to hold
> m copies of one number — and would admit states where two domains of one
> subject disagree about how much history they have, which nothing can produce
> and everything downstream would have to handle.
>
> **dt comes from the clock, not from a fixed sample interval.** A missed tick,
> a slow pass or a restart then weights its sample by the time it actually
> represents, so a subject whose pods churn does not learn faster than a quiet
> one that is equally wrong. Twelve samples an hour and one sample an hour
> converge to the same estimate.
>
> **Freezing stops the clock, not just the arithmetic.** A frozen set still
> advances `UpdatedAt`. If it did not, a subject frozen for three hours would
> thaw with dt = 3 h — α ≈ 0.16 at a 12 h half-life — and absorb a sixth of the
> drift the freeze existed to keep out in a single sample, compounding from
> there. The freeze also covers §7.6 suppression, not only a firing finding, on
> the same reasoning: a baseline that learns through a zone outage decides the
> outage is normal and then decides the recovery is drift.
>
> **The first sample seeds rather than converges**, and **the deviation is
> corrected for its warm-up.** Share can be seeded; deviation cannot, because
> one sample has nothing to deviate from, so it starts at zero and climbs. With
> the stated defaults a baseline matures at 6 h having accumulated only ~29% of
> a 12 h half-life's weight, so the raw EWMAD understates real dispersion by
> about 3.5× and every band built from it is 3.5× too tight — which fires on
> exactly the workloads whose placement legitimately moves, the population Tier
> C exists to avoid paging about. `DevWeight` accumulates `α(1−w)` and
> `DeviationOf` divides by it; this is the standard bias correction and removes
> the artefact exactly. §12's corpus has the case both ways.
>
> **A mature baseline is rendered as an `Intent`, not as a parallel comparison
> path.** `BaselineSet.Intent` returns `Source: SourceLearnedBaseline`,
> `ExplicitShares` = the learned shares and `Bands` = k·max(dev, floor), so a
> learned expectation apportions, scores, exports and routes through exactly
> the code a declared TopologySpreadConstraint does — which is what §5.1 means
> by normalising every source into one shape. What makes it Tier C is the
> source field, and what makes it judged against a band rather than ρ is the
> presence of `Bands`, which no declared source sets. An *immature* baseline
> returns nil, which is the pre-Phase-5 behaviour: scored against an even
> apportionment. Tier C without a baseline is not unmonitored, it is measured
> against the only expectation available.
>
> **Why the band and not ρ against the learned shares.** They answer different
> questions. ρ asks how much of the subject is in the wrong place, against a
> threshold loose enough for the whole estate; the band asks whether the
> subject is doing something *it* does not normally do. For a workload that has
> always sat 80/10/10, a shift to 50/40/10 is ρ = 0.3 — over the threshold, but
> only just — while for one at 60/20/20 the same magnitude of change would not
> be. Learning a dispersion and then judging with a global constant would throw
> away the estimate.

> **The source wiring shipped 2026-09-20**, learning on by default. The
> estimators live in a `baselineLog` keyed by (subject, topology key) — the
> same grain as an episode and as a persisted row — fed on its own ticker
> (default 60 s) rather than from the evaluation queue, and flushed on another
> (§9.2's 30 s). Four decisions in it are worth keeping.
>
> **The sampler is a ticker, not a hook on the queue.** dt from the clock only
> buys an estimate independent of event volume if the *sampling* is too, and a
> queue-driven sampler would feed a churning subject twelve times an hour and a
> quiet one twice a day. It also makes the cost of learning a property of the
> estate's size rather than of how much is going wrong in it.
>
> **The freeze decision is made in the source, because it is two facts from two
> different places.** §7.6 suppression is already on the evaluation, carried
> from scoring time; the alert phase belongs to the machine, and
> `AlertPhase.Firing()` deliberately covers `Resolving` as well as `Firing` — a
> subject whose episode is still clearing has not been shown to be back to
> normal, and learning through the tail of an episode is learning from the
> drift. Freezing and thawing both mark the set dirty, because `Frozen` does
> not persist but `UpdatedAt` does: writing on the edge is what keeps a long
> freeze from reading as downtime to §9.3 step 7 after a restart.
>
> **Sampling collects first and applies second.** `State.EachEvaluation` holds
> the index lock for the whole walk, and both things a sample needs next — the
> episode's phase and the estimator — live behind other locks. The same rule
> `judgements()` already follows: two locks never held together cannot be taken
> in two orders.
>
> **The reap is a sweep, and it waits out `ReconcileGrace`.** A subject-axis
> with no evaluation is one nothing is scoring, whether the workload went away
> or the last node carrying its topology label did — the index already knows,
> so there is no need for a hook on every removal path. But at startup nothing
> has been scored yet, and a reap on the first tick would delete every row
> §9.3 step 1 had just read; the grace the alert machine already keeps for its
> own reconcile covers this one too.

### 7.6 Transient-state suppression

Skew during these states is expected, and is suppressed or evaluated against a
relaxed multiplier (`transientThresholdMultiplier`, default 2.5):

| State | Detection |
|---|---|
| Rollout in progress | `observedGeneration < generation`, or >1 ReplicaSet with replicas > 0, or `updatedReplicas < replicas`. The `rollout` source already tracks this — consume it rather than recomputing |
| Recent scale event | `spec.replicas` changed within `scaleSettleWindow` (default 5 min) |
| Node drain | Node became unschedulable within `drainSettleWindow` (default 10 min) |
| Cluster warmup | Sentinel not yet armed (§9.3) |
| Domain outage | Ready node count in a domain dropped >50% within 15 min — suppresses *workload* drift findings and raises one *domain* finding instead |

The last row is the difference between a useful tool and a pager storm. It also
overlaps lookout's storm correlation (DESIGN §7.5), which solves the same
one-cause-many-symptoms problem generically; the right split is for leeway to emit
the domain-level finding and let storm correlation handle cross-source fan-in
(spike S10).

> **Shipped 2026-09-19 as `leeway.Transients.Classify` / `JudgeTransient`.** Four
> things the table above leaves open, decided:
>
> **Which rows suppress and which relax is not arbitrary.** A state suppresses
> when it makes the counts *unreliable* — during warmup they are a partial view
> of the cluster, and during a domain outage they describe a cluster that is not
> the one the expectation was apportioned over. A state relaxes when the counts
> are correct and merely unflattering: a rollout, a drain and a scale event all
> produce an accurately measured distribution that has not finished moving yet.
>
> **The relaxation reaches the drift path and nothing else.** The contract rules
> (§8.1 Tier A) compare against numbers the *user* declared — a `DoNotSchedule`
> `maxSkew`, a required anti-affinity's per-domain ceiling — and multiplying
> somebody else's stated bound by 2.5 because a rollout is running substitutes
> our judgement for theirs. Suppression is the opposite and covers Tier A too,
> which is the whole point of the outage row: a zone dies, every `DoNotSchedule`
> constraint in the cluster is violated in the same second, and §14 asks for one
> finding rather than four hundred.
>
> **The multiplier scales the tolerances, not the eligibility gate.**
> `minReplicasForScoring` answers "does this subject have enough objects for a
> distribution to mean anything", which is a property of the subject and not of
> what the cluster happens to be doing to it. Scaling it would let a transient
> silently change which subjects are *measured*, and §7.4 records the metrics
> even for subjects it declines to judge. `maxDomainShare` is capped at 1 — 1.25
> and 1.0 are equally unreachable, but only one prints in a finding without
> looking like a bug.
>
> **The outage row compares against the peak in the window, not the oldest
> sample in it.** A zone that goes 9 → 4 → 4 over ten minutes is out; under an
> oldest-sample rule it stops being out the moment the 9 ages out of a sliding
> window, un-detecting an outage that is still in progress. The >50% test is
> strict, so a clean 4 → 2 is a rolling node-pool upgrade rather than a zone.
>
> Detection itself stays in the source, which is the only layer that can see a
> cluster (NFR-10); `Transients` is five answered questions. A timestamp a few
> seconds in the future counts as recent — the kubelet, the API server and this
> process do not share a clock, and that skew lands exactly when suppression
> matters most — but one more than a whole window ahead is bad data and is
> ignored rather than suppressing the subject forever.

> **Three of the five rows went live in the source 2026-09-19** — cluster
> warmup, node drain and domain outage, the ones leeway can answer by itself.
> `ScoreAxis` now judges through `JudgeTransient`, and the suppression is kept
> on the `Evaluation` beside the scores. **The remaining two followed the same
> day; see the second note below.**
>
> **The outage row needs a sampled series, not an event log.** `DomainOutage`
> compares against the peak *inside* the window, so something has to have
> recorded the peak while the domain was still healthy. Appending a
> `ReadyCount` on every change cannot: the log holds the value in force *after*
> each change, and a zone that sat at thirty ready nodes for a week and then
> lost all of them changes exactly once. The peak would read zero and the
> outage would be invisible precisely because it was total. The inventory
> therefore samples every domain's ready count on a timer
> (`DefaultReadySampleInterval`, 30 s) and retains twice the outage window.
>
> **Ready is a third count, not a reuse of `Usable`.** §6.2 already tracks
> `Nodes` and `Usable` (ready *and* schedulable), and a domain that shrinks
> looks identical through `Usable` whether its nodes died or were cordoned —
> which are the two rows of the table above with *opposite* answers. The outage
> test reads readiness alone; the drain test reads cordon times alone.
>
> **A cordon is dated from the unschedulable taint where there is one.**
> `TimeAdded` is the API server's own record and the only source that survives
> a restart of this process; failing that, the moment we watched
> `spec.unschedulable` flip. A node first seen *already* cordoned is dated
> **zero**, not now — guessing "now" would relax every subject in its domain
> for a whole settle window after every restart, and a suppression that
> switches itself on at startup is worse than one that under-reports. The
> timestamp also survives an *un*cordon, because the row asks whether a node
> *became* unschedulable recently and the pods a completed drain evicted are
> still landing.
>
> **`DrainedAt` is scoped by eligible domain, not by node**, because
> `leeway.Eligibility` carries domains and not node names (§7.1). That is the
> right scope anyway: a drain narrow enough to miss every domain a subject can
> reach cannot have moved its pods, and one that empties a whole domain removes
> it from the eligible set, at which point §7.1 re-apportions rather than §7.6
> relaxing.
>
> **Suppression is decided at scoring time and kept**, rather than recomputed
> when §8.2 reads the verdict. It therefore goes stale with the evaluation
> carrying it: a subject scored during an outage stays suppressed until
> something re-evaluates it. That bound is accepted because the alternative
> lets the verdict in a finding disagree with the verdict behind the exported
> scores, and because the cost is only latency — the end of an outage is itself
> a burst of node events, and a subject coming out of suppression still owes
> §8.2 a full dwell, which is longer than the staleness.

> **The last two rows — rollout and recent scale — went live 2026-09-19.** All
> five of the table's rows are now answered. Four decisions:
>
> **The rollout answer comes from the rollout source, as the table says, and
> the adapter lives at the composition root.** `rollout.Source.RollingOut()`
> returns the whole cluster's mid-rollout set; `topologydrift.RolloutOracle` is
> a `func() []leeway.SubjectRef`; `internal/watch` owns the ten lines between
> them. Neither package imports the other, and a deployment that turned the
> rollout source off leaves the row unanswered and says so at startup rather
> than pretending nothing is ever rolling.
>
> **`RollingOut` is NOT the rollout source's own completeness predicate.**
> `deploymentComplete` additionally requires every replica to be *available*,
> which is right for a stall verdict bounded by an observation window and
> wrong here: a Deployment with one pod in `CrashLoopBackOff` is permanently
> incomplete, and reusing it would relax that subject's placement thresholds
> forever. The predicate is the table's three clauses, each of which closes on
> its own once the controller stops moving pods, whether or not the new pods
> are healthy. A *paused* Deployment reports **not** rolling out: it can sit
> half-rolled for weeks, which is a steady state somebody chose and exactly
> the workload that should still be judged.
>
> **Both cluster-wide inputs are snapshotted on the same timer.** `evaluate`
> runs per subject, so asking an oracle that scans every Deployment in the
> cluster from inside it would turn one pass over 20,000 subjects into 20,000
> scans. The rollout set is refreshed on the ready-count tick
> (`DefaultReadySampleInterval`, 30 s) and read from a map; the cost is
> noticing a rollout up to one interval late, against a dwell measured in
> minutes.
>
> **A scale event has to be watched, and a first sighting is not one.**
> Kubernetes records no timestamp for a `spec.replicas` change —
> `metadata.generation` moves but also moves for template edits, and neither
> says when — so the source keeps a `scaleLog` fed by Deployment and
> StatefulSet handlers and stamps only a size that differs from the one on
> file. Re-seeing the same size stamps nothing, or the informer's own resync
> would be a permanent cluster-wide scale event; a workload first seen at
> startup is dated **zero**, the same call the cordon row makes and for the
> same reason. Entries outlive the settle window because the count in them is
> the baseline the next change is measured against, and are dropped on delete.
> DaemonSet and Job subjects have no scale row: neither has a `spec.replicas`,
> and a DaemonSet's size is the node count, which the drain and outage rows
> already cover.
>
> This costs two LIST+WATCH streams, which §6.1 permits because the sentinel
> already carries both — the rollout source watches Deployments, StatefulSets
> and ReplicaSets, and objectstate watches Deployments. `RequiredAccess` now
> declares `apps/deployments` and `apps/statefulsets` rather than treating them
> as optional, because §11's check is a coverage contract and a missing grant
> here is a transient row that silently never fires.

### 7.7 Preference-rank tracking (GKE custom compute classes)

A GKE custom compute class is an **ordered** list of provisioning priorities. A
workload asks for the class; GKE attempts priority 0 and on insufficient capacity
falls back down the list. Whichever rule it lands on, the pod is `Running` and the
Deployment is at full replica count — so a cluster can spend weeks on its
third-choice fallback with nothing anywhere indicating it. Same silent-degradation
failure mode, different axis, same plumbing.

**It is explicitly not skew.** Zones are peers and the desired distribution is even.
Priorities are *ranked*, and the desired distribution is all mass at rank 0. None of
§7.2–7.4 applies — no apportionment, no `maxSkew`, no relocation distance. Reusing
`ρ` here would be a category error. The score is rank-weighted concentration over
the same machinery.

#### 7.7.1 Model

```go
// PreferenceAxis generalises "ordered fallback list". GKE compute classes are the
// first provider, but Karpenter NodePool weights and weighted preferred node
// affinity have the same shape, so this is deliberately not GKE-specific.
type PreferenceAxis struct {
    Provider string           // "gke-computeclass"
    Name     string           // class name
    SpecHash string           // hash of the ordered rule list — see §7.7.5
    Rules    []PreferenceRule // in spec.priorities order
    Ordering OrderingMode     // ByListPosition | ByPriorityScore | OrderingInvalid
    Tiers    int              // distinct preference levels (≤ len(Rules))
}

type PreferenceRule struct {
    Index int  // position in spec.priorities — what ccc_priority_index holds
    Score *int // spec.priorities[].priorityScore; GKE 1.35.2-gke.1842000+, optional
    Rank  int  // derived preference tier, 0 = most preferred. NOT the same as Index.
    Match NodeProfileMatcher
    Raw   map[string]any // original rule, carried into finding evidence
}

// NodeProfile is the normalised attribute set rules are matched against.
type NodeProfile struct {
    InstanceType  string // "n2-standard-8"
    MachineFamily string // "n2"
    Spot          bool
    Reservation   ReservationRef
    Accelerator   string
    Labels        map[string]string // raw, for user-defined matchers
}

// ReservationRef identifies a consumed GCE reservation. A reservation name is
// unique only within its project, and GKE can consume one shared from another
// project, so Name alone is not an identity — S3 measured Project on the node.
type ReservationRef struct {
    Name     string // cloud.google.com/reservation-name
    Project  string // cloud.google.com/reservation-project
    Affinity string // cloud.google.com/reservation-affinity: "specific" | "any" | ""
}
```

**Rule identity and preference rank are different things, and conflating them is the
trap this model exists to avoid.** The optional `priorityScore` field
(1.35.2-gke.1842000+) sets preference explicitly, *higher meaning more preferred* —
the opposite direction to list position — and **several rules may share a score**,
making them equal-preference alternatives rather than a fallback sequence. GKE
requires the field on all rules in a class or none.

**Spike S2 confirmed all three claims against a live cluster** (`std-simian-test`,
2026-09-15) and the confirmation of the last one is not a documentation reading:
applying a class where one of two rules carried a score was refused at admission
with *"PriorityScore must be set for all priorities or for none of them"*. The
all-or-nothing rule is enforced by the API server, not merely documented.

The decisive observation for the first claim: a class whose **least** preferred rule
sat at list position 0 (`n2`, score 10) ahead of two tied rules (`n4` and `c3`, both
score 50). GKE skipped index 0 entirely and provisioned `c3`, stamping
`ccc_priority_index: "2"` — the *index*, on a node the class ranks first. Had the
field meant a rank, a top-tier node would have read `"0"`. Preference and identity
are genuinely separate wire values, and only one of them is on the node.

It also produced the peer case by accident, which is the better evidence. The first
scale-up attempt was `n4` (index 1) and it hit a real stockout — *"GCE out of
resources"* in `us-central1-f`. GKE fell through to `c3` (index 2). The index rose
by one; the rank did not move, because both rules score 50. **An implementation that
read the annotation as a rank would have reported a fallback here, on the first
workload it ever saw, and it would have been wrong** — the class author declared
those two families interchangeable.

```go
// Rank is always derived, never read off the wire. Without priorityScore, list
// position is the preference order and Rank == Index. With scores, rules group by
// descending score and every rule in a group shares a Rank.
func assignRanks(rules []PreferenceRule) OrderingMode {
    scored := 0
    for _, r := range rules {
        if r.Score != nil {
            scored++
        }
    }
    switch {
    case scored == 0:
        for i := range rules {
            rules[i].Rank = rules[i].Index
        }
        return ByListPosition
    case scored == len(rules):
        // Descending: highest score is most preferred, so it becomes rank 0.
        distinct := sortedDescendingDistinctScores(rules)
        for i := range rules {
            rules[i].Rank = indexOf(distinct, *rules[i].Score)
        }
        return ByPriorityScore
    default:
        // Unreachable against a current GKE API server, which rejects a mixed
        // class at admission (S2). Kept because this decodes objects we did not
        // admit — an older control plane, a CRD restored from backup, or a
        // non-GKE provider reusing the shape. Refuse to invent an ordering.
        return OrderingInvalid
    }
}
```

This reproduces the peer/degradation distinction §7.1 draws for topology domains, and
for the same reason: **rules sharing a tier are peers.** A workload moving to a
sibling at the same score has not degraded — it took an equally-preferred
alternative, and alerting on it would be a false positive of exactly the kind §7.1
exists to prevent. Every score in §7.7.3 is computed over `Rank`, never `Index`.

**Single-tier axes are skipped.** An axis with `Tiers < 2` has no fallback to detect
— rank is always 0 by construction — so it is excluded from drift scoring and emits
`axis_info` only. Not a corner case: the four GKE-managed classes on the inspected
cluster (`autopilot`, `autopilot-arm`, `autopilot-spot`, `autopilot-arm-spot`) each
define exactly one priority, so without this rule every GKE cluster starts with four
axes of guaranteed-silent noise.

#### 7.7.2 Determining the achieved rank

**Verified against a live GKE cluster, 2026-09-11.** GKE records the provisioned
priority on the node, so this is ground truth rather than inference:

```yaml
# node gke-std-simian-test-nap-n4-highmem-2--c2a26868-0ue3
metadata:
  annotations:
    ccc_priority_index: "0"            # ← index into spec.priorities, 0-based
  labels:
    cloud.google.com/compute-class: n4-preferred
    cloud.google.com/machine-family: n4
    cloud.google.com/gke-provisioning: standard
    node.kubernetes.io/instance-type: n4-highmem-2
spec:
  taints:                              # ← see §11; affects topology eligibility
  - key: cloud.google.com/compute-class
    value: n4-preferred
    effect: NoSchedule
---
# ComputeClass n4-preferred — cloud.google.com/v1, cluster-scoped
spec:
  priorities:
  - machineFamily: n4                  # index 0  ← the node above
  - machineFamily: n2                  # index 1
  activeMigration: { optimizeRulePriority: true }
  nodePoolAutoCreation: { enabled: true }
  whenUnsatisfiable: DoNotScaleUp
```

The node is `machineFamily: n4` and carries `ccc_priority_index: "0"` — confirming a
0-based index into the ordered list. **This replaces inference as the primary path**
and removes the largest correctness risk in §7.7.

Note precisely what it gives us: **rule identity, not preference rank.** On this
class the two coincide because no `priorityScore` is set. On a scored class they
diverge, and rank must come from `assignRanks`. Reading the annotation as a rank
would invert the ordering on any scored class — S2 measured exactly that divergence
(index `"2"` on a rank-0 node) and §7.7.1 records the run.

The consequence is a hard dependency, not a preference: **a node cannot be ranked
from the node alone.** The annotation names a rule; only the `ComputeClass` object
says what that rule is worth. So the class is a watched input, and a rank resolved
while its class is unsynced is `RankUnknown` — never `0`.

**The annotation is not always a number, and not always present.** Spike S1 drove a
real class through fallback, migration-back and rule invalidation on
`std-simian-test` (2026-09-15) and observed three distinct value shapes:

| Value | Meaning | When |
|---|---|---|
| `"0"`, `"1"`, … | 0-based index into `spec.priorities` | a rule matched |
| `ccc_no_rule_matching` | the class has **no** rule this node satisfies | class edited under a running node |
| `ccc_scale_up_anyway` | provisioned outside the priority list entirely | `whenUnsatisfiable: ScaleUpAnyway` and nothing matched |
| *(absent)* | not stamped **yet** | first ~43 s of every node's life |

`strconv.Atoi` on this field fails on three of the four cases, so `parseIndex` must
be total over strings, not a numeric parse with an error branch. The two sentinels
are also not errors — they are **findings**. `ccc_no_rule_matching` says the class
was edited out from under running capacity; `ccc_scale_up_anyway` says the class
stopped constraining placement at all. Both are exactly the silent degradation this
subsystem exists to surface, and both would be discarded by a parser that only
believes in integers.

**The absent case is the dangerous one, because it is transient and universal.**
Every node observed carried the class *label* and the class *taint* from creation
but no rank annotation for 33–44 s afterwards, and in two of four cases pods were
already `Running` on the node before the rank appeared:

| node | created | rank stamped | lag |
|---|---|---|---|
| `…nap-n2-highcpu-4--ee04a49d-bq7h` | 18:18:19 | `1` at 18:18:52 | 33 s |
| `…nap-n4-highcpu-4--d7cef40b-th48` | 18:25:11 | `0` at 18:25:55 | 44 s |
| `…nap-n4-highcpu-4--d156f7c3-rpvc` | 18:32:03 | `0` at 18:32:47 | 44 s |
| `…nap-n4-highcpu-4--d7cef40b-x627` | 18:45:04 | `ccc_scale_up_anyway` at 18:45:47 | 43 s |

Defaulting absent-to-zero — the obvious reading, and the one the §13 sweep snippet
originally used — manufactures a spurious rank-0 → rank-1 fallback 40 seconds into
the life of every node that lands anywhere but rank 0. Absent must stay absent:
`RankUnknown`, contributing to no bucket, with the pods on that node parked until
the annotation arrives.

The same node also mutates through these states in place — `rpvc` went
`(absent) → 0 → ccc_no_rule_matching → ccc_scale_up_anyway` across 13 minutes without
being replaced — so the annotation is a live field to be watched, not a birth
certificate to be read once at node-add.

Two further things temper it:

- **It is undocumented.** `ccc_priority_index` does not appear in GKE's public
  documentation, and the unprefixed key marks it as internal with no stability
  guarantee. The team's judgement is that it is unlikely to change in the relevant
  timeframe, and this design *accepts* that risk rather than designing around it.
  The mitigation is that we never depend on it alone: the matcher stays as a live
  fallback, and the disagreement/unmatched counters turn a silent GKE-side change
  into a loud one, most likely at cluster-upgrade time. Depending on an undocumented
  field is fine when losing it degrades you to a slower path instead of blinding you.
- **Annotations are the first thing a transform discards** (§6.1). The
  retained-annotation set is a functional dependency of this feature, and under a
  *shared* transform that dependency is now cross-source.

The matcher survives, demoted to two jobs: **fallback** when the annotation is
absent, **cross-check** when it is present.

```go
// Resolve returns which rule the node was provisioned from, and the preference tier
// that rule belongs to. The annotation supplies rule identity; the tier always comes
// from assignRanks, because with priorityScore the two differ.
//
// The cross-check is the valuable part: running both paths costs a memoised lookup
// and catches the two failures that are otherwise invisible — GKE changing the
// annotation's meaning, and our own parsing of spec.priorities being wrong.
func (r *RankResolver) Resolve(node *v1.Node, axis *PreferenceAxis) RankPlacement {
    // Total over strings: the field carries integers, two sentinels, or nothing
    // at all (S1). Only declaredIndex is an index; the rest are states.
    declared := parseDeclared(node.Annotations[r.cfg.PriorityIndexAnnotation])
    inferred, quality := r.match(profileOf(node), axis) // first-match-wins

    switch declared.kind {
    case declaredNoRuleMatching:
        // The class was edited while this node kept running. Not an error, and
        // not inferable either — no rule matches by construction.
        r.metrics.NoRuleMatching.Add(1, attribute.String("axis", axis.Name))
        return RankPlacement{Rank: RankUnsatisfiable, RuleIndex: -1, Source: SourceNodeAnnotation}
    case declaredScaleUpAnyway:
        // The class stopped constraining placement. Every pod here is off-axis.
        r.metrics.ScaleUpAnyway.Add(1, attribute.String("axis", axis.Name))
        return RankPlacement{Rank: RankOffAxis, RuleIndex: -1, Source: SourceNodeAnnotation}
    case declaredAbsent:
        // Transient for the first ~43s of every node's life, so this is the
        // common path at node-add, not an anomaly. Inference may still answer;
        // if it does not, stay unknown rather than defaulting to rank 0.
        if quality != matchOK {
            r.metrics.Pending.Add(1, attribute.String("axis", axis.Name))
            return RankPlacement{Rank: RankUnknown, RuleIndex: -1, Source: SourceNone}
        }
    }

    var idx int
    var src RankSource
    hasDeclared := declared.kind == declaredIndex
    switch {
    case hasDeclared && quality == matchOK && declared.index != inferred:
        r.metrics.Disagreement.Add(1, attribute.String("axis", axis.Name))
        fallthrough
    case hasDeclared:
        idx, src = declared.index, SourceNodeAnnotation
    case quality == matchOK:
        idx, src = inferred, SourceInferred
    default:
        r.metrics.Unmatched.Add(1, attribute.String("axis", axis.Name))
        return RankPlacement{Rank: RankUnknown, RuleIndex: -1, Source: SourceNone}
    }

    // The class can be edited while nodes provisioned under the old spec are still
    // running, so a stale index is expected, not exceptional — and indexing the
    // slice without this check is a panic waiting for a rule deletion.
    if idx < 0 || idx >= len(axis.Rules) {
        r.metrics.OutOfRange.Add(1, attribute.String("axis", axis.Name))
        return RankPlacement{Rank: RankUnknown, RuleIndex: idx, Source: src}
    }
    return RankPlacement{Rank: axis.Rules[idx].Rank, RuleIndex: idx, Source: src}
}
```

The extractor table stays configuration rather than code:

```yaml
preferenceAxes:
  - provider: gke-computeclass
    source:
      apiVersion: cloud.google.com/v1     # verified
      kind: ComputeClass                  # verified — cluster-scoped, short name "cc"
      prioritiesPath: .spec.priorities    # verified — ordered, 0-based
    podClassSelector:
      nodeSelectorKey: cloud.google.com/compute-class    # verified
    rank:
      priorityIndexAnnotation: ccc_priority_index        # verified — primary
      inferenceEnabled: true                             # fallback + cross-check
    nodeProfile:
      instanceType: { label: node.kubernetes.io/instance-type }    # verified
      machineFamily:
        label: cloud.google.com/machine-family                     # verified
        deriveFrom: { field: instanceType, regex: '^([a-z0-9]+)-' }
      spot:
        anyOf:
          - { label: cloud.google.com/gke-provisioning, notEquals: standard } # verified
          - { label: cloud.google.com/gke-spot, equals: "true" }
      reservation:
        name:     { label: cloud.google.com/reservation-name }     # verified — S3
        project:  { label: cloud.google.com/reservation-project }  # verified — S3
        affinity: { label: cloud.google.com/reservation-affinity } # verified — S3
      # Three labels, not one. A reservation name is unique only within a project
      # and GKE can consume a reservation shared from another project, so the
      # matcher compares the (project, name) pair — never the name alone. The
      # rule side carries both: reservations.specific[].{name,project}. Affinity
      # is the rule's reservations.affinity lowercased ("specific"); it is not
      # matched on, but it distinguishes a node that had to take this exact
      # reservation from one that merely happened to land on it.
      accelerator: { label: cloud.google.com/gke-accelerator }     # verified — S3
      # Value is the GPU type verbatim as written in the rule's gpu.type
      # ("nvidia-tesla-t4"), so the matcher compares the two directly with no
      # normalisation. A sibling label cloud.google.com/gke-gpu-driver-version
      # also appears; it is not placement-relevant and is deliberately unmodelled.
```

Priority rules are **sparse**: `{machineFamily: n4}` constrains only the family and
wildcards everything else. The matcher must treat unspecified fields as
unconstrained, not required-empty.

They are also **open-ended**, which is the more dangerous property. Rule fields
observed on the inspected cluster already exceed what `NodeProfile` models — the
managed Autopilot classes use `podFamily: general-purpose` — and GKE adds fields over
time (`priorityScore` being the current example). A matcher that ignores fields it
does not understand will **match rules it should not**, silently attributing nodes to
the wrong rule and corrupting the cross-check into agreement with nothing.

So the matcher fails closed: a rule field outside the supported set yields
`matchUnsupported` for that rule, not a match and not a miss. Unsupported rules are
excluded from inference, counted with a `field` label naming the offending key, and
fall back to the annotation. That turns "GKE shipped a new rule field" from a silent
correctness regression into a dashboard entry.

Currently supported: `machineFamily`, `machineType`, `podFamily`, `spot`,
`reservations`, `gpu`/accelerator, `minCores`/`minMemoryGb`. Anything else is
unsupported by definition until modelled.

Cost is negligible — the match memoises on `(nodeName, axisSpecHash)`, and nodes are
few relative to pods. The CRD is watched through a **dynamic/unstructured informer**
(the `gateway` source's precedent), so the binary carries no GKE types and a cluster
without the CRD reports the axis unavailable rather than crash-looping.

#### 7.7.3 Metrics — the "most of the time" part

The question is not *how many pods are at rank 2 right now* but *what fraction of the
time do we run at rank 2*, so the primary signal is **time-weighted**. A gauge
answers the wrong question: a 90-second burst of rank-3 pods during a scale-up and
three weeks parked on the spot fallback look identical to a scrape every 30 s.

```
# Primary — pod-seconds accumulated at each rank. (OTEL name; see §8.4 for the
# Prometheus spelling, which is derived, not hand-written.)
lookout.leeway.preference.pod_time            {provider,axis,spec_hash,rank}  unit s
lookout.leeway.preference.rank_weighted_time  {provider,axis,spec_hash}       unit s

# Supporting.
lookout.leeway.preference.pods                {...,rank}            gauge
lookout.leeway.preference.transitions         {...,from_rank,to_rank,lateral}
                                       # SUBJECT-level, not pod-level (S1): a pod is
                                       # born and dies at one rank and never moves.
lookout.leeway.preference.unmatched           {...}   # no rule matched, no annotation
lookout.leeway.preference.ambiguous           {...}   # node matched >1 rule
lookout.leeway.preference.disagreement        {...}   # annotation != inferred
lookout.leeway.preference.out_of_range        {...}   # stale index after a class edit
lookout.leeway.preference.axis_invalid        {...}   # partially-scored class

# The two GKE sentinels (S1). Both are findings, not parse errors — see §7.7.2.
lookout.leeway.preference.no_rule_matching    {...}   # ccc_no_rule_matching
lookout.leeway.preference.off_axis            {...}   # ccc_scale_up_anyway
lookout.leeway.preference.rank_pending        {...}   # node has the class, no rank yet
lookout.leeway.preference.axis_info           {provider,axis,spec_hash,ordering,rules,tiers}
```

Every rank-bearing series carries three related labels, and the distinction between
the first two is the whole point of §7.7.1:

| Label | Meaning |
|---|---|
| `rank` | Preference **tier** ordinal, 0 = most preferred. Derived. All scoring uses this |
| `rule_index` | Which rule — the raw `ccc_priority_index`. Identity only, never ordering |
| `rule` | Rendered rule for humans, e.g. `machineFamily=n4` |

`source` (`annotation` or `inferred`) is carried too.

`lateral="true"` marks a transition between two rules sharing a tier. Counted —
they indicate capacity churn within a preference level — but **excluded from every
degradation signal**, because an equal-score alternative is not a fallback.

Accumulation is O(1) per transition, on the same delta path as everything else — no
per-pod timers:

```go
// Called on any change to a pod's (axis, rank) assignment, plus a flush ticker so a
// quiescent rank still advances.
func (b *rankBucket) accumulate(now time.Time) {
    b.podSeconds += float64(b.count) * now.Sub(b.lastFlush).Seconds()
    b.lastFlush = now
}

// Enter/Leave, not Move: S1 established that pods are created and destroyed at a
// rank, never moved between ranks. The two calls are independent and usually
// minutes apart, on different pods.
func (t *RankTracker) Enter(axis AxisKey, rank int, at time.Time) {
    t.bucket(axis, rank).accumulate(at); t.bucket(axis, rank).count++
}

func (t *RankTracker) Leave(axis AxisKey, rank int, at time.Time) {
    t.bucket(axis, rank).accumulate(at); t.bucket(axis, rank).count--
}
```

> **`Move` was modelling an event that does not occur. Spike S1 confirmed the
> pessimistic branch** (`std-simian-test`, 2026-09-15). A fallback provisions a new
> node in a new NAP pool and the ReplicaSet creates a *new pod* there; the old pod is
> deleted. Observed end to end, twice:
>
> ```
> 18:14:03  Deployment created, class rank 0 unsatisfiable
> 18:14:10  pod vd5cj  Pending/Unschedulable
> 18:18:52  node bq7h  rank 1     — new node, NEW NAP pool
> 18:18:30  pod vd5cj  Running on bq7h
> ---- rank 0 made satisfiable at 18:20:54 ----
> 18:25:11  node th48  rank 0     — new node, new pool, different zone
> 18:25:33  pod vhp6p  bound to th48          ← a DIFFERENT pod
> 18:27:11  node bq7h  drained and deleted    ← 2 min AFTER the rank-0 node came up
> ```
>
> **No pod ever changes rank.** `vd5cj` died at rank 1 and `vhp6p` was born at rank 0.
> `RankTracker.Move` has no per-object event to hang on and is deleted; pod-seconds
> accounting survives unchanged, because the old pod stops accruing and the new one
> starts, which is correct either way. The transitions counter and the migration-back
> finding must both be reconstructed at **subject** level — *this Deployment's rank
> mix shifted between evaluations* — exactly as the pessimistic branch predicted.
>
> Two details that only fall out of watching it happen:
>
> - **Ranks overlap during migration.** The rank-0 node was up at 18:25:11 and the
>   rank-1 node was not drained until 18:27:11. For two minutes the class legitimately
>   had capacity at both ranks. "The current rank of this workload" is not a scalar
>   during a migration, and a subject-level differ must compare *mixes*, not modes,
>   or it will report a spurious lateral flap every time GKE optimises.
> - **Migration-back works and is slow.** `activeMigration.optimizeRulePriority: true`
>   acted 4 m 17 s after the class edit. The finding is real; its detection window
>   must be minutes, not seconds.

**There is a second fallback shape, and it is the invisible one.** S2 tripped a real
stockout and produced a fallback that looks nothing like S1's:

```
19:30:36  Deployment created
19:30:41  pod jlfzm  TriggeredScaleUp   -> n4  (index 1)   us-central1-f
19:31:21  pod jlfzm  FailedScaleUp      -> "GCE out of resources"
19:32:41  node rbjh  appears            -> c3  (index 2)   us-central1-a
19:32:25  pod jlfzm  Scheduled on rbjh                     ← the SAME pod
```

S1's fallback was **post-hoc**: a node existed at one rank, the class changed
underneath it, and migration replaced the pod. S2's is **pre-emptive**: the preferred
rule never produced a node, so the pod simply waited 1 m 49 s in `Pending` and was
bound once at the rank it could get. One pod, one `Bind`, no transition — the pod was
*born* at the achieved rank.

The consequence for detection is uncomfortable. In the pre-emptive shape **nothing
observable ever exists at the preferred rank** — no node, no pod, no annotation. The
node and pod caches cannot distinguish "the class asked for `n4` and could not get
it" from "the class asked for `c3` and got it". The only trace is the `FailedScaleUp`
Event and the Pending duration, neither of which is in §7.7's inputs. So §7.7 scores
what a workload *achieved*, and is structurally blind to what it *attempted*:

- Pod-seconds and rank shares stay correct — the pod genuinely ran at that rank for
  that long, which is what the metric claims.
- "Unmet preference" is a **different detector** on a different input (the event
  stream), not a variation of this one. It is out of scope here; noting it as a
  known gap is the honest outcome rather than quietly widening §7.7 to cover it.

The original question — *is the second priority used more than the first?* — is a
PromQL one-liner:

```promql
# Share of time spent at each rank over the last week.
  sum by (rank) (increase(lookout_leeway_preference_pod_time_seconds_total{axis="high-perf"}[7d]))
/ ignoring(rank) group_left()
  sum         (increase(lookout_leeway_preference_pod_time_seconds_total{axis="high-perf"}[7d]))

# Mean achieved rank (0 is ideal).
  sum by (axis) (increase(lookout_leeway_preference_rank_weighted_time_seconds_total[1d]))
/ sum by (axis) (increase(lookout_leeway_preference_pod_time_seconds_total[1d]))
```

**Cardinality.** Default aggregation is per-axis; `namespace`/`subject` labels are
opt-in per axis. Rank count is small (2–5), so per-axis series are trivial —
per-subject series are what would multiply.

#### 7.7.4 What to report

The §8.2 state machine, dwell, hysteresis and §7.5 baselines all apply unchanged;
only the score differs.

| Signal | Meaning | Tier | Kind |
|---|---|---|---|
| Rank-0 time share below threshold | First-choice capacity has degraded | B | `leeway.rank_degraded` |
| Mean achieved rank rises vs. its own EWMA baseline | The mix got worse | C | `leeway.rank_degraded` |
| Sustained time at the last rank | Running on last-resort capacity, often spot | B | `leeway.rank_degraded` |
| An entire **tier** unused in 30d | Dead preference level — or a reservation being paid for and never used | info (cost) | `leeway.rank_tier_unused` |
| A single rule unused, siblings in its tier busy | Weak signal: an equal-score sibling absorbed demand, which is the design intent of grouping | info, off by default | — |
| No `from_rank > to_rank` transitions after capacity returns | Active migration back to preferred capacity is not happening | B | `leeway.rank_no_migration` |
| Pods Pending against a `DoNotScaleUp` class | No priority can be satisfied and GKE will not scale up — the class is wedged, and the pods are the only symptom | A | `leeway.rank_wedged` |
| Unmatched / ambiguous / disagreement rates | **Our integration is broken**, not the cluster | tool SLI | metrics only, never a Signal |

The last row is deliberately not a finding. Reporting a cluster finding derived from
an integration we already know is misconfigured would be worse than reporting nothing.

**Two gates come from the class spec**, which is why we parse it rather than just
counting indexes:

- `activeMigration.optimizeRulePriority` — when `true`, GKE is expected to migrate
  workloads back once capacity returns, so *failure to see* `from_rank > to_rank`
  after a recovery is a real finding. When absent or `false`, nothing will migrate and
  that finding must be suppressed or it fires forever on working clusters.
- `whenUnsatisfiable` — `DoNotScaleUp` means pods stay Pending rather than falling
  further, so on those classes Pending pods are the fallback signal and rank
  distribution alone will not show the problem.

> **Naming collision, worth guarding in review.** `ComputeClass.spec.whenUnsatisfiable`
> (`DoNotScaleUp`) and `topologySpreadConstraint.whenUnsatisfiable` (`DoNotSchedule` /
> `ScheduleAnyway`) share a field name and share nothing else. They must not use the
> same Go type — and in this repo they will now sit in adjacent packages.

#### 7.7.5 Caveats

- **Editing a class changes what a rank means.** Insert a priority at position 1 and
  every historical `rank=1` sample silently starts referring to something else. Hence
  `spec_hash` as a label, a baseline reset when it changes, and a retained
  `spec_hash → rules` mapping so old series stay interpretable.
- **Re-scoring re-tiers a class without touching its rules.** Changing only a
  `priorityScore` leaves the rule list identical while moving rules between tiers.
  `spec_hash` must be computed over `priorityScore` as well as rule bodies, or this
  class of edit slips through the baseline-reset guard — the edit most likely to be
  made casually.
- **`priorityScore` requires 1.35.2-gke.1842000+.** Older clusters cannot use it, so
  `ByListPosition` is not legacy and both orderings must be supported indefinitely.
  The inspected cluster runs 1.36.3-gke.1537000 — available and simply unused, so it
  can appear at any time without a cluster upgrade.
- **First-match-wins is a choice, not a fact.** Where rules overlap, attribution is
  arbitrary; count it rather than hiding it. Scoring makes ambiguity *less* harmful
  for the numbers that matter: if two overlapping rules share a tier, either
  attribution yields the same `rank`.
- **`DoNotScaleUp` and `activeMigration: true` appear to be GKE defaults** — every
  class on the inspected cluster carries both, none deliberately. The gates above are
  still correct, but expect them to be almost always on.
- **Not all fallback is bad.** A class whose rank-2 is a cheaper family may be
  *intended* to carry most of the load. Default to reporting *change* in the mix
  (Tier C) rather than absolute depth, unless policy says otherwise.
- **Never read topology from pod labels.** The sample pod carries
  `topology.kubernetes.io/zone` as a *pod* label, stamped by a chart or webhook. It
  agrees with the node today, but nothing keeps it in sync across a reschedule.
  Topology is always derived from the node the pod is currently on.

#### 7.7.6 Worked example (verified end to end)

From the live cluster, exercising most of the design at once:

```
Deployment chaos-dns-server (ns chaos-mesh)
  └─ ReplicaSet chaos-dns-server-545f4584b9        ← subject resolution, §5
       └─ Pod ...-tpmn4
            nodeSelector:  cloud.google.com/compute-class: n4-preferred
            tolerations:   cloud.google.com/compute-class=n4-preferred:NoSchedule
            affinity:      podAntiAffinity preferred, kubernetes.io/hostname, w=100
            topologySpreadConstraints: none
            nodeName:      gke-...-nap-n4-highmem-2--c2a26868-0ue3
                             ccc_priority_index: "0"     → rank 0 (machineFamily=n4)
                             topology.kubernetes.io/zone: us-central1-f
```

Four things fall out, each validating an earlier decision:

1. **Intent is inferred, not declared.** No TSC; the only signal is a *preferred*
   anti-affinity on hostname. Tier B via `SourcePodAntiAffinityPreferred` — and it is
   the common case, not the exotic one. A tool that only understood
   `topologySpreadConstraints` would have nothing to say about this workload. (Note
   the existing `audit.no_spread` check *would* also fire here, on the static half of
   the same observation.)
2. **The toleration is auto-injected.** Setting the class `nodeSelector` gets you a
   matching toleration for that class's taint, and only that one. So the pod is
   admissible on `n4-preferred` nodes and inadmissible on `n2-preferred` ones.

   S3 found this is not one injection but a pipeline of them, and that it is not
   confined to compute classes. A GPU node carries **two** `NoSchedule` taints —
   `cloud.google.com/compute-class=<name>` and `nvidia.com/gpu=present` — and the
   probe pod, whose manifest declared no tolerations at all, was admitted with both:

   ```yaml
   tolerations:                      # none of this was in the manifest
   - { key: nvidia.com/gpu,                    operator: Exists, effect: NoSchedule }
   - { key: cloud.google.com/compute-class, operator: Equal,
       value: leeway-gpu-probe,                                 effect: NoSchedule }
   ```

   **Eligibility must be computed from the admitted Pod, never from a workload
   template.** A `PodTemplateSpec` on the owning Deployment has neither toleration —
   injection happens at Pod admission, so the template is a strictly weaker document
   than the object that got scheduled. Any intent inference reading templates (FR-9's
   path) will compute an eligible-node set that is too small, and §7.1's eligibility
   reduction would then *over*-correct: it would conclude a workload is pinned to
   fewer domains than it really is, and suppress genuine drift. This is the mirror
   image of the §7.7.6 false positive and is the more dangerous direction, because it
   fails silent.
3. **Therefore its eligible zone set is one zone.** On this cluster `n4-preferred`
   has a single node, in `us-central1-f`; `n2-preferred`'s nodes are in `-a` and `-b`.
   Naive zone counting would report this Deployment as maximally skewed — 100% in one
   zone out of three. The §7.1 eligibility rule reduces the domain set to
   `{us-central1-f}`, the expectation to "all of it there", and the drift to zero.
   **This is the single largest false-positive class in the tool, and a compute-class
   cluster manufactures it by construction.**
4. **Eligible domains are set by the autoscaler, not by config.** With
   `nodePoolAutoCreation.enabled: true`, the zones where a class has nodes change as
   NAP creates and removes pools. For class-pinned workloads the eligible domain set
   is *dynamic*, so §7.5's "invalidate the baseline when eligibility changes" will
   trigger far more often than on a statically-provisioned cluster. Baseline resets
   need a debounce (default: ignore eligibility changes lasting < 30 min) or these
   workloads will never mature a baseline at all.

---

## 8. Findings, signals and metrics

### 8.1 Confidence tiers

| Tier | Meaning | Trigger | Severity |
|---|---|---|---|
| **A — Contract violation** | User declared it; Kubernetes is not honouring it | `S > maxSkew` on a `DoNotSchedule` TSC; required pod anti-affinity violated | `critical` |
| **B — Inferred intent deviation** | Strong signal of intent, not a hard contract | `ScheduleAnyway` TSC exceeded; preferred anti-affinity ignored; `ρ` over threshold | `warning` |
| **C — Behavioural drift** | No declared intent; deviation from the subject's own history | Baseline band breach (§7.5) | `info`, escalating to `warning` on `max domain share` breach |

**An assumed cluster default can never reach Tier A** (S4). Mostly this falls out of
the table already: the upstream `defaultConstraints` are `ScheduleAnyway`, and Tier A
needs `DoNotSchedule`. But an operator *may* declare a default that says
`DoNotSchedule`, so the exclusion is stated as its own rule rather than left to fall
out — an intent whose source is `cluster-default-assumed` is capped at Tier B
regardless of its `whenUnsatisfiable`, because we would be raising a `critical` on a
contract we guessed at. A `cluster-default-declared` intent carries no such cap: an
operator who writes the constraint down has made the assertion, and it is theirs.

> **Shipped 2026-09-19 as `leeway.Judge`, with the tier read off the rule that
> fired rather than off the numbers.** Four named breach rules — `per-domain-ceiling`,
> `max-skew`, `drift`, and none — and the first two are the Tier A gate. The
> alternative, re-deriving "a contract was violated" downstream from `E > 0`, is
> only correct if a contract existed, and by rendering time that context is gone.
>
> This forced the two hard contracts apart. `HardContract()` was one predicate
> answering for both a `DoNotSchedule` `maxSkew` and a required `podAntiAffinity`'s
> per-domain ceiling, and its only consumer paired it with `E` — a quantity derived
> from `maxSkew` alone. A subject whose only declared bound was the ceiling
> therefore fired Tier A with the reason "observed skew exceeds the declared
> maxSkew", against a floor the ceiling had nothing to do with. There are now two
> predicates, `HardSkewContract` and `HardPerDomainContract`, each checked against
> the quantity it actually bounds; the ceiling is compared to `max(a)`, which is
> what catches the `IgnoredDuringExecution` case it exists for — two pods sharing a
> domain after a relabel, at a skew of one that no skew rule would call a violation.
>
> §8.1's "an assumed cluster default can never reach Tier A" is enforced one layer
> down, in the precondition both hard-contract predicates share, rather than
> restated as a cap in the classifier: an assumed intent cannot fire a contract
> rule, so it cannot arrive at classification with a contract to be capped.

Tier A on a `DoNotSchedule` constraint is the most interesting case: the scheduler
enforces it at admission, so a violation means the pods were placed and *then* the
world changed — a node was relabelled, a zone's nodes went away, or the pods predate
the constraint. Those are exactly the cases no admission-time control can catch, and
they are the strongest justification for this subsystem existing.

### 8.2 State machine

```
   OK ──(score > threshold)──▶ Pending ──(held ≥ forDuration)──▶ Firing
    ▲                             │                                │
    │                             │(score ≤ threshold)             │
    └───────(held ≤ threshold ────┴────────────────────────────────┘
             for resolveDuration)         Resolving
```

`Pending.firstSeenAt` and `Firing.since` persist on every transition.
`resolveDuration` defaults to 3× `forDuration` to damp flapping; a subject
transitioning Firing→OK→Firing more than `flapCount` times (default 3) within
`flapWindow` (default 1 h) is marked flapping — findings continue but are annotated,
and delivery backs off exponentially.

> **Shipped 2026-09-19 as `leeway.AlertState.Advance`** — pure, clock-injected, and
> the diagram's four phases exactly. Three details the diagram does not carry:
>
> **There are five transitions, not four, and the fifth is why the machine returns
> one rather than letting the caller diff the phases.** `Pending → OK` and
> `Resolving → OK` are the same pair of phases and opposite obligations: the first
> is an episode that was never announced and must stay silent (`abandoned`), the
> second owes a resolution to whoever received the finding (`resolved`). Only
> `firing` and `resolved` emit.
>
> **An abandoned episode is not a flap.** A subject that twitches over the
> threshold for a minute every ten is below the dwell, which is the case the dwell
> exists for — counting it would mark the quietest subjects flapping.
>
> **Flapping is counted from a pruned list of firing instants, not a counter,
> and read at query time.** A counter needs something to tick it down, and a
> subject that has settled generates no events to tick it with, so it would stay
> marked flapping until the next time it misbehaved. Counting *firings* rather
> than the Firing→OK→Firing transitions between them is also what makes one rule
> cover both ways a subject comes back: through a completed resolve, and through a
> recurrence inside the resolve window. The latter does **not** serve a second
> `forDuration` — the finding never stopped being outstanding — and does not move
> `since`, or a subject oscillating just inside the resolve dwell would report
> itself as perpetually new.
>
> `DeliveryBackoff` is `forDuration` doubled once per recurrence past the
> allowance, capped at `flapWindow`.

**Half of this machine already exists, and spike S10 found the seam to plug into.**
`pkg/engine.RecoveryTracker` runs the resolve side almost exactly as drawn above:

```
symptomatic --predicate true--> clearing
clearing --predicate false--> symptomatic        (flap: window resets)
clearing --window elapsed--> resolved
resolved --recurs within revert window--> symptomatic   (resolved.reverted, re-arm)
resolved --revert window elapses--> untracked
```

That is our `Firing → Resolving → OK`, plus flap-reset and a revert path we had not
specified, plus the `kind=resolved` emission. It is driven by an injectable
predicate:

```go
type ClearanceObserver interface {
    // Second return is false when the observer cannot judge this incident at
    // all — wrong object kind, informer not synced — and the tracker keeps
    // waiting rather than treating "don't know" as "cleared".
    Clearance(inc Incident) (Clearance, bool)
}
```

So the division of labour is sharper than "hand `pkg/engine` the fire/resolve
edges". **Leeway owns the fire side and implements `ClearanceObserver` for the
resolve side** — the observer answers "is this subject's drift score back under
threshold", and flap handling, revert and the resolved signal come free and stay
consistent with every other source's outcomes.

What `RecoveryTracker` does *not* provide is the `OK → Pending → Firing` half: it
is only handed an incident once something has already fired, so `forDuration`
dwell, the score threshold and the Tier A/B/C classification remain ours. One
constraint to design around: `NewRecoveryTracker(stableFor, emit)` takes a single
global `stableFor`, while §10.1 lets each policy set its own `resolveAfter`. Either
per-policy resolve dwell is dropped, or leeway runs its own tracker instance
alongside the sentinel's — the latter is cheap and keeps the policy promise.

> **The machine went live in the source 2026-09-19**, driving real verdicts and
> persisting through §9.1, and **emission followed 2026-09-20** — a firing
> episode now raises a signal and the source registers the
> `ClearanceObserver` described above, so the sentinel's own
> `RecoveryTracker` carries the resolve. Note the two dwells compose rather
> than compete: `AlertPhase.Firing()` is true in both `Firing` and
> `Resolving`, so the episode stays cleared-pending until §8.2's resolve
> dwell *and* the tracker's `--recovery-stable-for` have both elapsed.
>
> **It runs on its own ticker (30 s), not on the evaluation path.** Two reasons,
> and both are about what a dwell is for. A dwell measures how long a condition
> has *held*; advancing it per event would make it run faster for a subject
> whose pods churn than for a quiet one that is equally wrong, so a busy
> Deployment would fire before a settled one with identical drift. And §8.2
> wants a whole pass judged against one instant — a slow sweep over twenty
> thousand subjects would hand the ones at the end a slightly longer dwell than
> the ones at the start.
>
> **A pass must be complete, because absence is information.** An episode with
> no verdict in a pass belongs to a subject the source is no longer tracking,
> and it is dropped. That is the map's only eviction rule; nothing else would
> stop a deleted Deployment dwelling forever against a cluster that has
> forgotten it.
>
> **A non-breaching subject with no history gets no entry at all.** Not a zero
> value, not a `PhaseOK` row — nothing. Same bound as the store's (§9.3's write
> policy): the machine is sized by how much trouble a cluster is in, not by how
> large it is.
>
> **Persisted records reconcile lazily, on the first pass that carries a verdict
> for their subject-axis.** A record can only be reconciled *against* something,
> and at startup there is nothing: the caches have synced but the queue has
> scored nothing yet. Records nobody claims within `ReconcileGrace` (5 min) are
> discarded — their subjects did not come back, and holding a dwell open for a
> Deployment deleted while we were down is the phantom episode §9.3's repair
> rules exist to avoid.
>
> **Only a move is written.** An episode that sat in Pending for nine of its ten
> minutes produced no transition and changed none of §9.1's fields, so writing
> it every tick would be twenty thousand UPSERTs an hour recording that nothing
> happened.

### 8.3 Severity routing and delivery

Leeway findings are mostly **not incidents**. Drift is a slow-moving condition, and
injecting an agent session every time a Deployment's zone mix wobbles would be the
same mistake as paging on it.

| Tier | Route |
|---|---|
| A | Signal → full pipeline, including inject. A contract violation with a live blast radius is exactly what warm agent context is for |
| B | Signal → store + watchboard. Inject only if it correlates with an active storm |
| C | Metrics only by default; Signal at info severity, opt-in per policy |
| Tool SLIs | Metrics only, never a Signal |

This uses the existing severity routing (DESIGN §7.7) unchanged — the same treatment
info-severity `notifications` signals already get. It also means the default
deployment adds approximately zero agent sessions, which is the property that makes
enabling leeway by default defensible.

> **Shipped 2026-09-19 as `leeway.Verdict.Route`.** The table above is mostly not
> leeway's to implement, and saying so out loud shrank the type to four lines.
> "A injects, B reaches the watchboard, C is stored only" *is* DESIGN §7.7's
> per-severity policy, which already does that for every other source; leeway does
> not get a second delivery mechanism, it gets a severity. Tier B's storm-correlated
> inject is likewise the correlator's decision downstream.
>
> What is genuinely ours is the one row §7.7 cannot express: **Tier C is metrics
> only unless a policy opts in.** Nothing in a severity table can say "do not emit
> at all", so that has to be decided before emitting, and it is the whole of
> `Delivery`. The fourth row needs no code either — tool SLIs are §8.4 counters and
> never become a `Verdict`, so "never a Signal" is enforced by there being no path
> from one to the other rather than by a branch that could be got wrong.
>
> **A §7.6 suppression is metrics-only at every tier, Tier A included.** That is
> §14's exit criterion stated as a routing rule: a zone going away puts four hundred
> workloads in breach of spread contracts they each genuinely declared, and Tier A is
> precisely the tier that would otherwise open four hundred agent sessions about one
> fact. A *relaxation* is the opposite and changes nothing here — it decides whether
> a subject breached, never what happens once it has.

> **Wired 2026-09-20.** The source calls `Route` for every episode that crosses
> its dwell and drops the ones that come back metrics-only, silently — a Tier C
> log line per subject per episode would be the loudest thing in the process on a
> large estate, and the `lookout_leeway_*` gauges already say the finding exists.
> The opt-in is `--topology-tier-c-signals`, off by default. The severity on the
> wire is the verdict's, so the routing the table describes happens downstream in
> §7.7 exactly as this section argues it should.

### 8.4 Metrics and the export pipeline

```
lookout.leeway.domain_objects   {subject_kind,namespace,subject,topology_key,domain,state}
lookout.leeway.domain_expected  {subject_kind,namespace,subject,topology_key,domain}
lookout.leeway.observed_skew    {...}
lookout.leeway.excess_skew      {...}
lookout.leeway.drift            {...}   # ρ
lookout.leeway.max_domain_share {...}
lookout.leeway.relocation_distance {...}
lookout.leeway.intent_info      {...,source,confidence,weighting,max_skew}
lookout.leeway.domain_ready_nodes {topology_key,domain}
lookout.leeway.alert_state      {...,tier}   # 0=ok 1=pending 2=firing
lookout.leeway.transient_subjects {topology_key,transient}   # §7.6, added 2026-09-19

# Self-observability.
lookout.leeway.counter_mismatch     {subject_kind}
lookout.leeway.evaluation_duration  {subject_kind}   unit s
lookout.leeway.subjects_tracked     {subject_kind}
lookout.leeway.last_event_timestamp {resource}       unit s
```

**Instrument once, export twice.** Metrics are defined against the OpenTelemetry Go
metric API and served by two readers on a single `MeterProvider`, so there is no
second bookkeeping path to keep in sync:

```go
// Pull. WithRegisterer is the load-bearing argument (S11): the reader
// registers as a Collector into the registry internal/watch already
// serves on --metrics-addr, so leeway's OTEL-native instruments and the
// sentinel's 43 Prometheus-native ones come out of one scrape.
promExporter, _ := otelprom.New(
    otelprom.WithRegisterer(sentinelRegistry),
    otelprom.WithoutTargetInfo(), // neither exists on /metrics today;
    otelprom.WithoutScopeInfo(),  // adding them changes every dashboard's gather
)
otlpExporter, _ := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint))

mp := metric.NewMeterProvider(
    metric.WithResource(resource.NewWithAttributes(semconv.SchemaURL,
        semconv.ServiceName("lookout"),
        semconv.K8SClusterName(cfg.ClusterName),
    )),
    metric.WithReader(promExporter),
    metric.WithReader(metric.NewPeriodicReader(otlpExporter,
        metric.WithInterval(cfg.OTLPInterval))), // default 60s
)
```

Either reader can be disabled; neither requires the other.

> **Spike S11 resolved this, and it killed the premise.** The draft above assumed
> two bad options: migrate the sentinel's existing instruments first, or run two
> registries and let only leeway reach OTLP. There is a third, and it is the one
> the prototype proved: `otelprom` registers the `MeterProvider`'s pull reader
> **into the sentinel's existing `prometheus.Registry`**. One registry, one
> `promhttp` handler, one scrape — serving migrated and unmigrated instruments
> side by side, indistinguishably. Leeway declares its instruments on the OTEL API
> from day one and reaches both readers; the existing 43 `lookout_*` instruments
> stay exactly as they are. **Migrating them is not a prerequisite** and, per S11,
> costs two regressions it would be wrong to pay for on leeway's schedule.

**Names are declared in OTEL form; the Prometheus spelling is derived.** The
exporter mangles names — `.` → `_`, unit appended, `_total` appended for monotonic
counters. Hand-maintaining two lists guarantees drift. **Measured against
`otelprom` v0.66.0 during S11:**

| Declared | Unit | Exported as |
|---|---|---|
| `lookout_recoveries_reverted` | — | `lookout_recoveries_reverted_total` |
| `lookout_events_seen_total` | — | `lookout_events_seen_total` |
| `lookout_storms_active` (gauge) | — | `lookout_storms_active` |
| `lookout_enrichment_payload` | `By` | `lookout_enrichment_payload_bytes_total` |

`_total` is idempotent — the translator does not double it — so a name that already
carries the suffix survives a naive port. **The unit is the live hazard:** it is
injected into the middle of the name, before `_total`, so any instrument that sets
`WithUnit` gets a Prometheus name nobody chose. §7.7.3's pod-seconds counter must
therefore either leave the unit unset (and lose it on the OTLP side) or be named so
that the injected suffix lands where intended — `...pod_time` + unit `s` →
`..._pod_time_seconds_total`. Every metric name in this document has been written in
OTEL form for that reason.

**Counters no longer start at zero.** Also measured in S11 and worth stating
plainly, because it is the one behaviour the bridge does not preserve: a Prometheus
counter publishes `{...} 0` from the moment it is registered, whereas an OTEL
instrument publishes **nothing at all** until its first record. A leeway counter
that has never fired is absent from `/metrics`, not zero. Alerts must use
`absent_over_time()` or `or vector(0)` rather than assuming a series exists, and
`rate()` over a window containing the first-ever record has no zero anchor. This is
acceptable for new instruments where the convention can be set up front; it is the
first of the two reasons not to retrofit it onto the existing ones.

**Temporality is cumulative on both readers (S5).** The draft here proposed
per-instrument selectors on the grounds that Prometheus requires cumulative while many
OTLP backends prefer delta, and that delta would dissolve the §7.7.3 restart concern.
S5 measured the actual backend and closed the option. The target estate ships to the
GKE managed collector, whose `metrics/otlp` pipeline contains **no `deltatocumulative`
or `cumulativetodelta` processor** — whatever temporality we emit reaches Google Cloud
Metrics unconverted. Since both readers hang off one `MeterProvider` and the Prometheus
reader is cumulative by construction, a delta OTLP reader would make a single
instrument name mean two different things depending on which exit it left by. So:
cumulative everywhere, and the §7.7.3 pod-seconds counter keeps its reliance on
`increase()` tolerating a reset — it must stay monotonic for a process lifetime, and a
restart must present as a reset rather than be smoothed away.

**The collector is one pod, and it recycles connections on purpose (S5).** The endpoint
is `opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317`, a ClusterIP in
front of a **single-replica Deployment** — not a node-local DaemonSet. Its receivers
carry no TLS or auth (the Google credentials sit in the collector's export-side
`googleclientauth` extension), so lookout connects with `WithInsecure()` and holds no
credential for the metrics path at all. Its gRPC server sets `max_connection_age: 10m`
with a 1 m grace to rebalance load, so a `GOAWAY` every ten minutes is routine: the
exporter must reconnect quietly and the export-failure SLI must not count it.

**Push makes cardinality worse, not better.** Under pull, a series that stops being
emitted goes stale at the scraper. Under push we actively ship every active series
every interval, and there is no OTLP equivalent of "don't scrape that endpoint". The
gating below matters *more* with OTLP on.

**A dead collector must not kill the process.** Same hazard class as the informer
ring buffer in §6.6.2: an exporter with an unbounded queue and an unreachable
endpoint is an OOM. Bounded queue, drop on full, never block the recording path.
Export failures and dropped points are the SLIs — and an alerting pipeline should not
depend solely on OTLP unless the backend is at least as reliable as the thing being
monitored.

**Cardinality control.** `domain_objects` is subjects × topology keys × domains ×
states — at 20k subjects, 2 keys, 3 domains, 4 states that is 480k series, far too
many. Mitigations, all configurable: emit per-domain series only for subjects
currently non-OK or above a `ρ` floor (default 0.05); collapse `state` to
`active`/`pending` by default; cap tracked topology keys per subject; namespace
allow/deny list. Aggregate per-subject scalars (`drift`, `max_domain_share`) are
always emitted — ~40k series, which is fine. `internal/watch/metrics.go`'s
`reasonLabelCap` is the existing precedent for this kind of bound and the same
approach should be reused rather than reinvented.

> **The naming pass landed 2026-09-17**, with the instruments declared in
> `pkg/sources/topologydrift/metrics.go` against the OTEL API and nothing
> Prometheus-native anywhere in the source. Two things to record.
>
> **The derivation table above reproduces exactly, on `otelprom` v0.68.0** —
> two minor versions past the v0.66.0 S11 measured. `lookout.leeway.evaluation_duration`
> with unit `s` exports as `lookout_leeway_evaluation_duration_seconds`, and
> `lookout.leeway.last_event_timestamp` + `s` as
> `..._last_event_timestamp_seconds`. Both names are what we want *because* the
> instrument is named without the unit; writing `_seconds` into the declaration
> would have produced `_seconds_seconds`. `TestInstrumentNames_PrometheusSpelling`
> gathers from a real registry and compares the exported name set, so this stops
> being a claim in a document and starts being something a dependency bump
> breaks loudly.
>
> **Phase 2 ships the cardinality gate as a boolean, not as the `ρ` floor.**
> `perDomainSeriesMinDrift` needs a drift figure, and there is none until Phase
> 3 infers intent. So `domain_objects` is behind an explicit off-by-default
> opt-in and everything else is the cheap aggregate set. The default posture is
> the one the paragraph below argues for; only the shape of the knob is
> temporary.

> **`intent_info` landed 2026-09-18**, as the first metric Phase 3 fills in, and
> its label set is one wider than the line above:
>
> ```
> lookout_leeway_intent_info {namespace,subject,subject_kind,topology_key,
>                             mode,source,confidence,weighting,max_skew}
> ```
>
> **`mode` is the added label** and it is not decoration: §5.1 precedence can
> resolve one axis to an *enforced* intent and another on the same workload to a
> *preference*, and a dashboard that cannot tell them apart will read a preferred
> anti-affinity's soft nudge as a hard guarantee the scheduler broke. The other
> eight are as drafted.
>
> **It is ungated, unlike `domain_objects`.** The cardinality argument does not
> transfer: the series is emitted only for subjects that actually expressed an
> intent, and only on the axes they expressed it on — no domain, no state, and no
> row at all for the large majority of workloads that declare nothing. That is
> bounded by *declared* constraints rather than by cluster shape, which is the
> distinction the gate exists to draw.
>
> **`max_skew` carries a ceiling that is not a skew.** A required
> `podAntiAffinity` is one-per-domain, which §5 models as `MaxPerDomain` and not
> as `MaxSkew: 1` — the two are different bounds and conflating them would
> understate what the workload asked for. The label reads `max-per-domain=1` in
> that case and `none` where neither is set, rather than inventing a number.

**S5 turned the ceiling into a bill, which makes the gating more important, not
less.** The estate runs Google Managed Prometheus, which does not reject a
high-cardinality target — it charges per sample ingested. There is no scrape that
fails and no alert that fires; a default-on subsystem simply grows a line item. At a
30 s scrape each series is 86,400 samples/month, so the 480k worst case above is ~41
billion samples/month against ~3.5 billion for the aggregate-only path. That 12× is
the reason `perDomainSeriesMinDrift: 0.05` is a **default**, not a knob large estates
are expected to find: the cheap posture must be what you get for free, and a
deployment that wants per-domain series across the board has to ask. Push makes this
bind harder still, per the paragraph above.

> **The score metrics and the real gate landed 2026-09-19**, replacing the Phase
> 2 boolean. Three things to record.
>
> **The gate is `PerDomainGate{All bool; MinDrift float64}`,** not one number.
> Overloading a sentinel — a negative floor meaning "everything" — would have
> made `--topology-per-domain-series` and the floor the same knob, and they are
> different statements: one is "export the breakdown for whoever is drifting",
> the other is "export it for everyone, I have budgeted for it". `Admits(drift,
> scored)` is `All || (scored && drift >= MinDrift)`, and **an unscored subject
> is never admitted by the floor**: a zero floor means "every *measured*
> subject", and reading it as "everything" would put the whole estate's
> breakdown on the wire the moment somebody set it to zero meaning the former.
> `domain_expected` rides the same gate as `domain_objects`, because the two are
> a numerator and a denominator.
>
> **Gating is per subject, not per axis.** `State.DriftOf` returns the maximum
> drift across a subject's axes. A subject drifting on zone is one somebody will
> investigate, and serving them the region breakdown while withholding the zone
> one would be exactly the wrong half.
>
> **The subject-level score series are narrower than the table above**, and
> deliberately so: they are exported only for axes where `Scores.Evaluable`. A
> gated subject — below `MinReplicasForScoring`, no eligible domains, or
> `ModeIgnore` — has no drift figure, and the zero is not one. Publishing it
> would be five series per axis asserting a perfectly balanced workload for
> every single-replica Deployment, which at §6.6's baseline row is the majority
> of the cardinality and is also simply a false statement. A reader asking why a
> tracked subject has no `drift` series finds it in `subjects_tracked` and
> absent here, which is the same answer.

> **The remaining three mitigations landed 2026-09-22** (#475), completing the
> paragraph above. Each has a flag, each is independently reversible, and with
> all three off the export is byte-for-byte what it was the day before. Four
> things to record, two of which are departures from the draft.
>
> | Control | Flag | Default | Effect at §6.6's baseline row |
> |---|---|---|---|
> | ρ floor / non-OK | `--topology-per-domain-min-drift` | 0.05 | admits a handful, not 20k |
> | Collapse `state` | `--topology-per-domain-collapse-states` | on | 24 → 12 series per subject |
> | Cap axes | `--topology-per-domain-max-keys` | 4 | no effect at 2 axes; 6 axes → 4 |
> | Namespace lists | `--topology-per-domain-namespaces`, `…-exclude-namespaces` | empty | whatever you ask for, incl. 0 |
>
> The numbers are not an estimate: `TestSource_PerDomainSeriesAtTheScaleTier`
> measures series per subject on a worst-case miniature estate and multiplies
> out, so the budget is an assertion and a future label lands as a red test.
>
> **The collapsed labels are `active`/`waiting`, not `active`/`pending`** as
> drafted. `pending` is also one of the four uncollapsed `CountState` spellings,
> so the two label sets would have overlapped on exactly one value — and a
> dashboard written against four states and pointed at a collapsed exporter
> would then have rendered `state="pending"` as a plausible number that was
> actually pending-plus-unschedulable, while silently losing running and
> terminating. Disjoint sets make the same mistake return no data, which is a
> question somebody asks rather than a number they believe. (`Terminating`
> collapses to `active`: the pod is still holding the domain's capacity, and a
> zone draining is not a zone that has drained.)
>
> **What the caps drop is counted, rather than folded into an `other` bucket.**
> `reasonLabelCap` is the cited precedent and it folds, which is right there —
> it caps *event reasons*, and the overflow is a count of events that can be
> summed into a residual. It is wrong here: the overflow is a topology *axis*,
> and the same pods are counted on every axis, so an `other` row summing the
> capped axes' `domain_objects` would report the same object two or three times
> under a label that looks like a domain. Instead there is one new gauge:
>
> ```
> lookout.leeway.domain_series_withheld {reason}   # gate | namespace | key_cap
> ```
>
> counted in **subjects** for all three reasons, so they are summable, and
> present at zero from the first scrape so "nothing was dropped" is
> distinguishable from "nothing is looking". A subject can be admitted and still
> counted under `key_cap` — that reason means a breakdown was narrowed, not
> withheld.
>
> **The cap takes a prefix of `--topology-keys`, not of the drift ranking.**
> Keeping the top-drifting axes would sound better and would be unusable: an
> axis would enter and leave `/metrics` as placement moved, breaking every
> `rate()` over it. The operator already stated which axes matter most, in the
> order they wrote the flag.
>
> **The decision is made once per scrape** (`seriesPlan`), because
> `domain_objects` and `domain_expected` are filled from two different walks —
> distributions and evaluations — and are read against each other. Deciding
> independently in each walk would eventually have them disagree about which
> axes exist, which renders as a dashboard dividing by nothing.

### 8.5 Finding payload

```json
{
  "kind": "leeway.placement_drift",
  "subject":     { "kind": "Deployment", "namespace": "payments", "name": "api" },
  "topologyKey": "topology.kubernetes.io/zone",
  "tier": "B", "severity": "warning",
  "score": { "drift": 0.42, "observedSkew": 7, "minAchievableSkew": 1,
             "excessSkew": 6, "relocationDistance": 5, "maxDomainShare": 0.67 },
  "intent": {
    "source": "PodAntiAffinityPreferred",
    "confidence": "Inferred",
    "weighting": "AllocatableCPU",
    "evidence": [
      "preferredDuringScheduling podAntiAffinity on topology.kubernetes.io/zone (weight 100)",
      "no topologySpreadConstraints declared",
      "eligible domains restricted to [us-east-1a,us-east-1b,us-east-1c] by nodeSelector workload=general"
    ]
  },
  "domains": [
    { "domain": "us-east-1a", "actual": 8, "expected": 4, "delta": 4,  "readyNodes": 12 },
    { "domain": "us-east-1b", "actual": 3, "expected": 4, "delta": -1, "readyNodes": 11 },
    { "domain": "us-east-1c", "actual": 1, "expected": 4, "delta": -3, "readyNodes": 3,
      "note": "9 nodes lost in last 2h" }
  ],
  "suspectedCause": "domain_capacity_shortfall",
  "contributingFactors": [
    "us-east-1c ready node count fell 12 → 3 at 2026-09-10T09:14:22Z",
    "2 pods Pending with FailedScheduling: 0/26 nodes available: 9 Insufficient cpu",
    "0 pods pinned by zonal volumes"
  ],
  "firstSeenAt": "2026-09-10T09:31:10Z"
}
```

> **Shipped 2026-09-19 as `leeway.NewFinding`.** The example above is a test,
> asserted byte for byte, with three deliberate departures.
>
> **`kind` is not a constant.** The example shows `leeway.placement_drift`
> because the example is Tier B; §2.3 gives Tier A `leeway.contract_violated` and
> Tier C `leeway.baseline_breach`. `FindingKind` lets the **tier lead**, so kind and
> tier can never disagree — one combination is otherwise structurally reachable,
> because a learned-baseline intent is not an assumed cluster default, so it
> satisfies `declaredContract` and could in principle carry a `MaxSkew` and fire a
> contract rule. §8.1 has already decided that case is Tier C, and the kind has to
> follow rather than claim a contract nobody declared. Within Tier C the intent
> source decides, because the two Tier C cases measured different things: a learned
> baseline is a deviation from the subject's own history, while a subject with *no*
> intent was scored against an apportioned expectation, which is placement drift —
> less confident than Tier B's, the same observation. `leeway.domain_unavailable`
> is reachable from no verdict at all and the source raises it directly.
>
> **`source` and `confidence` ship kebab-case** — `pod-anti-affinity-preferred`,
> `inferred` — not as the Go identifiers written above. Those spellings are already
> the §8.4 `intent_info` labels, and one fact should not have two names.
> (`weighting` is `AllocatableCPU` in both, which is inconsistent with its two
> neighbours and is left alone: it is a shipped label.)
>
> **`score` is a projection, not `Scores`.** χ², concentration and the gate reason
> stay in metrics and the debugger. Freezing the payload against the internal
> struct would make every future scoring field a wire change.
>
> Two fields the draft did not have: `transient` and `relaxed`, both omitted when
> empty. A subject that breached *through* a rollout is a stronger finding than one
> that breached on a quiet cluster, and nothing else in the payload says which
> happened. `firstSeenAt` is read from the §8.2 dwell state and normalised to UTC,
> so a re-emitted finding still reports when the *episode* started and two
> sentinels cannot disagree about when that was.
>
> The payload is built whether or not it will be emitted; `Route` decides that
> separately, so a suppressed subject can still be rendered for a dry run.

Cause attribution is a small rules engine over evidence we already hold. These are
`suspectedCause` values, a separate namespace from the §2.3 kinds — which is why
`domain_outage` survives here while the kind it once named became
`leeway.domain_unavailable`. The distinction is the point: the kind states what we
measured (a domain has no eligible nodes), the cause states what we think produced it
(the nodes went `NotReady` together), and only the first is an observation.

| Suspected cause | Signals |
|---|---|
| `domain_capacity_shortfall` | Ready-node drop in under-filled domain; `FailedScheduling` citing insufficient resources |
| `domain_outage` | Sharp ready-node drop; `NotReady` nodes in one domain |
| `volume_pinning` | High `Pinned` count in over-filled domain; StatefulSet with zonal PVs |
| `constraint_ignored` | `whenUnsatisfiable: ScheduleAnyway` with skew above `maxSkew` |
| `taint_exclusion` | Domain became ineligible via new taint within the drift window |
| `consolidation` | Karpenter/CA consolidation events correlated with node removal in under-filled domain |
| `rollout_bias` | Drift appeared during rollout and did not recover after settle |
| `unknown` | Fallback — still reports the numbers |

Several of these are already available from sibling sources — `capacity` owns CA
stockout and pending-pod aging, `objectstate` owns node Ready transitions. Attribution
should consume those signals rather than re-derive them; that is what one shared
pipeline is for.

> **Shipped 2026-09-19 as `leeway.Scores.Attribute`.** The sibling-source rule is
> enforced by the input type: `Evidence` is a plain struct of other people's
> observations, so the rules engine has no way to re-derive anything and the whole
> of it runs without a cluster, a clock or an informer.
>
> **The table's rows are not mutually exclusive, so the order matters, and the
> worked example above is what fixes it.** A domain falling 12 → 3 with nothing
> `NotReady` is attributed there to `domain_capacity_shortfall`, not
> `domain_outage` — so the line between those two is **presence, not magnitude**:
> the nine nodes did not break, they left. Read that way the outage row's two
> signals are one condition seen twice, because when nine of twelve nodes go
> `NotReady` the ready count falls sharply *and* the `NotReady` census passes half.
> Testing the census is the form that also answers when there is no history to have
> watched the fall, the process having started after the zone was already down.
>
> "At least half" is §7.6's own line, reused rather than re-tuned. A finding that
> suppressed at one threshold and attributed at another would be incoherent, and
> the two are not in conflict: §7.6's suppression is windowed, and when it lapses
> with the domain still down, the finding fires and this is why.
>
> The ladder is therefore the under-filled side first, **most specific mechanism
> downwards, with `domain_capacity_shortfall` as their catch-all**; then the
> over-filled side (`volume_pinning`); then history (`rollout_bias`); then policy.
> Consolidation has to outrank the shortfall it is indistinguishable from —
> both show up as a domain that lost ready nodes, and "the autoscaler removed it"
> is a different remedy from "there aren't enough" even though the number that
> moved is the same. `constraint_ignored` comes last for the mirror reason: it says
> nothing malfunctioned and you permitted this, which is only the answer once no
> mechanism is available to be the answer instead.
>
> Every rule here but pinning asks about an *under-filled* domain. Drift has two
> ends and the causes are not interchangeable between them — nodes failing in the
> domain holding the surplus would push objects away, not draw them in.
>
> **The cause is singular; the evidence is not.** `contributingFactors` is built in
> a fixed order independent of which rule won, so two findings on one subject an
> hour apart diff cleanly, and the losing hypotheses still get a line. That is also
> why the worked example carries "0 pods pinned by zonal volumes" in a finding that
> is not about pinning: a ruled-out hypothesis is what turns "we think it was
> capacity" into "we think it was capacity, and here is what it wasn't". Per-domain
> fall lines are capped at three — a forty-zone cluster losing nodes everywhere is
> one story, not forty — while every domain still gets its own `note`.

> **Wired 2026-09-20.** The source assembles `Evidence` from what it already
> holds — the inventory's per-domain census, its ready-count series and the
> subject's own distribution — and answers `zonalVolumes` locally, since
> "bound to a zonal volume" is exactly the pin predicate. Four fields stay at
> their zero values because each is a whole sibling source and Phase 8 owns
> consuming them: `insufficientResource` and `schedulingMessage` are
> `capacity`'s judgement about Pending pods, `consolidatedAt` is the
> autoscaler's, and `rolloutEndedAt` needs a rollout's *completion* time where
> §7.6's seam reports only what is rolling out now. The cost is bounded and
> known: the shortfall row loses its corroborating pod evidence, and
> `consolidation` and `rollout_bias` cannot win at all — so attribution falls
> through to a cause below them on the ladder, never to a wrong one, because
> every rule needs positive evidence.
>
> Reading the peak and the fall out of the ready series made the retention the
> longer of §7.6's window and this one: **one series, two readers.** Both scan
> their own window over identical samples, so they cannot disagree about when a
> domain lost nodes. The fall is the *last* decrease, not the first — a zone that
> fell 12 → 3 an hour ago and 3 → 1 a minute ago lost nodes a minute ago, and the
> older step is still in the peak, which is the number the finding compares
> against.
>
> The finding rides `inject.Payload` like every other source-namespaced kind, and
> `message` is a rendering of the document above rather than the document itself.
> Adding a fifth wire struct to signal-schema v1 would freeze the whole finding
> shape against fleet consumers on a payload days old; `cmd/leeway` and the store
> keep the struct, and that the two agree is what §8.5 is for.

---

## 9. Restart resilience

### 9.1 What is and is not rebuildable

| State | Source of truth | Persist? |
|---|---|---|
| Pod/node placement, distributions, inventory | API server | No — rebuilt from informer sync |
| Resolved intent | Object specs | No — recomputed |
| Alert state, `firstSeenAt`, `since`, flap counters | Us | **Yes** |
| Learned baselines (EWMA/EWMAD) | Us | **Yes** |
| Drift episode history | Us | **Yes** |

Because current state is rebuildable, the durability problem is small and bounded —
a few hundred KB to a few MB. That is deliberate: persisting the object cache would
buy faster startup at the cost of a whole class of cache-coherence bugs.

The requirement "withstand process restarts" mostly means: **a 30-minute dwell timer
must not restart its clock because a pod was rescheduled at minute 29.**

### 9.2 Storage

Use `pkg/store`. It already provides findings, history, prune and query, and lookout
DESIGN §9.1 already establishes sentinel-local raw occurrence storage. Leeway adds
two record types — `AlertState` and `Baseline` — with different write policies:

- **Alert-state transitions: synchronous and durable.** Losing them means losing
  dwell timers, the one thing we promised to keep.
- **Baselines: batched every 30 s.** Losing 30 s of EWMA is irrelevant.

If the store is unavailable at startup, start degraded: Tier A and B proceed with
fresh dwell timers, Tier C is disabled until baselines reload or relearn. Never
refuse to start — a missing history file should not mean no monitoring.

> **The baseline half shipped 2026-09-20 as store migration v8**, one table
> (`leeway_baseline`) at v7's grain — one row per (cluster, subject, topology
> key) — and the opposite write policy: `PutLeewayBaselines` takes a batch and
> writes it in **one** transaction. At fleet scale that is the difference
> between twenty thousand transactions per flush and one, and it buys nothing
> the alert path needs, because thirty seconds lost from a twelve-hour
> half-life is not a lost promise.
>
> **The batch is all-or-nothing, and the reason is `updated_at`.** Since a
> subject's whole domain set lives in one row, a partial flush cannot corrupt a
> distribution — it can only leave some subjects on this interval's estimate
> and some on the last one. But `updated_at` is precisely what step 7 below
> reads as downtime, so a torn flush is a row lying about how old it is, which
> is the one thing that decides whether its bands are widened or discarded.
>
> **Three fields do not cross.** `Frozen` is re-derived, because a persisted
> freeze can outlive the finding that justified it and quietly stop a subject
> learning for good. `WidenBy` and `WidenUntil` are *produced* by loading
> rather than carried through it — step 7 decides them from the gap it
> measures, and a widening persisted from a previous outage would be applied
> against the wrong clock. `DevWeight`, by contrast, **must** persist: §7.5's
> warm-up correction divides by it, and a restart that dropped it would divide
> a converged deviation by a freshly-zeroed weight.
>
> **A damaged row is refused whole, not half-believed.** A share outside
> [0, 1], a negative deviation, a duplicate domain, a missing `updated_at`:
> each reads back as "nothing learned", which is a state §7.5 already handles,
> because an absent baseline is an immature one. Two things are repaired
> instead, both losing information in the safe direction — a scrambled domain
> order is re-canonicalised, since the order *is* the invalidation fingerprint
> and loading it as written would reset a perfectly good baseline on the next
> sample; and a missing `FirstSeen` is treated as now, which costs six hours of
> relearning rather than making the set mature the instant it loaded. A
> negative sample count gets the same treatment at the SQL boundary: it would
> wrap to an enormous `uint64` and satisfy the maturity gate it exists to hold
> shut.
>
> **This table needs a prune where `leeway_alert_state` does not.** An alert
> row is deleted when its episode resolves, so that table is bounded by how
> many subjects are drifting right now. A baseline row exists for every subject
> ever sampled, and a namespace torn down between restarts leaves no event to
> act on. Discarding a row older than step 7's stale window costs nothing:
> loading it restarts the maturity clock anyway.

The standalone design also specced a PostgreSQL backend and CRD-status writes. Both
are dropped: backend choice is `pkg/store`'s decision, not leeway's, and lookout is
deliberately not a controller with a status-writing CRD surface.

### 9.3 Startup sequence

Leeway hooks the sentinel's existing arming flip (`sources.SyncReporter`) rather than
inventing a warmup window:

```
1. Load alert states and baselines from pkg/store.        (fast, local)
2. Wait for the shared informer caches to sync (sentinel-owned).
3. Compute and export metrics; do NOT fire, resolve, or update baselines.
4. Full reconcile: evaluate every subject once.
5. Settle window (default 60 s after sync) — lets the resync burst land.
6. Arm; reconcile persisted alert state against fresh scores:
     • persisted Firing + still drifting  → stay Firing, preserve `since`
     • persisted Firing + now OK          → enter Resolving with the FULL
                                            resolveDuration (do not resolve
                                            instantly; the cluster may look fine
                                            only because we just started)
     • persisted Pending + still drifting → stay Pending, preserve `firstSeenAt`;
                                            may fire immediately if the dwell
                                            elapsed during downtime
     • no persisted state + drifting      → new Pending, `firstSeenAt` = now
7. Assess downtime since the last write:
     • ≤ 2 h            → resume baselines as-is
     • > 2 h, ≤ 24 h    → resume, widen bands ×1.5 for one half-life
     • > 24 h / unclean → mark stale; Tier C suppressed until re-matured
8. Normal operation.
```

> **Shipped 2026-09-19 as store migration v7 + `leeway.Reconcile`.** Steps 1 and
> 6 for alert state; steps 3, 5 and 7 belong to the source and the baselines,
> which arrive with the pipeline and Phase 5 respectively. **Steps 3, 5 and 7
> shipped 2026-09-20** with migration v8 and `leeway_baseline` — the baselines
> load at startup, the sweep runs on its own timer, and the three downtime
> bands above are the restore path's own test.
>
> **Three of step 6's four rows are already what `Advance` does.** A persisted
> Firing that is still drifting stays Firing with its `since`; a persisted
> Pending whose dwell elapsed during the downtime fires on the spot; an absent
> record becomes a new Pending. None of them depends on having watched the
> interval, so the machine needs no telling that a restart happened.
>
> **The fourth does, and the list is missing its twin.** §9.3 states the
> full-`resolveDuration` rule for a persisted *Firing* that now looks OK, but the
> same hazard is sharper for a persisted *Resolving*: a process down two hours,
> holding a `clearSince` from before the outage, finds the window long elapsed
> and closes the finding on a single sample taken seconds after start-up. So the
> rule is generalised — **any subject arriving in Resolving restarts its window
> from now**, and `since` is preserved through it.
>
> The asymmetry with the fire side is deliberate, and is the one
> `resolveDuration = 3× forDuration` already encodes. Firing on a dwell that
> elapsed while we were blind at worst reports drift we *did* watch for the full
> dwell and which is still true; resolving on a stale clear silently drops a
> finding nobody was told was going away.
>
> **A decoded record gets repaired before it is run.** Everything repaired is a
> state `Advance` cannot have written, so reaching it means the row is damaged,
> newer than this build, or was read across a clock that moved. A phase with no
> transitions is forgotten outright rather than guessed at; `time.Time{}` is the
> year 1 rather than a neutral default, so a truncated Pending would fire on the
> spot; and a *future* timestamp — a VM restore, an NTP step — is clamped to now,
> because left alone it makes `now.Sub` negative and wedges the subject in
> Pending, silently, for as long as the skew lasts. Throughout: prefer the
> reading that loses information over the one that acts on a lie. A forgotten
> episode costs one dwell; a phantom one pages somebody.
>
> **Table grain and write policy.** One row per (cluster, subject, topology
> key) — a workload can be evenly spread across zones while stacked three-deep
> on one node, and those are two independent episodes. Writes are synchronous
> and bypass the buffered writer that occurrences use, per §9.2: an occurrence
> lost to a buffer is a missing history line, a dwell transition lost to a buffer
> is the one durability promise §9.1 makes. Resolved episodes are **deleted**,
> which keeps the table bounded by "how many subjects are drifting right now"
> rather than by how many subjects exist. A store predating v7 reads as "no
> state" and starts degraded with fresh dwells — §9.2's "never refuse to start" —
> while a *write* to one is refused, because a write that vanishes is exactly
> what persistence exists to prevent.

Step 6's asymmetry is the important part: honour elapsed dwell in the *firing*
direction (a problem that persisted through a restart is more real, not less) but not
in the *resolving* direction (we have no evidence about what happened while we were
down). Eager to fire, reluctant to resolve is the right bias for a detector.

---

## 10. Configuration and RBAC

### 10.1 Policy CRD

`ClusterLeewayPolicy` (cluster-scoped) and `LeewayPolicy` (namespaced) share a
schema. **Optional** — inference works with no CRD installed, and lookout is not
otherwise a CRD-installing product, so this must degrade to "absent" cleanly rather
than being a deployment prerequisite.

```yaml
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: LeewayPolicy
metadata: { name: payments-api, namespace: payments }
spec:
  selector: { matchLabels: { app: api } }
  subjectKinds: [Deployment]
  topologyKeys:
    - key: topology.kubernetes.io/zone
      mode: Spread                # Spread | Colocate | Ignore
      weighting: AllocatableCPU   # Equal | NodeCount | AllocatableCPU | AllocatableMemory
      expectedDistribution:       # explicit intent; omit to use inference
        us-east-1a: 40
        us-east-1b: 40
        us-east-1c: 20
      thresholds:
        drift: 0.2
        maxDomainShare: 0.5
        for: 10m
        resolveAfter: 30m
        severity: warning
  inference:
    enabled: true
    sources: [TopologySpreadConstraint, PodAntiAffinityRequired, PodAntiAffinityPreferred,
              PodAffinityRequired, PodAffinityPreferred]
  baseline: { enabled: true, halfLife: 12h, deviationSigmas: 4.0 }
  exclusions:
    minReplicas: 3
    ignorePinnedByVolume: true
    suppressDuringRollout: true
```

Per-workload annotation overrides remain the lightest-weight path:
`leeway.lookout.go-steer.io/max-drift: "0.2"`.

> **Shipped 2026-09-19 — and the shipped schema is a strict subset of the document
> above.** `selector`, `subjectKinds`, `topologyKeys` (`key`, `mode`, `weighting`,
> `expectedDistribution`) and `inference` (`enabled`, `sources`) are honoured.
> `thresholds`, `baseline` and `exclusions` are **not in the CRD at all**, and that
> is the point: a structural schema prunes undeclared fields, so writing one today
> makes it visibly disappear rather than leaving an operator believing a threshold
> is in force that nothing reads. They arrive with the code that honours them, which
> is Phase 4's; adding a field to a structural schema is additive and safe.
>
> Two clarifications the worked example leaves open. `expectedDistribution` values
> are **relative weights, not percentages** — they are normalised over whichever
> named domains are currently eligible, so the `40/40/20` above keeps working after
> `us-east-1c` is drained, and a domain that is eligible but unnamed is expected to
> hold nothing. And an omitted `selector` matches **everything** in scope, which is
> the opposite of how a workload's own selector reads an empty value; the selector
> is matched against the **pod's** labels, since the source resolves subjects from
> pods and never reads the owning workload object.

See the FR-10 note in §4 for the precedence, ambiguity and discovery decisions, and
`deploy/crds/leewaypolicies.yaml` for the shipped schema — the two duplicated
`openAPIV3Schema` blocks, the enums and the defaults are all held to this package's
decoder by `pkg/sources/topologydrift/crd_test.go`.

### 10.2 Source configuration

Under the sentinel's existing source config:

```yaml
sources:
  topology-drift:
    enabled: true
    topologyKeys: [topology.kubernetes.io/zone, topology.kubernetes.io/region]
    nodeGroupLabelKeys:      # ordered precedence
      - cloud.google.com/compute-class   # before gke-nodepool: NAP pools are ephemeral
      - karpenter.sh/nodepool
      - eks.amazonaws.com/nodegroup
      - cloud.google.com/gke-nodepool
      - kops.k8s.io/instancegroup
      - agentpool
    # Mirrors kube-scheduler's PodTopologySpread defaultConstraints, which S4
    # confirmed is not readable on managed GKE. Manual sync, and three-state:
    #   key absent -> UNSET. Leeway assumes the upstream system defaults and marks
    #                 every intent derived from them confidence=assumed, source=
    #                 cluster-default-assumed. Honest, and visibly a guess.
    #   []         -> DECLARED EMPTY. An operator asserts this cluster has none.
    #   populated  -> DECLARED. Trusted, source=cluster-default-declared.
    # The Go field must be a *[]Constraint, not a []Constraint: a nil slice and an
    # empty one are the same value after decoding, which collapses the first two
    # states and reintroduces exactly the silent-assumption bug S4 exists to stop.
    # Either way these only apply to Pods declaring no topologySpreadConstraints of
    # their own, and being soft they can never raise a Tier A finding (§13 S4).
    clusterDefaultConstraints:
      - { maxSkew: 3, topologyKey: topology.kubernetes.io/zone, whenUnsatisfiable: ScheduleAnyway }
    scoring:
      minReplicasForScoring: 3
      smallNThreshold: 10
      capacityWeightingRatioTrigger: 1.25
    runtime:
      coalesceWindow: 2s
      rolloutCoalesceWindow: 15s
      verifyInterval: 10m
      verifyShards: 6
      workers: 8
    metrics:
      perDomainSeriesMinDrift: 0.05
  compute-class:
    enabled: auto            # auto = enable iff the ComputeClass CRD is present
    # preferenceAxes: see §7.7.2
```

### 10.3 RBAC

Declared via `sources.AccessDeclarer` so a missing permission fails loudly at
startup instead of producing an empty watch. Most of this the sentinel already holds:

```yaml
# Already held by object-state / graph:
  - { apiGroups: [""],      resources: [pods, nodes], verbs: [get, list, watch] }
  - { apiGroups: ["apps"],  resources: [deployments, statefulsets, daemonsets, replicasets],
      verbs: [get, list, watch] }
  - { apiGroups: ["batch"], resources: [jobs, cronjobs], verbs: [get, list, watch] }

# New for leeway:
#   The volume rule SHIPPED 2026-09-19 as list+watch (no `get`: both are read
#   through informer listers, never fetched one at a time). It is in
#   deploy/12-clusterrole-watcher.yaml and the chart copy that CI diffs
#   against it.
  - { apiGroups: [""], resources: [persistentvolumeclaims, persistentvolumes],
      verbs: [list, watch] }             # volume pinning, FR-8
  - { apiGroups: ["cloud.google.com"], resources: [computeclasses],
      verbs: [get, list, watch] }        # optional; source self-disables if absent
#   The policy rule SHIPPED 2026-09-19 as list+watch (again no `get`: read
#   through a dynamic informer). It is in the shipped ClusterRole and the chart,
#   but deliberately NOT in the source's RequiredAccess() — the sentence at the
#   top of this section is the reason to be careful here. §11's check fails
#   startup on a missing declared grant, and the source does its whole job with
#   no policy at all, so declaring it would turn an optional override into a
#   cluster-wide prerequisite. A missing grant is handled where it happens
#   instead: the discovery gate logs one line and continues.
  - { apiGroups: ["leeway.lookout.go-steer.io"],
      resources: [leewaypolicies, clusterleewaypolicies],
      verbs: [list, watch] }             # optional; FR-10
```

No write access to any workload or node.

---

## 11. Edge cases

| Case | Handling |
|---|---|
| Replicas not divisible by domains | Min achievable skew (§7.3); never report below it |
| Domain with zero eligible nodes | Excluded from expectation unless `minDomains` requires it |
| Nodes missing the topology label | Bucketed as `__unknown__`, surfaced as a distinct low-severity finding — a mislabelled node is itself a topology defect |
| DaemonSets | Expectation is one pod per eligible node; drift = nodes missing a pod, not count imbalance |
| StatefulSet with zonal PVs | Pods marked `Pinned`; excluded from actionable drift, and `leeway.placement_drift` carries `suspectedCause: volume_pinning` |
| Multiple ReplicaSets during rollout | Aggregated at Deployment level; per-RS available for debugging |
| Job/CronJob pods | Short-lived; scored only if `parallelism ≥ minReplicas` and lifetime > `for` duration |
| Single-replica workloads | Distribution recorded, drift not evaluated |
| `whenUnsatisfiable: ScheduleAnyway` | Tier B, not A — the user asked for best-effort. Still worth reporting, since "silently ignored" is the whole failure mode |
| Custom controller owners (Argo Rollouts, etc.) | Generic owner-chain walk to the topmost owner; falls back to pod labels plus a template hash |
| Pod on a deleted node | Placement retained until the pod is deleted; counted `Terminating` once `deletionTimestamp` is set |
| Terminating pods | Counted separately; excluded from `n` for scoring |
| Zone added to the cluster | Eligible set changes → affected baselines reset; a settle window prevents an instant finding on every workload |
| Clock skew / suspend | Dwell uses monotonic deltas in-process, wall-clock only across restarts |
| Very large fanout (zone outage) | Domain-outage detection (§7.6) suppresses per-workload findings and raises one domain-level finding; storm correlation handles cross-source fan-in |
| Store write failure | Alert on the store's existing write-drop metric; continue evaluating in memory; degrade Tier C first |
| Compute-class taint narrows eligibility | Class nodes carry `cloud.google.com/compute-class=<name>:NoSchedule` and GKE auto-injects the matching toleration. Class-pinned workloads are eligible only in zones where *that class* has nodes — often one zone. Handled by FR-7; see §7.7.6, which is otherwise a guaranteed false positive |
| NAP-created pools churn | With `nodePoolAutoCreation`, pool names are generated per machine type (`nap-n4-highmem-2-zsqafil2`), numerous and short-lived. Node-group subjects keyed on `gke-nodepool` would be high-cardinality and ephemeral, so `compute-class` takes precedence when present |

---

## 12. Testing

Following lookout's conventions (DESIGN §13): presubmits are hermetic.

- **Unit (`pkg/leeway`)** — apportionment under caps as a property test
  (`Σe_i == n`, caps respected, monotonic in weights); scoring against hand-computed
  tables; intent precedence; eligibility over taint/toleration/affinity matrices.
  All of this runs with no cluster and no client-go, which is the payoff for the
  §2.4 discipline.
- **Delta correctness (property-based)** — generate random event sequences
  (add/update/delete/relabel/node-churn), apply incrementally, assert equality with a
  full rebuild. The single highest-value test in the suite; the §6.5 verifier is the
  same invariant enforced in production.
- **Transform contract** — assert that the shared `trimPod` preserves every field
  listed in the §6.1 registry, and that adding a field to the strip list without a
  registry entry fails. This is the test that would have caught the `pkg/graph`
  breakage before it shipped.
- **Integration (`envtest` / `e2e-kind`)** — real API server, synthetic workloads and
  nodes; assert metrics and state transitions. `e2e-kind.yml` already exists.
- **Restart tests** — kill and restart mid-dwell; assert `firstSeenAt` preserved, no
  duplicate notification, no storm, correct resolve behaviour for the four §9.3
  step-6 cases.
- **Rank resolution** — table-driven over the §7.7.2 precedence: annotation present,
  absent (matcher fallback), disagreeing (ground truth wins, counter increments),
  overlapping rules, no match, sparse rules, unsupported fields, machine-family
  derivation, and the two sentinels `ccc_no_rule_matching` / `ccc_scale_up_anyway`.
  Seed fixtures from the real `ComputeClass` specs and node dumps — the risk here is
  that our platform assumptions are wrong, and synthetic cases cannot falsify them.
  S1's capture is the seed: node dumps at every state plus a 1,537-row timeline of one
  class through fallback, migration-back, rule invalidation and spot preemption.
  **One case must be a replay of that timeline**, because the ordering — class label
  before rank annotation, rank-0 node up before rank-1 node drained — is what the
  naive implementations get wrong, and no static fixture encodes ordering.
- **Scored classes** — a separate table, because S2 showed index and rank diverge only
  here and a fixture corpus drawn from real clusters contains no scored class at all.
  Required cases: the S2 arrangement itself (lowest score at index 0 — assert the
  rank-0 node resolves from annotation `"2"`); a tied score group (assert index 1 → 2
  is **not** a rank change and emits no transition); an all-unscored class (assert
  `Rank == Index`); and a mixed class (assert `OrderingInvalid`, even though GKE
  rejects it at admission, because this path exists precisely for objects we did not
  admit).
- **Reservation identity** — two nodes carrying the same
  `cloud.google.com/reservation-name` under different `reservation-project` values,
  matched against a rule naming one of the projects: assert exactly one matches. A
  name-only matcher passes every other test in this file and fails this one, which is
  the whole reason S3's arity finding is written down rather than absorbed silently.
- **Eligibility under compute classes** — assert a class-pinned workload whose class
  exists in one zone reports *zero* drift, not maximal skew (§7.7.6). This belongs in
  the false-positive corpus; it is the failure mode most likely to discredit the tool
  on a GKE cluster.
- **Export pipeline** — a golden-file test over every instrument's derived Prometheus
  name (this is what stops the §8.4 double-suffix bug reaching a dashboard); both
  readers reporting consistent values; a black-holed OTLP endpoint yielding rising
  drop counts with flat heap and an unaffected `/metrics`.
- **False-positive corpus** — a fixture set of clusters that are *fine* (unavoidable
  skew, restricted eligibility, pinned volumes below threshold, mid-rollout, small-n,
  capped domains). Reporting on any of them is a test failure. Grows with every false
  positive found in production.

  *"Pinned volumes below threshold"* rather than "pinned volumes": the amendment to
  FR-8 dropped `leeway.pinned_skew` as a kind and made pinning a `suspectedCause` on
  `leeway.placement_drift`, so a pinned subject with real skew *does* report, at lower
  severity. The corpus fixture is a pinned subject whose distribution is genuinely
  sub-threshold; the case this bullet originally described — pinned and skewed, and
  suppressed for it — is recorded separately as a test asserting that it breaches
  today, so that whoever turns pinning-based suppression on moves the cluster into the
  corpus rather than deleting the assertion.

### 12.1 The kwok harness

We will not get a 5,000-node cluster to test against, and the NFRs that matter most —
marginal memory at 200k pods, throughput at 500 pods/sec — are exactly the ones that
need scale. `kwok` is how we get it. A `resume-kwok` worktree already exists in this
repo, so this is extending prior work rather than starting from zero.

**Why `kwokctl` and not bare `kwok`.** `kwokctl` stands up a cluster with a *real*
kube-apiserver, etcd, kube-scheduler and kube-controller-manager, faking only the
kubelet. That is the right fidelity: the whole cost model lives in the watch path —
apiserver encoding, wire transfer, protobuf decode, DeltaFIFO, informer indexing —
and all of it is real under kwok. The only simulated component is the container
runtime, which we do not model. Running the real scheduler is a second win: topology
spread, affinity and taint behaviour are genuine, so the false-positive corpus asserts
against actual scheduling decisions rather than our beliefs about them.

What it buys, concretely:

1. **Scale tiers (§6.6).** Apply node and pod templates in bulk. Node count is cheap
   to fake; pod count is what costs.
2. **Event-rate benchmark (§6.6.1).** kwok `Stage` CRs define lifecycle transitions
   and delays, so churn is declarative — a stage set cycling pods through
   Pending → Running → Succeeded gives a dial for events/sec independent of pod
   count. This validates the per-event cost model and finds the real saturation point.
3. **Deterministic scenario replay.** Constructing an exact cluster state — this many
   pods in this zone, this node cordoned, this rollout half-complete — is trivial
   under kwok and near-impossible on a real cluster.
4. **Compute-class simulation at scale.** Apply the `ComputeClass` CRD and real specs,
   then fake nodes carrying the class label and `ccc_priority_index` annotation. That
   exercises all of §7.7's resolution, time-weighting and cardinality behaviour at
   200k pods with no GKE and no cost. What it *cannot* test is fallback dynamics —
   kwok will faithfully serve whatever annotation we invent, which is worth nothing
   when the open question is what GKE actually does (spike S1). Simulate the
   mechanism, never the semantics.

> **The caveat that decides whether any of this is worth running: kwok's synthetic
> objects are far smaller than real ones.** Fake nodes have an empty `status.images`;
> fake pods have no realistic `managedFields`, annotations, or container env. The
> memory model is built on per-object sizes *after* trimming from ~20 KiB/pod and
> ~10–40 KiB/node — and the bytes the transforms exist to remove are exactly the bytes
> kwok does not generate. A naive run would show us passing every budget while
> validating nothing, and since decode cost scales with wire size, the CPU model would
> be understated by the same factor.
>
> **Object padding is mandatory, not optional.** Node templates carry a realistic
> `status.images` list; pod templates carry realistic annotations and env, sized from
> percentiles measured on a real cluster (spike **S8**). A check asserts the mean
> serialised object size in the fixture is within tolerance of the measured p50, so
> fixtures cannot silently deflate over time.

**Shipped 2026-09-21 with S8.** `pad_node_images`, `pad_pod_annotations` and
`pad_pod_env` in `examples/kwok/lib.sh` do the padding (on by default; `KWOK_PAD=0`
is a deliberate, loudly-announced opt-out), and `examples/kwok/verify-padding`
enforces it — `scale-up` runs it for you. It measures the **applied** objects, not
the manifests, because the largest thing a manifest cannot carry is `managedFields`,
which the apiserver writes itself and which is 6 KB of a GKE pod. Two details are
load-bearing and both cost time to find:

- `kubectl get -o json` **strips `managedFields` by default**. Without
  `--show-managed-fields` the check reads a third under the truth and a correctly
  padded fixture fails itself. With it, kubectl and the Go measurement agree to
  within 31 bytes, which is the only reason one can set the target the other asserts.
- The padding separates a **target** (the real-cluster p50, reported) from a
  **floor** (what the fixture actually reaches, enforced). The fixture lands at
  83% of p50 for pods and 82% for nodes, and the gap is structural rather than
  sloppy: kwok writes fewer `managedFields` entries, `status.conditions` is
  deliberately unpadded because kwok's node stage owns that field and objectstate's
  node detectors read it, and the node reaches its weight partly through
  `last-applied-configuration`, which real nodes do not carry. Asserting the target
  would fail forever; asserting nothing is how fixtures deflate.

One finding from calibration is worth repeating because it fails so unhelpfully: do
not pad with the beta AppArmor or alpha seccomp **pod annotations**. A kubelet that
does not honour `container.apparmor.security.beta.kubernetes.io/*` rejects the pod,
and behind a ReplicaSet that is an unbounded create-reject loop — 1,657 rejected
pods from one replica before it was noticed. `examples/kwok/lib.sh` carries a comment
saying so at the point where someone would add them back.

**Practicalities.** etcd is the binding constraint, not our process: 200k pods needs a
large machine and tuned etcd (`--quota-backend-bytes`, compaction interval). The 200k
target is directly measurable; the 1M-pod headroom tier probably is not, and should be
reported as extrapolation from measured per-object cost rather than dressed up as a
measurement. CI runs a small padded tier (500 nodes / 5k pods) on every change as a
regression gate on the per-event cost model — cheap, fast, and it catches the change
that doubles decode cost. Large tiers run nightly or on demand. Measure cgroup RSS and
`pprof` heap profiles alongside workqueue and latency metrics, and scrape the kwok
apiserver's own `apiserver_*` metrics to confirm the harness is not the bottleneck
before believing any number that comes out of it.

---

## 13. Prerequisite spikes

Decisions here rest on assumptions only observation can settle. Each spike is a
question, a method, and the decision it unblocks — no production code, roughly four
days total. Spikes S9–S11 are new, and exist only because of the fold-in.

**S9, S10 and S11 are done** (2026-09-15) — all three were desk work against this
repo and needed no cluster. Between them they changed five things: the terminal-pod
field selector is narrower, the transform's narrowing is confirmed safe, leeway owns
its own indexes because the graph runs only under `--storm`, half the §8.2 state
machine is now `pkg/engine.RecoveryTracker` rather than new code, and the OTEL
metrics migration §8.4 called a prerequisite turns out not to be one. Every one of
the three deleted or narrowed work.

**S1 and S2 are also done** (2026-09-15), both on `std-simian-test`, and they cut in
opposite directions. S1 *added* work: it confirmed the pessimistic branch of §7.7.3 and
turned up three states the data model did not have. S2 confirmed §7.7.1 exactly as
written and changed nothing structural — which matters, because between them they were
the last two spikes that could have invalidated a data model. **No spike now gates a
data model.**

**S4 and S5 closed 2026-09-16, S7 was deferred by the maintainer, and S6 closed
2026-09-17 without being run.** S4 found what it expected (the managed control plane is
unreadable) and spent its value on the mitigation instead; S5 answered all three of
its questions, and answered the hardest one by reading the managed collector's own
pipeline rather than by asking anyone. S6 is the odd one out and the most instructive:
it was closed on its *method*, not on a finding, and the note below is as much about
which spikes are worth running as about namespaces.

**S8 closed 2026-09-21, and with it the whole spike programme.** It is the only
spike that overturned a quantitative claim rather than confirming or narrowing one:
the trimmed pod is 12× the assumed size, and the assumption had been carried, in
writing, since the standalone design. It is also the only one whose output is code
that keeps running — the padding and its check — rather than an amended paragraph.
Both facts argue the same thing, which is that the spike worth running is the one
measuring a number other numbers are derived from.

| ID | Question | Unblocks | Effort |
|---|---|---|---|
| ~~S1~~ | ~~What *is* a compute-class fallback — new node or mutated node?~~ | **RESOLVED** — §7.7.2 and §7.7.3 amended; `Move` deleted | done |
| ~~S2~~ | ~~Does `ccc_priority_index` stay an array index when `priorityScore` is set?~~ | **RESOLVED** — yes; §7.7.1 unchanged, §7.7.3 gained a second fallback shape | done |
| ~~S3~~ | ~~Reservation and accelerator node label keys~~ | **RESOLVED** — accelerator verified; reservation is three labels, and identity is (project, name) | ~~0.25 d~~ |
| ~~S4~~ | ~~Can we read the scheduler's `defaultConstraints`?~~ | **RESOLVED** — no, confirmed live; FR-9 is a manual sync, and §13 S4 is the mitigation | ~~0.25 d~~ |
| ~~S5~~ | ~~Prometheus series budget and OTLP backend~~ | **RESOLVED** — GMP (cost, not a cap), the GKE managed collector, cumulative | ~~0.25 d~~ |
| ~~S6~~ | ~~Namespace count and watch-stream headroom~~ | **CLOSED without measuring** — the answer is the selector grammar, not a count; §6.7 rewritten, #431 shipped, #407 held | ~~0.25 d~~ |
| ~~S7~~ | ~~Real pod event rate per scheduled pod~~ | **DEFERRED** by the maintainer — see below | ~~0.5 d~~ |
| ~~S8~~ | ~~Real object sizes, for kwok padding and the trimmed-pod budget~~ | **RESOLVED** — §6.1 and §6.6 corrected (a trimmed pod is 18.6 KB of heap, not 1.5 KiB); padding shipped and enforced | ~~0.5 d~~ |
| ~~S9~~ | ~~Does any source need terminal pods, or fields the transform strips?~~ | **RESOLVED** — §6.1 amended | done |
| ~~S10~~ | ~~How much of §6.2 / §8.2 does `pkg/graph` + `pkg/engine` already provide?~~ | **RESOLVED** — §6.2 and §8.2 amended | done |
| ~~S11~~ | ~~Cost of migrating the sentinel's ~30 metrics to the OTEL API~~ | **RESOLVED** — §8.4 amended; migration descoped | done |

**~~S1 — Observe a fallback.~~ RESOLVED 2026-09-15.** Provoked rather than found: no
node in any inspected cluster had ever left rank 0, so the fleet sweep was skipped in
favour of building the condition. On `std-simian-test` (1.36.4-gke.1082000,
us-central1), namespace `leeway`, a `ComputeClass` named `leeway-fallback-probe` whose
rank-0 rule asks for a **large N4 spot machine** — `machineFamily: n4, spot: true,
minCores: 128`, where N4 caps at 80 vCPU in this region — and whose rank-1 rule is a
plain `machineFamily: n2`, plus one `pause` Deployment. Making rank 0 unsatisfiable by
*shape* rather than by stockout made the fallback deterministic and free: rank 0 never
provisions, so the expensive half of the experiment cost nothing.

| | Answer |
|---|---|
| (i) rank-1 node carries `ccc_priority_index: "1"` | **Yes.** First node observed off rank 0 |
| (ii) new node or mutated node | **New node, new NAP pool.** Mutation is impossible: existing pools carry another class's `NoSchedule` taint |
| (iii) direct bind or Pending | **Pending, 4 m 27 s.** Scale-up triggered at 7 s |
| (iv) `DoNotScaleUp` vs `ScaleUpAnyway` | Both leave running pods alone. `DoNotScaleUp` refuses the scale-up and stamps `ccc_no_rule_matching`; `ScaleUpAnyway` provisions off-list and stamps `ccc_scale_up_anyway` |
| (v) migration back | **Real, 4 m 17 s, and a different pod.** See §7.7.3 |

Four results beyond the question asked:

- **The annotation has a non-numeric domain** — `ccc_no_rule_matching`,
  `ccc_scale_up_anyway`, and absent-for-43 s — and it mutates in place on a live node.
  §7.7.2 now carries the full state machine. This is the largest single change S1
  caused, and no amount of reading would have produced it.
- **`RankTracker.Move` is deleted.** Pods are born and die at a rank; they never move.
- **Spot churn is a false-positive generator.** `gcloud` recorded
  `compute.instances.preempted` on a rank-0 spot node after 7 m 30 s, followed by
  `compute.instances.repair.recreateInstance` **reusing the node name**, with the
  replacement pod landing in a different zone. Node identity keyed on name is not
  stable across preemption, and a zone-skew detector watching a spot-backed tier will
  see clean zone migrations that are not drift. §7.1's normal-deviation split has to
  absorb this; it is now the second-largest false-positive class after §7.7.6.
- **`capacityCheckWaitTimeSeconds` is not usable here** — admission rejects it with
  *"only supported for Flex Start and for multi-host TPUs"* — so fallback latency is
  GKE's and cannot be tuned down for tests.

Also confirmed in passing, because §7.7.6 rests on it: GKE **auto-injects** the
`cloud.google.com/compute-class=<name>:NoSchedule` toleration into a pod whose
manifest declares none.

The class is kept as the permanent test fixture, restored to its rank-0-unsatisfiable
form so it provisions nothing while idle. Node dumps and the 1,537-row observation
timeline seed the §16 fixtures.

> **For anyone repeating this:** do not write
> `.metadata.annotations.ccc_priority_index // "0"`. That default — which an earlier
> draft of this section used — reads every unstamped and every sentinel-stamped node
> as rank 0.

**~~S2 — `priorityScore` semantics.~~ RESOLVED 2026-09-15.** No class on any inspected
cluster uses the field, so this too was provoked. On `std-simian-test`, a class
(`leeway-scored-probe`) arranged so index order and preference order cannot coincide:

```yaml
priorities:
  - machineFamily: n2   # index 0, score 10 — LEAST preferred
    priorityScore: 10
  - machineFamily: n4   # index 1, score 50 — tied most preferred
    priorityScore: 50
  - machineFamily: c3   # index 2, score 50 — tied most preferred
    priorityScore: 50
```

| | Answer |
|---|---|
| Index or rank under scoring? | **Still an index.** The winning node was `c3` — list position 2, preference tier 0 — and was stamped `ccc_priority_index: "2"`. A rank reading would have said `"0"` |
| Is scoring honoured over list order? | **Yes.** Index 0 was skipped outright; scale-up went straight to a score-50 rule |
| Partially-scored class at admission | **Rejected.** `spec: Invalid value: PriorityScore must be set for all priorities or for none of them`. `OrderingInvalid` is unreachable via the GKE API server |

So §7.7.1 needed no change. That is the useful result — it was the last spike that
could have invalidated a data model, and it did not.

Two results beyond the question asked:

- **The peer case occurred unprompted, and it is a live false-positive source.** The
  `n4` scale-up hit a genuine stockout (*"GCE out of resources"*, `us-central1-f`) and
  GKE fell through to `c3`. Index 1 → index 2, **rank unchanged**, because the author
  scored them equal. Any implementation reading the annotation as a rank reports a
  fallback on the very first workload it sees. §7.7.1 records the timeline.
- **A second, structurally invisible fallback shape.** Because the preferred rule never
  produced a node, the *same* pod waited in `Pending` and was bound once at index 2 —
  no transition, nothing ever observable at the preferred rank. §7.7.3 now separates
  this pre-emptive shape from S1's post-hoc one and marks "unmet preference" as a
  known gap belonging to a different detector on the event stream, not to §7.7.

The absent-annotation window from S1 reproduced here on an unrelated class (`<none>`
at 19:32:41, `"2"` at 19:32:53), which is the independent confirmation §7.7.2 wanted.

**S3 — Reservation and accelerator labels. RESOLVED 2026-09-16.** The two keys are
independent; the accelerator half closed on 2026-09-15 and the reservation half a day
later, once a reservation existed to consume.

**Accelerator: verified.** A class with `gpu: {type: nvidia-tesla-t4, count: 1}` on
`machineFamily: n1` provisioned `n1-standard-2` + 1×T4 (the first attempt, L4 on `g2`,
hit *"GCE out of resources"* in `us-central1-a` — L4 capacity is scarce and `n1`/T4 is
the reliable probe). The node carries:

```
cloud.google.com/gke-accelerator        = nvidia-tesla-t4   ← the extractor key
cloud.google.com/gke-gpu-driver-version = default
```

The value is the rule's `gpu.type` verbatim, so the matcher compares without
normalising. §7.7.2's `accelerator` marker is cleared.

**Reservation: verified — and it is three labels, not one.** A single-VM
`n2-standard-2` reservation in `us-central1-a` created with
`--require-specific-reservation`, consumed by a class whose only rule was
`reservations: {affinity: Specific, specific: [{name, project, zones}]}`. The node
carries:

```
cloud.google.com/reservation-name      = leeway-probe-res        ← the extractor key
cloud.google.com/reservation-project   = gke-demos-345619        ← not modelled before
cloud.google.com/reservation-affinity  = specific                ← not modelled before
```

Consumption is proven, not assumed: the reservation reported `inUseCount: 1` while the
node was up, and `--require-specific-reservation` means the node could not have been
created on-demand instead.

**The finding that changes the model is `reservation-project`.** A reservation name is
unique only within a project, and a reservation can be shared *to* other projects, so a
node consuming a reservation from a neighbouring project would carry a name that
collides with an unrelated local one. Name alone is therefore not an identity. §7.7.2's
`NodeProfile.Reservation` becomes a `ReservationRef{Name, Project, Affinity}` and the
matcher compares the pair against the rule's `reservations.specific[].{name, project}`.
On a single-project cluster the two agree and nothing changes; the point is that the
failure is silent on the clusters where they do not.

`reservation-affinity` is recorded but not matched on. It restates the rule's
`affinity` lowercased, so as a matcher input it is circular — its value is in evidence,
distinguishing a node that *had* to take this reservation (`specific`) from one that
merely landed on it (`any`).

**Two obstacles worth writing down, because both mislead.**

*GKE Warden rejects `location` combined with specific reservations* — "compute-class …
contains priorities using location config with specific reservations enabled". The zone
must be pinned through `specific[].zones` alone. Nothing in the CRD schema hints at this;
it is an admission webhook, so it surfaces as a rejected apply rather than a status.

*A reservation's default sharing policy makes it invisible to GKE.* With
`reservationSharingPolicy.serviceShareType: DISALLOW_ALL` — the default — scale-up fails
with `ReservationNotFound`: *"does not exist or not accessible"*, on a reservation that
is `READY` and in the right project and zone. The fix is
`gcloud compute reservations update <name> --zone=<zone> --reservation-sharing-policy=ALLOW_ALL`
(the enum is underscored; `allow-all` is rejected). This matters beyond the spike: **a
user reporting that leeway's reservation axis "sees nothing" may have a policy problem,
not a detector problem**, and the GKE error text points away from the cause. Note also
that the `ComputeClass` status condition does not re-evaluate on `kubectl apply` — the
class must be deleted and recreated to retest.

The in-cluster probe (class, pod, auto-provisioned pool) was torn down immediately; the
reservation is deleted separately, since it bills whether or not anything consumes it.

**Beyond the question asked:** the GPU node carries a *second* `NoSchedule` taint
(`nvidia.com/gpu=present`) and the probe pod was admitted with tolerations for both,
having declared neither. §7.7.6 now records the consequence — eligibility must be read
off the admitted Pod, because a `PodTemplateSpec` has none of the injected tolerations
and yields an eligible-node set that is too small.

**S4 — Scheduler default constraints. RESOLVED 2026-09-16: no live read, and the
mitigation is a downgrade rather than a workaround.** The target estate is all
managed GKE, so the question had a single answer to find. On `std-simian-test`
(1.36.4-gke.1082000) the control plane is not a workload: `kubectl get pods -n
kube-system -l component=kube-scheduler` returns nothing, there is no
scheduler-shaped ConfigMap in `kube-system`, and there is no API surface exposing
`KubeSchedulerConfiguration`. FR-9 is a documented manual sync. That was the expected
answer; what the spike was really for is what to do about it, because static config
that silently desyncs from the real scheduler is a correctness hazard and "write it
down and hope" is not a mitigation.

The mitigation has four parts, and the first two shrink the problem far more than the
last two solve it.

- **The blast radius is much smaller than it looks.** `defaultConstraints` apply only
  to a Pod that declares *no* `topologySpreadConstraints` of its own — a Pod with even
  one constraint ignores the cluster defaults entirely. So a wrong assumption cannot
  corrupt intent for any workload that has actually expressed intent. It can only
  affect the population where leeway's intent was weakest anyway, which is precisely
  where it should already be cautious.
- **The defaults are soft, so they are never a contract.** The upstream system default
  set is `kubernetes.io/hostname` at `maxSkew: 3` and `topology.kubernetes.io/zone` at
  `maxSkew: 5`, both `whenUnsatisfiable: ScheduleAnyway`.
  > **Corrected 2026-09-19 while implementing FR-9: the two keys were written the
  > wrong way round here.** kube-scheduler's `systemDefaultConstraints` pairs the
  > *tighter* bound with the *finer* axis — hostname 3, zone 5 — which reads
  > backwards if you expect the coarser axis to be the stricter one, and is
  > presumably how the swap got in. It matters: under *unset* these are the numbers
  > every undeclared workload in the fleet is scored against, and swapping them is
  > wrong on both axes at once with nothing else in the system to notice.
  > `SystemDefaultConstraints` in `pkg/sources/topologydrift` is now the single
  > place they are written down, and `TestSystemDefaultConstraints_MatchUpstream`
  > pins the pairing.

  Soft constraints are a
  scheduler preference that placement is free to violate, so a breach of one is not a
  breach of anything anybody promised. **An assumed cluster default is therefore
  capped at Tier B** — it can raise a statistical finding or feed a Tier C baseline,
  and it can never raise `contract_violated`. §8.1 states that as its own rule rather
  than relying on `ScheduleAnyway` to imply it, because an operator who *declares* a
  default is free to declare a `DoNotSchedule` one, and then the assertion is theirs
  and Tier A is fair. This is the part that matters: for the assumed case it converts
  a correctness hazard into a precision one.
  (Those two values are version-dependent and are stated here as the upstream default,
  not as a measurement — confirming them per GKE version is the operator's job, which
  is exactly what the next bullet is for.)
- **Three states, and "unset" is not "empty".** The FR-9 config distinguishes *unset*
  (we have never been told), *declared empty* (an operator has asserted the cluster has
  no defaults), and *declared* (an operator has supplied them). Defaulting a missing
  config to "no constraints" is the failure this spike exists to prevent: it produces
  confident findings built on an assumption nobody made. Under *unset*, leeway assumes
  the upstream set above, marks every intent derived from it `confidence: assumed`, and
  logs once at startup naming the assumption and the version it came from.
- **The assumption is visible on the wire.** §8.4's `intent_info` already carries
  `source` and `confidence`; cluster defaults split into two sources,
  `cluster-default-declared` and `cluster-default-assumed`. A dashboard can then answer
  "how much of this fleet's intent rests on a guess?" without reading the config, and
  an estate that cares can drive it to zero by declaring. Declaring is cheap; the point
  of the design is that not declaring is *honest* rather than silently wrong.

**S5 — Metrics budget. RESOLVED 2026-09-16, all three questions, and one of them
answered itself.** The estate uses Google Managed Prometheus and the GKE managed
OpenTelemetry collector, with Google Cloud Metrics behind both interfaces.

- **The Prometheus budget is a cost, not a cap.** GMP does not reject a high-cardinality
  target; it bills per sample ingested. For a default-on subsystem that is the worse
  failure mode, because nothing breaks — the bill just grows, in a line item nobody is
  watching. At a 30 s scrape one series is 86,400 samples/month, so §8.4's ungated worst
  case of 480k series is ~41 billion samples/month against ~3.5 billion for the
  aggregate-only path. The 12× is the whole decision. **`perDomainSeriesMinDrift: 0.05`
  stays, and stays on by default**; the gating is not an opt-in tuning knob for large
  estates, it is the default posture, and a deployment that wants per-domain series for
  everything has to ask for it. Ingest is via a `PodMonitoring` CR rather than a
  self-hosted Prometheus, which is a Phase 8 deployment artifact — and it must be
  optional in the chart, since `monitoring.googleapis.com` CRDs do not exist off GKE.
  The `manifests` CI stage will require `deploy/` and the Helm chart to agree on it.
- **The OTLP endpoint exists and needs no credentials from us.** Measured on the same
  cluster: `opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317` (gRPC) and
  `:4318` (HTTP). Its receivers are plaintext on the pod IP with no TLS or auth block —
  the Google credentials live in the collector's `googleclientauth` extension on the
  *export* side, so lookout ships to a cluster-local ClusterIP with `WithInsecure()` and
  no service-account key anywhere in the metrics path. Two operational details worth
  designing against rather than discovering: it is a **single-replica Deployment**, not
  a per-node DaemonSet, so it is a shared chokepoint and §8.4's bounded-queue,
  drop-on-full requirement is load-bearing rather than theoretical; and its gRPC server
  sets `max_connection_age: 10m` with a 1 m grace, deliberately recycling connections to
  rebalance, so a clean `GOAWAY` every ten minutes is normal and must not be counted as
  an export failure in the SLIs.
- **Cumulative — and the collector proved it rather than the docs.** The managed
  pipeline's `metrics/otlp` stage runs `memory_limiter`, `k8s_attributes`, two
  `resource`/`transform` passes and `batch`, then exports straight to
  `telemetry.googleapis.com:443`. There is **no `deltatocumulative` or
  `cumulativetodelta` processor anywhere in it**, so whatever temporality lookout emits
  is what lands in Cloud Metrics, unconverted. Since S11 put both readers on one
  `MeterProvider` and the Prometheus reader is cumulative by construction, choosing
  delta for the OTLP reader would make one instrument name mean two different things
  depending on which exit it left by. **Cumulative on both readers.** The consequence is
  that §8.4's "delta improves the restart story" escape hatch is closed: the §7.7.3
  pod-seconds counter keeps its reliance on `increase()` tolerating a reset, so the
  counter must be monotonic for a process lifetime and a restart must show as a reset
  rather than be papered over.

**S7 — Real event rate. DEFERRED 2026-09-16, by maintainer decision, and recorded
here so it does not read as merely open.** §6.6.1's ~8 watch events per pod lifecycle
stays a modelled number. This is a deliberate accepted risk, and a bounded one: the
multiplier scales a CPU estimate, not a data model, so being wrong costs a resized
budget rather than a rewrite — which is why it was the right one to drop. The
measurement remains worth doing before Phase 8's scale gate, where the modelled figure
is what the soak is being judged against; until then §6.6.1 should be read as an
estimate and not quoted as a measurement.

**~~S6 — Namespace count.~~ CLOSED 2026-09-17 without being run** — by maintainer
decision, on the method rather than on the finding.

The planned method was `kubectl get ns | wc -l` across the fleet plus apiserver
watcher counts. Two things were wrong with it, and the second is the one worth
carrying forward.

*The sample was never going to generalise.* The maintainer's estate is not a random
draw from the population of clusters lookout will run in, so a namespace-count
distribution measured across it would describe that estate and be quoted as though
it described the world. The corrective is a classification worth applying to every
future spike on this design:

- **Mechanical ratios** — measure them anywhere, because estate bias is
  second-order. Events per pod lifecycle (S7), the §6.1 transform's reduction ratio
  (S8): these are properties of Kubernetes and of our code, and one cluster's answer
  is approximately every cluster's answer. **S8 later tested this split and it
  held** — the transform's pod-heap ratio came out 25.7% on GKE and 25.3% on kind,
  while the absolute pod size varied 1.9× between the same two clusters. Same spike,
  same objects: the ratio generalises, the size does not.
- **Population parameters** — never sample these, *declare* them as a support
  envelope. Namespace counts, pod-spec fatness, pod distribution across namespaces:
  these are properties of who is running the software, and a sample of the clusters
  we happen to have says nothing about the clusters we do not. State what we intend
  to support and size for it.

*And the question did not need a measurement.* S6 existed to tell §6.7 whether
server-side scoping was viable. It is: exclusion is one stream at any length,
inclusion is `10M + 1`, and **neither crosses over at a namespace count**. No value
S6 could have returned would have changed a line of the design. The thing that
actually resolved it was reading the field-selector grammar and then verifying the
one asymmetry that matters — that `metadata.namespace` is rejected outright on
cluster-scoped resources — against a live API server.

Both halves are now built or filed rather than pending: `--exclude-namespace` became
a real watch scope in [#431](https://github.com/go-steer/k8s-lookout/pull/431), and
the include-list is [#407](https://github.com/go-steer/k8s-lookout/issues/407), held
until someone is blocked by its absence. §6.7 carries the reasoning; the operator
guidance is in `operations/scoping.md`.

**S7 — Real event rate.** §6.6.1 assumes ~8 watch events per pod lifecycle, a modelled
number the whole CPU budget scales off. *Method:* a throwaway watch on the busiest
cluster counting pod events over an hour against pods scheduled in the same window.
*Done when* the multiplier is measured.

**~~S8 — Real object sizes.~~ RESOLVED 2026-09-21, and it corrected the design.**
Double duty as planned: it sized the kwok padding *and* settled the trimmed-pod
budget that §6.1's narrowed transform invalidated. *Method:* `Pod` and `Node`
sampled on two live clusters — `std-simian-test` (GKE 1.36) and a kind node —
serialised before and after the real `sharedTransform`, plus the `status.images`
length distribution, plus retained heap measured directly by holding 5,000 deep
copies. The measurement is committed as `internal/watch/objectsize_test.go`,
env-gated on `LOOKOUT_MEASURE_CONTEXT` and asserting nothing, so it re-runs against
any cluster without becoming a test that fails on somebody else's hardware.

Findings, in descending order of how much they changed:

- **A trimmed pod is 18,630 B of retained heap, not ~1.5 KiB — 12× out.** §6.1 and
  §6.6 are corrected. The error's mechanism is worth more than the number: memory
  had been derived from serialised size, and a trimmed GKE pod is 1.7× larger in the
  heap than on the wire.
- **The default `memory: 256Mi` is exhausted by the pod cache at ~14k pods**, inside
  the range DESIGN §6.2 calls typical. Not leeway's bug and not leeway's to fix, but
  measured now rather than suspected — [#480](https://github.com/go-steer/k8s-lookout/issues/480).
- **The transform's *ratios* are stable across both clusters** (pod heap 25.7% /
  25.3%) while the absolute sizes vary 1.9×. That is the S6 mechanical-ratio versus
  population-parameter split showing up again, in the one spike that measured both
  kinds of quantity at once — and it is why the padding targets are quoted from GKE
  and the tolerance is a floor rather than a band.
- **Two measurement traps**, both of which produced confidently wrong numbers first:
  `kubectl get -o json` silently strips `managedFields` (34% of a pod), and a Go
  heap measurement that only takes `len()` of the slice it allocated measures
  nothing, because the compiler proves the copies dead before the GC runs. §12.1
  carries the first; the test carries a comment about the second.

**S9 — Shared-transform safety. RESOLVED (2026-09-15), with one design change.**
Four findings, all by inspection of this repo:

- **There is no transform today.** `wiring.go:963` is a bare
  `NewSharedInformerFactory(client, 0)`. Ours would be the first, process-wide.
- **Nulling `Env[].Value` is safe.** Every informer-reachable reader — `pkg/graph`
  (`derive.go:159–173`, `changes.go:272–286`), `pkg/checks/state`
  (`edges_checks.go:390–409`, `wi.go:330`) — reads only `Name` and `ValueFrom`.
  Not one reads the literal value.
- **Container statuses must stay** (`podclearance.go:349`, `rollout.go:501`).
- **The terminal-phase field selector was wrong and is now narrowed.** Excluding
  `phase=Failed` would have silently killed objectstate's eviction-burst detector
  (`objectstate.go:1220`). §6.1 now excludes `Succeeded` only.

*Remaining work* was the deliverable, not the question — and it is **done**
(2026-09-16): `internal/watch/transform_registry.go` plus the behavioural guard in
`transform_test.go`. The Phase 2 change it was blocking — attaching the transform
to the factory — landed on 2026-09-17; see §6.1.

**Implementing it found two things S9's own inspection missed**, which is the
case for the registry rather than an argument against the spike:

- **Ephemeral containers were not covered.** They share
  `EphemeralContainerCommon` with a regular container, so `kubectl debug --env`
  put literal secret values in the shared cache while every strip S9 specified
  still passed. A hole in the security goal, not a missing optimisation.
- **Two more informer-reachable readers.** `internal/watch/enrich.go` imports
  `pkg/checks/delta` and `pkg/checks/logs` from the same block that makes
  `pkg/checks/state` reachable. `delta` reads container statuses (already
  preserved, no change) and `logs` reads `EphemeralContainers[].Name` — which is
  exactly the field the ephemeral strip had to be careful not to take.

Both were found by enumerating readers a second time, by hand, one day after the
first enumeration. Neither would have been caught by anything failing. That is
the argument for making the mechanism a test.

**S10 — Boundary with `pkg/graph` and `pkg/engine`. RESOLVED (2026-09-15).** The
answer moved work in both directions:

- **Leeway owns every index in §6.2**, chiefly because the graph feed is built
  inside `if f.stormEnabled()` — with `--storm=off` there is no graph at all, and
  a drift detector cannot have its counters disappear with an unrelated
  correlation flag. The graph also offers only traversal (`Radius`, `PodsUnder`,
  `OwnerChain`), no aggregation, and swaps too slowly for counting.
- **But `pkg/engine.RecoveryTracker` takes over half of §8.2.** Its
  `ClearanceObserver` interface is a clean predicate seam, and it already
  implements clearing/flap-reset/resolved/reverted. Leeway owns the fire side and
  implements the observer for the resolve side. Constraint recorded in §8.2: one
  global `stableFor` per tracker versus per-policy `resolveAfter`.
- `PodsUnder` and `OwnerChain` stay useful off the hot path — verifier rebuild and
  owner resolution — with a fallback for when storm is off.

**S11 — OTEL metrics migration. RESOLVED (2026-09-15). The answer is "half a day
*and* a week — and leeway only needs the half day."** The spike asked which of the
two it was; the prototype's real finding is that they are separable, and the design
had them fused.

*Method, as specified:* three instruments were actually migrated in a throwaway
branch — `lookout_recoveries_reverted_total` (plain counter), `lookout_storms_active`
(gauge), and `lookout_events_seen_total` (labelled counter through `boundReason`) —
onto `go.opentelemetry.io/otel/metric`, with `otelprom` bridging the `MeterProvider`
into the sentinel's existing registry. It compiled, served, and was then reverted.

**The scaffolding is genuinely small.** ~45 lines: a `newMeter` helper building the
bridge and the `MeterProvider`, a generic `must` unwrapper (every OTEL instrument
constructor returns `(T, error)`, and threading 43 error returns through
`buildMetrics` is not the trade anyone wants), and passing one `otelmetric.Meter`
down through `resolveRunners` → `newRunner`. Both readers can then hang off the one
provider exactly as §8.4 draws it. **This is all leeway needs**, and it can land in
Phase 2 alongside the source rather than gating it.

**Migrating the existing instruments is a different job.** Measured, not estimated:

| | Count |
|---|---|
| Instruments in `internal/watch/metrics.go` | 43 (23 `CounterVec`, 11 `Counter`, 5 `Gauge`, 3 `GaugeVec`, 1 `Histogram`) |
| Record sites outside `metrics.go` | 85 |
| …in a function with no `context.Context` in scope | 15 |
| Test files touching instruments directly | 25 |
| `testutil.ToFloat64` assertions | 88 |
| Diff for **three** instruments | 5 files, +118/−42 |
| Test files that stopped compiling from those three | 7, with 17 errors |

There is no wrapper layer: call sites hold the `*prometheus.CounterVec` and call
`WithLabelValues(...).Inc()` directly, so every one of the 85 is edited by hand.
Fifteen need a `ctx` that is not there — `countResolved(sig)` became
`countResolved(ctx, sig)` in the prototype, and that propagates. `testutil.ToFloat64`
takes a `prometheus.Collector` and cannot accept an OTEL instrument at all, so all 88
assertions are rewritten against a gather-and-parse or the SDK's `metricdata` reader.
Extrapolating the three-instrument diff, this is **4–6 engineer-days of mechanical
edits with a large blast radius and no user-visible benefit** beyond putting the old
metrics on OTLP too.

**And it is not neutral — it loses two things.** Both surfaced only because the
prototype was built rather than reasoned about:

1. **`MetricsInventory()` stops being derived.** `internal/watch/metricsdocs.go`
   builds the docs-site metrics page by calling `Describe()` on each live collector
   and regex-parsing `prometheus.Desc.String()`, precisely so names and help strings
   "cannot drift from metrics.go". An `otelmetric.Int64Counter` is an opaque
   interface whose only method is `Add` — the concrete type is an unexported
   `*metric.int64Inst`. There is no `Describe`, no name accessor, no description
   accessor. The three migrated rows had to be hand-written back into the table,
   which is exactly the drift `TestMetricsInventoryComplete` and
   `TestDescRegexpParsesHelp` exist to prevent. Migrating all 43 deletes that
   guarantee for the whole metrics page.
2. **Zero-valued series disappear** — see §8.4. Every counter that has not yet fired
   vanishes from `/metrics` instead of reading `0`.

**Dependency cost** is modest but real, and rubs against the recorded
dependency-minimisation policy: `go.opentelemetry.io/otel/metric` is already in
`go.mod` as *indirect* and `sdk/metric` is already in the module graph, so both come
free. New direct requirements are `go.opentelemetry.io/otel/exporters/prometheus`
(**v0.66.0 — still pre-1.0**, with its own release line), which pulls
`github.com/prometheus/otlptranslator`, plus an OTLP metric exporter when the push
side is switched on. The pre-1.0 bridge is the dependency worth flagging: it is the
component whose name-mangling behaviour the table in §8.4 pins, and a v0.x module is
entitled to change it.

*Decision.* §8.4's "migrate the existing instruments first" is **descoped**, and its
"two registries" trap does not apply — there is one registry either way. Leeway's
instruments are OTEL-native from Phase 2; the 43 existing ones stay Prometheus-native
and reach OTLP only if someone later files the refactor on its own merits. Record
that as a known asymmetry rather than a surprise: until then, an OTLP backend sees
leeway's metrics and not the sentinel's.

---

## 14. Delivery plan

Sized as lookout increments rather than as a standalone product. Phases 1–4 are a
shippable `topology-drift` source; Phase 6 is the `compute-class` source and is
independent of 5.

| Phase | Scope | Exit criteria |
|---|---|---|
| **0 — Spikes** (0.5 wk) | S1–S5 and S9–S11 **done**, S7 **deferred**, S6 **closed unrun** (2026-09-17 — the selector grammar answered it), **S8 resolved 2026-09-21** — the trimmed pod is 12× the assumed size (§6.1, §6.6) and the kwok padding it existed to size is shipped and enforced. **The spike programme is complete** | Package boundaries fixed (done); OTLP scope known (done); §7.7 transition model settled (done); scoring semantics settled (done); §8.4 export defaults measured rather than guessed (done); FR-9 mitigation designed (done); transform registry written (done) |
| **1 — Engine** (2 wks) | `pkg/leeway`: intent model, eligibility, apportionment, scoring. No informers, no source | Property tests green; zero client-go imports, enforced by test |
| **2 — Source skeleton** (2 wks) | `topology-drift` source, **default-on**: delta application, indexes, domain inventory, verifier, metrics on the OTEL API. Shared-transform change per S9, gated on its preserved-field registry | Counters provably correct under property tests at 10k pods; existing source tests still green; **transform attached (done, 2026-09-17)** — `newSharedFactory` is the single construction site and a cache-boundary test fails if the option is dropped; **domain inventory + `Placement` done, 2026-09-17**; **indexes, delta rules and subject resolution done, 2026-09-17** — counters checked against a from-scratch recount after 60k mixed events over 10k pods; **source skeleton, coalescing queue and OTEL instruments done, 2026-09-17** — the source runs against a live informer set and emits nothing, and the exported Prometheus names are pinned against the real exporter; **wired into the sentinel default-on, 2026-09-17** — `--topology-keys` and `--topology-per-domain-series`, no new watch stream and no new grant, and the bridged metrics documented against a real exporter because `MetricsInventory` cannot derive them; **§6.5 verifier done, 2026-09-17** — one shard of subjects rebuilt from the pod cache every 5 minutes, `lookout_leeway_counter_mismatch_total` on disagreement, repaired in place, proven by replaying the 60k-event churn with 1 event in 12 dropped. **Phase 2 complete.** |
| **3 — Intent inference** (2 wks) | TSC, affinity/anti-affinity, node selectors, tolerations, volume pinning, cluster defaults, precedence. **FR-7's node predicate done, 2026-09-18** — `Constraints` is read from an admitted Pod (never a template, per the S3 injection finding) and decides `MatchesSelector`/`Tolerated` for `NodeViews`, so §7.1 eligibility is now real; the §7.7.6 class-pinned arrangement is a test asserting zero drift rather than maximal skew. **COMPLETE 2026-09-19** — FR-4…FR-10 all shipped, ending with the three-state cluster defaults (FR-9) and the policy CRD (FR-10), and both exit criteria are now standing tests: a 22-scenario intent corpus exhaustive per axis, and a false-positive corpus scored end to end through `Resolve` → `Apportion` → `Score` → `Breach`. Nothing emits yet — that is Phase 4, which owns the state machine, dwell, hysteresis, tiers and severity routing | Correct intent on the scenario corpus; false-positive corpus clean |
| **4 — Findings** (2 wks) | State machine, dwell, hysteresis, tiers A/B, transient suppression, severity routing, `pkg/store` persistence. **Verdict layer, §8.2 state machine, §7.6 transient suppression, the v7 `leeway_alert_state` table with §9.3 startup reconcile, §8.5 cause attribution and the finding payload all done as library code, 2026-09-19.** **The scoring pass is now wired into the source, 2026-09-19** — `Source.evaluate` runs §7.1–§7.4 for every *eligible* axis of every subject (not only the declared ones, which would stop measuring the majority of an estate) and publishes the §8.4 score gauges; the per-domain cardinality gate is the real `ρ` floor rather than Phase 2's boolean; and the false-positive corpus now scores through the shipped `ScoreAxis` instead of reassembling the pass itself. **The §8.2 machine is now live in the source, 2026-09-19** — a 30 s alert tick advances every subject-axis against one instant (see §8.2 for why not the evaluation path), episodes persist through the v7 table under the watch process's `--store`, `loadAlertState` restores them for lazy reconcile against the first fresh verdict, and `lookout_leeway_alert_state` exports where each one sits. **§7.6 suppression is now live for the three rows leeway can answer alone, 2026-09-19** — cluster warmup, node drain and domain outage; the inventory samples each domain's ready-node count on a 30 s timer so the outage test has a peak to compare against, cordons are dated from the unschedulable taint, and `lookout_leeway_transient_subjects` reports what is being held back. The zone-outage exit criterion is a standing test at the mechanism level: twenty workloads all skewed by one dead zone open **zero** episodes, and the same skew without an outage still fires. **All five §7.6 rows are now answered, 2026-09-19** — the rollout row consumes `rollout.Source.RollingOut()` through a `RolloutOracle` seam adapted at the composition root (and a deployment without the rollout source says so at startup rather than reporting no rollouts), and the recent-scale row watches `spec.replicas` on Deployments and StatefulSets, stamping only a size that differs from the one already on file so that neither the informer's resync nor a restart reads as a cluster-wide scale event. **Emission shipped, 2026-09-20 — PHASE 4 COMPLETE.** A firing episode builds the §8.5 payload from evidence the source already holds, routes it through §8.3, and reaches the wire as `leeway.contract_violated` or `leeway.placement_drift` (two kinds, not five: `baseline_breach` needs a learned baseline, which is Phase 5, and `domain_unavailable` is Phase 7's source-raised kind). The incident UID is synthetic and carries the axis — `leeway:<subject>|<topology-key>` — because one subject drifting on zone and fine on region is two findings with two dwell timers. Resolution is the clearance observer's job rather than a second signal: §7.4 owns outcome records, `PhaseResolving` reports firing, so §8.2's resolve dwell and `--recovery-stable-for` compose. Tier C is metrics-only unless `--topology-tier-c-signals` says otherwise, and the dwell is `--topology-dwell` | Restart tests pass; zone-outage scenario yields one finding, not four hundred |
| **5 — Baselines** (2 wks) | EWMA/EWMAD, freeze-while-firing, maturity gates, invalidation, Tier C. **The estimator shipped as library code, 2026-09-20** — `leeway.BaselineSet` holds one EWMA share and one EWMAD deviation per domain, matures behind both §7.5 gates, freezes while an episode fires (the clock stops, not just the arithmetic, so a week-long incident does not teach the baseline that the incident is normal), and invalidates on the events §7.5 lists. The warm-up bias is real and corrected: at a 12 h half-life a baseline matures having accumulated ~29 % of its weight, so raw EWMAD understates dispersion ~3.5× — `DevWeight` accumulates `α(1−w)` and `DeviationOf` divides by it. **Persistence shipped, 2026-09-20** — store migration v8 and `leeway_baseline`, written in one batched transaction every 30 s (§9.2's second write policy; alert state stays synchronous), with §9.3 step 7's downtime rules on restore: ≤ 2 h resume, ≤ 24 h widen ×1.5 for one half-life, beyond that mark stale. `DevWeight` persists, because a baseline restored without it would re-acquire the bias it was corrected for. **Wired into the source, 2026-09-20** — a sample timer on its own clock, independent of the evaluation coalescer, so what is learned is a placement's duration and not its edit rate. **Tier C turn-on, 2026-09-20 — PHASE 5 COMPLETE.** A mature baseline renders as an `*Intent` with `Source: LearnedBaseline`, so it reuses apportionment, scoring, the per-domain gauges and routing unchanged; the presence of `Bands` (k·max(deviation, floor)) is what switches the breach rule from ρ to the per-domain band test, and no declared source sets them. An immature baseline is nil, which means "scored against an even apportionment" and not "unmonitored". The finding reaches the wire as `leeway.baseline_breach`, the 52nd kind in the frozen schema, still metrics-only unless `--topology-tier-c-signals` says otherwise. Three flags: `--topology-learn-baselines` (default on), `--topology-baseline-half-life`, `--topology-baseline-band`. **This phase amended §5.1** — see the delta there: `SourceClusterDefaultAssumed` moved to the bottom of the precedence list, without which the whole phase would have shipped inert | Tier C detects injected drift in soak without firing on the FP corpus. **Both halves now hold as standing tests, 2026-09-20**: a Deployment that declared nothing and has always sat evenly across three zones, with all six replicas in one, produces exactly one `leeway.baseline_breach` quoting the *learned* expectation; and all seven §12 fixtures run a second time with their declarations stripped — so the baseline is what scores them — and add no findings at the narrowest (floor-width) band |
| **6 — Preference ranks** (2 wks) | `compute-class` source: dynamic ComputeClass informer, configurable extractors, rank resolution with cross-check, time-weighted pod-seconds, attribution SLIs. **COMPLETE 2026-09-20.** The node model and rule matcher (#457), rank resolution and the pod-second tracker (#458), the ComputeClass reader (#459) and the source itself (#460) shipped counters-first, the same staging Phase 2 used. **Findings shipped 2026-09-20 (#462)**: the four §7.7.4 kinds, judged over a *sampled* window rather than the cumulative counters — the tracker's totals run for the axis's life, so a share taken off them would still be reporting a bad week in March in June; the judge keeps a ring and diffs the ends over `--compute-class-window`. §8.2's machine and `leeway.Reconcile` are reused unchanged, and the persisted rows share the one `leeway_alert_state` table with `topology-drift`, told apart by subject kind at `Load` on both sides. The incident UID prefix is `leeway-rank:` with a hyphen rather than a colon, so the other source's parse fails outright instead of half-succeeding and reporting a rank incident `cleared/object_deleted` on its first sweep. **A class with fewer than two priorities is not judged at all, which takes the wedged rule with it** — deliberate, and confirmed live: the four GKE-managed Autopilot classes each declare exactly one priority, and a Pending pod on one of those is the ordinary out-of-capacity story `capacity` already reports. `wedged_pods` still counts them. One §7.7.4 row is deferred to #463: mean achieved rank against the axis's own EWMA baseline, which needs §7.5's `BaselineSet` plumbed into this source | Rank shares match a hand-audited sample of a live GKE cluster; unmatched and disagreement rates 0. **Both met on `std-simian-test`, 2026-09-20**: `preference_nodes` read 2 at rank 0 for `n2-preferred` and 1 for `n4-preferred`, which is exactly what `kubectl get nodes -L cloud.google.com/compute-class` shows, and 100 % of pod-seconds on both axes sat at rank 0; `unmatched`, `disagreement`, `no_rule_matching`, `ambiguous`, `off_axis`, `out_of_range`, `axis_invalid` and `tracker_underflows_total` were **all zero** over the run |
| **7 — Nodes** (1.5 wks) | Node-group subjects, capacity weighting, `leeway.domain_unavailable`. **Capacity weighting shipped 2026-09-21 (#468)**: `Weighting` gained an `Auto` zero value so that a declared `Equal` and an undeclared weighting stop being the same thing, every `NodeView` now carries both allocatable dimensions (the trigger has to read CPU to *choose* the weighting, so projecting one resource during eligibility was a chicken-and-egg), and §5.1 resolution writes the resolved weighting back onto the intent with an evidence line quoting the ratio. **This phase amended §7.2** — see the four carve-outs there; the pod-count one was found by breaking two §12 fixtures, and the `minDomains` one would have hidden every padded subject's missing domain. **Node-group subjects shipped 2026-09-21 (#469)**: the inventory groups nodes off FR-3's precedence list, a 30 s pass scores each group against the cluster's domains with no intent (Tier C, deliberately — see FR-3's three rules), and `--topology-max-node-groups` bounds the cardinality by dropping *all* groups rather than an arbitrary subset, with `lookout_leeway_node_groups_discovered` still reporting the true count. Node groups live in their own map inside `State`, absent from `Subjects()`: §6.5's verifier rebuilds each shard from the pod cache, and a subject with no pods would be found empty, called counter drift and erased every sweep. **This shipped a second §7.2 amendment, in the opposite direction to the first** — node-group subjects were to default to `AllocatableCPU` and are instead *pinned* to `Equal`, because a node group's objects are the capacity the weighting apportions over, and the circularity does not only excuse a lopsided pool, it makes an evenly spread pool alongside it read as drifting. **`leeway.domain_unavailable` shipped 2026-09-21 (#467) — PHASE 7 COMPLETE**: the last of §2.3's eight names now has a producer and the v1 ledger is 57 kinds. The subject is the domain, `SubjectDomain`, the one subject kind that holds no objects — which is why the §8.2 machine and the clearance observer took it unchanged but two things around them did not: `judgements` has to append the domain set or `Alerts.Pass` reads every domain episode as gone, and `Clearance` has to skip its pod-index check or it reports a zone that is still down as recovered-`object_deleted` on the first observation. The rule is `Known && usable == 0` against a latch of which domains the cluster has — see §2.3 on why not a window, why *usable* rather than Ready, and why the axis set is configuration | Node-pool imbalance detected and attributed. **The §12 corpus gained a heterogeneous-capacity fixture, 2026-09-21**: ten replicas sitting `[8 1 1]` across zones that are 64, 8 and 8 cores are quiet under the trigger and breach at ρ = 0.4 without it. **The node-group half now holds as a standing test, 2026-09-21**: three zones, a six-node pool wholly in one of them and a balanced pool alongside it — the concentrated pool produces one finding naming the pool (ρ = 2/3, R = 4), the balanced pool scores ρ = 0, and a Deployment pinned to the concentrated pool by `nodeSelector` is narrowed by FR-7 to that one zone and stays quiet rather than restating its pool's imbalance. **The domain half holds as a standing test, 2026-09-21**: three zones and twenty workloads spread over them, every node in one zone lost — **exactly one** `leeway.domain_unavailable` reaches the wire and not one of the twenty workloads emits, because §7.6's outage row suppresses them; cordoning every node in a zone instead produces the same kind with `taint_exclusion` where deleting them produces `consolidation`; and the zone coming back resolves through the existing clearance observer rather than as a second signal |
| **8 — Hardening** (2 wks) | Cause attribution consuming sibling sources, cardinality controls, OTLP hardening, `cmd/leeway`, docs, dashboards | Scale + soak met on the padded kwok harness; `cmd/leeway` built and smoke-tested in CI; process survives a black-holed OTLP endpoint for 24 h with flat RSS |

Roughly 14 weeks, against ~19 for the standalone version — the difference is almost
entirely the plumbing lookout already owns.

**Phase 0 is not optional and not a formality.** S9, S10 and S11 have already earned
it: between them they caught a field selector that would have silently disabled
eviction detection, moved half the §8.2 state machine from "write it" to "implement
one interface", and took a week-long metrics refactor off the critical path by
finding that the pull exporter registers into the registry the sentinel already has.
All three were desk work — a morning's reading apiece, and S11 a short-lived
prototype. **S1 then justified the whole argument for running spikes first**: it took
an afternoon on a live cluster, it invalidated the §7.7.3 transition model outright,
and it turned up an annotation state machine that no amount of desk work would have
produced. Discovering that at week 12 would have meant rebuilding the data model
rather than adjusting it.

**S2 is the counterexample that makes the same point.** It confirmed §7.7.1 exactly
as written and changed no structure — half a day to learn nothing, on the face of it.
But it was the last open question that *could* have inverted rank ordering on every
scored class, and a confirmation is only worthless once you have it. It also caught
the tied-score fallback live, which is a false-positive class §7.7.1 predicted on
paper and nobody had seen.

**S3 is the third shape: the spike that looked like a lookup.** "Confirm two label
keys" reads as desk work you could skip, and skipping it would have shipped a
`Reservation string` that silently conflates same-named reservations in different
projects — a wrong answer on exactly the multi-project clusters least able to notice.
The keys were as predicted; the *arity* was not.

**S8 is the fourth shape, and the only one that made the design wrong rather than
incomplete.** Every other spike confirmed a claim, narrowed one, or deleted work.
S8 took a number the document had carried since the standalone design — the trimmed
pod at ~1.5 KiB — and found it 12× out, which in turn was the difference between
Phase 8's scale gate meaning something and being theatre. It had been sitting at the
bottom of the list as the soft one.

Phases 1–4 are a shippable increment at around week 9: declared and inferred intent,
findings, restart-safe, no baselines and no compute classes.

---

## 15. Open questions

1. ~~Fleet size~~ — **resolved.** 150–200k pods at the high end, 500 pods/sec
   sustained. NFR-1 is set from these; §6.6.1 shows the marginal cost inside the
   sentinel is ~0.013 core. The residual — namespace count — is **withdrawn, not
   pending**: §6.7 shows the scoping decision does not turn on it, and how many
   namespaces a cluster has is a support envelope to declare rather than a number to
   go and find (S6).
2. ~~GKE compute class label keys~~ — **resolved** against `simian-test` on
   2026-09-11. GKE records the achieved priority in the `ccc_priority_index` node
   annotation, so §7.7.2 reads ground truth and demotes inference to fallback plus
   cross-check. Undocumented and unguaranteed; risk accepted, mitigated by the
   fallback path and the attribution SLIs. **S1 closed 2026-09-15** and materially
   revised this: the annotation is not purely numeric and is not immediately
   present — see §7.7.2. **S2 closed the same day** and did not revise it: the field
   stays an index under `priorityScore`, so §7.7.1 stands. **S3 closed 2026-09-16**
   and finished the extractor config: every key in §7.7.2 is now measured, with no
   `UNVERIFIED` marker left. It also widened one — a consumed reservation is
   identified by (project, name), not name — so `NodeProfile.Reservation` became a
   `ReservationRef`. No residual.
3. ~~Scale posture~~ — **resolved 2026-09-15: the higher targets stand, and
   DESIGN §6.2 was edited.** 200k pods and 500 pods/sec are now the repo's stated
   design point, split across the two axes they were always two answers to. See
   §2.5 item 2 and DESIGN §6.2. **Residual closed 2026-09-21 by S8**, which
   remeasured the pod-cache line §6.1's narrowed transform invalidated: 18,630 B per
   trimmed pod, so the shared cache at the 200k design point is ~3.5 GiB and the
   shipped 256Mi default is undersized from ~14k pods. The §6.6 tiers are corrected;
   the sizing consequence is Phase 8's.
4. ~~Should `topology-drift` be enabled by default?~~ — **resolved 2026-09-15:
   yes, default-on.** §8.3 routes Tier C to metrics only, so the default
   deployment adds roughly zero agent sessions, and a drift detector nobody turns
   on detects nothing. The condition attached to it is the one that was always the
   real question: default-on means the §6.1 informer transform reaches *every*
   deployment, and today there is no transform at all (`wiring.go:963` is a bare
   `NewSharedInformerFactory`). **The transform must not ship before S9's remaining
   deliverable** — the preserved-field registry and the test that fails when a field
   is added to the strip list without an entry. Until that lands, the source builds
   default-on but the transform stays off, which costs only the pod-cache memory
   S8 has since measured (25.7% of a pod's heap, 40.6% of its bytes). Both landed in
   Phase 2.
5. **Is fallback depth per-workload or per-class?** §7.7 aggregates per axis by
   default. If two Deployments share a class but only one is falling back, per-axis
   metrics hide it — but per-subject labels multiply cardinality.
6. ~~Cardinality budget~~ — **resolved 2026-09-16 (S5).** The estate is Google
   Managed Prometheus, so there is no ceiling to hit: GMP bills per sample rather
   than rejecting, which for a default-on subsystem is worse, because the failure is
   a growing line item and not a broken scrape. `perDomainSeriesMinDrift: 0.05`
   therefore stays a default rather than a knob — see §8.4. The OTLP half is the
   GKE managed collector, measured, and push does make it bind harder as this item
   said. No residual.
7. ~~Cluster default constraints~~ — **resolved 2026-09-16 (S4): neither of the two
   options this question offered.** Confirmed unreadable on managed GKE, so FR-9 is a
   documented manual sync — but the answer is not a widened Tier A threshold either.
   Default-derived intent is **capped below Tier A entirely**, because the upstream
   defaults are `ScheduleAnyway` and a soft preference is not a contract to violate.
   The residual risk is precision, not correctness, and it is bounded further by the
   fact that the defaults apply only to Pods that declare no spread of their own. See
   §13 S4 for the three-state config and the `cluster-default-assumed` provenance
   label. No residual.
8. **Karpenter consolidation.** Consolidation deliberately packs pods and produces
   drift by design. Default Karpenter-managed pools to `Ignore` on zone, or alert with
   consolidation as an annotated suspected cause? Leaning the latter — intentional and
   safe are not the same thing.
9. **Descheduler integration.** Emitting a
   `RemovePodsViolatingTopologySpreadConstraint` hint is a natural v2 and sits
   uncomfortably against lookout's read-only posture. Worth reserving surface now?
10. **Region-level topology.** Multi-region clusters are rare but the model supports
    them. In scope for the target fleet, or is zone the only axis that matters?
