# leeway — Placement Drift Detection

**Status:** Draft v0.3 — proposal. Spikes S9 and S10 resolved against this repo;
S1 still gates the §7.7 data model.
**Date:** 2026-09-15
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
  several commands. Proposed: `leeway.skew`, `leeway.contract_violated`,
  `leeway.domain_outage`, `leeway.pinned_skew`, `leeway.rank_depth`,
  `leeway.rank_wedged`, `leeway.rank_no_migration`.
- **Metrics are `lookout_leeway_*`**, matching the existing `lookout_*` prefix.

> **The kinds are the expensive part.** `TestSchemaV1_KindInventory` pins the
> inventory and v1 is frozen, so kind names are a durable commitment in a way that
> source names, package names and metric names are not. They should be settled
> before the first source lands, not during.

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

Three things, all load-bearing:

1. **We do not own the informers, so `trimPod` becomes a negotiation.** See §6.1 —
   this is the one place where the fold-in has a real cost, and it invalidates part
   of the original memory model.
2. **The scale posture needs reconciling.** lookout DESIGN §6.2 states *"typical
   single clusters are 1–15k pods; 100k is the ceiling, not the design point"* and
   drops a 50k events/s target as fiction. This document targets 200k pods at 500
   pods/sec. These are less far apart than they look — §6.6 concludes that node
   count is nearly free, memory is linear in pods, and 5,000 events/s costs about a
   quarter of a core, which *agrees* with lookout's "hundreds to low thousands of
   events/sec" framing. But someone has to edit that paragraph rather than leave
   two design docs in the same repo contradicting each other.
3. **Output shape.** Lookout's watch path turns signals into agent sessions with
   warm context. Drift is a slow-moving condition, not an incident. §8 handles this
   by routing most leeway findings to the store and metrics rather than to inject,
   using the existing severity routing (DESIGN §7.7) — the same treatment
   `notifications` info-severity signals already get.

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
  configurable label-key precedence list. Compute class outranks node pool because
  auto-created pools are per-machine-type and ephemeral (§11).

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
- **FR-9** Support the scheduler's `PodTopologySpread` `defaultConstraints` as
  static configuration, since they are not readable from the API server (spike S4).
- **FR-10** Allow explicit declaration of intent via CRD, overriding inference.

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

    // Expectation shaping.
    Weighting       Weighting // Equal, NodeCount, AllocatableCPU, AllocatableMemory
    EligibleDomains sets.Set[Domain]
    DomainCaps      map[Domain]int64

    Confidence Confidence // Declared, Inferred, Learned
    Evidence   []EvidenceItem
}
```

**Precedence** (highest first): `SourcePolicyCRD` → `SourceWorkloadAnnotation` →
`SourceTopologySpreadConstraint` → `SourcePodAntiAffinityRequired` →
`SourceClusterDefaultConstraints` → `SourcePodAntiAffinityPreferred` →
`SourceLearnedBaseline`. Multiple intents on *different* topology keys coexist; on
the same key the highest-precedence source wins and the others are retained as
evidence.

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

`pkg/checks/state` is in this table because it is *not* only read-path code:
`internal/watch/enrich.go:80` imports it, and the enricher's `livePod` reads
straight from the shared pod lister. Anything the transform does reaches the
enrichment payload too.

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
    for i := range pod.Spec.Containers {
        c := &pod.Spec.Containers[i]
        for j := range c.Env {
            c.Env[j].Value = "" // drop literal values; keep Name + ValueFrom
        }
        c.Command, c.Args, c.Lifecycle = nil, nil, nil
        c.ReadinessProbe, c.LivenessProbe, c.StartupProbe = nil, nil, nil
    }
    pod.Spec.InitContainers = trimContainers(pod.Spec.InitContainers)
    return pod, nil
}
```

**Consequence: the pod memory model in §6.6 needs remeasuring.** Retaining
`ContainerStatuses` and the `Env` name/ref structure puts a trimmed pod well above
the original ~1.5 KiB estimate. That is what spike **S8** exists to settle, and the
tier table below should be treated as provisional until it does.

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

### 6.4 Coalescing

The workqueue is keyed by subject, so a rollout churning 200 pods for one
Deployment collapses into a few evaluations. `AddAfter(key, coalesceWindow)`
(default 2 s) turns a burst into one evaluation at the end; during an active
rollout the window widens to `rolloutCoalesceWindow` (default 15 s).

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
already paid for — which is the entire quantitative case for §2.1. Absolute
process footprint is lookout's number to own, and §6.1 means the pod-cache line in
it has to be remeasured (spike S8).

The "Typical" row is added deliberately: lookout DESIGN §6.2 puts real clusters at
1–15k pods, and at that size leeway costs single-digit MiB. The larger rows exist
because the design has no cliff in it, not because we expect them.

Set `GOMEMLIMIT` to ~80% of the container limit so the GC becomes aggressive under
pressure instead of the kernel OOM-killing us.

#### 6.6.1 Event-rate cost model

This, not memory, is what the design target stresses. At 500 pods/sec sustained
scheduling, steady state implies ~500 pods/sec terminating too, and each pod
lifecycle produces roughly eight watch events (spike S7 measures the real figure):

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

### 6.7 Sharding

The standalone design carried a full horizontal-sharding section. Folded in, it is
**out of scope**: sharding is a property of the sentinel, not of leeway, and
lookout DESIGN §11 already answers the fleet question by putting fan-in in the
fleet layer.

Two things carry over as cheap seams:

- **Store keys are subject-keyed, not shard-keyed**, so baselines survive any future
  resharding of the sentinel.
- **Subjects never span namespaces**, so leeway's state partitions cleanly on
  namespace if the sentinel is ever sharded that way.

For the record, in case the question returns: client-side filtering divides memory
by N but *multiplies* aggregate decode cost and watch fanout by N, because every
shard still receives every event. Only server-side (namespace-scoped) watches reduce
decode, and field selectors are single-valued, so a shard owning *k* namespaces opens
*k* streams — total streams equals the cluster's namespace count regardless of shard
count (spike S6).

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
node-group subjects and for workloads whose eligible domains have materially unequal
capacity (triggered when max/min domain capacity ratio > 1.25).

### 7.3 Metrics

Let `a_i` be actual count in domain `i`, `e_i` expected.

| Metric | Definition | Use |
|---|---|---|
| **Observed skew** | `S = max(a) − min(a)` over eligible domains | Direct comparison to TSC `maxSkew` |
| **Min achievable skew** | `S* = 0 if n mod m == 0 else 1`, raised by caps | The floor imposed by arithmetic |
| **Excess skew** | `E = max(0, S − max(S*, maxSkew))` | Tier A/B primary signal |
| **Relocation distance** | `R = Σ_i max(0, a_i − e_i)` | "How many pods must move" — the number humans act on |
| **Normalised drift** | `ρ = R / n ∈ [0,1]` | Scale-free threshold; equals total variation distance |
| **Concentration** | `H = Σ (a_i/n)²` (Herfindahl) | Blast-radius framing |
| **Max domain share** | `max(a_i)/n` | The number that matters for zone-failure risk |
| **Goodness of fit** | `χ² = Σ (a_i − e_i)²/e_i`, df = m−1 | Significance gate for weighted expectations, only where all `e_i ≥ 5` |

**Primary signal:** `ρ` for threshold evaluation, `R` for the human-readable body,
`max domain share` for severity escalation. `E` supersedes both where a hard
contract (`maxSkew`) exists.

Rationale for `R`/`ρ` over raw skew: skew is a max-min statistic and therefore blind
to the shape of the distribution. `[10,0,0,0]` and `[10,3,3,4]` both have skew 10
across four domains but represent very different risks. `ρ` is 0.75 vs. 0.15, which
matches intuition.

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
    Reservation   string
    Accelerator   string
    Labels        map[string]string // raw, for user-defined matchers
}
```

**Rule identity and preference rank are different things, and conflating them is the
trap this model exists to avoid.** The optional `priorityScore` field
(1.35.2-gke.1842000+) sets preference explicitly, *higher meaning more preferred* —
the opposite direction to list position — and **several rules may share a score**,
making them equal-preference alternatives rather than a fallback sequence. GKE
requires the field on all rules in a class or none.

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
        // GKE documents this as all-or-nothing. Seeing otherwise means our
        // understanding is wrong, so refuse to invent an ordering.
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
would invert the ordering on any scored class.

Two things temper it:

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
    declared, hasDeclared := parseIndex(node.Annotations[r.cfg.PriorityIndexAnnotation])
    inferred, quality := r.match(profileOf(node), axis) // first-match-wins

    var idx int
    var src RankSource
    switch {
    case hasDeclared && quality == matchOK && declared != inferred:
        r.metrics.Disagreement.Add(1, attribute.String("axis", axis.Name))
        fallthrough
    case hasDeclared:
        idx, src = declared, SourceNodeAnnotation
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
      reservation: { label: cloud.google.com/reservation-name }    # UNVERIFIED — S3
      accelerator: { label: cloud.google.com/gke-accelerator }     # UNVERIFIED — S3
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
lookout.leeway.preference.unmatched           {...}   # no rule matched, no annotation
lookout.leeway.preference.ambiguous           {...}   # node matched >1 rule
lookout.leeway.preference.disagreement        {...}   # annotation != inferred
lookout.leeway.preference.out_of_range        {...}   # stale index after a class edit
lookout.leeway.preference.axis_invalid        {...}   # partially-scored class
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

func (t *RankTracker) Move(axis AxisKey, from, to int, at time.Time) {
    t.bucket(axis, from).accumulate(at); t.bucket(axis, from).count--
    t.bucket(axis, to).accumulate(at);   t.bucket(axis, to).count++
}
```

> **`Move` may be modelling an event that does not occur.** No cluster we have
> inspected has ever fallen back — every node observed sits at rank 0 — so we have not
> established whether a fallback *mutates* a pod's node assignment or simply
> provisions a new node in a new NAP pool and schedules a *new* pod there. If it is
> the latter, which the per-machine-type pool naming makes likely, then no individual
> pod ever changes rank: rank-0 pods die and rank-1 pods are born.
>
> Pod-seconds accounting is unaffected — the old pod stops accruing at rank 0 and the
> new one starts at rank 1, correct either way. But the transitions counter would be
> permanently empty, and the migration-back finding is built on observing
> `from_rank > to_rank`, so it would never fire. In that case the signal must be
> reconstructed at **subject** level — the Deployment's rank *mix* shifted between
> evaluations — rather than per pod.
>
> This is a data-model question, not a test-coverage question, and it is why spike
> **S1** must precede the compute-class source rather than accompany it.

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
| Rank-0 time share below threshold | First-choice capacity has degraded | B | `leeway.rank_depth` |
| Mean achieved rank rises vs. its own EWMA baseline | The mix got worse | C | `leeway.rank_depth` |
| Sustained time at the last rank | Running on last-resort capacity, often spot | B | `leeway.rank_depth` |
| An entire **tier** unused in 30d | Dead preference level — or a reservation being paid for and never used | info (cost) | `leeway.rank_depth` |
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
promExporter, _ := otelprom.New() // pull; registers as a prometheus.Collector
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

> **This is a sentinel-wide change, not a leeway one.** `internal/watch/metrics.go`
> builds a `prometheus.NewRegistry()` directly and `internal/telemetry/otel.go`
> bootstraps traces only. Adding OTLP metrics means migrating the existing ~30
> `lookout_*` instruments onto the OTEL API, or running two registries and accepting
> that only leeway's metrics reach OTLP. The first is correct and is a prerequisite
> task, not a side effect; the second is a trap that will be discovered by whoever
> builds the first cross-subsystem dashboard. Sizing this is spike **S11**.

**Names are declared in OTEL form; the Prometheus spelling is derived.** The
exporter mangles names — `.` → `_`, unit appended, `_total` appended for monotonic
counters. Hand-maintaining two lists guarantees drift. Watch for double suffixing: an
instrument named `...pod_seconds` with unit `s` exports as `..._pod_seconds_seconds_total`;
naming it `...pod_time` with unit `s` lands correctly on `..._pod_time_seconds_total`.
Every metric name in this document has been written in OTEL form for that reason.

**Temporality is configurable, and delta improves the restart story.** Prometheus
requires cumulative; many OTLP backends prefer delta. Per-instrument selectors handle
both. The §7.7.3 pod-seconds counter relies on `increase()` tolerating a reset across
restarts under cumulative — under **delta** that concern disappears, since each export
carries only the interval's increment.

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

### 8.5 Finding payload

```json
{
  "kind": "leeway.skew",
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

Cause attribution is a small rules engine over evidence we already hold:

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
    sources: [TopologySpreadConstraint, PodAntiAffinityRequired, PodAntiAffinityPreferred]
  baseline: { enabled: true, halfLife: 12h, deviationSigmas: 4.0 }
  exclusions:
    minReplicas: 3
    ignorePinnedByVolume: true
    suppressDuringRollout: true
```

Per-workload annotation overrides remain the lightest-weight path:
`leeway.lookout.go-steer.io/max-drift: "0.2"`.

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
    # Mirrors kube-scheduler's PodTopologySpread defaultConstraints, which is not
    # readable from the API (spike S4). Must be kept in sync manually.
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
  - { apiGroups: [""], resources: [persistentvolumeclaims, persistentvolumes],
      verbs: [get, list, watch] }        # volume pinning, FR-8
  - { apiGroups: ["cloud.google.com"], resources: [computeclasses],
      verbs: [get, list, watch] }        # optional; source self-disables if absent
  - { apiGroups: ["leeway.lookout.go-steer.io"],
      resources: [leewaypolicies, clusterleewaypolicies],
      verbs: [get, list, watch] }        # optional
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
| StatefulSet with zonal PVs | Pods marked `Pinned`; excluded from actionable drift, reported as `leeway.pinned_skew` |
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
  derivation. Seed fixtures from the real `ComputeClass` specs and node dumps — the
  risk here is that our platform assumptions are wrong, and synthetic cases cannot
  falsify them.
- **Eligibility under compute classes** — assert a class-pinned workload whose class
  exists in one zone reports *zero* drift, not maximal skew (§7.7.6). This belongs in
  the false-positive corpus; it is the failure mode most likely to discredit the tool
  on a GKE cluster.
- **Export pipeline** — a golden-file test over every instrument's derived Prometheus
  name (this is what stops the §8.4 double-suffix bug reaching a dashboard); both
  readers reporting consistent values; a black-holed OTLP endpoint yielding rising
  drop counts with flat heap and an unaffected `/metrics`.
- **False-positive corpus** — a fixture set of clusters that are *fine* (unavoidable
  skew, restricted eligibility, pinned volumes, mid-rollout). Reporting on any of them
  is a test failure. Grows with every false positive found in production.

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
> `status.images` list; pod templates carry realistic managedFields, annotations, env
> and container statuses, sized from percentiles measured on a real cluster (spike
> **S8**). A CI check should assert the mean serialised object size in the fixture is
> within tolerance of the measured p50, so fixtures cannot silently deflate over time.

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

**S9 and S10 are done** (2026-09-15) — both were code archaeology against this repo
and needed no cluster. Between them they changed four things: the terminal-pod field
selector is narrower, the transform's narrowing is confirmed safe, leeway owns its
own indexes because the graph runs only under `--storm`, and half the §8.2 state
machine is now `pkg/engine.RecoveryTracker` rather than new code. Three days remain,
and **S1 is the one that gates a data model**.

| ID | Question | Unblocks | Effort |
|---|---|---|---|
| S1 | What *is* a compute-class fallback — new node or mutated node? | §7.7.3 transition model | 0.5 d |
| S2 | Does `ccc_priority_index` stay an array index when `priorityScore` is set? | §7.7.1 `assignRanks` | 0.5 d |
| S3 | Reservation and accelerator node label keys | §7.7.2 extractor config | 0.25 d |
| S4 | Can we read the scheduler's `defaultConstraints`? | FR-9 correctness | 0.25 d |
| S5 | Prometheus series budget and OTLP backend | §8.4 gating defaults | 0.25 d |
| S6 | Namespace count and watch-stream headroom | §6.7 sharding viability | 0.25 d |
| S7 | Real pod event rate per scheduled pod | §6.6.1 cost model | 0.5 d |
| S8 | Real object sizes, for kwok padding and the trimmed-pod budget | §6.6, §12.1 validity | 0.5 d |
| ~~S9~~ | ~~Does any source need terminal pods, or fields the transform strips?~~ | **RESOLVED** — §6.1 amended | done |
| ~~S10~~ | ~~How much of §6.2 / §8.2 does `pkg/graph` + `pkg/engine` already provide?~~ | **RESOLVED** — §6.2 and §8.2 amended | done |
| S11 | Cost of migrating the sentinel's ~30 metrics to the OTEL API | §8.4 scope | 0.25 d |

**S1 — Observe a fallback.** No node in any cluster inspected has ever left rank 0, so
the entire fallback half of §7.7 is unvalidated. This is not a test-coverage gap;
§7.7.3 shows it can invalidate the transition data model outright.

*Method, free part first:* sweep the fleet for a cluster that has already fallen back,
before building anything.

```
for c in $(kubectl config get-contexts -o name); do
  kubectl --context "$c" get nodes -o json 2>/dev/null \
  | jq -r --arg c "$c" '.items[]
      | select(.metadata.annotations.ccc_priority_index // "0" != "0")
      | [$c, .metadata.name, .metadata.annotations.ccc_priority_index,
         .metadata.labels["cloud.google.com/compute-class"]] | @tsv'
done
```

*Otherwise provoke one* in a test project: a `ComputeClass` whose rank-0 rule is
unsatisfiable (a machine family absent from the region) and whose rank-1 rule is
ordinary, plus one throwaway Deployment. Capture, in order: (i) does the rank-1 node
carry `ccc_priority_index: "1"`; (ii) is a **new** node and NAP pool created, or is an
existing node re-labelled — this decides the transition model; (iii) does the pod bind
directly or go Pending; (iv) the difference between `whenUnsatisfiable: DoNotScaleUp`
and the default; (v) on making rank 0 satisfiable again, whether
`activeMigration.optimizeRulePriority` produces an observable move, whether it is the
same pod, and how long it takes.

Item (ii) is the only thing that can validate `RankTracker.Move`; (v) is the only thing
that can validate the migration-back finding. *Done when* we know whether per-pod rank
transitions exist at all. Keep the class as the permanent test fixture.

**S2 — `priorityScore` semantics.** The inspected cluster runs 1.36.3-gke.1537000, so
the field is supported, but no class uses it. *Method:* add a scored class, with a tie,
to the S1 spike. *Done when* we know whether `ccc_priority_index` remains an index into
`spec.priorities` under scoring or switches to meaning a rank — and whether GKE rejects
a partially-scored class at admission or leaves us to handle `OrderingInvalid`.

**S3 — Reservation and accelerator labels.** The two remaining unverified extractor
keys. *Method:* one reservation-backed node and one GPU node, dump labels. *Done when*
the §7.7.2 config has no `UNVERIFIED` markers left.

**S4 — Scheduler default constraints.** `PodTopologySpread.defaultConstraints` shapes
scheduling but is not readable through the API, and static config that silently
desyncs from the real scheduler is a correctness hazard. *Method:* attempt a read-only
mount or API read of the `KubeSchedulerConfiguration` per environment. *Done when* we
know whether FR-9 is a live read or a documented manual sync. On managed GKE the answer
is probably "no", which needs its own mitigation.

**S5 — Metrics budget.** The §8.4 gating thresholds are guesses. *Method:* ask for the
Prometheus series budget; confirm whether an OTLP collector endpoint exists and which
temporality its backend wants. *Done when* `perDomainSeriesMinDrift` and the OTLP
defaults come from a number rather than a guess.

**S6 — Namespace count.** *Method:* `kubectl get ns --no-headers | wc -l` across the
fleet, plus current apiserver watcher counts. *Done when* §6.7 records whether
server-side sharding is available should the sentinel ever need it.

**S7 — Real event rate.** §6.6.1 assumes ~8 watch events per pod lifecycle, a modelled
number the whole CPU budget scales off. *Method:* a throwaway watch on the busiest
cluster counting pod events over an hour against pods scheduled in the same window.
*Done when* the multiplier is measured.

**S8 — Real object sizes.** Now doing double duty: it sizes the kwok padding *and*
settles the trimmed-pod budget that §6.1's narrowed transform invalidated. *Method:*
sample p50/p95/p99 serialised sizes of `Pod` and `Node` on a real cluster, before and
after the §6.1 transform, plus the `status.images` length distribution. *Done when* the
kwok templates are padded to match and the §6.6 tiers are confirmed or corrected.

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

*Remaining work* is the deliverable, not the question: the preserved-field registry
and the test that fails when a field is added to the strip list without an entry.
**This still blocks the transform change**, which is a Phase 2 item.

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

**S11 — OTEL metrics migration.** §8.4 requires a `MeterProvider`, but
`internal/watch/metrics.go` uses a raw Prometheus registry and
`internal/telemetry/otel.go` bootstraps traces only. *Method:* prototype the migration
of three existing instruments (a counter, a gauge, a labelled counter with
`reasonLabelCap`) and measure the diff. *Done when* we know whether this is a
half-day prerequisite or a week-long refactor — and therefore whether leeway lands
before or after it.

---

## 14. Delivery plan

Sized as lookout increments rather than as a standalone product. Phases 1–4 are a
shippable `topology-drift` source; Phase 6 is the `compute-class` source and is
independent of 5.

| Phase | Scope | Exit criteria |
|---|---|---|
| **0 — Spikes** (0.5 wk) | S9 and S10 **done**; S11 next, then S1–S8 in parallel | Package boundaries fixed (done); transform registry written; OTLP scope known |
| **1 — Engine** (2 wks) | `pkg/leeway`: intent model, eligibility, apportionment, scoring. No informers, no source | Property tests green; zero client-go imports, enforced by test |
| **2 — Source skeleton** (2 wks) | `topology-drift` source: delta application, indexes, domain inventory, verifier, metrics on the OTEL API. Shared-transform change per S9 | Counters provably correct under property tests at 10k pods; existing source tests still green |
| **3 — Intent inference** (2 wks) | TSC, affinity/anti-affinity, node selectors, tolerations, volume pinning, cluster defaults, precedence | Correct intent on the scenario corpus; false-positive corpus clean |
| **4 — Findings** (2 wks) | State machine, dwell, hysteresis, tiers A/B, transient suppression, severity routing, `pkg/store` persistence | Restart tests pass; zone-outage scenario yields one finding, not four hundred |
| **5 — Baselines** (2 wks) | EWMA/EWMAD, freeze-while-firing, maturity gates, invalidation, Tier C | Tier C detects injected drift in soak without firing on the FP corpus |
| **6 — Preference ranks** (2 wks) | `compute-class` source: dynamic ComputeClass informer, configurable extractors, rank resolution with cross-check, time-weighted pod-seconds, attribution SLIs | Rank shares match a hand-audited sample of a live GKE cluster; unmatched and disagreement rates 0 |
| **7 — Nodes** (1.5 wks) | Node-group subjects, capacity weighting, domain-outage detection | Node-pool imbalance detected and attributed |
| **8 — Hardening** (2 wks) | Cause attribution consuming sibling sources, cardinality controls, OTLP hardening, `cmd/leeway`, docs, dashboards | Scale + soak met on the padded kwok harness; `cmd/leeway` built and smoke-tested in CI; process survives a black-holed OTLP endpoint for 24 h with flat RSS |

Roughly 14 weeks, against ~19 for the standalone version — the difference is almost
entirely the plumbing lookout already owns.

**Phase 0 is not optional and not a formality.** S9 and S10 have already earned it:
between them they caught a field selector that would have silently disabled eviction
detection, and moved half the §8.2 state machine from "write it" to "implement one
interface". Both were a morning's reading. What is left carries the same shape of
risk — **S1 gates the Phase 6 data model**, and discovering its answer at week 12
means rebuilding the transition model rather than adjusting it. S8 is a softer gate
on Phase 8: without it, the scale numbers are measured against undersized objects
and mean nothing.

Phases 1–4 are a shippable increment at around week 9: declared and inferred intent,
findings, restart-safe, no baselines and no compute classes.

---

## 15. Open questions

1. ~~Fleet size~~ — **resolved.** 150–200k pods at the high end, 500 pods/sec
   sustained. NFR-1 is set from these; §6.6.1 shows the marginal cost inside the
   sentinel is ~0.013 core. Residual: namespace count (**S6**).
2. ~~GKE compute class label keys~~ — **resolved** against `simian-test` on
   2026-09-11. GKE records the achieved priority in the `ccc_priority_index` node
   annotation, so §7.7.2 reads ground truth and demotes inference to fallback plus
   cross-check. Undocumented and unguaranteed; risk accepted, mitigated by the
   fallback path and the attribution SLIs. Residual: **S1**, **S2**, **S3**.
3. **Does lookout's DESIGN §6.2 scale paragraph get edited, or does this document
   lower its target?** The two currently disagree in the same repo (§2.5). They are
   reconcilable — the numbers agree once you separate memory-in-pods from
   CPU-in-events — but one of them has to change, and it is a maintainer call, not
   a design one.
4. **Should `topology-drift` be enabled by default?** §8.3 routes Tier C to metrics
   only, so the default deployment adds roughly zero agent sessions, which makes
   default-on defensible. But it also adds the §6.1 transform change to every
   deployment. Leaning default-on after Phase 4, default-off before.
5. **Is fallback depth per-workload or per-class?** §7.7 aggregates per axis by
   default. If two Deployments share a class but only one is falling back, per-axis
   metrics hide it — but per-subject labels multiply cardinality.
6. **Cardinality budget** (**S5**). Per §6.6 this is the first ceiling we hit, and
   §8.4 push export makes it bind harder.
7. **Cluster default constraints** (**S4**). On managed GKE the answer is probably
   "unreadable" — is the fallback a documented manual sync, or a widened Tier A
   threshold?
8. **Karpenter consolidation.** Consolidation deliberately packs pods and produces
   drift by design. Default Karpenter-managed pools to `Ignore` on zone, or alert with
   consolidation as an annotated suspected cause? Leaning the latter — intentional and
   safe are not the same thing.
9. **Descheduler integration.** Emitting a
   `RemovePodsViolatingTopologySpreadConstraint` hint is a natural v2 and sits
   uncomfortably against lookout's read-only posture. Worth reserving surface now?
10. **Region-level topology.** Multi-region clusters are rare but the model supports
    them. In scope for the target fleet, or is zone the only axis that matters?
