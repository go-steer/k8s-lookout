# Signal Schema v1 — the frozen fleet-rollup contract

Status: **FROZEN** (M5, 2026-07-26). This is the fleet-rollup wire
contract of DESIGN.md §8, closed out per §14 M5 ("fingerprint schema
finalized for fleet rollup") and the standing session decision: frozen
as v1 **here**, with contract tests in this repository as the ledger —
no external filing. The fleet-level consumer takes these payloads
**as-is**; any change that removes or renames a frozen field, or
alters the fingerprint recipe, is a **v2 negotiation with fleet
consumers**, not a patch.
Additions are allowed within v1 (additive-only), but must land in the
same change as their ledger and doc updates.

Machine-readable ledger: `pkg/inject/schema_freeze_test.go`
(`TestSchemaV1_FieldSetsFrozen` pins every struct's ordered json field
list; `TestSchemaV1_KindInventory` pins the kind inventory;
`TestSchemaV1_RoundTrip` proves lossless re-serialization). The
fingerprint definition is pinned by the cross-cluster vectors in
`pkg/engine/fingerprint_test.go` — a failing vector is a breaking
change, never a test to update.

## The fingerprint

The §8 incident-class key, `pkg/engine.Fingerprint`:

```
"sha256:" + hex(sha256(kind ∥ NUL ∥ reason-class ∥ NUL ∥ object-class ∥ NUL ∥ zone))
```

- `kind` — the signal kind (below); `"storm"` for storm aggregates
  (with the blast-radius ancestor's kind as object-class).
- `reason-class` — the **canonicalized** reason: `ErrImagePull` and
  `ImagePullBackOff` hash identically, mirroring the dedup family
  collapse. The push path canonicalizes **message-aware**
  (`engine.CanonicalReasonForEvent`): kubelet's generic
  `BackOff`/`Failed` event reasons are classified by their message —
  pull-shaped messages land in `ImagePullBackOff`, the rest in
  `CrashLoopBackOff` (`BackOff`) or stay `Failed`. Messageless callers
  (`engine.CanonicalReason`) keep the reason-only mapping.
- `object-class` — the KIND of the affected object (`Pod`, `Node`,
  `NodeGroup`), never its name or UID.
- `zone` — the failure domain, empty when unknown. Zone is inside the
  hash (cluster is NOT) because zone-scoped causes — stockouts, zonal
  outages — are exactly what fleet rollup must group: the same
  stockout hitting 40 clusters in a zone carries 40 identical
  fingerprints, and `cluster`/`project` ride alongside as join
  dimensions.

**The fleet rollup is a join, not a parse**: group by `fingerprint`,
fan out by `cluster`/`project`/`zone`. Demonstrated as a test
assertion in `internal/watch/m5_corpus_rollup_test.go`
(`TestDrill_MultiClusterRollup_Stockout`: two sentinel instances, one
staged zonal stockout, one fleet-level group).

### Scan-source mapping (push/pull dedup)

Read-path findings are Signals with `source: "scan"` (§8: one schema
for push and pull). A point-in-time scan observes a **symptom**, so
scan findings always fingerprint under the frozen reactive kind —
`engine.ScanFingerprint(reason, objectClass, zone)` ≡
`Fingerprint("k8s-event", CanonicalReason(reason), objectClass, zone)`
— never under a scan-local or source-namespaced kind. `lookout
health` and `lookout triage delta` stamp this on every symptom-class
finding (`fingerprint=` envelope field, omitted where a finding has
no incident-class identity), and the §9.4 triage-status join has used
this exact recipe since M4. Parity is pinned by
`TestFingerprintParity_PushAndScan`.

A consequence worth stating, because it decides where a new check's
`kind=` gets documented: a scan finding's own `kind` value
(`top.unlimited`, `top.unrequested`, `health.*`, …) is a **check-local
label**, not a v1 signal kind, and is deliberately absent from the
inventory below. The inventory enumerates the wire kinds that carry a
`pkg/inject` payload struct and participate in fingerprinting; it is
pinned at its exact size by `TestSchemaV1_KindInventory`. Scan kinds
are documented where they are produced — the emitting command's
`Output` field glossary, rendered into `--help`, the MCP tool schema,
and the generated reference page — and adding one is additive there,
not here.

### Posture-source mapping (the `audit` group, #182)

An `audit` posture finding is not the pull-path view of a symptom the
sentinel could also push — there is no k8s event for "this Deployment
has no PodDisruptionBudget" — so it does not borrow the reactive
recipe. `engine.PostureFingerprint(kind, reason, objectClass)` ≡
`Fingerprint(kind, reason, objectClass, "")`, differing from
`ScanFingerprint` in exactly three ways, each deliberate:

- **`kind` is the detector's own** (`audit.no_pdb`), not
  `"k8s-event"`. For posture the check slug IS the incident class;
  there is nothing on the push path to dedupe against.
- **`reason` is NOT canonicalized.** `CanonicalReason` maps k8s event
  reasons onto their families. A posture reason is not an event
  reason, so running it through that table can only mis-map — a
  posture reason colliding with a table key would be silently
  rewritten into someone else's class — and can never merge two
  classes that should merge.
- **`zone` is empty.** Posture is a property of a spec, identical in
  every zone the workload lands in; stamping a zone would fragment the
  fleet rollup into one class per zone. Instance identity is the
  subject key's job (`docs/audit-ingestion-contract.md` §4), not the
  fingerprint's.

This is an **addition**, not an amendment: `ScanFingerprint` and every
pinned vector above are untouched, because they are the recipe every
open §9.4 triage-status record was joined under. Posture vectors are
pinned alongside them in `pkg/engine/fingerprint_test.go`.

`objectClass` earns its place in the recipe: the same claim about a
Deployment and about a StatefulSet are different classes, because the
remedies differ enough that a fleet rollup merging them would be
reporting a number nobody can act on. The kinds shipped so far, from
`audit workloads` (#190), `audit exemptions` (#234),
`audit hardening` (#183) and `audit netpol` (#185):

| Kind | Reason | Object classes |
| --- | --- | --- |
| `audit.no_pdb` | `NoPodDisruptionBudget` | `Deployment`, `StatefulSet` |
| `audit.single_replica` | `SingleReplica` | `Deployment`, `StatefulSet` |
| `audit.no_spread` | `NoTopologySpread` | `Deployment`, `StatefulSet` |
| `audit.no_readiness_probe` | `NoReadinessProbe` | `Deployment`, `StatefulSet`, `DaemonSet` |
| `audit.no_liveness_probe` | `NoLivenessProbe` | `Deployment`, `StatefulSet`, `DaemonSet` |
| `audit.rigid_scheduling` | `NoEligibleNodes`, `SingleEligibleNode`, `FewerEligibleNodesThanReplicas` | `Deployment`, `StatefulSet` |
| `audit.hpa_cannot_scale` | `HPAMinEqualsMax`, `HPATargetMissingRequests`, `HPATargetMissing` | `HorizontalPodAutoscaler` |
| `audit.exemption_expired` | `ExemptionExpired` | — (the subject is a file entry) |
| `audit.exemption_expiring` | `ExemptionExpiring` | — |
| `audit.privileged_container` | `PrivilegedContainer`, `DangerousCapability` | `Deployment`, `StatefulSet`, `DaemonSet`, `CronJob`, `Job`, `Pod` |
| `audit.host_namespace` | `HostNetwork`, `HostPID`, `HostIPC` | as above |
| `audit.hostpath_mount` | `WritableHostPath`, `ReadOnlyHostPath` | as above |
| `audit.default_sa_automount` | `DefaultServiceAccountAutomount` | `ServiceAccount` |
| `audit.podsecurity_gaps` | `NoPodSecurityEnforce`, `PodSecurityEnforcePrivileged` | `Namespace` |
| `audit.netpol_missing` | `NoIngressPolicies`, `NoEgressPolicies`, `IngressPoliciesSelectNothing`, `EgressPoliciesSelectNothing` | `Namespace` |
| `audit.netpol_missing` | `UnselectedIngress`, `UnselectedEgress` | `Deployment`, `StatefulSet`, `DaemonSet`, `CronJob`, `Job`, `Pod` |

Several of these carry more than one reason, which is the recipe
working as intended rather than an exception to it. `audit.rigid_scheduling`,
`audit.hpa_cannot_scale` and `audit.host_namespace` each name one
condition with several distinct causes, and the cause is what an
operator acts on: "no node satisfies the constraint" and "exactly one
does" are the same kind and different classes, so they fingerprint
apart and a rollup counts them apart. Splitting them into one kind per
reason instead would have said the same thing with three times the
entries in the consumer's kind table.

Not every `audit` kind's subject is the workload. `audit.hpa_cannot_scale`
sets `objectClass` to `HorizontalPodAutoscaler` and `name` to the
autoscaler's, `audit.default_sa_automount` addresses the
`ServiceAccount`, and `audit.podsecurity_gaps` addresses the
`Namespace` — in each case because that is the object an operator
edits, even though the finding comes out of the same pass that judges
the workloads behind it.

`audit.netpol_missing` takes that furthest: it has two rows above
because its subject depends on which reason fired. Where nothing in a
namespace is covered there is one decision and one remedy, so the
`Namespace` is the subject; where the namespace is policed and one
workload fell out of the selectors, the workload is, because its
labels are the defect. The kind is shared deliberately — a consumer
counting "how much of the fleet is unpoliced" wants one number, and
the `objectClass` in the fingerprint keeps the two shapes apart in the
rollup without a second kind in the table.

The pod-template kinds share one object-class set on purpose: the six
workload kinds that carry a pod template are all judged by reading
that template, so the same claim about a `DaemonSet` and about a bare
`Pod` differ only in what an operator has to edit to fix it — which is
exactly what `objectClass` is for. A `Job` or `Pod` with an
`ownerReference` is not judged in its own right; the finding is
reported once, against the owner that generated it.

Like every other check-local `kind`, these are documented where they
are produced — the emitting command's output glossary — and are
deliberately absent from the wire-kind inventory below. The table here
exists only to pin the fingerprint INPUTS, which are a cross-cluster
contract in a way a glossary entry is not.

## Kind inventory (v1: 48 kinds — 32 at the M5 freeze, +2 `workload.*` #129, +3 `notification.*` #130, +3 `ingress.*` #135, +1 `family.member` #132, +2 `objectstate.*` #134, +1 `capacity.cluster_forecast` #131, +2 `autoscaling.*` #131, +2 `gateway.*` #168, additive-only)

Cross-cutting kinds, each with its own schema-stable struct
(`pkg/inject/payload.go`):

| Kind | Struct | Role |
| --- | --- | --- |
| `k8s-event`, `k8s-event-followup` | `Payload` | frozen M0 reactive pair |
| `resolved`, `resolved.reverted` | `ResolvedPayload` | §7.4 outcome records — the §9.3 ground-truth labels |
| `storm` | `StormPayload` | §7.5 aggregate incident |
| `storm.member`, `storm.member_superseded` | `StormMemberPayload` | membership / supersede pointer |
| `storm.update` | `StormUpdatePayload` | size refresh (latest wins) |
| `watchboard.digest` | `WatchboardDigestPayload` | §7.7 warning batch |
| `watchboard.rotated` | `WatchboardRotatedPayload` | §15 Q2 rotation pointer |
| `triage.regressed` | `TriageRegressedPayload` | §9.4 regression evidence |
| `family.member` | `FamilyMemberPayload` | §10.3 cross-source join notice (added post-M5, #132): a different source family attached to the session's incident — max one per source family per incident per window; never fanned out to storm sessions (§7.5). Carries the joining signal's identity, the canonical `family`, the `opened_by` source family, and `design_ref` |

Source-namespaced kinds — all ride `Payload`: `objectstate.
node_notready|node_flapping|progress_deadline|endpoints_empty|
pdb_gridlocked|restart_burst|node_pressure|eviction_burst`
(`node_pressure`/`eviction_burst` added post-M5, #134), `rollout.stall`, `workload.
job_failed|cron_missed` (added post-M5, #129 — kinds are
append-only), `autoscaling.hpa_pinned|hpa_metrics_dead` (added
post-M5, #131), `saturation.forecast`,
`degradation.capacity|probe_flap`, `expiry.warning`, `capacity.
pending|scaleup|scaledown|scaleup_gap|stockout|quota_blocked|
ip_exhausted|pending-aged|cluster_forecast` (`cluster_forecast`
added post-M5, #131), `ingress.
sync_failed|translate_failed|neg_failed` (added post-M5, #135 —
kinds are append-only), `gateway.
programming_failed|route_rejected` (added post-M5, #168 — the
Gateway-API sibling of `ingress.*`: sustained `Programmed`/`Accepted`/
`ResolvedRefs`=False status conditions on `Gateway`/`HTTPRoute`),
`quota.forecast`,
`notification.upgrade|upgrade_available|security_bulletin` (added
post-M5, #130), `token.burn`.

## Frozen field sets

`Payload` (the §8 superset, M5-final + 2026-07-27 amendment): `kind`,
`reason`, `namespace`, `kind_of_object`, `name`, `container`\*,
`uid`, `message`, `count`, `first_seen`, `last_seen`, `cluster`,
`project`\*, `zone`\*, `source`\*, `severity`\*, `fingerprint`\*,
`context` (`controller_ref`\*, `node`\*, `labels`\*), `type` (the
k8s `Event.Type`, `Normal`/`Warning`; empty for synthetic source
signals — NOT omitempty, positioned after `context` to match
kube-agents' watcher wire), `enrichment`\* (`bundle`), `forecast`\*
(`eta`, `confidence_basis`), `quota_increase_draft`\* (`quota_id`,
`region`, `unit`\*, `current_usage`, `current_limit`,
`suggested_limit`, `slope_per_day`, `justification`). Fields marked
\* are omitempty.

**The M0 freeze inside the freeze:** on `kind=k8s-event` /
`k8s-event-followup` the dispatcher never stamps
`project`/`zone`/`source`/`severity`/`fingerprint` — those payloads
stay byte-identical to the original watcher (playbook back-compat;
wire pins in `internal/watch`), re-baselined ONCE on 2026-07-27 to add
`type` (see §Amendments). Every OTHER kind carries the full §8
identity. Consumers needing the k8s-event class key compute it from
the frozen fields (`ScanFingerprint`) or take it from the incident's
outcome record, which always carries `fingerprint`.

`ResolvedPayload`: `kind`, `reason` (canonical reason-class),
`namespace`, `kind_of_object`, `name`, `container`\*, `uid`,
`fingerprint`, `cluster`, `first_seen`, `resolved_at`,
`cleared_after`, `observed_stable_for`, `resolution`
(`recovered`|`object_deleted` — its own field, never prose),
`reverted_after`\*, `context`. Durations are Go `time.Duration`
strings (`"2m30s"`): fixed grammar, parseable without NLP (§9.3).

Storm / watchboard / triage.regressed / family.member field sets: see
the ledger in `schema_freeze_test.go` — reproduced there
field-for-field with the same ordering the wire emits.

## §9.3 harvestability

Outcome records are schema-stable structured injects, never prose, so
a harvester extracts labeled trajectories (symptom → diagnosis →
action → externally verified outcome) from a captured eventlog by
pure schema walks. Reference implementation: `pkg/corpus` (CLI:
`go run ./dev/tools/harvest-corpus`), validated end-to-end against
the real dispatcher in `TestDrill_CorpusHarvest_EndToEnd`.

## Amendments (dated pre-consumer corrections)

Gari's standing policy for this window: there are **zero deployed
consumers** of lookout today (only kube-agents' in-tree watcher fork
and the core-agent demos), so frozen pins and vectors may be amended
**cleanly, once**, with no migration machinery or compat shims — each
amendment recorded here with its date. This section closes when the
first external consumer deploys.

- **2026-07-27 — reason-class table corrected pre-any-consumer:**
  pull-related `BackOff`/`Failed` events classify as
  `ImagePullBackOff` (message-aware `engine.CanonicalReasonForEvent`,
  adopted from kube-agents' watcher #406). Consequence: dedup keys,
  session bindings, store `canonical_reason` rows, and fingerprints
  computed for pull-shaped `BackOff`/`Failed` **events** change class
  from `CrashLoopBackOff`/`Failed` to `ImagePullBackOff`. The
  `Fingerprint` recipe itself (hash, separator, field order) is
  untouched and the pinned vectors in `fingerprint_test.go` stand
  unchanged — only the reason-class INPUT for those event shapes was
  corrected. Messageless `CanonicalReason` keeps its old mapping.
- **2026-07-27 — `type` field added; M0 byte pins re-baselined:** the
  frozen M0 `k8s-event`/`k8s-event-followup` payloads (and every
  `Payload`-shaped kind) gained `type` — the k8s `Event.Type` — after
  `context`, not omitempty, matching kube-agents' watcher wire
  exactly. The M0 "byte-identical" pins in `internal/watch` and the
  webhook-sink wire pins were re-pinned once to the new frozen truth.
  Scan-side `emit.Finding` deliberately does NOT gain `type`: scan
  findings are point-in-time observations of object state, not
  Events — there is no `Event.Type` to report, and inventing one from
  severity would be a lie.

## Evolution

- **Additive (v1.x):** new field at the END of a struct, omitempty,
  same change updates `frozenFields` + this doc. New kinds extend the
  inventory (source-namespaced or design-doc'd cross-cutting).
- **Breaking (v2):** removing/renaming a field, changing a json tag,
  reordering, or touching the fingerprint recipe (encoding,
  separator, field order, field set). Requires coordination with
  fleet consumers and a coordinated fleet upgrade — a unilateral change silently
  splits every fleet-wide rollup into disjoint halves during a
  rolling upgrade.

**Envelope vs. wire.** The §4.2 finding envelope
(`emit.EnvelopeFields`) and the §8 wire `Payload` are different field
sets that happen to share most of their names, and they evolve
independently. Two examples, in both directions:

- `type` (2026-07-27) was added to `Payload` and deliberately NOT to
  `emit.Finding` — a scan observes object state, and there is no
  `Event.Type` to report.
- `exempt_reason` / `exempt_expires` (#234) were added to
  `emit.Finding` and deliberately NOT to `Payload`. They are stamped
  by the Writer when an operator's `--exemptions` file covers a
  finding; they annotate a REPORT, they do not describe the cluster.
  Nothing about the pinned field sets, byte pins, or fingerprint
  recipe changes, and a consumer that ignores them sees exactly the
  stream it saw before. Both are empty and omitted unless
  `--exemptions` was passed.
