# Signal Schema v1 — the frozen AX contract

Status: **FROZEN** (M5, 2026-07-26). This is the fleet-rollup wire
contract of DESIGN.md §8, closed out per §14 M5 ("fingerprint schema
finalized with AX") and the standing session decision: frozen as v1
**here**, with contract tests in this repository as the ledger — no
filing in the ax repo. AX consumes these payloads **as-is**; any
change that removes or renames a frozen field, or alters the
fingerprint recipe, is a **v2 negotiation with AX**, not a patch.
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
- `reason-class` — the **canonicalized** reason
  (`engine.CanonicalReason`): `ErrImagePull` and `ImagePullBackOff`
  hash identically, mirroring the dedup family collapse.
- `object-class` — the KIND of the affected object (`Pod`, `Node`,
  `NodeGroup`), never its name or UID.
- `zone` — the failure domain, empty when unknown. Zone is inside the
  hash (cluster is NOT) because zone-scoped causes — stockouts, zonal
  outages — are exactly what fleet rollup must group: the same
  stockout hitting 40 clusters in a zone carries 40 identical
  fingerprints, and `cluster`/`project` ride alongside as join
  dimensions.

**The AX rollup is a join, not a parse**: group by `fingerprint`,
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

## Kind inventory (v1: 32 kinds)

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

Source-namespaced kinds — all ride `Payload`: `objectstate.
node_notready|node_flapping|progress_deadline|endpoints_empty|
pdb_gridlocked|restart_burst`, `rollout.stall`, `saturation.forecast`,
`degradation.capacity|probe_flap`, `expiry.warning`, `capacity.
pending|scaleup|scaledown|scaleup_gap|stockout|quota_blocked|
ip_exhausted|pending-aged`, `quota.forecast`, `token.burn`.

## Frozen field sets

`Payload` (the §8 superset, M5-final): `kind`, `reason`, `namespace`,
`kind_of_object`, `name`, `container`\*, `uid`, `message`, `count`,
`first_seen`, `last_seen`, `cluster`, `project`\*, `zone`\*,
`source`\*, `severity`\*, `fingerprint`\*, `context`
(`controller_ref`\*, `node`\*, `labels`\*), `enrichment`\*
(`bundle`), `forecast`\* (`eta`, `confidence_basis`),
`quota_increase_draft`\* (`quota_id`, `region`, `unit`\*,
`current_usage`, `current_limit`, `suggested_limit`, `slope_per_day`,
`justification`). Fields marked \* are omitempty.

**The M0 freeze inside the freeze:** on `kind=k8s-event` /
`k8s-event-followup` the dispatcher never stamps
`project`/`zone`/`source`/`severity`/`fingerprint` — those payloads
stay byte-identical to the original watcher (playbook back-compat;
wire pins in `internal/watch`). Every OTHER kind carries the full §8
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

Storm / watchboard / triage.regressed field sets: see the ledger in
`schema_freeze_test.go` — reproduced there field-for-field with the
same ordering the wire emits.

## §9.3 harvestability

Outcome records are schema-stable structured injects, never prose, so
a harvester extracts labeled trajectories (symptom → diagnosis →
action → externally verified outcome) from a captured eventlog by
pure schema walks. Reference implementation: `pkg/corpus` (CLI:
`go run ./dev/tools/harvest-corpus`), validated end-to-end against
the real dispatcher in `TestDrill_CorpusHarvest_EndToEnd`.

## Evolution

- **Additive (v1.x):** new field at the END of a struct, omitempty,
  same change updates `frozenFields` + this doc. New kinds extend the
  inventory (source-namespaced or design-doc'd cross-cutting).
- **Breaking (v2):** removing/renaming a field, changing a json tag,
  reordering, or touching the fingerprint recipe (encoding,
  separator, field order, field set). Requires negotiation with AX
  and a coordinated fleet upgrade — a unilateral change silently
  splits every fleet-wide rollup into disjoint halves during a
  rolling upgrade.
