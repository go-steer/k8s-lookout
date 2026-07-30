---
title: Signals & fingerprints
description: The frozen v1 wire schema, severity classes, dedup families, and the incident-class fingerprint that makes fleet rollup a join instead of a parsing project.
sidebar:
  order: 3
---

Everything `lookout` tells you — a finding printed by a scan, an incident
the sentinel opens — arrives in one shape: a **Signal**. This page
explains that shape, the **fingerprint** that names an incident's
*class* so the same problem seen twice is counted once, and why the
schema is frozen as v1: other tools parse it, so it changes only by
agreement. The complete kind catalog is generated from the same ledger
the freeze tests pin:
[Reference → Signal kinds](/reference/signal-kinds/); the normative
contract is
[`docs/signal-schema-v1.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/signal-schema-v1.md).

## The payload

The inject payload carries the incident's identity (`kind`, `reason`,
`namespace`, `kind_of_object`, `name`, `uid`), its history (`count`,
`first_seen`, `last_seen`), fleet join dimensions (`cluster`, `project`,
`zone`), the class key (`fingerprint`), and optional attachments: a
`forecast` (`eta`, `confidence_basis`) on trend signals, an
`enrichment.bundle` on warmed sessions, and a `quota_increase_draft` on
quota forecasts. Kinds are namespaced by source — `rollout.stall`, `workload.job_failed`,
`saturation.forecast`, `capacity.stockout`, `quota.forecast`, `token.burn` —
plus cross-cutting kinds like `resolved`, `storm`, and `watchboard.digest`.

One freeze sits inside the freeze: the original `k8s-event` /
`k8s-event-followup` pair stays byte-identical to the pre-lookout
`k8s-event-watcher` for playbook back-compat, and never gains the newer
identity fields. Every other kind carries them.

## The fingerprint

The incident-class key:

```
"sha256:" + hex(sha256(kind ∥ NUL ∥ reason-class ∥ NUL ∥ object-class ∥ NUL ∥ zone))
```

- `reason-class` is **canonicalized** — `ErrImagePull` and
  `ImagePullBackOff` hash identically, mirroring the dedup family collapse.
- `object-class` is the *kind* of the affected object (`Pod`, `Node`,
  `NodeGroup`), never its name or UID.
- `zone` is inside the hash; `cluster` is not. Zone-scoped causes —
  stockouts, zonal outages — are exactly what fleet rollup must group: the
  same stockout hitting 40 clusters in a zone carries 40 identical
  fingerprints, with `cluster`/`project` riding alongside as join
  dimensions.

That makes the fleet-tier rollup **a join, not a parse**. From the M5
multi-cluster drill — two sentinel instances, one staged zonal stockout,
grouped by `fingerprint` alone:

```
fleet group sha256:0aad7654…5034c → clusters [prod-east prod-west]   (capacity.stockout)
fleet group sha256:95fa2f13…fbd18 → clusters [prod-east prod-west]   (capacity.quota_blocked)
```

## One schema for push and pull

Read-path findings are Signals too, with `source: "scan"` instead of
`"sentinel"`. A point-in-time scan observes a *symptom*, so scan findings
fingerprint under the reactive kind — the same class key the sentinel would
stamp. `lookout health` and `lookout triage delta` emit `fingerprint=` on
every symptom-class finding, which is what lets:

- `health` merge "the sentinel paged on this 20 minutes ago" and "the scan
  still sees it" into one finding instead of two;
- the [triage-status join](/concepts/closed-loop/) recognize a finding an
  agent already diagnosed;
- fleets avoid double-counting a symptom reported by both paths.

## Severity classes and routing

Every signal kind has a default severity (`critical` / `warning` / `info`),
overridable per deployment with `--severity=kind=level`. Severity is a
*routing* decision:

| Severity | Default routing |
| --- | --- |
| `critical` | its own per-incident session, enrichment attached |
| `warning` | batched into the shared watchboard session as a rolling digest |
| `info` | stored only (with `--store`); surfaced by read-path queries |

Leading indicators must not each open a page-priority session — that is the
entire reason routing exists. The watchboard and its rotation are covered in
[The closed loop](/concepts/closed-loop/).

## Dedup families

Dedup keys on `(uid, canonical reason)`, and *families* collapse the same
underlying incident observed from different angles into one session:

- leading ↔ reactive: `objectstate.node_notready` and the `NodeNotReady`
  Event; `objectstate.restart_burst` and `CrashLoopBackOff`;
- cross-source capacity joins: `capacity.pending` / `capacity.pending-aged`
  and the `FailedScheduling` Event; `quota.forecast` and
  `capacity.quota_blocked` collapse on a `quota:<NAME>/<SCOPE>` key — the
  forecast that predicted exhaustion and the autoscaler failure that
  confirmed it are one incident, not two alerts a human joins.

## Evolution

Additions are v1-additive (new omitempty field at the end of a struct, new
kinds extending the inventory, ledger and doc updated in the same change).
Removing or renaming a field, or touching the fingerprint recipe, is a v2
negotiation with the fleet consumer — a unilateral change would silently
split every fleet-wide rollup into disjoint halves during a rolling upgrade.
