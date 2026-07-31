---
title: A node just died
description: Storm correlation in practice — 33 affected objects, one storm session, and the full member/update/resolved lifecycle. Real captured drill output.
sidebar:
  order: 4
---

**The problem:** a worker node goes dark. Thirty-plus pods are suddenly
NotReady, and a naive per-incident pipeline opens thirty-plus sessions —
none of which mentions the actual cause.

With storm correlation on (`--storm=auto`, the default, resolves on
when the graph grants are present), the sentinel groups incidents sharing a **blast-radius
key** — the nearest common ancestor in the topology graph — into one
`kind=storm` session. All output below is from a live validation drill,
abridged: 30 victim pods pinned to `kl-m2-worker2`, then
`docker stop kl-m2-worker2` at 00:50:38 — a hard kubelet death.

## Detection and formation

```txt
00:51:18 fire node_notready pod=/kl-m2-worker2 → sid=stub-sess-0005 (mode=per-incident)
00:51:18 fire NodeNotReady pod=kube-system/kube-proxy-pkx94 → sid=stub-sess-0006 (mode=per-incident)
00:51:18 enrich storm Node kl-m2-worker2: 5273B (outcome=ok, sections=1, truncated=false, errors=0)
00:51:18 storm formed on Node kl-m2-worker2: 3 incidents across 2 namespace(s) → sid=stub-sess-0007 (mode=per-incident)
00:51:18 storm attach NodeNotReady stormlab/victim-… (sid=stub-sess-0007, members=4)
   ⋮  (one attach line per member)
00:51:20 storm attach NodeNotReady stormlab/victim-7d47b46468-mnf6b → Node kl-m2-worker2 (sid=stub-sess-0007, members=33)
```

Forty seconds from kubelet death to detection. The leading
`objectstate.node_notready` transition opened the node incident *before*
the reactive `NodeNotReady` Event arrived (which then joined its dedup
family); the storm formed the same second; all 33 members — 30 victims
plus the node's own `kube-proxy` and CNI pods, which are real storm
members — attached within two seconds.

## The session ledger: 3, not 33

The complete session/inject ledger for the burst window, from the captured
daemon side:

```txt
SESSION-CREATE ×3   (stub-sess-0005, -0006, -0007)
INJECT sid=stub-sess-0005 kind=objectstate.node_notready   (the seed incident)
INJECT sid=stub-sess-0006 kind=k8s-event                   (2nd pre-storm arrival)
INJECT sid=stub-sess-0007 kind=storm                       (THE storm session)
INJECT sid=stub-sess-0007 kind=storm.member          ×30
INJECT sid=stub-sess-0005 kind=storm.member_superseded
INJECT sid=stub-sess-0006 kind=storm.member_superseded
```

Zero of the 30 victim pods opened a session. The first two arrivals fired
per-incident before the formation threshold (`--storm-min=3`) was
reachable — inherent to any burst — and were immediately superseded:
`storm.member_superseded` pointers landed in their sessions and their
dedup bindings were rebound, so all their followups and outcomes route to
the storm.

The `kind=storm` inject itself (abridged) names the ancestor, the spread,
and representative incidents, and carries a radius-only enrichment bundle —
the blast map of the node from the live topology snapshot:

```json
{"kind":"storm","fingerprint":"sha256:48bb2e3a…","severity":"critical",
 "cluster":"kl-m2","ancestor_kind":"Node","ancestor_name":"kl-m2-worker2",
 "reason":"NodeNotReady",
 "message":"Node kl-m2-worker2: 3 incidents across 2 namespace(s) share this blast-radius key; 3 representative incident(s) attached; member sessions are suppressed and route here",
 "affected_count":3,"namespaces_count":2, "…":"…"}
```

`affected_count` is the formation-time number; as membership grows, the
sentinel injects schema-stable `kind=storm.update` refreshes
(`affected_count`, `namespaces_count`, `new_members_since_last`) so the
headline size is readable without folding the whole session. (That
followup kind exists *because* this drill showed the formation payload
underselling the final blast radius.)

## Recovery: the storm collapses the bookkeeping too

`docker start kl-m2-worker2` at 00:52. As victims returned Ready and held
stable, 32 member `kind=resolved` records (`resolution=recovered`,
`cleared_after≈2m50s`) flowed into the storm session — and once every
member *including the node's own incident* clears, the storm's aggregate
`resolved` fires. One session tells the whole story: cause, spread,
representative details, and verified recovery.

Working such a session as a human, the reads are the usual ones:

```sh
lookout triage radius Node//kl-m2-worker2      # the live blast map
lookout triage delta --only=pods,nodes         # what is still abnormal now
lookout stab drain                             # before maintenance: what will block a drain
```

## As an agent skill

The incident-investigation decision tree an agent applies inside a storm
session — radius for impact, delta for current state, bundle for any
member worth its own dig — is
[`skills/k8s-triage`](https://github.com/go-steer/k8s-lookout/tree/main/skills/k8s-triage).
