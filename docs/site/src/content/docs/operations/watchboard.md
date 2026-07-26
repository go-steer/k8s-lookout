---
title: The watchboard
description: How warning-class signals batch into rolling digests, the size-based rotation lifecycle, and how lineage and incident bindings survive rotation.
sidebar:
  order: 2
---

Leading indicators must not each open a page-priority session. In
per-incident mode, severity routing sends each signal class its own way:
`critical` opens a per-incident session with enrichment, `warning`
batches into the shared **watchboard** session as a rolling digest, and
`info` is stored only (with `--store`; counted and dropped without).
In `--mode=shared` the watchboard is disabled — everything routes to
`--target-session`.

## Digest cadence

Warnings buffer until either `--watchboard-batch` of them accumulate
(default 5) or the oldest buffered warning reaches `--watchboard-flush`
in age (default 60s) — whichever comes first. The board session is
created lazily on the first flush. Captured live in the M2 exit check:

```
00:58:45 watchboard: buffered objectstate.endpoints_empty weblab/web (severity=warning, buffered=1/5)
00:59:47 watchboard: digest 1 entry(ies) → sid=stub-sess-0009 (generation=1, injects=1/200, mode=per-incident)
```

And the digest inject itself — one schema-stable record per flush, each
entry carrying its own fingerprint and object coordinates:

```json
{"kind":"watchboard.digest","cluster":"kl-m2","board_generation":1,"sequence":1,
 "window_start":"2026-07-25T00:58:45.68219189Z","window_end":"2026-07-25T00:59:47.098792925Z",
 "entries":[{"kind":"objectstate.endpoints_empty","fingerprint":"sha256:638f0f4d…",
   "reason":"endpoints_empty","namespace":"weblab","kind_of_object":"Service",
   "name":"web","uid":"052e28a1-…","count":1, …}]}
```

## Rotation lifecycle

A shared session would otherwise grow without bound, so the board
rotates **by size, not by calendar**: after `--watchboard-rotate` digest
injects (default 200), the next flush opens a successor session. The cap
bounds the agent's cost of consuming the board no matter how noisy the
cluster is, while a quiet cluster keeps one session for months. The
mechanics:

1. `POST /sessions` creates the successor (same `--owner`
   asserted-caller as every sentinel session).
2. The final inject into the old session is the lineage record —
   `kind=watchboard.rotated` with `successor_session_id`,
   `injects_count`, `rotated_at`, `cluster`, and `board_generation` — so
   anyone reading the old session can follow the pointer forward.
3. The pending digest flushes into the successor; counters restart at
   `board_generation+1`, `sequence=1`.

Failure posture: if creating the successor fails, rotation is deferred —
the digest flushes into the over-threshold session and rotation retries
on the next flush. Warnings are never dropped to enforce a size cap.

## Lineage and finding the board

The daemon's session API has no name parameter, so watchboard sessions
identify themselves in-band: every inject into one is `kind=watchboard.*`,
and each digest carries `board_generation` (1-based count of board
sessions this sentinel has opened) and `sequence` (digest ordinal within
the session). `watchboard.rotated` links predecessor → successor, so the
chain is walkable from either end.

## What rotation does — and does not — touch

- **Incident bindings stay put.** Each flushed warning's dedup entry is
  bound to the session its digest landed in. After rotation, followups
  and `kind=resolved` / `resolved.reverted` outcomes for an incident
  bound to the old board keep routing there; only new warnings flow to
  the successor. Old boards drain naturally and never orphan an open
  fix-verify loop. Bindings persist through `--dedup-persist` like all
  bindings.
- **Storms bypass the board.** A storm can be warning-class (severity is
  the max of its members), but it always opens its own session — an
  aggregate incident an agent works is not a digest entry.
- **Triage-status overrides feed it.** An incident an agent downgraded
  via [`triage status`](/reference/triage-status/) `--severity-override`
  routes its next dedup cycle to the watchboard instead of re-paging —
  the M4 exit check captured the digest entry carrying the downgraded
  incident's exact fingerprint.

Per-kind routing is tunable with `--severity kind=level` (repeatable) —
the M2 drill demoted a default-critical kind to warning
(`--severity=objectstate.endpoints_empty=warning`) and the M3 drill
promoted `rollout.stall` to critical for a staging cluster where bad
deploys are the hunt. The full decision record is
[`docs/watchboard-rotation-design.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/watchboard-rotation-design.md);
the wire shapes are pinned byte-exact by tests.
