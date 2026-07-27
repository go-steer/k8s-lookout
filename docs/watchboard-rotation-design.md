# Watchboard session lifecycle — §15 Q2, decided

DESIGN.md §7.7 routes `warning`-severity signals to a shared "watchboard"
session as a batched rolling digest, and §15 Q2 asked M2 for a small design
note on its lifecycle: the session grows unboundedly, so what rotates it —
daily, or size-based — and how does rotation interact with session resume
and the sentinel's dedup bindings?

**Decision: size-based rotation.** After `--watchboard-rotate` digest
injects (default 200), the next flush opens a fresh session; the old session
is closed with a final `kind=watchboard.rotated` lineage inject.

This note records the settled decisions. The normative severity table stays
in DESIGN.md §7.7; the wire shapes are pinned byte-exact by
`TestWatchboardDigest_ExactWireShape` / `TestWatchboardRotated_ExactWireShape`
in `internal/watch`.

## Settled decisions

### Why size-based, not daily

The watchboard exists to bound the *agent's* cost of consuming warning
noise, and that cost is measured in context, not calendar time:

- **Bounded context regardless of noisiness.** A daily session on a noisy
  cluster can accumulate thousands of digests before midnight — exactly the
  unbounded-growth problem Q2 was opened for, rescheduled rather than
  solved. N injects caps the session's transcript size no matter how loud
  the cluster is.
- **Quiet clusters keep long-lived sessions.** A cluster emitting a handful
  of warnings a week would churn through 365 near-empty sessions a year
  under daily rotation, fragmenting the board's history for no benefit.
  Under size-based rotation it keeps one session for months — the digest
  timestamps already carry the calendar.
- **One knob, one meaning.** "Rotate after N injects" needs no timezone, no
  midnight edge cases, and no interaction with sentinel restarts (the
  counter is per-session and observable from the session itself: the last
  digest's `sequence`).

Time-based rotation solves a problem the watchboard does not have (log
retention); size-based solves the one it does (context growth).

### Rotation mechanics

The rotation counter is the number of `watchboard.digest` injects into the
current session. When a flush finds the counter at or past
`--watchboard-rotate`:

1. `POST /sessions` creates the successor (same `--owner` asserted-caller
   as every sentinel session).
2. The **final** inject into the old session is the schema-stable
   `kind=watchboard.rotated` record:
   `{successor_session_id, injects_count, rotated_at}` (plus `cluster` and
   `board_generation`) — an agent resuming the old session, or a human
   reading it later, follows the pointer forward.
3. The pending digest flushes into the successor; its counters restart at
   `board_generation+1`, `sequence=1`.

Failure posture: if the successor `POST /sessions` fails, rotation is
deferred — the digest flushes into the over-threshold session and rotation
retries on the next flush (warnings are never dropped to enforce a size
cap). If only the lineage inject fails, the successor is already live; the
lineage stays reconstructable from the generation/sequence coordinates.

### Session naming and lineage

The daemon's `POST /sessions` has no name parameter, so watchboard sessions
are identified **in-band**, not by name:

- Ownership: created with `X-Asserted-Caller` = `--owner`, like every
  session this sentinel opens.
- Content marker: every inject into a watchboard session is
  `kind=watchboard.*` — a session list filtered on the first inject's kind
  distinguishes the board from per-incident sessions.
- Lineage coordinates: each digest carries `board_generation` (1-based
  count of watchboard sessions this sentinel has opened; rotation
  increments it) and `sequence` (1-based digest ordinal within the
  session). `watchboard.rotated` links predecessor → successor, so the
  chain is walkable from either end.

### Interaction with session resume and dedup bindings

**Bindings are per-incident and rotation does not touch them.** When a
digest flushes, each flushed warning's dedup entry is bound to the session
the digest landed in (same `BindIncident` used by per-incident routing), and
the §7.4 recovery tracker watches it against that session. After rotation:

- Followups (dedup re-fires) and `kind=resolved` / `resolved.reverted`
  outcomes for an incident bound to the **old** watchboard keep routing
  there — the incident *lives* in that session, and an agent resumed on it
  has the full local context. Only **new** warnings flow to the successor.
- The old session therefore drains naturally: once its incidents resolve
  (or their dedup entries age out), nothing routes to it anymore and it can
  be concluded like any other session. Rotation never orphans an open
  fix-verify loop.
- Bindings persist through `--dedup-persist` like all bindings, so a
  sentinel restart resumes recovery tracking against whichever watchboard
  generation each incident is bound to.

If a §7.5 storm claims an incident that is sitting in the watchboard buffer,
the storm's binding wins (the digest entry remains as a record of the
observed warning, but followups and outcomes route to the storm session).

### What routes here — and what bypasses it

- Severity routing is **per-incident-mode machinery**: the watchboard is
  the per-incident-mode answer to warning noise. In `--mode=shared` the
  frozen contract holds — ALL severities route to `--target-session` and
  the watchboard is disabled.
- **Storms bypass warning routing.** A storm's severity is the max of its
  members (§7.5), so a storm can be warning-class — but it still always
  opens its own session: §7.5's purpose is ONE aggregate incident an agent
  works, which a digest entry is not.
- `info` never reaches the board: stored only per §7.7/§9.1 — counted in
  `info_dropped_total` and debug-logged until the M3 raw store lands
  (TODO(M3 store) in the dispatcher), never silently ignored.
- Recovery outcomes route wherever the incident is bound (per-incident
  session, storm session, or a watchboard generation) — unchanged by this
  design.

### Flag surface (additive)

| Flag | Default | Meaning |
| --- | --- | --- |
| `--watchboard-batch` | 5 | Buffered warnings that trigger a digest flush. |
| `--watchboard-flush` | 60s | Max age of a buffered warning before flushing anyway (whichever comes first). |
| `--watchboard-rotate` | 200 | Digest injects per session before rotation. |
| `--severity` | (none) | Per-kind severity override, `kind=level[,kind=level...]`, repeatable/additive. |

## Out of scope

- **Daily/TTL rotation as a second policy.** Rejected above; revisit only
  with evidence that a deployment needs calendar-aligned boards (and then
  it belongs in the daemon's session-retention story, not the sentinel).
- **Concluding / deleting old watchboard sessions.** Session retention is
  the daemon's concern; the sentinel only stops writing to a rotated
  session.
- **The M3 raw store** (§9.1) — `info` persistence and read-path digest
  queries over stored signals.
- **Enrichment of digest entries** (§7.6 is per-incident/critical; a
  digest entry that needs investigation gets `lookout` reads from the
  agent, not a pre-warmed bundle).
- **Fleet-level watchboards.** Cross-cluster rollup of warning digests
  belongs to the fleet layer, joining on the fingerprints each entry
  already carries.
