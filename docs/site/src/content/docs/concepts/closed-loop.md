---
title: The closed loop
description: Recovery injects, storm correlation, the watchboard, and triage-status records — sessions with verified outcomes instead of alerts.
sidebar:
  order: 4
---

Most monitoring tools send alerts: one-shot notifications that someone
must collect, connect, and eventually silence. When `lookout`'s sentinel
spots a problem it instead opens a **session** — an ongoing record of
one incident that accumulates the follow-up observations, the
diagnosis, and finally proof the problem is actually gone. This page
explains the four mechanisms that make that work; for the inventory of
what's watched in the first place, see [What the sentinel
watches](/detect/sentinel/).

## Recovery injects: fix-verify without polling

The dedup cache binds each incident to its session. Each source that can
observe a symptom can also observe its absence — pod Ready and
restart-stable, rollout completed, endpoints back to full ratio, cert
renewed, node Ready again. When a bound incident's symptom stays clear for
`--recovery-stable-for` (default 5m), the sentinel injects `kind=resolved`
into the *same session*, carrying `cleared_after`, `observed_stable_for`,
and a structured `resolution` (`recovered` or `object_deleted`). Recurrence
within the window fires `kind=resolved.reverted` instead.

From a live drill: a crashlooping pod's ConfigMap was patched, nothing else
touched — 76 seconds later (stability window + one tracker tick) the
incident's session received, with zero polling by anyone:

```json
{"kind":"resolved","reason":"CrashLoopBackOff","namespace":"fixlab",
 "kind_of_object":"Pod","name":"payment-869d9b5594-d6gtm",
 "cleared_after":"1m36s","observed_stable_for":"1m10.695183392s",
 "resolution":"recovered", "…":"…"}
```

This closes the fix-and-verify loop from the signal side: the agent no
longer polls to confirm its fix stuck, sessions can auto-conclude — and
every incident becomes a trajectory with an externally verified outcome
(symptom → diagnosis → action → result), harvestable as labeled data
because outcome records are schema-stable structs, never prose.

## Storm correlation: one session, not thirty

A naive per-incident model turns one node failure into ~30 sessions for 30
evicted pods. With storm correlation on (`--storm` defaults to auto:
on whenever the graph grants are present), a second-level correlation window groups new
incidents sharing a **blast-radius key** — the nearest common ancestor in
the [topology graph](/concepts/topology-graph/) (node, owner chain, shared
ConfigMap/PVC, namespace) — into one `kind=storm` session: *"Node X
NotReady; N pods affected across M namespaces; representative incidents
attached."*

Members are recorded as `kind=storm.member` followups instead of opening
sessions; incidents that fired before the formation threshold are
superseded into the storm (`kind=storm.member_superseded` pointers, dedup
bindings rebound so their followups and outcomes route to the storm). As
membership grows, `kind=storm.update` refreshes the headline size. Recovery
composes with it: member resolutions flow into the storm session, and the
storm's own aggregate `resolved` fires once all members — including the
node — clear. The measured drill is the
[node-failure guide](/guides/node-failure/).

## The watchboard: warning noise, bounded

Warning-class signals batch into a shared **watchboard** session as
`kind=watchboard.digest` injects (flushed on `--watchboard-batch` or
`--watchboard-flush`, whichever first). The board rotates by **size**, not
calendar: after `--watchboard-rotate` digests (default 200), the next flush
opens a successor session and the old one closes with a
`kind=watchboard.rotated` lineage pointer. Size-based rotation bounds the
*agent's* cost of consuming the board regardless of how noisy the cluster
is, while a quiet cluster keeps one session for months. Incidents bound to
a rotated board keep their followups and outcomes routing there — rotation
never orphans an open fix-verify loop. The full rationale is
[`docs/watchboard-rotation-design.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/watchboard-rotation-design.md).

## Triage-status records: scans report triaged reality

An incident agent triages a crashloop at 08:00; a health scan at 08:15 must
not re-report it as a fresh unknown, re-page, or re-burn tokens re-deriving
the diagnosis. Raw telemetry says "broken"; *triaged reality* says
"diagnosed, PR open, downgraded".

The mechanism: the diagnosing agent writes a compact record — status,
root-cause hypothesis, action taken, and optionally a `severity_override` —
keyed by the incident's `fingerprint` and resource, via
[`lookout triage status`](/reference/triage-status/) against the sentinel's
store. Three consumers honor it:

- **`lookout health` and `bundle --store`** join open findings against the
  records, so scan output carries the diagnosis and paper trail
  (`triage_status=`, `triage_root_cause=`, `triage_action=`,
  `triage_session=`), and the scorecard severity downgrades with the
  agent's judgment.
- **Sentinel routing** honors `severity_override`: a downgraded incident's
  next dedup cycle routes to the watchboard instead of re-paging;
  `escalated` keeps it hot. If a downgraded incident's recurrence rate
  spikes, the sentinel emits `kind=triage.regressed` *evidence* into the
  bound session — it never overrides the agent's judgment automatically.
- **Lifecycle is automatic:** when recovery observes the symptom clear, the
  record flips to `resolved` — no manual TTL bookkeeping.

`--status` accepts only the agent-written states
(`investigating|triaged|actioned|escalated`); `resolved` is reserved for
the sentinel's observed-stability flip, so the outcome labels stay
trustworthy. The [capacity guide](/guides/capacity-quota/) shows the
whole flow live.

## Why sessions, not alerts

Put together: a symptom opens one session (not thirty), pre-warmed with the
context the first tool calls would have fetched; followups, correlated
observations from other sources, and the agent's own diagnosis accumulate
in it; routing respects what has already been triaged; and the world —
not the agent — declares the outcome. An alert asks a human to find out
what happened. A `lookout` session already knows, and can prove when it's
over.
