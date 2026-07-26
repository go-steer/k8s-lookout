---
title: Your rollout is stuck
description: rollout.stall fires while the old revision still serves — a session before any user notices, a rollback, and a verified resolved record. Real output from the M3 exit drill.
sidebar:
  order: 2
---

**The problem:** you shipped a deploy and nothing seems to be happening.
With `maxUnavailable=0`, a bad deploy *cannot* hurt users — as long as
somebody notices before it proceeds. The default noticer is
`progressDeadlineSeconds` (600s by default), which is an autopsy timer.

The sentinel's `rollout` source is evidence-based instead: a new
ReplicaSet making zero ready-count progress for `--rollout-observe`
(default 3m) *while the old revision stays healthy* is a probable bad
deploy — fired well before the deadline.

All output below is from the M3 exit drill (kind cluster, sentinel with
`--sources=…,rollout,…`), abridged. The staged failure: `drill-a/webapp`
(2 replicas serving HTTP behind a Service, `maxUnavailable=0 maxSurge=1`)
updated to a valid image with a crashing command.

## The timeline — leading beats reactive at the right altitude

Sentinel log from the drill, bad deploy applied at 10:51:41:

```txt
10:51:47 fire BackOff pod=drill-a/webapp-55866d5cff-cwgp4 → sid=stub-sess-0004   (reactive, pod-level, +6s)
10:54:55 watchboard: buffered rollout.stall drill-a/webapp (severity=warning)    (leading, Deployment-level, +3m14s)
10:55:59 watchboard: digest 1 entry(ies) → sid=stub-sess-0009
10:58:24 kubectl rollout undo
10:58:40 resolved rollout_stall drill-a/webapp → sid=stub-sess-0009 (resolution=recovered)
```

The reactive pod events are earlier (+6s) but name the wrong object at the
wrong altitude: a crashing pod, not a stalled Deployment. `rollout.stall`
is the signal that says "probable bad deploy, old revision healthy" — and
in this drill it beat `progressDeadlineSeconds`' own leading indicator by
almost five minutes.

**Proven user-invisible**, captured mid-stall, between the signal and the
rollback:

```txt
HTTP/1.0 200 OK   × 5    (Server: SimpleHTTP/0.6 Python/3.12.13 — the OLD revision)
webapp-55866d5cff-cwgp4   0/1   CrashLoopBackOff   4
webapp-77f8d7558c-7gvwx   1/1   Running            0
webapp-77f8d7558c-c2nmj   1/1   Running            0
```

## Choosing the routing: watchboard or page

`rollout.stall` defaults to warning class — it lands in the shared
watchboard digest, not a page. That's the right default for production
(the deploy is *not hurting users*). For a staging cluster where bad
deploys should page, turn the severity knob; the drill's second run did
exactly that (`--severity=rollout.stall=critical`):

```txt
11:09:12 bad deploy #2 applied (new revision, crashing command)
11:12:22 enrich drill-a/webapp: 3019B (outcome=ok, sections=4)
11:12:22 fire rollout_stall pod=drill-a/webapp → sid=stub-sess-0019    (+3m10s, own session, enriched)
11:13:32 kubectl rollout undo
11:13:52 resolved rollout_stall → sid=stub-sess-0019 (resolution=recovered)
```

The session opens with the full enrichment bundle attached, and the
signal's message carries the evidence verbatim:

```txt
rollout stalled: new ReplicaSet webapp-6cfdc68df new_ready=0/1 old_ready=2/2 elapsed=3m10s
— new-revision pods failing while the old revision stays healthy
(probable bad deploy, fired ahead of progressDeadlineSeconds)
```

## The fix, and the loop closing

The fix is a standard rollback — `kubectl rollout undo` (or a GitOps
revert). Nobody polls to confirm: the same source that watched the stall
appear watches the rollout complete, and a schema-stable
`kind=resolved` (`resolution=recovered`) record lands in the same session.
Session ledger: one create, one enriched stall inject, one resolved. Done.

If you're investigating a stall by hand rather than from an inject,
`lookout triage delta` surfaces it as `workload.rollout
reason=RolloutIncomplete` with the desired/ready/updated counts, and
`lookout triage changes Deployment/<ns>/<name> --since=30m` names the
rollout that started it — see
[what changed before the incident](/guides/what-changed/).

## As an agent skill

The investigation surface for stalled workloads — bundle-first flow,
`triage changes` for the trigger, the output-envelope semantics — is
taught to agents by
[`skills/k8s-triage`](https://github.com/go-steer/k8s-lookout/tree/main/skills/k8s-triage).
