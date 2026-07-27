---
title: Connect to core-agent
description: The daemon contract — sessions and injects, per-incident vs shared routing, tokens and asserted callers, and what an inject looks like on the wire.
sidebar:
  order: 4
---

The sentinel speaks to a
[core-agent](https://github.com/go-steer/core-agent) daemon over its
pre-existing HTTP API — `POST /sessions` to open an incident session,
`POST /sessions/<sid>/inject` to deliver signals into it. Nothing new is
required on the daemon side.

## Wiring

- **`--daemon-url`** — base URL of the daemon, no trailing slash. The
  shipped manifest points at the local Service:
  `http://core-agent.agent-triage.svc.cluster.local:7777`. For a sentinel
  in a remote cluster, override to
  `https://<external-daemon-endpoint>:7777` — one daemon may serve many
  sentinels.
- **`--token-env`** — the name of the environment variable holding the
  bearer token (the shipped manifest sources `WATCHER_TOKEN` from the
  `k8s-event-watcher-token` Secret). Every request carries it as
  `Authorization`.
- **`--cluster-name`** — stamped into every payload, so a daemon serving
  several clusters can tell the streams apart.

## Per-incident vs shared

- **`--mode=per-incident`** (default) — the sentinel creates a session
  per `(uid, reason)` incident. Requires **`--owner`**: the value sent as
  `X-Asserted-Caller` on `POST /sessions`, which must match a proxy
  identity in the daemon's `users.json` — the daemon attributes every
  sentinel-opened session to that owner. Severity routing is
  per-incident-mode machinery: critical signals open their own enriched
  sessions, warnings batch into the [watchboard](/operations/watchboard/),
  info-class signals are stored only.
- **`--mode=shared`** — all injects, every severity, go to one
  pre-existing session named by **`--target-session`**. The watchboard is
  disabled; nothing is created. The right shape when an existing agent
  session should receive everything.

## What an inject looks like

Captured on the wire by the M0 exit check (a stub daemon logging every
request —
[`docs/milestones/M0.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/milestones/M0.md)):

```
REQ POST /sessions
  Authorization: <bearer token present>
  X-Asserted-Caller: sre-oncall@example.com
REQ POST /sessions/stub-sess-0005/inject
  Authorization: <bearer token present>
  X-Asserted-Caller: sre-oncall@example.com
  BODY: {"message":"{\"kind\":\"k8s-event\",\"reason\":\"BackOff\",\"namespace\":\"default\",\"kind_of_object\":\"Pod\",\"name\":\"crashloop-demo\",\"container\":\"spec.containers{crasher}\",\"uid\":\"7503ea47-d147-4342-92b2-743a1d88cd4b\",\"message\":\"Back-off restarting failed container crasher in pod crashloop-demo_default(7503ea47-d147-4342-92b2-743a1d88cd4b)\",\"count\":1,\"first_seen\":\"2026-07-24T17:12:00Z\",\"last_seen\":\"2026-07-24T17:12:00Z\",\"cluster\":\"local\",\"context\":{\"node\":\"kl-m0-control-plane\"}}"}
```

The payload is a structured signal, not prose: `kind`, the object
coordinates, a cross-cluster-stable `fingerprint` (on all
source-namespaced kinds), and per-kind fields. The `k8s-event` /
`k8s-event-followup` pair above is byte-frozen from the predecessor;
everything the sentinel can inject — `storm`, `resolved`,
`watchboard.digest`, `saturation.forecast` with its ETA attachment, and
the rest — is cataloged in the generated
[signal-kind reference](/reference/signal-kinds/).

Beyond the initial inject, sessions receive followups without any agent
polling: dedup-window repeats, `kind=storm.member` attachments, and —
the closed loop — `kind=resolved` when the sentinel observes the symptom
clear and hold stable for `--recovery-stable-for` (with
`kind=resolved.reverted` if it comes back).

## Trying it without a daemon

- **`--dry-run`** watches the cluster for real — informers, sources,
  filter/dedup/routing all run, so it needs cluster access like a
  normal run — but prints inject payloads to stdout instead of calling
  any daemon or sink. Point it at a kubeconfig (or run it in-cluster),
  break something, and watch the payloads appear.
- The capture stub
  [`dev/drills/stub-daemon.py`](https://github.com/go-steer/k8s-lookout/blob/main/dev/drills/stub-daemon.py)
  implements the two endpoints and logs every request body — deployed
  behind a Service named `core-agent:7777`, it is the wire-level
  evidence capture every drill and milestone record uses.
