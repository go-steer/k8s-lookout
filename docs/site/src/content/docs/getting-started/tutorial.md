---
title: "Tutorial: catch a bad deploy in the act"
description: A ~20-minute walkthrough on a disposable kind cluster — the sentinel wired to a capture stub, a staged crashloop, a user-invisible bad deploy, and the closed loop from detection to verified resolution.
sidebar:
  label: "Tutorial: see it work"
  order: 3
---

This walkthrough stands up everything on a disposable local cluster and
stages two real failures, so you can watch both halves of `lookout`
work: the sentinel catching trouble and opening incidents, and the
read-path CLI investigating them. Every command is copy-runnable —
by you or by an AI agent you hand this page to — and every output
block below is real output captured from live runs of these exact
commands, never invented (abridged where marked).

**Takes:** ~20 minutes. **Needs:** `docker`, `kind`, `kubectl`, and
Go 1.26+. **Safety:** the cluster is disposable, and every script
refuses to run unless your current kubectl context is the tutorial
cluster (`kind-lookout-examples`).

## What you will stand up

- A 3-node kind cluster with metrics-server (`examples/kind/up`).
- The **sentinel** (`lookout watch`), deployed from the same shipped
  manifests a production install uses, tuned to drill-speed windows.
- A **capture stub** standing in for a core-agent daemon: a one-page
  Python server behind a Service named `core-agent:7777` that logs
  every session-create and inject it receives — so you can read the
  exact wire traffic a real agent daemon would get.
- A small demo app to break: `web`, `api` (with a PDB), `worker`, and
  a `vantage` pod for in-cluster HTTP checks.

## 1. Stand it up

```sh
go install github.com/go-steer/k8s-lookout/cmd/lookout@latest
git clone https://github.com/go-steer/k8s-lookout && cd k8s-lookout
examples/kind/up                       # cluster + metrics-server
examples/sentinel/up                   # RBAC + sentinel + capture stub
kubectl apply -f examples/workloads/   # the demo app
```

`examples/sentinel/up` ends by printing the sentinel's startup log.
Read it — every armed stage announces itself, and every degradation is
a named line, not silence (abridged):

```console
2026/07/31 12:18:37 recovery: tracking enabled (stable-for=1m0s, tick=15s)
2026/07/31 12:18:37 storm: correlation enabled (window=1m0s, min=3)
2026/07/31 12:18:37 k8s-event-watcher: starting on cluster "lookout-examples" → daemon http://core-agent.agent-triage.svc.cluster.local:7777 (mode=per-incident, owner=lookout-examples@local)
2026/07/31 12:18:37 capacity: provider scale-decision sub-source disabled: unavailable reason="no cloud provider configured" — Events + status-ConfigMap sub-sources still fire on scaleup failures, without the structured why
2026/07/31 12:18:37 storm: topology graph ready (88 nodes, 115 edges) — blast-radius correlation armed
```

Then take the baseline. Healthy resources print nothing; every
category answers; the summary line is mandatory:

```sh
lookout health
```

```console
kind=health.category severity=info reason=Unavailable message="requires cloud provider metrics; no cloud provider configured" category=control-plane status=unavailable
kind=health.category severity=info category=nodes status=healthy
kind=health.category severity=info category=crashloops status=healthy
kind=health.category severity=info category=pending status=healthy
kind=health.category severity=info category=rollouts status=healthy
kind=health.category severity=info category=storage status=healthy
kind=health.category severity=info category=addons status=healthy
kind=health.category severity=info category=quota status=healthy
kind=health.category severity=info category=certs status=healthy
kind=health.category severity=info category=webhooks status=healthy
scanned=35 findings=10 elapsed=87ms
```

In a second terminal, follow the wire — this is what a real agent
daemon would be receiving from here on:

```sh
kubectl -n agent-triage logs deploy/stub-daemon -f
```

## 2. Break something simple: a crashloop

```sh
examples/scenarios/crashloop/inject    # worker's command → exit(1) after 2s
```

Within a minute or two, the stub log shows the sentinel opening a
per-incident session and injecting the signal — **with a pre-warmed
evidence bundle already attached** (sanitized spec, blast radius,
distilled logs), so an agent's first tool calls are pre-answered
(abridged):

```console
SESSION-CREATE sid=stub-sess-0004 caller=… token=present
INJECT sid=stub-sess-0004 kind=k8s-event token=present body={"message":"{\"kind\":\"k8s-event\",\"reason\":\"BackOff\",\"namespace\":\"lookout-demo\",\"kind_of_object\":\"Pod\",\"name\":\"worker-7cd49696bf-pl7lh\",\"container\":\"spec.containers{worker}\",… \"message\":\"Back-off restarting failed container worker in pod worker-7cd49696bf-pl7lh_lookout-demo(…)\",… \"enrichment\":{\"bundle\":\"kind=bundle.target … sections=spec,radius,logs\n kind=spec.container … container=worker image=python:3.12-alpine requests=…,memory=32Mi limits=…,memory=64Mi\n kind=radius.neighbor … kind_of_object=Node name=lookout-examples-worker2 … relation=downstream hop=1\n kind=log.template … template=\"worker: tick\" count=3 …\n overflow section=edges cmd=\"lookout state edges --workload=Deployment/lookout-demo/worker\"\"}}"}
INJECT sid=stub-sess-0004 kind=objectstate.restart_burst token=present body={"message":"{\"kind\":\"objectstate.restart_burst\",\"reason\":\"restart_burst\",\"namespace\":\"lookout-demo\",\"kind_of_object\":\"Pod\",\"name\":\"worker-7cd49696bf-pl7lh\",… \"message\":\"container restart count grew by 3 within 10m0s (total=3)\",… \"severity\":\"warning\",\"fingerprint\":\"sha256:e869fa95…\",…}"}
```

The sentinel's own log shows the decisions: the fire, dedup absorbing
the repeats, and a leading-indicator source joining the same incident
rather than opening a second one (abridged):

```console
2026/07/31 12:09:33 dedup BackOff pod=lookout-demo/worker-7cd49696bf-pl7lh (count=2, window active)
2026/07/31 12:09:56 dedup restart_burst pod=lookout-demo/worker-7cd49696bf-pl7lh (count=3, window active)
2026/07/31 12:09:57 followup restart_burst lookout-demo/worker-7cd49696bf-pl7lh → sid=stub-sess-0004 (cross-source join: objectstate joined a k8s-event-opened incident)
2026/07/31 12:10:00 dedup BackOff pod=lookout-demo/worker-7cd49696bf-pl7lh (count=4, window active)
```

Now investigate from the read path, like an agent would:

```sh
lookout triage events --namespace=lookout-demo
```

```console
kind=event.normal severity=info namespace=lookout-demo kind_of_object=Pod name=worker-7cd49696bf-pl7lh reason=Started message="Container started" count=5 first_seen=2026-07-31T12:09:12Z last_seen=2026-07-31T12:10:42Z source=kubelet
kind=event.warning severity=warning namespace=lookout-demo kind_of_object=Pod name=worker-7cd49696bf-pl7lh reason=CrashLoopBackOff message="Back-off restarting failed container worker in pod worker-7cd49696bf-pl7lh_lookout-demo(…)" count=4 first_seen=2026-07-31T12:09:17Z last_seen=2026-07-31T12:10:45Z source=kubelet
scanned=46 findings=42 elapsed=28ms
```

```sh
lookout bundle --workload=Deployment/lookout-demo/worker
```

```console
kind=bundle.target severity=info namespace=lookout-demo kind_of_object=Deployment name=worker workload=Deployment/lookout-demo/worker pods=1 sections=spec,delta,edges,radius,logs
kind=spec.container severity=info namespace=lookout-demo kind_of_object=Deployment name=worker section=spec container=worker image=python:3.12-alpine requests="cpu=10m,memory=32Mi" limits="cpu=100m,memory=64Mi"
kind=spec.condition severity=warning namespace=lookout-demo kind_of_object=Deployment name=worker reason=MinimumReplicasUnavailable message="Deployment does not have minimum availability." section=spec condition="Available=False" since=2026-07-31T12:10:45Z
kind=workload.rollout severity=critical namespace=lookout-demo kind_of_object=Deployment name=worker reason=RolloutIncomplete section=delta desired=1 ready=0 updated=1 available=0
kind=radius.neighbor severity=info kind_of_object=Node name=lookout-examples-worker2 section=radius relation=downstream hop=1
scanned=158 findings=13 elapsed=154ms
```

Or skip the manual reads and hand it to your agent mid-crash:

> Something keeps restarting in the lookout-demo namespace — find it
> and tell me why.

Check its answer, then heal the workload:

```sh
examples/scenarios/crashloop/verify    # asserts the wire + read-path evidence
examples/scenarios/crashloop/revert
```

## 3. The flagship: a bad deploy your users cannot see

`web` runs `maxUnavailable=0`, so rolling it to a broken image parks
one crashing surge pod next to the healthy old revision. Dashboards
stay green; users keep getting 200s. This is the failure class the
sentinel exists for.

```sh
examples/scenarios/bad-rollout/inject
```

```console
T0 (rollout onset): 2026-07-31T12:23:31Z
```

`verify` proves the user-invisibility claim mid-stall, through the
Service, from inside the cluster:

```console
▸ proving user-invisibility: 5 requests through the Service
    HTTP 200
    HTTP 200
    HTTP 200
    HTTP 200
    HTTP 200
  ✓ 5/5 requests answered 200 mid-stall
```

At 96 seconds after onset — ahead of `progressDeadlineSeconds` and
ahead of any user noticing — the sentinel fires at the Deployment
altitude and opens an enriched incident session:

```console
2026/07/31 12:25:08 fire rollout_stall pod=lookout-demo/web → sid=stub-sess-0007 (mode=per-incident)
```

On the wire, the payload says exactly what an agent needs to decide
(abridged):

```console
INJECT sid=stub-sess-0007 kind=rollout.stall token=present body={"message":"{\"kind\":\"rollout.stall\",\"reason\":\"rollout_stall\",\"namespace\":\"lookout-demo\",\"kind_of_object\":\"Deployment\",\"name\":\"web\",… \"message\":\"rollout stalled: new ReplicaSet web-59ffb5fc9c new_ready=0/1 old_ready=2/2 elapsed=1m36s — new-revision pods failing while the old revision stays healthy (probable bad deploy, fired ahead of progressDeadlineSeconds)\",… \"cluster\":\"lookout-examples\",…
```

The read path sees it too — note the altitude trap this catches:
`ready=2` looks fine at the pod level, and the rollouts category is
degraded anyway:

```sh
lookout health
```

```console
kind=health.category severity=warning category=rollouts status=degraded total=1 top="workload.rollout lookout-demo/web"
kind=workload.rollout severity=warning namespace=lookout-demo kind_of_object=Deployment name=web reason=RolloutIncomplete fingerprint=sha256:f954cac0… category=rollouts desired=2 ready=2 updated=1 available=2
scanned=36 findings=11 elapsed=91ms
```

And "what changed?" is one command — the new template revision is
named, with its image (abridged):

```sh
lookout triage changes --workload=Deployment/lookout-demo/web
```

```console
kind=change.rollout severity=info namespace=lookout-demo kind_of_object=ReplicaSet name=web-59ffb5fc9c reason=NewReplicaSet message="new template revision created inside the window" at=2026-07-31T12:12:42Z relation=upstream origin=api revision=4 image=python:3.11-alpine
scanned=229 findings=5 elapsed=114ms source=live-approximation window=2026-07-31T11:55:38Z..2026-07-31T12:25:38Z
```

Agent prompt to try instead:

> We just shipped web in lookout-demo and dashboards look fine, but the
> sentinel opened an incident. Is it real? Should we roll back?

Now fix it and watch the loop close. `revert` runs `kubectl rollout
undo` and waits — the sentinel observes the recovery hold stable, then
injects a verified `resolved` **into the same session**, with proof
attached. No agent polling, no human guessing it is safe to close:

```sh
examples/scenarios/bad-rollout/revert
```

```console
2026/07/31 12:27:07 resolved rollout_stall pod=lookout-demo/web → sid=stub-sess-0007 (resolution=recovered, cleared_after=53.602705831s, stable_for=1m6.298204053s)
```

```console
INJECT sid=stub-sess-0007 kind=resolved token=present body={"message":"{\"kind\":\"resolved\",\"reason\":\"rollout_stall\",\"namespace\":\"lookout-demo\",\"kind_of_object\":\"Deployment\",\"name\":\"web\",… \"fingerprint\":\"sha256:35bf767b…\",\"cluster\":\"lookout-examples\",…
```

That is the whole story on one screen: detected before users noticed,
enriched at open, correlated at the right altitude, and closed with
observed proof.

## 4. Where next

- **Eight more failure classes** — OOM, cert expiry, PDB gridlock,
  empty endpoints, node death (one storm session, not thirty), and
  more: each has the same `inject`/`verify`/`revert` shape under
  [`examples/scenarios/`](https://github.com/go-steer/k8s-lookout/tree/main/examples/scenarios),
  and `examples/e2e` runs the non-destructive set unattended.
  Re-runs are deliberately quieter — dedup, `resolved.reverted`, and
  storm absorption are the sentinel working as designed.
- **Test your own agent against it** — inject a scenario, hand your
  agent the prompt from its README, compare its findings with
  `verify`'s:
  [`examples/agent-harness.md`](https://github.com/go-steer/k8s-lookout/blob/main/examples/agent-harness.md).
- **Deploy for real** — the same manifests, minus the stub, plus a
  real sink: [Deploy the sentinel](/getting-started/deploy/), then
  [Connect to core-agent](/getting-started/connect-core-agent/) or
  [any webhook receiver](/getting-started/integrations/).
- **Clean up** — `examples/kind/down`.
