---
title: Investigate a broken workload
description: The bundle-first flow — root-causing a double fault in one call, then narrowing with targeted reads. Real output from the M1 exit check.
sidebar:
  order: 1
---

**The problem:** `shop/checkout` is broken and you don't yet know how.
In this (real) case it was broken twice over: the image was updated to a
nonexistent tag (`busybox:1.36-nonexistent-m1`), *and* the ConfigMap key
its pod template references (`log.level`) was deleted. A healthy
Deployment (`web`) runs alongside. All output below is captured from the
M1 exit check on a kind cluster, abridged.

## 1. `bundle` first — one correlated payload

`lookout bundle` converts the first 4–5 reads of an investigation into one
call: sanitized spec, everything abnormal, broken dependency edges, blast
radius, and distilled logs, scoped to the workload:

```sh
lookout bundle --workload=Deployment/shop/checkout
```

```txt
kind=bundle.target severity=info namespace=shop kind_of_object=Deployment name=checkout workload=Deployment/shop/checkout pods=3 sections=spec,delta,edges,radius,logs
kind=spec.container severity=info namespace=shop kind_of_object=Deployment name=checkout section=spec container=checkout image=busybox:1.36-nonexistent-m1 env="LOG_LEVEL=configMapKeyRef:checkout-config.log.level,DB_PASSWORD=secretKeyRef:checkout-db.password"
kind=pod.imagepull severity=critical namespace=shop kind_of_object=Pod name=checkout-5898857498-vw894 reason=ImagePullBackOff message="Back-off pulling image \"busybox:1.36-nonexistent-m1\": ErrImagePull: rpc error: code = NotFound …" section=delta container=checkout image=busybox:1.36-nonexistent-m1
kind=workload.rollout severity=warning namespace=shop kind_of_object=Deployment name=checkout reason=RolloutIncomplete section=delta desired=2 ready=2 updated=1 available=2
kind=edge.missing_key severity=critical namespace=shop kind_of_object=ConfigMap name=checkout-config reason=CreateContainerConfigError message="key log.level not found in configmap checkout-config (env LOG_LEVEL in container checkout)" section=edges workload=Deployment/shop/checkout container=checkout env=LOG_LEVEL key=log.level pods=3
kind=edge.selector_unready severity=warning namespace=shop kind_of_object=Service name=checkout reason=PodsNotReady message="service selects 3 pod(s), 2 ready" section=edges workload=Deployment/shop/checkout selector="app=checkout" selected=3 ready=2
kind=log.template severity=warning namespace=shop section=logs template="ERROR db pool exhausted retry=<*>" count=12 pods=2 level=error first_seen=2026-07-24T20:51:18Z last_seen=2026-07-24T20:52:28Z sample="ERROR db pool exhausted retry=1"
…(abridged)…
scanned=257 findings=21 elapsed=1.195s
```

Both root causes are named with exact references in the first screen of
one call: the bad image tag (`pod.imagepull`, with the tag inline) and the
missing ConfigMap key (`edge.missing_key`, naming the key, the env var,
the container, and the blast count). Note what is *not* here: the healthy
`web` Deployment emits nothing anywhere in this guide — healthy resources
are always silent, and the summary line proves they were scanned.

If your session started from a sentinel inject, pass the payload straight
in — `--incident='{"kind":"k8s-event",…}'` resolves the pod to its owning
workload through the graph's owner chain and produces the same bundle.

## 2. Narrow with targeted reads

Cluster-wide sanity check — exactly the abnormal objects, nothing else:

```sh
lookout triage delta
```

```txt
kind=pod.imagepull severity=critical namespace=shop kind_of_object=Pod name=checkout-5898857498-vw894 reason=ImagePullBackOff … container=checkout image=busybox:1.36-nonexistent-m1
kind=workload.rollout severity=warning namespace=shop kind_of_object=Deployment name=checkout reason=RolloutIncomplete desired=2 ready=2 updated=1 available=2
scanned=20 findings=2 elapsed=148ms
```

Verify the config wiring claim, and confirm the key really is gone
(abridged):

```sh
lookout state edges --workload=Deployment/shop/checkout
lookout triage spec cm/shop/checkout-config
```

```txt
kind=edge.missing_key severity=critical namespace=shop kind_of_object=ConfigMap name=checkout-config reason=CreateContainerConfigError message="key log.level not found in configmap checkout-config (env LOG_LEVEL in container checkout)" workload=Deployment/shop/checkout container=checkout env=LOG_LEVEL key=log.level pods=3
scanned=146 findings=2 elapsed=230ms

kind=spec.resource severity=info namespace=shop kind_of_object=ConfigMap name=checkout-config keys=feature.flags(9B)
scanned=1 findings=1 elapsed=64ms
```

The ConfigMap now holds only `feature.flags` — `log.level` is confirmed
missing. And the secret-safety contract at work: reading the Secret shows
key name and byte size only —

```txt
kind=spec.resource severity=info namespace=shop kind_of_object=Secret name=checkout-db keys=password(19B)
scanned=1 findings=1 elapsed=61ms
```

## 3. What the application itself said

```sh
lookout triage logs --workload=Deployment/shop/checkout --since=30m
```

```txt
kind=log.fetch_error severity=warning namespace=shop kind_of_object=Pod name=checkout-5898857498-vw894 reason=LogFetchFailed message="container \"checkout\" … is waiting to start: trying and failing to pull image" container=checkout
kind=log.template severity=warning namespace=shop template="ERROR db pool exhausted retry=<*>" count=14 pods=2 level=error … sample="ERROR db pool exhausted retry=1"
kind=log.template severity=info namespace=shop template="INFO handled request path=<*> status=<*> dur=<*>" count=100 pods=2 level=info …
scanned=124 findings=5 elapsed=157ms
```

124 raw lines distilled to a handful of templates with counts and pod
spread — plus an honest `log.fetch_error` for the pod that cannot start,
instead of silence.

## The shape of the flow

1. `bundle` (or `bundle --incident=…`) — the wide, correlated read. The
   root cause is usually in its critical `delta`/`edges` findings.
2. Targeted reads to confirm and dig: `triage delta`, `state edges`,
   `triage spec`, `triage logs`.
3. Sudden regression instead? Ask
   [what changed](/guides/what-changed/) *first*.

In the M1 exit check this double fault was fully root-caused with `lookout`
reads alone — no kubectl was needed for the diagnosis.

## As an agent skill

Agents learn this exact decision tree — bundle first, when to go direct,
how to read the envelope — from
[`skills/k8s-triage`](https://github.com/go-steer/k8s-lookout/tree/main/skills/k8s-triage),
with per-symptom playbooks in
[`skills/playbooks`](https://github.com/go-steer/k8s-lookout/tree/main/skills/playbooks).
