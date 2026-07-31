---
title: First reads
description: The read-path against your current kubeconfig — lookout health and lookout triage delta, with real captured output.
sidebar:
  order: 2
---

Every read command works against your current kubeconfig context. Nothing
is deployed, nothing is mutated — the read-path only ever gets, lists, and
watches.

## "Any issues with this cluster?"

```sh
lookout health
```

```console
kind=health.category severity=info reason=Unavailable message="requires cloud provider metrics; no cloud provider configured" category=control-plane status=unavailable
kind=health.category severity=info category=nodes status=healthy
kind=health.category severity=warning category=crashloops status=degraded total=8 top="pod.restarts agent-sandbox-system/agent-sandbox-controller-7c69875fcc-n7xms; pod.restarts kube-system/coredns-7d764666f9-g82j9; …"
kind=health.category severity=info category=pending status=healthy
kind=health.category severity=info category=rollouts status=healthy
…
kind=pod.restarts severity=warning namespace=kube-system kind_of_object=Pod name=coredns-7d764666f9-g82j9 reason=ExcessiveRestarts fingerprint=sha256:e094a6ee… category=crashloops container=coredns restarts=62
scanned=16 findings=18 elapsed=537ms
```

(Real output against a kind cluster, abridged.) Three things to notice,
because they are the output contract everything else follows:

- **Every category answers.** Ten categories each report
  `healthy | degraded | unavailable` — healthy resources are omitted, but
  a category is never silently absent. `control-plane` honestly reports
  `unavailable` here: its metric packs need a cloud provider, and none is
  configured. Absent capability is always explicit, never an error.
- **The summary line is mandatory.** `scanned=16 findings=18
  elapsed=537ms` closes every invocation, so "cluster healthy"
  (`findings=0`) is distinguishable from "wrong flag / broken tool".
- **Findings are one record per line** (logfmt by default, `--format=json`
  for JSON), each with a stable `fingerprint` for cross-referencing.

## Everything abnormal, in one scan

`lookout triage delta` is the first call for "anything wrong?": broken
workloads, aged Pending pods, node pressure, gridlocked PDBs, degraded
kube-system add-ons, quotas at their limits — one pass, only the abnormal:

```sh
lookout triage delta -A
```

```console
kind=pod.imagepull severity=critical namespace=shop kind_of_object=Pod name=checkout-5898857498-vw894 reason=ImagePullBackOff … container=checkout image=busybox:1.36-nonexistent-m1
kind=workload.rollout severity=warning namespace=shop kind_of_object=Deployment name=checkout reason=RolloutIncomplete desired=2 ready=2 updated=1 available=2
scanned=20 findings=2 elapsed=148ms
```

(Real output from a live validation drill — a kind cluster seeded with
a deliberately broken Deployment.)
The healthy Deployment, kube-system, and the node emitted nothing;
`scanned=20` proves they were examined.

From a delta finding, the usual next move is one correlated snapshot of
the broken workload — sanitized spec, abnormal objects, broken dependency
edges, blast radius, distilled logs, in a single payload:

```sh
lookout bundle --workload=Deployment/shop/checkout
```

## The output contract, in brief

- **Exit 0:** pure payload on stdout — no banners, no progress —
  terminated by the summary line. Diagnostics go to stderr only, so a
  captured stream never corrupts an agent's context window.
- **Exit 1** is a runtime failure (structured diagnostics on stderr);
  **exit 2** is a usage error.
- **Common flags** on every command: `--namespace` / `-A`,
  `--workload=<Kind>/<ns>/<name>`, `--since`, `--format=logfmt|json`,
  `--timeout=10s`.
- **Sanitized always:** secret values, credential-shaped strings, and
  system metadata are masked or stripped from every output surface.

Provider-gated commands (`cloud …`, `state wi`, `perf probe`) on a
cluster without a cloud provider answer explicitly and exit 0:

```console
kind=cloud.unavailable severity=info reason=CapabilityUnavailable message="cloud quota needs the provider quota capability: no cloud provider configured" capability=quota provider=none
scanned=0 findings=1 elapsed=0s unavailable="no cloud provider configured"
```

The [Reference](/reference/) section documents every command's flags,
output fields, and examples — generated from the same metadata that
produces `--help`.
