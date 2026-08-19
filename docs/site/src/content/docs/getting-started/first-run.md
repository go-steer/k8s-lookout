---
title: First reads
description: The read-path against your current kubeconfig — lookout scan, lookout audit, lookout health and lookout triage delta, with real captured output.
sidebar:
  order: 2
---

Every read command works against your current kubeconfig context. Nothing
is deployed, nothing is mutated — the read-path only ever gets, lists, and
watches.

## Start here: `lookout scan`

If you know something is wrong but not what, this is the first call. It
runs every target-free incident check in one invocation — broken
workloads, dead admission webhooks, stuck volumes and PVCs, rejected
Gateway routes, config drift — then drills into the dependency edges of
whatever it flagged. No target, no flags, nothing deployed:

```sh
lookout scan
```

```console
kind=pod.imagepull severity=critical namespace=lookout-demo kind_of_object=Pod name=api-f599769c4-swfxx reason=ErrImagePull message="failed to pull and unpack image \"ghcr.io/go-steer/lookout-examples-nosuch:v0\"…" fingerprint=sha256:e95154c7… check="triage delta" container=api image=ghcr.io/go-steer/lookout-examples-nosuch:v0
kind=pod.restarts severity=warning namespace=lookout-demo kind_of_object=Pod name=worker-7cd49696bf-9sbf5 reason=ExcessiveRestarts fingerprint=sha256:e094a6ee… check="triage delta" container=worker restarts=6
kind=workload.rollout severity=critical namespace=lookout-demo kind_of_object=Deployment name=worker reason=RolloutIncomplete fingerprint=sha256:f954cac0… check="triage delta" desired=1 ready=0 updated=1 available=0
…
kind=crd.unavailable severity=info reason=APIGroupNotServed message="Gateway API is not installed: the gateway.networking.k8s.io/v1 API group is not served by this cluster…" check="state gateway" api_group=gateway.networking.k8s.io/v1
kind=cloud.unavailable severity=info reason=CapabilityUnavailable message="state wi needs the provider workload-identity capability: no cloud provider configured" check="state wi" capability=workload-identity provider=none
kind=edge.missing_ref severity=critical namespace=lookout-demo kind_of_object=ConfigMap name=missing-config reason=FailedMount message="configmap missing-config not found (volume config)" check="state edges" workload=Deployment/lookout-demo/mounter volume=config pods=1
scanned=330 findings=9 elapsed=442ms unavailable="state gateway,state wi" detection=none detection_reason=no-majority-manager candidate=kubectl-client-side-apply share=45% checks=7 skipped=audit,cloud,perf drilldown=3
```

(Real output against a kind cluster with three faults staged, abridged.)
One call, and four things are visible that no single command would have
given you:

- **Stage 1 named the incidents** — a bad image tag, a container
  restarting, the rollouts those two are holding up. Every finding is
  stamped `check=<command>`, which is also the command to run for the
  detail behind it, so the output is both a worklist and a set of next
  moves.
- **Stage 2 drilled in.** `drilldown=3` means three flagged workloads
  had their dependency edges verified, which is where
  `edge.missing_ref` came from: nothing in stage 1 knew *why* `mounter`
  could not start, and the answer is a ConfigMap that does not exist.
- **What could not run said so.** No Gateway API CRDs and no cloud
  provider, both reported as `info` findings and rolled up into
  `unavailable=` — an empty scan means "nothing is wrong", never
  "nothing ran".
- **`skipped=audit,cloud,perf`** names the groups that are off by
  default, so they stay discoverable while off.

[What `lookout scan` finds](/detect/scan/) lists every check it runs and
every kind it can emit, grouped by stage.

`scan` reports **incidents**: things broken now, which clear themselves
when fixed. The posture sweep is one flag away —
`lookout scan --include=audit`, or on its own:

```sh
lookout audit workloads -A
```

```console
kind=audit.no_pdb severity=warning namespace=lookout-demo kind_of_object=Deployment name=web reason=NoPodDisruptionBudget message="2 replicas and no PodDisruptionBudget selecting them: the eviction API will let a drain or upgrade take all 2 at once" fingerprint=sha256:7fe356f9… replicas=2 namespace_pdbs=1
kind=audit.single_replica severity=warning namespace=lookout-demo kind_of_object=Deployment name=worker reason=SingleReplica message="spec.replicas=1: a node drain, upgrade, or eviction takes the workload fully down, and no PodDisruptionBudget can prevent that" fingerprint=sha256:8a0b879a… replicas=1
kind=audit.no_spread severity=info namespace=lookout-demo kind_of_object=Deployment name=api reason=NoTopologySpread message="2 replicas with no topologySpreadConstraints and no pod anti-affinity: nothing in the spec stops the scheduler putting them all on one node…" fingerprint=sha256:994489a9… replicas=2
…
scanned=8 findings=19 elapsed=281ms pdbs=1 hpas=0 nodes=3 workloads=6/0/2/0
```

That answers a different question — *what has no safety net while it is
still healthy*. Note what it did **not** say: none of these workloads is
unhealthy, and none of these findings will clear on its own. `web` has
two replicas and no budget protecting them — `namespace_pdbs=1` says the
namespace has a PDB, just not one selecting `web`, which is the mistake
worth catching. [What `lookout audit` checks](/detect/audit/) is its
coverage map.

The rest of this page is the individual commands behind those two.

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

## Everything abnormal, in one pass

`lookout triage delta` is `scan`'s first and broadest stage, and useful
on its own: broken workloads, aged Pending pods, node pressure,
gridlocked PDBs, degraded kube-system add-ons, quotas at their limits —
one pass, only the abnormal:

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
