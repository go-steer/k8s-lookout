---
title: Portability & providers
description: What runs on any conformant Kubernetes cluster, what needs a cloud provider, and how absence degrades loudly instead of silently.
sidebar:
  order: 6
---

You do not need GKE — or any cloud — to use `lookout`: around 80% of the
suite is pure `client-go` and works on **any conformant Kubernetes
cluster**, a local kind cluster included. This page explains exactly
which pieces do need a cloud provider, and how a missing capability
announces itself plainly instead of erroring out or staying silent —
the surface is small, enumerable, and walled off behind a provider
boundary.

## The split

Portable everywhere (vanilla Kubernetes, kind included):

- the entire `triage` group, `state edges|webhooks|volumes`,
  `stab drift|drain`, `bundle`, `health`, `net probe`;
- the topology graph and everything built on it;
- the sentinel sources `k8s-events`, `object-state`, `rollout`,
  `saturation` (metrics.k8s.io + kubelet stats), `degradation`, `expiry`,
  and `token-burn`.

Provider-gated (GKE/GCP today):

- the `cloud` command group (`stockout|orphans|ipspace|quota`);
- `state wi` (Workload Identity verification) and the `perf probe` metric
  packs (Cloud Monitoring is the only metrics backend so far);
- the `quota` source, and one of the capacity source's sub-seams (below).

## The boundary is architectural, and tested

Check and source code never imports cloud SDKs; cloud-touching
functionality asks a `Provider` interface for capabilities (metrics, quota,
stockout, orphans, ipspace, workload identity). The **default build links
no cloud SDK at all** — a CI test builds the default binary and scans its
symbol table for GCP package markers, so the isolation is pinned, not
aspirational. The GKE provider compiles in behind build tags
(`gke` / `allproviders`).

Released images follow the same split, from one Dockerfile with only
`BUILD_TAGS` differing: `ghcr.io/go-steer/lookout:<version>` (the
provider-free default) and `ghcr.io/go-steer/lookout:<version>-gke` (with
the GKE provider compiled in).

## No provider → explicit, not broken

A missing provider is never a crash and never silence:

- Provider-gated commands exit 0 with an explicit finding —
  `kind=cloud.unavailable reason=CapabilityUnavailable … provider=none` —
  and a summary-line marker, so an agent can tell "no cloud provider here"
  from "swept and clean".
- Provider-gated sources refuse loudly at startup: `--sources=…,quota` in
  the default binary names the missing provider instead of running an empty
  watch. The same fail-loudly rule applies to RBAC: a source whose scope the
  ServiceAccount cannot support is a named startup error, never a silently
  empty informer.
- `lookout mcp` and `--help` mark or omit unavailable commands.

## Degradation inside features

The boundary is per-capability, not per-command:

- The **capacity source** runs on the upstream-portable cluster-autoscaler
  seams everywhere — `NotTriggerScaleUp`/`TriggeredScaleUp` Events and the
  `cluster-autoscaler-status` ConfigMap — so scale-up failures fire on any
  CA-running cluster. The third seam, structured provider scale decisions
  naming the *why* (`GCE_STOCKOUT`, `GCE_QUOTA_EXCEEDED`), lights up only
  with the GKE provider. On a vanilla cluster the startup log says exactly
  that, and the portable seams keep firing.
- `triage top` answers point-in-time from metrics.k8s.io everywhere;
  `--history` needs the provider metrics backend and says so.
- The `health` scorecard's control-plane category reports `unavailable`
  with a reason on clusters without provider metrics; the other categories
  answer regardless.

## Other clouds

A future EKS/AKS provider is a new provider implementation — IRSA is the
`state wi` analog, capacity-insufficiency events the stockout analog — with
no engine, schema, or skill changes. A Prometheus metrics backend for
`perf probe` is deferred until a non-GKE consumer materializes, but the
pack queries avoid Cloud-Monitoring-only constructs where a PromQL
equivalent is obvious.
