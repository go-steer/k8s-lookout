# Running the examples on GKE

The scenarios are cluster-agnostic k8s: everything except
`node-failure` (kind-only `docker stop`) runs unmodified on a GKE
cluster.

> **STAGING CLUSTERS ONLY.** The scenarios break workloads on
> purpose. Same rule as dev/drills: bring your own victim cluster,
> never one users depend on.

## Setup deltas from the kind quickstart

```sh
gcloud container clusters get-credentials <cluster> --zone <zone>
export LOOKOUT_EXAMPLES_CONTEXT="$(kubectl config current-context)"   # opt out of the kind guard
examples/sentinel/up          # GKE ships metrics-server — saturation stays enabled
kubectl apply -f examples/workloads/
examples/e2e crashloop image-pull failed-mount oom pending cert-expiry pdb-gridlock endpoints-empty bad-rollout
```

Notes:

- **Image**: the default `ghcr.io/go-steer/lookout:latest` (GCP-free)
  is right for these scenarios. Pin `:latest-gke` only when you want
  the provider-backed extras (`--sources=…,quota`, `cloud quota`,
  `state wi`, `perf probe`) — same flags, same signing.
- **No image pre-pull / side-load** — nodes pull from GHCR and Docker
  Hub; the first bad-rollout run pays one real registry pull, which is
  the drill fidelity dev/drills/bad-deploy.md wants anyway.
- **Sentinel placement**: the kind worker-pinning in sentinel/up is
  skipped automatically on non-kind contexts.
- **--project/--zone**: on the `-gke` image these are detected from
  metadata and stamped into every payload (zone participates in the
  fingerprint); on the default image pass them explicitly in
  sentinel/up's args if you want zone-scoped fingerprints.

## What to run from dev/drills instead

The GKE-only halves stay human-run runbooks, with store forensics and
timing capture the automated scenarios don't attempt:

- [`dev/drills/bad-deploy.md`](../../dev/drills/bad-deploy.md) — the
  real-registry bad-rollout, plus the `--at` post-mortem (blast radius
  at onset from a copied-out store).
- [`dev/drills/node-failure.md`](../../dev/drills/node-failure.md) —
  node death via node-pool resize.
- [`dev/drills/quota-exhaustion.md`](../../dev/drills/quota-exhaustion.md)
  — `quota.forecast` with the `-gke` image against real project quotas.
- [`dev/drills/memory-leak.md`](../../dev/drills/memory-leak.md) — the
  slow leaker for `saturation.forecast` (leading, not reactive).
