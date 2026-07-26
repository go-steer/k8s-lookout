---
title: Install
description: Container images (default and -gke), cosign verification, go install, and the entrypoint / image-swap compatibility contract.
sidebar:
  order: 1
---

## Container images

Images are published at `ghcr.io/go-steer/lookout` — multi-arch
(amd64 + arm64), distroless static, running as nonroot, Sigstore-signed:

```sh
docker pull ghcr.io/go-steer/lookout:latest       # default: GCP-free, runs on any conformant cluster
docker pull ghcr.io/go-steer/lookout:latest-gke   # same binary + GKE/GCP provider (-tags allproviders)
```

Which flavor you need:

- **Default (`:latest`, `:vX.Y.Z`)** — links zero GCP SDKs, by design
  (a conformance test in CI keeps it that way). Most of the suite is pure
  `client-go`: the entire `triage` group, `state edges|webhooks|volumes`,
  `stab drift|drain`, `bundle`, `health`, `net probe`, and the sentinel
  sources `k8s-events`, `object-state`, `rollout`, `saturation`,
  `degradation`, `expiry`, and `token-burn`. Provider-gated commands in
  this image never break or lie — they emit an explicit
  `cloud.unavailable` finding and exit 0 (see
  [Troubleshooting](/operations/troubleshooting/)).
- **`-gke` (`:latest-gke`, `:vX.Y.Z-gke`)** — the same binary compiled
  with the GKE/GCP cloud provider. Required for the `cloud` command
  group, `state wi`, the `perf probe` metric packs, the `quota` source,
  and the capacity source's GKE scale-decision sub-source. Same flags,
  same signing; only the compiled-in cloud provider differs. Project-tier
  deployments (the one sentinel per GCP project that enables the `quota`
  source) must pin this flavor — `--sources=…,quota` in the default image
  refuses at startup, loudly and correctly.

### Verify signatures

```sh
cosign verify ghcr.io/go-steer/lookout:vX.Y.Z \
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

`cosign verify` works identically against the `-gke` tags.

## From source

Go 1.26+:

```sh
go install github.com/go-steer/k8s-lookout/cmd/lookout@latest
```

That builds the GCP-free default. For a provider-enabled binary, build
with tags — `-tags gke` for the GKE provider alone, `-tags allproviders`
for everything (what the `-gke` image ships):

```sh
go build -tags allproviders ./cmd/lookout
```

## Entrypoint and image-swap compatibility

The image's entrypoint is `["/lookout", "watch"]`, so a Deployment's bare
`args:` splice in behind `watch`. This is a frozen compatibility
contract: the image is a drop-in swap for existing
`ghcr.io/go-steer/k8s-event-watcher` deployments — same flags, same exit
codes, same `k8s_event_watcher_*` metric names, byte-identical
`k8s-event` inject payloads. The M0 exit check
([`docs/milestones/M0.md`](https://github.com/go-steer/k8s-lookout/blob/main/docs/milestones/M0.md))
verified a predecessor deployment running with only the image line
changed.

To run a read-path command from the image (rather than the sentinel),
override the entrypoint — e.g. `--entrypoint /lookout` with `docker run`,
or `command: ["/lookout"]` in a pod spec. On a workstation the natural
path is the plain binary: every read command works against your current
kubeconfig context, which is the next page.
