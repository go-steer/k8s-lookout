---
title: Install
description: Get the lookout binary — go install for the workstation, container images (default and -gke) for cluster deployments — plus cosign verification and the image-swap compatibility contract.
sidebar:
  order: 1
---

One binary covers all three surfaces — the CLI, the MCP server
(`lookout mcp`), and the sentinel (`lookout watch`). Installing means
getting that binary where you need it:

- **On a workstation** — for the CLI and the MCP server — download a
  prebuilt binary from the
  [latest release](https://github.com/go-steer/k8s-lookout/releases/latest)
  (v0.13.0 and later; Linux and macOS on amd64/arm64, Windows on
  amd64 — Windows archives are `.zip`):

  ```sh
  gh release download -R go-steer/k8s-lookout -p 'lookout_*_linux_amd64.tar.gz'
  tar -xzf lookout_*_linux_amd64.tar.gz && sudo install lookout /usr/local/bin/
  ```

  The `lookout-gke_*` assets are the same binary with the GKE/GCP
  provider compiled in (see the flavor guide below). Or build from
  source with Go 1.26+:

  ```sh
  go install github.com/go-steer/k8s-lookout/cmd/lookout@latest
  ```

  Either way that is the whole install; `lookout health` against your
  current kubeconfig works immediately
  ([First reads](/getting-started/first-run/) is the next page).

- **In a cluster** — for the sentinel — use the container images below;
  [Deploy the sentinel](/getting-started/deploy/) applies the shipped
  manifests with one `kubectl apply -k`, no clone needed.

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

`cosign verify` works identically against the `-gke` tags. Release
binaries are covered by a keyless-signed checksums file attached to
each release:

```sh
cosign verify-blob lookout_vX.Y.Z_SHA256SUMS \
  --bundle lookout_vX.Y.Z_SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
sha256sum -c lookout_vX.Y.Z_SHA256SUMS --ignore-missing
```

**GKE Autopilot:** both flavors run on Autopilot, with one platform
limitation — Warden denies `nodes/proxy` to every principal, so the
saturation source's PVC dimension is disabled there (CPU/memory
forecasting still works). The sentinel reports this at startup; see
[Concepts → Portability](/concepts/portability/#gke-autopilot).

## Building with a cloud provider

`go install` builds the GCP-free default. For a provider-enabled binary,
build with tags — `-tags gke` for the GKE provider alone,
`-tags allproviders` for everything (what the `-gke` image ships):

```sh
go build -tags allproviders ./cmd/lookout
```

## Entrypoint

The image's entrypoint is `["/lookout", "watch"]`, so a Deployment's bare
`args:` splice in behind `watch` (no explicit `command:` needed). The
sentinel's core flag surface is pinned by CI contract tests, so an
existing deployment can upgrade the image with zero config change.

To run a read-path command from the image (rather than the sentinel),
override the entrypoint — e.g. `--entrypoint /lookout` with `docker run`,
or `command: ["/lookout"]` in a pod spec. On a workstation the natural
path is the plain binary: every read command works against your current
kubeconfig context, which is the next page.
