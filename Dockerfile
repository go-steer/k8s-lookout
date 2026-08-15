# syntax=docker/dockerfile:1.7
#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Multi-stage distroless build for the `lookout` multicall binary.
# Mirrors core-agent's Dockerfile conventions (alpine builder,
# distroless/static final stage, version stamped via -ldflags).
#
# ENTRYPOINT: the sentinel (`lookout watch`) is the container's default
# command, so this image's ENTRYPOINT is ["/lookout", "watch"] and a
# Deployment's bare-flag `args:` splice in after `watch` (no explicit
# `command:` needed). Other subcommands (`lookout version`, and the
# read-path verbs) are reachable by overriding the entrypoint
# (`command:` in k8s, `--entrypoint` in docker run).
#
# The Go toolchain version is passed in from `go.mod` via GO_VERSION,
# so the build image automatically tracks the project's Go version
# without a hardcoded duplicate that can drift.

# ---- Builder stage ----
# Alpine base for a smaller builder image — faster CI cold-cache
# pulls. CGO_ENABLED=0 below means we don't care about the builder's
# libc (musl vs glibc); we only ship the binary.
ARG GO_VERSION=1.26.6
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

# Cache module downloads in a separate layer so iterative source
# changes don't re-fetch dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Bring in the rest of the source.
COPY . .

# Build-time inputs. Defaults are appropriate for `docker build`
# without explicit args (produces a dev-flavored binary); the
# release-images.yml GitHub Action overrides them all.
ARG VERSION=v0.0.0-dev

# Commit SHA + build date, stamped alongside VERSION (#146). The
# defaults mark a build where nothing was injected; internal/version
# then falls back to Go's embedded VCS metadata where available.
ARG COMMIT=none
ARG BUILD_DATE=unknown

# Go build tags. Empty (default) produces the GCP-free binary the §2
# conformance test pins — no cloud SDK linked, `--sources=…,quota`
# refuses loudly at startup. Release builds also publish a
# BUILD_TAGS=allproviders flavor (ghcr.io/go-steer/lookout:<v>-gke)
# for project-tier quota deployments (M4 drill observation 5).
ARG BUILD_TAGS=""

# Cross-compile target. Set by `docker buildx build --platform`
# when building multi-arch images. Without buildx these default to
# the host's GOOS/GOARCH.
ARG TARGETOS
ARG TARGETARCH

# CGO_ENABLED=0 is mandatory — we want a fully-static binary that
# drops into distroless/static without any glibc/musl runtime.
ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH}

# -s -w strips DWARF + symbol table to shrink the binary by ~30%.
# -trimpath strips the absolute paths in stack traces (which would
# otherwise leak the build host's filesystem layout).
# The internal/version stamps are the release identity `lookout
# version` / `--version` report and the sentinel logs at startup
# (#146: version + SHA + date, core-agent's scheme).
RUN go build \
    -tags "${BUILD_TAGS}" \
    -ldflags "-s -w \
      -X github.com/go-steer/k8s-lookout/internal/version.Version=${VERSION} \
      -X github.com/go-steer/k8s-lookout/internal/version.Commit=${COMMIT} \
      -X github.com/go-steer/k8s-lookout/internal/version.Date=${BUILD_DATE}" \
    -trimpath \
    -o /out/lookout \
    ./cmd/lookout

# ---- Final stage ----
# distroless/static-debian12 carries only the bits needed to run a
# static Go binary (CA certs, /etc/passwd with the nonroot user,
# tzdata). No shell, no package manager, no userland — minimal
# attack surface.
#
# :nonroot tag pre-creates UID 65532 + GID 65532 (user "nonroot")
# and sets USER, so the binary runs unprivileged out of the box.
FROM gcr.io/distroless/static-debian12:nonroot

# OCI image labels for registry tooling + GHCR metadata pages.
# Per-release `title` / `description` / version labels are set by
# docker/metadata-action in .github/workflows/release-images.yml and
# win over anything set here.
LABEL org.opencontainers.image.source="https://github.com/go-steer/k8s-lookout" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /out/lookout /lookout

WORKDIR /workspace

# nonroot is already set by the base image's :nonroot tag, but we
# redeclare for clarity and to insulate against any future base-image
# default change.
USER nonroot:nonroot

# ["/lookout", "watch"] — NOT bare ["/lookout"] — so a Deployment's
# bare-flag `args:` splice in behind `watch`. See the header comment.
ENTRYPOINT ["/lookout", "watch"]
