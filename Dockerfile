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
# ENTRYPOINT compatibility (M0 exit criterion): the predecessor image,
# ghcr.io/go-steer/k8s-event-watcher, set ENTRYPOINT to the watcher
# binary itself and its deployment manifests pass bare flags via
# `args:` (no explicit `command:`). To let those deployments swap
# images with ZERO config change, this image's ENTRYPOINT is
# ["/lookout", "watch"] — the container-args flags splice in after
# `watch` exactly as they used to splice in after the old binary.
# Other subcommands (`lookout version`, and the M1 read-path verbs)
# are reachable by overriding the entrypoint (`command:` in k8s,
# `--entrypoint` in docker run).
#
# The Go toolchain version is passed in from `go.mod` via GO_VERSION,
# so the build image automatically tracks the project's Go version
# without a hardcoded duplicate that can drift.

# ---- Builder stage ----
# Alpine base for a smaller builder image — faster CI cold-cache
# pulls. CGO_ENABLED=0 below means we don't care about the builder's
# libc (musl vs glibc); we only ship the binary.
ARG GO_VERSION=1.26.3
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
# -X main.version stamps the release identity `lookout version` reports.
RUN go build \
    -ldflags "-s -w -X main.version=${VERSION}" \
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

# ["/lookout", "watch"] — NOT bare ["/lookout"] — so existing
# k8s-event-watcher deployments, whose `args:` are bare watcher flags,
# keep working with only the image line changed. See the header
# comment.
ENTRYPOINT ["/lookout", "watch"]
