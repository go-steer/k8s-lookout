# Release process

Adapted from core-agent's (`core-agent/docs/release-process.md`),
minus the dev.N pre-release machinery — lookout cuts straight GA
minors. Adopted after the Autopilot first-run report (#144/#146):
registry tags existed but `gh release view` returned null, the deploy
manifest pin sat eleven releases stale, and `:latest` served the
wrong flavor with nothing to catch it.

## Cutting vX.Y.Z

1. **Cut commit** (`chore: cut X.Y.Z`), one PR:
   - CHANGELOG.md: move `[Unreleased]` into `## [X.Y.Z] - <date>`
     with a summary paragraph (this section becomes the Release
     notes verbatim — compose-release-notes.sh extracts it and fails
     the release if it is missing).
   - `deploy/51-deployment-watcher.yaml`: bump the image pin to
     `vX.Y.Z`. The release workflow's preflight guard refuses to tag
     on a stale pin.
   - `deploy/chart/Chart.yaml`: `appVersion: "vX.Y.Z"` and
     `version: X.Y.Z` (the chart version tracks the app version, no
     leading `v`). `dev/tools/verify-helm-parity` fails CI on any
     disagreement between these two and the deploy/51 pin.
   - `docs/site/src/content/docs/getting-started/deploy.md`: bump the
     `?ref=vX.Y.Z` in the `kubectl apply -k` line, and the
     `--version X.Y.Z` + `charts/lookout:X.Y.Z` in the Helm block.
     `grep -rn 'v\?X\.Y\.(Z-1)' --include='*.md' --include='*.yaml'`
     catches the rest — the ServiceMonitor `?ref=` in
     `operations/observability.md`, the `image.tag` example in
     `deploy/chart/values.yaml`, the issue-template placeholders.
     Files under `docs/assessments/` and `docs/milestones/` are dated
     records and stay as written.
2. **Tag**: `git tag vX.Y.Z <merge-commit> && git push origin vX.Y.Z`.
3. **The workflow does the rest** (`.github/workflows/release-images.yml`):
   - preflight: deploy/51 pin == tag;
   - both flavors build (multi-arch, cosign-signed, version + commit
     + date stamped into `internal/version`), publish, and verify
     (signatures by tag, smoke run, entrypoint pin);
   - flavor-divergence guard: `:latest` ≠ `:latest-gke`, each equal
     to its version tag (#144);
   - prebuilt binaries (linux amd64/arm64, darwin amd64/arm64,
     windows amd64 × both flavors) cross-compiled with the same
     internal/version stamp, smoke-tested, checksummed, the
     SHA256SUMS signed keyless (`cosign sign-blob`);
   - the Helm chart packaged and pushed as an OCI artifact to
     `ghcr.io/go-steer/charts/lookout`, cosign-signed with the same
     keyless identity, then pulled back and rendered to prove the
     published copy templates the tagged image (#287);
   - GitHub Release created with the CHANGELOG section + image
     footer (`dev/release/compose-release-notes.sh`), binaries
     attached as assets.
4. **Post-release bump**: on main, set
   `internal/version/version.go` `Version = "vX.(Y+1).0-dev"`.
   `dev/ci/presubmits/verify-version-fallback` fails CI until done.

## Verifying a release

```bash
gh release view vX.Y.Z          # 12 assets: 5 platforms × 2 flavors + SHA256SUMS + .sigstore.json
cosign verify ghcr.io/go-steer/lookout:vX.Y.Z \
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
cosign verify-attestation ghcr.io/go-steer/lookout:vX.Y.Z \
  --type spdxjson \
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  | jq -r '.payload | @base64d | fromjson | .predicate.name'   # 2 lines: amd64, arm64
docker buildx imagetools inspect ghcr.io/go-steer/lookout:latest      # == vX.Y.Z digest
docker buildx imagetools inspect ghcr.io/go-steer/lookout:latest-gke  # == vX.Y.Z-gke digest
helm show chart oci://ghcr.io/go-steer/charts/lookout --version X.Y.Z
cosign verify ghcr.io/go-steer/charts/lookout:X.Y.Z \
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
cosign verify-blob lookout_vX.Y.Z_SHA256SUMS \
  --bundle lookout_vX.Y.Z_SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```
