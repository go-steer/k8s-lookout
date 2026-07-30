#!/usr/bin/env bash
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

# compose-release-notes.sh TAG NOTES_PATH [CHANGELOG]
#
# Adapted from core-agent's dev/release/compose-release-notes.sh
# (issue #146: lookout had registry tags but no GitHub Releases, so
# `gh release view` returned null and versions were undiscoverable).
# Lookout's simplification: the CHANGELOG discipline guarantees every
# release a `## [X.Y.Z] - date` section (the `chore: cut X.Y.Z`
# commit), so the section IS the notes — no git-cliff fallback, and a
# missing section fails the release loudly instead of shipping an
# empty body. No dev.N pre-release machinery either: lookout cuts
# straight GA minors (docs/release-process.md).
set -euo pipefail

TAG="${1:?TAG required (vX.Y.Z)}"
NOTES="${2:?NOTES_PATH required}"
CHANGELOG="${3:-CHANGELOG.md}"
REPO_URL="${REPO_URL:-https://github.com/go-steer/k8s-lookout}"

if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "::error::expected vX.Y.Z, got: ${TAG}" >&2
  exit 1
fi
VERSION="${TAG#v}"

extract_section() {
  awk -v ver="$VERSION" '
    $0 ~ "^## \\[" ver "\\]" { in_section=1; next }
    in_section && /^## \[/    { exit }
    in_section                { print }
  ' "$CHANGELOG"
}

BODY="$(extract_section)"
if [[ -z "${BODY//[[:space:]]/}" ]]; then
  echo "::error::CHANGELOG section for ${VERSION} not found in ${CHANGELOG} — cut the release (chore: cut ${VERSION}) before tagging" >&2
  exit 1
fi

{
  echo "$BODY"
  cat <<FOOTER

---

## Images

Two multi-arch (amd64/arm64) flavors, cosign-signed (Sigstore keyless):

\`\`\`bash
docker pull ghcr.io/go-steer/lookout:${TAG}        # GCP-free default (DESIGN.md §2)
docker pull ghcr.io/go-steer/lookout:${TAG}-gke    # -tags allproviders (GKE/GCP provider)

cosign verify ghcr.io/go-steer/lookout:${TAG} \\
  --certificate-identity-regexp '^https://github.com/go-steer/k8s-lookout' \\
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
\`\`\`

Full changelog: ${REPO_URL}/blob/${TAG}/CHANGELOG.md
FOOTER
} > "$NOTES"

echo "composed $(wc -l < "$NOTES") lines of notes for ${TAG} → ${NOTES}"
