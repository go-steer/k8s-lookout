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

# One-time Google Cloud and cluster setup for .github/workflows/
# deploy-demo.yml; see README.md beside this file. Safe to re-run: each
# step skips what already exists.
#
# Required environment:
#   PROJECT     the Google Cloud project holding the cluster
#   CLUSTER     the GKE cluster name
#   LOCATION    its region or zone
#   NAMESPACE   where lookout runs (created here)
# Optional:
#   POOL        Workload Identity pool (default github-actions)
#   PROVIDER    provider in it (default k8s-lookout-demo)
#   SA_NAME     deploy service account (default lookout-demo-deploy)
#   REPO        GitHub repository (default go-steer/k8s-lookout)
#   ENVIRONMENT GitHub environment (default demo)
set -euo pipefail

: "${PROJECT:?}" "${CLUSTER:?}" "${LOCATION:?}" "${NAMESPACE:?}"
POOL="${POOL:-github-actions}"
PROVIDER="${PROVIDER:-k8s-lookout-demo}"
SA_NAME="${SA_NAME:-lookout-demo-deploy}"
REPO="${REPO:-go-steer/k8s-lookout}"
ENVIRONMENT="${ENVIRONMENT:-demo}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
number="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')"
repo_id="$(gh api "repos/$REPO" -q .id)"
sa="$SA_NAME@$PROJECT.iam.gserviceaccount.com"

# The pool is shared with other repositories' workflows; each gets its
# own provider, restricted to its own repository.
if ! gcloud iam workload-identity-pools describe "$POOL" --location=global --project="$PROJECT" >/dev/null 2>&1; then
  gcloud iam workload-identity-pools create "$POOL" --location=global \
    --project="$PROJECT" --display-name="GitHub Actions"
fi

# Only this repository (by its numeric ID, which a rename or a
# re-created repository of the same name does not keep) and only jobs
# running in the deployment environment, which carries the protection
# rules.
if ! gcloud iam workload-identity-pools providers describe "$PROVIDER" \
  --workload-identity-pool="$POOL" --location=global --project="$PROJECT" >/dev/null 2>&1; then
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
    --workload-identity-pool="$POOL" --location=global --project="$PROJECT" \
    --issuer-uri=https://token.actions.githubusercontent.com \
    --attribute-mapping=google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_id=assertion.repository_id,attribute.environment=assertion.environment \
    --attribute-condition="assertion.repository_id == '$repo_id' && assertion.environment == '$ENVIRONMENT'"
fi

if ! gcloud iam service-accounts describe "$sa" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --project="$PROJECT" \
    --display-name="lookout demo deploy (GitHub Actions)"
fi

# The workflow may act as the service account; nothing else may.
gcloud iam service-accounts add-iam-policy-binding "$sa" --project="$PROJECT" \
  --role=roles/iam.workloadIdentityUser \
  --member="principal://iam.googleapis.com/projects/$number/locations/global/workloadIdentityPools/$POOL/subject/repo:$REPO:environment:$ENVIRONMENT" \
  --condition=None >/dev/null

# Enough to fetch the cluster's endpoint and credentials; it grants no
# access to any Kubernetes object. That comes from the namespace Role.
gcloud projects add-iam-policy-binding "$PROJECT" \
  --role=roles/container.clusterViewer --member="serviceAccount:$sa" \
  --condition=None >/dev/null

gcloud container clusters get-credentials "$CLUSTER" --location="$LOCATION" --project="$PROJECT"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
sed "s/DEPLOY_SA/$sa/" "$here/rbac.yaml" | kubectl -n "$NAMESPACE" apply -f -

cat <<EOF

Done. The demo environment's variables:
  WIF_PROVIDER=projects/$number/locations/global/workloadIdentityPools/$POOL/providers/$PROVIDER
  DEPLOY_SERVICE_ACCOUNT=$sa
  GCP_PROJECT=$PROJECT
  GKE_CLUSTER=$CLUSTER
  GKE_LOCATION=$LOCATION
  NAMESPACE=$NAMESPACE
EOF
