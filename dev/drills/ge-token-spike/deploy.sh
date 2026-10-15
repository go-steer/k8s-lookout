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

# Deploy the #698 spike server to Cloud Run. See README.md for the whole
# runbook; this is its step 3.
#
# Required: PROJECT, SPIKE_CLIENT_ID, SPIKE_MEMBERS, SPIKE_CLUSTER
# Optional: REGION (us-central1)
#
#   ./deploy.sh          deploy (or redeploy) the service
#   ./deploy.sh delete   delete the service and its service account
set -euo pipefail
cd "$(dirname "$0")"

: "${PROJECT:?set PROJECT}"
REGION="${REGION:-us-central1}"
SERVICE=ge-token-spike
SA="${SERVICE}@${PROJECT}.iam.gserviceaccount.com"

if [[ "${1:-}" == "delete" ]]; then
  gcloud run services delete "$SERVICE" --project "$PROJECT" --region "$REGION" --quiet || true
  gcloud iam service-accounts delete "$SA" --project "$PROJECT" --quiet || true
  echo "deleted ${SERVICE}. Delete the data store in Gemini Enterprise and the OAuth client by hand (README step 6)."
  exit 0
fi

: "${SPIKE_CLIENT_ID:?set SPIKE_CLIENT_ID (the OAuth client from README step 2)}"
: "${SPIKE_MEMBERS:?set SPIKE_MEMBERS (comma-separated emails or @domain)}"
: "${SPIKE_CLUSTER:?set SPIKE_CLUSTER (projects/P/locations/L/clusters/C)}"

NUMBER="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')"
URL="https://${SERVICE}-${NUMBER}.${REGION}.run.app"

gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  --project "$PROJECT"

# The service reads everything as the caller, with the caller's token. Its
# own identity gets no roles: if a check passes, it was the person's token.
if ! gcloud iam service-accounts describe "$SA" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SERVICE" --project "$PROJECT" \
    --display-name "lookout #698 Gemini Enterprise token spike (no roles)"
fi

# --no-invoker-iam-check: Gemini Enterprise calls without a Cloud Run
# identity token; the server checks each caller's Google token itself.
gcloud run deploy "$SERVICE" --project "$PROJECT" --region "$REGION" \
  --source . \
  --service-account "$SA" \
  --no-invoker-iam-check \
  --max-instances 1 \
  --set-env-vars "^|^SPIKE_CLIENT_ID=${SPIKE_CLIENT_ID}|SPIKE_MEMBERS=${SPIKE_MEMBERS}|SPIKE_CLUSTER=${SPIKE_CLUSTER}|SPIKE_RESOURCE=${URL}/mcp"

echo
echo "MCP server URL (for the data store): ${URL}/mcp"
echo "Check:  curl -s ${URL}/.well-known/oauth-protected-resource/mcp"
