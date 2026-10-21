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

# Run kube-agents' fleet audits and lookout against the same fleet and
# compare what each found and what each cost. See README.md.
#
# Usage:
#   dev/compare/kube-agents/run.sh --kube-agents-dir DIR --project ID --out DIR \
#       [--lookout BIN] [--leeway BIN] [--phase roster|ka|lookout|prose|watchers|crosswalk|all]
#       [--only-streams a,b] [--only-lookout stem,stem] [--exclude-clusters name,name] [--waste-timeout 45m] [--lookout-timeout 5m]
#
# Phases needing network + gcloud credentials: ka, lookout, prose,
# watchers. roster and crosswalk are pure file processing.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/lib.sh"

# roster.py imports kube-agents' scripts by path; keep the bytecode out of
# their checkout (and ours).
export PYTHONDONTWRITEBYTECODE=1

PHASE=all
export KUBE_AGENTS_DIR="${KUBE_AGENTS_DIR:-}"
export COMPARE_PROJECT="${COMPARE_PROJECT:-}"
export COMPARE_OUT="${COMPARE_OUT:-}"
export LOOKOUT_BIN="${LOOKOUT_BIN:-}"

while (( $# )); do
  case "$1" in
    --kube-agents-dir) KUBE_AGENTS_DIR="$2"; shift 2 ;;
    --project) COMPARE_PROJECT="$2"; shift 2 ;;
    --out) COMPARE_OUT="$2"; shift 2 ;;
    --lookout) LOOKOUT_BIN="$2"; shift 2 ;;
    --leeway) export LEEWAY_BIN="$2"; shift 2 ;;
    --phase) PHASE="$2"; shift 2 ;;
    --only-streams) export COMPARE_ONLY_STREAMS="$2"; shift 2 ;;
    --only-lookout) export COMPARE_LOOKOUT_ONLY="$2"; shift 2 ;;
    --exclude-clusters) export COMPARE_EXCLUDE_CLUSTERS="$2"; shift 2 ;;
    --waste-timeout) export COMPARE_WASTE_TIMEOUT="$2"; shift 2 ;;
    --lookout-timeout) export COMPARE_LOOKOUT_TIMEOUT="$2"; shift 2 ;;
    -h|--help) sed -n '16,26p' "$0"; exit 0 ;;
    *) die "unknown flag: $1" ;;
  esac
done

[[ -n "$COMPARE_OUT" ]] || die "--out is required"
[[ -n "$KUBE_AGENTS_DIR" && -d "$KUBE_AGENTS_DIR/agents/platform" ]] || die "--kube-agents-dir must be a kube-agents checkout"
mkdir -p "$COMPARE_OUT"
COMPARE_OUT="$(cd "$COMPARE_OUT" && pwd)"
export COMPARE_OUT KUBE_AGENTS_DIR COMPARE_PROJECT LOOKOUT_BIN

phase() { [[ "$PHASE" == all || "$PHASE" == "$1" ]]; }

if phase roster; then
  log "▸ phase roster"
  python3 "$HERE/roster.py" --kube-agents-dir "$KUBE_AGENTS_DIR" --out "$COMPARE_OUT/roster.json"
fi
if phase ka; then
  [[ -n "$COMPARE_PROJECT" ]] || die "--project is required for the ka phase"
  log "▸ phase ka"
  "$HERE/ka-run.sh"
fi
if phase lookout; then
  [[ -x "$LOOKOUT_BIN" ]] || die "--lookout must name an executable built with -tags gke"
  log "▸ phase lookout"
  "$HERE/lookout-run.sh"
fi
if phase prose; then
  log "▸ phase prose"
  "$HERE/prose-run.sh"
fi
if phase watchers; then
  [[ -n "$COMPARE_PROJECT" ]] || die "--project is required for the watchers phase"
  log "▸ phase watchers"
  "$HERE/watchers-run.sh"
fi
if phase crosswalk; then
  log "▸ phase crosswalk"
  python3 "$HERE/crosswalk.py" --out "$COMPARE_OUT"
fi
log "done: $COMPARE_OUT"
