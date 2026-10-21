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

# Phase 1: run kube-agents' fleet-audit collectors in place, exactly as
# the cron prompts invoke them, against $COMPARE_PROJECT. No install:
# the scripts are stdlib-only, take --project, and self-wire their one
# sibling import. Every kubectl/gcloud they spawn goes through the
# tracing shims; every write goes under $COMPARE_OUT (see lib.sh ka_env).
#
# Output: $COMPARE_OUT/ka/<stream>.{out,err,time,rc,ms} and
#         $COMPARE_OUT/trace/ka-<stream>.jsonl

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

: "${COMPARE_OUT:?}" "${KUBE_AGENTS_DIR:?}" "${COMPARE_PROJECT:?}"
ka_env
export COMPARE_SHIM_TMP="$COMPARE_OUT/tmp"
mkdir -p "$COMPARE_SHIM_TMP" "$COMPARE_OUT/ka"

FA="$KUBE_AGENTS_DIR/agents/platform/skills/fleet-audit/scripts"
NET="$KUBE_AGENTS_DIR/agents/platform/skills/gcp-networking-fabric-audit/scripts"
GCE="$KUBE_AGENTS_DIR/agents/platform/skills/gce-compute-fleet-audit/scripts"

# <stream-id> <script> <args...>; order matches the cron day, slowest last.
run_stream() {
  local stream="$1"; shift
  log "▸ kube-agents $stream: $*"
  with_trace "ka-$stream"
  timed "$COMPARE_OUT/ka/$stream" "$@"
}

ONLY="${COMPARE_ONLY_STREAMS:-}"
want() { [[ -z "$ONLY" || ",$ONLY," == *",$1,"* ]]; }

want compliance-audit && run_stream compliance-audit \
  python3 "$FA/collect.py" compliance-audit --project "$COMPARE_PROJECT"
want obtainability-audit && run_stream obtainability-audit \
  python3 "$FA/collect.py" obtainability-audit --project "$COMPARE_PROJECT"
want ai-security-audit && run_stream ai-security-audit \
  python3 "$FA/collect.py" ai-security-audit --project "$COMPARE_PROJECT"
want gce-compute-fleet-audit && run_stream gce-compute-fleet-audit \
  python3 "$GCE/compute_fleet_audit.py" --project-id "$COMPARE_PROJECT"
want gcp-networking-fabric-audit && run_stream gcp-networking-fabric-audit \
  python3 "$NET/networking_audit.py" --project-id "$COMPARE_PROJECT"
want stockout-prevention && run_stream stockout-prevention \
  python3 "$FA/fleet_stockout.py" --project "$COMPARE_PROJECT"
want security-patch-orchestrator && run_stream security-patch-orchestrator \
  python3 "$FA/patch_readiness.py" --project "$COMPARE_PROJECT"
want fleet-consistency-drift && run_stream fleet-consistency-drift \
  python3 "$FA/fleet_drift.py" --project "$COMPARE_PROJECT"
want upgrade-retrospective && run_stream upgrade-retrospective \
  python3 "$FA/upgrade_retrospective.py" --project "$COMPARE_PROJECT" --dry-run --no-report
# Reads a week of Cloud Monitoring per workload: last, and bounded.
want fleet-wide-cost-analysis && run_stream fleet-wide-cost-analysis \
  timeout "${COMPARE_WASTE_TIMEOUT:-45m}" python3 "$FA/fleet_waste.py" --project "$COMPARE_PROJECT"

log "kube-agents phase done: $(ls "$COMPARE_OUT"/ka/*.rc | wc -l) streams; kubeconfigs: $(kubeconfigs | wc -l)"
