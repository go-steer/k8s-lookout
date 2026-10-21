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

# Shared helpers for the kube-agents comparison harness. Sourced by run.sh
# and the per-phase scripts; never executed directly.
#
# Every phase script expects these variables, exported by run.sh:
#   COMPARE_OUT       output root (everything the harness writes lands here)
#   KUBE_AGENTS_DIR   a kube-agents checkout
#   COMPARE_PROJECT   the GCP project both tools audit
#   LOOKOUT_BIN       a `lookout` binary built with -tags gke

set -euo pipefail

COMPARE_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export COMPARE_HERE

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# The isolation env for every kube-agents script: all of their writes --
# per-cluster kubeconfigs, scratch, ledgers, reports -- go under
# $COMPARE_OUT. Nothing lands in /opt/data, ~/.kube or the checkout.
ka_env() {
  export HERMES_HOME="$COMPARE_OUT/ka-home"
  export FLEET_AUDIT_SCRATCH_DIR="$COMPARE_OUT/ka-scratch"
  export FLEET_AUDIT_REPORTS_DIR="$COMPARE_OUT/ka-reports"
  export UPGRADE_RETROSPECTIVE_HOME="$COMPARE_OUT/ka-store"
  export STALL_WATCH_STATE="$COMPARE_OUT/ka-store/stall_watch.json"
  mkdir -p "$HERMES_HOME" "$FLEET_AUDIT_SCRATCH_DIR" "$FLEET_AUDIT_REPORTS_DIR" "$UPGRADE_RETROSPECTIVE_HOME"
}

# Put the tracing shims first on PATH and point them at a trace file.
# Usage: with_trace <trace-name>
with_trace() {
  mkdir -p "$COMPARE_OUT/trace"
  export COMPARE_TRACE="$COMPARE_OUT/trace/$1.jsonl"
  case ":$PATH:" in
    *":$COMPARE_HERE/bin:"*) ;;
    *) export PATH="$COMPARE_HERE/bin:$PATH" ;;
  esac
}

# Run a command under /usr/bin/time -v, capturing stdout, stderr, the
# time report and the exit code beside each other.
# Usage: timed <name-prefix> <cmd...>
#   writes <prefix>.out <prefix>.err <prefix>.time <prefix>.rc
timed() {
  local prefix="$1"; shift
  mkdir -p "$(dirname "$prefix")"
  local rc=0
  local start end
  start=$(date +%s%N)
  /usr/bin/time -v -o "$prefix.time" "$@" >"$prefix.out" 2>"$prefix.err" || rc=$?
  end=$(date +%s%N)
  printf '%s\n' "$rc" >"$prefix.rc"
  printf '%s\n' $(( (end - start) / 1000000 )) >"$prefix.ms"
  log "  rc=$rc ms=$(( (end - start) / 1000000 )) $(basename "$prefix")"
  return 0
}

# The kubeconfig files collect.py wrote, one per cluster, in the order
# gcloud listed them. Both tools read the clusters through these so the
# targets and the credentials are identical.
kubeconfigs() {
  local kc
  for kc in "$COMPARE_OUT"/ka-home/.kubeconfigs/kubeconfig_*.yaml; do
    [[ -e "$kc" ]] || continue
    if [[ -n "${COMPARE_EXCLUDE_CLUSTERS:-}" ]]; then
      local skip=0 pat
      local IFS=','
      for pat in $COMPARE_EXCLUDE_CLUSTERS; do
        [[ "$kc" == *"$pat"* ]] && skip=1
      done
      unset IFS
      (( skip )) && continue
    fi
    printf '%s\n' "$kc"
  done
}

# <project>/<location>/<name> from a kubeconfig file name.
cluster_target() {
  local base
  base="$(basename "$1" .yaml)"
  base="${base#kubeconfig_}"
  # kubeconfig_<project>_<cluster>_<location>; project ids and cluster
  # names may themselves contain '-', never '_'.
  local project="${base%%_*}"
  local rest="${base#*_}"
  local cluster="${rest%_*}"
  local location="${rest##*_}"
  printf '%s/%s/%s\n' "$project" "$location" "$cluster"
}

# A filesystem-safe slug for a cluster target.
cluster_slug() { printf '%s\n' "$1" | tr '/' '_'; }
