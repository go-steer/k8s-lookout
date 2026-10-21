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

# Phase 2: run lookout's read-path matrix against every cluster the
# kube-agents collectors enumerated, through the very kubeconfig files
# they fetched (identical targets, identical credentials). Each command
# runs with --format json so the cross-walk parses records, not prose.
#
# Output: $COMPARE_OUT/lookout/<cluster-slug>/<command>.{out,err,time,rc,ms}
#         $COMPARE_OUT/lookout/_fleet/scan-parallel.ms  (all clusters at once)

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

: "${COMPARE_OUT:?}" "${LOOKOUT_BIN:?}"
TIMEOUT="${COMPARE_LOOKOUT_TIMEOUT:-5m}"

mapfile -t KCS < <(kubeconfigs)
(( ${#KCS[@]} > 0 )) || die "no kubeconfigs under $COMPARE_OUT/ka-home/.kubeconfigs; run the ka phase first"

# The matrix: one line per invocation, `<file-stem>|<args...>`. Grouped
# by the kube-agents stream each answers (see equivalence.yaml).
MATRIX=(
  # Workload Reliability (obtainability-audit)
  "audit-workloads|audit workloads -A"
  # `state edges` is per-workload by design (--workload=<Kind>/<ns>/<name>);
  # the cluster-wide Service->endpoint questions are answered by `scan`
  # and `triage delta` below, and by the watch path.
  "state-volumes|state volumes -A"
  "stab-drain|stab drain -A"
  "triage-top|triage top -A --show-unrequested --show-unlimited"
  # Security & RBAC (compliance-audit)
  "audit-hardening|audit hardening -A"
  "audit-rbac|audit rbac -A"
  "audit-netpol|audit netpol -A"
  "audit-cluster|audit cluster"
  "state-wi|state wi -A"
  # Consistency drift / Upgrade & Patch readiness
  "audit-upgrades|audit upgrades"
  "stab-drift|stab drift -A"
  # Stockout / capacity
  "cloud-stockout|cloud stockout"
  "cloud-quota|cloud quota"
  # Waste
  "cloud-orphans|cloud orphans --only=disks,lbs,addresses,nodepools"
  "stab-scaledown|stab scaledown"
  # Networking
  "cloud-ipspace|cloud ipspace"
  "state-gateway|state gateway -A"
  "state-webhooks|state webhooks"
  # Ecosystem state the inventory SOP asks about
  "state-keda|state keda -A"
  "state-storage|state storage -A"
  # Startup / first report, and the always-on watchers' questions
  "health|health"
  "scan|scan"
  "triage-list|triage list -A"
  "triage-delta|triage delta -A"
)

run_cluster() {
  local kc="$1"
  local target slug dir
  target="$(cluster_target "$kc")"
  slug="$(cluster_slug "$target")"
  dir="$COMPARE_OUT/lookout/$slug"
  mkdir -p "$dir"
  printf '%s\n' "$target" >"$dir/target"
  log "▸ lookout on $target"
  local entry stem args
  for entry in "${MATRIX[@]}"; do
    stem="${entry%%|*}"
    args="${entry#*|}"
    if [[ -n "${COMPARE_LOOKOUT_ONLY:-}" && ",$COMPARE_LOOKOUT_ONLY," != *",$stem,"* ]]; then
      continue
    fi
    # shellcheck disable=SC2086 -- the matrix is word-split on purpose
    timed "$dir/$stem" "$LOOKOUT_BIN" $args --format json --timeout "$TIMEOUT" --kubeconfig "$kc"
  done
  # k8s-lookout's cmd/leeway is the resident exporter; the one-shot
  # `leeway report` kube-agents' topology-drift skill calls lives in the
  # standalone github.com/go-steer/leeway. Pass it as LEEWAY_BIN to add
  # its report to the placement-drift rows.
  if [[ -n "${LEEWAY_BIN:-}" && -x "$LEEWAY_BIN" ]]; then
    KUBECONFIG="$kc" timed "$dir/leeway-report" "$LEEWAY_BIN" report --format json
  fi
}

for kc in "${KCS[@]}"; do
  run_cluster "$kc"
done

# COMPARE_LOOKOUT_ONLY names matrix stems to re-run; `fleet-scan` names
# the parallel scan below. Unset runs everything.
ONLY="${COMPARE_LOOKOUT_ONLY:-}"
if [[ -n "$ONLY" && ",$ONLY," != *",fleet-scan,"* ]]; then
  log "lookout subset done (${ONLY}); fleet-parallel scan skipped"
  exit 0
fi

# Fleet wall-clock: the whole fleet's `scan`, all clusters at once, as a
# resident sentinel (or a fleet layer fanning out) would run it.
mkdir -p "$COMPARE_OUT/lookout/_fleet"
log "▸ lookout scan, all clusters in parallel"
start=$(date +%s%N)
for kc in "${KCS[@]}"; do
  slug="$(cluster_slug "$(cluster_target "$kc")")"
  "$LOOKOUT_BIN" scan --format json --timeout "$TIMEOUT" --kubeconfig "$kc" \
    >"$COMPARE_OUT/lookout/_fleet/scan-$slug.out" 2>"$COMPARE_OUT/lookout/_fleet/scan-$slug.err" &
done
wait
end=$(date +%s%N)
printf '%s\n' $(( (end - start) / 1000000 )) >"$COMPARE_OUT/lookout/_fleet/scan-parallel.ms"
log "lookout phase done: ${#KCS[@]} clusters; fleet scan in parallel: $(cat "$COMPARE_OUT/lookout/_fleet/scan-parallel.ms") ms"
