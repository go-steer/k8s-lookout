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

# Phase 3b: kube-agents' always-on `no_agent` watchers, run as the leaf
# scripts they wrap (the tick wrappers need scaffolded Cluster Agent
# profiles and the Session KV server, neither of which exists outside an
# install):
#
#   stall-watch (*/30 min)  -> stall_report.py --json, per cluster x
#                              non-system namespace, as stall_watch.py
#                              runs it
#   upgrade-readiness-watch -> fleet_upgrade_report.py --readiness, the
#   (daily)                    skill script upgrade_readiness_watch.py
#                              ships into its sandbox
#
# Output: $COMPARE_OUT/watchers/<cluster-slug>/stall-<ns>.{out,err,ms,rc}
#         $COMPARE_OUT/watchers/_fleet/upgrade-readiness.{out,err,time,rc,ms}

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

: "${COMPARE_OUT:?}" "${KUBE_AGENTS_DIR:?}" "${COMPARE_PROJECT:?}"
ka_env
export COMPARE_SHIM_TMP="$COMPARE_OUT/tmp"
mkdir -p "$COMPARE_SHIM_TMP" "$COMPARE_OUT/watchers/_fleet"

SCRIPTS="$KUBE_AGENTS_DIR/agents/platform/scripts"
UPG="$KUBE_AGENTS_DIR/agents/platform/skills/fleet-upgrade-verification/scripts"
SYSTEM_NS='^(kube-system|kube-public|kube-node-lease|gke-managed-.*|gke-gmp-system|gmp-system|gmp-public|gke-connect|gke-mcs|config-management-system|config-management-monitoring|resource-group-system|composer-system|istio-system|asm-system|gatekeeper-system|cnrm-system)$'

mapfile -t KCS < <(kubeconfigs)
(( ${#KCS[@]} > 0 )) || die "no kubeconfigs; run the ka phase first"

for kc in "${KCS[@]}"; do
  target="$(cluster_target "$kc")"
  slug="$(cluster_slug "$target")"
  dir="$COMPARE_OUT/watchers/$slug"
  mkdir -p "$dir"
  printf '%s\n' "$target" >"$dir/target"
  with_trace "watchers-$slug"
  log "▸ stall_report.py on $target"
  export KUBECONFIG="$kc"
  mapfile -t NSS < <(kubectl get ns -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -Ev "$SYSTEM_NS" || true)
  start=$(date +%s%N)
  for ns in "${NSS[@]}"; do
    # The same bounded kind list stall_watch.py passes (its DEFAULT_KINDS);
    # unbounded, stall_report.py lists every namespaced kind the API
    # serves (~60 kubectl calls a namespace).
    timed "$dir/stall-$ns" python3 "$SCRIPTS/stall_report.py" --namespace "$ns" --json \
      --kind "${COMPARE_STALL_KINDS:-deployments,statefulsets,daemonsets,jobs,gateways.gateway.networking.k8s.io,httproutes.gateway.networking.k8s.io,certificates.cert-manager.io}"
  done
  end=$(date +%s%N)
  printf '%s\n' $(( (end - start) / 1000000 )) >"$dir/stall-all.ms"
  printf '%s\n' "${#NSS[@]}" >"$dir/stall-namespaces"
  unset KUBECONFIG
done

with_trace "watchers-upgrade-readiness"
log "▸ fleet_upgrade_report.py --readiness on $COMPARE_PROJECT"
timed "$COMPARE_OUT/watchers/_fleet/upgrade-readiness" \
  python3 "$UPG/fleet_upgrade_report.py" --project "$COMPARE_PROJECT" --readiness \
    --state-dir "$COMPARE_OUT/ka-store/upgrade-readiness" \
    --kubeconfig-dir "$HERMES_HOME/.kubeconfigs" \
    --output "$COMPARE_OUT/watchers/_fleet/upgrade-readiness.json"
log "watchers phase done"
