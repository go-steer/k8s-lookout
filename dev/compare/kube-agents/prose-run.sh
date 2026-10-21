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

# Phase 3a: the "startup" audit. kube-agents' onboarding files one
# `cluster_inventory_audit_sop.md` card per cluster; the SOP is prose a
# Cluster Agent follows with kubectl. roster.py shows every *cron* check
# is collector-backed, so this SOP is the one place an agent still reads
# raw kubectl output to find things. Each concrete recipe the SOP names
# is ported here verbatim-as-possible (kubectl + jq), run through the
# tracing shims so its API cost is measured, and its hits recorded in
# the manifest shape the cross-walk reads (`check`, `namespace`,
# `object`).
#
# SOP steps ported (agents/platform/governance/cluster_inventory_audit_sop.md):
#   §3.1 non-system namespaces without ResourceQuota / LimitRange
#   §3.2 workloads with no startupProbe
#   §3.2 containers without readOnlyRootFilesystem: true
#   §3.2 containers adding a capability other than SYS_ADMIN  (high)
#   §3.3 / §4.1 addon + toolchain detection (gateway, nginx, cert-manager,
#        otel, GMP, Config Sync / Argo CD / Flux)
# §2 (topology) and §4.2–4.3 (alerting hygiene, WI usage) are judgment
# steps with no recipe; they are listed as `manual` and not run.
#
# Output: $COMPARE_OUT/prose/<cluster-slug>/inventory.json (+ .ms)
#         $COMPARE_OUT/trace/prose-<cluster-slug>.jsonl

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

: "${COMPARE_OUT:?}"
export COMPARE_SHIM_TMP="$COMPARE_OUT/tmp"
mkdir -p "$COMPARE_SHIM_TMP"

SYSTEM_NS='^(kube-system|kube-public|kube-node-lease|gke-managed-.*|gke-gmp-system|gmp-system|gmp-public|gke-connect|gke-mcs|config-management-system|config-management-monitoring|resource-group-system|composer-system|istio-system|asm-system|gatekeeper-system|cnrm-system|default)$'

inventory() {
  local kc="$1" dir="$2"
  export KUBECONFIG="$kc"
  local start end
  start=$(date +%s%N)

  # §3.1 namespaces, quotas, limit ranges
  kubectl get ns -o json >"$dir/ns.json" 2>"$dir/ns.err" || true
  kubectl get resourcequotas,limitranges -A -o json >"$dir/quotas.json" 2>"$dir/quotas.err" || true
  # §3.2 workload specs (a pod list is not this audit)
  kubectl get deployments,statefulsets,daemonsets,jobs -A -o json >"$dir/workloads.json" 2>"$dir/workloads.err" || true
  # §3.3 addons: CRDs name the toolchains, deployments name the collectors
  kubectl get crd -o json >"$dir/crds.json" 2>"$dir/crds.err" || true

  end=$(date +%s%N)
  printf '%s\n' $(( (end - start) / 1000000 )) >"$dir/inventory.ms"

  jq -n --arg sysre "$SYSTEM_NS" \
    --slurpfile ns "$dir/ns.json" \
    --slurpfile quotas "$dir/quotas.json" \
    --slurpfile wl "$dir/workloads.json" \
    --slurpfile crds "$dir/crds.json" '
    def nss: ($ns[0].items // []) | map(.metadata.name) | map(select(test($sysre) | not));
    def quota_ns: ($quotas[0].items // []) | map(select(.kind=="ResourceQuota") | .metadata.namespace) | unique;
    def lr_ns:    ($quotas[0].items // []) | map(select(.kind=="LimitRange")    | .metadata.namespace) | unique;
    def wls: ($wl[0].items // []) | map(select(.metadata.namespace | test($sysre) | not));
    def ctrs(w): (w.spec.template.spec.containers // []);
    def crdnames: ($crds[0].items // []) | map(.metadata.name);
    {
      "sop": "cluster_inventory_audit_sop.md",
      "candidates": (
        [ nss[] | select(. as $n | (quota_ns | index($n)) == null) | {check:"inventory-no-resourcequota", namespace:., object:("Namespace/" + .), severity:"minor"} ]
        + [ nss[] | select(. as $n | (lr_ns | index($n)) == null) | {check:"inventory-no-limitrange", namespace:., object:("Namespace/" + .), severity:"minor"} ]
        + [ wls[] | select(.kind != "Job") | select([ctrs(.)[] | has("startupProbe")] | all | not)
            | {check:"inventory-no-startup-probe", namespace:.metadata.namespace, object:(.kind + "/" + .metadata.name), severity:"minor"} ]
        + [ wls[] | select([ctrs(.)[] | (.securityContext.readOnlyRootFilesystem // false)] | all | not)
            | {check:"inventory-writable-rootfs", namespace:.metadata.namespace, object:(.kind + "/" + .metadata.name), severity:"minor"} ]
        + [ wls[] | . as $w | [ctrs(.)[] | (.securityContext.capabilities.add // [])[] | select(. != "SYS_ADMIN")] | unique
            | select(length > 0)
            | {check:"inventory-added-capabilities", namespace:$w.metadata.namespace, object:($w.kind + "/" + $w.metadata.name), severity:"major", excerpt:(join(","))} ]
      ),
      "addons": {
        "gateway_api":   (crdnames | map(select(endswith("gateway.networking.k8s.io"))) | length > 0),
        "cert_manager":  (crdnames | map(select(endswith("cert-manager.io"))) | length > 0),
        "gmp":           (crdnames | map(select(endswith("monitoring.googleapis.com"))) | length > 0),
        "config_sync":   (crdnames | map(select(endswith("configsync.gke.io") or endswith("configmanagement.gke.io"))) | length > 0),
        "argocd":        (crdnames | map(select(endswith("argoproj.io"))) | length > 0),
        "flux":          (crdnames | map(select(endswith("toolkit.fluxcd.io"))) | length > 0),
        "keda":          (crdnames | map(select(endswith("keda.sh"))) | length > 0),
        "kcc":           (crdnames | map(select(endswith("cnrm.cloud.google.com"))) | length > 0),
        "nginx_ingress": ((($wl[0].items // []) | map(select(.metadata.name | test("ingress-nginx|nginx-ingress"))) | length) > 0),
        "otel_collector":((($wl[0].items // []) | map(select(.metadata.name | test("otel|opentelemetry"))) | length) > 0)
      },
      "manual": ["§2 cluster topology (GKE MCP)", "§4.2 alerting hygiene", "§4.3 WI usage / Shielded / DPv2 judgment"],
      "raw_bytes": {
        "ns": (($ns[0] | tostring | length)),
        "quotas": (($quotas[0] | tostring | length)),
        "workloads": (($wl[0] | tostring | length)),
        "crds": (($crds[0] | tostring | length))
      }
    }' >"$dir/inventory.json"
  # The raw dumps are what the agent would read, not ours to keep: sizes
  # stay (above, and in the trace), payloads go.
  rm -f "$dir/ns.json" "$dir/quotas.json" "$dir/workloads.json" "$dir/crds.json"
  unset KUBECONFIG
}

mapfile -t KCS < <(kubeconfigs)
(( ${#KCS[@]} > 0 )) || die "no kubeconfigs; run the ka phase first"
for kc in "${KCS[@]}"; do
  target="$(cluster_target "$kc")"
  slug="$(cluster_slug "$target")"
  dir="$COMPARE_OUT/prose/$slug"
  mkdir -p "$dir"
  printf '%s\n' "$target" >"$dir/target"
  with_trace "prose-$slug"
  log "▸ inventory SOP recipes on $target"
  inventory "$kc" "$dir"
  log "  candidates=$(jq '.candidates | length' "$dir/inventory.json") ms=$(cat "$dir/inventory.ms")"
done
log "prose phase done"
