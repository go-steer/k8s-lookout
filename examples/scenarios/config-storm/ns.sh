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

# Per-run namespace for config-storm. Sourced by inject/verify/revert
# after lib.sh; the name is generated once by inject and recorded in
# $STATE_DIR so the other two agree with it.
#
# Every other scenario in the suite uses a fixed namespace. This one
# cannot, and the reason is the scenario's own collateral damage.
# Breaking the mount also stalls four rollouts, and four rollout_stall
# incidents that share no finer key form a storm on
# `Namespace//<ns>`. That storm outlives the namespace: a storm key is
# a name, not an object identity, so it stays open for the full 30m
# stormIdleTTL no matter what happens to the objects it was keyed on.
# A second run inside that half hour recreates the same namespace
# name, and step 1 of StormCorrelator.Observe attaches the new
# FailedMounts to the stale storm — no ConfigMap storm forms and
# verify times out at 240s blaming the tier it was testing. A fresh
# name each run makes the key unreachable.
#
# (What held that first storm open for the whole TTL was #397: a
# rollout_stall whose Deployment is deleted never resolved, so the
# members never cleared. That is fixed, but resolving every member
# only closes the storm — it does not un-key it, and the key is
# re-attachable for as long as the storm is open.)
#
# CI never saw this (each run gets a new cluster and a new sentinel);
# it only shows up in the workflow the examples are actually for —
# running a scenario twice against one long-lived sentinel.

CONFIG_STORM_NS_FILE="$STATE_DIR/config-storm.ns"

# config_storm_new_ns — mint and record this run's namespace.
config_storm_new_ns() {
  mkdir -p "$STATE_DIR"
  printf 'lookout-storm-%s\n' "$(date +%s)" >"$CONFIG_STORM_NS_FILE"
  cat "$CONFIG_STORM_NS_FILE"
}

# config_storm_ns — the namespace inject minted, for verify/revert.
config_storm_ns() {
  if [[ ! -s "$CONFIG_STORM_NS_FILE" ]]; then
    echo "ERROR: no config-storm namespace recorded in $CONFIG_STORM_NS_FILE" >&2
    echo "       — run examples/scenarios/config-storm/inject first" >&2
    return 1
  fi
  cat "$CONFIG_STORM_NS_FILE"
}

# config_storm_sweep — delete namespaces left by runs that died before
# revert. Bounded by the name prefix and the suite's part-of label, so
# it can never touch anything outside the examples.
config_storm_sweep() {
  local stale
  stale="$(kubectl get namespaces \
    -l app.kubernetes.io/part-of=lookout-examples \
    -o name 2>/dev/null | grep -E 'namespace/lookout-storm-' || true)"
  [[ -z "$stale" ]] && return 0
  echo "▸ sweeping namespaces left by earlier runs"
  echo "$stale" | sed 's|^namespace/|    |'
  echo "$stale" | xargs kubectl delete --ignore-not-found --wait=false
}
