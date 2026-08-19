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

# Shared helpers for the kwok scale layer. Source from each script with:
#   . "$(dirname "$0")/lib.sh"
#
# Everything here sits ON TOP of examples/lib.sh — the kwok layer is
# additive to the kind cluster, never a replacement for it.

. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

kwok_root() {
  cd "$(dirname "${BASH_SOURCE[0]}")" && pwd
}

# The kwok release the scale layer is pinned to. Bumping this is a
# deliberate act: the controller image, the CRDs and the default Stages
# ship together and are only guaranteed consistent within one release.
KWOK_VERSION="${KWOK_VERSION:-v0.8.0}"
KWOK_RELEASE_URL="https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}"

# kwok's own convention, and the default in the shipped controller
# config (manageNodesWithAnnotationSelector). A node carrying this
# annotation is kwok's to maintain; a node without it is left entirely
# alone — which is what keeps the real kind kubelets out of kwok's
# reach, and is also how `node-fail` stops a fake node's heartbeat.
FAKE_NODE_ANNOTATION="kwok.x-k8s.io/node"
FAKE_NODE_ANNOTATION_VALUE="fake"

# Every fake node carries this taint and every fake pod tolerates it,
# so nothing real is ever scheduled onto a node with no kubelet behind
# it — and the fake fleet never displaces the sentinel or the demo app.
FAKE_NODE_TAINT_KEY="kwok.x-k8s.io/node"
FAKE_NODE_LABEL="type=kwok"

# Marks a fake node that node-fail took down, so node-heal can find
# exactly those again. Inferring it from the missing kwok annotation
# would also sweep up a node somebody unmanaged by hand.
FAILED_NODE_LABEL="lookout.examples/kwok-node-failed=true"

# Namespaces the synthetic fleet is generated into, and the label that
# marks everything the scale layer created (so scale-down can delete by
# selector rather than by remembering what it made).
FLEET_NS_PREFIX="kwok-fleet"
# Where scale-up parks kindnet's original memory limit so scale-down can
# put it back; see the kindnet_headroom comment in scale-up.
KINDNET_LIMIT_ANNOTATION="lookout.examples/kindnet-original-memory-limit"
FLEET_LABEL="app.kubernetes.io/managed-by=lookout-kwok-scale"

# fake_nodes — names of the nodes kwok is currently managing.
fake_nodes() {
  kubectl get nodes -l "$FAKE_NODE_LABEL" -o name 2>/dev/null | sed 's|^node/||'
}

# fake_node_count — how many, as a bare integer.
fake_node_count() {
  fake_nodes | grep -c . || true
}

require_kwok_installed() {
  if ! kubectl -n kube-system get deploy kwok-controller >/dev/null 2>&1; then
    echo "ERROR: kwok-controller is not installed — run examples/kwok/up first" >&2
    exit 1
  fi
}

# kwok_host_ip — the node address the controller serves its fake kubelet
# endpoint on. `up` runs it with hostNetwork precisely so this is a REAL
# node IP: see the long comment in scale-up for why every fake node has
# to advertise it as its own InternalIP.
kwok_host_ip() {
  kubectl -n kube-system get pods -l app=kwok-controller \
    -o jsonpath='{.items[0].status.hostIP}' 2>/dev/null
}

# ---- fake container logs ---------------------------------------------------

# kwok serves a pod's logs from a file on the CONTROLLER's filesystem,
# named by a ClusterLogs/Logs CR. `up` mounts one ConfigMap at /logs and
# scenarios add a key to it, so publishing a log stream never needs a
# controller rollout — the kubelet reprojects the volume in place.
KWOK_LOGS_CM="kwok-logs"
KWOK_LOGS_MOUNT="/logs"

# kwok_logs_dir — local staging for the ConfigMap's keys. Republishing
# from a directory keeps this jq-free and makes revert a file delete.
kwok_logs_dir() {
  local d="$STATE_DIR/kwok-logs"
  mkdir -p "$d"
  echo "$d"
}

# kwok_logs_publish — push the staging directory to the cluster. Safe to
# call with the directory empty; that is how revert clears a stream.
kwok_logs_publish() {
  local dir
  dir="$(kwok_logs_dir)"
  local args=(-n kube-system create configmap "$KWOK_LOGS_CM")
  local f
  for f in "$dir"/*; do
    [[ -e "$f" ]] && args+=("--from-file=$(basename "$f")=$f")
  done
  kubectl "${args[@]}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

# cri_log <stream> <age-seconds> <message>
#
# One line in the CRI log format the kwok controller parses:
#
#   <RFC3339Nano> <stdout|stderr> <F|P> <message>
#
# Anything else is rejected outright with "unsupported log format", and
# the timestamp is load-bearing a second time: every read path asks the
# kubelet for a --since window, so a fixture stamped at authoring time
# is simply filtered out and the scenario reads as healthy.
cri_log() {
  local stream="$1" age="$2" msg="$3"
  printf '%s %s F %s\n' \
    "$(date -u -d "-${age} seconds" +%Y-%m-%dT%H:%M:%S.000000000Z)" "$stream" "$msg"
}
