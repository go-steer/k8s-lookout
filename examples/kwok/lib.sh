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

# ---- object padding (spike S8) ---------------------------------------------

# kwok generates objects that are nothing like the size of real ones, and
# the bytes it omits are EXACTLY the bytes lookout's shared transform
# exists to remove: managedFields, env values, probe definitions, and a
# node's status.images. Unpadded, a scale run passes every budget in §6.6
# while validating nothing, and — since decode cost scales with wire size
# — understates the CPU model by the same factor.
#
# So padding is on by default and turning it off is the deliberate act,
# not the other way round. KWOK_PAD=0 exists for isolating the harness's
# own overhead, and nothing that reports a budget should use it.
KWOK_PAD="${KWOK_PAD:-1}"

# Measured by internal/watch/objectsize_test.go (spike S8) against a live
# GKE cluster on 2026-09-21, serialised JSON, p50 across the sample:
#
#             before transform   after transform
#   Pod            18,164 B          10,788 B
#   Node           25,163 B           3,381 B
#
# and 25 entries in a node's status.images. Re-measure with:
#
#   LOOKOUT_MEASURE_CONTEXT=<ctx> go test ./internal/watch -run ObjectSizes -v
#
# TARGET is what a real object weighs; FLOOR is what this fixture must not
# fall below. They are two different questions and conflating them makes
# the check useless in one direction or the other.
#
# The floors were calibrated on a throwaway kind cluster on 2026-09-21 by
# applying exactly these blocks and measuring what came back: a pod landed
# at 15,146 B and a node at 20,843 B, both 83% of the GKE p50. The floors
# sit ~11% under those, which is room for a different apiserver version
# and no room at all for a template that lost its padding.
#
# Three notes on where the remaining 17% is, and on one place the fixture
# gets there by a route the target did not:
#
#   metadata.managedFields — 6,109 B at the GKE p50, 5,030 B here. It is a
#   record of how many distinct appliers have touched an object, and a
#   generated fleet is touched by two. A manifest cannot carry it; the
#   apiserver writes it. This one nearly closed itself, which was a
#   surprise: forty env vars is forty field-ownership entries.
#
#   status.conditions — 6,200 B on the GKE node, 835 B here, and
#   deliberately not padded. kwok's node stage OWNS status.conditions and
#   rewrites it on every heartbeat, so anything written here is gone within
#   seconds; conditions are also exactly what object-state's node detectors
#   read, so a fixture that fought kwok for that field would be a fixture
#   that flaps. This is the whole node shortfall.
#
#   kubectl.kubernetes.io/last-applied-configuration — 9,226 B on the
#   padded node, which is client-side apply echoing the manifest (images
#   included) back into an annotation. Real GKE nodes do not carry it.
#   It is honest bytes for THIS fixture — the sentinel really does decode
#   them — but it means the node's weight is not distributed the way a real
#   node's is, and anyone reading a per-field breakdown off the harness
#   should know that before drawing a conclusion from it.
PAD_POD_TARGET_BYTES=18164
PAD_NODE_TARGET_BYTES=25163
PAD_POD_FLOOR_BYTES=13500
PAD_NODE_FLOOR_BYTES=18000

# The knobs the floor was calibrated with. Turning these down is exactly
# the deflation verify-padding exists to catch, so it will.
PAD_NODE_IMAGES=25
PAD_POD_ENV_VARS=40
PAD_POD_ANNOTATION_BLOBS=6

# pad_node_images — a node's status.images, the single largest node-side
# line item (the transform strips it entirely, which is where its 86%
# node-side saving comes from). Real registries and real digests, because
# the length of these strings IS the measurement; a list of "img-1" would
# pad the count and not the bytes.
pad_node_images() {
  ((KWOK_PAD)) || return 0
  local i repo
  echo "  images:"
  for ((i = 0; i < PAD_NODE_IMAGES; i++)); do
    # Three names apiece: a digest reference, a version tag and a
    # floating tag. That is the arity a real node reports (the sample ran
    # 1 to 4, ~270 B per entry), and two names apiece undershoots it by a
    # third — the count is the easy half of this padding, the string
    # lengths are the half that matters.
    repo="registry.k8s.io/kwok-fleet/synthetic-workload-component-$(printf '%02d' "$i")"
    cat <<EOF
  - names:
    - ${repo}@sha256:$(printf '%064d' "$i")
    - ${repo}:v1.3$(printf '%d' "$i").2-gke.1$(printf '%03d' "$i")
    - ${repo}:latest
    sizeBytes: $((41000000 + i * 1373))
EOF
  done
}

# pad_pod_annotations — the annotation bulk a real pod carries and a kwok
# pod does not. Indented for a pod TEMPLATE's metadata (six spaces for the
# key, eight for its entries).
#
# The blobs are the honest part. Real annotation weight is not twenty tidy
# scrape hints; it is a handful of large opaque strings written by tooling
# — a rendered config checksum, a last-applied snapshot, an injection
# sidecar's status. Padding with many small keys would reproduce the byte
# count and not the shape, and the shape is what the decoder walks.
pad_pod_annotations() {
  ((KWOK_PAD)) || return 0
  local i
  cat <<'EOF'
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: /metrics
        checksum/config: 8f14e45fceea167a5a36dedd4bea2543a1c4b6c9e3a4f2d1b0e7c8a9d6f3b2e1
        checksum/secrets: 3c59dc048e8850243be8079a5c74d079a1d9b5e6f2c8a4b7d3e0f1a2c5b8d9e6
        kubernetes.io/change-cause: deployed by the kwok fleet generator, revision 41
        app.kubernetes.io/part-of: kwok-synthetic-fleet
        sidecar.istio.io/inject: "false"
EOF
  # Deliberately absent, and worth a line so nobody adds them back as
  # "realistic": the alpha seccomp and beta AppArmor pod annotations are
  # not inert padding. The apiserver warns on every apply, and a kubelet
  # that does not honour the AppArmor one REJECTS the pod — which, behind
  # a ReplicaSet, is an unbounded create/reject loop. Calibrating this
  # padding produced 1,657 rejected pods from one replica before anyone
  # noticed, which is the entire argument for measuring the applied
  # object rather than the manifest.
  for ((i = 0; i < PAD_POD_ANNOTATION_BLOBS; i++)); do
    printf '        lookout.examples/rendered-%02d: "%s"\n' "$i" "$(pad_blob "$i")"
  done
}

# pad_blob — one ~380-byte opaque value, deterministic in its argument so
# that two runs of the generator produce byte-identical manifests and a
# diff of two fleets is empty rather than noise.
pad_blob() {
  local seed="$1" out="" j
  for ((j = 0; j < 6; j++)); do
    out+="$(printf '%064d' $((seed * 13 + j)))"
  done
  printf '%s' "$out"
}

# pad_pod_env — environment the transform will strip the VALUES from,
# keeping the names and the ValueFrom structure. This is padding that
# does double duty: it is bulk a real pod carries, and it is the exact
# shape §6.1 claims a saving on, so a scale run measures the claim
# rather than assuming it. Indented for a container (eight spaces).
pad_pod_env() {
  ((KWOK_PAD)) || return 0
  local i
  # Long enough to look like a connection string or a JWT, which is what
  # env values on a real pod actually are.
  local val="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.c3ludGhldGljLWZsZWV0LXZhbHVl"
  echo "        env:"
  for ((i = 0; i < PAD_POD_ENV_VARS; i++)); do
    cat <<EOF
        - name: FLEET_SETTING_$(printf '%02d' "$i")
          value: "${val}-${i}"
EOF
  done
  cat <<'EOF'
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
EOF
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
