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

# The third-party stack gateway-cert needs: cert-manager and Envoy
# Gateway (which also brings the Gateway API CRDs). Sourced by inject
# and revert after lib.sh.
#
# Pinned, so a weekly run tests lookout and not whatever upstream
# released that morning. Override to try a newer release by hand.
CERT_MANAGER_VERSION="${LOOKOUT_CERT_MANAGER_VERSION:-v1.21.2}"
ENVOY_GATEWAY_VERSION="${LOOKOUT_ENVOY_GATEWAY_VERSION:-v1.9.2}"

CERT_MANAGER_URL="https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
ENVOY_GATEWAY_URL="https://github.com/envoyproxy/gateway/releases/download/${ENVOY_GATEWAY_VERSION}/install.yaml"

# Why Envoy Gateway: it is one controller Deployment plus one Envoy
# Deployment per Gateway, it runs on kind with nothing but a Service
# type override (see gateway.yaml), and it reports per-listener
# conditions exactly as the Gateway API spec words them — a missing
# certificateRef Secret is ResolvedRefs=False/InvalidCertificateRef and
# Programmed=False/Invalid on that listener alone.

# gateway_cert_stack_up — install both, and wait until each one's
# admission path answers. Idempotent.
gateway_cert_stack_up() {
  echo "▸ installing cert-manager $CERT_MANAGER_VERSION"
  kubectl apply -f "$CERT_MANAGER_URL" >/dev/null
  # The static manifest sets no memory limit, and kind nodes are capped
  # at 8g with no swap (examples/README.md § The resource budget).
  kubectl -n cert-manager set resources deployment --all \
    --requests=cpu=10m,memory=32Mi --limits=memory=256Mi >/dev/null
  kubectl -n cert-manager rollout status deployment --timeout=240s >/dev/null

  echo "▸ installing Envoy Gateway $ENVOY_GATEWAY_VERSION (+ Gateway API CRDs)"
  # Server-side: the Gateway API CRDs exceed the client-side
  # last-applied annotation limit.
  kubectl apply --server-side --force-conflicts -f "$ENVOY_GATEWAY_URL" >/dev/null
  kubectl -n envoy-gateway-system rollout status deployment/envoy-gateway --timeout=240s >/dev/null
  kubectl wait --for=condition=Established --timeout=60s \
    crd/gateways.gateway.networking.k8s.io \
    crd/certificates.cert-manager.io >/dev/null

  # cert-manager's webhook is Ready before its CA bundle is injected,
  # and an Issuer applied in that window is rejected. Retry a server
  # dry-run of the real manifest until admission accepts it.
  local manifest waited=0
  manifest="$(dirname "${BASH_SOURCE[0]}")/fault.yaml"
  until kubectl apply --dry-run=server -f "$manifest" >/dev/null 2>&1; do
    if ((waited >= 180)); then
      echo "ERROR: cert-manager / Envoy Gateway admission still refusing fault.yaml after 180s:" >&2
      kubectl apply --dry-run=server -f "$manifest" >&2 || true
      return 1
    fi
    sleep 5
    waited=$((waited + 5))
  done
  echo "  ✓ cert-manager and Envoy Gateway admitting (${waited}s)"
}

# gateway_cert_stack_down — uninstall both, and wait for the CRDs to be
# gone. The wait matters: sentinel_resync decides whether to name the
# gateway source by asking whether the CRDs are served, and an explicit
# --sources=…,gateway on a cluster without them is fatal at startup.
gateway_cert_stack_down() {
  echo "▸ uninstalling Envoy Gateway and cert-manager"
  kubectl delete -f "$ENVOY_GATEWAY_URL" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl delete -f "$CERT_MANAGER_URL" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl wait --for=delete --timeout=180s \
    crd/gateways.gateway.networking.k8s.io \
    crd/httproutes.gateway.networking.k8s.io \
    crd/certificates.cert-manager.io >/dev/null 2>&1 || true
  kubectl wait --for=delete --timeout=180s \
    namespace/envoy-gateway-system namespace/cert-manager >/dev/null 2>&1 || true
}

# sentinel_resync — re-run examples/sentinel/up with the image the
# sentinel is ALREADY running, then restart it.
#
# Both sources this scenario needs read the cluster's API surface once,
# at startup: sentinel/up only names `gateway` in --sources when the
# Gateway API CRDs are served, and the expiry source only includes
# cert-manager Certificates when it discovers their CRD as it starts.
# So installing the stack under a running sentinel is not enough.
#
# Re-running sentinel/up bare would also swap the image back to its
# GHCR default, which in CI is the wrong binary (kind/up --build).
sentinel_resync() {
  local image
  image="$(kubectl -n "$SENTINEL_NS" get deployment lookout-watch \
    -o jsonpath='{.spec.template.spec.containers[0].image}')"
  echo "▸ re-running sentinel/up with the running image ($image)"
  LOOKOUT_IMAGE="$image" "$(examples_root)/sentinel/up" >/dev/null
  # sentinel/up only rolls the pod when the args changed; force it, so
  # the expiry source's cert-manager discovery runs again either way.
  kubectl -n "$SENTINEL_NS" rollout restart deployment/lookout-watch >/dev/null
  kubectl -n "$SENTINEL_NS" rollout status deployment/lookout-watch --timeout=180s >/dev/null
}
