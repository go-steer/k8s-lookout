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

# Shared helpers for examples/ scripts (kind/sentinel setup, scenario
# inject/verify/revert, the e2e driver).
#
# Source this from each script with:
#   . "$(dirname "$0")/lib.sh"          # or ../../lib.sh from a scenario
#
# Everything here assumes the CURRENT kubeconfig context. Because the
# scenarios deliberately break workloads (and node-failure stops a
# node), require_examples_context refuses to run unless the context
# matches LOOKOUT_EXAMPLES_CONTEXT (default: kind-lookout-examples).
# Point it at a staging cluster explicitly to run elsewhere — never a
# cluster users depend on.

set -euo pipefail

examples_root() {
  # examples/ directory, resolved from this file's location.
  cd "$(dirname "${BASH_SOURCE[0]}")" && pwd
}

repo_root() {
  git -C "$(examples_root)" rev-parse --show-toplevel
}

# ---- cluster + namespaces ------------------------------------------------

CLUSTER_NAME="${LOOKOUT_EXAMPLES_CLUSTER:-lookout-examples}"
EXAMPLES_CONTEXT="${LOOKOUT_EXAMPLES_CONTEXT:-kind-${CLUSTER_NAME}}"
SENTINEL_NS="agent-triage"
DEMO_NS="lookout-demo"
STATE_DIR="${LOOKOUT_EXAMPLES_STATE:-${TMPDIR:-/tmp}/lookout-examples}"

require_examples_context() {
  local ctx
  ctx="$(kubectl config current-context 2>/dev/null || true)"
  if [[ -z "$ctx" ]]; then
    echo "ERROR: no current kubectl context — run examples/kind/up first" >&2
    exit 1
  fi
  if [[ "$ctx" != "$EXAMPLES_CONTEXT" ]]; then
    echo "ERROR: current context is '$ctx', expected '$EXAMPLES_CONTEXT'." >&2
    echo "The scenarios deliberately break workloads. To run against a" >&2
    echo "different (STAGING-ONLY) cluster, set LOOKOUT_EXAMPLES_CONTEXT" >&2
    echo "to that context name explicitly." >&2
    exit 1
  fi
}

is_kind_context() {
  [[ "$EXAMPLES_CONTEXT" == kind-* ]]
}

# ---- the lookout binary (read-path verification) -------------------------

# Resolution order: $LOOKOUT_BIN, `lookout` on PATH, then a one-time
# `go build` into $STATE_DIR/bin. Read commands run against the current
# kubeconfig context — no in-cluster deployment needed (README §read-path).
lookout_bin() {
  if [[ -n "${LOOKOUT_BIN:-}" ]]; then
    echo "$LOOKOUT_BIN"
    return 0
  fi
  if command -v lookout >/dev/null 2>&1; then
    command -v lookout
    return 0
  fi
  local cached="$STATE_DIR/bin/lookout"
  if [[ ! -x "$cached" ]]; then
    mkdir -p "$STATE_DIR/bin"
    echo "▸ building lookout into $cached (first run only)" >&2
    (cd "$(repo_root)" && go build -o "$cached" ./cmd/lookout)
  fi
  echo "$cached"
}

run_lookout() {
  "$(lookout_bin)" "$@"
}

# ---- stub-daemon wire capture ---------------------------------------------

# The stub daemon (dev/drills/stub-daemon.py, deployed by
# examples/sentinel/up as Service core-agent:7777) logs one line per
# session-create / inject — `kubectl logs` of it is the wire capture.
stub_logs() {
  kubectl -n "$SENTINEL_NS" logs deploy/stub-daemon --tail=-1 2>/dev/null || true
}

# stub_mark <name> — record the current stub log length, so later
# stub_since/await_inject calls for <name> only see new lines. Call at
# the top of every scenario inject script.
stub_mark() {
  mkdir -p "$STATE_DIR"
  stub_logs | wc -l >"$STATE_DIR/$1.mark"
}

# stub_since <name> — stub log lines appended after stub_mark <name>.
stub_since() {
  local mark=0
  [[ -f "$STATE_DIR/$1.mark" ]] && mark="$(cat "$STATE_DIR/$1.mark")"
  stub_logs | tail -n +"$((mark + 1))"
}

# await <timeout-seconds> <description> <command...>
#
# Polls <command...> every 5s until it exits 0 or the timeout expires.
# The command's output is suppressed — callers print their own
# evidence line. Returns 1 on timeout (with a loud message).
#
# LOOKOUT_E2E_TIMEOUT_SCALE (integer, default 1) multiplies every
# await timeout — CI runners are slower than a workstation; the
# e2e-kind workflow sets 2.
await() {
  local timeout="$1" desc="$2"
  shift 2
  timeout=$((timeout * ${LOOKOUT_E2E_TIMEOUT_SCALE:-1}))
  local waited=0
  while true; do
    if "$@" >/dev/null 2>&1; then
      echo "  ✓ $desc (${waited}s)"
      return 0
    fi
    if ((waited >= timeout)); then
      echo "  ✗ TIMEOUT after ${timeout}s waiting for: $desc" >&2
      return 1
    fi
    sleep 5
    waited=$((waited + 5))
  done
}

# await_namespace_settled <name>
#
# Block while a previous incarnation of <name> finishes terminating.
#
# Every UAT fixture owns a namespace and reverts by deleting it with
# --wait=false, which returns long before the namespace is gone. Two
# runs back to back — the normal loop while writing a case — then race:
# `kubectl apply` of a Namespace already in Terminating succeeds and
# every object created inside it is rejected, so the inject fails a
# step later with an error that says nothing about the real cause.
await_namespace_settled() {
  # Terminating specifically, not merely present: waiting for the
  # delete of a namespace that is simply already there — the ordinary
  # re-inject — would block for the whole timeout and then carry on.
  if [[ "$(kubectl get namespace "$1" -o jsonpath='{.status.phase}' 2>/dev/null)" == Terminating ]]; then
    kubectl wait --for=delete "namespace/$1" --timeout=180s >/dev/null 2>&1 || true
  fi
}

# fresh_namespace <name> — await_namespace_settled, then create it.
# Fixtures that need extra labels on the namespace call the two halves
# themselves.
fresh_namespace() {
  local ns="$1"
  await_namespace_settled "$ns"
  kubectl apply -f - <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $ns
  labels: { app.kubernetes.io/part-of: lookout-examples }
YAML
}

# await_inject <name> <timeout> <grep-pattern...>
#
# Waits until a single stub-log line appended after stub_mark <name>
# matches EVERY given extended-regexp pattern. Prints the matching line
# as evidence on success.
await_inject() {
  local name="$1" timeout="$2"
  shift 2
  _stub_match() {
    local out
    out="$(stub_since "$name")"
    local pat
    for pat in "$@"; do
      out="$(grep -E "$pat" <<<"$out" || true)"
      [[ -n "$out" ]] || return 1
    done
    return 0
  }
  if await "$timeout" "sentinel inject matching: $*" _stub_match "$@"; then
    local out
    out="$(stub_since "$name")"
    local pat
    for pat in "$@"; do
      out="$(grep -E "$pat" <<<"$out" || true)"
    done
    head -n1 <<<"$out" | cut -c1-200 | sed 's/^/    wire: /'
    return 0
  fi
  return 1
}

# await_finding <timeout> <grep-pattern> -- <lookout args...>
#
# Polls a read-path command until its stdout matches the pattern.
# Prints the matching finding line as evidence on success.
#
# Both the match and the evidence CAPTURE the output first and match
# against the capture. Piping lookout straight into `grep -q` (or into
# `grep | head -n1`) reads better and is wrong once a command emits more
# than a pipe buffer of findings: grep exits at the first match, lookout
# takes SIGPIPE mid-stream, and `set -o pipefail` turns a pass into an
# exit 141 that kills the scenario. Invisible on a two-worker cluster,
# reproducible at fleet scale — see examples/kwok/.
await_finding() {
  local timeout="$1" pattern="$2"
  shift 2
  [[ "${1:-}" == "--" ]] && shift
  _finding_match() {
    local out
    out="$(run_lookout "$@" 2>/dev/null || true)"
    grep -Eq "$pattern" <<<"$out"
  }
  if await "$timeout" "lookout $* → /$pattern/" _finding_match "$@"; then
    local out line
    out="$(run_lookout "$@" 2>/dev/null || true)"
    line="$(grep -E -m1 "$pattern" <<<"$out" || true)"
    [[ -n "$line" ]] && printf '    read: %s\n' "${line:0:200}"
    return 0
  fi
  return 1
}

# soft <command...> — run a check but only warn on failure (for signals
# whose timing depends on cluster noise, e.g. storm formation).
soft() {
  if ! "$@"; then
    echo "  ⚠ soft check failed (continuing): $*" >&2
  fi
  return 0
}
