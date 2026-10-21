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

# Transparent tracing shim for kubectl / gcloud. Sourced by the two
# wrappers beside it with $1 = the tool name.
#
# Runs the real binary (the first one on PATH that is not this
# directory) with the caller's arguments, stdin and environment, passes
# its stdout, stderr and exit code through unchanged, and appends one
# JSON line to $COMPARE_TRACE:
#
#   {"ts":"...","tool":"kubectl","argv":[...],"kubeconfig":"<basename>",
#    "ms":123,"stdout_bytes":4567,"stderr_bytes":0,"rc":0}
#
# Only sizes and argv are recorded, never payloads: the sanitizer rule
# that governs every lookout output surface applies to this trace too.
# Values following credential-shaped flags are masked defensively even
# though the collectors never pass any.
#
# With COMPARE_TRACE unset the shim is a plain exec, so a stray PATH
# entry costs nothing and changes nothing.

shim_main() {
  local tool="$1"; shift
  local here real=""
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local IFS=':'
  for dir in $PATH; do
    [[ -z "$dir" || "$dir" == "$here" ]] && continue
    if [[ -x "$dir/$tool" && ! -d "$dir/$tool" ]]; then
      real="$dir/$tool"; break
    fi
  done
  unset IFS
  [[ -n "$real" ]] || { echo "$tool: real binary not found on PATH (shim at $here)" >&2; exit 127; }

  if [[ -z "${COMPARE_TRACE:-}" ]]; then
    exec "$real" "$@"
  fi

  local tmpdir="${COMPARE_SHIM_TMP:-${TMPDIR:-/tmp}}"
  local out err
  out="$(mktemp "$tmpdir/shim.out.XXXXXX")"
  err="$(mktemp "$tmpdir/shim.err.XXXXXX")"
  local start end rc=0
  start=$(date +%s%N)
  "$real" "$@" >"$out" 2>"$err" || rc=$?
  end=$(date +%s%N)

  # Pass-through, preserving order of the two streams as best a
  # non-streaming shim can: stdout first, then stderr.
  cat "$out"
  cat "$err" >&2

  local so se
  so=$(stat -c %s "$out")
  se=$(stat -c %s "$err")
  rm -f "$out" "$err"

  # argv as a JSON array; backslash and double quote escaped, control
  # characters dropped (none of these tools take any).
  local argv="" a esc mask=0
  for a in "$@"; do
    if (( mask )); then a="<masked>"; mask=0; fi
    case "$a" in
      --token|--password|--access-token-file|--key-file) mask=1 ;;
      --token=*|--password=*) a="${a%%=*}=<masked>" ;;
    esac
    esc="${a//\\/\\\\}"
    esc="${esc//\"/\\\"}"
    esc="$(printf '%s' "$esc" | tr -d '\000-\037')"
    argv+="${argv:+,}\"$esc\""
  done
  local kc="${KUBECONFIG:-}"
  kc="${kc##*/}"
  printf '{"ts":"%s","tool":"%s","argv":[%s],"kubeconfig":"%s","ms":%d,"stdout_bytes":%d,"stderr_bytes":%d,"rc":%d,"ppid":%d}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$tool" "$argv" "$kc" \
    $(( (end - start) / 1000000 )) "$so" "$se" "$rc" "$PPID" >>"$COMPARE_TRACE"
  exit "$rc"
}
