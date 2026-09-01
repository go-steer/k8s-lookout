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

# Assertion helpers for the CLI UAT layer (docs/testing/cli-uat.md).
#
# examples/lib.sh is about *provoking* signals — break a workload, wait
# for the sentinel to notice. This file is about *reading*: run a
# read-path command, then make claims about its exit code, its stdout,
# and the shape of what it printed. The two are sourced together by
# examples/uat; nothing here duplicates a helper from there.
#
# The unit is a check, not a test binary: every uat_* assertion records
# a PASS or a FAIL and RETURNS, so one broken command reports one
# failure instead of aborting the run. That is deliberate — a UAT run
# that stops at the first failure tells you about one command, and the
# question this layer answers is "which of the 34 are wrong".

# ---- tiers ----------------------------------------------------------------

# The tier this run is allowed to reach (docs/testing/cli-uat.md §
# Environment tiers). A case tagged above it is SKIPPED and counted,
# never silently dropped — a kind run must be able to say what it did
# not cover.
UAT_TIER="${UAT_TIER:-T0}"

uat_tier_enabled() {
  local want="$1"
  [[ "${want#T}" -le "${UAT_TIER#T}" ]]
}

# ---- tallies --------------------------------------------------------------

UAT_PASS=0
UAT_FAIL=0
UAT_SKIP=0
UAT_FAILURES=()
UAT_SKIPS=()

# uat_ok <description> — record a passing check.
uat_ok() {
  UAT_PASS=$((UAT_PASS + 1))
  printf '  \033[32m✓\033[0m %s\n' "$1"
}

# uat_bad <description> [evidence...] — record a failing check. The
# evidence lines are printed indented under it AND kept for the final
# summary, so a 34-command run does not make you scroll back.
uat_bad() {
  local desc="$1"
  shift
  UAT_FAIL=$((UAT_FAIL + 1))
  UAT_FAILURES+=("$desc")
  printf '  \033[31m✗\033[0m %s\n' "$desc" >&2
  local line
  for line in "$@"; do
    printf '      %s\n' "${line:0:300}" >&2
  done
}

# uat_skipped <description> <why> — record a check this tier cannot run.
uat_skipped() {
  UAT_SKIP=$((UAT_SKIP + 1))
  UAT_SKIPS+=("$1 — $2")
  printf '  \033[33m∅\033[0m %s (%s)\n' "$1" "$2"
}

# uat_section <title> — visual grouping only.
uat_section() {
  printf '\n\033[1m── %s ──\033[0m\n' "$1"
}

# ---- running a command ----------------------------------------------------

# uat_run <lookout args...> — run a read-path command, capturing its
# three outputs into globals for the assertions below:
#
#   UAT_OUT  stdout, verbatim
#   UAT_ERR  stderr, verbatim
#   UAT_RC   exit status
#   UAT_CMD  the invocation, for failure messages
#
# stdout and stderr are captured SEPARATELY rather than merged, because
# half the contract (§4.2 stdout purity) is a claim about which stream a
# line went to. Merging them would make that claim untestable.
uat_run() {
  UAT_CMD="lookout $*"
  local err_file
  err_file="$(mktemp)"
  set +e
  UAT_OUT="$(run_lookout "$@" 2>"$err_file")"
  UAT_RC=$?
  set -e
  UAT_ERR="$(cat "$err_file")"
  rm -f "$err_file"
}

# ---- assertions on the last uat_run ---------------------------------------

# The §4.2 exit-code contract: 0 = data, 1 = runtime error, 2 = usage
# error. Note that "no findings" is exit 0 — an empty result is data.
uat_expect_exit() {
  local want="$1" desc="${2:-$UAT_CMD → exit $1}"
  if [[ "$UAT_RC" == "$want" ]]; then
    uat_ok "$desc"
    return 0
  fi
  uat_bad "$desc" "got exit $UAT_RC, want $want" "stdout: ${UAT_OUT:-<empty>}" "stderr: ${UAT_ERR:-<empty>}"
  return 1
}

# Every read command ends with `scanned=N findings=N elapsed=D`, and
# that line is on STDOUT (it is payload, not diagnostics). Extra keys
# after elapsed= are allowed and expected — exempt=, context=,
# unavailable=, skipped_no_subject= are all documented summary keys.
uat_expect_summary_line() {
  local desc="${1:-$UAT_CMD → summary line}"
  local last
  last="$(tail -n1 <<<"$UAT_OUT")"
  if [[ "$last" =~ ^scanned=[0-9]+\ findings=[0-9]+\ elapsed=[^\ ]+ ]]; then
    uat_ok "$desc"
    return 0
  fi
  uat_bad "$desc" "last stdout line: ${last:-<empty>}"
  return 1
}

# stdout purity: with stderr discarded, every stdout line is payload.
# A logfmt payload line starts with `kind=`; the final line is the
# summary. Anything else — a warning, a progress note, a stray print —
# is a contract violation, because stdout is what a pipeline consumes.
uat_expect_stdout_pure() {
  local desc="${1:-$UAT_CMD → stdout is payload only}"
  local bad=()
  local n=0 total
  total="$(wc -l <<<"$UAT_OUT")"
  while IFS= read -r line; do
    n=$((n + 1))
    [[ -z "$line" ]] && continue
    # The last line is the summary, checked separately.
    ((n == total)) && continue
    [[ "$line" == kind=* ]] && continue
    bad+=("line $n: $line")
  done <<<"$UAT_OUT"
  if ((${#bad[@]} == 0)); then
    uat_ok "$desc"
    return 0
  fi
  uat_bad "$desc" "${bad[@]}"
  return 1
}

# --format=json: every line parses as an object. The format is
# newline-delimited JSON (one record per line), NOT a JSON array —
# asserting `jq .` over the whole blob would pass on a single record
# and fail on two, which is the wrong test.
uat_expect_json() {
  local desc="${1:-$UAT_CMD → newline-delimited JSON}"
  local n=0 bad=()
  while IFS= read -r line; do
    n=$((n + 1))
    [[ -z "$line" ]] && continue
    if ! jq -e . >/dev/null 2>&1 <<<"$line"; then
      bad+=("line $n is not JSON: ${line:0:160}")
    fi
  done <<<"$UAT_OUT"
  if ((${#bad[@]} == 0)) && ((n > 0)); then
    uat_ok "$desc"
    return 0
  fi
  ((n == 0)) && bad+=("no output at all")
  uat_bad "$desc" "${bad[@]}"
  return 1
}

# uat_expect_stdout <extended-regexp> [description]
uat_expect_stdout() {
  local pattern="$1" desc="${2:-$UAT_CMD → /$1/}"
  if grep -Eq "$pattern" <<<"$UAT_OUT"; then
    uat_ok "$desc"
    return 0
  fi
  uat_bad "$desc" "stdout did not match /$pattern/" "${UAT_OUT:-<empty>}"
  return 1
}

# uat_refute_stdout <extended-regexp> [description] — the assertion
# that carries the secret-safety contract, so its failure message
# deliberately does NOT echo the matching line.
uat_refute_stdout() {
  local pattern="$1" desc="${2:-$UAT_CMD → NOT /$1/}"
  if grep -Eq "$pattern" <<<"$UAT_OUT"; then
    uat_bad "$desc" "stdout matched /$pattern/ (match withheld — it may be the secret)"
    return 1
  fi
  uat_ok "$desc"
  return 0
}

# uat_expect_stderr <extended-regexp> [description] — for the loud-skip
# and refusal messages, which are diagnostics and belong on stderr.
uat_expect_stderr() {
  local pattern="$1" desc="${2:-$UAT_CMD → stderr /$1/}"
  if grep -Eq "$pattern" <<<"$UAT_ERR"; then
    uat_ok "$desc"
    return 0
  fi
  uat_bad "$desc" "stderr did not match /$pattern/" "${UAT_ERR:-<empty>}"
  return 1
}

# ---- the command registry -------------------------------------------------

# uat_read_commands — every read-path check, as its CLI command path,
# one per line ("state edges", "triage delta", …).
#
# Derived from `mcp --list-tools`, which prints the tool registry: the
# same source the MCP server, the site Reference pages and `--help` are
# generated from. A hand-written list here would go stale the first
# time someone adds a command, and the whole point of this layer is to
# be the thing that notices.
uat_read_commands() {
  run_lookout mcp --list-tools 2>/dev/null |
    awk '$1 ~ /^k8s_/ { $1=""; $2=""; sub(/^ +/, ""); print }'
}

# uat_accepts_flag <command...> <flag> — does this command's --help
# advertise <flag>? Used to build a valid invocation per command
# without a hand-maintained table of who takes what.
uat_accepts_flag() {
  local flag="${*: -1}"
  local cmd=("${@:1:$#-1}")
  run_lookout "${cmd[@]}" --help 2>&1 | grep -Eq -- "(^|[[:space:]])${flag}(=|[[:space:]]|$)"
}

# uat_tool_names — the registry again, as "<tool_name> <cli path>" per
# line. The two are NOT a mechanical transform of each other: `bundle`
# is served as k8s_triage_workload and `state webhooks` as
# k8s_admission_webhooks, so anything pairing a CLI command with its
# tool has to read the mapping rather than derive it.
uat_tool_names() {
  run_lookout mcp --list-tools 2>/dev/null |
    awk '$1 ~ /^k8s_/ { name=$1; $1=""; $2=""; sub(/^ +/, ""); print name, $0 }'
}

# ---- an MCP client, in curl -------------------------------------------------

# The server speaks streamable HTTP: responses come back as SSE frames
# ("event: message" / "data: {...}"), and every request after the
# handshake must echo the session id the handshake handed out.
UAT_MCP_URL=""
UAT_MCP_SID=""
UAT_MCP_PID=""
UAT_MCP_LOG=""

# uat_mcp_start [extra flags...] — start a server on a free loopback
# port and complete the handshake. Returns non-zero if it never came
# up, so a case can report that once instead of failing every call.
uat_mcp_start() {
  local port
  port="$(uat_free_port)"
  UAT_MCP_URL="http://127.0.0.1:$port"
  UAT_MCP_LOG="$UAT_WORKDIR/mcp-$port.log"

  "$(lookout_bin)" mcp --listen="127.0.0.1:$port" "$@" >"$UAT_MCP_LOG" 2>&1 &
  UAT_MCP_PID=$!

  local i
  for i in $(seq 1 50); do
    UAT_MCP_SID="$(curl -sS -m 5 -X POST "$UAT_MCP_URL" \
      -H 'Content-Type: application/json' \
      -H 'Accept: application/json, text/event-stream' \
      -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"lookout-uat","version":"0"}}}' \
      -D - -o /dev/null 2>/dev/null | grep -i '^Mcp-Session-Id:' | tr -d '\r' | awk '{print $2}')"
    [[ -n "$UAT_MCP_SID" ]] && break
    kill -0 "$UAT_MCP_PID" 2>/dev/null || break
    sleep 0.2
  done
  [[ -n "$UAT_MCP_SID" ]] || return 1

  uat_mcp_raw '{"jsonrpc":"2.0","method":"notifications/initialized"}' >/dev/null
}

uat_mcp_stop() {
  [[ -n "$UAT_MCP_PID" ]] && kill "$UAT_MCP_PID" 2>/dev/null
  wait "$UAT_MCP_PID" 2>/dev/null
  UAT_MCP_PID=""
  UAT_MCP_SID=""
}

# uat_mcp_raw <json> — one JSON-RPC request, SSE unwrapped to the bare
# JSON response (or empty for a notification).
uat_mcp_raw() {
  curl -sS -m 120 -X POST "$UAT_MCP_URL" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' \
    -H "Mcp-Session-Id: $UAT_MCP_SID" \
    -d "$1" 2>/dev/null | sed -n 's/^data: //p'
}

# uat_mcp_call <tool> <arguments-json> — a tools/call, leaving the
# result in UAT_MCP_RESULT (the full JSON-RPC response). Sets
# UAT_MCP_TEXT to the concatenated text content, which is where a
# command's payload arrives.
UAT_MCP_RESULT=""
UAT_MCP_TEXT=""
uat_mcp_call() {
  local tool="$1" args="${2:-\{\}}"
  UAT_MCP_RESULT="$(uat_mcp_raw "$(jq -cn --arg n "$tool" --argjson a "$args" \
    '{jsonrpc:"2.0",id:2,method:"tools/call",params:{name:$n,arguments:$a}}')")"
  UAT_MCP_TEXT="$(jq -r '(.result.content // []) | map(select(.type=="text") | .text) | join("")' <<<"$UAT_MCP_RESULT" 2>/dev/null)"
}

# uat_free_port — ask the kernel for an unused one rather than guessing
# a constant; parallel jobs on a CI runner do collide.
uat_free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

# uat_normalize_payload — strip what two invocations of the same
# command cannot agree on, so that everything else can be compared byte
# for byte.
#
# The normalizer is deliberately small and every entry has to earn its
# place, because each one is a field the parity check stops covering.
# Every one is an observation of something that moves on its own
# between two calls, not a value a command chooses:
#
#   elapsed=      how long this run took
#   first_seen=   \ the ends of a sliding log window: the log tail is a
#   last_seen=    / fixed size, so new lines push old ones out
#   sample=       an example line drawn from that window
#   window=       a lookback anchored to now (triage changes reports
#                 the span it approximated over)
#   age=          now minus creationTimestamp (or a condition's last
#                 transition), rendered to the second below 48h — so it
#                 ticks between two calls on any young object. A
#                 workstation cluster is days old and hides this
#                 entirely; a CI cluster is six minutes old and every
#                 inventory line drifts.
#
# Counters are NOT normalized — count= and scanned= are stable across
# calls (the window is a fixed size), so a change in one is a real
# difference and should fail. Anything else differing between the CLI
# and the MCP tool is the regression this exists to catch.
uat_normalize_payload() {
  sed -E \
    -e 's/elapsed=[0-9.]+[a-zµ]*/elapsed=NORM/' \
    -e 's/(first_seen|last_seen|window)=[^ ]+/\1=NORM/g' \
    -e 's/sample="([^"\\]|\\.)*"/sample="NORM"/g' \
    -e 's/(^|[[:space:]])age=[^ ]*/\1age=NORM/g' |
    sed -e 's/[[:space:]]*$//' -e '/^$/d'
}
