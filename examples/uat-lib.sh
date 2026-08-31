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
