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

# Part 3 of docs/testing/cli-uat.md — the cross-cutting contract, run
# against EVERY read-path command rather than a chosen few.
#
# The command list comes from `mcp --list-tools`, i.e. the registry the
# MCP server, `--help` and the site Reference pages are all generated
# from. That is the whole design: a check added next month is in this
# harness the moment it is registered, and if nobody gave it a valid
# invocation the run FAILS rather than quietly covering 34 of 35.
# Silent under-coverage is the failure mode a contract harness exists to
# prevent, so it is the one thing here that is never a warning.
#
# The invocation table itself lives in examples/uat-invocations.sh,
# because 10-mcp.sh drives the same calls through the MCP server.

# ---- the per-command contract ---------------------------------------------

uat_contract_one() {
  local cmd="$1"
  local tier="${UAT_COMMAND_TIER[$cmd]:-T0}"
  if ! uat_tier_enabled "$tier"; then
    uat_skipped "$cmd → contract (4 checks)" "needs tier $tier, running $UAT_TIER"
    return 0
  fi
  if [[ -v UAT_COMMAND_SKIP[$cmd] ]]; then
    uat_skipped "$cmd → contract (4 checks)" "${UAT_COMMAND_SKIP[$cmd]}"
    return 0
  fi
  local -a argv
  # shellcheck disable=SC2206 — word splitting into an argv is the point
  argv=($cmd ${UAT_INVOCATION[$cmd]})

  # 1. Exit code: a valid invocation against a live cluster is data.
  uat_run "${argv[@]}"
  local rc0=$UAT_RC
  uat_expect_exit 0 "$cmd → exit 0 on a valid invocation"

  # 2/3. Summary line and stdout purity — only meaningful if it ran.
  if [[ "$rc0" == 0 ]]; then
    uat_expect_summary_line "$cmd → summary line"
    uat_expect_stdout_pure "$cmd → stdout is payload only"
  else
    uat_skipped "$cmd → summary line" "command did not exit 0"
    uat_skipped "$cmd → stdout is payload only" "command did not exit 0"
  fi

  # 4. --format=json parses, line by line.
  uat_run "${argv[@]}" --format=json
  if [[ "$UAT_RC" == 0 ]]; then
    uat_expect_json "$cmd → --format=json parses"
  else
    uat_skipped "$cmd → --format=json parses" "command did not exit 0"
  fi

  # 5. An unknown flag is a usage error, exit 2 — never a silent
  #    ignore, and never a crash.
  uat_run "${argv[@]}" --no-such-flag=1
  uat_expect_exit 2 "$cmd → unknown flag is a usage error"
}

# ---- the run --------------------------------------------------------------

uat_case_contract() {
  uat_section "contract: every registered read-path command"

  uat_contract_fixtures
  uat_build_invocations

  local -a commands
  mapfile -t commands < <(uat_read_commands)
  if ((${#commands[@]} == 0)); then
    uat_bad "registry enumeration" "mcp --list-tools returned no commands"
    return 1
  fi
  printf '  %d commands from the registry\n' "${#commands[@]}"

  # The anti-staleness guard, FIRST: a command nobody gave an
  # invocation is a failure of this file, and saying so before the
  # 130-odd checks scroll past is the difference between noticing and
  # not.
  local cmd missing=()
  for cmd in "${commands[@]}"; do
    [[ -v UAT_INVOCATION[$cmd] ]] || missing+=("$cmd")
  done
  if ((${#missing[@]} > 0)); then
    uat_bad "every registered command has a UAT invocation" \
      "no entry in UAT_INVOCATION for: ${missing[*]}" \
      "add one to examples/uat-cases/00-contract.sh — a new command is not covered until you do"
  else
    uat_ok "every registered command has a UAT invocation (${#commands[@]})"
  fi

  # The reverse: an entry for a command that no longer exists means the
  # table is describing a surface that is gone.
  local -A registered=()
  for cmd in "${commands[@]}"; do registered[$cmd]=1; done
  local stale=()
  for cmd in "${!UAT_INVOCATION[@]}"; do
    [[ -v registered[$cmd] ]] || stale+=("$cmd")
  done
  if ((${#stale[@]} > 0)); then
    uat_bad "no UAT invocation names a command that does not exist" "stale entries: ${stale[*]}"
  else
    uat_ok "no UAT invocation names a command that does not exist"
  fi

  for cmd in "${commands[@]}"; do
    [[ -v UAT_INVOCATION[$cmd] ]] || continue
    uat_contract_one "$cmd"
  done

  uat_section "contract: scope flags mean what they say"

  # A command that does not read namespaced objects must REFUSE -A.
  # Accepting and ignoring it is the failure mode worth catching: the
  # caller believes they widened the scan and they did not.
  local rejecter
  for rejecter in "${UAT_SCOPE_REJECTERS[@]}"; do
    local -a argv2
    # shellcheck disable=SC2206 — as above
    argv2=($rejecter ${UAT_INVOCATION[$rejecter]})
    uat_run "${argv2[@]}" -A
    uat_expect_exit 2 "$rejecter → rejects -A rather than ignoring it"
  done

  uat_section "contract: --at belongs to the graph-backed commands"

  # --at is only meaningful where a store can answer "as of then".
  # Everywhere else it must be a usage error rather than a flag that
  # looks accepted and silently reports now.
  local graph_backed=" triage radius triage changes "
  for cmd in "${commands[@]}"; do
    [[ -v UAT_INVOCATION[$cmd] ]] || continue
    [[ "$graph_backed" == *" $cmd "* ]] && continue
    local -a argv3
    # shellcheck disable=SC2206 — as above
    argv3=($cmd ${UAT_INVOCATION[$cmd]})
    uat_run "${argv3[@]}" --at=2026-01-01T00:00:00Z
    uat_expect_exit 2 "$cmd → rejects --at (not graph-backed)"
  done

  # And where it IS accepted, it may not be answered from thin air:
  # --at without a store would silently report now, which is the one
  # wrong answer a post-mortem must never give.
  local gb
  for gb in "triage radius" "triage changes"; do
    local -a argv4
    # shellcheck disable=SC2206 — as above
    argv4=($gb ${UAT_INVOCATION[$gb]})
    uat_run "${argv4[@]}" --at=2026-01-01T00:00:00Z
    uat_expect_exit 2 "$gb → --at without --store is a usage error"
  done

  uat_section "contract: --timeout expires cleanly"

  # A deadline that cannot possibly be met is the cheapest way to walk
  # every command's cancellation path. Two answers are correct: give up
  # (exit 1, and blame the DEADLINE — a bare "connection refused" would
  # send the caller after the wrong problem), or report the partial
  # result as incomplete. Exit 2 would mean the flag is not accepted at
  # all; anything above 1 is a crash on a context that got cancelled,
  # which is the bug this check exists for.
  #
  # The wording IS pinned, and that is the #352 regression test. Which
  # layer notices the deadline first is a race the cluster's speed
  # decides — on a slow apiserver client-go's rate limiter gets there
  # first — so this check used to accept either phrasing and only
  # insist the message named a deadline. It no longer has to: the
  # headline is the caller's own --timeout whatever noticed it, and
  # anything the client said rides behind it as detail. A run where
  # this fails on "rate: Wait(n=1) would exceed context deadline" is
  # #352 back.
  local deadline_re="timed out after 1ms"
  for cmd in "${commands[@]}"; do
    [[ -v UAT_INVOCATION[$cmd] ]] || continue
    uat_tier_enabled "${UAT_COMMAND_TIER[$cmd]:-T0}" || continue
    [[ -v UAT_COMMAND_SKIP[$cmd] ]] && continue
    local -a argv5
    # shellcheck disable=SC2206 — as above
    argv5=($cmd ${UAT_INVOCATION[$cmd]})
    uat_run "${argv5[@]}" --timeout=1ms
    if ((UAT_RC == 1)); then
      uat_expect_stderr "$deadline_re" "$cmd → --timeout=1ms blames the deadline"
    elif ((UAT_RC == 0)); then
      uat_ok "$cmd → --timeout=1ms returns a partial result"
    else
      uat_bad "$cmd → --timeout=1ms expires cleanly" \
        "exit $UAT_RC (want 1 naming a deadline, or 0 with a partial result)" \
        "$(head -3 <<<"$UAT_ERR")"
    fi
  done
}
