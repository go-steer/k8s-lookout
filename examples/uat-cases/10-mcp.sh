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

# Part 4 of docs/testing/cli-uat.md — the MCP surface.
#
# `lookout mcp` is a second way into every read-path command, and the
# one an agent actually uses. Its help states the contract plainly:
# "tool results carry the same payload the CLI prints". Nothing checked
# that, which means a command could regress on the path most callers
# take while every CLI test stayed green.
#
# So this replays the SAME invocations 00-contract.sh uses — from the
# shared table in uat-invocations.sh — through a real server over HTTP,
# and compares byte for byte against the CLI. Driving both sides from
# one description of a valid call is what makes the comparison mean
# something; two hand-written lists would drift into agreeing about the
# wrong thing.
#
# The CLI-name-to-tool-name mapping is read, never derived: 20 of the
# 34 differ (`bundle` is k8s_triage_workload, `state webhooks` is
# k8s_admission_webhooks), so guessing would silently test nothing.

# ---- turning a CLI invocation into tool arguments --------------------------

# uat_mcp_args <tool-schema-json> <invocation-string>
#
# The table holds CLI fragments ("--pack=apiserver", "-A", or a bare
# positional). The tool wants a JSON object. The transform is
# mechanical but not type-blind: flag names lose their dashes
# ("--all-namespaces" → all_namespaces), a positional becomes "target",
# and each value is cast to whatever the schema declares — passing
# "true" as a string to a boolean property is a different call, and a
# parity check that quietly makes a different call is worse than none.
uat_mcp_args() {
  local schema="$1" invocation="$2"
  local out='{}' tok name value type
  for tok in $invocation; do
    if [[ "$tok" == "-A" ]]; then
      name="all_namespaces"
      value="true"
    elif [[ "$tok" == --*=* ]]; then
      name="${tok%%=*}"
      name="${name#--}"
      name="${name//-/_}"
      value="${tok#*=}"
    elif [[ "$tok" == --* ]]; then
      name="${tok#--}"
      name="${name//-/_}"
      value="true"
    else
      name="target"
      value="$tok"
    fi

    type="$(jq -r --arg n "$name" '.properties[$n].type // "string"' <<<"$schema")"
    case "$type" in
      boolean) out="$(jq -c --arg n "$name" --argjson v "$value" '.[$n]=$v' <<<"$out")" ;;
      integer | number) out="$(jq -c --arg n "$name" --argjson v "$value" '.[$n]=$v' <<<"$out")" ;;
      *) out="$(jq -c --arg n "$name" --arg v "$value" '.[$n]=$v' <<<"$out")" ;;
    esac
  done
  echo "$out"
}

# ---- the run ---------------------------------------------------------------

uat_case_mcp() {
  if ! command -v jq >/dev/null 2>&1; then
    uat_skipped "mcp parity" "jq is not installed"
    return 0
  fi

  uat_section "mcp: the advertised surface"

  uat_contract_fixtures
  uat_build_invocations

  local -a commands
  mapfile -t commands < <(uat_read_commands)

  # tool name ↔ CLI path, read from the registry.
  local -A tool_of=()
  local line tool cli
  while read -r line; do
    tool="${line%% *}"
    cli="${line#* }"
    tool_of["$cli"]="$tool"
  done < <(uat_tool_names)

  if ! uat_mcp_start; then
    uat_bad "mcp server starts on loopback" "no session id after 10s" "$(cat "$UAT_MCP_LOG" 2>/dev/null)"
    return 1
  fi
  uat_ok "mcp server starts on loopback and completes the handshake"

  local tools_json
  tools_json="$(uat_mcp_raw '{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}')"

  # Every registered command must materialize as a tool. This is the
  # same anti-staleness idea as the contract case, pointed at the other
  # surface: a command that exists on the CLI but never reaches the
  # tool list is invisible to every agent, which is a worse failure
  # than a wrong answer because nothing reports it.
  local cmd missing=()
  for cmd in "${commands[@]}"; do
    tool="${tool_of[$cmd]:-}"
    if [[ -z "$tool" ]] || ! jq -e --arg t "$tool" '.result.tools[] | select(.name==$t)' <<<"$tools_json" >/dev/null 2>&1; then
      missing+=("$cmd")
    fi
  done
  if ((${#missing[@]} > 0)); then
    uat_bad "every registered command is served as a tool" "not in tools/list: ${missing[*]}"
  else
    uat_ok "every registered command is served as a tool (${#commands[@]})"
  fi

  # And each one must carry a usable input schema — an object with
  # properties. A tool advertised with an empty schema is one a model
  # cannot call correctly.
  local bad_schema=()
  for cmd in "${commands[@]}"; do
    tool="${tool_of[$cmd]:-}"
    [[ -n "$tool" ]] || continue
    jq -e --arg t "$tool" \
      '.result.tools[] | select(.name==$t) | .inputSchema | select(.type=="object") | .properties | select(length>0)' \
      <<<"$tools_json" >/dev/null 2>&1 || bad_schema+=("$cmd")
  done
  if ((${#bad_schema[@]} > 0)); then
    uat_bad "every tool advertises a JSON object schema with properties" "empty or non-object schema: ${bad_schema[*]}"
  else
    uat_ok "every tool advertises a JSON object schema with properties"
  fi

  uat_section "mcp: tool result equals CLI output"

  local schema args cli_out mcp_out
  for cmd in "${commands[@]}"; do
    [[ -v UAT_INVOCATION[$cmd] ]] || continue
    tool="${tool_of[$cmd]:-}"
    [[ -n "$tool" ]] || continue

    if ! uat_tier_enabled "${UAT_COMMAND_TIER[$cmd]:-T0}"; then
      uat_skipped "$cmd ↔ $tool → parity" "needs tier ${UAT_COMMAND_TIER[$cmd]}, running $UAT_TIER"
      continue
    fi
    if [[ -v UAT_COMMAND_SKIP[$cmd] ]]; then
      uat_skipped "$cmd ↔ $tool → parity" "${UAT_COMMAND_SKIP[$cmd]}"
      continue
    fi

    schema="$(jq -c --arg t "$tool" '.result.tools[] | select(.name==$t) | .inputSchema' <<<"$tools_json")"
    args="$(uat_mcp_args "$schema" "${UAT_INVOCATION[$cmd]}")"

    # Rewind between the two calls: the store-backed commands persist
    # what they report, so without this the second caller is asking a
    # different question and a passing comparison would prove nothing.
    uat_store_rewind
    local -a argv
    # shellcheck disable=SC2206 — word splitting into an argv is the point
    argv=($cmd ${UAT_INVOCATION[$cmd]})
    uat_run "${argv[@]}"
    cli_out="$(uat_normalize_payload <<<"$UAT_OUT")"

    uat_store_rewind
    uat_mcp_call "$tool" "$args"
    mcp_out="$(uat_normalize_payload <<<"$UAT_MCP_TEXT")"

    if [[ "$cli_out" == "$mcp_out" ]]; then
      uat_ok "$cmd ↔ $tool → same payload"
    else
      # A mismatch has two possible causes and they deserve different
      # verdicts: the tool path really does differ from the CLI, or the
      # cluster moved between the two calls and nothing was comparable
      # in the first place. The control for the second is to ask the
      # CLI the same question twice — if it cannot reproduce itself,
      # the comparison is void, so say that instead of blaming the
      # tool. (#367: `web` writes a probe log line every 5s, and the
      # count of them is in the payload.)
      uat_store_rewind
      uat_run "${argv[@]}"
      local cli_again
      cli_again="$(uat_normalize_payload <<<"$UAT_OUT")"
      if [[ "$cli_again" != "$cli_out" ]]; then
        uat_skipped "$cmd ↔ $tool → parity" \
          "the cluster moved between two identical CLI calls, so the halves are not comparable: $(diff <(echo "$cli_out") <(echo "$cli_again") | grep -E '^[<>]' | head -2 | cut -c1-120 | tr '\n' '|')"
      else
        uat_bad "$cmd ↔ $tool → same payload" \
          "tool result differs from CLI stdout, and the CLI reproduced itself" \
          "args: $args" \
          "$(diff <(echo "$cli_out") <(echo "$mcp_out") | head -12)"
      fi
    fi
  done

  uat_mcp_stop

  uat_section "mcp: a bind flag must not open an unauthenticated cluster-read API"

  # The whole surface reads the cluster, so the refusal is the security
  # property, not a convenience. Assert it is refused as a USAGE error
  # rather than accepted-and-warned.
  uat_run mcp --listen=0.0.0.0:0
  uat_expect_exit 2 "mcp → refuses a non-loopback bind"
  uat_expect_stderr 'loopback' "mcp → says why it refused"

  # Each of the three flags alone is still a refusal: the point is that
  # opting in does not on its own make the bind safe.
  uat_run mcp --listen=0.0.0.0:0 --allow-non-loopback
  uat_expect_exit 2 "mcp → --allow-non-loopback alone is still refused"

  local token_file="$UAT_WORKDIR/mcp-token" access_log="$UAT_WORKDIR/mcp-access.log"
  echo "uat-secret-token" >"$token_file"
  chmod 600 "$token_file"

  uat_run mcp --listen=0.0.0.0:0 --allow-non-loopback --auth-token-file="$token_file"
  uat_expect_exit 2 "mcp → without --access-log it is still refused"

  uat_section "mcp: the off-host bind authenticates"

  # All three flags together is the one accepted off-host configuration,
  # so this is where the 401 has to hold.
  local port
  port="$(uat_free_port)"
  "$(lookout_bin)" mcp --listen="0.0.0.0:$port" --allow-non-loopback \
    --auth-token-file="$token_file" --access-log="$access_log" \
    >"$UAT_WORKDIR/mcp-offhost.log" 2>&1 &
  local off_pid=$!
  local i code
  for i in $(seq 1 50); do
    curl -sS -m 2 -o /dev/null "http://127.0.0.1:$port" 2>/dev/null && break
    kill -0 "$off_pid" 2>/dev/null || break
    sleep 0.2
  done

  code="$(curl -sS -m 10 -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port" \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"uat","version":"0"}}}' 2>/dev/null)"
  if [[ "$code" == "401" ]]; then
    uat_ok "mcp → off-host request without the token gets 401"
  else
    uat_bad "mcp → off-host request without the token gets 401" "got HTTP $code" "$(cat "$UAT_WORKDIR/mcp-offhost.log" 2>/dev/null)"
  fi

  local off_sid
  off_sid="$(curl -sS -m 10 -D - -o /dev/null -X POST "http://127.0.0.1:$port" \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    -H "Authorization: Bearer uat-secret-token" \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"uat","version":"0"}}}' 2>/dev/null |
    grep -i '^Mcp-Session-Id:' | tr -d '\r' | awk '{print $2}')"
  if [[ -n "$off_sid" ]]; then
    uat_ok "mcp → off-host request with the token is served"
  else
    uat_bad "mcp → off-host request with the token is served" "no session id returned" "$(cat "$UAT_WORKDIR/mcp-offhost.log" 2>/dev/null)"
  fi

  # A wrong token must fail the same way a missing one does.
  code="$(curl -sS -m 10 -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port" \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    -H "Authorization: Bearer not-the-token" \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"uat","version":"0"}}}' 2>/dev/null)"
  if [[ "$code" == "401" ]]; then
    uat_ok "mcp → off-host request with a wrong token gets 401"
  else
    uat_bad "mcp → off-host request with a wrong token gets 401" "got HTTP $code"
  fi

  # The access log records one line per TOOL CALL, so it takes an
  # actual tool call to test — an authenticated handshake alone leaves
  # it legitimately empty.
  if [[ -n "$off_sid" ]]; then
    curl -sS -m 10 -o /dev/null -X POST "http://127.0.0.1:$port" \
      -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
      -H "Authorization: Bearer uat-secret-token" -H "Mcp-Session-Id: $off_sid" \
      -d '{"jsonrpc":"2.0","method":"notifications/initialized"}' 2>/dev/null
    curl -sS -m 60 -o /dev/null -X POST "http://127.0.0.1:$port" \
      -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
      -H "Authorization: Bearer uat-secret-token" -H "Mcp-Session-Id: $off_sid" \
      -d '{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"k8s_admission_webhooks","arguments":{}}}' 2>/dev/null
  fi

  kill "$off_pid" 2>/dev/null
  wait "$off_pid" 2>/dev/null

  # Mandatory off-host precisely so the calls are recorded. Assert the
  # documented shape too — a line that names the tool and carries an
  # exit code is the operational record; a line that merely exists is
  # not. It must NOT carry the arguments or the response body, which is
  # the difference between an audit trail and a second copy of cluster
  # data.
  if [[ ! -s "$access_log" ]]; then
    uat_bad "mcp → the mandatory off-host access log records tool calls" "$access_log is empty or absent"
  elif ! grep -q 'k8s_admission_webhooks' "$access_log"; then
    uat_bad "mcp → the access log names the tool called" "no k8s_admission_webhooks line" "$(cat "$access_log")"
  else
    uat_ok "mcp → the mandatory off-host access log records tool calls"
    if grep -Eq 'tool=[^ ]+.*(exit|status|code)=' "$access_log"; then
      uat_ok "mcp → the access log line carries tool and exit code"
    else
      uat_bad "mcp → the access log line carries tool and exit code" "unexpected shape" "$(head -3 "$access_log")"
    fi
  fi

  # 0600: it is an operational record of who read the cluster and when.
  if [[ -f "$access_log" ]]; then
    local mode
    mode="$(stat -c '%a' "$access_log" 2>/dev/null)"
    if [[ "$mode" == "600" ]]; then
      uat_ok "mcp → the access log is mode 0600"
    else
      uat_bad "mcp → the access log is mode 0600" "got $mode"
    fi
  fi

  uat_section "mcp: the advertised surface is selectable"

  # Every tool costs tokens and choice accuracy on every model call, so
  # a profile that quietly served everything would be a real cost bug.
  local full_n audit_n triage_n
  full_n="$(run_lookout mcp --list-tools 2>/dev/null | grep -c '^k8s_')"
  audit_n="$(run_lookout mcp --profile=audit --list-tools 2>/dev/null | grep -c '^k8s_')"
  triage_n="$(run_lookout mcp --profile=triage --list-tools 2>/dev/null | grep -c '^k8s_')"

  if ((audit_n > 0 && audit_n < full_n)); then
    uat_ok "mcp → --profile=audit narrows the surface ($audit_n of $full_n)"
  else
    uat_bad "mcp → --profile=audit narrows the surface" "audit=$audit_n full=$full_n"
  fi
  if ((triage_n > 0 && triage_n < full_n)); then
    uat_ok "mcp → --profile=triage narrows the surface ($triage_n of $full_n)"
  else
    uat_bad "mcp → --profile=triage narrows the surface" "triage=$triage_n full=$full_n"
  fi

  # --tools is documented as left-to-right over the profile, so a
  # removal has to actually remove.
  local minus_n
  minus_n="$(run_lookout mcp --profile=triage --tools=-k8s_triage_logs --list-tools 2>/dev/null | grep -c '^k8s_')"
  if ((minus_n == triage_n - 1)); then
    uat_ok "mcp → --tools=-<name> removes one from the profile ($minus_n of $triage_n)"
  else
    uat_bad "mcp → --tools=-<name> removes one from the profile" "got $minus_n, want $((triage_n - 1))"
  fi
  if run_lookout mcp --profile=triage --tools=-k8s_triage_logs --list-tools 2>/dev/null | grep -q '^k8s_triage_logs'; then
    uat_bad "mcp → the removed tool is really gone" "k8s_triage_logs is still advertised"
  else
    uat_ok "mcp → the removed tool is really gone"
  fi

  # An unknown name is a usage error, not a silently empty surface.
  uat_run mcp --tools=k8s_no_such_tool --list-tools
  uat_expect_exit 2 "mcp → --tools with an unknown name is a usage error"
}
