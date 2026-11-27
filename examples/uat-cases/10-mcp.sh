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

    # Server-scoped flags (--store, --exemptions, --report, --kubeconfig,
    # --context) belong to `lookout mcp` startup rather than the MCP input
    # schema; skip any CLI flag absent from .properties.
    jq -e --arg n "$name" '.properties | has($n)' <<<"$schema" >/dev/null || continue

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

  # --max-response-bytes=0: parity compares whole payloads, and on a big
  # cluster the default limit would cut the tool result (#747) and fail
  # a comparison the CLI was never going to match.
  if ! uat_mcp_start --store="$UAT_STORE" --exemptions="$UAT_EXEMPTIONS" --report="$UAT_REPORT" --max-response-bytes=0; then
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

    # Match the operator-pinned flags passed to `lookout mcp` so CLI and
    # MCP see identical exemptions, store, and report inputs.
    local -a cli_pinned=(--exemptions="$UAT_EXEMPTIONS")
    local help_txt
    help_txt="$(run_lookout $cmd --help 2>/dev/null || true)"
    [[ "$help_txt" == *"--store"* ]] && cli_pinned+=(--store="$UAT_STORE")
    [[ "$help_txt" == *"--report"* ]] && cli_pinned+=(--report="$UAT_REPORT")

    # Rewind between the two calls: the store-backed commands persist
    # what they report, so without this the second caller is asking a
    # different question and a passing comparison would prove nothing.
    uat_store_rewind
    local -a argv
    # shellcheck disable=SC2206 — word splitting into an argv is the point
    argv=($cmd "${cli_pinned[@]}" ${UAT_INVOCATION[$cmd]})
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

  uat_section "mcp: ergonomics contracts (#867)"

  # Configured server tools/list count must match the full configured
  # --list-tools registry exactly.
  local live_cfg_n
  live_cfg_n="$(jq -r '.result.tools | length' <<<"$tools_json" 2>/dev/null)"
  if [[ "$live_cfg_n" == "${#commands[@]}" ]]; then
    uat_ok "mcp → configured tools/list count matches full registry ($live_cfg_n)"
  else
    uat_bad "mcp → configured tools/list count matches full registry" "got $live_cfg_n, want ${#commands[@]}"
  fi

  # Inapplicable common scope properties are omitted from schemas:
  # k8s_audit_all omits workload; k8s_audit_cluster omits namespace,
  # all_namespaces, and workload.
  if jq -e '.result.tools[] | select(.name=="k8s_audit_all") | .inputSchema.properties | (has("workload") | not) and has("namespace") and has("all_namespaces")' <<<"$tools_json" >/dev/null 2>&1; then
    uat_ok "mcp → k8s_audit_all schema omits workload and keeps namespace/all_namespaces"
  else
    uat_bad "mcp → k8s_audit_all schema omits workload and keeps namespace/all_namespaces" \
      "$(jq -c '.result.tools[] | select(.name=="k8s_audit_all") | .inputSchema.properties | keys' <<<"$tools_json")"
  fi
  if jq -e '.result.tools[] | select(.name=="k8s_audit_cluster") | .inputSchema.properties | (has("namespace") | not) and (has("all_namespaces") | not) and (has("workload") | not)' <<<"$tools_json" >/dev/null 2>&1; then
    uat_ok "mcp → k8s_audit_cluster schema omits namespace, all_namespaces, and workload"
  else
    uat_bad "mcp → k8s_audit_cluster schema omits namespace, all_namespaces, and workload" \
      "$(jq -c '.result.tools[] | select(.name=="k8s_audit_cluster") | .inputSchema.properties | keys' <<<"$tools_json")"
  fi

  # Namespace-scoped audit tools default to all_namespaces: true over MCP.
  uat_mcp_call "k8s_audit_all" "{}"
  local default_err
  default_err="$(jq -r '.error.message // empty' <<<"$UAT_MCP_RESULT" 2>/dev/null)"
  if [[ -z "$default_err" ]] && jq -e '(.result.isError // false) == false' <<<"$UAT_MCP_RESULT" >/dev/null 2>&1; then
    local default_mcp_out default_cli_out
    default_mcp_out="$(uat_normalize_payload <<<"$UAT_MCP_TEXT")"
    uat_run audit all -A --exemptions="$UAT_EXEMPTIONS"
    default_cli_out="$(uat_normalize_payload <<<"$UAT_OUT")"
    if [[ "$default_mcp_out" == "$default_cli_out" ]]; then
      uat_ok "mcp → k8s_audit_all with {} defaults to all_namespaces=true and matches CLI -A"
    else
      uat_run audit all -A --exemptions="$UAT_EXEMPTIONS"
      local default_cli_again
      default_cli_again="$(uat_normalize_payload <<<"$UAT_OUT")"
      if [[ "$default_cli_again" != "$default_cli_out" ]]; then
        uat_skipped "mcp → k8s_audit_all with {} matches CLI -A" "cluster moved between identical CLI calls"
      else
        uat_bad "mcp → k8s_audit_all with {} defaults to all_namespaces=true and matches CLI -A" \
          "$(diff <(echo "$default_cli_out") <(echo "$default_mcp_out") | head -12)"
      fi
    fi
  else
    uat_bad "mcp → k8s_audit_all with {} defaults to all_namespaces=true" "error: ${default_err:-$UAT_MCP_RESULT}"
  fi

  # Passing a stripped property returns JSON-RPC InvalidParams (-32602)
  # without the CLI `Run 'lookout ... --help'` trailer.
  uat_mcp_call "k8s_audit_all" '{"workload":"Deployment/default/api"}'
  local rej_code rej_msg
  rej_code="$(jq -r '.error.code // empty' <<<"$UAT_MCP_RESULT" 2>/dev/null)"
  rej_msg="$(jq -r '.error.message // empty' <<<"$UAT_MCP_RESULT" 2>/dev/null)"
  if [[ "$rej_code" == "-32602" && "$rej_msg" == *'unknown argument "workload"'* && "$rej_msg" != *"Run 'lookout"* ]]; then
    uat_ok "mcp → k8s_audit_all rejects stripped workload with MCP-worded InvalidParams"
  else
    uat_bad "mcp → k8s_audit_all rejects stripped workload with MCP-worded InvalidParams" \
      "code=$rej_code msg=$rej_msg"
  fi

  # Exercising an emit.Run ExitUsage error over live MCP (explicit
  # all_namespaces=false with no namespace) must use MCP property names
  # and omit the CLI `Run 'lookout ... --help'` trailer (#867).
  uat_mcp_call "k8s_audit_all" '{"all_namespaces":false}'
  local usage_code usage_msg
  usage_code="$(jq -r '.error.code // empty' <<<"$UAT_MCP_RESULT" 2>/dev/null)"
  usage_msg="$(jq -r '.error.message // empty' <<<"$UAT_MCP_RESULT" 2>/dev/null)"
  if [[ "$usage_code" == "-32602" && "$usage_msg" == *"namespace"* && "$usage_msg" == *"all_namespaces"* && "$usage_msg" != *"Run 'lookout"* ]]; then
    uat_ok "mcp → k8s_audit_all usage error uses MCP property names and omits --help hint"
  else
    uat_bad "mcp → k8s_audit_all usage error uses MCP property names and omits --help hint" \
      "code=$usage_code msg=$usage_msg"
  fi

  uat_mcp_stop

  # Unconfigured live server (no --store, no --exemptions) must omit
  # k8s_audit_exemptions and k8s_findings_diff from tools/list, retain
  # k8s_triage_status/k8s_cluster_health/k8s_findings_ack, and match bare
  # `lookout mcp --list-tools`.
  if ! uat_mcp_start; then
    uat_bad "mcp unconfigured server starts on loopback" "no session id after 10s" "$(cat "$UAT_MCP_LOG" 2>/dev/null)"
  else
    local bare_tools_json live_bare_n cli_bare_n
    bare_tools_json="$(uat_mcp_raw '{"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}')"
    live_bare_n="$(jq -r '.result.tools | length' <<<"$bare_tools_json" 2>/dev/null)"
    cli_bare_n="$(uat_mcp_clean --list-tools 2>/dev/null | grep -c '^k8s_' || true)"
    if [[ "$live_bare_n" == "$cli_bare_n" && "$live_bare_n" == $((${#commands[@]} - 2)) ]]; then
      uat_ok "mcp → unconfigured tools/list count matches bare --list-tools ($live_bare_n)"
    else
      uat_bad "mcp → unconfigured tools/list count matches bare --list-tools" \
        "live=$live_bare_n cli=$cli_bare_n full=${#commands[@]}"
    fi
    if jq -e '([.result.tools[].name] | (index("k8s_audit_exemptions") == null) and (index("k8s_findings_diff") == null) and (index("k8s_triage_status") != null) and (index("k8s_cluster_health") != null) and (index("k8s_findings_ack") != null))' <<<"$bare_tools_json" >/dev/null 2>&1; then
      uat_ok "mcp → unconfigured tools/list omits server-flag tools and keeps store-optional ones"
    else
      uat_bad "mcp → unconfigured tools/list omits server-flag tools and keeps store-optional ones" \
        "$(jq -c '[.result.tools[].name]' <<<"$bare_tools_json")"
    fi
    uat_mcp_stop
  fi

  uat_section "mcp: a bind flag must not open an unauthenticated cluster-read API"

  # The whole surface reads the cluster, so the refusal is the security
  # property, not a convenience. Assert it is refused as a USAGE error
  # rather than accepted-and-warned.
  uat_run_mcp --listen=0.0.0.0:0
  uat_expect_exit 2 "mcp → refuses a non-loopback bind"
  uat_expect_stderr 'loopback' "mcp → says why it refused"

  # Each of the three flags alone is still a refusal: the point is that
  # opting in does not on its own make the bind safe.
  uat_run_mcp --listen=0.0.0.0:0 --allow-non-loopback
  uat_expect_exit 2 "mcp → --allow-non-loopback alone is still refused"

  local token_file="$UAT_WORKDIR/mcp-token" access_log="$UAT_WORKDIR/mcp-access.log"
  echo "uat-secret-token" >"$token_file"
  chmod 600 "$token_file"

  uat_run_mcp --listen=0.0.0.0:0 --allow-non-loopback --auth-token-file="$token_file"
  uat_expect_exit 2 "mcp → without --access-log it is still refused"

  uat_section "mcp: the off-host bind authenticates"

  # All three flags together is the one accepted off-host configuration,
  # so this is where the 401 has to hold.
  local port bin
  port="$(uat_free_port)"
  bin="$(lookout_bin)"
  env "${UAT_MCP_CLEAN_ENV[@]}" "$bin" mcp --listen="0.0.0.0:$port" --allow-non-loopback \
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
  local full_out full_n audit_n triage_n
  full_out="$(uat_mcp_clean --list-tools 2>/dev/null)"
  full_n="$(grep -c '^k8s_' <<<"$full_out" || true)"
  audit_n="$(uat_mcp_clean --profile=audit --list-tools 2>/dev/null | grep -c '^k8s_' || true)"
  triage_n="$(uat_mcp_clean --profile=triage --list-tools 2>/dev/null | grep -c '^k8s_' || true)"

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

  # Server-scoped flag gating matrix (#867): bare --list-tools omits
  # k8s_audit_exemptions and k8s_findings_diff; each flag unlocks its
  # own tool; both together restore the full read-path registry.
  if grep -q '^k8s_audit_exemptions ' <<<"$full_out" || grep -q '^k8s_findings_diff ' <<<"$full_out"; then
    uat_bad "mcp → bare --list-tools omits k8s_audit_exemptions and k8s_findings_diff" \
      "$(grep -E '^k8s_(audit_exemptions|findings_diff) ' <<<"$full_out")"
  else
    uat_ok "mcp → bare --list-tools omits k8s_audit_exemptions and k8s_findings_diff"
  fi

  local ex_out ex_n st_out st_n both_out both_n audit_ex_n audit_st_n triage_st_out triage_st_n triage_ex_n
  ex_out="$(uat_mcp_clean --exemptions=/dev/null --list-tools 2>/dev/null)"
  ex_n="$(grep -c '^k8s_' <<<"$ex_out" || true)"
  if ((ex_n == full_n + 1)) && grep -q '^k8s_audit_exemptions ' <<<"$ex_out" && ! grep -q '^k8s_findings_diff ' <<<"$ex_out"; then
    uat_ok "mcp → --exemptions adds k8s_audit_exemptions only ($ex_n)"
  else
    uat_bad "mcp → --exemptions adds k8s_audit_exemptions only" "got=$ex_n want=$((full_n + 1))"
  fi

  st_out="$(uat_mcp_clean --store=/dev/null --list-tools 2>/dev/null)"
  st_n="$(grep -c '^k8s_' <<<"$st_out" || true)"
  if ((st_n == full_n + 1)) && grep -q '^k8s_findings_diff ' <<<"$st_out" && ! grep -q '^k8s_audit_exemptions ' <<<"$st_out"; then
    uat_ok "mcp → --store adds k8s_findings_diff only ($st_n)"
  else
    uat_bad "mcp → --store adds k8s_findings_diff only" "got=$st_n want=$((full_n + 1))"
  fi

  both_out="$(uat_mcp_clean --exemptions=/dev/null --store=/dev/null --list-tools 2>/dev/null)"
  both_n="$(grep -c '^k8s_' <<<"$both_out" || true)"
  if ((both_n == full_n + 2)) && grep -q '^k8s_audit_exemptions ' <<<"$both_out" && grep -q '^k8s_findings_diff ' <<<"$both_out"; then
    uat_ok "mcp → --exemptions + --store adds both gated tools ($both_n)"
  else
    uat_bad "mcp → --exemptions + --store adds both gated tools" "got=$both_n want=$((full_n + 2))"
  fi

  audit_ex_n="$(uat_mcp_clean --profile=audit --exemptions=/dev/null --list-tools 2>/dev/null | grep -c '^k8s_' || true)"
  audit_st_n="$(uat_mcp_clean --profile=audit --store=/dev/null --list-tools 2>/dev/null | grep -c '^k8s_' || true)"
  if ((audit_ex_n == audit_n + 1 && audit_st_n == audit_n)); then
    uat_ok "mcp → --profile=audit gains k8s_audit_exemptions with --exemptions only ($audit_n → $audit_ex_n)"
  else
    uat_bad "mcp → --profile=audit gains k8s_audit_exemptions with --exemptions only" \
      "base=$audit_n with_exemptions=$audit_ex_n with_store=$audit_st_n"
  fi

  triage_st_out="$(uat_mcp_clean --profile=triage --store=/dev/null --list-tools 2>/dev/null)"
  triage_st_n="$(grep -c '^k8s_' <<<"$triage_st_out" || true)"
  triage_ex_n="$(uat_mcp_clean --profile=triage --exemptions=/dev/null --list-tools 2>/dev/null | grep -c '^k8s_' || true)"
  if ((triage_st_n == triage_n + 1 && triage_ex_n == triage_n)) && grep -q '^k8s_findings_diff ' <<<"$triage_st_out"; then
    uat_ok "mcp → --profile=triage gains k8s_findings_diff with --store only ($triage_n → $triage_st_n)"
  else
    uat_bad "mcp → --profile=triage gains k8s_findings_diff with --store only" \
      "base=$triage_n with_store=$triage_st_n with_exemptions=$triage_ex_n"
  fi

  # --tools is documented as left-to-right over the profile, so a
  # removal has to actually remove.
  local minus_n
  minus_n="$(uat_mcp_clean --profile=triage --tools=-k8s_triage_logs --list-tools 2>/dev/null | grep -c '^k8s_' || true)"
  if ((minus_n == triage_n - 1)); then
    uat_ok "mcp → --tools=-<name> removes one from the profile ($minus_n of $triage_n)"
  else
    uat_bad "mcp → --tools=-<name> removes one from the profile" "got $minus_n, want $((triage_n - 1))"
  fi
  if uat_mcp_clean --profile=triage --tools=-k8s_triage_logs --list-tools 2>/dev/null | grep -q '^k8s_triage_logs'; then
    uat_bad "mcp → the removed tool is really gone" "k8s_triage_logs is still advertised"
  else
    uat_ok "mcp → the removed tool is really gone"
  fi

  # Removing an already-omitted server-flag tool stays a valid no-op for
  # existing configs; positively requesting one without its flag exits 2.
  uat_run_mcp --profile=audit --tools=-k8s_audit_exemptions --list-tools
  uat_expect_exit 0 "mcp → --tools=-k8s_audit_exemptions without --exemptions is a valid no-op"
  uat_run_mcp --tools=all,-k8s_findings_diff --list-tools
  uat_expect_exit 0 "mcp → --tools=all,-k8s_findings_diff without --store is a valid no-op"

  uat_run_mcp --tools=k8s_audit_exemptions --list-tools
  uat_expect_exit 2 "mcp → explicit k8s_audit_exemptions without --exemptions is a usage error"
  uat_expect_stderr '--exemptions' "mcp → missing --exemptions names the flag"

  uat_run_mcp --tools=k8s_findings_diff --list-tools
  uat_expect_exit 2 "mcp → explicit k8s_findings_diff without --store is a usage error"
  uat_expect_stderr '--store' "mcp → missing --store names the flag"

  uat_run_mcp --exemptions=/dev/null --tools=k8s_audit_exemptions --list-tools
  uat_expect_exit 0 "mcp → explicit k8s_audit_exemptions with --exemptions succeeds"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == "1" ]] && grep -q '^k8s_audit_exemptions ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --exemptions --tools=k8s_audit_exemptions advertises exactly 1 tool"
  else
    uat_bad "mcp → --exemptions --tools=k8s_audit_exemptions advertises exactly 1 tool" "$UAT_OUT"
  fi

  uat_run_mcp --store=/dev/null --tools=k8s_findings_diff --list-tools
  uat_expect_exit 0 "mcp → explicit k8s_findings_diff with --store succeeds"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == "1" ]] && grep -q '^k8s_findings_diff ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --store --tools=k8s_findings_diff advertises exactly 1 tool"
  else
    uat_bad "mcp → --store --tools=k8s_findings_diff advertises exactly 1 tool" "$UAT_OUT"
  fi

  # Privileged opt-in: --probe-from-image (digest-pinned) adds
  # k8s_net_probe_from; tag-only refs and bare --tools=k8s_net_probe_from fail.
  local probe_img="ghcr.io/go-steer/lookout@sha256:0000000000000000000000000000000000000000000000000000000000000000"
  uat_run_mcp --probe-from-image="$probe_img" --list-tools
  uat_expect_exit 0 "mcp → digest-pinned --probe-from-image --list-tools succeeds"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == $((full_n + 1)) ]] && grep -q '^k8s_net_probe_from ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --probe-from-image adds k8s_net_probe_from ($((full_n + 1)))"
  else
    uat_bad "mcp → --probe-from-image adds k8s_net_probe_from" "$UAT_OUT"
  fi
  uat_run_mcp --probe-from-image="$probe_img" --exemptions=/dev/null --store=/dev/null --list-tools
  uat_expect_exit 0 "mcp → --probe-from-image + --exemptions + --store --list-tools succeeds"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == $((both_n + 1)) ]] && grep -q '^k8s_net_probe_from ' <<<"$UAT_OUT" && grep -q '^k8s_audit_exemptions ' <<<"$UAT_OUT" && grep -q '^k8s_findings_diff ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --probe-from-image + --exemptions + --store advertises all $((both_n + 1)) tools"
  else
    uat_bad "mcp → --probe-from-image + --exemptions + --store advertises all $((both_n + 1)) tools" "$UAT_OUT"
  fi
  uat_run_mcp --profile=triage --probe-from-image="$probe_img" --list-tools
  uat_expect_exit 0 "mcp → --profile=triage --probe-from-image --list-tools succeeds"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == $((triage_n + 1)) ]] && grep -q '^k8s_net_probe_from ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --profile=triage --probe-from-image adds k8s_net_probe_from ($((triage_n + 1)))"
  else
    uat_bad "mcp → --profile=triage --probe-from-image adds k8s_net_probe_from" "$UAT_OUT"
  fi
  uat_run_mcp --probe-from-image=ghcr.io/go-steer/lookout:latest --list-tools
  uat_expect_exit 2 "mcp → tag-only --probe-from-image is a usage error"
  uat_expect_stderr '--probe-from-image' "mcp → tag-only --probe-from-image names the flag"
  uat_run_mcp --tools=k8s_net_probe_from --list-tools
  uat_expect_exit 2 "mcp → --tools=k8s_net_probe_from without --probe-from-image is a usage error"
  uat_expect_stderr 'k8s_net_probe_from' "mcp → privileged tool refusal names k8s_net_probe_from"

  # Multi-cluster --list-tools: adds k8s_list_clusters, combines cleanly
  # with --tools=k8s_audit_all (2 tools), auto-enables on injected Google
  # auth unless --kubeconfig pins single-cluster mode, and rejects invalid combinations.
  uat_run_mcp --clusters=a=https://a --list-tools
  uat_expect_exit 0 "mcp → --clusters --list-tools succeeds"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == $((full_n + 1)) ]] && grep -q '^k8s_list_clusters ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --clusters adds k8s_list_clusters ($((full_n + 1)))"
  else
    uat_bad "mcp → --clusters adds k8s_list_clusters" "$UAT_OUT"
  fi
  uat_run_mcp --clusters-from=auto --tools=k8s_audit_all --list-tools
  uat_expect_exit 0 "mcp → --clusters-from=auto --tools=k8s_audit_all --list-tools succeeds"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == "2" ]] && grep -q '^k8s_list_clusters ' <<<"$UAT_OUT" && grep -q '^k8s_audit_all ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --clusters-from=auto --tools=k8s_audit_all advertises exactly 2 tools"
  else
    uat_bad "mcp → --clusters-from=auto --tools=k8s_audit_all advertises exactly 2 tools" "$UAT_OUT"
  fi
  uat_run_mcp --google-auth=injected --list-tools
  uat_expect_exit 0 "mcp → --google-auth=injected --list-tools auto-enables multi-cluster"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == $((full_n + 1)) ]] && grep -q '^k8s_list_clusters ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --google-auth=injected adds k8s_list_clusters ($((full_n + 1)))"
  else
    uat_bad "mcp → --google-auth=injected adds k8s_list_clusters" "$UAT_OUT"
  fi
  uat_run_mcp --google-auth=injected --kubeconfig=/dev/null --list-tools
  uat_expect_exit 0 "mcp → --google-auth=injected with --kubeconfig stays single-cluster"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == "$full_n" ]] && ! grep -q '^k8s_list_clusters ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --google-auth=injected with --kubeconfig omits k8s_list_clusters ($full_n)"
  else
    uat_bad "mcp → --google-auth=injected with --kubeconfig omits k8s_list_clusters" "$UAT_OUT"
  fi
  uat_run_mcp --clusters-from=auto --store=/dev/null --list-tools
  uat_expect_exit 0 "mcp → --clusters-from=auto --store --list-tools succeeds"
  if [[ "$(grep -c '^k8s_' <<<"$UAT_OUT" || true)" == $((full_n + 2)) ]] && grep -q '^k8s_list_clusters ' <<<"$UAT_OUT" && grep -q '^k8s_findings_diff ' <<<"$UAT_OUT"; then
    uat_ok "mcp → --clusters-from=auto --store adds k8s_list_clusters and k8s_findings_diff ($((full_n + 2)))"
  else
    uat_bad "mcp → --clusters-from=auto --store adds k8s_list_clusters and k8s_findings_diff" "$UAT_OUT"
  fi
  uat_run_mcp --clusters=a=https://a --clusters-from=auto --list-tools
  uat_expect_exit 2 "mcp → --clusters and --clusters-from are mutually exclusive"
  uat_expect_stderr 'mutually exclusive' "mcp → --clusters + --clusters-from explains exclusivity"
  uat_run_mcp --clusters-from=auto --kubeconfig=/dev/null --list-tools
  uat_expect_exit 2 "mcp → --clusters-from=auto rejects --kubeconfig"
  uat_expect_stderr '--kubeconfig' "mcp → --clusters-from + --kubeconfig names --kubeconfig"
  uat_run_mcp --listen=127.0.0.1:0 --google-client-id=c --google-allow=@x.example --resource-url=https://l.example/mcp --clusters-from=auto --store=/dev/null --list-tools
  uat_expect_exit 2 "mcp → Google-token multi-cluster rejects --store"
  uat_expect_stderr '--store and --report' "mcp → Google-token multi-cluster --store names the restriction"
  uat_run_mcp --listen=127.0.0.1:0 --google-client-id=c --google-allow=@x.example --resource-url=https://l.example/mcp --clusters-from=auto --report=/dev/null --list-tools
  uat_expect_exit 2 "mcp → Google-token multi-cluster rejects --report"
  uat_expect_stderr '--store and --report' "mcp → Google-token multi-cluster --report names the restriction"

  # Unknown profile or unknown tool name is a usage error, not a silently empty surface.
  uat_run_mcp --profile=no_such_profile --list-tools
  uat_expect_exit 2 "mcp → --profile with an unknown name is a usage error"
  uat_expect_stderr 'no_such_profile' "mcp → unknown profile names the bad value"
  uat_run_mcp --tools=k8s_no_such_tool --list-tools
  uat_expect_exit 2 "mcp → --tools with an unknown name is a usage error"
  uat_expect_stderr 'k8s_no_such_tool' "mcp → unknown tool names the bad value"
}
