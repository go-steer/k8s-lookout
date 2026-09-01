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

# UAT case: root / infra — docs/testing/cli-uat.md Part 1 § Root / infra
# (issue #174).
#
# Everything here is about the binary itself rather than about any one
# read-path check: what `lookout` does when it is given nothing, a
# typo, or a command; what `version` answers when the cluster does not;
# and the four claims `watch` makes at startup — the source probe, the
# liveness/readiness split, the metrics listener, and --dry-run's
# payloads on stdout with its diagnostics on stderr.
#
# The watch cases run the real pipeline against the real cluster. They
# never need a daemon: --dry-run is the whole point, and a sentinel
# deployment (examples/sentinel) is explicitly NOT a prerequisite.

UAT_ROOT_NS=lookout-uat-broken

# ---- version --------------------------------------------------------------

uat_root_version() {
  uat_section "version: the one string that ties a running binary to a tree"

  uat_run version
  uat_expect_exit 0
  uat_expect_stdout '^lookout v[0-9]+\.[0-9]+\.[0-9]+' \
    "version → a semver, prefixed with the binary name"

  # Not decoration: an operator holding a misbehaving pod needs the
  # commit to check out and the build time to tell a stale image from a
  # fresh one. A bare "v0.24.0" cannot distinguish two builds of the
  # same tag.
  uat_expect_stdout 'commit [0-9a-f]{7}' \
    "version → names the commit it was built from"
  uat_expect_stdout 'built [0-9]{4}-[0-9]{2}-[0-9]{2}T' \
    "version → names the build timestamp"

  local canonical="$UAT_OUT"
  if [[ "$(wc -l <<<"$UAT_OUT")" == 1 ]]; then
    uat_ok "version → exactly one line"
  else
    uat_bad "version → exactly one line" "got $(wc -l <<<"$UAT_OUT") lines"
  fi

  # #146: operators and scripts reach for --version on any binary, and
  # the two spellings have to be the same answer, not merely similar.
  local spelling
  for spelling in --version -version; do
    uat_run "$spelling"
    uat_expect_exit 0
    if [[ "$UAT_OUT" == "$canonical" ]]; then
      uat_ok "lookout $spelling → byte-identical to the version subcommand"
    else
      uat_bad "lookout $spelling → byte-identical to the version subcommand" \
        "subcommand: $canonical" "flag:       $UAT_OUT"
    fi
  done

  # The question `version` answers most often is asked while the
  # cluster is unreachable — during an outage, or from a laptop with no
  # kubeconfig at all. A version command that needs an apiserver is
  # useless exactly when it is wanted.
  local saved="${KUBECONFIG-}" had_kubeconfig=0
  [[ -n "${KUBECONFIG+x}" ]] && had_kubeconfig=1
  export KUBECONFIG="$UAT_WORKDIR/no-such-kubeconfig"
  uat_run version
  if ((had_kubeconfig)); then export KUBECONFIG="$saved"; else unset KUBECONFIG; fi
  uat_expect_exit 0 "version with no reachable cluster → exit 0"
  if [[ "$UAT_OUT" == "$canonical" ]]; then
    uat_ok "version with no reachable cluster → the same answer"
  else
    uat_bad "version with no reachable cluster → the same answer" \
      "with cluster:    $canonical" "without cluster: $UAT_OUT"
  fi
}

# ---- the multicall root ---------------------------------------------------

uat_root_dispatch() {
  uat_section "the root: discovery, and what a typo costs"

  # No arguments is a usage error, and the usage text goes to stderr so
  # that a caller which captured stdout gets an EMPTY capture rather
  # than a command list it will try to parse as findings.
  uat_run
  uat_expect_exit 2 "lookout with no arguments → exit 2"
  if [[ -z "$UAT_OUT" ]]; then
    uat_ok "lookout with no arguments → stdout is empty"
  else
    uat_bad "lookout with no arguments → stdout is empty" "stdout: $UAT_OUT"
  fi
  uat_expect_stderr '^Usage: lookout <command>' \
    "lookout with no arguments → usage on stderr"

  # ... and asking for help is not an error, so the same text goes the
  # other way: `lookout --help | grep` is how the surface is discovered.
  local spelling
  for spelling in --help -h help; do
    uat_run "$spelling"
    uat_expect_exit 0 "lookout $spelling → exit 0"
    uat_expect_stdout '^Usage: lookout <command>' \
      "lookout $spelling → usage on stdout"
  done

  uat_run frobnicate
  uat_expect_exit 2 "an unknown command → exit 2"
  uat_expect_stderr 'unknown command "frobnicate"' \
    "an unknown command → says which word it did not know"
  uat_expect_stderr '^Usage: lookout <command>' \
    "an unknown command → and what the known ones are"

  # Enumerated, not listed by hand (like uat-cases/00-contract.sh): a
  # group that exists in the registry but never reaches the root's
  # usage text is undiscoverable, and nothing else in the suite would
  # notice. The read-path groups come from `mcp --list-tools`; watch
  # and version are the two commands that are not read-path checks.
  uat_run --help
  local help="$UAT_OUT"
  local -a groups=()
  mapfile -t groups < <(uat_read_commands | awk '{print $1}' | sort -u)
  groups+=(watch version)
  local group missing=()
  for group in "${groups[@]}"; do
    grep -Eq "^  $group +" <<<"$help" || missing+=("$group")
  done
  if ((${#groups[@]} > 5)) && ((${#missing[@]} == 0)); then
    uat_ok "lookout --help → lists all ${#groups[@]} command groups the registry serves"
  else
    uat_bad "lookout --help → lists all command groups the registry serves" \
      "groups read from the registry: ${#groups[@]}" \
      "missing from usage: ${missing[*]:-<none>}"
  fi
}

# ---- a live watch process --------------------------------------------------

UAT_WATCH_PID=""
UAT_WATCH_OUT=""
UAT_WATCH_ERR=""
UAT_WATCH_URL=""

# uat_watch_start <extra flags...> — start `lookout watch` in the
# background with its two streams captured SEPARATELY (the --dry-run
# contract is a claim about which stream a line went to) and a metrics
# listener on a free loopback port. Returns immediately; the readiness
# assertions below are what wait.
uat_watch_start() {
  local port
  port="$(uat_free_port)"
  UAT_WATCH_URL="http://127.0.0.1:$port"
  UAT_WATCH_OUT="$UAT_WORKDIR/watch-$port.out"
  UAT_WATCH_ERR="$UAT_WORKDIR/watch-$port.err"
  "$(lookout_bin)" watch --metrics-addr="127.0.0.1:$port" "$@" \
    >"$UAT_WATCH_OUT" 2>"$UAT_WATCH_ERR" &
  UAT_WATCH_PID=$!
}

uat_watch_stop() {
  [[ -n "$UAT_WATCH_PID" ]] || return 0
  kill "$UAT_WATCH_PID" 2>/dev/null || true
  wait "$UAT_WATCH_PID" 2>/dev/null || true
  UAT_WATCH_PID=""
}

# uat_watch_probe <path> — "<http-status> <body>" for one GET, or
# "000 " if the listener is not answering yet.
uat_watch_probe() {
  local code body
  body="$(curl -sS -m 5 -o - -w '\n%{http_code}' "$UAT_WATCH_URL$1" 2>/dev/null)" || {
    echo "000 "
    return 0
  }
  code="$(tail -n1 <<<"$body")"
  body="$(head -n -1 <<<"$body")"
  printf '%s %s\n' "$code" "$body"
}

# uat_watch_await <seconds> <shell-test...> — poll every 2s.
uat_watch_await() {
  local timeout=$(($1 * ${LOOKOUT_E2E_TIMEOUT_SCALE:-1}))
  shift
  local waited=0
  while true; do
    "$@" && return 0
    ((waited >= timeout)) && return 1
    sleep 2
    waited=$((waited + 2))
  done
}

uat_root_watch_runtime() {
  uat_section "watch: the source probe, the readiness gate, and --dry-run"

  # The dry-run payload assertions need something in the cluster that
  # is actually broken, and scoping the watch to one fixture namespace
  # is what makes "these are the payloads" a closed claim rather than
  # "these are the payloads plus whatever else is wrong today".
  if ! uat_fixture broken-workloads; then
    uat_bad "watch --dry-run → fixture" "broken-workloads did not inject; skipping the watch cases"
    return 0
  fi

  # --storm=off is not a convenience: with correlation on, three
  # incidents sharing a blast-radius key inside the window become ONE
  # kind=storm session and the members are suppressed, so which
  # payloads reach stdout depends on how fast the fixture's signals
  # arrive relative to each other. Storm formation has its own
  # coverage; this case is about the payload itself, so it asks for the
  # per-incident shape explicitly rather than hoping for it.
  uat_watch_start --dry-run --storm=off --namespace="$UAT_ROOT_NS"

  # --- the readiness gate ---------------------------------------------------
  #
  # /healthz and /readyz are different questions and the kubelet uses
  # them differently: a sentinel whose informers are still listing is
  # ALIVE (do not restart it) but not READY (do not count it as
  # watching yet). Collapsing them costs a restart loop on a big
  # cluster, so the first thing this asserts is that they disagree.
  local first_ready="" first_health="" i
  for i in $(seq 1 300); do
    first_ready="$(uat_watch_probe /readyz)"
    [[ "$first_ready" == "000 "* ]] || break
    kill -0 "$UAT_WATCH_PID" 2>/dev/null || break
    sleep 0.2
  done
  first_health="$(uat_watch_probe /healthz)"

  if [[ "$first_ready" == 503* ]]; then
    uat_ok "/readyz on the first answer → 503, the gate is real"
    uat_expect_text "$first_ready" '(syncing|not started)' \
      "/readyz 503 → the body names what it is waiting on"
  elif [[ "$first_ready" == 200* ]]; then
    # Not a failure of the binary: on a small cluster the caches can
    # sync before the first poll lands. It IS a failure of the check,
    # so say so rather than passing.
    uat_skipped "/readyz red-then-green" \
      "the listener was already ready on the first poll — nothing was gated (docs/testing/cli-uat.md § /readyz)"
  else
    uat_bad "/readyz on the first answer → 503, the gate is real" \
      "got: ${first_ready:-<no answer>}" "$(tail -n 5 "$UAT_WATCH_ERR" | tr '\n' '|')"
  fi

  if [[ "$first_health" == "200 ok" ]]; then
    uat_ok "/healthz while /readyz is still red → 200 ok (alive, not ready)"
  else
    uat_bad "/healthz while /readyz is still red → 200 ok" "got: ${first_health:-<no answer>}"
  fi

  if uat_watch_await 180 bash -c "[[ \"\$(curl -sS -m 5 -o /dev/null -w '%{http_code}' $UAT_WATCH_URL/readyz 2>/dev/null)\" == 200 ]]"; then
    uat_ok "/readyz → turns green once the informers have synced"
  else
    uat_bad "/readyz → turns green once the informers have synced" \
      "still ${first_ready%% *} after 180s" "$(tail -n 5 "$UAT_WATCH_ERR" | tr '\n' '|')"
  fi

  local health_after path_404
  health_after="$(uat_watch_probe /healthz)"
  [[ "$health_after" == "200 ok" ]] &&
    uat_ok "/healthz after ready → still 200 ok" ||
    uat_bad "/healthz after ready → still 200 ok" "got: $health_after"

  path_404="$(uat_watch_probe /not-an-endpoint)"
  [[ "$path_404" == 404* ]] &&
    uat_ok "an unknown path on the metrics listener → 404" ||
    uat_bad "an unknown path on the metrics listener → 404" "got: $path_404"

  # --- the metrics listener -------------------------------------------------
  #
  # Same port as the health endpoints on purpose: one container port to
  # expose, one Service, one scrape config.
  #
  # Asserted on a GAUGE, deliberately: a labelled counter has no
  # exposition at all until its first observation, so
  # lookout_events_seen_total is legitimately absent from the scrape of
  # a sentinel that has not seen an event yet, and a check written
  # against one would pass or fail on cluster noise.
  local metrics
  metrics="$(curl -sS -m 10 "$UAT_WATCH_URL/metrics" 2>/dev/null || true)"
  if grep -q '^# HELP lookout_active_incidents ' <<<"$metrics" &&
    grep -q '^# TYPE lookout_active_incidents gauge' <<<"$metrics"; then
    uat_ok "/metrics → Prometheus exposition, with lookout's own counters"
  else
    uat_bad "/metrics → Prometheus exposition, with lookout's own counters" \
      "first lines: $(head -n 3 <<<"$metrics" | tr '\n' '|')"
  fi
  local families
  families="$(grep -c '^# HELP lookout_' <<<"$metrics" || true)"
  if ((families >= 10)); then
    uat_ok "/metrics → the whole sentinel metric set is registered ($families families)"
  else
    uat_bad "/metrics → the whole sentinel metric set is registered" \
      "only $families lookout_ metric families exposed"
  fi

  # --- the --sources=auto probe ---------------------------------------------
  #
  # The Autopilot-safety contract (§11): a source whose needs this
  # deployment cannot meet is SKIPPED LOUDLY, and the sentinel keeps
  # running. Enumerated rather than spot-checked, because the failure
  # this guards against is a source that silently says nothing at all —
  # neither enabled nor disabled — and is therefore off with no trace.
  local err
  err="$(cat "$UAT_WATCH_ERR")"
  uat_expect_text "$err" 'sources: auto — probing the portable set' \
    "watch --sources=auto → announces the probe"

  local portable=(k8s-events object-state rollout workload autoscaling
    saturation degradation expiry capacity ingress gateway)
  local src silent=()
  for src in "${portable[@]}"; do
    grep -Eq "^.*source $src: (enabled|disabled)" <<<"$err" || silent+=("$src")
  done
  if ((${#silent[@]} == 0)); then
    uat_ok "watch --sources=auto → every portable source reports enabled or disabled (${#portable[@]}/${#portable[@]})"
  else
    uat_bad "watch --sources=auto → every portable source reports enabled or disabled" \
      "no line at all for: ${silent[*]}"
  fi

  # The kind cluster serves no Gateway API CRDs, so `gateway` is the
  # miss this tier can rely on. A loud skip has to say three things:
  # which source, why, and how to make it fatal instead.
  if grep -q 'source gateway: disabled' <<<"$err"; then
    uat_expect_text "$err" 'source gateway: disabled .*Gateway API CRDs not served' \
      "a source whose capability is missing → skipped with the reason"
    uat_expect_text "$err" 'source gateway: disabled .*--sources.*fatal' \
      "a loud skip → names the flag that turns it into a failure"
  else
    uat_skipped "the loud-skip path" \
      "this cluster serves the Gateway API, so no portable source missed its probe"
  fi

  uat_expect_text "$err" 'sources: auto resolved → .*k8s-events' \
    "watch --sources=auto → prints the resolved set, not just the per-source lines"
  local resolved
  resolved="$(grep -o 'auto resolved → [^ ]*' <<<"$err" | head -n1)"
  if [[ -n "$resolved" ]] && ! grep -Eq '(^|,)(quota|notifications|token-burn)(,|$)' <<<"${resolved#auto resolved → }"; then
    uat_ok "watch --sources=auto → the explicit-only sources stay off (quota, notifications, token-burn)"
  else
    uat_bad "watch --sources=auto → the explicit-only sources stay off" "resolved: ${resolved:-<not printed>}"
  fi

  if kill -0 "$UAT_WATCH_PID" 2>/dev/null; then
    uat_ok "a missed source probe does not stop the sentinel (§11 loud skip, not crash)"
  else
    uat_bad "a missed source probe does not stop the sentinel" \
      "the process is gone; last stderr: $(tail -n 5 "$UAT_WATCH_ERR" | tr '\n' '|')"
  fi

  # --- the dry-run payloads -------------------------------------------------
  #
  # Started with no --daemon-url at all, so nothing here can be an
  # artefact of a reachable sink: if a payload appears on stdout, the
  # whole pipeline — informers, sources, filter, dedup, routing,
  # enrichment — ran for real and only the POST was replaced.
  #
  # The wait is for a payload FROM THE FIXTURE, not for any payload at
  # all. --namespace scopes namespaced signals, and cannot scope what
  # has no namespace to scope: a cluster-scoped signal (an expiring
  # webhook CA bundle, say) is in scope by construction and routinely
  # lands first. Waiting on it and then reading the set 5s later closes
  # the window before the crash loop's own inject is even routed.
  if ! uat_watch_await 180 grep -q "\"namespace\": \"$UAT_ROOT_NS\"" "$UAT_WATCH_OUT"; then
    uat_bad "watch --dry-run → prints an inject payload" \
      "nothing from $UAT_ROOT_NS on stdout after 180s with a crash-looping and an unschedulable workload there" \
      "$(tail -n 5 "$UAT_WATCH_ERR" | tr '\n' '|')"
    uat_watch_stop
    return 0
  fi
  uat_ok "watch --dry-run → prints an inject payload, with no --daemon-url given"
  uat_expect_text "$err" 'dry-run: watching cluster .*no daemon/sink calls' \
    "watch --dry-run → says on stderr that no sink will be called"

  # Give the second payload a moment: the assertions below are about
  # the SET of payloads (which workloads are in it, which are not), and
  # a set read the instant the first one lands is not yet a set.
  sleep 5
  uat_watch_stop

  local out payloads
  out="$(cat "$UAT_WATCH_OUT")"
  uat_expect_text "$out" '^--- dry-run payload for session ' \
    "watch --dry-run → each payload carries a header naming its session"

  # Stream separation, the reason the two are captured apart: stdout is
  # the payload stream a pipeline consumes, stderr is the operator log.
  # Every stdout line is either a payload header or part of the JSON.
  payloads="$(grep -v '^--- dry-run payload' <<<"$out")"
  if jq -e -s 'length >= 1 and (.[0] | type == "object")' >/dev/null 2>&1 <<<"$payloads"; then
    uat_ok "watch --dry-run → stdout is payload headers and JSON objects, nothing else"
  else
    uat_bad "watch --dry-run → stdout is payload headers and JSON objects, nothing else" \
      "jq could not slurp stdout: $(head -n 3 <<<"$payloads" | tr '\n' '|')"
  fi
  if grep -q 'lookout watch: starting on cluster' <<<"$out"; then
    uat_bad "watch --dry-run → the startup log stays off stdout" \
      "a diagnostic line reached the payload stream"
  else
    uat_ok "watch --dry-run → the startup log stays off stdout"
  fi

  # What is IN the payload set, read as JSON rather than by grepping
  # the pretty-printed text.
  # Namespaced payloads only: a digest or a cluster-scoped signal
  # carries an empty namespace, and a namespace filter has nothing to
  # match it against — such a signal is in scope no matter what
  # --namespace says, so it is not evidence either way for the scope
  # claim below.
  local subjects
  subjects="$(jq -s -r '.[] | select((.namespace // "") != "") | "\(.namespace)/\(.name) \(.kind) \(.reason)"' <<<"$payloads" 2>/dev/null)"
  if grep -Eq "^$UAT_ROOT_NS/faulty-.* k8s-event BackOff$" <<<"$subjects"; then
    uat_ok "watch --dry-run → the crash loop is injected as a k8s-event/BackOff payload"
  else
    uat_bad "watch --dry-run → the crash loop is injected as a k8s-event/BackOff payload" \
      "payload subjects: $(tr '\n' '|' <<<"$subjects")"
  fi

  # --namespace is a scope, and the negative control for a scope is an
  # object of the same shape just outside it — plus, inside it, the
  # healthy workload the fixture put there for exactly this purpose.
  local stray
  stray="$(grep -Ev "^$UAT_ROOT_NS/" <<<"$subjects" | grep -v '^$' || true)"
  if [[ -z "$stray" ]]; then
    uat_ok "watch --namespace → nothing outside the scoped namespace is injected"
  else
    uat_bad "watch --namespace → nothing outside the scoped namespace is injected" \
      "out-of-scope payloads: $(tr '\n' '|' <<<"$stray")"
  fi
  if grep -q "^$UAT_ROOT_NS/steady-" <<<"$subjects"; then
    uat_bad "watch --dry-run → the healthy workload beside them is not injected" \
      "steady produced a payload"
  else
    uat_ok "watch --dry-run → the healthy workload beside them is not injected"
  fi

  # §7.6: a critical inject carries its enrichment bundle, resolved to
  # the OWNER of the pod the event named — which is the difference
  # between an incident session that opens with a diagnosis and one
  # that opens with a pod name.
  local bundle
  bundle="$(jq -s -r '[.[] | select(.reason == "BackOff")][0].enrichment.bundle // ""' <<<"$payloads" 2>/dev/null)"
  if grep -Eq 'kind=bundle\.target .*name=faulty workload=Deployment/' <<<"$bundle"; then
    uat_ok "watch --dry-run → the inject carries a §7.6 bundle, resolved to the owning Deployment"
  else
    uat_bad "watch --dry-run → the inject carries a §7.6 bundle, resolved to the owning Deployment" \
      "bundle head: ${bundle:0:200}"
  fi
  # The claim is that the delta section RODE ALONG, not what it says —
  # the section contents are `bundle`'s own case (uat-cases/40-toplevel.sh).
  # Deliberately not asserted on a specific finding line: the §15 cap
  # (--enrich-cap, 4096B) truncates at section boundaries
  # least-signal-first, so which individual line survives depends on how
  # much else the workload has to say.
  if grep -Eq 'section=delta' <<<"$bundle"; then
    uat_ok "watch --dry-run → the attached bundle carries its delta section"
  else
    uat_bad "watch --dry-run → the attached bundle carries its delta section" \
      "bundle: ${bundle:0:300}"
  fi
}

# ---- refusals --------------------------------------------------------------

uat_root_watch_refusals() {
  uat_section "watch: what it refuses to start without"

  # NOTE ON EXIT CODES. The read path follows §4.2 — 2 for a usage
  # error — but `watch` deliberately keeps the standalone sentinel's
  # 0/1 convention (internal/watch/main.go:33): it is a daemon, and its
  # supervisor only ever asks whether it came up. So these assert exit
  # 1 and spend their weight on the DIAGNOSIS instead, which is what
  # actually gets someone unstuck.
  local rc out err
  _watch_fails() { # <description-suffix> <args...>
    local desc="$1"
    shift
    out="$("$(lookout_bin)" watch "$@" 2>"$UAT_WORKDIR/refuse.err")" && rc=0 || rc=$?
    err="$(cat "$UAT_WORKDIR/refuse.err")"
    if [[ "$rc" == 1 ]]; then
      uat_ok "$desc → exit 1"
    else
      uat_bad "$desc → exit 1" "got exit $rc" "stderr: ${err:0:200}"
    fi
    if [[ -z "$out" ]]; then
      uat_ok "$desc → stdout stays clean"
    else
      uat_bad "$desc → stdout stays clean" "stdout: ${out:0:200}"
    fi
  }

  _watch_fails "watch with no --daemon-url and no --dry-run"
  uat_expect_text "$err" '\-\-daemon-url is required \(unless --dry-run\)' \
    "watch without a sink → names the flag, and the one alternative"

  _watch_fails "watch --sources=bogus" --dry-run --sources=bogus
  uat_expect_text "$err" 'unknown source "bogus"' \
    "an unknown source → quotes the word it did not know"
  uat_expect_text "$err" 'known: k8s-events, object-state.*or auto' \
    "an unknown source → lists the known ones, so the fix needs no docs"

  _watch_fails "watch with an undefined flag" --dry-run --no-such-flag
  uat_expect_text "$err" 'flag provided but not defined: -no-such-flag' \
    "an undefined flag → names it"

  # --- the strict-§11 posture ----------------------------------------------
  #
  # An EXPLICIT --sources list means "I know what this deployment
  # supports": a named source whose capability is missing is fatal,
  # where the same miss under `auto` is a loud skip. gateway is the
  # reliable miss on kind (no Gateway API CRDs).
  uat_watch_start --dry-run --namespace="$UAT_ROOT_NS" --sources=k8s-events,gateway
  if uat_watch_await 90 bash -c "! kill -0 $UAT_WATCH_PID 2>/dev/null"; then
    wait "$UAT_WATCH_PID" 2>/dev/null && rc=0 || rc=$?
    UAT_WATCH_PID=""
    if ((rc != 0)); then
      uat_ok "an explicitly named source that cannot run → the process exits non-zero"
    else
      uat_bad "an explicitly named source that cannot run → the process exits non-zero" "got exit 0"
    fi
  else
    # This is the #364 shape: RunAll returns the error, but the
    # watchboard join waits on a context nothing cancelled, so the
    # process wedges with its diagnosis unprinted and only speaks on
    # SIGTERM. Fixed — the board is stopped by the join itself — so a
    # process still running here is that defect returning, not an
    # environment quirk.
    uat_bad "an explicitly named source that cannot run → the process exits non-zero" \
      "still running after 90s — the fatal-source path is wedged on the watchboard join again (#364)"
    uat_watch_stop
  fi

  err="$(cat "$UAT_WATCH_ERR")"
  uat_expect_text "$err" 'source "gateway"' \
    "a fatal source → the diagnosis names the source"
  uat_expect_text "$err" 'Gateway API not found' \
    "a fatal source → and the capability that is missing"
  uat_expect_text "$err" 'drop "gateway" from --sources' \
    "a fatal source → and how to proceed without it"
}

# ---- one assertion over a captured string ---------------------------------
#
# The uat_expect_* helpers in uat-lib.sh assert against the last
# uat_run. The watch cases run a long-lived process instead, so they
# need the same claim made about a string they captured themselves;
# this keeps the reporting identical rather than open-coding grep.

uat_expect_text() { # <text> <extended-regexp> <description>
  if grep -Eq "$2" <<<"$1"; then
    uat_ok "$3"
  else
    uat_bad "$3" "no line matched /$2/" "$(tail -n 5 <<<"$1" | tr '\n' '|')"
  fi
}

uat_case_root() {
  uat_root_version
  uat_root_dispatch
  uat_root_watch_runtime
  uat_root_watch_refusals
}
