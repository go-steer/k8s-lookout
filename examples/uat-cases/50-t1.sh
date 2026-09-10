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

# The T1 tier — docs/testing/cli-uat.md § Environment tiers: kind PLUS
# metrics-server. `examples/kind/up` installs it, but a UAT run may be
# pointed at a cluster that has none, so the whole case is gated and
# SKIPPED (counted, named) rather than failed.
#
# `triage top` is the only command here. HPA thrash was the tier
# table's other T1 entry and turned out not to be: the detector reads
# event messages, not metrics, so it runs at T0 in 20-fixtures.sh.
#
# What T1 buys is the one command whose entire output is a judgment
# about numbers that only exist if something is sampling them. Without
# metrics-server there is no row to judge, and the shape assertions in
# 00-contract.sh already cover "it fails cleanly when there is none".

uat_case_t1() {
  if ! uat_tier_enabled T1; then
    uat_skipped "the whole T1 case (triage top)" "UAT_TIER=$UAT_TIER, needs metrics-server"
    return 0
  fi
  uat_top
}

# ---- triage top: the OOM asymmetry, on four live containers ----------------

uat_top() {
  uat_section "triage top: usage vs limits, judged (cpu-pressure)"

  if ! kubectl top pod -n kube-system --no-headers >/dev/null 2>&1; then
    uat_skipped "triage top → the whole section" "metrics.k8s.io is not answering (no metrics-server?)"
    return 0
  fi

  if ! uat_fixture cpu-pressure; then
    uat_skipped "triage top → the whole section" "fixture cpu-pressure did not inject"
    return 0
  fi

  local ns=lookout-uat-top
  local scope=(--namespace="$ns")

  uat_run triage top "${scope[@]}"
  uat_expect_exit 0 "triage top → exit 0 with saturated containers present"

  # Both hogs are WARNING, and that is the point rather than a
  # limitation. The fixture deliberately holds memhog at ~86% instead of
  # driving it into the ≥95% critical band: 95% of a small limit leaves
  # a few megabytes of headroom, so aiming there turns a UAT fixture
  # into a flaky OOM scenario, and the oom/ scenario already owns a real
  # OOM kill. See examples/scenarios/cpu-pressure/inject.
  #
  # The OOM asymmetry is still asserted here, from the two warnings
  # side by side — cpuhog at the HIGHER percentage of the two.
  uat_expect_stdout 'kind=top\.saturation.*severity=warning.*name=cpuhog.*resource=cpu' \
    "triage top → a container pinned at ~100% of its CPU limit is a WARNING"
  uat_refute_stdout 'kind=top\.saturation.*severity=critical.*name=cpuhog' \
    "triage top → CPU never reaches critical: over-limit throttles, it does not kill"
  uat_expect_stdout 'kind=top\.saturation.*severity=warning.*name=memhog.*resource=memory' \
    "triage top → memory below the critical band is a warning too"
  uat_refute_stdout 'kind=top\.saturation.*severity=critical.*name=memhog' \
    "triage top → and if it ever reports critical the fixture's ballast has drifted"

  # The messages carry the reasoning, not just the number, and this is
  # where the asymmetry is actually legible: two warnings, one of which
  # announces a critical band above it and one of which announces that
  # there is none. An operator reading "86%" learns nothing about which
  # way to act; "critical from 95%" does.
  uat_expect_stdout 'name=cpuhog.*reason=CPUNearLimit.*throttling risk only.*point-in-time ceiling' \
    "triage top → says why cpu tops out at warning however hard it is pushed"
  uat_expect_stdout 'name=memhog.*reason=MemoryNearLimit.*critical from 95%' \
    "triage top → and why the same severity on memory means something else"
  uat_expect_stdout 'name=memhog.*resource=memory usage=[0-9].*limit=[0-9].*pct=[0-9]' \
    "triage top → the memory row shows usage, limit and percent"
  uat_expect_stdout 'name=cpuhog.*resource=cpu.*node=' \
    "triage top → and where the container is running"

  # Negative controls, two kinds. memhog is nearly idle on CPU and
  # cpuhog nearly idle on memory, so neither may appear on the other
  # dimension while only above-threshold rows are emitted; and `steady`
  # is limited on both dimensions and quiet on both, so a check that
  # reports rows rather than saturation would name it.
  uat_refute_stdout 'name=memhog.*resource=cpu' \
    "triage top → an idle CPU row is not reported (zero nominal state)"
  uat_refute_stdout 'name=cpuhog.*resource=memory' \
    "triage top → nor an idle memory row"
  uat_refute_stdout 'name=steady' \
    "triage top → nor the limited-but-quiet container sitting next to both hogs"
  uat_refute_stdout 'kind=top\.saturation.*name=nolimits' \
    "triage top → and a container with no limit has no percentage to be rated on"

  uat_expect_summary_line "triage top → summary line"

  # The censuses. nolimits has neither a limit nor a request, so it is
  # invisible to every judgment above — which is exactly why it has to
  # be COUNTED. A silent census is indistinguishable from a clean one.
  uat_expect_stdout 'kind=top\.unlimited.*severity=info.*pods=1 containers=1' \
    "triage top → the container with no limits is counted, not dropped"
  uat_expect_stdout 'kind=top\.unrequested.*severity=info.*pods=1 containers=1' \
    "triage top → and counted again for the missing request (the worse half)"
  uat_refute_stdout 'kind=top\.unlimited_container' \
    "triage top → but not listed until asked"

  uat_run triage top "${scope[@]}" --show-unlimited --show-unrequested
  uat_expect_exit 0 "triage top --show-unlimited --show-unrequested → exit 0"
  uat_expect_stdout 'kind=top\.unlimited_container.*name=nolimits' \
    "triage top --show-unlimited → names the container behind the count"
  uat_expect_stdout 'kind=top\.unrequested_container.*name=nolimits' \
    "triage top --show-unrequested → and the same one for requests"
  uat_refute_stdout 'kind=top\.(unlimited|unrequested)_container.*name=(cpuhog|memhog|steady)' \
    "triage top → a container that HAS both is in neither census"

  # --top-warn moves the attention line, and only that. Lowering it
  # brings back the very container the default run refuted above:
  # `steady` is quiet, not absent, and the difference between those two
  # is what proves the threshold is a dial rather than a hardcode. The
  # censuses are not a threshold question, so they must not move with it.
  uat_run triage top "${scope[@]}" --top-warn=1
  uat_expect_exit 0 "triage top --top-warn=1 → exit 0"
  uat_expect_stdout 'kind=top\.saturation.*severity=warning.*name=steady' \
    "triage top --top-warn=1 → the control that was silent at 80 is now above the line"
  uat_expect_stdout 'kind=top\.unlimited' \
    "triage top --top-warn=1 → the census does not move with the threshold"

  uat_run triage top "${scope[@]}" --top-warn=200
  uat_expect_exit 2 "triage top --top-warn=200 → exit 2, a percent is 1..100"
  uat_expect_stderr 'top-warn' \
    "triage top --top-warn=200 → and names the flag it rejected"

  # --all is the exploratory dump: every sampled row, including the
  # ones below the line, as info WITHOUT a reason or message (zero
  # nominal state applies to fields too).
  uat_run triage top "${scope[@]}" --all
  uat_expect_exit 0 "triage top --all → exit 0"
  uat_expect_stdout 'kind=top\.saturation.*severity=info.*name=memhog.*resource=cpu' \
    "triage top --all → the idle rows appear, as info"
  # name → container with nothing in between: reason and message sit
  # between them in the envelope, so their absence is the assertion.
  uat_expect_stdout 'severity=info.*name=memhog container=[a-z]+ resource=cpu' \
    "triage top --all → a below-threshold row goes straight from the name to the numbers, no verdict"

  uat_run triage top "${scope[@]}" --all --limit=1
  uat_expect_exit 0 "triage top --all --limit=1 → exit 0"
  if [[ "$(grep -c 'kind=top\.saturation' <<<"$UAT_OUT")" == 1 ]]; then
    uat_ok "triage top --all --limit=1 → the dump is capped at one row"
  else
    uat_bad "triage top --all --limit=1 → the dump is capped at one row" "$UAT_OUT"
  fi

  # -A adds the node dimension: usage vs allocatable, the
  # node-pressure precursor. On a kind node the percentages are
  # whatever they are, so assert the row exists and is well-formed
  # rather than pinning a number the runner's load decides.
  uat_run triage top -A --all
  uat_expect_exit 0 "triage top -A --all → exit 0"
  uat_expect_stdout 'kind=top\.node.*kind_of_object=Node.*resource=cpu.*allocatable=' \
    "triage top -A → node cpu is measured against allocatable, not against a limit"
  uat_expect_stdout 'kind=top\.node.*resource=memory.*allocatable=' \
    "triage top -A → and node memory too"

  # --history is T2: it needs a cloud metrics provider. On kind the
  # contract is not silence and not an error — it is an explicit
  # unavailable finding plus a summary marker, with the point-in-time
  # output untouched.
  uat_run triage top "${scope[@]}" --history=1h
  uat_expect_exit 0 "triage top --history=1h → exit 0 without a provider"
  uat_expect_stdout 'kind=cloud\.unavailable.*capability=metrics' \
    "triage top --history → says the capability it could not get (T2)"
  uat_expect_stdout 'unavailable=' \
    "triage top --history → and marks the summary line, so a reader cannot miss it"
  uat_expect_stdout 'kind=top\.saturation.*name=memhog' \
    "triage top --history → the point-in-time findings are unaffected"
  uat_refute_stdout 'max_pct=|p95_pct=' \
    "triage top --history → and no window stats are invented"
}
