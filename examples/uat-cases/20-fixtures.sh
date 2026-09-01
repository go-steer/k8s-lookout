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

# Part 4 of docs/testing/cli-uat.md — the commands that have nothing to
# say about a healthy cluster.
#
# 00-contract.sh proves every command is well-SHAPED. It cannot prove
# any of them is RIGHT: on a stock kind cluster `state webhooks`,
# `stab drift`, `state edges` and `stab drain` all return a perfectly
# well-formed `findings=0`, and a check that only ever sees findings=0
# is not testing the check — it is testing that the binary starts.
#
# So each section here injects a fixture from examples/scenarios/,
# asserts the specific findings that fixture is built to produce, and —
# just as important — asserts that the healthy objects sitting next to
# them stay quiet. A detector that fires on everything and a detector
# that fires correctly are indistinguishable without a negative control.
#
# The driver reverts every injected fixture from its EXIT trap, so a
# failure here cannot leave a dead admission webhook behind.
#
# Everything in this file is T0: the fixtures build what they need.

# ---- triage logs: a fixed corpus to distill --------------------------------

uat_fixture_logs() {
  uat_section "triage logs: distilling a known corpus (chatty-logs)"

  if ! uat_fixture chatty-logs; then
    uat_skipped "triage logs → the whole section" "fixture chatty-logs did not inject"
    return 0
  fi

  local pod=(--pod=chatty --namespace=lookout-uat-logs)

  uat_run triage logs "${pod[@]}"
  uat_expect_exit 0 "triage logs → exit 0 on the corpus"
  # The counts are exact because the pod prints its corpus once and
  # then sleeps. A pod still printing would answer differently every
  # second and none of these numbers could be asserted at all.
  uat_expect_stdout 'kind=log\.template.*count=600' \
    "triage logs → the 600 request lines are ONE template with count=600"
  uat_expect_stdout 'kind=log\.template.*count=20' \
    "triage logs → the 20 error lines are a second template"
  uat_expect_stdout '<\*>' \
    "triage logs → variable positions are masked to <*>"
  uat_expect_stdout 'kind=log\.stacktrace.*lang=python' \
    "triage logs → the traceback is collapsed to a stacktrace finding"
  uat_expect_stdout 'kind=log\.stacktrace.*frames=' \
    "triage logs → the stacktrace names its top frames"

  # Probe noise: stripped by default, and REPORTED as stripped. Silently
  # dropping lines would be the same output with none of the honesty.
  uat_expect_stdout 'kind=log\.probe_noise.*count=60' \
    "triage logs → 60 probe lines stripped and counted"
  uat_refute_stdout 'kube-probe/' \
    "triage logs → no probe line survives into a template"

  uat_run triage logs "${pod[@]}" --keep-probes
  uat_expect_exit 0 "triage logs --keep-probes → exit 0"
  uat_expect_stdout 'kube-probe' \
    "triage logs --keep-probes → the probe lines become their own template"
  uat_refute_stdout 'kind=log\.probe_noise' \
    "triage logs --keep-probes → nothing was stripped, so nothing is reported as stripped"

  # The cap has to name what it dropped. A truncated list that does not
  # say it is truncated is worse than no list.
  uat_run triage logs "${pod[@]}" --max-templates=1
  uat_expect_exit 0 "triage logs --max-templates=1 → exit 0"
  uat_expect_stdout 'kind=log\.overflow.*omitted_templates=[1-9]' \
    "triage logs --max-templates=1 → the dropped clusters are counted, not hidden"
  uat_expect_stdout 'kind=log\.overflow.*omitted_lines=[1-9]' \
    "triage logs --max-templates=1 → and so are the lines behind them"

  # A container that never restarted has no previous instance. The
  # correct answer is a runtime error naming that, not an empty success
  # that reads as "it said nothing before it died".
  uat_run triage logs "${pod[@]}" --previous
  uat_expect_exit 1 "triage logs --previous → exit 1 when there is no previous instance"
  uat_expect_stderr 'previous|not found' \
    "triage logs --previous → says why rather than returning empty"
}

# ---- state edges: five broken references -----------------------------------

uat_fixture_edges() {
  uat_section "state edges: naming each broken reference (broken-edges)"

  if ! uat_fixture broken-edges; then
    uat_skipped "state edges → the whole section" "fixture broken-edges did not inject"
    return 0
  fi

  uat_run state edges --workload="Deployment/lookout-uat-edges/edgy"
  uat_expect_exit 0 "state edges → exit 0 on a workload with broken references"

  # Specificity is the contract. "Something is wrong with this
  # workload" is what the pod's own status already says; the value here
  # is the NAME of the thing that is missing.
  uat_expect_stdout 'kind=edge\.missing_key.*absent-key' \
    "state edges → names the missing ConfigMap KEY, not just the ConfigMap"
  uat_expect_stdout 'kind=edge\.missing_key.*name=edgy-config' \
    "state edges → and the ConfigMap that has the other keys"
  uat_expect_stdout 'kind=edge\.missing_ref.*name=edgy-absent-secret' \
    "state edges → names the missing Secret"

  # #357: the sanitizer used to read the interior hyphen of
  # `edgy-absent-secret` as a credential flag and redact the next word,
  # turning "not found" into "[REDACTED] found". Nothing about this
  # message should be masked — there is no secret VALUE in it.
  uat_expect_stdout 'edgy-absent-secret not found' \
    "state edges → an object name ending in -secret is prose, not a flag (#357)"

  uat_run state edges --workload="Service/lookout-uat-edges/edgy-ghost"
  uat_expect_exit 0 "state edges → exit 0 on a dangling Service"
  uat_expect_stdout 'kind=edge\.selector_empty' \
    "state edges → a selector matching no pod is reported"
  uat_expect_stdout 'likely_workload=Deployment/lookout-uat-edges/edgy' \
    "state edges → and guesses the workload that was meant"

  # The healthy Service. It is backed by edgy-ok, which is genuinely
  # Ready and genuinely in the Endpoints, so none of the selector or
  # endpoint findings may appear for it. This is the negative control
  # for the whole section.
  uat_run state edges --workload="Service/lookout-uat-edges/edgy-web"
  uat_expect_exit 0 "state edges → exit 0 on a healthy Service"
  uat_refute_stdout 'kind=edge\.selector_empty' \
    "state edges → a Service whose selector matches is NOT reported"
  uat_refute_stdout 'kind=edge\.(selector_unready|endpoints_unready)' \
    "state edges → a Service with ready endpoints is NOT reported"

  # The certificate. 10 days out: inside the 720h default, outside a 1h
  # window. Both directions, because a finding that is simply always on
  # would pass the first assertion alone.
  #
  # Against edgy-ok, not edgy: the secret is reached through the routing
  # chain (ingress edgy-ing → service edgy-web → selector), and edgy-web
  # selects edgy-ok. A TLS secret nothing routes to is not an edge of
  # the workload under test, and `state edges` is right not to look at
  # it.
  uat_run state edges --workload="Deployment/lookout-uat-edges/edgy-ok" --cert-warn=336h
  uat_expect_stdout 'kind=edge\.cert_expiring.*name=edgy-tls.*days_left=[0-9]+' \
    "state edges --cert-warn=336h → the expiring certificate is reported"
  uat_expect_stdout 'kind=edge\.cert_expiring.*via=ingress ingress=edgy-ing' \
    "state edges --cert-warn=336h → and says HOW the workload reaches it"
  uat_refute_stdout 'BEGIN CERTIFICATE|tls\.key' \
    "state edges → the certificate bytes never enter a finding"
  uat_run state edges --workload="Deployment/lookout-uat-edges/edgy-ok" --cert-warn=1h
  uat_expect_exit 0 "state edges --cert-warn=1h → exit 0"
  uat_refute_stdout 'kind=edge\.cert_expiring' \
    "state edges --cert-warn=1h → and NOT reported outside the window"
}

# ---- state webhooks: the same dead backend, two consequences ---------------

uat_fixture_webhooks() {
  uat_section "state webhooks: failurePolicy is the whole story (broken-webhook)"

  if ! uat_fixture broken-webhook; then
    uat_skipped "state webhooks → the whole section" "fixture broken-webhook did not inject"
    return 0
  fi

  uat_run state webhooks --cert-warn=336h
  uat_expect_exit 0 "state webhooks → exit 0 with broken webhooks present"

  # Two webhooks, identical dead backend, one field apart. That one
  # field is the difference between "every gated write is rejected" and
  # "the policy you think is enforced is not running", so they must not
  # collapse into one finding kind.
  uat_expect_stdout 'kind=webhook\.failing_closed.*severity=critical.*name=lookout-uat-failclosed' \
    "state webhooks → failurePolicy=Fail + dead backend is critical"
  uat_expect_stdout 'kind=webhook\.dead_backend.*severity=warning.*name=lookout-uat-ignore' \
    "state webhooks → failurePolicy=Ignore + the SAME dead backend is a warning"

  # Blast radius on the critical one: "a webhook is broken" is not
  # actionable, "this webhook rejects configmap creates in one
  # namespace" is.
  uat_expect_stdout 'kind=webhook\.failing_closed.*gates=' \
    "state webhooks → the critical finding carries which namespaces it gates"
  uat_expect_stdout 'kind=webhook\.failing_closed.*rules="CREATE configmaps"' \
    "state webhooks → and which operations it matches"

  uat_expect_stdout 'kind=webhook\.ca_expired.*days_left=-[0-9]+' \
    "state webhooks → an expired caBundle reports negative days rather than erroring"
  uat_expect_stdout 'kind=webhook\.ca_expiring.*days_left=[0-9]+' \
    "state webhooks → a soon-expiring caBundle is a separate, lesser finding"

  # slow_risk needs a LIVE backend — a dead one is failing_closed and
  # never also slow. So this assertion doubles as proof that the
  # backend-health branch actually distinguishes alive from dead.
  uat_expect_stdout 'kind=webhook\.slow_risk.*name=lookout-uat-slow' \
    "state webhooks → a long timeout on a live fail-closed webhook is info"
  uat_refute_stdout 'kind=webhook\.(failing_closed|dead_backend).*name=lookout-uat-slow' \
    "state webhooks → and that live webhook is NOT reported as dead"

  uat_run state webhooks --cert-warn=1h
  uat_expect_exit 0 "state webhooks --cert-warn=1h → exit 0"
  uat_refute_stdout 'kind=webhook\.ca_expiring' \
    "state webhooks --cert-warn=1h → the expiring bundle drops out of the window"
  uat_expect_stdout 'kind=webhook\.ca_expired' \
    "state webhooks --cert-warn=1h → the already-expired one does not (it is not a window question)"
}

# ---- stab drift: a hand edit against a GitOps controller -------------------

uat_fixture_drift() {
  uat_section "stab drift: out-of-band edits (config-drift)"

  if ! uat_fixture config-drift; then
    uat_skipped "stab drift → the whole section" "fixture config-drift did not inject"
    return 0
  fi

  local ns=lookout-uat-drift

  uat_run stab drift --namespace="$ns"
  uat_expect_exit 0 "stab drift → exit 0 in a GitOps-managed namespace"

  # Auto-detection is the path most callers take, and the one that can
  # silently pick the wrong controller. Assert the summary says how it
  # decided, not just that it found something.
  uat_expect_stdout 'manager=argocd-controller detection=majority share=[0-9]+%' \
    "stab drift → auto-detects the GitOps manager and shows its share"

  uat_expect_stdout 'kind=drift\.manual_edit.*severity=critical.*name=drift-hot' \
    "stab drift → a drifted container image is critical"
  uat_expect_stdout 'name=drift-hot.*fields=spec\.template\.spec\.containers\[app\]\.image' \
    "stab drift → and names the field path, not a diff"
  uat_expect_stdout 'kind=drift\.manual_edit.*severity=warning.*name=drift-cold' \
    "stab drift → a drifted grace period is only a warning"
  uat_expect_stdout 'name=drift-hot.*manager=kubectl-edit.*tool=kubectl' \
    "stab drift → reports the writing TOOL (a manager string, never a person)"

  # The negative control: same namespace, same controller, untouched.
  uat_refute_stdout 'name=drift-clean' \
    "stab drift → an object nobody edited is NOT reported"

  # --manager is the escape hatch, not a different answer.
  uat_run stab drift --namespace="$ns" --manager=argocd-controller
  uat_expect_exit 0 "stab drift --manager → exit 0"
  uat_expect_stdout 'detection=declared' \
    "stab drift --manager → skips detection and says so"
  uat_expect_stdout 'name=drift-hot' \
    "stab drift --manager → and reports the same drift"

  # A namespace with no GitOps controller must resolve to nothing and
  # SAY nothing, rather than measuring drift against whatever manager
  # happens to lead. The demo app is applied with kubectl, so it is
  # exactly that cluster.
  uat_run stab drift --namespace="$DEMO_NS"
  uat_expect_exit 0 "stab drift → exit 0 where there is no GitOps controller"
  uat_expect_stdout 'detection=none detection_reason=not-a-gitops-manager candidate=' \
    "stab drift → names the candidate it refused to measure against"
  uat_refute_stdout 'kind=drift\.manual_edit' \
    "stab drift → and emits nothing rather than drift against a guess"
}

# ---- stab drain: what a drain destroys quietly -----------------------------

uat_fixture_drain() {
  uat_section "stab drain: pre-maintenance blockers (drain-blockers)"

  if ! uat_fixture drain-blockers; then
    uat_skipped "stab drain → the whole section" "fixture drain-blockers did not inject"
    return 0
  fi

  # The fixture pins everything to whichever node the scheduler chose
  # for the bare pod, and does not label the node, so read it back
  # rather than guessing.
  local node
  node="$(kubectl -n lookout-uat-drain get pod drain-bare -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
  if [[ -z "$node" ]]; then
    uat_bad "stab drain → locate the fixture node" "drain-bare has no spec.nodeName"
    return 0
  fi

  uat_run stab drain --node="$node"
  uat_expect_exit 0 "stab drain --node → exit 0 on a node with blockers"

  uat_expect_stdout 'kind=drain\.bare_pod.*name=drain-bare' \
    "stab drain → an unowned pod is reported (eviction deletes it for good)"
  # The same pod, twice, for two different reasons. Collapsing them to
  # "this pod is a problem" would lose the one the operator can act on.
  uat_expect_stdout 'kind=drain\.local_storage.*name=drain-bare.*volumes=' \
    "stab drain → and its emptyDir volumes are a SEPARATE finding"
  uat_expect_stdout 'kind=drain\.local_storage.*medium=Memory' \
    "stab drain → a memory-backed emptyDir is marked as such"
  uat_expect_stdout 'kind=drain\.singleton.*workload=Deployment/lookout-uat-drain/drain-solo.*replicas=1' \
    "stab drain → a single-replica pod is named by its CONTROLLER, not its hash"
  uat_expect_stdout 'drainable=no blockers=[0-9]+' \
    "stab drain --node → the summary answers the question that was asked"

  # The -A roll-up must count the classes it collapsed, or it is just a
  # list of node names.
  uat_run stab drain -A
  uat_expect_exit 0 "stab drain -A → exit 0"
  uat_expect_stdout 'kind=drain\.node.*name='"$node"'.*bare_pods=[1-9]' \
    "stab drain -A → the node roll-up counts the bare pods on it"
  uat_expect_stdout 'kind=drain\.node.*name='"$node"'.*singletons=[1-9]' \
    "stab drain -A → and the singletons"
  uat_expect_stdout 'nodes=[0-9]+ blocked=[0-9]+' \
    "stab drain -A → the summary says how many nodes were examined and how many are blocked"
}

# ---- net probe: the one fixture that is not in the cluster -----------------

uat_fixture_netprobe() {
  uat_section "net probe: confirming a hypothesis from where lookout runs"

  # Deliberately NOT a scenarios/ fixture. `net probe` probes from
  # wherever the binary runs, which during a UAT run is the workstation
  # or the CI runner — a ClusterIP is not reachable from there, so a
  # cluster-side fixture would be testing the kind network, not the
  # command. A local listener is the honest target: it is exactly the
  # vantage point the command claims to report from.
  if ! command -v python3 >/dev/null 2>&1; then
    uat_skipped "net probe → the whole section" "no python3 to run a local listener"
    return 0
  fi

  local port
  port="$(uat_free_port)"
  python3 -m http.server "$port" --bind 127.0.0.1 >/dev/null 2>&1 &
  local listener=$!
  # shellcheck disable=SC2064 — $listener must expand now, not on exit
  trap "kill $listener 2>/dev/null || true" RETURN

  local i=0
  while ((i < 50)) && ! (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; do
    sleep 0.1
    i=$((i + 1))
  done
  if ((i == 50)); then
    uat_skipped "net probe → the whole section" "no local listener on 127.0.0.1:$port"
    return 0
  fi

  # A probe result is emitted on SUCCESS too. That is unusual for this
  # tool — every other command is silent when healthy — and it is
  # deliberate: "can this be reached" has two answers and both are the
  # point.
  uat_run net probe --tcp="127.0.0.1:$port" --probe-timeout=3s
  uat_expect_exit 0 "net probe --tcp → exit 0"
  uat_expect_stdout 'kind=probe\.tcp.*severity=info' \
    "net probe --tcp → a successful connect is reported, as info"
  uat_expect_stdout 'kind=probe\.tcp.*latency=' \
    "net probe --tcp → with how long it took"

  uat_run net probe --http="http://127.0.0.1:$port/" --probe-timeout=3s
  uat_expect_exit 0 "net probe --http → exit 0"
  uat_expect_stdout 'kind=probe\.http.*severity=info.*status=200' \
    "net probe --http → reports the status code"

  # A 404 is a reachable server giving a bad answer, which is a
  # different diagnosis from an unreachable one — hence warning, not
  # critical, and an error_class that says which.
  uat_run net probe --http="http://127.0.0.1:$port/no-such-path" --probe-timeout=3s
  uat_expect_exit 0 "net probe --http 404 → exit 0 (a 4xx is data, not a runtime error)"
  uat_expect_stdout 'kind=probe\.http.*error_class=http_4xx' \
    "net probe --http 404 → classified as http_4xx"

  # Failures. Both are the answer to the question, so both are exit 0.
  local closed
  closed="$(uat_free_port)"
  uat_run net probe --tcp="127.0.0.1:$closed" --probe-timeout=3s
  uat_expect_exit 0 "net probe --tcp refused → exit 0 (the probe answered)"
  uat_expect_stdout 'kind=probe\.tcp.*error_class=refused' \
    "net probe --tcp refused → classified as refused, not a generic error"

  uat_run net probe --dns=lookout-uat-no-such-host.invalid --probe-timeout=3s
  uat_expect_exit 0 "net probe --dns nxdomain → exit 0"
  uat_expect_stdout 'kind=probe\.dns.*error_class=nxdomain' \
    "net probe --dns nxdomain → classified as nxdomain"

  uat_run net probe --dns=localhost --probe-timeout=3s
  uat_expect_exit 0 "net probe --dns localhost → exit 0"
  uat_expect_stdout 'kind=probe\.dns.*severity=info.*ips=' \
    "net probe --dns localhost → resolves and lists the addresses"
}

# ---- the run ---------------------------------------------------------------

uat_case_fixtures() {
  uat_fixture_logs
  uat_fixture_edges
  uat_fixture_webhooks
  uat_fixture_drift
  uat_fixture_drain
  uat_fixture_netprobe
}
