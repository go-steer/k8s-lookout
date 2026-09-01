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

# Part 1 of docs/testing/cli-uat.md — the top-level commands (`bundle`,
# `health`) and the two `triage` checks that answer about a spec rather
# than about time (`delta`, `spec`).
#
# Two fixtures carry this whole file, and both exist because the
# alternative is an assertion that reads the cluster's mood:
#
#   broken-workloads  a namespace whose contents are known exactly —
#                     one crash loop, one unschedulable pod, and one
#                     healthy workload that must be named by neither
#                     `health` nor `triage delta`
#   secret-workload   one Deployment that consumes a Secret four ways,
#                     every value being the same canary string, so a
#                     single grep is a complete redaction answer
#
# Everything here is T0.

# The canary planted by examples/scenarios/secret-workload. Kept in one
# place: every command that can reach the Secret is swept for it, and
# the sweep is only worth anything if all of them look for the same
# string.
UAT_CANARY='c4nary-Sh0uld-Never-Appear-9f3a2b'

UAT_BROKEN_NS=lookout-uat-broken
UAT_SECRET_NS=lookout-uat-secrets

# ---- bundle: the first call of every incident ------------------------------

uat_toplevel_bundle() {
  uat_section "bundle: one correlated snapshot of a broken workload"

  if ! uat_fixture broken-workloads; then
    uat_skipped "bundle → the whole section" "fixture broken-workloads did not inject"
    return 0
  fi

  local target="Deployment/$UAT_BROKEN_NS/faulty"

  uat_run bundle --workload="$target"
  uat_expect_exit 0 "bundle → exit 0"
  uat_expect_summary_line "bundle → summary line"

  # The head names every section it produced. Asserting the list rather
  # than only the contents means a section that stops being emitted
  # fails here instead of silently thinning every later assertion.
  uat_expect_stdout 'kind=bundle\.target.*sections=spec,delta,edges,radius,logs' \
    "bundle → the head names all five sections"
  uat_expect_stdout 'kind=bundle\.target.*workload=Deployment/'"$UAT_BROKEN_NS"'/faulty' \
    "bundle → and the workload it resolved"

  # (a) the sanitized, default-elided spec.
  uat_expect_stdout 'kind=spec\.container.*section=spec.*image=python:3\.12-alpine' \
    "bundle spec → the container's image"
  uat_refute_stdout 'terminationMessagePath|dnsPolicy|restartPolicy=Always' \
    "bundle spec → API defaults are elided, not echoed"

  # (b) the abnormal children, from the same scanner triage delta uses.
  uat_expect_stdout 'kind=pod\.crashloop.*section=delta.*exit_code=1' \
    "bundle delta → the crash-looping pod and why it died"

  # (c) the broken dependency edge: a Service in front of a workload
  # that never becomes ready is the reason the incident is user-visible.
  uat_expect_stdout 'kind=edge\.selector_unready.*section=edges.*selected=1 ready=0' \
    "bundle edges → the Service selects the pod but has 0 ready"

  # (d) blast radius. Both directions and both hop distances, because a
  # radius that only walks one way is the failure mode worth catching.
  uat_expect_stdout 'kind=radius\.neighbor.*section=radius relation=upstream hop=1' \
    "bundle radius → the upstream owners at hop 1"
  uat_expect_stdout 'kind=radius\.neighbor.*section=radius relation=downstream hop=1' \
    "bundle radius → the node and configmaps it depends on"

  # (e) distilled logs, from the PREVIOUS container: the pod is in
  # CrashLoopBackOff, so there is no running container to read, and a
  # bundle that gave up there would be useless on the one workload
  # state it exists to explain. The counts are exact because the
  # fixture prints a fixed corpus and exits.
  uat_expect_stdout 'kind=log\.template.*section=logs.*template="boot step=<\*> phase=init" count=40' \
    "bundle logs → 40 boot lines collapse to one template, read from the dead container"
  uat_expect_stdout 'kind=log\.template.*section=logs.*level=fatal' \
    "bundle logs → the fatal line is classified, not just clustered"

  # --max-templates: the cap has to say what it dropped, and it has to
  # keep the ERROR template over the chatty one — a truncation that
  # discards the diagnosis to make room for boot noise is worse than no
  # truncation.
  uat_run bundle --workload="$target" --max-templates=1
  uat_expect_exit 0 "bundle --max-templates=1 → exit 0"
  uat_expect_stdout 'kind=log\.template.*level=fatal' \
    "bundle --max-templates=1 → the surviving template is the fatal one"
  uat_expect_stdout 'kind=log\.overflow.*omitted_templates=1 omitted_lines=40' \
    "bundle --max-templates=1 → and it names what it dropped"

  # --depth bounds the DIRECTED walk only. Lateral neighbours are
  # derived from downstream hits and land at hop+1, so a co-tenant pod
  # is reported at hop=2 even at --depth=1. That is the design
  # (pkg/graph/query.go), not a leak: the point of the lateral relation
  # is "these share a node with you", and it costs no extra traversal.
  uat_run bundle --workload="$target" --depth=1
  uat_expect_exit 0 "bundle --depth=1 → exit 0"
  uat_expect_stdout 'kind=radius\.neighbor.*relation=lateral hop=2' \
    "bundle --depth=1 → laterals still appear at hop 2 (derived, not traversed)"

  uat_run bundle --workload="$target" --depth=0
  uat_expect_exit 2 "bundle --depth=0 → exit 2 (a zero-hop radius is a usage error)"
  uat_expect_stderr 'depth must be at least 1' \
    "bundle --depth=0 → and says so"

  # --incident: the same bundle, selected by the payload a watch
  # session opened with instead of by a --workload string. The payload
  # names the POD; the target has to come back as the Deployment,
  # because that is the object an operator acts on.
  local pod incident
  pod="$(kubectl -n "$UAT_BROKEN_NS" get pods -l app.kubernetes.io/name=faulty \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  if [[ -z "$pod" ]]; then
    uat_skipped "bundle --incident" "could not read the faulty pod's name"
  else
    incident="$(
      printf '{"kind":"k8s-event","reason":"CrashLoopBackOff","namespace":"%s",' "$UAT_BROKEN_NS"
      printf '"kind_of_object":"Pod","name":"%s","severity":"critical","source":"sentinel"}' "$pod"
    )"
    uat_run bundle --incident="$incident"
    uat_expect_exit 0 "bundle --incident → exit 0"
    uat_expect_stdout 'kind=bundle\.target.*workload=Deployment/'"$UAT_BROKEN_NS"'/faulty' \
      "bundle --incident → a Pod reference resolves up the owner chain to its Deployment"

    uat_run bundle --incident="$incident" --workload="Deployment/$UAT_BROKEN_NS/steady"
    uat_expect_exit 2 "bundle --incident + --workload → exit 2 (two targets is a usage error)"
  fi

  uat_run bundle --incident='{"nope"'
  uat_expect_exit 2 "bundle --incident with malformed JSON → exit 2"
  uat_expect_stderr 'not valid inject payload JSON' \
    "bundle --incident malformed → names the parse failure"
}

# ---- bundle + triage spec: the secret-safety contract ----------------------

uat_toplevel_secrets() {
  uat_section "secret-safety: a workload that consumes a Secret four ways"

  if ! uat_fixture secret-workload; then
    uat_skipped "secret-safety → the whole section" "fixture secret-workload did not inject"
    return 0
  fi

  # This is the one assertion in the suite where a false PASS is a
  # security bug rather than a missing test. It is written as a sweep —
  # every command that can reach the Secret, one canary — because a
  # per-field allowlist can only catch the leaks someone thought of.
  local target="Deployment/$UAT_SECRET_NS/keeper"

  uat_run triage spec --workload="$target"
  uat_expect_exit 0 "triage spec → exit 0"
  uat_expect_stdout 'kind=spec\.container.*env="DB_PASSWORD=secretKeyRef:vault-creds\.db-password"' \
    "triage spec → an env var from a Secret is rendered as its REFERENCE"
  uat_expect_stdout 'kind=spec\.container.*env_from=secretRef:vault-creds' \
    "triage spec → and so is envFrom"
  uat_refute_stdout "$UAT_CANARY" \
    "triage spec Deployment → no secret value reaches stdout"

  # The Secret itself, which is the harder case: the command's whole
  # job here is to describe an object whose entire content is secret.
  uat_run triage spec --workload="Secret/$UAT_SECRET_NS/vault-creds"
  uat_expect_exit 0 "triage spec Secret → exit 0"
  uat_expect_stdout 'kind=spec\.resource.*keys=api-token\(33B\),db-password\(33B\)' \
    "triage spec Secret → key names and byte lengths, which is the useful half"
  uat_refute_stdout "$UAT_CANARY" \
    "triage spec Secret → and never the values"

  uat_run triage spec --workload="Secret/$UAT_SECRET_NS/vault-pull"
  uat_expect_exit 0 "triage spec dockerconfigjson → exit 0"
  uat_expect_stdout 'kind=spec\.resource.*type=kubernetes\.io/dockerconfigjson' \
    "triage spec dockerconfigjson → reports the type"
  uat_refute_stdout "$UAT_CANARY" \
    "triage spec dockerconfigjson → the registry password stays inside"

  # The bundle reaches the Secret a different way — as a graph
  # neighbour of the workload that mounts it — so it gets its own
  # sweep rather than inheriting triage spec's.
  uat_run bundle --workload="$target"
  uat_expect_exit 0 "bundle on a secret-consuming workload → exit 0"
  uat_expect_stdout 'kind=radius\.neighbor.*kind_of_object=Secret name=vault-creds' \
    "bundle radius → the mounted Secret is a neighbour, by name"
  uat_refute_stdout "$UAT_CANARY" \
    "bundle → naming a Secret is not reading it"

  uat_run triage delta --namespace="$UAT_SECRET_NS" --restarts=1 --pending-age=10s
  uat_refute_stdout "$UAT_CANARY" \
    "triage delta → no secret value reaches stdout"

  uat_run health --namespace="$UAT_SECRET_NS"
  uat_refute_stdout "$UAT_CANARY" \
    "health → no secret value reaches stdout"

  uat_run state edges --workload="$target"
  uat_refute_stdout "$UAT_CANARY" \
    "state edges → no secret value reaches stdout"

  # --diff is honestly unimplemented, and the UAT records that rather
  # than skipping it: an unimplemented flag that exits 2 and says which
  # section owns it is a contract, and silently accepting the flag
  # would be the regression.
  uat_run triage spec --workload="$target" --diff
  uat_expect_exit 2 "triage spec --diff → exit 2 (declared, not yet implemented)"
  uat_expect_stderr 'not yet implemented' \
    "triage spec --diff → says so, with the design section that owns it"

  # The two error paths, which differ: a missing object is a runtime
  # error (the call was well-formed, the cluster said no), a missing
  # target is a usage error.
  uat_run triage spec --workload="Deployment/$UAT_SECRET_NS/no-such-deployment"
  uat_expect_exit 1 "triage spec on a nonexistent resource → exit 1 (runtime, not usage)"
  uat_expect_stderr 'not found' \
    "triage spec nonexistent → the apiserver's answer is passed through"

  uat_run triage spec
  uat_expect_exit 2 "triage spec with no target → exit 2"
  uat_expect_stderr 'no target' \
    "triage spec with no target → says what to pass"
}

# ---- health: the ten-category scorecard ------------------------------------

uat_toplevel_health() {
  uat_section "health: the scorecard always answers"

  if ! uat_fixture broken-workloads; then
    uat_skipped "health → the whole section" "fixture broken-workloads did not inject"
    return 0
  fi

  uat_run health --namespace="$UAT_BROKEN_NS"
  uat_expect_exit 0 "health → exit 0"
  uat_expect_summary_line "health → summary line"
  uat_expect_stdout_pure "health → stdout is payload only"

  # Every category reports, including the ones with nothing to say.
  # A scorecard that omits its healthy rows cannot be distinguished
  # from one that failed to run them.
  local cat
  for cat in control-plane nodes crashloops pending rollouts storage addons quota certs webhooks; do
    uat_expect_stdout "kind=health\.category.*category=$cat status=(healthy|degraded|unavailable)" \
      "health → category $cat answers"
  done

  # The two the fixture provokes, each landing where it belongs.
  uat_expect_stdout 'kind=health\.category.*category=crashloops status=degraded.*top="pod\.crashloop' \
    "health → the crash loop is categorized as crashloops, and named inline"
  uat_expect_stdout 'kind=health\.category.*category=rollouts status=degraded total=2' \
    "health → both incomplete rollouts are counted under rollouts"

  # --top caps the inline names without changing the count, so the
  # scorecard stays one line whatever the blast radius.
  uat_run health --namespace="$UAT_BROKEN_NS" --top=1
  uat_expect_stdout 'category=rollouts status=degraded total=2 top="workload\.rollout [^;]*"$' \
    "health --top=1 → one name inline, total still 2"

  # A namespace-scoped run reports the cluster-scoped categories as
  # unavailable WITH a reason, rather than as healthy. Reporting
  # "healthy" for something it did not look at is the failure mode this
  # pins.
  uat_run health --namespace="$UAT_BROKEN_NS"
  uat_expect_stdout 'reason=Unavailable message="Nodes are cluster-scoped; run without --namespace" category=nodes status=unavailable' \
    "health --namespace → a cluster-scoped category is unavailable WITH a reason, not healthy"

  # The healthy path. It needs a namespace known to be clean, which on
  # a shared cluster means one this suite created: the fixture
  # namespace whose only workload is up. Asserting "no findings
  # anywhere" against the cluster at large would fail on whatever the
  # last scenario left behind.
  if ! uat_fixture secret-workload; then
    uat_skipped "health → the healthy path" "fixture secret-workload did not inject"
    return 0
  fi
  uat_run health --namespace="$UAT_SECRET_NS"
  uat_expect_exit 0 "health on a healthy namespace → exit 0"
  for cat in crashloops pending rollouts storage quota certs; do
    uat_expect_stdout "kind=health\.category.*category=$cat status=healthy" \
      "health healthy-path → $cat is explicitly healthy"
  done
  uat_refute_stdout 'severity=critical' \
    "health healthy-path → nothing critical is invented"
}

# ---- triage delta: every abnormal object in one scan -----------------------

uat_toplevel_delta() {
  uat_section "triage delta: the classes, and the healthy object between them"

  if ! uat_fixture broken-workloads; then
    uat_skipped "triage delta → the whole section" "fixture broken-workloads did not inject"
    return 0
  fi

  local scope=(--namespace="$UAT_BROKEN_NS" --restarts=1 --pending-age=10s)

  uat_run triage delta "${scope[@]}"
  uat_expect_exit 0 "triage delta → exit 0"
  uat_expect_summary_line "triage delta → summary line"
  uat_expect_stdout 'kind=pod\.crashloop.*name=faulty-.*restarts=[0-9]+ last_state=Error exit_code=1' \
    "triage delta → the crash loop, with its restart count and exit code"
  uat_expect_stdout 'kind=pod\.pending.*name=stuck-.*reason=Unschedulable.*age=' \
    "triage delta → the unschedulable pod, with the scheduler's own reason"

  # The negative control. Two broken workloads and one healthy one in
  # the same namespace: a scanner that reports all three looks exactly
  # like a correct one until this line runs.
  uat_refute_stdout 'name=steady' \
    "triage delta → the healthy workload beside them is not reported"

  # --only restricts the scan rather than filtering the output:
  # scanned= drops too, which is the difference between not looking and
  # looking and discarding.
  uat_run triage delta "${scope[@]}" --only=quota
  uat_expect_exit 0 "triage delta --only=quota → exit 0"
  uat_expect_stdout '^scanned=0 findings=0' \
    "triage delta --only=quota → the pod classes are not scanned at all"

  uat_run triage delta "${scope[@]}" --only=pods
  uat_expect_exit 0 "triage delta --only=pods → exit 0"
  uat_expect_stdout 'kind=pod\.crashloop' \
    "triage delta --only=pods → keeps the pod classes"

  uat_run triage delta "${scope[@]}" --only=bogus
  uat_expect_exit 2 "triage delta --only=bogus → exit 2"
  uat_expect_stderr 'unknown class "bogus" \(want a subset of' \
    "triage delta --only=bogus → names the valid subset"

  # --restarts is a threshold on the restart COUNT, and CrashLoopBackOff
  # is a state — so raising the threshold above the pod's actual count
  # must NOT suppress the crash loop. The two are different findings
  # about the same pod, and collapsing them would mean a workload that
  # has been wedged for an hour disappears from the scan the moment
  # someone tunes the flag up to quiet a noisy neighbour.
  uat_run triage delta --namespace="$UAT_BROKEN_NS" --restarts=100000 --pending-age=10s
  uat_expect_stdout 'kind=pod\.crashloop.*name=faulty-' \
    "triage delta --restarts=100000 → a sustained state is not gated by a count threshold"

  # The §8 fingerprint is a hash of the incident CLASS, deliberately not
  # of the object (pkg/engine/fingerprint.go, FROZEN). Two different
  # Deployments failing the same way therefore carry the SAME
  # fingerprint — surprising enough that it is worth a check, because
  # "make the fingerprint unique per object" is the obvious wrong fix
  # and it would split every fleet-wide rollup.
  uat_run triage delta "${scope[@]}"
  local fps
  fps="$(grep -o 'kind=workload\.rollout.*fingerprint=[^ ]*' <<<"$UAT_OUT" |
    grep -o 'fingerprint=[^ ]*' | sort -u | wc -l)"
  if [[ "$fps" == "1" ]]; then
    uat_ok "triage delta → two distinct workloads failing alike share one class fingerprint (§8)"
  else
    uat_bad "triage delta → two distinct workloads failing alike share one class fingerprint (§8)" \
      "got $fps distinct workload.rollout fingerprints, want 1"
  fi
}

# ---- the run ---------------------------------------------------------------

uat_case_toplevel() {
  uat_toplevel_bundle
  uat_toplevel_secrets
  uat_toplevel_health
  uat_toplevel_delta
}
