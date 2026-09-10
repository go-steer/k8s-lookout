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

# The store-backed half of the read path (issue #177): everything that
# needs a `--store`, which is everything that can answer about a cluster
# state that no longer exists.
#
# TIER: T0. The doc listed this as a tier of its own ("T-store") on the
# assumption that reading a store meant extracting one from the deployed
# sentinel's container. It does not — `examples/scenarios/store-postmortem`
# runs a LOCAL `lookout watch --dry-run --store=…` against the same
# kubeconfig every other case already uses, so this needs nothing a bare
# kind cluster lacks. See lib.sh for why local rather than in-cluster.
#
# The shape of every claim here is a PAIR: what the store answers, and
# what the live cluster answers to the same question. A case that only
# proved `--at` returns rows would pass just as well if `--at` silently
# reported *now*, which is the single wrong answer a post-mortem tool
# must never give. So the fixture manufactures an object that exists
# only inside the window — `$POSTMORTEM_CANARY`, created and destroyed
# by the inject — and every `--at` assertion has a live control that
# must FAIL to find what `--at` finds.
#
# This is the last case file on purpose. It is the only UAT fixture that
# touches the demo app (it scales `web` 2→3 to put a rescale inside the
# window, and its revert scales back), so nothing that assumes a
# pristine `lookout-demo` runs after it.

UAT_STORE_BROKEN_NS=lookout-uat-broken

uat_case_store() {
  if ! uat_fixture store-postmortem; then
    uat_skipped "the whole store case" "fixture store-postmortem did not inject"
    return 0
  fi

  UAT_STORE_ONSET="$(postmortem_onset)"
  UAT_STORE_AFTER="$(postmortem_after)"
  if [[ -z "$UAT_STORE_ONSET" || -z "$UAT_STORE_AFTER" ]]; then
    uat_bad "store → the fixture recorded both edges of the window" \
      "onset='$UAT_STORE_ONSET' after='$UAT_STORE_AFTER' — inject claimed success without them"
    return 0
  fi
  echo "  window: $UAT_STORE_ONSET .. $UAT_STORE_AFTER"

  uat_store_radius
  uat_store_changes
  uat_store_status
  uat_store_annotation
  uat_store_guards
}

# ---- triage radius --at: the topology as it was ----------------------------

uat_store_radius() {
  uat_section "triage radius --at: the topology as it was, not as it is"

  local canary="Pod/$DEMO_NS/$POSTMORTEM_CANARY"
  local web="Deployment/$DEMO_NS/web"

  # (a) The differential, at its sharpest: the target itself is gone.
  # Not "live returns fewer rows" — live cannot resolve the object at
  # all, and that is a runtime error rather than an empty result.
  uat_run triage radius "$canary"
  uat_expect_exit 1 "triage radius (live) → exit 1 on an object that no longer exists"
  uat_expect_stderr 'not found in the topology' \
    "triage radius (live) → and says the target is not in the topology"

  uat_run triage radius "$canary" --at "$UAT_STORE_ONSET" --store "$POSTMORTEM_STORE"
  uat_expect_exit 0 "triage radius --at → exit 0 on the same object, from the store"
  uat_expect_summary_line "triage radius --at → summary line"
  uat_expect_stdout "source=history at=$UAT_STORE_ONSET" \
    "triage radius --at → the summary says which mode answered, and as of when"
  uat_expect_stdout \
    "kind_of_object=ConfigMap name=$POSTMORTEM_CANARY direction=downstream relation=Mounts hop=1" \
    "triage radius --at → the canary's Mounts edge to its own ConfigMap"

  # (b) The same query in both modes against a target that still
  # exists, which is where the FIDELITY of history is legible.
  uat_run triage radius "$web"
  uat_expect_exit 0 "triage radius (live) → exit 0 on web"
  uat_expect_stdout 'source=live' "triage radius (live) → source=live"
  uat_expect_stdout 'direction=lateral .*ready=(true|false)' \
    "triage radius (live) → a lateral pod carries its readiness"
  uat_expect_stdout 'kind_of_object=Service name=web direction=upstream relation=Selects' \
    "triage radius (live) → the Service in front of the workload"
  uat_expect_stdout 'kind_of_object=EndpointSlice .*direction=upstream relation=RoutesTo' \
    "triage radius (live) → and the EndpointSlice behind it"
  uat_refute_stdout 'observed=unknown' \
    "triage radius (live) → nothing is merely referenced: every node was seen"

  uat_run triage radius "$web" --at "$UAT_STORE_ONSET" --store "$POSTMORTEM_STORE"
  uat_expect_exit 0 "triage radius --at → exit 0 on web"
  uat_expect_stdout "kind_of_object=Pod name=$POSTMORTEM_CANARY direction=lateral" \
    "triage radius --at → web's neighbourhood still contains the deleted canary"
  uat_expect_stdout 'kind_of_object=ConfigMap .*relation=Mounts .*observed=unknown' \
    "triage radius --at → a referenced-only kind is marked observed=unknown, not asserted"
  uat_refute_stdout 'ready=' \
    "triage radius --at → and readiness is absent rather than guessed from now"

  # A KNOWN GAP, asserted so it is visible rather than discovered
  # (issue #396). The graph feed watches pods, nodes and replicasets;
  # Services and EndpointSlices are deliberately outside it, which is
  # right for storm correlation and silently sets the ceiling for the
  # post-mortem path too. Live has the routing layer two assertions
  # above; history does not, and nothing in the output says so.
  # Whichever way #396 is fixed, this refutation fails and points there.
  uat_refute_stdout 'relation=(Selects|RoutesTo)' \
    "triage radius --at → history has no routing layer at all (#396: it should at least say so)"

  # (c) --depth bounds the walk. Asserted from a POD, because from a
  # Deployment the interesting hops are laterals, and laterals are a
  # one-hop reflection off downstream hits stamped hop+1 outside the
  # bound — so they are hop=2 at --depth=1 by design and prove nothing
  # about the bound. Pod → ReplicaSet → Deployment is a real chain.
  local pod
  pod="$(kubectl -n "$DEMO_NS" get pods -l app.kubernetes.io/name=web \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$pod" ]]; then
    uat_skipped "triage radius --depth → the bound" "no web pod to walk up from"
    return 0
  fi

  uat_run triage radius "Pod/$DEMO_NS/$pod" --depth=1
  uat_expect_exit 0 "triage radius --depth=1 → exit 0"
  uat_expect_stdout 'kind_of_object=ReplicaSet name=web-.*direction=upstream relation=Owns hop=1' \
    "triage radius --depth=1 → the owning ReplicaSet, one hop up"
  uat_refute_stdout 'kind_of_object=Deployment name=web ' \
    "triage radius --depth=1 → but not the Deployment two hops up"
  uat_expect_stdout 'direction=lateral .*hop=2' \
    "triage radius --depth=1 → laterals still reflect off the downstream hits (hop+1, outside the bound)"

  uat_run triage radius "Pod/$DEMO_NS/$pod" --depth=2
  uat_expect_exit 0 "triage radius --depth=2 → exit 0"
  uat_expect_stdout 'kind_of_object=Deployment name=web direction=upstream relation=Owns hop=2' \
    "triage radius --depth=2 → and now the Deployment, so the bound is a dial"
}

# ---- triage changes --at: what changed, from the delta log -----------------

uat_store_changes() {
  uat_section "triage changes --at: the delta log, not an approximation of it"

  local web="Deployment/$DEMO_NS/web"

  uat_run triage changes "$web" --at "$UAT_STORE_ONSET" --store "$POSTMORTEM_STORE" --since=30m
  uat_expect_exit 0 "triage changes --at → exit 0"
  uat_expect_summary_line "triage changes --at → summary line"
  uat_expect_stdout "source=history at=$UAT_STORE_ONSET window=" \
    "triage changes --at → the summary names the mode, the instant and the window"

  # All three relations, from one query. The scoping vocabulary is
  # self|upstream|lateral, and a case that asserted one of them would
  # be calling the scoping tested on a third of it.
  uat_expect_stdout \
    "kind=change\.rollout .*kind_of_object=Pod name=$POSTMORTEM_CANARY reason=Added .*relation=lateral origin=log" \
    "triage changes --at → the canary's Added record, at relation=lateral"
  uat_expect_stdout \
    'kind=change\.scale .*kind_of_object=ReplicaSet name=web-.*reason=Updated .*relation=upstream origin=log fields="replicas=2→3"' \
    "triage changes --at → the rescale as a field delta on the ReplicaSet, at relation=upstream"
  uat_expect_stdout \
    'kind=change\.rollout .*kind_of_object=Pod name=web-.*reason=Added .*relation=self origin=log' \
    "triage changes --at → the pod that rescale created, at relation=self"

  # §6.5: a field delta carries names, counts and hashes — never a
  # value. `replicas=2→3` is a count; an env var's contents would be a
  # leak, and the delta log is written by a process with cluster-wide
  # read, so this is the assertion that keeps it publishable.
  uat_refute_stdout 'fields="[^"]*(password|token|secret|BEGIN )' \
    "triage changes --at → a field delta names what changed, never what it changed to"

  uat_run triage changes "$web" --since=30m
  uat_expect_exit 0 "triage changes (live) → exit 0"
  uat_expect_stdout 'source=live-approximation' \
    "triage changes (live) → degrades honestly: it says the answer is an approximation"
  uat_expect_stdout 'kind=change\.scale .*kind_of_object=Deployment name=web .*origin=event' \
    "triage changes (live) → and reconstructs the rescale from an Event instead of the log"
  uat_refute_stdout "name=$POSTMORTEM_CANARY" \
    "triage changes (live) → but cannot see the canary, which is the whole difference"

  # A KNOWN GAP, asserted so it is visible rather than discovered
  # (issue #393). Ask the same question at the far edge of the window —
  # an instant the store answers for in which the canary is already
  # gone — and its Added record from earlier in that same window has
  # vanished retroactively, with no Deleted record in its place.
  # `reason=Deleted` is structurally unreachable: the neighbourhood is
  # built from the snapshot as of --at, so an object absent from that
  # snapshot has every one of its records filtered out, including the
  # one saying it went away. The instant is recorded by the fixture
  # rather than computed here, because "deleted" and "absent from the
  # newest snapshot" are up to one snapshot interval apart and the
  # assertion must not race that.
  uat_run triage changes "$web" --at "$UAT_STORE_AFTER" --store "$POSTMORTEM_STORE" --since=30m
  uat_expect_exit 0 "triage changes --at (after the delete) → exit 0"
  uat_refute_stdout 'reason=Deleted' \
    "triage changes --at → no deletion is ever reported (#393)"
  uat_refute_stdout "name=$POSTMORTEM_CANARY" \
    "triage changes --at → and after the delete the canary's earlier Added record is gone too (#393)"
}

# ---- triage status: the one read-path command that writes ------------------

uat_store_status() {
  uat_section "triage status: write, then read back, pinned by resource"

  if ! uat_fixture broken-workloads; then
    uat_skipped "triage status → the whole section" "fixture broken-workloads did not inject"
    return 0
  fi

  local ns="$UAT_STORE_BROKEN_NS"
  local target="Deployment/$ns/faulty"

  # The fingerprint comes from the finding, not from a constant: it is
  # an incident-CLASS identity, so hardcoding one here would be
  # hardcoding a hash the checks are free to re-derive.
  uat_run health --namespace="$ns"
  uat_expect_exit 0 "health → exit 0 on the broken namespace (a finding is data)"
  local fp
  fp="$(grep -E 'kind=workload\.rollout .*name=faulty ' <<<"$UAT_OUT" |
    grep -oE 'sha256:[0-9a-f]{64}' | head -1)"
  if [[ -z "$fp" ]]; then
    uat_skipped "triage status → the whole section" "no workload.rollout fingerprint for faulty"
    return 0
  fi

  uat_run triage status --store="$POSTMORTEM_STORE" \
    --fingerprint="$fp" --resource="$target" \
    --status=triaged --severity-override=warning --session=uat-store \
    --root-cause="uat: recorded by the store case" \
    --action="none — the fixture reverts"
  uat_expect_exit 0 "triage status (write) → exit 0"
  uat_expect_summary_line "triage status (write) → summary line"
  uat_expect_stdout "resource_key=$target triage_status=triaged" \
    "triage status (write) → echoes the record it stored, pin first"
  uat_expect_stdout 'severity_override=warning' \
    "triage status (write) → including the routing judgment"

  uat_run triage status --store="$POSTMORTEM_STORE" --resource="$target"
  uat_expect_exit 0 "triage status (read by resource) → exit 0"
  uat_expect_stdout 'triage_status=triaged triage_root_cause="uat: recorded by the store case"' \
    "triage status (read by resource) → the record comes back whole"
  uat_expect_stdout 'triage_session=uat-store' \
    "triage status (read by resource) → with the pointer back to the transcript"

  uat_run triage status --store="$POSTMORTEM_STORE" --fingerprint="$fp"
  uat_expect_exit 0 "triage status (read by fingerprint) → exit 0"
  uat_expect_stdout "resource_key=$target" \
    "triage status (read by fingerprint) → finds the same record"

  # The pin is the RESOURCE, and this fixture is the reason that
  # matters: `faulty` and `stuck` are two different broken Deployments
  # carrying the SAME workload.rollout fingerprint, because the
  # fingerprint is an incident class. A record keyed on it alone would
  # have just triaged both.
  uat_run triage status --store="$POSTMORTEM_STORE" --resource="Deployment/$ns/stuck"
  uat_expect_exit 0 "triage status (read the other resource) → exit 0"
  uat_expect_stdout 'findings=0' \
    "triage status → the sibling sharing the fingerprint has no record: the pin is the resource"
  uat_refute_stdout 'triage_status=' \
    "triage status → and nothing of the first record leaks onto it"
}

# ---- --store on a read command: the record changes what is reported --------

uat_store_annotation() {
  uat_section "--store on health and bundle: a triaged incident reads differently"

  local ns="$UAT_STORE_BROKEN_NS"
  if [[ " ${UAT_FIXTURES[*]-} " != *" broken-workloads "* ]]; then
    uat_skipped "health --store → the whole section" "fixture broken-workloads is not injected"
    return 0
  fi

  # The control: without the store, the record does not exist as far as
  # any read command is concerned.
  uat_run health --namespace="$ns"
  uat_expect_stdout 'kind=workload\.rollout severity=critical .*name=faulty' \
    "health (no store) → the triaged workload is still reported critical"
  uat_refute_stdout 'triage_status=' \
    "health (no store) → and carries no triage annotation at all"

  uat_run health --namespace="$ns" --store="$POSTMORTEM_STORE"
  uat_expect_exit 0 "health --store → exit 0"
  uat_expect_stdout 'kind=workload\.rollout severity=warning .*name=faulty .*triage_status=triaged' \
    "health --store → the override lands on the finding: critical becomes warning"
  uat_expect_stdout 'name=faulty .*triage_root_cause="uat: recorded by the store case" .*triage_age=' \
    "health --store → with the diagnosis and how long it has been open"
  uat_expect_stdout 'kind=workload\.rollout severity=critical .*name=stuck' \
    "health --store → the sibling on the same fingerprint is untouched"

  # Scorecard altitude. `rollouts` must stay critical, because `stuck`
  # still is — an override that moved the category would be downgrading
  # an incident nobody triaged.
  uat_expect_stdout 'kind=health\.category severity=critical category=rollouts status=degraded' \
    "health --store → the category rolls up the WORST remaining finding, not the triaged one"

  uat_run bundle --workload="Deployment/$ns/faulty" --store="$POSTMORTEM_STORE"
  uat_expect_exit 0 "bundle --store → exit 0"
  uat_expect_stdout 'kind=bundle\.target severity=warning .*name=faulty .*triage_status=triaged' \
    "bundle --store → the same record merges into the incident's first call"
}

# ---- the guards ------------------------------------------------------------

uat_store_guards() {
  uat_section "the guards: a post-mortem may fail, but may not answer 'now'"

  # `--at` without `--store` is exit 2 everywhere, and 00-contract.sh
  # asserts that for every command. What only this case can assert is
  # the other side: with a store, but for an instant the store cannot
  # reach. That is a RUNTIME error (exit 1), not a usage error — the
  # invocation was well-formed, the data does not exist — and the
  # difference is what tells a caller whether to fix the command or
  # widen the retention.
  uat_run triage radius "Deployment/$DEMO_NS/web" \
    --at=2000-01-01T00:00:00Z --store="$POSTMORTEM_STORE"
  uat_expect_exit 1 "triage radius --at before the first snapshot → exit 1, not 2 and not now"
  uat_expect_stderr 'no graph history at or before' \
    "triage radius --at before the first snapshot → and says the store cannot reach that far back"

  uat_run triage changes "Deployment/$DEMO_NS/web" \
    --at=2000-01-01T00:00:00Z --store="$POSTMORTEM_STORE"
  uat_expect_exit 1 "triage changes --at before the first snapshot → exit 1 as well"

  # triage status has two guards of its own, and both are usage errors
  # because both are the caller asking for something incoherent.
  uat_run triage status --resource="Deployment/$DEMO_NS/web"
  uat_expect_exit 2 "triage status without --store → exit 2"
  uat_expect_stderr 'store is required' \
    "triage status without --store → names the flag, and where the records live"

  uat_run triage status --store="$POSTMORTEM_STORE"
  uat_expect_exit 2 "triage status with neither --fingerprint nor --resource → exit 2"
  uat_expect_stderr 'needs --fingerprint and/or --resource' \
    "triage status → a read with no selector is a usage error, not a dump of every record"
}
