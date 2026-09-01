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

# The shared UAT fixture table: what a valid invocation looks like for
# every registered read-path command, and the scratch files those
# invocations refer to.
#
# This lives beside the cases rather than inside one because more than
# one case needs it. 00-contract.sh asserts the generic CLI contract
# against these invocations; 10-mcp.sh replays the same invocations
# through the MCP server and compares. Sharing the table is the point —
# a parity check is only meaningful if both sides are driven from one
# description of what a valid call is.

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

# ---- what a valid invocation looks like, per command ----------------------

# Every command accepts --workload and -A, because those are common
# flags printed in every --help — so which flags a command *advertises*
# says nothing about what it *needs*. These entries are each justified
# by the command's own usage error; the categories are:
#
#   ""                 not namespace-scoped: passing -A is a usage error
#   "-A"               namespace-scoped, whole cluster
#   "--workload=…"     requires a target
#   "<positional>"     takes its target positionally
#   other              a command-specific requirement (--store, --exemptions)
#
# Values must not contain spaces — they are word-split into an argv.
declare -A UAT_INVOCATION

uat_build_invocations() {
  UAT_INVOCATION=(
    # Read the provider's cluster record, not objects in the cluster.
    ["audit cluster"]=""
    ["audit upgrades"]=""
    ["cloud ipspace"]=""
    ["cloud orphans"]=""
    ["cloud quota"]=""
    ["cloud stockout"]=""
    # Not scoped to a namespace either, but it does need a query pack
    # named; on kind every pack degrades to pack_unavailable at exit 0.
    ["perf probe"]="--pack=apiserver"
    # Cluster-scoped objects; -A is meaningless and refused.
    ["state webhooks"]=""
    # Probes from where lookout runs, so it is not cluster-scoped
    # either. localhost always resolves, including on a CI runner.
    ["net probe"]="--dns=localhost --probe-timeout=3s"
    # scan takes no target by design — that is the point of it.
    ["scan"]=""
    # Namespace-scoped sweeps.
    ["audit hardening"]="-A"
    ["audit netpol"]="-A"
    ["audit workloads"]="-A"
    ["health"]="-A"
    ["stab drift"]="-A"
    ["state gateway"]="-A"
    ["state storage"]="-A"
    ["state volumes"]="-A"
    ["state wi"]="-A"
    ["triage delta"]="-A"
    ["triage events"]="-A"
    ["triage list"]="-A"
    ["triage top"]="-A"
    # Targeted reads.
    ["bundle"]="--workload=$UAT_WORKLOAD"
    ["state edges"]="--workload=$UAT_WORKLOAD"
    ["triage logs"]="--workload=$UAT_WORKLOAD"
    # Targeted, positionally.
    ["triage changes"]="$UAT_WORKLOAD"
    ["triage radius"]="$UAT_WORKLOAD"
    ["triage spec"]="$UAT_WORKLOAD"
    # Exactly one of --node/-A; --node is the interesting half.
    ["stab drain"]="--node=$UAT_NODE"
    # Store-backed.
    ["findings diff"]="--store=$UAT_STORE --report=$UAT_REPORT"
    ["findings ack"]="$UAT_SUBJECT --store=$UAT_STORE"
    # Read mode (no --status) still has to select records to read.
    ["triage status"]="--store=$UAT_STORE --resource=$UAT_WORKLOAD"
    # Reports on an exemption file, so it needs one.
    ["audit exemptions"]="--exemptions=$UAT_EXEMPTIONS"
  )
}

# Commands that are NOT namespace-scoped must say so — passing -A has
# to be a usage error, not a silently ignored flag, because a caller
# who believes they widened the scan and did not is worse off than one
# who got an error.
#
# Deliberately a list rather than "the entries with an empty
# invocation": `net probe` has a non-empty invocation (it needs a
# target) and still rejects -A, since its vantage point is wherever
# lookout runs rather than a namespace.
# Commands whose *valid invocation* needs more than a bare kind
# cluster. Everything absent from this map is T0. Note that the
# cloud/GKE commands are NOT here: on kind they are expected to exit 0
# with an `unavailable=` note, and asserting that graceful degradation
# is precisely a T0 case (docs/testing/cli-uat.md § Environment tiers).
# Only a command that cannot produce a well-formed answer at all
# belongs here.
declare -A UAT_COMMAND_TIER=(
  ["triage top"]="T1" # reads metrics.k8s.io; exit 1 without metrics-server
)

# Filled in at fixture time, not here: a command whose invocation needs
# something the cluster may or may not have (an open finding to ack).
# Skipping with the reason is the honest answer — the alternative is a
# fabricated argument that turns "this environment can't run the check"
# into "this command is broken".
declare -A UAT_COMMAND_SKIP=()

UAT_SCOPE_REJECTERS=(
  "audit cluster"   # reads the provider's cluster record
  "audit upgrades"  # ditto
  "cloud ipspace"   # reads the cloud project
  "cloud orphans"
  "cloud quota"
  "cloud stockout"
  "perf probe"      # reads control-plane metrics
  "state webhooks"  # webhook configurations are cluster-scoped
  "net probe"       # probes from where lookout runs
)

# ---- fixtures the invocations above refer to ------------------------------

uat_contract_fixtures() {
  UAT_WORKLOAD="Deployment/$DEMO_NS/web"
  UAT_NODE="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  UAT_STORE="$UAT_WORKDIR/contract.db"
  UAT_REPORT="$UAT_WORKDIR/contract-report.logfmt"
  UAT_EXEMPTIONS="$UAT_WORKDIR/contract-exemptions.yaml"

  # The report `findings diff` consumes is produced by the pipeline the
  # command documents — `lookout health | lookout findings diff` — so
  # this fixture is itself a small integration test of that contract.
  run_lookout health -A >"$UAT_REPORT" 2>/dev/null || true

  cat >"$UAT_EXEMPTIONS" <<'YAML'
exemptions:
  - kind: audit.no_pdb
    namespace: lookout-demo
    name: web
    reason: "UAT fixture — asserts the exemption file parses and annotates"
    expires: "2099-01-01"
YAML

  # `findings ack` can only be exercised against a subject that is
  # actually open in the store, so take one from a real diff. A
  # synthetic key is NOT a substitute: the command correctly refuses a
  # subject it has no row for, so a made-up one tests the error path
  # while the table claims it should exit 0. Whether the cluster has an
  # open finding is a property of the environment — a healthy cluster
  # legitimately has none — so when there is no subject, say so and
  # skip rather than manufacture one. #175's fixtures will guarantee it.
  UAT_SUBJECT="$(run_lookout findings diff --store="$UAT_STORE" --report="$UAT_REPORT" --cluster=uat 2>/dev/null |
    grep -Eo 'subject_key=[^ ]+' | head -n1 | cut -d= -f2- | tr -d '"')"
  if [[ -z "$UAT_SUBJECT" ]]; then
    UAT_SUBJECT="uat/$DEMO_NS/Deployment/web/NoOpenFinding"
    UAT_COMMAND_SKIP["findings ack"]="no open finding in the store to ack — the cluster is healthy (needs a fixture, #175)"
  fi

  # The store as it stands right now, before anything acks or re-diffs
  # it. `findings diff` persists what it reports and `findings ack`
  # closes a row, so these commands do not answer the same question
  # twice — a second identical call legitimately reports nothing new.
  # Any check that runs one of them more than once and compares has to
  # rewind first, or it is comparing a full store to a drained one.
  uat_store_snapshot
}

# uat_store_snapshot / uat_store_rewind — the SQLite store plus its
# write-ahead sidecars, saved and put back. The sidecars matter: a
# copied .db without them can be an older state than the one just
# written.
uat_store_snapshot() {
  local f
  for f in "$UAT_STORE" "$UAT_STORE-wal" "$UAT_STORE-shm"; do
    [[ -f "$f" ]] && cp -p "$f" "$UAT_WORKDIR/golden.$(basename "$f")"
  done
  return 0
}

uat_store_rewind() {
  local f base
  rm -f "$UAT_STORE" "$UAT_STORE-wal" "$UAT_STORE-shm"
  for f in "$UAT_WORKDIR"/golden.*; do
    [[ -f "$f" ]] || continue
    base="$(basename "$f")"
    cp -p "$f" "$UAT_WORKDIR/${base#golden.}"
  done
  return 0
}
