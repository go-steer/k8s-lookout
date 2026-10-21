#!/usr/bin/env python3
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

"""findings-adapter.py -- Arm 3: how much of kube-agents' findings document
is a deterministic function of the finding?

kube-agents' audit pipeline is: collector manifest -> `draft` (skeleton with
ids, scope, evidence) -> the model writes title/impact/recommendation/
remediation for every finding -> `finish` validates and publishes. This
script replays that pipeline with no model:

  --source lookout     lookout's --format json records stand in for the
                       collector: each record whose kind maps (via
                       equivalence.yaml) to a check of the stream becomes a
                       manifest candidate
  --source collector   the real collect.py manifest the harness captured
  --source document    an existing findings document (e.g. one an agent
                       authored on the install), validated as-is

then runs kube-agents' own `draft_findings` and fills the model's fields
from templates.yaml, and runs kube-agents' own `validate_findings` -- the
same code `finish` calls -- recording per finding whether the document
would be accepted and, if not, which field failed and why.

It measures validity, not quality: a templated rationale that validates is
not thereby a good rationale.

Usage: findings-adapter.py --kube-agents-dir DIR --out RUN_DIR --audit ID
          --source lookout|collector|document [--document FILE]
          [--include-system-namespaces]
Writes RUN_DIR/arm3/<audit>.<source>.findings.json and .result.json.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from crosswalk import (  # noqa: E402
    LOOKOUT_BY_STREAM,
    ka_system_namespace,
    load_lookout,
    pod_owner_name,
)

SEVERITY_MAP = {"critical": "critical", "error": "critical", "warning": "major", "warn": "major", "info": "minor", "notice": "minor"}
EXCERPT_MAX = 400


def load_audit_report(ka: Path):
    scripts = ka / "agents/platform/skills/fleet-audit/scripts"
    os.environ.setdefault("FLEET_AUDIT_SCRATCH_DIR", "/tmp")
    for p in (str(scripts), str(ka / "agents/platform/scripts")):
        if p not in sys.path:
            sys.path.insert(0, p)
    spec = importlib.util.spec_from_file_location("ka_audit_report", scripts / "audit_report.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def inverse_equivalence(eq: dict, roster: list[str]) -> dict[tuple[str, str | None], list[str]]:
    """(lookout kind, reason-or-None) -> [check slugs of this audit]."""
    inv: dict[tuple[str, str | None], list[str]] = defaultdict(list)
    for slug in roster:
        e = eq.get(slug) or {}
        for kind in e.get("kinds") or []:
            reasons = e.get("reasons")
            if reasons:
                for r in reasons:
                    inv[(kind, r)].append(slug)
            else:
                inv[(kind, None)].append(slug)
    return inv


def lookout_manifest(run: Path, audit: str, eq: dict, roster: list[str], include_system: bool) -> dict:
    """A collect.py-shaped manifest built from lookout's JSON records."""
    clusters_raw = load_lookout(run)
    inv = inverse_equivalence(eq, roster)
    # `scan` drills into degraded workloads and is the cluster-wide source of
    # the edge.* kinds (service-selects-nothing); it answers every stream.
    stems = list(dict.fromkeys(LOOKOUT_BY_STREAM.get(audit, []) + ["scan"]))
    kcs = {}
    for kc in sorted((run / "ka-home" / ".kubeconfigs").glob("kubeconfig_*.yaml")):
        base = kc.stem[len("kubeconfig_"):]
        project, rest = base.split("_", 1)
        cluster, location = rest.rsplit("_", 1)
        kcs[f"{project}_{location}_{cluster}"] = str(kc)
    entries = []
    for slug_dir, c in clusters_raw.items():
        target = c["target"]
        project, location, name = target.split("/")
        cmds = c["commands"]
        reachable = cmds.get("audit-workloads", {}).get("rc") == 0
        if not reachable:
            entries.append({"name": target, "project": project, "location": location, "outcome": "unreachable",
                            "error": (cmds.get("audit-workloads", {}).get("stderr_tail") or "no lookout read succeeded")[:200]})
            continue
        # Which lookout command answers which check, for checks_run / evidence.
        kind_to_stem: dict[str, str] = {}
        for stem in stems:
            for r in cmds.get(stem, {}).get("records", []):
                kind_to_stem.setdefault(r.get("kind"), stem)
        commands = []
        seen_checks: set[str] = set()
        for (kind, _reason), slugs in inv.items():
            stem = kind_to_stem.get(kind) or next((s for s in stems if cmds.get(s, {}).get("rc") == 0), None)
            if not stem or cmds.get(stem, {}).get("rc") != 0:
                continue
            for slug in slugs:
                if slug in seen_checks:
                    continue
                seen_checks.add(slug)
                commands.append({"check": slug, "command": f"lookout {stem.replace('-', ' ', 1)} -A --format json --kubeconfig {kcs.get(slug_dir, '<kubeconfig>')}", "rc": 0})
        candidates = []
        seen_objects: set[tuple] = set()
        for stem in stems:
            cmd = cmds.get(stem)
            if not cmd:
                continue
            for r in cmd["records"]:
                ns = str(r.get("namespace") or "")
                if ns and not include_system and ka_system_namespace(ns):
                    continue
                slugs = list(dict.fromkeys(inv.get((r.get("kind"), r.get("reason")), []) + inv.get((r.get("kind"), None), [])))
                if not slugs:
                    continue
                kind_of = str(r.get("kind_of_object") or "")
                obj_name = str(r.get("name") or "")
                for slug in slugs:
                    e = eq.get(slug) or {}
                    objects: list[str] = []
                    if e.get("objects_from") and r.get(e["objects_from"]):
                        objects = [o.strip() for o in str(r[e["objects_from"]]).split(",") if o.strip()]
                    elif e.get("lookout_names") == "pods" and obj_name:
                        objects = [f"Workload/{pod_owner_name(obj_name)}"]
                    elif kind_of and obj_name:
                        objects = [f"{kind_of}/{obj_name}"]
                    elif e.get("match") in ("cluster", "project"):
                        objects = [f"Cluster/{name}"]
                    for obj in objects:
                        key = (slug, ns, obj)
                        if key in seen_objects:
                            continue
                        seen_objects.add(key)
                        candidates.append({
                            "check": slug,
                            "cluster": target,
                            "namespace": ns,
                            "object": obj,
                            "severity": SEVERITY_MAP.get(str(r.get("severity") or "").lower(), "minor"),
                            "excerpt": (str(r.get("message") or "")[:EXCERPT_MAX]) or f"{r.get('kind')} reason={r.get('reason')}",
                            "impact": "",
                            "command": f"lookout {stem.replace('-', ' ', 1)} -A --format json --kubeconfig {kcs.get(slug_dir, '<kubeconfig>')}",
                            "_reason": r.get("reason"),
                            "_kind": r.get("kind"),
                        })
        entries.append({"name": target, "project": project, "location": location, "outcome": "collected",
                        "commands": commands, "candidates": candidates})
    return {"version": 1, "audit": audit, "source": "lookout", "clusters": entries}


def collector_manifest(run: Path, audit: str) -> dict:
    text = (run / "ka" / f"{audit}.out").read_text(encoding="utf-8")
    return json.loads(text[text.find("{"):])


def fill(doc: dict, templates: dict, audit: str) -> dict:
    """Fill the model's fields from templates; return per-finding notes."""
    notes: dict[str, Counter] = defaultdict(Counter)
    for f in doc["findings"]:
        slug = f["check"]
        t = templates.get(slug)
        if not t:
            notes[slug]["no_template"] += 1
            continue
        cluster = f.get("cluster") or ""
        parts = cluster.split("/")
        project, location, cluster_name = (parts + ["", "", ""])[:3] if len(parts) == 3 else ("", "", cluster)
        obj = f.get("object") or ""
        kind, _, name = obj.partition("/")
        ctx = {
            "object": obj, "kind": kind, "name": name or obj, "namespace": f.get("namespace") or "(cluster)",
            "cluster": cluster, "cluster_name": cluster_name, "project": project, "location": location,
            "excerpt": (f.get("evidence", {}).get("excerpt") or "").replace("\n", " ")[:EXCERPT_MAX],
            "reason": f.get("_reason") or "",
        }

        def render(s: str) -> str:
            try:
                return s.format(**ctx)
            except (KeyError, IndexError):
                return s

        if t.get("title"):
            f["title"] = render(t["title"])
        if not f.get("impact") and t.get("impact"):
            f["impact"] = render(t["impact"])
            notes[slug]["impact_from_template"] += 1
        f["recommendation"] = {k: render(t.get(k, "")) for k in ("action", "rationale", "risk")}
        rem = t.get("remediation") or {"kind": "manual", "note": ""}
        f["remediation"] = {"kind": rem.get("kind", "manual"), "note": render(rem.get("note", ""))}
        if rem.get("kind") == "manifest":
            notes[slug]["manifest_needs_repo_path"] += 1
    return {k: dict(v) for k, v in notes.items()}


def validate_each(ar, doc: dict, audit: str) -> dict:
    """Validate the whole document; on rejection, bisect to per-finding verdicts."""
    import copy

    result = {"whole_document_ok": False, "whole_document_error": None, "findings": len(doc["findings"]), "accepted": 0, "rejected": 0, "errors": Counter(), "rejected_examples": []}
    try:
        ar.validate_findings(copy.deepcopy(doc), audit)
        result["whole_document_ok"] = True
        result["accepted"] = len(doc["findings"])
        return result
    except ar.ValidationError as exc:
        result["whole_document_error"] = str(exc)[:300]
    # Per-finding: the scope alone must validate; then each finding singly.
    base = {"audit": doc["audit"], "scope": doc["scope"], "findings": []}
    try:
        ar.validate_findings(copy.deepcopy(base), audit)
    except ar.ValidationError as exc:
        result["scope_error"] = str(exc)[:300]
    for f in doc["findings"]:
        single = {"audit": doc["audit"], "scope": doc["scope"], "findings": [copy.deepcopy(f)]}
        try:
            ar.validate_findings(single, audit)
            result["accepted"] += 1
        except ar.ValidationError as exc:
            result["rejected"] += 1
            msg = str(exc)
            field = re.sub(r"findings\[\d+\]\.?", "", msg.split(":")[0]).strip() or "document"
            result["errors"][field] += 1
            if len(result["rejected_examples"]) < 8:
                result["rejected_examples"].append({"check": f.get("check"), "object": f.get("object"), "error": msg[:220]})
    result["errors"] = dict(result["errors"])
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--kube-agents-dir", required=True)
    ap.add_argument("--out", required=True, help="a comparison run directory (roster.json, ka/, lookout/)")
    ap.add_argument("--audit", required=True)
    ap.add_argument("--source", choices=["lookout", "collector", "document"], required=True)
    ap.add_argument("--document", help="findings document to validate (source=document)")
    ap.add_argument("--templates", default=str(HERE / "templates.yaml"))
    ap.add_argument("--equivalence", default=str(HERE / "equivalence.yaml"))
    ap.add_argument("--include-system-namespaces", action="store_true")
    ap.add_argument("--allow-lookout-binary", action="store_true",
                    help="add `lookout` to audit_report.INSPECTION_BINARIES, the allow-list of commands the validator "
                         "accepts as evidence that a check read something (a one-line change on kube-agents' side)")
    args = ap.parse_args(argv)

    ka = Path(args.kube_agents_dir)
    run = Path(args.out)
    ar = load_audit_report(ka)
    if args.allow_lookout_binary and hasattr(ar, "INSPECTION_BINARIES") and "lookout" not in ar.INSPECTION_BINARIES:
        ar.INSPECTION_BINARIES = tuple(ar.INSPECTION_BINARIES) + ("lookout",)
    roster = list(ar.audit_checks(args.audit))
    templates = yaml.safe_load(Path(args.templates).read_text(encoding="utf-8")) or {}
    eq = yaml.safe_load(Path(args.equivalence).read_text(encoding="utf-8")) or {}

    t0 = time.monotonic()
    if args.source == "document":
        doc = json.loads(Path(args.document).read_text(encoding="utf-8"))
        manifest = None
        notes = {}
    else:
        manifest = lookout_manifest(run, args.audit, eq, roster, args.include_system_namespaces) if args.source == "lookout" else collector_manifest(run, args.audit)
        doc = ar.draft_findings(manifest, args.audit)
        notes = fill(doc, templates, args.audit)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    verdict = validate_each(ar, doc, args.audit)
    elapsed_total_ms = int((time.monotonic() - t0) * 1000)

    by_check = Counter(f["check"] for f in doc["findings"])
    out = {
        "audit": args.audit,
        "source": args.source,
        "allow_lookout_binary": args.allow_lookout_binary,
        "clusters_in_scope": len(doc["scope"]["clusters"]),
        "clusters_skipped": len(doc["scope"].get("skipped") or []),
        "checks_with_findings": len(by_check),
        "roster": len(roster),
        "checks_run_coverage": sorted({c["check"] for cl in doc["scope"]["clusters"] for c in cl.get("checks_run", [])}),
        "findings_by_check": dict(by_check),
        "build_ms": elapsed_ms,
        "build_and_validate_ms": elapsed_total_ms,
        "template_notes": notes,
        "distinct": {
            "actions": len({f["recommendation"]["action"] for f in doc["findings"] if isinstance(f.get("recommendation"), dict)}),
            "rationales": len({f["recommendation"]["rationale"] for f in doc["findings"] if isinstance(f.get("recommendation"), dict)}),
            "risks": len({f["recommendation"]["risk"] for f in doc["findings"] if isinstance(f.get("recommendation"), dict)}),
            "remediation_kinds": dict(Counter(f.get("remediation", {}).get("kind") for f in doc["findings"])),
        },
        "validation": verdict,
    }
    arm3 = run / "arm3"
    arm3.mkdir(exist_ok=True)
    stem = f"{args.audit}.{args.source}" + ("+allowlist" if args.allow_lookout_binary else "")
    for f in doc["findings"]:
        f.pop("_reason", None)
        f.pop("_kind", None)
    (arm3 / f"{stem}.findings.json").write_text(json.dumps(doc, indent=1) + "\n", encoding="utf-8")
    if manifest is not None and args.source == "lookout":
        (arm3 / f"{stem}.manifest.json").write_text(json.dumps(manifest, indent=1) + "\n", encoding="utf-8")
    (arm3 / f"{stem}.result.json").write_text(json.dumps(out, indent=1) + "\n", encoding="utf-8")
    v = verdict
    print(f"{stem}: clusters={out['clusters_in_scope']} findings={v['findings']} accepted={v['accepted']} rejected={v['rejected']} whole_doc_ok={v['whole_document_ok']} build={elapsed_ms}ms total={elapsed_total_ms}ms checks={len(by_check)}/{len(roster)} distinct_rationales={out['distinct']['rationales']}")
    if v["whole_document_error"]:
        print("  first error:", v["whole_document_error"])
    if v["errors"]:
        print("  errors by field:", v["errors"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
