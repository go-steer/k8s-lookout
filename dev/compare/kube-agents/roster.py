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

"""roster.py -- extract kube-agents' audit check roster from its own code.

The authoritative list is `AUDITS` in
agents/platform/skills/fleet-audit/scripts/audit_report.py: one entry per
cron-scheduled governance stream, each with its SOP file and its check
slugs in SOP heading order. This script imports that module (stdlib-only)
and, for each slug, records:

  stream        the audit id (`obtainability-audit`, ...)
  title         the cron job's human name
  sop           the SOP file name under agents/platform/governance/
  section       the `#### N.N ... (`slug`)` heading in that SOP, if found
  collector     the script whose source names the slug literally, so the
                check is mechanical (collected by code), else null (prose:
                the agent runs the SOP's recipe by hand)
  needs_triage  collect.py's TRIAGE_BY_SLUG marker, when present
  scope         `cluster` / `project` / `subnet` from AuditSpec.scopes
  declarable    whether a GitOps declaration may move the finding aside

It also measures the fixed reading the cron prompt mandates per stream
(SOP bytes + fleet-audit SKILL.md + its references), which is what an
agent pays before a single cluster is read.

Usage: roster.py --kube-agents-dir DIR --out roster.json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path

HEADING_RE = re.compile(r"^#{2,5}\s+(?P<num>\d+\.\d+)\s+(?P<title>.*?)\s*\(`(?P<slugs>[^`]+)`(?:,\s*`([^`]+)`)*\)?\s*$")
SLUG_RE = re.compile(r"`([a-z0-9-]+)`")

COLLECTORS = {
    "obtainability-audit": "agents/platform/skills/fleet-audit/scripts/collect.py",
    "compliance-audit": "agents/platform/skills/fleet-audit/scripts/collect.py",
    "ai-security-audit": "agents/platform/skills/fleet-audit/scripts/collect.py",
    "fleet-consistency-drift": "agents/platform/skills/fleet-audit/scripts/fleet_drift.py",
    "security-patch-orchestrator": "agents/platform/skills/fleet-audit/scripts/patch_readiness.py",
    "stockout-prevention": "agents/platform/skills/fleet-audit/scripts/fleet_stockout.py",
    "fleet-wide-cost-analysis": "agents/platform/skills/fleet-audit/scripts/fleet_waste.py",
    "gcp-networking-fabric-audit": "agents/platform/skills/gcp-networking-fabric-audit/scripts/networking_audit.py",
    "gce-compute-fleet-audit": "agents/platform/skills/gce-compute-fleet-audit/scripts/compute_fleet_audit.py",
}


def load_module(path: Path, name: str):
    """Import a kube-agents script by path without running its main()."""
    scripts_dir = str(path.parent)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def sop_sections(sop_path: Path) -> dict[str, dict]:
    """slug -> {section, heading} from the SOP's `#### N.N title (`slug`)` lines."""
    out: dict[str, dict] = {}
    if not sop_path.exists():
        return out
    for line in sop_path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("#"):
            continue
        m = re.match(r"^#{2,5}\s+(\d+\.\d+)\s+(.*)$", line)
        if not m:
            continue
        num, rest = m.group(1), m.group(2)
        slugs = SLUG_RE.findall(rest)
        for slug in slugs:
            out.setdefault(slug, {"section": num, "heading": rest.strip()})
    return out


def cron_jobs(ka: Path) -> dict[str, dict]:
    jobs = json.loads((ka / "agents/platform/cron/jobs.json").read_text(encoding="utf-8"))
    if isinstance(jobs, dict):
        jobs = jobs.get("jobs", [])
    return {j.get("id"): j for j in jobs}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--kube-agents-dir", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    ka = Path(args.kube_agents_dir)

    scripts = ka / "agents/platform/skills/fleet-audit/scripts"
    audit_report = load_module(scripts / "audit_report.py", "ka_audit_report")
    collect = load_module(scripts / "collect.py", "ka_collect")

    jobs = cron_jobs(ka)
    collector_sources = {
        p: (ka / p).read_text(encoding="utf-8") if (ka / p).exists() else ""
        for p in set(COLLECTORS.values())
    }
    collect_slugs = {
        spec.slug: audit_id
        for audit_id, table in collect.CHECK_TABLES.items()
        for spec in table
    }
    triage = getattr(collect, "TRIAGE_BY_SLUG", {})

    skill_dir = ka / "agents/platform/skills/fleet-audit"
    skill_bytes = (skill_dir / "SKILL.md").stat().st_size
    reference_bytes = sum(p.stat().st_size for p in (skill_dir / "references").glob("*.md"))

    checks: list[dict] = []
    streams: dict[str, dict] = {}
    for audit_id, spec in audit_report.AUDITS.items():
        sop_path = ka / "agents/platform/governance" / spec.sop
        sections = sop_sections(sop_path)
        scopes: dict[str, list[str]] = {}
        for kind, slugs in spec.scopes:
            for s in slugs:
                scopes.setdefault(s, []).append(kind)
        job = jobs.get(audit_id, {})
        schedule = (job.get("schedule") or {}).get("display") or (job.get("schedule") or {}).get("expr")
        collector_path = COLLECTORS.get(audit_id)
        source = collector_sources.get(collector_path or "", "")
        streams[audit_id] = {
            "title": spec.title,
            "sop": spec.sop,
            "sop_bytes": sop_path.stat().st_size if sop_path.exists() else 0,
            "sop_lines": len(sop_path.read_text(encoding="utf-8").splitlines()) if sop_path.exists() else 0,
            "schedule": schedule,
            "cron_prompt_bytes": len(job.get("prompt") or ""),
            "collector": collector_path,
            "checks": list(spec.checks),
            "derived": list(spec.derived),
            # The reading the cron prompt mandates before anything runs:
            # the SOP in full ("all N lines of it"), the skill, its references.
            "mandated_read_bytes": (sop_path.stat().st_size if sop_path.exists() else 0) + skill_bytes + reference_bytes,
        }
        for slug in spec.checks:
            mechanical = collector_path is not None and f'"{slug}"' in source
            if slug in collect_slugs:
                mechanical = True
            checks.append(
                {
                    "slug": slug,
                    "stream": audit_id,
                    "title": spec.title,
                    "sop": spec.sop,
                    "section": sections.get(slug, {}).get("section"),
                    "heading": sections.get(slug, {}).get("heading"),
                    "collector": collector_path if mechanical else None,
                    "needs_triage": triage.get(slug),
                    "scope": scopes.get(slug, ["cluster"]),
                    "declarable": slug in spec.declarable,
                }
            )

    out = {
        "kube_agents_dir": str(ka),
        "skill_bytes": skill_bytes,
        "reference_bytes": reference_bytes,
        "streams": streams,
        "checks": checks,
        "totals": {
            "streams": len(streams),
            "checks": len(checks),
            "mechanical": sum(1 for c in checks if c["collector"]),
            "prose": sum(1 for c in checks if not c["collector"]),
        },
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    t = out["totals"]
    print(f"roster: streams={t['streams']} checks={t['checks']} mechanical={t['mechanical']} prose={t['prose']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
