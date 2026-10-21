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

"""crosswalk.py -- join what kube-agents and lookout each found, and what
each cost, from one comparison run's output directory.

Reads (all under --out):
  roster.json                      roster.py's check roster
  ka/<stream>.out|.ms|.time|.rc    collector manifests + timings
  trace/*.jsonl                    every kubectl/gcloud call the shims saw
  lookout/<cluster>/<cmd>.out|...  lookout JSON records + timings
  lookout/_fleet/scan-parallel.ms  whole-fleet scan, all clusters at once
  prose/<cluster>/inventory.json   startup-inventory SOP recipe hits
  watchers/...                     stall_report / upgrade readiness

Writes (under --out/report):
  findings.json      every normalised finding from both sides
  crosswalk.md       per check: counts, matched, only-ka, only-lookout
  timing.md          per stream / per cluster cost tables
  cost.md            bytes an agent must read to reach the findings
  summary.json       the numbers the report quotes

Usage: crosswalk.py --out DIR [--equivalence equivalence.yaml] [--selftest]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent

# lookout sub-commands grouped by the kube-agents stream they answer, for
# the per-stream timing comparison. A command may serve two streams.
LOOKOUT_BY_STREAM = {
    "obtainability-audit": ["audit-workloads", "state-volumes", "stab-drain", "triage-top", "leeway-report"],
    "compliance-audit": ["audit-hardening", "audit-rbac", "audit-netpol", "audit-cluster", "state-wi"],
    "ai-security-audit": [],
    "security-patch-orchestrator": ["audit-upgrades", "audit-cluster", "stab-drain", "state-webhooks"],
    "fleet-consistency-drift": ["audit-cluster", "audit-upgrades", "stab-drift"],
    "stockout-prevention": ["cloud-stockout", "cloud-quota", "audit-cluster"],
    "fleet-wide-cost-analysis": ["cloud-orphans", "stab-scaledown", "triage-top", "state-volumes", "state-storage"],
    "gcp-networking-fabric-audit": ["cloud-ipspace", "state-gateway"],
    "gce-compute-fleet-audit": [],
    "startup-inventory": ["health", "scan", "triage-list", "audit-hardening", "state-keda", "state-storage"],
    "stall-watch": ["triage-delta"],
    "upgrade-readiness-watch": ["audit-upgrades", "stab-drain", "state-webhooks"],
}

PROJECT_SCOPED = {"cloud-orphans", "cloud-quota"}

SYSTEM_KIND_WORDS = ("scan.", "health.", "inventory.", "triage.", "log.", "spec.", "bundle.", "graph.", "findings.", "demo.")

# collect.py's namespace exclusion (SYSTEM_NAMESPACES + prefixes), mirrored
# so a lookout-only hit in a namespace kube-agents never reads is reported
# as scope, not as detection.
KA_SYSTEM_NAMESPACES = {
    "kube-system", "kube-public", "kube-node-lease", "gmp-system", "gmp-public", "gke-gmp-system",
    "cnrm-system", "configconnector-operator-system", "krmapihosting-system", "istio-system",
    "asm-system", "anthos-identity-service", "gatekeeper-system", "composer-system",
}


def ka_system_namespace(ns: str) -> bool:
    return ns in KA_SYSTEM_NAMESPACES or ns.startswith("gke-") or ns.startswith("config-management-")


# ----------------------------------------------------------------- helpers

def read(path: Path, default=None):
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return default


def read_int(path: Path) -> int | None:
    t = read(path)
    if t is None:
        return None
    try:
        return int(t.strip())
    except ValueError:
        return None


def parse_time_v(text: str | None) -> dict:
    """/usr/bin/time -v output -> {user_s, sys_s, max_rss_kb, wall_s}."""
    out: dict = {}
    if not text:
        return out
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("User time (seconds):"):
            out["user_s"] = float(line.rsplit(":", 1)[1])
        elif line.startswith("System time (seconds):"):
            out["sys_s"] = float(line.rsplit(":", 1)[1])
        elif line.startswith("Maximum resident set size (kbytes):"):
            out["max_rss_kb"] = int(line.rsplit(":", 1)[1])
        elif line.startswith("Elapsed (wall clock) time"):
            hms = line.rsplit(": ", 1)[1].split(":")
            secs = 0.0
            for part in hms:
                secs = secs * 60 + float(part)
            out["wall_s"] = secs
    return out


def trace_stats(path: Path) -> dict:
    """One shim trace file -> call counts, latency and bytes by tool."""
    stats = {"calls": 0, "kubectl": 0, "gcloud": 0, "ms": 0, "stdout_bytes": 0, "stderr_bytes": 0, "failed": 0, "verbs": Counter()}
    text = read(path)
    if not text:
        return stats
    for line in text.splitlines():
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        stats["calls"] += 1
        stats[rec.get("tool", "?")] = stats.get(rec.get("tool", "?"), 0) + 1
        stats["ms"] += int(rec.get("ms", 0))
        stats["stdout_bytes"] += int(rec.get("stdout_bytes", 0))
        stats["stderr_bytes"] += int(rec.get("stderr_bytes", 0))
        if rec.get("rc", 0) != 0:
            stats["failed"] += 1
        argv = rec.get("argv") or []
        verb = " ".join([rec.get("tool", "?")] + [a for a in argv[:3] if not a.startswith("-")])
        stats["verbs"][verb] += 1
    return stats


def fmt_bytes(n: int | float | None) -> str:
    if n is None:
        return "—"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


def fmt_ms(ms: int | float | None) -> str:
    if ms is None:
        return "—"
    if ms < 1000:
        return f"{ms:.0f} ms"
    if ms < 60000:
        return f"{ms / 1000:.1f} s"
    return f"{ms / 60000:.1f} min"


def tokens(n_bytes: int | None) -> str:
    """A stated estimate: ~4 bytes per token for JSON/English."""
    if n_bytes is None:
        return "—"
    return f"~{n_bytes / 4 / 1000:.1f}k"


# ----------------------------------------------------------- kube-agents side

def ka_cluster_key(name: str) -> str:
    """`<project>/<location>/<cluster>` -> `<project>_<location>_<cluster>` (the lookout dir slug)."""
    return name.replace("/", "_")


def walk_candidates(node, out: list, cluster: str | None = None, outcome: str | None = None):
    """Collect every `{check, object}` dict in a manifest, with the cluster
    or target entry that holds it. Manifests differ in shape across the
    collectors (clusters[], targets[], project entries); the candidate
    record itself is uniform."""
    if isinstance(node, dict):
        here_cluster = node.get("name") if ("outcome" in node or "candidates" in node) and isinstance(node.get("name"), str) else cluster
        here_outcome = node.get("outcome", outcome) if isinstance(node, dict) else outcome
        if "check" in node and ("object" in node or "target" in node):
            rec = dict(node)
            rec["_cluster"] = node.get("cluster") or here_cluster or ""
            out.append(rec)
            return
        for v in node.values():
            walk_candidates(v, out, here_cluster, here_outcome)
    elif isinstance(node, list):
        for v in node:
            walk_candidates(v, out, cluster, outcome)


def walk_targets(node, out: list):
    """Every manifest entry with an `outcome` (a cluster, project or subnet row)."""
    if isinstance(node, dict):
        if "outcome" in node and isinstance(node.get("name"), str):
            out.append({k: node.get(k) for k in ("name", "outcome", "error", "autopilot", "project", "location") if k in node})
        for v in node.values():
            walk_targets(v, out)
    elif isinstance(node, list):
        for v in node:
            walk_targets(v, out)


def load_ka(out: Path, roster: dict) -> dict:
    streams: dict = {}
    for stream in list(roster["streams"]) + ["upgrade-retrospective"]:
        base = out / "ka" / stream
        text = read(base.with_suffix(".out"))
        rec = {
            "stream": stream,
            "rc": read_int(base.with_suffix(".rc")),
            "ms": read_int(base.with_suffix(".ms")),
            "time": parse_time_v(read(base.with_suffix(".time"))),
            "manifest_bytes": len(text.encode("utf-8")) if text else 0,
            "trace": trace_stats(out / "trace" / f"ka-{stream}.jsonl"),
            "candidates": [],
            "targets": [],
            "error": None,
            "stderr_tail": (read(base.with_suffix(".err")) or "")[-600:],
        }
        manifest = None
        if text:
            # `--out` not used: stdout carries the manifest, possibly after
            # a summary line; take the first `{` onwards.
            i = text.find("{")
            if i >= 0:
                try:
                    manifest = json.loads(text[i:])
                except json.JSONDecodeError as exc:
                    rec["error"] = f"manifest unparseable: {exc}"
        if manifest is not None:
            rec["error"] = manifest.get("error") or rec["error"]
            rec["summary"] = {k: manifest.get(k) for k in ("audit", "checks_run", "candidate_count", "cluster_count") if k in manifest}
            walk_candidates(manifest, rec["candidates"])
            walk_targets(manifest, rec["targets"])
            rec["checks_run"] = manifest.get("checks_run") or manifest.get("checks") or []
        streams[stream] = rec
    return streams


# --------------------------------------------------------------- lookout side

def load_lookout(out: Path) -> dict:
    clusters: dict = {}
    root = out / "lookout"
    if not root.is_dir():
        return clusters
    for cdir in sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith("_")):
        target = (read(cdir / "target") or cdir.name).strip()
        commands: dict = {}
        for outf in sorted(cdir.glob("*.out")):
            stem = outf.stem
            text = read(outf) or ""
            records, summary = [], None
            for line in text.splitlines():
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "scanned" in obj and "findings" in obj and "kind" not in obj:
                    summary = obj
                elif "kind" in obj:
                    records.append(obj)
            user_bytes = sum(
                len(json.dumps(r, separators=(",", ":")).encode("utf-8")) + 1
                for r in records
                if not ka_system_namespace(str(r.get("namespace") or ""))
            ) + (len(json.dumps(summary).encode("utf-8")) + 1 if summary else 0)
            commands[stem] = {
                "rc": read_int(cdir / f"{stem}.rc"),
                "ms": read_int(cdir / f"{stem}.ms"),
                "time": parse_time_v(read(cdir / f"{stem}.time")),
                "stdout_bytes": len(text.encode("utf-8")),
                # re-serialised, so capped at what was actually written
                "stdout_user_ns_bytes": min(user_bytes, len(text.encode("utf-8"))),
                "records": records,
                "summary": summary,
                "stderr_tail": (read(cdir / f"{stem}.err") or "")[-400:],
            }
        clusters[cdir.name] = {"target": target, "commands": commands}
    return clusters


def load_prose(out: Path) -> dict:
    res: dict = {}
    root = out / "prose"
    if not root.is_dir():
        return res
    for cdir in sorted(p for p in root.iterdir() if p.is_dir()):
        inv = read(cdir / "inventory.json")
        try:
            data = json.loads(inv) if inv else {}
        except json.JSONDecodeError:
            data = {}
        res[cdir.name] = {
            "target": (read(cdir / "target") or cdir.name).strip(),
            "ms": read_int(cdir / "inventory.ms"),
            "candidates": data.get("candidates", []),
            "addons": data.get("addons", {}),
            "raw_bytes": data.get("raw_bytes", {}),
            "trace": trace_stats(out / "trace" / f"prose-{cdir.name}.jsonl"),
        }
    return res


def load_watchers(out: Path) -> dict:
    res: dict = {"clusters": {}, "upgrade_readiness": None}
    root = out / "watchers"
    if not root.is_dir():
        return res
    for cdir in sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith("_")):
        stalls = []
        for f in sorted(cdir.glob("stall-*.out")):
            if f.stem in ("stall-all",):
                continue
            text = read(f) or ""
            try:
                data = json.loads(text) if text.strip().startswith(("{", "[")) else None
            except json.JSONDecodeError:
                data = None
            rows = data if isinstance(data, list) else (data or {}).get("findings") or (data or {}).get("rows") or []
            stalls.append({"namespace": f.stem[len("stall-"):], "rows": len(rows) if isinstance(rows, list) else 0, "rc": read_int(cdir / f"{f.stem}.rc")})
        res["clusters"][cdir.name] = {
            "target": (read(cdir / "target") or cdir.name).strip(),
            "ms": read_int(cdir / "stall-all.ms"),
            "namespaces": read_int(cdir / "stall-namespaces"),
            "stalls": stalls,
            "trace": trace_stats(out / "trace" / f"watchers-{cdir.name}.jsonl"),
        }
    base = root / "_fleet" / "upgrade-readiness"
    if base.with_suffix(".rc").exists():
        text = read(base.with_suffix(".json")) or read(base.with_suffix(".out")) or ""
        try:
            data = json.loads(text) if text.strip().startswith("{") else {}
        except json.JSONDecodeError:
            data = {}
        res["upgrade_readiness"] = {
            "rc": read_int(base.with_suffix(".rc")),
            "ms": read_int(base.with_suffix(".ms")),
            "trace": trace_stats(out / "trace" / "watchers-upgrade-readiness.jsonl"),
            "members": len(data.get("members") or data.get("clusters") or []),
            "bytes": len(text.encode("utf-8")),
        }
    return res


# ----------------------------------------------------------------- the join

def norm_object(kind: str, name: str) -> str:
    return f"{kind}/{name}".lower()


def ka_object(c: dict) -> str:
    obj = str(c.get("object") or c.get("target") or "")
    return obj.lower()


POD_SUFFIX_RS = re.compile(r"-[a-f0-9]{5,10}-[a-z0-9]{5}$")   # Deployment -> ReplicaSet -> Pod
POD_SUFFIX_DS = re.compile(r"-[a-z0-9]{5}$")                  # DaemonSet / bare ReplicaSet pod
POD_SUFFIX_STS = re.compile(r"-\d+$")                         # StatefulSet ordinal
JOB_SUFFIX = re.compile(r"-\d{8,}$")                          # CronJob -> Job


def pod_owner_name(pod: str) -> str:
    """The controller name a pod name was derived from, by suffix shape.
    A heuristic, used only to join `triage top`'s per-pod rows to the
    workload collect.py names; it errs toward joining."""
    for rx in (POD_SUFFIX_RS, POD_SUFFIX_DS, POD_SUFFIX_STS):
        if rx.search(pod):
            base = rx.sub("", pod)
            # A CronJob pod: <cronjob>-<schedule-stamp>-<5>
            return JOB_SUFFIX.sub("", base)
    return pod


def lookout_findings_for(clusters: dict, kinds: list[str], reasons: list[str] | None) -> list[dict]:
    out = []
    for slug, c in clusters.items():
        for stem, cmd in c["commands"].items():
            for r in cmd["records"]:
                if r.get("kind") in kinds and (not reasons or r.get("reason") in reasons):
                    out.append({**r, "_cluster": slug, "_command": stem})
    return out


def crosswalk(roster: dict, equivalence: dict, ka: dict, lookout: dict, prose: dict) -> list[dict]:
    rows = []
    ka_by_check: dict[str, list[dict]] = defaultdict(list)
    for stream, rec in ka.items():
        for c in rec["candidates"]:
            ka_by_check[c["check"]].append({**c, "_stream": stream})
    for slug, p in prose.items():
        for c in p["candidates"]:
            ka_by_check[c["check"]].append({**c, "_cluster": p["target"], "_stream": "startup-inventory"})

    checks = list(roster["checks"]) + [
        {"slug": s, "stream": "startup-inventory", "title": "Single-Cluster Inventory Audit", "sop": "cluster_inventory_audit_sop.md", "section": None, "collector": None}
        for s in ("inventory-no-resourcequota", "inventory-no-limitrange", "inventory-no-startup-probe", "inventory-writable-rootfs", "inventory-added-capabilities")
    ]
    lookout_slugs = set(lookout)

    for chk in checks:
        slug = chk["slug"]
        eq = equivalence.get(slug) or {}
        kinds = eq.get("kinds") or []
        reasons = eq.get("reasons")
        match = eq.get("match", "object")
        objects_from = eq.get("objects_from")     # lookout field listing the objects the finding is about
        names_pods = eq.get("lookout_names") == "pods"
        ka_hits = ka_by_check.get(slug, [])
        lk_hits = lookout_findings_for(lookout, kinds, reasons) if kinds else []

        def key_ka(c):
            cl = ka_cluster_key(str(c.get("_cluster") or ""))
            if match == "cluster":
                return (cl,)
            if match == "project":
                return ("project",)
            if match == "namespace":
                return (cl, str(c.get("namespace") or "").lower())
            obj = ka_object(c)
            if names_pods:
                # join on the owner name alone: collect.py says Deployment/x,
                # lookout says Pod/x-<hash>-<id>; the kind is implied
                obj = obj.split("/", 1)[-1]
            return (cl, str(c.get("namespace") or "").lower(), obj)

        def keys_lk(r):
            cl = r["_cluster"]
            if match == "cluster":
                return [(cl,)]
            if match == "project":
                return [("project",)]
            if match == "namespace":
                return [(cl, str(r.get("namespace") or "").lower())]
            ns = str(r.get("namespace") or "").lower()
            if objects_from and r.get(objects_from):
                return [(cl, ns, o.strip().lower()) for o in str(r[objects_from]).split(",") if o.strip()]
            if names_pods:
                if not r.get("name"):
                    return []  # the per-cluster roll-up row names nothing
                return [(cl, ns, pod_owner_name(str(r.get("name"))).lower())]
            return [(cl, ns, norm_object(str(r.get("kind_of_object") or ""), str(r.get("name") or "")))]

        ka_keys = {key_ka(c) for c in ka_hits}
        lk_keys = {k for r in lk_hits for k in keys_lk(r)}
        matched = ka_keys & lk_keys
        only_ka = ka_keys - lk_keys
        only_lk = lk_keys - ka_keys
        # A lookout-only hit in a namespace collect.py never reads is scope,
        # not detection: split it out so "beyond" means beyond on the same
        # ground.
        only_lk_system = {k for k in only_lk if len(k) >= 2 and ka_system_namespace(k[1])} if match in ("object", "namespace") else set()
        only_lk_user = only_lk - only_lk_system
        # A ka-only hit on a cluster lookout never reached is reachability,
        # not detection.
        unreached = {k for k in only_ka if k and k[0] and k[0] not in lookout_slugs and match != "project"}
        rows.append(
            {
                "slug": slug,
                "stream": chk["stream"],
                "section": chk.get("section"),
                "claim": eq.get("claim", "unmapped"),
                "match": match,
                "kinds": kinds,
                "reasons": reasons or [],
                "note": eq.get("note", ""),
                "needs_triage": chk.get("needs_triage"),
                "ka_count": len(ka_hits),
                "ka_keys": len(ka_keys),
                "lookout_count": len(lk_hits),
                "lookout_keys": len(lk_keys),
                "matched": len(matched),
                "only_ka": len(only_ka - unreached),
                "only_ka_unreached": len(unreached),
                "only_lookout": len(only_lk_user),
                "only_lookout_system_ns": len(only_lk_system),
                "only_ka_examples": sorted("/".join(k) for k in list(only_ka - unreached))[:6],
                "only_lookout_examples": sorted("/".join(k) for k in list(only_lk_user))[:6],
                "ka_severities": dict(Counter(str(c.get("severity")) for c in ka_hits)),
                "lookout_severities": dict(Counter(str(r.get("severity")) for r in lk_hits)),
            }
        )
    return rows


def verdict(row: dict) -> str:
    """Grade the pre-run claim against the live join."""
    if row["match"] == "none":
        return "out-of-scope"
    if not row["kinds"]:
        if row["ka_count"] == 0:
            return "gap (silent here)" if row["claim"] == "gap" else row["claim"] + " (silent here)"
        return "GAP, live" if row["claim"] != "intentional" else "intentional (ka fires)"
    if row["ka_keys"] == 0 and row["lookout_keys"] == 0:
        return "both silent"
    if row["ka_keys"] == 0 and row["lookout_keys"] > 0:
        return "lookout-only (beyond or threshold)"
    if row["matched"] == row["ka_keys"]:
        return "parity" if row["only_lookout"] == 0 else "parity + beyond"
    if row["matched"] > 0:
        return "partial match"
    return "MISS, live" if row["only_ka"] > 0 else "unreached"


# ------------------------------------------------------------------ renderers

def render_crosswalk(rows: list[dict], roster: dict, ka: dict) -> str:
    L = ["# Check-by-check cross-walk (live)", ""]
    L.append("Columns: **ka** = kube-agents candidates (distinct join keys); **lk** = lookout records matching the mapped kinds (distinct keys); "
             "**matched** = same cluster/namespace/object on both sides; **only-ka** / **only-lk** = the remainder in namespaces both read; **lk sys-ns** = lookout-only hits in namespaces collect.py skips (scope, not detection); "
             "**unreached** = ka-only hits on clusters lookout did not run against. `claim` is the Oct-18 source-review expectation; `verdict` is what the live join says.")
    L.append("")
    by_stream: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_stream[r["stream"]].append(r)
    for stream, srows in by_stream.items():
        meta = roster["streams"].get(stream, {})
        title = meta.get("title", stream)
        sched = meta.get("schedule", "")
        karec = ka.get(stream, {})
        L.append(f"## {title} (`{stream}`{', cron ' + sched if sched else ''})")
        L.append("")
        if karec:
            L.append(f"Collector run: rc={karec.get('rc')} wall={fmt_ms(karec.get('ms'))} subprocesses={karec['trace']['calls']} "
                     f"(kubectl {karec['trace'].get('kubectl', 0)}, gcloud {karec['trace'].get('gcloud', 0)}) "
                     f"bytes-read={fmt_bytes(karec['trace']['stdout_bytes'])} manifest={fmt_bytes(karec['manifest_bytes'])} "
                     f"candidates={len(karec['candidates'])}" + (f" **error: {karec['error']}**" if karec.get("error") else ""))
            L.append("")
        L.append("| § | check | claim | ka | lk | matched | only-ka | only-lk | lk sys-ns | unreached | verdict | lookout kinds / note |")
        L.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |")
        for r in srows:
            kinds = ", ".join(f"`{k}`" for k in r["kinds"]) or "—"
            if r["reasons"]:
                kinds += " (" + ", ".join(r["reasons"]) + ")"
            note = f" — {r['note']}" if r["note"] else ""
            L.append(f"| {r['section'] or ''} | `{r['slug']}` | {r['claim']} | {r['ka_keys']} | {r['lookout_keys']} | {r['matched']} | {r['only_ka']} | {r['only_lookout']} | {r['only_lookout_system_ns']} | {r['only_ka_unreached']} | {verdict(r)} | {kinds}{note} |")
        L.append("")
        ex = [r for r in srows if r["only_ka_examples"] or r["only_lookout_examples"]]
        if ex:
            L.append("<details><summary>Examples of unmatched keys</summary>")
            L.append("")
            for r in ex:
                if r["only_ka_examples"]:
                    L.append(f"- `{r['slug']}` only-ka: " + ", ".join(f"`{e}`" for e in r["only_ka_examples"]))
                if r["only_lookout_examples"]:
                    L.append(f"- `{r['slug']}` only-lookout: " + ", ".join(f"`{e}`" for e in r["only_lookout_examples"]))
            L.append("")
            L.append("</details>")
            L.append("")
    return "\n".join(L) + "\n"


def render_timing(ka: dict, lookout: dict, prose: dict, watchers: dict, out: Path) -> str:
    L = ["# Cost: wall time, CPU, subprocesses, bytes", ""]
    L.append("## kube-agents collectors (one run = the whole project, all clusters)")
    L.append("")
    L.append("| stream | rc | wall | cpu (user+sys) | max RSS | subprocesses | kubectl | gcloud | failed calls | bytes read from APIs | manifest | candidates |")
    L.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    tot = Counter()
    for stream, r in ka.items():
        t = r["time"]
        cpu = (t.get("user_s", 0) + t.get("sys_s", 0)) if t else None
        tr = r["trace"]
        L.append(f"| `{stream}` | {r['rc']} | {fmt_ms(r['ms'])} | {cpu:.1f} s | {fmt_bytes((t.get('max_rss_kb') or 0) * 1024)} | {tr['calls']} | {tr.get('kubectl', 0)} | {tr.get('gcloud', 0)} | {tr['failed']} | {fmt_bytes(tr['stdout_bytes'])} | {fmt_bytes(r['manifest_bytes'])} | {len(r['candidates'])} |"
                 if cpu is not None else f"| `{stream}` | {r['rc']} | {fmt_ms(r['ms'])} | — | — | {tr['calls']} | {tr.get('kubectl', 0)} | {tr.get('gcloud', 0)} | {tr['failed']} | {fmt_bytes(tr['stdout_bytes'])} | {fmt_bytes(r['manifest_bytes'])} | {len(r['candidates'])} |")
        tot["ms"] += r["ms"] or 0
        tot["calls"] += tr["calls"]
        tot["bytes"] += tr["stdout_bytes"]
        tot["manifest"] += r["manifest_bytes"]
        tot["cands"] += len(r["candidates"])
    L.append(f"| **total** | | **{fmt_ms(tot['ms'])}** | | | **{tot['calls']}** | | | | **{fmt_bytes(tot['bytes'])}** | **{fmt_bytes(tot['manifest'])}** | **{tot['cands']}** |")
    L.append("")
    # Top subprocess verbs across all collectors
    verbs: Counter = Counter()
    for r in ka.values():
        verbs.update(r["trace"]["verbs"])
    if verbs:
        L.append("<details><summary>Most frequent subprocess shapes (all collectors)</summary>")
        L.append("")
        for verb, n in verbs.most_common(25):
            L.append(f"- {n:>5} × `{verb}`")
        L.append("")
        L.append("</details>")
        L.append("")

    L.append("## lookout (per cluster, sequential; each command is one process)")
    L.append("")
    cmd_names = sorted({stem for c in lookout.values() for stem in c["commands"]})
    L.append("| cluster | " + " | ".join(cmd_names) + " | **sum** | findings |")
    L.append("| --- | " + " | ".join("---:" for _ in cmd_names) + " | ---: | ---: |")
    fleet_sum = 0
    for slug, c in lookout.items():
        cells, s, nf = [], 0, 0
        for stem in cmd_names:
            cmd = c["commands"].get(stem)
            if not cmd:
                cells.append("—")
                continue
            ms = cmd["ms"] or 0
            s += ms
            nf += len([r for r in cmd["records"] if not str(r.get("kind", "")).startswith(SYSTEM_KIND_WORDS)])
            mark = "" if cmd["rc"] == 0 else f" (rc {cmd['rc']})"
            cells.append(fmt_ms(ms) + mark)
        fleet_sum += s
        L.append(f"| `{c['target']}` | " + " | ".join(cells) + f" | **{fmt_ms(s)}** | {nf} |")
    L.append("")
    par = read_int(out / "lookout" / "_fleet" / "scan-parallel.ms")
    L.append(f"- Sum of every lookout command on every cluster, run one after another: **{fmt_ms(fleet_sum)}**.")
    if par is not None:
        L.append(f"- `lookout scan` on every cluster at once (how a sentinel or fleet layer would run it): **{fmt_ms(par)}** wall.")
    # lookout CPU and RSS
    cpu_tot, rss_max = 0.0, 0
    for c in lookout.values():
        for cmd in c["commands"].values():
            t = cmd["time"]
            cpu_tot += t.get("user_s", 0) + t.get("sys_s", 0)
            rss_max = max(rss_max, t.get("max_rss_kb", 0))
    L.append(f"- lookout CPU over all of the above: **{cpu_tot:.1f} s**; largest process RSS: **{fmt_bytes(rss_max * 1024)}**.")
    L.append("")

    # Per-stream: the collector's one fleet run against the lookout commands
    # that answer the same stream. Project-scoped lookout commands (cloud
    # orphans/quota sweep the project, not the cluster) are counted once,
    # as the collector runs them once; `fleet-seq` is every cluster one after
    # another, `fleet-par` is the slowest cluster (all clusters at once).
    L.append("## Per stream: one kube-agents fleet run vs the lookout commands that answer it")
    L.append("")
    L.append("| stream | ka wall | ka subprocesses | ka bytes read | lookout fleet-seq | lookout fleet-par | lookout processes | lookout stdout | lookout stdout, user ns only |")
    L.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    reachable = {slug: c for slug, c in lookout.items() if c["commands"].get("audit-workloads", {}).get("rc") == 0}
    for stream, stems in LOOKOUT_BY_STREAM.items():
        karec = ka.get(stream)
        seq = 0
        per_cluster_max = 0
        procs = 0
        out_bytes = 0
        user_bytes = 0
        project_done: set = set()
        for slug, c in reachable.items():
            this = 0
            for stem in stems:
                cmd = c["commands"].get(stem)
                if not cmd or cmd["rc"] not in (0, 1):
                    continue
                if stem in PROJECT_SCOPED:
                    if stem in project_done:
                        continue
                    project_done.add(stem)
                ms = cmd["ms"] or 0
                this += ms
                procs += 1
                out_bytes += cmd["stdout_bytes"]
                user_bytes += cmd.get("stdout_user_ns_bytes", 0)
            seq += this
            per_cluster_max = max(per_cluster_max, this)
        if karec:
            L.append(f"| `{stream}` | {fmt_ms(karec['ms'])} | {karec['trace']['calls']} | {fmt_bytes(karec['trace']['stdout_bytes'])} | {fmt_ms(seq)} | {fmt_ms(per_cluster_max)} | {procs} | {fmt_bytes(out_bytes)} | {fmt_bytes(user_bytes)} |")
        elif stems:
            L.append(f"| `{stream}` | — | — | — | {fmt_ms(seq)} | {fmt_ms(per_cluster_max)} | {procs} | {fmt_bytes(out_bytes)} | {fmt_bytes(user_bytes)} |")
    L.append("")
    L.append(f"Reachable clusters counted: {len(reachable)} of {len(lookout)}. A lookout command answers more than one stream, so the per-stream figures overlap; the honest fleet total is the per-cluster table above.")
    L.append("")

    if prose:
        L.append("## Startup inventory SOP recipes (kubectl + jq, per cluster)")
        L.append("")
        L.append("| cluster | wall | kubectl calls | bytes read | candidates |")
        L.append("| --- | ---: | ---: | ---: | ---: |")
        for slug, p in prose.items():
            L.append(f"| `{p['target']}` | {fmt_ms(p['ms'])} | {p['trace']['calls']} | {fmt_bytes(p['trace']['stdout_bytes'])} | {len(p['candidates'])} |")
        L.append("")
    if watchers["clusters"]:
        L.append("## Always-on watchers")
        L.append("")
        L.append("| cluster | stall_report.py namespaces | wall | kubectl calls | bytes read | stall rows | lookout `triage delta -A` wall | delta records |")
        L.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for slug, w in watchers["clusters"].items():
            lk = lookout.get(slug, {}).get("commands", {}).get("triage-delta", {})
            rows = sum(s["rows"] for s in w["stalls"])
            L.append(f"| `{w['target']}` | {w['namespaces']} | {fmt_ms(w['ms'])} | {w['trace']['calls']} | {fmt_bytes(w['trace']['stdout_bytes'])} | {rows} | {fmt_ms(lk.get('ms'))} | {len(lk.get('records', []))} |")
        L.append("")
        ur = watchers.get("upgrade_readiness")
        if ur:
            L.append(f"`fleet_upgrade_report.py --readiness` (what upgrade-readiness-watch runs): rc={ur['rc']} wall={fmt_ms(ur['ms'])} subprocesses={ur['trace']['calls']} bytes-read={fmt_bytes(ur['trace']['stdout_bytes'])} report={fmt_bytes(ur['bytes'])}; "
                     f"lookout `audit upgrades` per cluster: " + ", ".join(fmt_ms(c['commands'].get('audit-upgrades', {}).get('ms')) for c in lookout.values()))
            L.append("")
    return "\n".join(L) + "\n"


def render_cost(roster: dict, ka: dict, lookout: dict, prose: dict) -> str:
    L = ["# What an agent must read to reach the findings", ""]
    L.append("kube-agents' cron prompt tells the Platform Agent to read the stream's SOP in full (\"all N lines of it\") plus the fleet-audit skill and its references, then the collector's manifest. "
             "lookout's agent reads the skill's SKILL.md once (optional) and the command's stdout. Tokens are a stated estimate at 4 bytes/token.")
    L.append("")
    L.append("| stream | SOP lines | mandated reading (SOP+skill+refs) | manifest | **ka total** | lookout stdout (all clusters, mapped commands) | same, namespaces collect.py reads | **lookout total (user ns)** |")
    L.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    lk_skill = sum(p.stat().st_size for p in (HERE.parent.parent.parent / "skills").rglob("SKILL.md")) if (HERE.parent.parent.parent / "skills").is_dir() else 0
    grand_ka = grand_lk = 0
    for stream, meta in roster["streams"].items():
        karec = ka.get(stream, {})
        mandated = meta["mandated_read_bytes"]
        manifest = karec.get("manifest_bytes", 0)
        ka_total = mandated + manifest
        lk_bytes = 0
        lk_user = 0
        project_done: set = set()
        for c in lookout.values():
            for stem in LOOKOUT_BY_STREAM.get(stream, []):
                if stem in PROJECT_SCOPED:
                    if stem in project_done:
                        continue
                    project_done.add(stem)
                lk_bytes += c["commands"].get(stem, {}).get("stdout_bytes", 0)
                lk_user += c["commands"].get(stem, {}).get("stdout_user_ns_bytes", 0)
        grand_ka += ka_total
        grand_lk += lk_user
        L.append(f"| `{stream}` | {meta['sop_lines']} | {fmt_bytes(mandated)} ({tokens(mandated)} tok) | {fmt_bytes(manifest)} | **{fmt_bytes(ka_total)} ({tokens(ka_total)} tok)** | {fmt_bytes(lk_bytes)} | {fmt_bytes(lk_user)} | **{fmt_bytes(lk_user)} ({tokens(lk_user)} tok)** |")
    L.append(f"| **all streams** | | | | **{fmt_bytes(grand_ka)} ({tokens(grand_ka)} tok)** | | | **{fmt_bytes(grand_lk)} ({tokens(grand_lk)} tok)** |")
    L.append("")
    L.append(f"lookout's skills, if the agent loads every SKILL.md in `skills/`: {fmt_bytes(lk_skill)} ({tokens(lk_skill)} tok) once, not per stream.")
    L.append("")
    if prose:
        raw = sum(p["trace"]["stdout_bytes"] for p in prose.values())
        L.append(f"Startup inventory SOP: the recipes pull **{fmt_bytes(raw)}** of raw kubectl JSON across the fleet ({tokens(raw)} tok if read as-is; the SOP says \"read the specs\"), against lookout `health`+`scan`+`triage list` stdout of "
                 f"{fmt_bytes(sum(c['commands'].get(s, {}).get('stdout_bytes', 0) for c in lookout.values() for s in ('health', 'scan', 'triage-list')))}.")
        L.append("")
    return "\n".join(L) + "\n"


# -------------------------------------------------------------------- selftest

def selftest() -> int:
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        (out / "ka").mkdir()
        (out / "trace").mkdir()
        (out / "lookout" / "p_us-central1_c1").mkdir(parents=True)
        roster = {"streams": {"obtainability-audit": {"title": "t", "schedule": "x", "sop_lines": 1, "mandated_read_bytes": 10}},
                  "checks": [{"slug": "no-pdb", "stream": "obtainability-audit", "section": "3.3"}, {"slug": "strategy-causes-downtime", "stream": "obtainability-audit", "section": "3.14"}]}
        (out / "roster.json").write_text(json.dumps(roster))
        manifest = {"audit": "obtainability-audit", "clusters": [{"name": "p/us-central1/c1", "outcome": "collected", "candidates": [
            {"check": "no-pdb", "cluster": "p/us-central1/c1", "namespace": "prod", "object": "Deployment/api", "severity": "major"},
            {"check": "no-pdb", "cluster": "p/us-central1/c1", "namespace": "prod", "object": "Deployment/web", "severity": "major"},
            {"check": "strategy-causes-downtime", "cluster": "p/us-central1/c1", "namespace": "prod", "object": "Deployment/web", "severity": "minor"}]}]}
        (out / "ka" / "obtainability-audit.out").write_text(json.dumps(manifest))
        (out / "ka" / "obtainability-audit.ms").write_text("1234\n")
        (out / "ka" / "obtainability-audit.rc").write_text("0\n")
        (out / "trace" / "ka-obtainability-audit.jsonl").write_text('{"tool":"kubectl","argv":["get","x"],"ms":10,"stdout_bytes":100,"stderr_bytes":0,"rc":0}\n')
        cdir = out / "lookout" / "p_us-central1_c1"
        (cdir / "target").write_text("p/us-central1/c1\n")
        (cdir / "audit-workloads.out").write_text(
            '{"kind":"audit.no_pdb","severity":"warning","namespace":"prod","kind_of_object":"Deployment","name":"api","reason":"NoPodDisruptionBudget"}\n'
            '{"kind":"audit.no_pdb","severity":"warning","namespace":"prod","kind_of_object":"Deployment","name":"extra","reason":"NoPodDisruptionBudget"}\n'
            '{"scanned":3,"findings":2,"elapsed":"10ms"}\n')
        (cdir / "audit-workloads.ms").write_text("50\n")
        (cdir / "audit-workloads.rc").write_text("0\n")
        eq = yaml.safe_load((HERE / "equivalence.yaml").read_text())
        ka = load_ka(out, roster)
        lk = load_lookout(out)
        rows = crosswalk(roster, eq, ka, lk, {})
        by = {r["slug"]: r for r in rows}
        assert by["no-pdb"]["matched"] == 1 and by["no-pdb"]["only_ka"] == 1 and by["no-pdb"]["only_lookout"] == 1, by["no-pdb"]
        assert verdict(by["no-pdb"]) == "partial match"
        assert verdict(by["strategy-causes-downtime"]) == "GAP, live"
        assert lk["p_us-central1_c1"]["commands"]["audit-workloads"]["summary"]["findings"] == 2
        assert ka["obtainability-audit"]["trace"]["calls"] == 1
        print("selftest ok")
    return 0


# ------------------------------------------------------------------------ main

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out")
    ap.add_argument("--equivalence", default=str(HERE / "equivalence.yaml"))
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)
    if args.selftest:
        return selftest()
    if not args.out:
        ap.error("--out is required")
    out = Path(args.out)
    roster = json.loads((out / "roster.json").read_text(encoding="utf-8"))
    equivalence = yaml.safe_load(Path(args.equivalence).read_text(encoding="utf-8")) or {}
    ka = load_ka(out, roster)
    lookout = load_lookout(out)
    prose = load_prose(out)
    watchers = load_watchers(out)
    rows = crosswalk(roster, equivalence, ka, lookout, prose)

    rep = out / "report"
    rep.mkdir(exist_ok=True)
    (rep / "crosswalk.md").write_text(render_crosswalk(rows, roster, ka), encoding="utf-8")
    (rep / "timing.md").write_text(render_timing(ka, lookout, prose, watchers, out), encoding="utf-8")
    (rep / "cost.md").write_text(render_cost(roster, ka, lookout, prose), encoding="utf-8")

    verdicts = Counter(verdict(r) for r in rows)
    summary = {
        "clusters_ka": sorted({t["name"] for r in ka.values() for t in r["targets"]}),
        "clusters_lookout": sorted(c["target"] for c in lookout.values()),
        "ka_streams": {s: {"rc": r["rc"], "ms": r["ms"], "calls": r["trace"]["calls"], "bytes": r["trace"]["stdout_bytes"], "candidates": len(r["candidates"]), "error": r["error"], "targets": r["targets"]} for s, r in ka.items()},
        "verdicts": dict(verdicts),
        "rows": rows,
        "lookout_fleet_scan_parallel_ms": read_int(out / "lookout" / "_fleet" / "scan-parallel.ms"),
        "lookout_commands": {slug: {stem: {"rc": c["rc"], "ms": c["ms"], "records": len(c["records"]), "summary": c["summary"], "stderr_tail": c["stderr_tail"]} for stem, c in cl["commands"].items()} for slug, cl in lookout.items()},
        "prose": {k: {"ms": v["ms"], "candidates": len(v["candidates"]), "addons": v["addons"], "calls": v["trace"]["calls"], "bytes": v["trace"]["stdout_bytes"]} for k, v in prose.items()},
        "watchers": watchers,
    }
    (rep / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
    findings = {
        "kube_agents": [{k: v for k, v in c.items()} | {"_stream": s} for s, r in ka.items() for c in r["candidates"]],
        "lookout": [{**rec, "_cluster": slug, "_command": stem} for slug, cl in lookout.items() for stem, c in cl["commands"].items() for rec in c["records"]],
        "prose": [{**c, "_cluster": p["target"]} for p in prose.values() for c in p["candidates"]],
    }
    (rep / "findings.json").write_text(json.dumps(findings, indent=1, default=str) + "\n", encoding="utf-8")
    print(f"crosswalk: checks={len(rows)} verdicts={dict(verdicts)}")
    print(f"wrote {rep}/crosswalk.md timing.md cost.md summary.json findings.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
