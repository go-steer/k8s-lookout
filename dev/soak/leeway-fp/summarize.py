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

"""Summarize a leeway false-positive soak directory.

Every leeway finding is listed once, with what is needed to judge it:
where it reached (a session, the watchboard digest, a storm, or the store),
its tier and suspected cause, the churn in the 30 minutes before it was
first seen, and the workload's per-zone placement at the nearest sample.

Usage: summarize.py <soak dir>
"""

import collections
import datetime as dt
import json
import os
import re
import sys

WINDOW = dt.timedelta(minutes=30)
LEEWAY = re.compile(r"leeway[.:]")


def ts(s):
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y/%m/%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def walk(node, found):
    """Collect every dict whose kind is leeway.*, descending into strings
    that hold JSON — the inject envelope nests the payload as a string.

    A leeway finding swallowed by a storm arrives without its kind: the
    storm's representative_incidents and a storm.member's incident carry
    only reason, name and a uid of the form
    "leeway:<Kind>/<ns>/<name>|<topology key>". Those are collected too,
    with the kind rebuilt as leeway.storm-member and the key from the uid."""
    if isinstance(node, str):
        s = node.strip()
        if s[:1] in "{[" and LEEWAY.search(s):
            try:
                walk(json.loads(s), found)
            except ValueError:
                pass
    elif isinstance(node, dict):
        kind = node.get("kind")
        uid = node.get("uid")
        if isinstance(kind, str) and kind.startswith("leeway."):
            found.append(node)
        elif isinstance(uid, str) and uid.startswith("leeway:"):
            found.append(dict(node, kind="leeway.storm-member",
                              topologyKey=uid.partition("|")[2]))
        for v in node.values():
            walk(v, found)
    elif isinstance(node, list):
        for v in node:
            walk(v, found)


def deep(d, *keys):
    """First value under any of keys, searched depth-first."""
    if isinstance(d, dict):
        for k in keys:
            if k in d and not isinstance(d[k], (dict, list)):
                return d[k]
        for v in d.values():
            r = deep(v, *keys)
            if r is not None:
                return r
    elif isinstance(d, list):
        for v in d:
            r = deep(v, *keys)
            if r is not None:
                return r
    elif isinstance(d, str) and d.strip()[:1] == "{":
        try:
            return deep(json.loads(d), *keys)
        except ValueError:
            pass
    return None


def wire_findings(path):
    out = []
    resolved = 0
    if not os.path.exists(path):
        return out, resolved
    for line in open(path, errors="replace"):
        m = re.match(r"(\S+) INJECT sid=(\S+) kind=(\S*) token=\S+ body=(.*)$", line)
        if not m:
            continue
        when, sid, inject_kind, body = ts(m.group(1)), m.group(2), m.group(3), m.group(4)
        if not LEEWAY.search(body):
            continue
        if inject_kind == "resolved":
            resolved += 1
            continue
        found = []
        try:
            walk(json.loads(body), found)
        except ValueError:
            continue
        route = "session"
        if inject_kind == "watchboard.digest":
            route = "digest"
        elif inject_kind.startswith("storm"):
            route = "storm"
        for f in found:
            out.append((when, route, sid, f))
    return out, resolved


def store_findings(soak):
    """info-routed findings: the sentinel logs one info-store line each."""
    out, seen = [], set()
    for name in ("sentinel.log", "sentinel-final.log"):
        p = os.path.join(soak, name)
        if not os.path.exists(p):
            continue
        for line in open(p, errors="replace"):
            m = re.match(r"(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d) .*info-(?:store|drop) (leeway\.\S+) (\S+)/(\S+)", line)
            if m and line not in seen:
                seen.add(line)
                out.append((ts(m.group(1)), m.group(2), m.group(3), m.group(4)))
    return out


def churn(soak):
    acts = []
    p = os.path.join(soak, "churn.log")
    if os.path.exists(p):
        for line in open(p):
            parts = line.split()
            if parts:
                fields = dict(x.split("=", 1) for x in parts[1:] if "=" in x)
                acts.append((ts(parts[0]), fields))
    return acts


def placements(soak):
    """[(time, {node: zone}, [(ns, pod, node, phase, owner)])]"""
    p = os.path.join(soak, "placement.log")
    snaps, cur = [], None
    if not os.path.exists(p):
        return snaps
    section = None
    for line in open(p, errors="replace"):
        line = line.rstrip("\n")
        if line.startswith("### "):
            cur = (ts(line[4:]), {}, [])
            snaps.append(cur)
            section = "nodes"
        elif line == "## pods":
            section = "pods"
        elif cur and line.strip():
            f = line.split()
            if section == "nodes" and len(f) >= 2:
                cur[1][f[0]] = f[1] if f[1] != "<none>" else "-"
            elif section == "pods" and len(f) >= 5:
                cur[2].append(tuple(f[:5]))
    return snaps


def owned_by(owner, name):
    return owner == name or owner.rsplit("-", 1)[0] == name


def zone_counts(snaps, when, ns, name):
    best = None
    for s in snaps:
        if s[0] and when and s[0] <= when:
            best = s
    if best is None:
        return "no sample yet"
    counts = collections.Counter()
    for pns, _pod, node, phase, owner in best[2]:
        if pns == ns and owned_by(owner, name):
            counts[best[1].get(node, "unscheduled") if phase == "Running" else phase] += 1
    return "%s @%s" % (dict(sorted(counts.items())), best[0].strftime("%H:%M"))


def main():
    soak = sys.argv[1]
    env = {}
    p = os.path.join(soak, "run.env")
    if os.path.exists(p):
        env = dict(line.strip().split("=", 1) for line in open(p) if "=" in line)
    acts = churn(soak)
    snaps = placements(soak)
    wire, resolved = wire_findings(os.path.join(soak, "wire", "wire.log"))
    stored = store_findings(soak)

    print("leeway false-positive soak — %s" % soak)
    for k in ("start", "planned_end", "actual_end", "duration", "image", "seed", "churn_scale"):
        print("  %-12s %s" % (k, env.get(k, "?")))
    # The image tag is local and reused; the build it holds is what a
    # result has to be read against.
    build = "?"
    for name in ("sentinel-startup.log", "sentinel.log"):
        p = os.path.join(soak, name)
        if build == "?" and os.path.exists(p):
            for line in open(p, errors="replace"):
                m = re.search(r" (lookout v\S+)", line)
                if m:
                    build = m.group(1)
                    break
    print("  %-12s %s" % ("build", build))
    tally = collections.Counter(a[1].get("action") for a in acts)
    print("  churn        %s" % ", ".join("%s=%d" % kv for kv in sorted(tally.items())))
    print("  samples      %d placement snapshots" % len(snaps))
    print()

    unique = collections.OrderedDict()
    for when, route, sid, f in wire:
        subj = f.get("subject") if isinstance(f.get("subject"), dict) else {}
        ns = f.get("namespace") or subj.get("namespace") or ""
        name = f.get("name") or subj.get("name") or ""
        key = (f["kind"], ns, name, str(deep(f, "topologyKey") or ""), route)
        if key not in unique:
            unique[key] = dict(first=when, last=when, n=0, sid=sid, f=f)
        u = unique[key]
        u["last"], u["n"] = when, u["n"] + 1

    print("== wire findings (sessions, digest entries, storms): %d unique, %d lines; %d leeway resolves"
          % (len(unique), len(wire), resolved))
    for (kind, ns, name, key, route), u in unique.items():
        f = u["f"]
        print("- %s %s %s/%s key=%s route=%s tier=%s sev=%s cause=%s source=%s drift=%s transient=%s"
              % (u["first"], kind, ns, name, key, route, deep(f, "tier"), deep(f, "severity"),
                 deep(f, "suspectedCause", "reason"), deep(f, "source"), deep(f, "drift"),
                 deep(f, "transient")))
        print("    seen %d× until %s (sid %s)" % (u["n"], u["last"], u["sid"]))
        print("    placement %s" % zone_counts(snaps, u["first"], ns, name))
        for when, a in acts:
            if when and u["first"] and u["first"] - WINDOW <= when <= u["first"]:
                if a.get("node") or (a.get("target", "").split("/")[-1] == name):
                    print("    churn %s %s" % (when.strftime("%H:%M:%S"), " ".join("%s=%s" % kv for kv in a.items())))
    print()

    print("== store-routed findings (info: Tier C at its floor): %d" % len(stored))
    per = collections.Counter((k, ns, n) for _, k, ns, n in stored)
    firsts = {}
    for when, k, ns, n in stored:
        firsts.setdefault((k, ns, n), when)
    for (k, ns, n), c in per.most_common():
        print("- %s %s %s/%s ×%d placement %s" % (firsts[(k, ns, n)], k, ns, n, c,
                                                   zone_counts(snaps, firsts[(k, ns, n)], ns, n)))
    print()

    # Metrics: what never reached the wire but was scored.
    p = os.path.join(soak, "metrics.log")
    if os.path.exists(p):
        alert, drift, last = collections.Counter(), {}, {}
        scrapes = 0
        for line in open(p, errors="replace"):
            if line.startswith("### "):
                scrapes += 1
                continue
            m = re.match(r"(\w+)(\{[^}]*\})? (\S+)", line)
            if not m:
                continue
            name, labels, val = m.group(1), m.group(2) or "", float(m.group(3))
            last[name + labels] = val
            if name == "lookout_leeway_alert_state" and val > 0:
                alert[labels] += 1
            if name == "lookout_leeway_drift":
                drift[labels] = max(drift.get(labels, 0), val)
        print("== metrics: %d scrapes" % scrapes)
        print("  alert_state>0 (series × scrapes): %d series" % len(alert))
        for labels, c in alert.most_common(15):
            print("    %3d× %s" % (c, labels))
        print("  highest max drift:")
        for labels, v in sorted(drift.items(), key=lambda kv: -kv[1])[:10]:
            print("    %.3f %s" % (v, labels))
        for k in sorted(last):
            if k.startswith(("lookout_info_dropped_total{kind=\"leeway", "lookout_leeway_domains_unavailable",
                             "lookout_leeway_baselines", "lookout_leeway_baseline_mature",
                             "process_resident_memory_bytes", "go_memstats_heap_alloc_bytes")):
                print("  last %s %g" % (k, last[k]))
    total = len(unique) + len(per)
    print()
    print("VERDICT: %d distinct leeway findings (%d wire, %d store-routed)" % (total, len(unique), len(per)))


if __name__ == "__main__":
    main()
