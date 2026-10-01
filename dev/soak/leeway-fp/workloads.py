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

"""The soak's workload mix, printed as a JSON List for `kubectl apply -f -`.

One entry per placement intent leeway reads (§5.1): nothing declared (the
assumed cluster default, and later a learned baseline), zone spread both
soft and hard, required and preferred anti-affinity on zone and host, and
colocation through podAffinity. Replica counts run 1..12. Every pod is a
pause container with token requests, so capacity never decides placement —
the scheduler's spread logic does.

churn.py imports WORKLOADS for each workload's replica bounds; the bounds
keep every scale inside what the cluster can always place, so churn never
manufactures an unschedulable workload.
"""

import json
import os

NS = "fp-soak"
ZONE = "topology.kubernetes.io/zone"
HOST = "kubernetes.io/hostname"
IMAGE = "registry.k8s.io/pause:3.9"


def tsc(key, skew, when, **extra):
    return dict(maxSkew=skew, topologyKey=key, whenUnsatisfiable=when, **extra)


def anti(key, required):
    return ("podAntiAffinity", key, required)


def colocate(key, required, target):
    return ("podAffinity", key, required, target)


# name, kind, replicas, (min, max), spread constraints, affinity terms
WORKLOADS = [
    # Nothing declared: the assumed cluster default, then a learned baseline.
    ("none-1", "Deployment", 1, (1, 2), [], []),
    ("none-2", "Deployment", 2, (1, 4), [], []),
    ("none-3", "Deployment", 3, (2, 5), [], []),
    ("none-4", "Deployment", 5, (3, 8), [], []),
    ("none-5", "Deployment", 8, (5, 11), [], []),
    ("none-6", "Deployment", 12, (9, 12), [], []),
    ("none-7", "Deployment", 4, (2, 7), [], []),
    ("none-8", "Deployment", 6, (4, 9), [], []),
    ("none-9", "Deployment", 2, (1, 3), [], []),
    ("none-10", "Deployment", 3, (1, 6), [], []),
    # Zone spread, soft.
    ("soft-1", "Deployment", 3, (2, 6), [tsc(ZONE, 1, "ScheduleAnyway")], []),
    ("soft-2", "Deployment", 6, (3, 9), [tsc(ZONE, 1, "ScheduleAnyway")], []),
    ("soft-3", "Deployment", 9, (6, 12), [tsc(ZONE, 1, "ScheduleAnyway")], []),
    ("soft-4", "Deployment", 4, (2, 7), [tsc(ZONE, 2, "ScheduleAnyway")], []),
    ("soft-5", "Deployment", 12, (8, 12), [tsc(ZONE, 1, "ScheduleAnyway")], []),
    ("soft-6", "Deployment", 2, (1, 4), [tsc(ZONE, 1, "ScheduleAnyway")], []),
    ("soft-honor", "Deployment", 6, (3, 9),
     [tsc(ZONE, 1, "ScheduleAnyway", nodeTaintsPolicy="Honor")], []),
    # Zone spread, hard.
    ("hard-1", "Deployment", 3, (2, 6), [tsc(ZONE, 1, "DoNotSchedule")], []),
    ("hard-2", "Deployment", 6, (3, 9), [tsc(ZONE, 1, "DoNotSchedule")], []),
    ("hard-3", "Deployment", 4, (2, 7), [tsc(ZONE, 1, "DoNotSchedule")], []),
    ("hard-4", "Deployment", 9, (6, 12), [tsc(ZONE, 1, "DoNotSchedule")], []),
    ("hard-5", "Deployment", 7, (4, 10), [tsc(ZONE, 2, "DoNotSchedule")], []),
    ("hard-6", "Deployment", 6, (3, 9),
     [tsc(ZONE, 1, "DoNotSchedule"), tsc(HOST, 1, "ScheduleAnyway")], []),
    ("hard-7", "Deployment", 2, (1, 3), [tsc(ZONE, 1, "DoNotSchedule")], []),
    ("hard-8", "Deployment", 12, (9, 12), [tsc(ZONE, 1, "DoNotSchedule")], []),
    ("tsc-both", "Deployment", 6, (3, 6),
     [tsc(ZONE, 1, "DoNotSchedule"), tsc(HOST, 1, "DoNotSchedule")], []),
    # Anti-affinity. Required-on-zone is capped at one per zone and
    # required-on-host below the node count minus a drained node.
    ("anti-zone-req", "Deployment", 3, (2, 3), [], [anti(ZONE, True)]),
    ("anti-host-req", "Deployment", 4, (2, 4), [], [anti(HOST, True)]),
    ("anti-zone-pref", "Deployment", 6, (3, 9), [], [anti(ZONE, False)]),
    ("anti-host-pref", "Deployment", 5, (3, 8), [], [anti(HOST, False)]),
    # Colocation: colo-req must share a node with an anchor pod.
    ("anchor", "Deployment", 2, (2, 3), [], []),
    ("colo-req", "Deployment", 4, (2, 6), [], [colocate(HOST, True, "anchor")]),
    ("colo-pref", "Deployment", 3, (2, 5), [], [colocate(ZONE, False, "anchor")]),
    # The autoscaled one: churn.py oscillates it between its bounds.
    ("hpa-osc", "Deployment", 4, (2, 10), [tsc(ZONE, 1, "ScheduleAnyway")], []),
    ("sts-hard", "StatefulSet", 3, (3, 6), [tsc(ZONE, 1, "DoNotSchedule")], []),
    ("sts-none", "StatefulSet", 2, (1, 4), [], []),
]

# A PodDisruptionBudget each, so drains go through the eviction API's
# budget check the way a production drain does.
PDBS = ["none-6", "soft-3", "hard-2", "hard-8", "sts-hard"]


def affinity(terms):
    out = {}
    for term in terms:
        kind, key, required = term[0], term[1], term[2]
        target = term[3] if len(term) > 3 else None
        selector = {"matchLabels": {"app": target}} if target else None
        out.setdefault(kind, {})
        if required:
            out[kind].setdefault("requiredDuringSchedulingIgnoredDuringExecution", []).append(
                {"topologyKey": key, "labelSelector": selector}
            )
        else:
            out[kind].setdefault("preferredDuringSchedulingIgnoredDuringExecution", []).append(
                {"weight": 100, "podAffinityTerm": {"topologyKey": key, "labelSelector": selector}}
            )
    return out


# Every soak pod requires a node with a zone label, which is every node a
# GKE workload can land on. Without it, kind's unlabelled control plane is
# an eligible "__unknown__" domain to leeway (though not to kube-scheduler's
# spread), and every evenly spread workload reads as skewed. See
# kind_config in ./soak. SOAK_ZONE_AFFINITY=0 drops it to reproduce that.
ZONE_AFFINITY = os.environ.get("SOAK_ZONE_AFFINITY", "1") == "1"


def manifest(name, kind, replicas, constraints, terms):
    labels = {"app": name}
    # Anti-affinity terms select the workload's own pods.
    terms = [t if t[0] == "podAffinity" else t + (name,) for t in terms]
    pod = {
        "terminationGracePeriodSeconds": 5,
        "containers": [
            {
                "name": "app",
                "image": IMAGE,
                "resources": {
                    "requests": {"cpu": "5m", "memory": "8Mi"},
                    "limits": {"cpu": "20m", "memory": "16Mi"},
                },
            }
        ],
    }
    if constraints:
        pod["topologySpreadConstraints"] = [
            dict(c, labelSelector={"matchLabels": labels}) for c in constraints
        ]
    if terms:
        pod["affinity"] = affinity(terms)
    if ZONE_AFFINITY:
        pod.setdefault("affinity", {})["nodeAffinity"] = {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{"matchExpressions": [{"key": ZONE, "operator": "Exists"}]}]
            }
        }
    spec = {
        "replicas": replicas,
        "selector": {"matchLabels": labels},
        "template": {"metadata": {"labels": labels}, "spec": pod},
    }
    if kind == "StatefulSet":
        spec["serviceName"] = name
        spec["podManagementPolicy"] = "Parallel"
    return {
        "apiVersion": "apps/v1",
        "kind": kind,
        "metadata": {"name": name, "namespace": NS, "labels": labels},
        "spec": spec,
    }


def main():
    items = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}]
    for name, kind, replicas, _bounds, constraints, terms in WORKLOADS:
        items.append(manifest(name, kind, replicas, constraints, terms))
    for name in PDBS:
        items.append(
            {
                "apiVersion": "policy/v1",
                "kind": "PodDisruptionBudget",
                "metadata": {"name": name, "namespace": NS},
                "spec": {"maxUnavailable": 1, "selector": {"matchLabels": {"app": name}}},
            }
        )
    print(json.dumps({"apiVersion": "v1", "kind": "List", "items": items}, indent=1))


if __name__ == "__main__":
    main()
