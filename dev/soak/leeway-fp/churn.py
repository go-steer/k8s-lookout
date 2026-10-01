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

"""Benign churn for the leeway false-positive soak.

Two loops, both seeded, and every action logged with a timestamp:

  workloads  every 1-4 min: a rollout restart, an image-tag bump, a scale
             of ±1..3 inside the workload's bounds, or a pod deletion; and
             every 5 min the autoscaled workload follows a 90-minute sine.
  nodes      every 30-90 min, ONE node: either drained (through the
             eviction API, so PDBs hold), left cordoned for 2-7 minutes and
             uncordoned, or paused in docker for 50-100 s so its kubelet
             misses heartbeats and the node blips NotReady.

Nothing here should legitimately produce a leeway finding: no zone ever
loses more than one of its two nodes, and no node is out for longer than
the drain transient (§7.6) covers. Scale-downs are the one place real
drift can arise — the ReplicaSet controller picks pods to delete without
regard to spread — and summarize.py checks findings against placement
for exactly that reason.

Node operations always undo themselves, including on SIGTERM.
"""

import argparse
import json
import math
import os
import random
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from workloads import NS, WORKLOADS  # noqa: E402

IMAGES = ["registry.k8s.io/pause:3.9", "registry.k8s.io/pause:3.10"]
HPA = "hpa-osc"

lock = threading.Lock()
stop = threading.Event()
logf = None
# Nodes currently cordoned or paused by us, so an interrupted run can
# put them back.
touched = {}


def now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log(**fields):
    line = now() + " " + " ".join("%s=%s" % (k, v) for k, v in fields.items())
    with lock:
        logf.write(line + "\n")
        logf.flush()


def run(*argv, timeout=420):
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout + p.stderr).strip().replace("\n", " | ")[:300]
    except subprocess.TimeoutExpired:
        return 124, "timeout"


def kubectl(*args, **kw):
    return run("kubectl", *args, **kw)


def current_replicas():
    out = subprocess.run(
        ["kubectl", "get", "deploy,statefulset", "-n", NS, "-o", "json"],
        capture_output=True, text=True, check=True,
    ).stdout
    return {i["metadata"]["name"]: i["spec"]["replicas"] for i in json.loads(out)["items"]}


def sleep(seconds):
    return stop.wait(seconds)


def workload_loop(rng, scale, deadline):
    kinds = {w[0]: w[1] for w in WORKLOADS}
    bounds = {w[0]: w[3] for w in WORKLOADS}
    replicas = current_replicas()
    names = [w[0] for w in WORKLOADS if w[0] != HPA]
    images = {n: 0 for n in kinds}
    next_hpa = time.time()
    t0 = time.time()
    while time.time() < deadline and not stop.is_set():
        if time.time() >= next_hpa:
            lo, hi = bounds[HPA]
            phase = 2 * math.pi * (time.time() - t0) / (90 * 60 * scale)
            want = round((lo + hi) / 2 + (hi - lo) / 2 * math.sin(phase))
            if want != replicas[HPA]:
                rc, out = kubectl("scale", "deployment/" + HPA, "-n", NS, "--replicas=%d" % want)
                log(action="hpa", target="Deployment/" + HPA, frm=replicas[HPA], to=want, rc=rc)
                replicas[HPA] = want
            next_hpa = time.time() + 300 * scale
        if sleep(rng.uniform(60, 240) * scale):
            break
        action = rng.choices(
            ["restart", "image", "scale", "delete-pod", "delete-burst"],
            weights=[25, 15, 35, 20, 5],
        )[0]
        name = rng.choice(names)
        ref = "%s/%s" % (kinds[name].lower(), name)
        if action == "restart":
            rc, out = kubectl("rollout", "restart", ref, "-n", NS)
            log(action=action, target=ref, rc=rc)
        elif action == "image":
            images[name] ^= 1
            rc, out = kubectl("set", "image", ref, "-n", NS, "app=" + IMAGES[images[name]])
            log(action=action, target=ref, image=IMAGES[images[name]], rc=rc)
        elif action == "scale":
            lo, hi = bounds[name]
            want = max(lo, min(hi, replicas[name] + rng.choice([-3, -2, -1, 1, 2, 3])))
            if want == replicas[name]:
                continue
            rc, out = kubectl("scale", ref, "-n", NS, "--replicas=%d" % want)
            log(action=action, target=ref, frm=replicas[name], to=want, rc=rc)
            replicas[name] = want
        else:
            rc, out = kubectl("get", "pods", "-n", NS, "-l", "app=" + name, "-o", "name")
            pods = [p for p in out.split(" | ") if p.startswith("pod/")] if rc == 0 else []
            if not pods:
                continue
            victims = rng.sample(pods, min(len(pods), 1 if action == "delete-pod" else rng.randint(2, 3)))
            rc, out = kubectl("delete", "-n", NS, "--wait=false", *victims)
            log(action=action, target=ref, pods=",".join(v[4:] for v in victims), rc=rc)


def workers(cluster):
    out = subprocess.run(
        ["kubectl", "get", "nodes", "-l", "!node-role.kubernetes.io/control-plane", "-o", "json"],
        capture_output=True, text=True, check=True,
    ).stdout
    return {
        i["metadata"]["name"]: i["metadata"]["labels"].get("topology.kubernetes.io/zone", "")
        for i in json.loads(out)["items"]
    }


def restore(node):
    state = touched.pop(node, None)
    if state == "paused":
        rc, out = run("docker", "unpause", node)
        log(action="unpause", node=node, rc=rc)
    elif state == "cordoned":
        rc, out = kubectl("uncordon", node)
        log(action="uncordon", node=node, rc=rc)


def node_loop(rng, cluster, scale, deadline):
    nodes = workers(cluster)
    while not stop.is_set():
        if sleep(rng.uniform(30 * 60, 90 * 60) * scale):
            return
        if time.time() >= deadline:
            return
        node = rng.choice(sorted(nodes))
        zone = nodes[node]
        if rng.random() < 0.6:
            touched[node] = "cordoned"
            log(action="drain", node=node, zone=zone, phase="start")
            rc, out = kubectl(
                "drain", node, "--ignore-daemonsets", "--delete-emptydir-data",
                "--grace-period=10", "--timeout=300s",
            )
            hold = rng.uniform(120, 420)
            log(action="drain", node=node, zone=zone, phase="drained", rc=rc, hold=int(hold),
                detail=out[-120:].replace(" ", "_") if rc else "ok")
            sleep(hold)
            restore(node)
        else:
            touched[node] = "paused"
            hold = rng.uniform(50, 100)
            rc, out = run("docker", "pause", node)
            log(action="pause", node=node, zone=zone, rc=rc, hold=int(hold))
            # Not stop.wait: a blip is always allowed to finish its hold
            # and unpause, even when the run is ending.
            time.sleep(hold)
            restore(node)


def main():
    global logf
    ap = argparse.ArgumentParser()
    ap.add_argument("--cluster", required=True)
    ap.add_argument("--seed", type=int, default=416)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--duration", type=int, required=True, help="seconds")
    ap.add_argument("--log", required=True)
    a = ap.parse_args()
    logf = open(a.log, "a")
    deadline = time.time() + a.duration

    def on_signal(signum, _frame):
        log(action="signal", signum=signum)
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    log(action="start", seed=a.seed, scale=a.scale, duration=a.duration)
    # Separate generators so the node schedule does not shift whenever a
    # workload action is skipped.
    t = threading.Thread(
        target=node_loop, args=(random.Random(a.seed + 1), a.cluster, a.scale, deadline), daemon=True
    )
    t.start()
    try:
        workload_loop(random.Random(a.seed), a.scale, deadline)
    finally:
        stop.set()
        t.join(timeout=600)
        for node in list(touched):
            restore(node)
        log(action="end")


if __name__ == "__main__":
    main()
