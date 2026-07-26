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

"""Slow memory leaker for the M3 saturation drill.

Appends touched (zero-filled, therefore resident) 1-MiB blocks to a
list on a fixed cadence and never frees them — a clean linear
working-set ramp for the saturation source's least-squares fit
(DESIGN.md §7.2 row 4 / §8). Tune the rate against the container's
memory LIMIT so the forecast fires at severity warning (ETA inside
[15m, 60m)) minutes before the OOM kill: with a 64Mi limit, the
defaults (+1 MiB every 30s ≈ 1.6% of limit per sample interval) put
the warning ~5 minutes in and the OOM ~25 minutes in.

Usage: memory-leaker.py [step-mib] [interval-seconds]

Used by dev/drills/memory-leak.md; the kind-run evidence is in
docs/milestones/M3.md. Deploy as a ConfigMap-mounted script on any
stock python image — no dependencies beyond the stdlib.
"""

import sys
import time

MIB = 1024 * 1024


def main():
    step_mib = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    interval = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
    ballast = []
    held = 0.0
    print("leaker: +%g MiB every %gs, never freed" % (step_mib, interval), flush=True)
    while True:
        # bytearray zero-fills, so every page is touched and counts
        # against the working set metrics.k8s.io reports.
        ballast.append(bytearray(int(step_mib * MIB)))
        held += step_mib
        print("leaker: holding %.0f MiB" % held, flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
