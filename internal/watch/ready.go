// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package watch

import (
	"fmt"
	"sort"
	"sync"
)

// readiness answers /readyz for the process: are the sentinel's
// watches actually established?
//
// The distinction /healthz cannot make (issue #285). A static 200 is a
// defensible liveness answer — the process is up — but the sentinel
// spends its first seconds listing every informer's world, and in that
// window it is running and watching nothing. Reporting ready there is
// how a rollout cuts over to a pod that will miss events, and how a
// multi-cluster process claims coverage it does not have yet.
//
// The tracker is two-phase on purpose. expect names the clusters this
// process is responsible for, as soon as they are resolved; each
// runner then registers a probe over its own sources when it is about
// to start watching, and withdraws it when it stops. Ready means every
// expected cluster has a live probe and that probe says its sources
// have synced — so a process that has resolved three clusters and
// started one is not ready, which a naive "all registered probes are
// happy" would get backwards.
type readiness struct {
	mu sync.Mutex
	// expected is nil until the runners are resolved, which is
	// itself a not-ready state: at that point the process does not
	// yet know what it is responsible for.
	expected []string
	probes   map[string]func() bool
}

func newReadiness() *readiness {
	return &readiness{probes: make(map[string]func() bool)}
}

// expect declares the clusters this process must be watching before it
// is ready. Called once, after the runners are resolved.
func (r *readiness) expect(clusters []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expected = append([]string(nil), clusters...)
}

// set registers a cluster's readiness probe — called by a runner just
// before it starts its sources, and again on every supervisor restart.
func (r *readiness) set(cluster string, probe func() bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probes[cluster] = probe
}

// clear withdraws a cluster's probe. A runner between restarts is not
// watching its cluster, so the process is not ready — which is exactly
// what a load balancer or a rollout gate should see.
func (r *readiness) clear(cluster string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.probes, cluster)
}

// ready reports whether every expected cluster is watching, and a
// short reason when it is not. The reason is the probe's response body:
// it names what is missing, because "503" alone sends an operator to
// the logs to answer a question the probe already knew.
func (r *readiness) ready() (bool, string) {
	r.mu.Lock()
	probes := make(map[string]func() bool, len(r.probes))
	for k, v := range r.probes {
		probes[k] = v
	}
	expected := append([]string(nil), r.expected...)
	r.mu.Unlock()

	if len(expected) == 0 {
		return false, "starting: no cluster runners resolved yet"
	}
	sort.Strings(expected)
	// The default deployment — one sentinel, one cluster, no
	// --cluster-name — has no name for its cluster, so the per-cluster
	// list has nothing to put in it and rendered a blank entry:
	// `waiting on 1 of 1 cluster(s): [ (not started)]` (#321). Report
	// the phase on its own there. The "n of m cluster(s)" form earns
	// its keep only when there are names to distinguish.
	unnamed := len(expected) == 1 && expected[0] == ""
	var waiting []string
	for _, c := range expected {
		var phase string
		// Probes are called outside r.mu: a source's HasSynced takes
		// its own lock, and holding the tracker's while waiting on a
		// source's would make a slow source able to block every other
		// cluster's readiness answer.
		switch probe, ok := probes[c]; {
		case !ok:
			phase = "not started"
		case !probe():
			phase = "syncing"
		default:
			continue
		}
		if unnamed {
			return false, unnamedReason[phase]
		}
		waiting = append(waiting, c+" ("+phase+")")
	}
	if len(waiting) > 0 {
		return false, fmt.Sprintf("waiting on %d of %d cluster(s): %v", len(waiting), len(expected), waiting)
	}
	return true, ""
}

// unnamedReason renders each not-ready phase for the unnamed
// single-cluster case, as a whole sentence: the handler prefixes
// "not ready: ", and naming a cluster that has no name only tells the
// operator less than the phase does.
var unnamedReason = map[string]string{
	"not started": "cluster runner not started",
	"syncing":     "informer caches syncing",
}
