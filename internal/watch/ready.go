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
	"strings"
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
	// degraded maps a cluster to the reason this process has stopped
	// trying to watch it (issue #383). A terminally-failed cluster is
	// dropped from the readiness expectation rather than held against
	// it: readiness is "is this replica fit to serve", and a fleet
	// sentinel that watches 24 of 26 clusters is fit — failing it
	// would take the working 24 out of the rollout too, and no
	// restart or reschedule brings the other 2 back. The two clusters
	// are still reported: as /readyz?verbose lines, as a log line, and
	// as lookout_runner_terminal, which is where an alert belongs.
	//
	// The exception is total loss — see ready.
	degraded map[string]string
}

func newReadiness() *readiness {
	return &readiness{
		probes:   make(map[string]func() bool),
		degraded: make(map[string]string),
	}
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

// degrade records that a cluster has failed in a way no retry fixes,
// so this process has stopped trying to watch it (issue #383). Called
// by the supervisor when it classifies a runner's exit as terminal;
// reason is the terminalReason, which is also the metric label.
//
// Idempotent, and it withdraws the cluster's probe on the way: a
// runner that is never coming back must not leave a stale probe
// behind for ready to consult.
func (r *readiness) degrade(cluster, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.degraded[cluster] = reason
	delete(r.probes, cluster)
}

// ready reports whether every expected cluster that CAN be watched is
// being watched, and a short reason when it is not. The reason is the
// probe's response body: it names what is missing, because "503" alone
// sends an operator to the logs to answer a question the probe already
// knew.
//
// Degraded clusters (see the field comment) are excluded from the
// expectation — with one exception. If every expected cluster has
// degraded, this replica is watching nothing at all, and reporting
// ready would be the same silently-empty-watch lie §11 exists to
// prevent. Total loss is not ready.
func (r *readiness) ready() (bool, string) {
	expected, probes, degraded := r.snapshot()

	if len(expected) == 0 {
		return false, "starting: no cluster runners resolved yet"
	}
	// The default deployment — one sentinel, one cluster, no
	// --cluster-name — has no name for its cluster, so the per-cluster
	// list has nothing to put in it and rendered a blank entry:
	// `waiting on 1 of 1 cluster(s): [ (not started)]` (#321). Report
	// the phase on its own there. The "n of m cluster(s)" form earns
	// its keep only when there are names to distinguish.
	unnamed := len(expected) == 1 && expected[0] == ""

	var live, waiting []string
	for _, c := range expected {
		if _, dead := degraded[c]; dead {
			continue
		}
		live = append(live, c)
		phase := phaseOf(c, probes)
		if phase == phaseWatching {
			continue
		}
		if unnamed {
			return false, unnamedReason[phase]
		}
		waiting = append(waiting, c+" ("+phase+")")
	}
	if len(live) == 0 {
		if unnamed {
			return false, "cluster runner failed terminally (" + degraded[""] + ")"
		}
		return false, fmt.Sprintf("every one of %d cluster(s) failed terminally: %v", len(expected), degradedList(expected, degraded))
	}
	if len(waiting) > 0 {
		return false, fmt.Sprintf("waiting on %d of %d cluster(s): %v", len(waiting), len(live), waiting)
	}
	return true, ""
}

// report renders the per-cluster breakdown behind /readyz?verbose,
// including the trailing verdict line. Served on both 200 and 503,
// because the interesting case is the 200 that hides a degraded
// cluster.
//
// The format follows kube-apiserver's /readyz?verbose so the shape is
// already familiar, with one addition: [!] is a cluster that is
// degraded and therefore EXCLUDED from the verdict, which the
// apiserver's two-state [+]/[-] cannot express.
//
//	[+]prod-us watching
//	[-]prod-eu syncing
//	[!]prod-ap degraded: access_denied
//	readyz check failed: waiting on 1 of 2 cluster(s): [prod-eu (syncing)]
//
// The verdict is a parameter rather than a second ready() call: the
// handler has already computed it to pick the status code, and
// recomputing could disagree with it if a source syncs in between.
func (r *readiness) report(ok bool, why string) string {
	expected, probes, degraded := r.snapshot()
	var b strings.Builder
	for _, c := range expected {
		name := c
		if name == "" {
			// The unnamed single-cluster default (#321) still needs a
			// subject; the literal reads correctly in every line.
			name = "cluster"
		}
		if reason, dead := degraded[c]; dead {
			fmt.Fprintf(&b, "[!]%s degraded: %s\n", name, reason)
			continue
		}
		phase := phaseOf(c, probes)
		mark := "-"
		if phase == phaseWatching {
			mark = "+"
		}
		fmt.Fprintf(&b, "[%s]%s %s\n", mark, name, phase)
	}
	if ok {
		b.WriteString("readyz check passed\n")
	} else {
		fmt.Fprintf(&b, "readyz check failed: %s\n", why)
	}
	return b.String()
}

// snapshot copies the tracker's state under the lock, sorted, so both
// answers are rendered from one consistent view.
func (r *readiness) snapshot() (expected []string, probes map[string]func() bool, degraded map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	expected = append([]string(nil), r.expected...)
	sort.Strings(expected)
	probes = make(map[string]func() bool, len(r.probes))
	for k, v := range r.probes {
		probes[k] = v
	}
	degraded = make(map[string]string, len(r.degraded))
	for k, v := range r.degraded {
		degraded[k] = v
	}
	return expected, probes, degraded
}

// Phases a live (non-degraded) cluster can be in.
const (
	phaseNotStarted = "not started"
	phaseSyncing    = "syncing"
	phaseWatching   = "watching"
)

// phaseOf asks one cluster's probe where it is.
//
// Probes are called outside r.mu: a source's HasSynced takes its own
// lock, and holding the tracker's while waiting on a source's would
// make a slow source able to block every other cluster's readiness
// answer. snapshot is what makes that possible.
func phaseOf(cluster string, probes map[string]func() bool) string {
	switch probe, ok := probes[cluster]; {
	case !ok:
		return phaseNotStarted
	case !probe():
		return phaseSyncing
	default:
		return phaseWatching
	}
}

// degradedList renders the total-loss reason, one "name (reason)" per
// expected cluster, in the expected order.
func degradedList(expected []string, degraded map[string]string) []string {
	out := make([]string, 0, len(expected))
	for _, c := range expected {
		out = append(out, c+" ("+degraded[c]+")")
	}
	return out
}

// unnamedReason renders each not-ready phase for the unnamed
// single-cluster case, as a whole sentence: the handler prefixes
// "not ready: ", and naming a cluster that has no name only tells the
// operator less than the phase does.
var unnamedReason = map[string]string{
	phaseNotStarted: "cluster runner not started",
	phaseSyncing:    "informer caches syncing",
}
