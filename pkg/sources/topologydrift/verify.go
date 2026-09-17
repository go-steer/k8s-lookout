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

package topologydrift

import (
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// Verification defaults (§6.5).
const (
	// DefaultVerifyInterval is how often one shard is checked. With
	// DefaultVerifyShards that puts a full pass at one hour, which is the
	// number to reason about: drift is detected and repaired within an hour of
	// appearing, and the cost of that guarantee is one pod-cache walk every
	// five minutes.
	DefaultVerifyInterval = 5 * time.Minute

	// DefaultVerifyShards is how many slices the subject set is cut into.
	// Sharding does not make the walk cheaper — every pass reads the whole pod
	// cache to decide which pods belong to this shard's subjects — it bounds
	// the REPAIR: a systematic bug discovered at 20,000 subjects rebuilds a
	// twelfth of them per tick instead of stalling the evaluation queue behind
	// all of them at once.
	DefaultVerifyShards = 12
)

// PodSnapshot returns every pod the cache currently holds.
type PodSnapshot func() ([]*corev1.Pod, error)

// VerifyOptions configures a Verifier.
type VerifyOptions struct {
	// State is the incremental state being checked. Required.
	State *State

	// Pods reads the pod cache. Required.
	Pods PodSnapshot

	// Interval and Shards take the defaults above when zero.
	Interval time.Duration
	Shards   int

	// OnMismatch is called once per drifted subject, before the repair. Nil
	// discards, which is what the counting tests use.
	OnMismatch func(leeway.SubjectKind)

	// Logf overrides log.Printf.
	Logf func(format string, args ...any)
}

func (o VerifyOptions) normalize() VerifyOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultVerifyInterval
	}
	if o.Shards <= 0 {
		o.Shards = DefaultVerifyShards
	}
	return o
}

// Verifier is §6.5: a shipped component, not a debugging aid.
//
// Incremental counters are correct in tests and subtly wrong in production
// sixty days in. The design treats that as a certainty rather than a risk, so
// the subsystem carries its own auditor: every interval it rebuilds one shard
// of the subject set directly from the pod cache, compares, and — when the two
// disagree — reports it on an SLI and repairs the state in place.
//
// Repairing rather than only reporting is the deliberate half. A counter that
// has drifted will keep drifting, and every finding derived from it until
// someone reads the alert would be wrong in the same direction. The alert is
// how the BUG gets fixed; the repair is how the next hour's output stays
// usable while that happens.
type Verifier struct {
	opts  VerifyOptions
	shard int
}

// NewVerifier returns a verifier over the given state.
func NewVerifier(opts VerifyOptions) *Verifier {
	return &Verifier{opts: opts.normalize()}
}

// Interval is the configured tick.
func (v *Verifier) Interval() time.Duration { return v.opts.Interval }

// Shards is the configured shard count.
func (v *Verifier) Shards() int { return v.opts.Shards }

func (v *Verifier) logger() func(string, ...any) {
	if v.opts.Logf != nil {
		return v.opts.Logf
	}
	return log.Printf
}

// Tick verifies the next shard and advances, so that repeated calls walk the
// whole subject set. Returns the shard's report.
func (v *Verifier) Tick() Report {
	shard := v.shard
	v.shard = (v.shard + 1) % v.opts.Shards
	return v.VerifyShard(shard)
}

// Report is what one shard's pass found.
type Report struct {
	Shard    int
	Subjects int // subjects compared
	Drifted  int // subjects whose rebuild disagreed, and were repaired
	Err      error
}

// VerifyShard rebuilds one shard of subjects from the pod cache, compares each
// against the live counters and repairs any that disagree.
func (v *Verifier) VerifyShard(shard int) Report {
	rep := Report{Shard: shard}

	pods, err := v.opts.Pods()
	if err != nil {
		rep.Err = err
		v.logger()("topology-drift: verify shard %d/%d: pod cache read failed: %v — counters unverified this pass",
			shard, v.opts.Shards, err)
		return rep
	}

	want := v.opts.State.RebuildShard(pods, shard, v.opts.Shards)

	// The union of both sides, not just the rebuild's: a subject the
	// incremental path still holds and the cache no longer supports is the
	// leak this whole component exists to catch, and it appears in `want`
	// only as an absence.
	for _, sub := range v.opts.State.SubjectsInShard(shard, v.opts.Shards) {
		if _, ok := want[sub]; !ok {
			want[sub] = &Rebuilt{}
		}
	}

	rep.Subjects = len(want)
	for sub, rebuilt := range want {
		got := v.opts.State.Snapshot(sub)
		if sameDistributions(rebuilt.Counts, got) {
			continue
		}
		rep.Drifted++
		if v.opts.OnMismatch != nil {
			v.opts.OnMismatch(sub.Kind)
		}
		// One line per drifted subject, naming the subject and the domains that
		// differ. The alert says only "something drifted"; this is where the
		// reader finds out what, and it is the only place they can.
		v.logger()("topology-drift: COUNTER DRIFT on %s — rebuilt from %d pod(s) disagrees with the incremental counters (%s); repairing. This is a bug in the delta rules, not a cluster condition (leeway design §6.5)",
			sub, len(rebuilt.Pods), describeDrift(rebuilt.Counts, got))
		v.opts.State.Repair(sub, rebuilt.Pods)
	}
	return rep
}

// sameDistributions compares two per-axis distribution maps, treating an
// absent axis and an empty one as the same thing.
func sameDistributions(a, b map[leeway.TopologyKey]*leeway.Distribution) bool {
	for key, da := range a {
		if !da.Equal(b[key]) {
			return false
		}
	}
	for key, db := range b {
		if _, ok := a[key]; !ok && db != nil {
			return false
		}
	}
	return true
}

// maxDriftDomains bounds how many domains one axis contributes to the log line.
// A systematic bug drifts every domain of every subject at once, and the log
// that reports it has to stay readable on exactly that day.
const maxDriftDomains = 4

// describeDrift renders what disagreed, as `counted→rebuilt` per domain.
//
// Per-domain and not per-axis totals, which is a correction worth stating: the
// commonest drift is a pod that moved and was never re-counted, and its totals
// match on both sides. A line reading "rebuilt=1 counted=1" would report the
// most likely bug in this subsystem as a difference of nothing.
func describeDrift(want, got map[leeway.TopologyKey]*leeway.Distribution) string {
	keys := make(map[leeway.TopologyKey]struct{}, len(want)+len(got))
	for k := range want {
		keys[k] = struct{}{}
	}
	for k := range got {
		keys[k] = struct{}{}
	}
	// Sorted so two passes over the same drift read identically; a log line
	// whose field order changes per occurrence cannot be diffed or grepped.
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, string(k))
	}
	slices.Sort(ordered)

	parts := make([]string, 0, len(ordered))
	for _, k := range ordered {
		key := leeway.TopologyKey(k)
		if axis := describeAxis(want[key], got[key]); axis != "" {
			parts = append(parts, k+": "+axis)
		}
	}
	if len(parts) == 0 {
		return "no per-domain difference"
	}
	return strings.Join(parts, "; ")
}

// describeAxis renders one axis's differing domains, or "" when they agree.
func describeAxis(want, got *leeway.Distribution) string {
	domains := make(map[leeway.Domain]struct{})
	for dom := range domainsOf(want) {
		domains[dom] = struct{}{}
	}
	for dom := range domainsOf(got) {
		domains[dom] = struct{}{}
	}
	ordered := make([]string, 0, len(domains))
	for dom := range domains {
		ordered = append(ordered, string(dom))
	}
	slices.Sort(ordered)

	var parts []string
	elided := 0
	for _, d := range ordered {
		dom := leeway.Domain(d)
		w, g := countIn(want, dom), countIn(got, dom)
		if w == g {
			continue
		}
		if len(parts) == maxDriftDomains {
			elided++
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %d→%d", d, g, w))
	}
	if len(parts) == 0 {
		return ""
	}
	if elided > 0 {
		parts = append(parts, fmt.Sprintf("and %d more domain(s)", elided))
	}
	return strings.Join(parts, ", ")
}

func domainsOf(d *leeway.Distribution) map[leeway.Domain]*leeway.DomainCount {
	if d == nil {
		return nil
	}
	return d.ByDomain
}

func countIn(d *leeway.Distribution, dom leeway.Domain) int64 {
	if d == nil {
		return 0
	}
	c, ok := d.ByDomain[dom]
	if !ok {
		return 0
	}
	return c.Total()
}
