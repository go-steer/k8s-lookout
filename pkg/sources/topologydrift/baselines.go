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
	"sort"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// baselineKey identifies one learned baseline: a subject on one axis.
//
// The same grain as an episode and as a persisted row, and for the same
// reason §9.3 gives for the alert table — a workload can be evenly spread
// across zones while stacked three-deep on one node, and what is normal for
// it on one axis says nothing about the other.
type baselineKey struct {
	Subject leeway.SubjectRef
	Key     leeway.TopologyKey
}

// baselineLog is the source's live set of §7.5 estimators, plus the
// bookkeeping the §9.2 flusher needs to write only what moved.
//
// It owns no clock and no store. Sampling, freezing and flushing are all
// driven from Run, which is what keeps a dwell and a half-life measuring
// elapsed time rather than event volume — the same rule the alert machine
// already follows, and a sharper one here: a subject whose pods churn would
// otherwise learn its way to maturity in minutes while a quiet one that is
// equally wrong took the full six hours.
type baselineLog struct {
	mu   sync.Mutex
	sets map[baselineKey]*leeway.BaselineSet

	// dirty is what has moved since the last flush. Tracked rather than
	// diffed because §9.2's flush is a batch: writing every set every thirty
	// seconds would be twenty thousand UPSERTs a minute to record that a
	// quiet estate is still quiet, and the estimator itself already knows
	// whether a sample changed it — Observe says so in its outcome.
	dirty map[baselineKey]bool

	// gone is what has been forgotten since the last flush and still has a
	// row. Deletions cannot ride the dirty set: a forgotten subject has no
	// set left to write, and leaving its row behind would hand the next
	// workload to take that name a predecessor's placement history.
	gone map[baselineKey]bool

	// outcomes counts what Observe decided, for §6.5's self-verification
	// counters. A healthy estate is almost all ObserveApplied; a rising
	// ObserveReset means something is churning the domain set, which is the
	// one failure mode that silently keeps every baseline immature forever.
	outcomes map[leeway.ObserveOutcome]int64
}

func newBaselineLog() *baselineLog {
	return &baselineLog{
		sets:     make(map[baselineKey]*leeway.BaselineSet),
		dirty:    make(map[baselineKey]bool),
		gone:     make(map[baselineKey]bool),
		outcomes: make(map[leeway.ObserveOutcome]int64),
	}
}

// Observe feeds one sample to one subject-axis, creating the estimator on
// first sight, and reports what the estimator did with it.
//
// Every outcome but ObserveHeld and ObserveEmpty marks the set dirty.
// ObserveHeld is a frozen set, which by construction learned nothing; and
// ObserveEmpty is a subject with nothing to learn from — scaled to zero, or
// every pod Pending — which must not be recorded, because a baseline taught
// that empty is normal reads the scale-up back as drift.
func (l *baselineLog) Observe(k baselineKey, domains []leeway.Domain, actual []int64, now time.Time, cfg leeway.BaselineConfig) leeway.ObserveOutcome {
	l.mu.Lock()
	defer l.mu.Unlock()

	b := l.sets[k]
	if b == nil {
		// Built on the sample's own domain set, so first sight reports
		// ObserveSeeded rather than ObserveReset. The two behave identically
		// — both seed the EWMAs from the sample — but conflating them would
		// put one reset on the counter for every subject the source has ever
		// seen, and drown the signal that counter exists for.
		b = leeway.NewBaselineSet(domains, now)
		l.sets[k] = b
	}
	out := b.Observe(domains, actual, now, cfg)
	l.outcomes[out]++
	switch out {
	case leeway.ObserveApplied, leeway.ObserveSeeded, leeway.ObserveReset:
		l.dirty[k] = true
	}
	// A key can be forgotten and then re-created — a Deployment deleted and
	// re-applied within one flush interval. The pending delete has to go, or
	// the flush would write the new estimate and then delete the row it just
	// wrote.
	delete(l.gone, k)
	return out
}

// SetFrozen holds or releases one subject-axis's clock, and reports whether
// that was a change.
//
// Freezing is §7.5's answer to the classic learned-baseline failure: a
// baseline that keeps learning through the very episode it is being used to
// judge converges on the drift and quietly stops calling it drift. It covers
// §7.6 as well as the alert phase — a subject learning through a zone outage
// decides the outage is normal and then reads the recovery as the anomaly.
//
// An edge is dirty because Frozen does not persist but UpdatedAt does, and a
// freeze advances UpdatedAt without changing anything else. Writing on the
// edge is what keeps a long freeze from looking like downtime to §9.3 step 7
// when the process does restart.
func (l *baselineLog) SetFrozen(k baselineKey, frozen bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.sets[k]
	if b == nil || !b.SetFrozen(frozen) {
		return false
	}
	l.dirty[k] = true
	return true
}

// Intent renders one subject-axis's learned normal as §5.1's shape, or nil
// where nothing is learned yet.
//
// Nil covers three states that are the same state downstream: no estimator,
// an immature one, and one whose maturity clock §9.3 step 7 restarted. All
// three mean the subject is scored against an apportionment over its eligible
// domains, which is what Tier C did before Phase 5 — not "unmonitored", but
// "measured against the only expectation available".
func (l *baselineLog) Intent(k baselineKey, now time.Time, cfg leeway.BaselineConfig) *leeway.Intent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sets[k].Intent(k.Key, now, cfg)
}

// Forget drops one subject-axis and queues its row for deletion.
//
// Called when a subject stops being tracked and when it loses an axis — the
// last node carrying a topology label going away is not a reason to keep
// learning about a label nothing has.
func (l *baselineLog) Forget(k baselineKey) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, had := l.sets[k]; !had {
		return
	}
	delete(l.sets, k)
	delete(l.dirty, k)
	l.gone[k] = true
}

// ForgetSubject drops every axis of one subject.
func (l *baselineLog) ForgetSubject(sub leeway.SubjectRef) {
	l.mu.Lock()
	keys := make([]baselineKey, 0, 2)
	for k := range l.sets {
		if k.Subject == sub {
			keys = append(keys, k)
		}
	}
	l.mu.Unlock()
	for _, k := range keys {
		l.Forget(k)
	}
}

// Load installs persisted baselines and applies §9.3 step 7's downtime rules,
// reporting how many came back and what was decided about each.
//
// A record that will not rebuild is dropped rather than repaired here;
// BaselineRecord.Set has already made that call, and its answer for a damaged
// row — nil, meaning nothing learned — is a state this package handles
// natively.
//
// Widened and stale sets come back dirty, because both changed something the
// row does not yet say. A resumed one does not: it is byte-for-byte what was
// read, and writing it back would make every restart a full-table rewrite.
func (l *baselineLog) Load(recs []leeway.BaselineRecord, now time.Time, cfg leeway.BaselineConfig) (int, map[leeway.DowntimeVerdict]int) {
	verdicts := make(map[leeway.DowntimeVerdict]int, 3)
	var n int

	l.mu.Lock()
	defer l.mu.Unlock()
	for _, rec := range recs {
		b := rec.Set()
		if b == nil {
			continue
		}
		sub, ok := leeway.ParseSubjectRef(rec.SubjectKey)
		if !ok || rec.TopologyKey == "" {
			// A row whose identity does not parse cannot be matched to a
			// subject, so it can never be compared, flushed or deleted by
			// key. Dropping it in memory leaves the row for the prune.
			continue
		}
		k := baselineKey{Subject: sub, Key: leeway.TopologyKey(rec.TopologyKey)}
		// A set already in memory wins. Load runs after arming and the
		// sampler starts with the queue, so a subject sampled in that window
		// has an estimator built from this cluster as it is now, which is
		// strictly better evidence than a row from before the restart.
		if _, live := l.sets[k]; live {
			continue
		}
		verdict := b.AssessDowntime(now, cfg)
		verdicts[verdict]++
		l.sets[k] = b
		if verdict != leeway.DowntimeResumed {
			l.dirty[k] = true
		}
		n++
	}
	return n, verdicts
}

// Flush drains what has moved since the last call: the records to write and
// the keys whose rows should go.
//
// Drained rather than copied, so a flush that fails loses those writes rather
// than retrying them forever. That is the right trade for this table and the
// wrong one for the alert table: a lost baseline write costs one flush
// interval of a twelve-hour half-life, while the same policy on a dwell timer
// would lose the transition §9.1 promises to keep. The next sample marks the
// set dirty again anyway.
func (l *baselineLog) Flush(cluster string) ([]leeway.BaselineRecord, []baselineKey) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var recs []leeway.BaselineRecord
	for k := range l.dirty {
		b := l.sets[k]
		if b == nil {
			continue
		}
		recs = append(recs, b.Record(cluster, k.Subject.String(), string(k.Key)))
	}
	gone := make([]baselineKey, 0, len(l.gone))
	for k := range l.gone {
		gone = append(gone, k)
	}

	l.dirty = make(map[baselineKey]bool)
	l.gone = make(map[baselineKey]bool)

	// Map iteration order is random, and a flush is a batch in one
	// transaction — sorting it keeps two runs over the same state producing
	// the same statement order, which is what makes a store test assertable
	// and a lock-ordering surprise reproducible.
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].SubjectKey != recs[j].SubjectKey {
			return recs[i].SubjectKey < recs[j].SubjectKey
		}
		return recs[i].TopologyKey < recs[j].TopologyKey
	})
	sort.Slice(gone, func(i, j int) bool {
		if a, b := gone[i].Subject.String(), gone[j].Subject.String(); a != b {
			return a < b
		}
		return gone[i].Key < gone[j].Key
	})
	return recs, gone
}

// Stats reports what the log holds, for §8.4's gauges.
type baselineStats struct {
	Tracked  int
	Mature   int
	Frozen   int
	Pending  int
	Outcomes map[leeway.ObserveOutcome]int64
}

// Stats counts the log at one instant. Maturity is evaluated rather than
// cached because it is a function of the clock — a set matures by sitting
// still, with no sample to notice it happening.
func (l *baselineLog) Stats(now time.Time, cfg leeway.BaselineConfig) baselineStats {
	l.mu.Lock()
	defer l.mu.Unlock()

	st := baselineStats{
		Tracked:  len(l.sets),
		Pending:  len(l.dirty),
		Outcomes: make(map[leeway.ObserveOutcome]int64, len(l.outcomes)),
	}
	for _, b := range l.sets {
		if b.Mature(now, cfg) {
			st.Mature++
		}
		if b.Frozen {
			st.Frozen++
		}
	}
	for out, n := range l.outcomes {
		st.Outcomes[out] = n
	}
	return st
}

// Keys snapshots what is being learned, so a caller can decide what to reap
// without holding the lock across whatever it consults to decide.
func (l *baselineLog) Keys() []baselineKey {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys := make([]baselineKey, 0, len(l.sets))
	for k := range l.sets {
		keys = append(keys, k)
	}
	return keys
}

// Len reports how many subject-axes are being learned.
func (l *baselineLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.sets)
}
