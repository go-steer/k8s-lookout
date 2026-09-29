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

package computeclass

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// §7.7.4's relative row: each axis's learned normal for mean achieved rank
// (#463). The estimator is §7.5's, unchanged; see pkg/leeway's rankbaseline.go
// for how a scalar is learned through it.
//
// # Why every change is written
//
// topologydrift writes only what moved and marks a freeze edge dirty, because
// it holds twenty thousand of these and a full rewrite every flush would be
// that many UPSERTs to record that a quiet estate is quiet. A cluster has
// seven compute classes. So here every sample is written, held ones included,
// and the reward is that a frozen set's UpdatedAt is never stale on disk: a
// long episode followed by a restart reads to §9.3 step 7 as what it was — a
// baseline that was watched the whole time — rather than as downtime.

// baselineRule is the discriminator a rank baseline is persisted under in the
// shared leeway_baseline table's topology_key column, with the provider folded
// in for the reason alertKey.stateKey gives.
const baselineRule = "rank-baseline"

// baselineStateKey renders an axis's topology_key column.
func baselineStateKey(axis leeway.AxisKey) string {
	return string(axis.Provider) + "/" + baselineRule
}

// parseBaselineKey inverts the persisted identity, refusing any row that is
// not a rank baseline. The table is shared with topologydrift, whose rows have
// a placed subject and a real topology key; claiming one would reap it.
func parseBaselineKey(sub, state string) (leeway.AxisKey, bool) {
	ref, ok := leeway.ParseSubjectRef(sub)
	if !ok || ref.Kind != leeway.SubjectPreferenceAxis {
		return leeway.AxisKey{}, false
	}
	provider, rule, ok := strings.Cut(state, "/")
	if !ok || provider == "" || rule != baselineRule {
		return leeway.AxisKey{}, false
	}
	return leeway.AxisKey{Provider: leeway.Provider(provider), Name: ref.Name}, true
}

// BaselineStore is §9.1's second persistence seam, satisfied by *store.Store.
//
// The same three methods topologydrift declares and the same table, kept apart
// by subject kind exactly as the alert rows are. Optional: a store without it
// learns in memory and relearns after a restart, which for a rule behind a
// six-hour maturity gate means six hours of saying nothing.
type BaselineStore interface {
	// LeewayBaselines returns one cluster's persisted baselines.
	LeewayBaselines(ctx context.Context, cluster string) ([]leeway.BaselineRecord, error)
	// PutLeewayBaselines writes a batch in one transaction.
	PutLeewayBaselines(ctx context.Context, recs []leeway.BaselineRecord) error
	// DeleteLeewayBaseline forgets one learned normal.
	DeleteLeewayBaseline(ctx context.Context, cluster, subjectKey, topologyKey string) error
}

// rankBaselines holds one estimator per axis. It has its own lock and never
// takes s.mu, so judgeAll may call it while holding s.mu.
type rankBaselines struct {
	mu    sync.Mutex
	sets  map[leeway.AxisKey]*leeway.BaselineSet
	dirty map[leeway.AxisKey]bool
	gone  map[leeway.AxisKey]bool
}

func newRankBaselines() *rankBaselines {
	return &rankBaselines{
		sets:  map[leeway.AxisKey]*leeway.BaselineSet{},
		dirty: map[leeway.AxisKey]bool{},
		gone:  map[leeway.AxisKey]bool{},
	}
}

// baseline renders one axis's learned normal for this version of it, or nil.
func (l *rankBaselines) baseline(axis *leeway.PreferenceAxis, now time.Time, cfg leeway.BaselineConfig) *leeway.RankBaseline {
	l.mu.Lock()
	defer l.mu.Unlock()
	return leeway.RankBaselineOf(l.sets[axis.Key], axis, now, cfg)
}

// observe folds one window into an axis's estimator, holding it frozen if
// asked to. A re-tiering needs no hook: the spec hash is in the domains, so
// the estimator resets itself on the first sample of the new tiers.
func (l *rankBaselines) observe(axis *leeway.PreferenceAxis, w leeway.RankWindow, frozen bool, now time.Time, cfg leeway.BaselineConfig) {
	domains, actual := leeway.RankBaselineSample(axis, w)

	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.sets[axis.Key]
	if b == nil {
		b = leeway.NewBaselineSet(domains, now)
		l.sets[axis.Key] = b
	}
	b.SetFrozen(frozen)
	out := b.Observe(domains, actual, now, cfg)
	if out != leeway.ObserveEmpty {
		l.dirty[axis.Key] = true
	}
	delete(l.gone, axis.Key)
}

// retain forgets every axis not in live and queues its row for deletion.
func (l *rankBaselines) retain(live map[leeway.AxisKey]struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key := range l.sets {
		if _, ok := live[key]; ok {
			continue
		}
		delete(l.sets, key)
		delete(l.dirty, key)
		l.gone[key] = true
	}
}

// load installs persisted baselines, applying §9.3 step 7, and reports how
// many came back. A set already learned in memory wins over its row.
func (l *rankBaselines) load(recs []leeway.BaselineRecord, now time.Time, cfg leeway.BaselineConfig) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	var n int
	for _, rec := range recs {
		key, ok := parseBaselineKey(rec.SubjectKey, rec.TopologyKey)
		if !ok {
			continue
		}
		if _, live := l.sets[key]; live {
			continue
		}
		b := rec.Set()
		if b == nil {
			continue
		}
		if b.AssessDowntime(now, cfg) != leeway.DowntimeResumed {
			l.dirty[key] = true
		}
		l.sets[key] = b
		n++
	}
	return n
}

// flush drains what has changed since the last call, in a stable order.
func (l *rankBaselines) flush(cluster string) ([]leeway.BaselineRecord, []leeway.AxisKey) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var recs []leeway.BaselineRecord
	for key := range l.dirty {
		if b := l.sets[key]; b != nil {
			recs = append(recs, b.Record(cluster, axisSubjectKey(key), baselineStateKey(key)))
		}
	}
	gone := make([]leeway.AxisKey, 0, len(l.gone))
	for key := range l.gone {
		gone = append(gone, key)
	}
	l.dirty = map[leeway.AxisKey]bool{}
	l.gone = map[leeway.AxisKey]bool{}

	sort.Slice(recs, func(i, j int) bool {
		if recs[i].SubjectKey != recs[j].SubjectKey {
			return recs[i].SubjectKey < recs[j].SubjectKey
		}
		return recs[i].TopologyKey < recs[j].TopologyKey
	})
	sort.Slice(gone, func(i, j int) bool { return gone[i].String() < gone[j].String() })
	return recs, gone
}

// axisSubjectKey is the §9.1 subject an axis is persisted under — the same
// one its alert rows use.
func axisSubjectKey(key leeway.AxisKey) string {
	return alertKey{Axis: key}.subjectKey()
}

// loadBaselines reads the persisted baselines after the barrier. Swallowed on
// failure: an unreadable baseline costs the rule six hours of abstaining and
// costs every other rule nothing.
func (s *Source) loadBaselines(ctx context.Context) {
	s.mu.Lock()
	st, cluster := s.baselineStore, s.cfg.Cluster
	s.mu.Unlock()
	if st == nil {
		return
	}
	recs, err := st.LeewayBaselines(ctx, cluster)
	if err != nil {
		s.logPrintf("%s: read persisted rank baselines: %v — relearning from scratch", Name, err)
		return
	}
	if n := s.baselines.load(recs, s.clock(), s.cfg.Baselines); n > 0 {
		s.logPrintf("%s: restored %d learned rank baseline(s)", Name, n)
	}
}

// flushBaselines writes what changed this pass. Drained, not retried: a lost
// write costs one alert interval of a twelve-hour half-life, and the next
// sample marks the set dirty again.
func (s *Source) flushBaselines(ctx context.Context) {
	s.mu.Lock()
	st, cluster := s.baselineStore, s.cfg.Cluster
	s.mu.Unlock()
	if st == nil {
		return
	}
	recs, gone := s.baselines.flush(cluster)
	if len(recs) > 0 {
		if err := st.PutLeewayBaselines(ctx, recs); err != nil {
			s.logPrintf("%s: persist %d rank baseline(s): %v", Name, len(recs), err)
		}
	}
	for _, key := range gone {
		if err := st.DeleteLeewayBaseline(ctx, cluster, axisSubjectKey(key), baselineStateKey(key)); err != nil {
			s.logPrintf("%s: delete rank baseline for %s: %v", Name, key, err)
		}
	}
}
