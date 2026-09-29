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

package leeway

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// §7.7.4's relative row: mean achieved rank judged against the axis's own
// learned normal (#463).
//
// # Why a BaselineSet and not a scalar EWMA
//
// The quantity judged is a scalar, but it is learned through §7.5's estimator
// unchanged, with an axis's preference tiers as the "domains" and window
// pod-seconds per tier as the counts. Three things come free that a bespoke
// scalar would have to rebuild: the maturity gates, §9.3 step 7's downtime
// rules, and the persisted row — BaselineRecord already carries exactly this
// shape, and DevWeight with it.
//
// The mean is recovered exactly. The EWMA is linear, so Σ r·EWMA(sᵣ) is the
// EWMA of Σ r·sᵣ, which is the mean rank. The dispersion is not linear, so
// the band is Σ r·bandᵣ, which the triangle inequality makes an upper bound on
// the band a scalar estimator would have learned: exact on a two-tier axis,
// wider on a longer one. Wider is the direction every gate in §7.5 fails, for
// §7.5's reason — this is the tier where nobody declared anything.
//
// # Why the spec hash is in the domain
//
// The domain set is §7.5's invalidation fingerprint, so folding the axis's
// spec hash into each domain makes a re-tiering discard the learned normal by
// the estimator's own rule, in memory and across a restart alike. A mean rank
// learned against last month's priority list is a mean over different
// hardware, and comparing today's against it is comparing two questions.

// RankDomain names one preference tier of one version of an axis, as the
// domain the rank baseline learns it under.
func RankDomain(axis *PreferenceAxis, r Rank) Domain {
	return Domain(axis.SpecHash + "/" + strconv.Itoa(int(r)))
}

// rankOfDomain inverts RankDomain for a domain of this axis version.
func rankOfDomain(axis *PreferenceAxis, d Domain) (Rank, bool) {
	hash, n, ok := strings.Cut(string(d), "/")
	if !ok || hash != axis.SpecHash {
		return RankUnknown, false
	}
	r, err := strconv.Atoi(n)
	if err != nil || r < 0 {
		return RankUnknown, false
	}
	return Rank(r), true
}

// RankBaselineSample turns one window into the sample the rank baseline learns
// from: every preference tier of the axis, in tier order, with the window's
// pod-seconds at it.
//
// Every tier, not only the occupied ones. The domain set is the invalidation
// fingerprint, and a tier dropping to zero pod-seconds for one window is a
// change in the shares, not a different axis — leaving it out would reset the
// baseline every time a class recovered off its fallback.
//
// Pod-seconds are rounded to whole seconds, because the estimator counts. A
// window with under a pod-second in total has nothing to teach and comes back
// all zeros, which Observe reports as ObserveEmpty.
func RankBaselineSample(axis *PreferenceAxis, w RankWindow) ([]Domain, []int64) {
	tiers := tiersOf(axis)
	domains := make([]Domain, len(tiers))
	actual := make([]int64, len(tiers))
	for i, r := range tiers {
		domains[i] = RankDomain(axis, r)
		actual[i] = int64(math.Round(w.PodSeconds[r]))
	}
	return domains, actual
}

// RankBaseline is an axis's learned normal for mean achieved rank, as the
// rule reads it and as the finding reports it.
type RankBaseline struct {
	// Mean is the learned mean achieved rank, 0 being every pod-second at the
	// first choice.
	Mean float64 `json:"mean"`
	// Band is how far above Mean the window's mean may sit before the rule
	// breaches.
	Band float64 `json:"band"`

	Samples   uint64    `json:"samples"`
	FirstSeen time.Time `json:"firstSeen"`
}

// Ceiling is the highest window mean still inside the learned normal.
func (b RankBaseline) Ceiling() float64 { return b.Mean + b.Band }

// RankBaselineOf renders a set as the normal the rule judges against, or nil
// where there is none to judge against.
//
// Nil covers an immature set, and a set learned against a different version of
// the axis — one whose domains are not exactly this axis's tiers under this
// spec hash. The second is the pass between a re-tiering and the first sample
// of the new tiers: the estimator has not reset yet, because nothing has told
// it to, and the rule must not spend that pass comparing new tiers against the
// old ones' mean.
func RankBaselineOf(b *BaselineSet, axis *PreferenceAxis, now time.Time, cfg BaselineConfig) *RankBaseline {
	if !b.Mature(now, cfg) {
		return nil
	}
	tiers := tiersOf(axis)
	if len(b.Domains) != len(tiers) {
		return nil
	}
	want := make(map[Rank]bool, len(tiers))
	for _, r := range tiers {
		want[r] = true
	}
	out := &RankBaseline{Samples: b.Samples, FirstSeen: b.FirstSeen}
	for i, d := range b.Domains {
		// Domains are unique within a set, so as many domains as tiers, each
		// one of the tiers, is exactly the tier set.
		r, ok := rankOfDomain(axis, d)
		if !ok || !want[r] {
			return nil
		}
		out.Mean += float64(r) * b.Baselines[i].Share
		out.Band += float64(r) * b.Band(i, now, cfg)
	}
	return out
}
