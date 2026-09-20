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
	"time"
)

// BaselineConfig is §7.5's behavioural-baseline configuration, plus §9.3 step
// 7's downtime rules. The zero value is not usable; call
// DefaultBaselineConfig, or Normalized on a partially-filled one.
type BaselineConfig struct {
	// HalfLife is the EWMA half-life: how long it takes a step change in a
	// domain's share to be half absorbed into "normal" (default 12 h).
	HalfLife time.Duration

	// K multiplies the learned dispersion to get the tolerance band
	// (default 4).
	K float64

	// FloorDeviation is the smallest dispersion the band may be built from
	// (default 0.05). Without it a subject that has sat at exactly 1/3 per
	// zone for a week learns a deviation of ~0 and then fires on a single
	// pod moving, which is the standard way a learned baseline becomes
	// useless the moment it becomes accurate.
	FloorDeviation float64

	// MinSamples and MinAge are §7.5's maturity gates: no Tier C finding
	// until the baseline has seen this many samples (default 200) *and* has
	// existed this long (default 6 h). Both, not either — a fast sampler
	// reaches 200 samples in under two hours, which is a good estimate of
	// the last two hours and no estimate at all of normal.
	MinSamples uint64
	MinAge     time.Duration

	// WidenAfter, WidenFactor and StaleAfter implement §9.3 step 7. A gap in
	// observation shorter than WidenAfter (default 2 h) is resumed as-is; a
	// longer one widens the bands by WidenFactor (default 1.5) for one
	// half-life; a gap past StaleAfter (default 24 h) is not a gap but a
	// different cluster, and the baseline restarts its maturity clock.
	WidenAfter  time.Duration
	WidenFactor float64
	StaleAfter  time.Duration
}

// DefaultBaselineConfig returns the §7.5 and §9.3 defaults.
func DefaultBaselineConfig() BaselineConfig {
	return BaselineConfig{
		HalfLife:       12 * time.Hour,
		K:              4,
		FloorDeviation: 0.05,
		MinSamples:     200,
		MinAge:         6 * time.Hour,
		WidenAfter:     2 * time.Hour,
		WidenFactor:    1.5,
		StaleAfter:     24 * time.Hour,
	}
}

// Normalized fills unset or nonsensical fields from the defaults.
//
// Every gate here fails *closed* — towards fewer and later Tier C findings —
// because a misconfigured baseline that fires is worse than one that does not:
// Tier C is the tier where nobody declared anything, so a false positive there
// is a page about a workload whose owner never asked us to watch its placement.
func (c BaselineConfig) Normalized() BaselineConfig {
	d := DefaultBaselineConfig()
	if c.HalfLife <= 0 {
		c.HalfLife = d.HalfLife
	}
	if c.K <= 0 {
		c.K = d.K
	}
	if c.FloorDeviation <= 0 {
		c.FloorDeviation = d.FloorDeviation
	}
	if c.MinSamples == 0 {
		c.MinSamples = d.MinSamples
	}
	if c.MinAge <= 0 {
		c.MinAge = d.MinAge
	}
	if c.WidenAfter <= 0 {
		c.WidenAfter = d.WidenAfter
	}
	if c.WidenFactor < 1 {
		c.WidenFactor = d.WidenFactor
	}
	if c.StaleAfter <= c.WidenAfter {
		c.StaleAfter = d.StaleAfter
	}
	return c
}

// Baseline is the learned normal for one domain: §7.5's EWMA of share and
// EWMA of absolute deviation from it.
//
// §7.5 sketches Samples, UpdatedAt and Frozen on this struct. They live on
// BaselineSet instead, because every domain of one (subject, topology key) is
// observed in the same pass from the same distribution: storing the sample
// count m times would cost m times the memory to hold m copies of one number,
// and — worse — would admit states where two domains of the same subject
// disagree about how much history they have, which nothing could produce and
// everything downstream would have to handle.
type Baseline struct {
	Share     float64 // EWMA of aᵢ/n
	Deviation float64 // EWMA of |sᵢ − Share|
}

// ObserveOutcome says what one Observe call did, so the caller can export
// §8.4's baseline SLIs without re-deriving it from the mutated set.
type ObserveOutcome uint8

// The outcomes.
const (
	// ObserveApplied moved the EWMAs.
	ObserveApplied ObserveOutcome = iota
	// ObserveSeeded was the first sample: the EWMAs were set to it rather
	// than dragged towards it from zero.
	ObserveSeeded
	// ObserveReset means the eligible domain set changed, so the old shares
	// described a different cluster and were discarded; the sample was then
	// taken as a seed.
	ObserveReset
	// ObserveHeld means the baseline is frozen and learned nothing.
	ObserveHeld
	// ObserveEmpty means there was nothing to learn from — no domains, or no
	// objects placed in them.
	ObserveEmpty
)

// String implements fmt.Stringer. These reach the `outcome` metric label.
func (o ObserveOutcome) String() string {
	switch o {
	case ObserveApplied:
		return "applied"
	case ObserveSeeded:
		return "seeded"
	case ObserveReset:
		return "reset"
	case ObserveHeld:
		return "held"
	case ObserveEmpty:
		return "empty"
	default:
		return "unknown"
	}
}

// BaselineSet is the learned normal for one (subject, topology key): one
// Baseline per eligible domain, plus the history those estimates rest on.
//
// The domain list is also the invalidation fingerprint (§7.5: "if the eligible
// domain set changes, reset the baseline — the old shares describe a different
// cluster"). It is stored in canonical SortDomains order and compared
// element-wise, so a set that reorders without changing does not reset.
type BaselineSet struct {
	// Domains and Baselines are index-aligned and in SortDomains order.
	Domains   []Domain
	Baselines []Baseline

	// Samples counts applied observations — seeds included, held ones not.
	Samples uint64

	// DevWeight is the total EWMA weight the deviation estimate has
	// accumulated, and it is what DeviationOf divides by.
	//
	// Share is seeded from the first sample, so it needs no such correction;
	// Deviation cannot be, because a single sample has nothing to deviate
	// from, so it starts at zero and climbs. With the default 12 h half-life
	// and a 6 h maturity gate, a baseline becomes usable having accumulated
	// only ~29% of its weight — so the raw EWMA understates the subject's
	// real dispersion by roughly 3.5×, and the band built from it is 3.5×
	// too tight. That is not a rounding error: it fires on every workload
	// whose placement legitimately moves around, which is the population
	// Tier C exists to *avoid* paging about. Dividing by the accumulated
	// weight is the standard bias correction and removes the artefact
	// exactly.
	DevWeight float64

	// FirstSeen is when the current estimate started accumulating, and is
	// what MinAge measures. A reset moves it; a freeze does not.
	FirstSeen time.Time
	// UpdatedAt is the timestamp of the last Observe call, held or not, and
	// is what dt is measured from.
	UpdatedAt time.Time

	// Frozen suspends learning. §7.5 requires it while a finding is firing;
	// the source also sets it while §7.6 suppression is in force, on the same
	// reasoning — a baseline that learns through a zone outage decides the
	// outage is normal and then decides the recovery is drift.
	Frozen bool

	// WidenUntil and WidenBy are §9.3 step 7's temporary tolerance after a
	// gap in observation. Zero means no widening.
	WidenUntil time.Time
	WidenBy    float64
}

// NewBaselineSet starts an empty baseline for a domain set. The domains are
// copied and sorted, so the caller may reuse its slice.
func NewBaselineSet(domains []Domain, now time.Time) *BaselineSet {
	b := &BaselineSet{FirstSeen: now, UpdatedAt: now}
	b.adopt(domains, now)
	return b
}

// adopt (re)points the set at a domain set, discarding any estimate.
func (b *BaselineSet) adopt(domains []Domain, now time.Time) {
	b.Domains = append([]Domain(nil), domains...)
	SortDomains(b.Domains)
	b.Baselines = make([]Baseline, len(b.Domains))
	b.Samples = 0
	b.DevWeight = 0
	b.FirstSeen = now
	// Widening is a statement about the estimate that was just thrown away,
	// so it goes with it. A fresh baseline is gated by maturity, which is a
	// far stronger brake than a 1.5× band.
	b.WidenUntil = time.Time{}
	b.WidenBy = 0
}

// sameDomains reports whether the set already describes exactly these domains.
// The argument must be in SortDomains order, which Observe guarantees.
func (b *BaselineSet) sameDomains(sorted []Domain) bool {
	if len(b.Domains) != len(sorted) {
		return false
	}
	for i := range sorted {
		if b.Domains[i] != sorted[i] {
			return false
		}
	}
	return true
}

// Observe folds one sample of a subject's placement into the baseline.
//
// domains and actual are index-aligned in the caller's order — the same pair
// Score takes — and need not be sorted: Observe sorts a copy and reorders the
// counts with it, because the caller's order is the eligible-domain order and
// the baseline's is its own fingerprint, and silently assuming they agree is
// how a baseline ends up learning zone-a's history under zone-b's name.
//
// Two behaviours matter more than the EWMA:
//
// A frozen set still advances UpdatedAt. If it did not, a subject frozen for
// three hours would thaw with dt=3h, α≈0.16 at a 12 h half-life, and absorb a
// sixth of the drift it was frozen to avoid absorbing in a single sample —
// each subsequent sample compounding it. Freezing has to stop the clock, not
// just the arithmetic.
//
// A sample with no objects teaches nothing and is not counted. A subject
// scaled to zero has no shares; recording zeros would teach the baseline that
// empty is normal, and then the scale-up would read as drift.
func (b *BaselineSet) Observe(domains []Domain, actual []int64, now time.Time, cfg BaselineConfig) ObserveOutcome {
	cfg = cfg.Normalized()
	if len(domains) == 0 || len(domains) != len(actual) {
		return ObserveEmpty
	}
	var total int64
	for _, a := range actual {
		total += a
	}
	if total <= 0 {
		return ObserveEmpty
	}

	sorted, shares := sharesByDomain(domains, actual, total)

	if b.Frozen {
		b.UpdatedAt = now
		return ObserveHeld
	}

	outcome := ObserveApplied
	switch {
	case !b.sameDomains(sorted):
		b.adopt(sorted, now)
		outcome = ObserveReset
	case b.Samples == 0:
		outcome = ObserveSeeded
	}

	if outcome != ObserveApplied {
		// Seed rather than converge. Starting the EWMA at zero means the
		// estimate spends a half-life climbing towards a share it already
		// knows, and the deviation it accumulates on the way up is an
		// artefact of the warm-up rather than a measurement of the subject.
		// Maturity would hide most of that, but not all of it: the inflated
		// deviation outlives the warm-up and shows up as a band far too wide
		// to catch anything.
		for i := range b.Baselines {
			b.Baselines[i] = Baseline{Share: shares[i]}
		}
		b.Samples = 1
		b.DevWeight = 0
		b.UpdatedAt = now
		return outcome
	}

	alpha := ewmaAlpha(now.Sub(b.UpdatedAt), cfg.HalfLife)
	for i := range b.Baselines {
		e := &b.Baselines[i]
		dev := math.Abs(shares[i] - e.Share)
		e.Share += alpha * (shares[i] - e.Share)
		e.Deviation += alpha * (dev - e.Deviation)
	}
	b.DevWeight += alpha * (1 - b.DevWeight)
	b.Samples++
	b.UpdatedAt = now
	return ObserveApplied
}

// sharesByDomain returns the domains in canonical order and their shares in
// the same order.
func sharesByDomain(domains []Domain, actual []int64, total int64) ([]Domain, []float64) {
	byDomain := make(map[Domain]int64, len(domains))
	for i, d := range domains {
		byDomain[d] += actual[i]
	}
	sorted := make([]Domain, 0, len(byDomain))
	for d := range byDomain {
		sorted = append(sorted, d)
	}
	SortDomains(sorted)
	shares := make([]float64, len(sorted))
	for i, d := range sorted {
		shares[i] = float64(byDomain[d]) / float64(total)
	}
	return sorted, shares
}

// ewmaAlpha is §7.5's smoothing factor, 1 − 2^(−dt/halfLife).
//
// dt comes from the clock rather than from a fixed sample interval, so a
// missed tick, a slow evaluation pass or a restart weights its sample by how
// much time it actually represents. A non-positive dt — two samples in the
// same instant, or a clock that went backwards — contributes nothing rather
// than a negative weight, which would move the estimate *away* from the
// sample.
func ewmaAlpha(dt, halfLife time.Duration) float64 {
	if dt <= 0 {
		return 0
	}
	if halfLife <= 0 {
		return 1
	}
	return 1 - math.Exp2(-float64(dt)/float64(halfLife))
}

// SetFrozen suspends or resumes learning, and reports whether it changed
// anything, so a caller can log the edges rather than every pass.
func (b *BaselineSet) SetFrozen(frozen bool) bool {
	if b.Frozen == frozen {
		return false
	}
	b.Frozen = frozen
	return true
}

// Mature reports whether §7.5's gates are both satisfied.
func (b *BaselineSet) Mature(now time.Time, cfg BaselineConfig) bool {
	cfg = cfg.Normalized()
	if b == nil || len(b.Domains) == 0 || b.Samples < cfg.MinSamples {
		return false
	}
	return !now.Before(b.FirstSeen.Add(cfg.MinAge))
}

// DeviationOf is a domain's learned dispersion, corrected for the EWMA's
// warm-up. See DevWeight for why the raw field is not the answer.
func (b *BaselineSet) DeviationOf(i int) float64 {
	if i < 0 || i >= len(b.Baselines) || b.DevWeight <= 0 {
		return 0
	}
	return b.Baselines[i].Deviation / b.DevWeight
}

// Band is the tolerance for one domain: k · max(deviation, floorDeviation),
// times any §9.3 widening still in force.
func (b *BaselineSet) Band(i int, now time.Time, cfg BaselineConfig) float64 {
	cfg = cfg.Normalized()
	if i < 0 || i >= len(b.Baselines) {
		return 0
	}
	dev := b.DeviationOf(i)
	if dev < cfg.FloorDeviation {
		dev = cfg.FloorDeviation
	}
	band := cfg.K * dev
	if b.WidenBy > 1 && now.Before(b.WidenUntil) {
		band *= b.WidenBy
	}
	return band
}

// Intent renders a mature baseline as the §7.5 Tier C intent, or nil.
//
// Returning an *Intent rather than a bespoke comparison type is the whole
// point: the scoring engine has one input shape, so a learned baseline
// apportions, scores, exports per-domain gauges and routes through exactly the
// code a declared TopologySpreadConstraint does. What makes it Tier C is the
// source field, which classify reads, and what makes it judged against a band
// instead of ρ is Bands, which breach reads.
//
// An immature baseline returns nil, which is the no-intent case: the subject
// keeps being scored against an even apportionment, exactly as it was before
// this phase existed. Tier C without a baseline is not "unmonitored", it is
// "measured against the only expectation available".
func (b *BaselineSet) Intent(key TopologyKey, now time.Time, cfg BaselineConfig) *Intent {
	if !b.Mature(now, cfg) {
		return nil
	}
	cfg = cfg.Normalized()
	shares := make(map[Domain]float64, len(b.Domains))
	bands := make(map[Domain]float64, len(b.Domains))
	for i, d := range b.Domains {
		shares[d] = b.Baselines[i].Share
		bands[d] = b.Band(i, now, cfg)
	}
	return &Intent{
		TopologyKey:    key,
		Mode:           ModeSpread,
		Source:         SourceLearnedBaseline,
		ExplicitShares: shares,
		Bands:          bands,
		Confidence:     ConfidenceLearned,
		Evidence: []EvidenceItem{{
			Source: SourceLearnedBaseline,
			Detail: "learned from " + strconv.FormatUint(b.Samples, 10) + " samples since " + b.FirstSeen.UTC().Format(time.RFC3339),
		}},
	}
}

// DowntimeVerdict is what §9.3 step 7 decided about a gap in observation.
type DowntimeVerdict uint8

// The step-7 outcomes.
const (
	// DowntimeResumed: the gap was short enough to ignore.
	DowntimeResumed DowntimeVerdict = iota
	// DowntimeWidened: bands are multiplied for one half-life.
	DowntimeWidened
	// DowntimeStale: the maturity clock restarts; Tier C is suppressed for
	// this subject until it re-matures.
	DowntimeStale
)

// String implements fmt.Stringer. Reaches the `outcome` metric label.
func (d DowntimeVerdict) String() string {
	switch d {
	case DowntimeResumed:
		return "resumed"
	case DowntimeWidened:
		return "widened"
	case DowntimeStale:
		return "stale"
	default:
		return "unknown"
	}
}

// AssessDowntime applies §9.3 step 7 to a baseline loaded from the store.
//
// The gap is measured from the set's own UpdatedAt rather than from a global
// "when did the process stop" for a reason worth stating: the two differ
// exactly when the process was running but this subject was not being
// observed — it was frozen, or scaled to zero, or absent — and those are cases
// where the estimate is just as old as it would be after a crash. One rule
// covers both.
//
// A stale baseline keeps its shares. They are the best available seed, and the
// maturity gate is what governs whether anything is fired from them; throwing
// them away would buy nothing and cost a day of relearning.
func (b *BaselineSet) AssessDowntime(now time.Time, cfg BaselineConfig) DowntimeVerdict {
	cfg = cfg.Normalized()
	gap := now.Sub(b.UpdatedAt)
	switch {
	case gap < cfg.WidenAfter:
		return DowntimeResumed
	case gap < cfg.StaleAfter:
		b.WidenBy = cfg.WidenFactor
		b.WidenUntil = now.Add(cfg.HalfLife)
		return DowntimeWidened
	}
	b.Samples = 0
	b.FirstSeen = now
	b.WidenBy = 0
	b.WidenUntil = time.Time{}
	return DowntimeStale
}

// Clone returns a deep copy, for handing a snapshot to a batched writer
// without holding the lock the sampler writes under.
func (b *BaselineSet) Clone() *BaselineSet {
	if b == nil {
		return nil
	}
	out := *b
	out.Domains = append([]Domain(nil), b.Domains...)
	out.Baselines = append([]Baseline(nil), b.Baselines...)
	return &out
}
