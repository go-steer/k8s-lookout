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
	"time"
)

// TransientState is one of §7.6's five expected-skew conditions.
//
// "Expected" is the operative word: every one of these is a cluster doing
// something it was told to do, and the skew that comes with it is the mechanism
// working, not failing. §1's fourth premise — transient skew is normal — is the
// reason this file exists at all.
type TransientState uint8

// The transient states, in the precedence order Classify applies.
const (
	TransientNone TransientState = iota
	// TransientWarmup is §9.3's sentinel: informers have not finished syncing,
	// so the counts are a partial view of the cluster rather than a
	// distribution.
	TransientWarmup
	// TransientDomainOutage is a domain that lost more than half its ready
	// nodes. This is the row that separates a useful tool from a pager storm.
	TransientDomainOutage
	// TransientRollout is a workload mid-rollout.
	TransientRollout
	// TransientDrain is a node in the eligible set cordoned recently.
	TransientDrain
	// TransientScale is a replica count changed recently.
	TransientScale
)

// String implements fmt.Stringer. These reach a metric label, so they are
// stable identifiers rather than prose; the prose is Suppression.Reason.
func (s TransientState) String() string {
	switch s {
	case TransientWarmup:
		return "cluster-warmup"
	case TransientDomainOutage:
		return "domain-outage"
	case TransientRollout:
		return "rollout"
	case TransientDrain:
		return "node-drain"
	case TransientScale:
		return "recent-scale"
	default:
		return ""
	}
}

// TransientConfig is §7.6's and §10.1's timing. The zero value is not usable;
// call DefaultTransientConfig, or rely on normalize, which every entry point
// applies.
type TransientConfig struct {
	// ScaleSettleWindow is how long after a spec.replicas change a subject is
	// still settling (default 5 min).
	ScaleSettleWindow time.Duration
	// DrainSettleWindow is the same for a node becoming unschedulable
	// (default 10 min). Longer than the scale window because a drain moves
	// pods that are already running, and the rescheduling is bounded by
	// termination grace periods rather than by scheduling latency.
	DrainSettleWindow time.Duration
	// OutageWindow is the span §7.6's last row measures the ready-node drop
	// over (default 15 min).
	OutageWindow time.Duration
	// Multiplier is §7.6's transientThresholdMultiplier (default 2.5).
	Multiplier float64
}

// DefaultTransientConfig returns §7.6's defaults.
func DefaultTransientConfig() TransientConfig {
	return TransientConfig{
		ScaleSettleWindow: 5 * time.Minute,
		DrainSettleWindow: 10 * time.Minute,
		OutageWindow:      15 * time.Minute,
		Multiplier:        2.5,
	}
}

// normalize fills in what a partial config leaves out.
//
// A multiplier below 1 would be a *tightening* under a transient, which is the
// opposite of what the knob means, so it is treated as unset rather than
// honoured — this is the one place where refusing a user's number is right,
// because there is no reading of "relax the threshold by 0.5×" that is not a
// mistake.
func (c TransientConfig) normalize() TransientConfig {
	d := DefaultTransientConfig()
	if c.ScaleSettleWindow <= 0 {
		c.ScaleSettleWindow = d.ScaleSettleWindow
	}
	if c.DrainSettleWindow <= 0 {
		c.DrainSettleWindow = d.DrainSettleWindow
	}
	if c.OutageWindow <= 0 {
		c.OutageWindow = d.OutageWindow
	}
	if c.Multiplier < 1 {
		c.Multiplier = d.Multiplier
	}
	return c
}

// Transients are the §7.6 facts observed about one subject at evaluation time.
//
// They are facts, not detections: working out that a rollout is in progress
// means reading `observedGeneration`, ReplicaSet counts and `updatedReplicas`,
// which is what the `rollout` source already does, and §7.6 says to consume it
// rather than recompute it. This package cannot see a cluster (NFR-10), so the
// caller answers the five questions and this file decides what they mean.
type Transients struct {
	// RolloutInProgress is the rollout source's answer for this subject.
	RolloutInProgress bool
	// ScaledAt is when spec.replicas last changed, or the zero time if it has
	// not changed within memory.
	ScaledAt time.Time
	// DrainedAt is when a node in this subject's eligible set most recently
	// became unschedulable, or the zero time.
	DrainedAt time.Time
	// Warming reports that the §9.3 sentinel has not armed yet.
	Warming bool
	// DomainOutage reports that one of this subject's eligible domains is out.
	// See TransientConfig.DomainOutage for the test.
	DomainOutage bool
}

// Suppression is what §7.6 says to do about a subject's transient state.
//
// Suppress and Relax are the two halves of the table's "suppressed or evaluated
// against a relaxed multiplier", and which one a state gets is not arbitrary.
// A state is suppressed outright when it makes the counts *unreliable* — during
// warmup they are a partial view, and during a domain outage they describe a
// cluster that is not the one the expectation was computed for. A state is
// relaxed when the counts are correct and merely unflattering: a rollout, a
// drain and a scale event all produce a real, accurately measured distribution
// that simply has not finished moving yet.
type Suppression struct {
	State      TransientState
	Suppress   bool
	Relax      bool
	Multiplier float64
	Reason     string
}

// Classify applies §7.6's table.
//
// Precedence runs down the table as declared: warmup first because it makes
// every other signal here untrustworthy — a half-synced informer can invent a
// domain outage out of nodes it has not seen yet — then the outage, then the
// three relaxing states. Only the reported state is affected by the order; a
// subject that is both warming and mid-rollout is suppressed either way.
func (tr Transients) Classify(now time.Time, c TransientConfig) Suppression {
	c = c.normalize()
	switch {
	case tr.Warming:
		return Suppression{
			State:    TransientWarmup,
			Suppress: true,
			Reason:   "the cluster sentinel has not armed, so the counts are a partial view",
		}
	case tr.DomainOutage:
		return Suppression{
			State:    TransientDomainOutage,
			Suppress: true,
			Reason:   "an eligible domain lost more than half its ready nodes",
		}
	case tr.RolloutInProgress:
		return relaxed(TransientRollout, "a rollout is in progress", c.Multiplier)
	case recent(now, tr.DrainedAt, c.DrainSettleWindow):
		return relaxed(TransientDrain, "a node was cordoned inside the drain settle window", c.Multiplier)
	case recent(now, tr.ScaledAt, c.ScaleSettleWindow):
		return relaxed(TransientScale, "the replica count changed inside the scale settle window", c.Multiplier)
	default:
		return Suppression{}
	}
}

func relaxed(s TransientState, reason string, mult float64) Suppression {
	return Suppression{State: s, Relax: true, Multiplier: mult, Reason: reason}
}

// recent reports whether at falls inside the window ending now.
//
// A timestamp slightly in the future counts: the kubelet, the API server and
// this process do not share a clock, and skew of a few seconds around an event
// that just happened is exactly the moment suppression matters most. One that
// is more than a whole window in the future does not — that is not skew, it is
// bad data, and honouring it would suppress the subject permanently.
func recent(now, at time.Time, window time.Duration) bool {
	if at.IsZero() {
		return false
	}
	age := now.Sub(at)
	return age < window && age > -window
}

// ReadyCount is one observation of a domain's ready node count.
type ReadyCount struct {
	At    time.Time
	Ready int64
}

// DomainOutage reports §7.6's last row for one domain: the ready node count has
// dropped by more than half inside the outage window.
//
// The comparison is against the *highest* count seen in the window, not the
// oldest sample in it. A zone that went 9 → 4 → 4 over ten minutes is out, and
// an oldest-sample rule would stop saying so the moment the 9 aged out of a
// sliding window — the outage would un-detect itself while still in progress,
// which is the one behaviour this row exists to prevent.
//
// history may be in any order and may contain samples outside the window; they
// are ignored. The current count is the caller's live reading rather than the
// newest sample, because the decision is about the cluster now.
func (c TransientConfig) DomainOutage(history []ReadyCount, ready int64, now time.Time) bool {
	cutoff := now.Add(-c.normalize().OutageWindow)
	var peak int64
	for _, h := range history {
		if h.At.After(cutoff) && h.Ready > peak {
			peak = h.Ready
		}
	}
	if ready > peak {
		peak = ready
	}
	// Strictly more than half, so a clean 4 → 2 halving is not an outage. Two
	// of four nodes gone is a rolling node-pool upgrade; three of four is a
	// zone.
	return peak > 0 && ready*2 < peak
}

// Relaxed returns the thresholds with §7.6's multiplier applied.
//
// It multiplies the *tolerances* and leaves the *eligibility* gate alone.
// MinReplicasForScoring answers "does this subject have enough objects for a
// distribution to mean anything", which is a property of the subject and not of
// what the cluster happens to be doing to it; scaling it would make a transient
// silently change which subjects are measured at all, and the §7.3 metrics are
// recorded even for subjects that are not judged.
//
// MaxDomainShare is capped at 1. A share cannot exceed 1, so 1.25 and 1.0 are
// equally unreachable and both mean "do not escalate during a transient" —
// but only one of them is a number that can be printed in a finding without
// looking like a bug.
func (t Thresholds) Relaxed(mult float64) Thresholds {
	if mult <= 1 {
		return t
	}
	t.Drift *= mult
	t.MaxDomainShare = math.Min(1, t.MaxDomainShare*mult)
	// Both small-n numbers round up: they are counts of objects that must be
	// misplaced before anyone is told, and rounding a relaxation down would
	// relax by less than asked.
	t.SmallNThreshold = scaleCount(t.SmallNThreshold, mult)
	t.MinRelocationSmallN = scaleCount(t.MinRelocationSmallN, mult)
	return t
}

func scaleCount(n int64, mult float64) int64 {
	if n <= 0 {
		return n
	}
	return int64(math.Ceil(float64(n) * mult))
}

// JudgeTransient is Judge under §7.6's transient-state rules.
//
// The relaxation reaches the drift path and nothing else, and that is not an
// oversight of the plumbing — it is the rule. The contract rules compare
// observed counts against numbers the *user* declared, a DoNotSchedule maxSkew
// and a required anti-affinity's per-domain ceiling, and multiplying somebody
// else's stated bound by 2.5 because a rollout is running substitutes our
// judgement for theirs. A rollout does not license breaking a promise the
// scheduler is supposed to be keeping.
//
// Suppression is the opposite: it covers Tier A too. That is the point of the
// domain-outage row — a zone dies, every DoNotSchedule constraint in the
// cluster is violated in the same second, and §14's exit criterion is one
// finding rather than four hundred.
func (s *Scores) JudgeTransient(intent *Intent, t Thresholds, sup Suppression) Verdict {
	if sup.Suppress {
		return Verdict{Transient: sup.State, Suppressed: true, Reason: sup.Reason}
	}
	if sup.Relax {
		t = t.Relaxed(sup.Multiplier)
	}
	v := s.Judge(intent, t)
	v.Transient = sup.State
	v.Relaxed = sup.Relax
	if sup.Relax && v.Breached {
		v.Reason += "; " + sup.Reason + ", and the relaxed threshold was exceeded anyway"
	}
	return v
}
