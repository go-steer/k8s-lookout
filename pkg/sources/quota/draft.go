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

package quota

import (
	"fmt"
	"math"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// The §10.3 write path, lookout's half: DRAFT the increase request
// with slope-derived paperwork; never file it. The draft rides the
// quota.forecast inject as a structured payload
// (inject.PayloadQuotaDraft) so the agent can act on it through
// core-agent's PERMISSION GATE — that gate, not this code, is where
// a QuotaPreference create happens.

// draftLeadTime is the assumed provider turnaround for a quota
// increase request (documented input to the suggested-limit
// formula): a week is the conservative end of GCP's stated range for
// non-auto-approved compute increases.
const draftLeadTime = 7 * 24 * time.Hour

// draftHeadroomFactor is the step-function term of the formula: an
// increase that is not at least 1.5× the current limit is not worth
// the paperwork round-trip.
const draftHeadroomFactor = 1.5

// Draft composes the §10.3 increase-request draft for one watched
// quota. slopePerDay is the fitted usage growth per day (0 when no
// trustworthy fit — hasETA false); eta is the projected exhaustion
// horizon when hasETA.
//
// SUGGESTED-LIMIT FORMULA (normative for the draft, tested exactly):
//
//	suggested = ceil( max( limit × 1.5,
//	                       usage + 2 × slopePerDay × leadTimeDays ) )
//
// with leadTimeDays = 7 (draftLeadTime). The first term covers
// step-function growth (and is the whole answer when there is no
// usable slope); the second covers observed drift — enough headroom
// for TWICE the assumed request-approval leadtime at the current
// slope, so the increase does not need re-requesting the week it
// lands. ceil because quota limits are integral counts.
func Draft(q cloud.QuotaUsage, slopePerDay float64, eta time.Duration, hasETA bool, now time.Time) engine.QuotaIncreaseDraft {
	leadDays := draftLeadTime.Hours() / 24
	byFactor := q.Limit * draftHeadroomFactor
	bySlope := 0.0
	if slopePerDay > 0 {
		bySlope = q.Usage + 2*slopePerDay*leadDays
	}
	suggested := math.Ceil(math.Max(byFactor, bySlope))

	id := q.ID
	if id == "" {
		id = q.Name
	}
	return engine.QuotaIncreaseDraft{
		QuotaID:        id,
		Region:         q.Scope,
		Unit:           q.Unit,
		CurrentUsage:   q.Usage,
		CurrentLimit:   q.Limit,
		SuggestedLimit: suggested,
		SlopePerDay:    slopePerDay,
		Justification:  justification(q, slopePerDay, eta, hasETA, suggested, now),
	}
}

// justification is the human-grade paperwork (§10.3): observed
// state, observed slope, projected exhaustion, and what the
// suggested limit buys — deterministic prose generated from the same
// numbers the forecast fired on, ready for the increase-request form.
func justification(q cloud.QuotaUsage, slopePerDay float64, eta time.Duration, hasETA bool, suggested float64, now time.Time) string {
	base := fmt.Sprintf("%s in %s is at %s of %s%s (%.1f%%).",
		q.Name, q.Scope, formatQty(q.Usage), formatQty(q.Limit), unitSuffix(q.Unit), q.Usage/q.Limit*100)
	if !hasETA {
		return base + fmt.Sprintf(
			" Requesting an increase to %s (1.5x the current limit) to restore headroom before the next scale-up is blocked.",
			formatQty(suggested))
	}
	return base + fmt.Sprintf(
		" Usage grew %s/day over the observation window; at that slope the quota is exhausted in ~%s (around %s). Requesting an increase to %s to cover twice the expected request turnaround at the observed growth.",
		formatQty(slopePerDay), formatETA(eta), now.Add(eta).UTC().Format("2006-01-02"), formatQty(suggested))
}
