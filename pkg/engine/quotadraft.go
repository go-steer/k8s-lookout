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

package engine

// QuotaIncreaseDraft is the §10.3 quota write path's first half: a
// DRAFTED increase request the quota source attaches to its
// quota.forecast signals. lookout only ever DRAFTS — the structured
// request rides the inject payload so the agent can file it through
// core-agent's permission gate; nothing in this repository calls a
// QuotaPreference create (or any other quota mutation) directly.
//
// SuggestedLimit is derived from the observed slope (the formula is
// documented at the one place that computes it,
// pkg/sources/quota.Draft); Justification is the human-grade
// paperwork generated from the same slope math, ready to paste into
// the increase request.
type QuotaIncreaseDraft struct {
	// QuotaID is the provider's canonical increase-request
	// identifier when the provider maps one (GCP: the Cloud Quotas
	// API "<service>/<quotaId>" pair a QuotaPreference names, e.g.
	// "compute.googleapis.com/CPUS-per-project-region"), else the
	// inventory quota name (e.g. "CPUS").
	QuotaID string
	// Region is the quota's scope: a region for regional quotas,
	// "global" otherwise.
	Region string
	// Unit is the quota's unit when the provider reports one.
	Unit string
	// CurrentUsage / CurrentLimit are the observed values the
	// suggestion was computed from.
	CurrentUsage float64
	CurrentLimit float64
	// SuggestedLimit is the drafted new limit (see
	// pkg/sources/quota.Draft for the formula).
	SuggestedLimit float64
	// SlopePerDay is the fitted usage growth per day over the
	// source's history window.
	SlopePerDay float64
	// Justification is the slope-derived prose for the request form.
	Justification string
}
