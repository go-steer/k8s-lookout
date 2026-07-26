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

package cloudcheck

// `cloud quota` (DESIGN.md §5, §10.2): the per-project usage/limit
// snapshot with nearest-to-exhaustion ranking — the read companion
// of the resident quota source. This command is exactly the "cheap
// 80%" of §10.2 (regions.get-style usage/limit pairs through the
// provider); slope→ETA math over Monitoring series is the SOURCE's
// job, not this snapshot's.
//
// Findings appear at/above --quota-warn (default 80%); critical is
// fixed at 95% (like `triage top`'s memory line, not a flag: a
// quota is incompressible — at the limit, scale-ups fail with
// GCE_QUOTA_EXCEEDED, and increase requests need lead time, §10.3).
// The summary's scanned counts EVERY quota examined, so
// "scanned=118 findings=0" reads as a genuinely clear project.
// Quotas with limit 0 (unentitled resources) cannot be rated and
// are counted but never listed.

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const (
	// defaultQuotaWarnPct is the --quota-warn default.
	defaultQuotaWarnPct = 80
	// quotaCritPct is where quota pressure turns critical.
	// Deliberately not a flag (package comment).
	quotaCritPct = 95.0
)

// QuotaCommand builds the `cloud quota` declaration around deps.
func QuotaCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "cloud quota",
		MCPName: "k8s_cloud_quota",
		Summary: "Per-project cloud quota usage vs limit, ranked nearest-to-exhaustion: findings from --quota-warn (default 80%), critical at 95% — quota is incompressible (scale-ups fail at the limit) and increases need lead time. Trend/ETA lives in the quota source.",
		Flags: []emit.FlagSpec{
			{Name: "quota-warn", Type: emit.FlagInt, Default: strconv.Itoa(defaultQuotaWarnPct),
				Help: "report a quota only at or above this percent of its limit (critical is fixed at 95%)"},
			{Name: "all", Type: emit.FlagBool, Default: "false",
				Help: "exploratory dump: emit every ratable quota regardless of --quota-warn (info severity below it), sorted by pct descending"},
		},
		Output: append([]checks.OutputField{
			{Name: "scope", Doc: "the quota's scope: a region name, or \"global\""},
			{Name: "usage", Doc: "current usage in the quota's own unit"},
			{Name: "limit", Doc: "the quota limit"},
			{Name: "unit", Doc: "the quota's unit, when the provider names one"},
			{Name: "pct", Doc: "usage as a percent of the limit, one decimal"},
		}, unavailableFields(cloud.CapabilityQuota)...),
		Examples: []string{
			"lookout cloud quota",
			"lookout cloud quota --quota-warn=60",
			"lookout cloud quota --all --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runQuota(ctx, deps, inv)
		},
	}
}

func runQuota(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if err := rejectClusterScope(inv, "cloud quota"); err != nil {
		return 0, err
	}
	warn := inv.Flags.Int("quota-warn")
	if warn < 1 || warn > 100 {
		return 0, emit.UsageErrorf("--quota-warn must be a percent in 1..100, got %d", warn)
	}
	all := inv.Flags.Bool("all")

	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	api, ok := provider.Quota()
	if !ok {
		return emitUnavailable(inv, provider, cloud.CapabilityQuota, "cloud quota")
	}
	quotas, err := api.Quotas(ctx)
	if err != nil {
		return 0, fmt.Errorf("quota inventory: %w", err)
	}

	type rated struct {
		q   cloud.QuotaUsage
		pct float64
	}
	var ratable []rated
	for _, q := range quotas {
		if q.Limit <= 0 {
			continue // unentitled resource: counted in scanned, unratable
		}
		ratable = append(ratable, rated{q: q, pct: q.Usage / q.Limit * 100})
	}
	// Nearest-to-exhaustion first; identity for determinism.
	sort.Slice(ratable, func(i, j int) bool {
		if ratable[i].pct != ratable[j].pct {
			return ratable[i].pct > ratable[j].pct
		}
		if ratable[i].q.Scope != ratable[j].q.Scope {
			return ratable[i].q.Scope < ratable[j].q.Scope
		}
		return ratable[i].q.Name < ratable[j].q.Name
	})

	for _, e := range ratable {
		if e.pct < float64(warn) && !all {
			continue
		}
		if err := inv.Out.Emit(quotaFinding(e.q, e.pct, float64(warn))); err != nil {
			return 0, err
		}
	}
	return len(quotas), nil
}

func quotaFinding(q cloud.QuotaUsage, pct, warn float64) emit.Finding {
	f := emit.Finding{
		Kind:         "quota.pressure",
		KindOfObject: "Quota",
		Name:         q.Name,
		Details: []emit.Field{
			{Key: "scope", Value: q.Scope},
			{Key: "usage", Value: fmtFloat(q.Usage)},
			{Key: "limit", Value: fmtFloat(q.Limit)},
		},
	}
	if q.Unit != "" {
		f.Details = append(f.Details, emit.Field{Key: "unit", Value: q.Unit})
	}
	f.Details = append(f.Details, emit.Field{Key: "pct", Value: fmtPct(pct)})
	switch {
	case pct >= 100:
		f.Severity = emit.SeverityCritical
		f.Reason = "QuotaExhausted"
		f.Message = fmt.Sprintf("%s exhausted in %s (%s/%s) — scale-ups fail with GCE_QUOTA_EXCEEDED until an increase lands", q.Name, q.Scope, fmtFloat(q.Usage), fmtFloat(q.Limit))
	case pct >= quotaCritPct:
		f.Severity = emit.SeverityCritical
		f.Reason = "QuotaNearLimit"
		f.Message = fmt.Sprintf("%s at %s%% of limit in %s — scale-ups fail at 100%%; increases need lead time, file now (§10.3)", q.Name, fmtPct(pct), q.Scope)
	case pct >= warn:
		f.Severity = emit.SeverityWarning
		f.Reason = "QuotaNearLimit"
		f.Message = fmt.Sprintf("%s at %s%% of limit in %s — critical from %.0f%%", q.Name, fmtPct(pct), q.Scope, quotaCritPct)
	default:
		// --all row below the line: numbers only (zero nominal
		// state applies to fields too).
		f.Severity = emit.SeverityInfo
	}
	return f
}
