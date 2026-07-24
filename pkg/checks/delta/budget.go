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

package delta

// The pdb and quota classes: budgets with zero headroom. A PDB at
// disruptionsAllowed=0 gridlocks node drains and upgrades; a
// ResourceQuota at its hard limit silently blocks every scale-up in
// the namespace while nothing looks "down".

import (
	"sort"
	"strconv"

	"github.com/go-steer/k8s-lookout/pkg/emit"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
)

// checkPDBs derives pdb.gridlocked. Only PDBs with pods behind them
// are reported: a selector matching nothing has no drain to block.
func (s *scanner) checkPDBs(pdbs []policyv1.PodDisruptionBudget) {
	for i := range pdbs {
		p := &pdbs[i]
		st := p.Status
		if st.DisruptionsAllowed > 0 || st.ExpectedPods == 0 {
			continue
		}
		// Below budget is an active violation; exactly at budget
		// means any single disruption would violate it.
		sev, reason := emit.SeverityWarning, "DisruptionsBlocked"
		if st.CurrentHealthy < st.DesiredHealthy {
			sev, reason = emit.SeverityCritical, "BudgetViolated"
		}
		s.add(emit.Finding{
			Kind:         "pdb.gridlocked",
			Severity:     sev,
			Namespace:    p.Namespace,
			KindOfObject: "PodDisruptionBudget",
			Name:         p.Name,
			Reason:       reason,
			Details: []emit.Field{
				{Key: "healthy", Value: itoa32(st.CurrentHealthy)},
				{Key: "required", Value: itoa32(st.DesiredHealthy)},
				{Key: "pods", Value: itoa32(st.ExpectedPods)},
			},
		})
	}
}

// checkQuotas derives quota.exhausted / quota.near per resource of
// each ResourceQuota, from the quota controller's own status.
func (s *scanner) checkQuotas(quotas []corev1.ResourceQuota) {
	for i := range quotas {
		q := &quotas[i]
		// Deterministic per-resource order within one quota.
		names := make([]string, 0, len(q.Status.Hard))
		for name := range q.Status.Hard {
			names = append(names, string(name))
		}
		sort.Strings(names)

		for _, name := range names {
			hard := q.Status.Hard[corev1.ResourceName(name)]
			used, ok := q.Status.Used[corev1.ResourceName(name)]
			if !ok {
				continue // controller has not accounted it yet
			}
			if hard.IsZero() {
				// hard=0 forbids the resource entirely; that
				// is configuration, not a delta, unless
				// something is somehow charged against it.
				if used.IsZero() {
					continue
				}
			}
			pct := 100
			if !hard.IsZero() {
				pct = int(used.AsApproximateFloat64() / hard.AsApproximateFloat64() * 100)
			}
			exhausted := used.Cmp(hard) >= 0
			if !exhausted && pct < s.th.quotaWarn {
				continue
			}
			kind, sev, reason := "quota.near", emit.SeverityWarning, "QuotaNearLimit"
			if exhausted {
				kind, sev, reason = "quota.exhausted", emit.SeverityCritical, "QuotaExhausted"
			}
			s.add(emit.Finding{
				Kind:         kind,
				Severity:     sev,
				Namespace:    q.Namespace,
				KindOfObject: "ResourceQuota",
				Name:         q.Name,
				Reason:       reason,
				Details: []emit.Field{
					{Key: "resource", Value: name},
					{Key: "used", Value: used.String()},
					{Key: "hard", Value: hard.String()},
					{Key: "pct", Value: strconv.Itoa(pct)},
				},
			})
		}
	}
}
