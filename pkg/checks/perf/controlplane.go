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

package perf

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// controlPlanePack is the pack `lookout health`'s control-plane
// category delegates to: apiserver — the cheapest meaningful
// control-plane latency read (one thresholded p99-by-verb/resource
// query) of the §5 "control-plane latency (perf probe packs)" row.
// The heavier packs (apf, etcd, startup) stay operator-invoked via
// `perf probe --pack=…`; a scorecard pass should not fan out into
// four Monitoring query batches.
const controlPlanePack = "apiserver"

// ControlPlaneProbe runs the apiserver pack against backend for the
// `health` scorecard's control-plane category (DESIGN.md §5:
// composition, not new checks — the category delegates to the same
// data-driven queries `perf probe --pack=apiserver` runs, and breach
// findings come back in exactly that command's shape).
//
// Semantics, mirroring runProbe's two degradation paths (§2, §11):
//
//   - findings hold the breaching series (the category is degraded);
//     none with scanned > 0 means every series stayed under its
//     thresholds (the category is healthy);
//   - absentMetric names the neutral metric when the workspace
//     POSITIVELY lacks it (cloud.ErrMetricAbsent — on GKE:
//     control-plane metrics not enabled). The capability is genuinely
//     absent, so the caller reports the category unavailable with the
//     pack_unavailable reason, never silence;
//   - err is a real backend failure (auth, transport, quota): the
//     caller fails the scan like any other category's read error.
//
// The caller must hold a working cloud.MetricsBackend — resolving
// provider capability absence (NoProvider, missing metrics) stays the
// caller's §2 responsibility.
func ControlPlaneProbe(ctx context.Context, backend cloud.MetricsBackend, now time.Time) (findings []emit.Finding, scanned int, absentMetric string, err error) {
	p := packs[controlPlanePack]
	w := cloud.TimeWindow{Start: now.Add(-p.window), End: now}
	for _, q := range p.queries {
		series, qerr := backend.QuerySeries(ctx, cloud.SeriesQuery{
			Metric:     q.metric,
			Matchers:   q.matchers,
			Window:     w,
			Step:       q.step,
			GroupBy:    q.groupBy,
			Percentile: q.percentile,
			Rate:       q.rate,
		})
		if errors.Is(qerr, cloud.ErrMetricAbsent) {
			absentMetric = q.metric
			continue
		}
		if qerr != nil {
			return nil, 0, "", fmt.Errorf("control-plane probe (pack %s): %s query: %w", controlPlanePack, q.metric, qerr)
		}
		kept := excludeSeries(series, q.exclude)
		scanned += len(kept)
		sortSeries(kept)
		for _, s := range kept {
			if f, breached := evaluate(controlPlanePack, q, s, p.window); breached {
				findings = append(findings, f)
			}
		}
	}
	return findings, scanned, absentMetric, nil
}
