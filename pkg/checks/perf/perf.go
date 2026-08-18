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

// Package perf implements `lookout perf probe` (DESIGN.md §5):
// control-plane and startup performance via data-driven metrics
// query packs — apiserver p99 by verb/resource, APF queue saturation
// + 429 rejects, etcd fsync p99 + DB size, pod-startup p95 trend.
// Queries go exclusively through the provider's cloud.MetricsBackend
// (§2: pkg/checks never imports cloud SDKs) in the backend-neutral
// SeriesQuery shape (§15 Q4), and pack definitions are data
// (packs.go).
//
// Two explicit degradation paths, never silence (§2, §11):
//
//   - no provider / no metrics capability → the standard
//     cloud.unavailable finding + `unavailable` summary marker, exit
//     0 with scanned=0 (mirrors the cloudcheck group);
//   - a pack metric positively absent from the workspace
//     (cloud.ErrMetricAbsent — on GKE: control-plane metrics not
//     enabled) → one warning-severity perf.pack_unavailable finding
//     naming the metric and the remedy, while the pack's remaining
//     queries still run. The operator asked for this pack by name;
//     an absent half is a finding, not an info line.
package perf

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func init() {
	checks.Register(Command(Deps{}))
}

// Deps are the injectable seams, mirroring pkg/checks/cloudcheck:
// the zero value is production wiring; tests inject a fake provider
// and clock (§13).
type Deps struct {
	// Provider yields the cloud provider. Nil means cloud.New
	// default detection (the NoProvider sentinel on vanilla builds —
	// the command then reports unavailable, never silence, §2).
	Provider func(ctx context.Context) (cloud.Provider, error)
	// Now anchors the query window's end. Nil means time.Now.
	Now func() time.Time
}

func (d Deps) provider(ctx context.Context) (cloud.Provider, error) {
	if d.Provider != nil {
		return d.Provider(ctx)
	}
	return cloud.New(ctx, cloud.Config{})
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Command builds the `perf probe` declaration around deps.
func Command(deps Deps) checks.Command {
	return checks.Command{
		Name:    "perf probe",
		MCPName: "k8s_perf_probe",
		Summary: "Control-plane and startup performance via metrics query packs: --pack=apiserver (p99 latency by verb/resource), apf (queue saturation + 429 rejects), etcd (WAL fsync p99 + DB size), startup (pod-first-ready p95 trend); apf/etcd need GKE control-plane metrics enabled — absence degrades to an explicit pack_unavailable finding.",
		Flags: []emit.FlagSpec{
			{Name: "pack", Type: emit.FlagString, Default: "",
				Help: "which query pack to run (required): " + packNames()},
		},
		Kinds: []checks.KindField{
			checks.Kind("perf.apiserver_p99", "apiserver request latency p99 crossed the pack threshold for a verb/resource — warning from 1s, critical from 4s", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("perf.apf_saturation", "an API Priority and Fairness level is holding a sustained queue — warning from 10 queued, critical from 100", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("perf.apf_rejects", "APF is shedding load: the apiserver is returning 429s at a priority level", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("perf.etcd_fsync", "etcd WAL fsync p99 crossed the pack threshold — warning from 10ms, critical from 100ms", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("perf.etcd_db_size", "the etcd database is approaching its quota — warning from 4 GiB, critical from 5.5 GiB", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("perf.startup_p95", "pod first-ready p95 crossed the pack threshold — warning from 60s, critical from 300s", emit.SeverityCritical, emit.SeverityWarning),
			checks.Kind("perf.pack_unavailable", "a metric the requested pack needs is not in the metrics workspace, so part of the pack could not run; the rest still did (§11: no coverage lies)", emit.SeverityWarning),
			checks.CloudUnavailableKind(),
		},
		Output: []checks.OutputField{
			{Name: "pack", Doc: "the pack this finding belongs to; also the summary-line note naming the pack that ran"},
			{Name: "metric", Doc: "the backend-neutral metric the query measured (pack_unavailable: the absent metric)"},
			{Name: "verb", Doc: "apiserver request verb for this series (apiserver pack)"},
			{Name: "resource", Doc: "apiserver request resource for this series (apiserver pack)"},
			{Name: "priority_level", Doc: "APF priority level for this series (apf pack)"},
			{Name: "code", Doc: "the HTTP status code the query matched (apf pack: 429)"},
			{Name: "observed", Doc: "the worst (maximum) aligned value in the window, in the query's unit — the breach basis"},
			{Name: "latest", Doc: "the newest aligned value in the window"},
			{Name: "threshold", Doc: "the crossed threshold: the critical one when severity=critical, else the warning one"},
			{Name: "window", Doc: "the lookback the series cover (--since, or the pack default); also a summary-line note"},
			{Name: "trend", Doc: "startup pack: second-half vs first-half mean delta of the window, e.g. \"+34%\" — the p95 trend direction"},
			{Name: "capability", Doc: "cloud.unavailable: the provider capability this command needed (metrics)"},
			{Name: "provider", Doc: "cloud.unavailable: the provider that was asked"},
			{Name: "unavailable", Doc: "summary-line note (§2 marker): why the metrics backend could not be served"},
		},
		Examples: []string{
			"lookout perf probe --pack=apiserver",
			"lookout perf probe --pack=apf",
			"lookout perf probe --pack=etcd --since=6h",
			"lookout perf probe --pack=startup",
			"lookout perf probe --pack=apiserver --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runProbe(ctx, deps, inv)
		},
	}
}

func runProbe(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	// Control-plane read: the §4.2 k8s scoping flags are a usage
	// error, not a silent no-op (same guard as the cloud group).
	if inv.Scope.Namespace != "" || inv.Scope.AllNamespaces || !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("perf probe reads control-plane metrics, not cluster objects: --namespace/-A/--workload do not apply")
	}
	packName := inv.Flags.String("pack")
	if packName == "" {
		return 0, emit.UsageErrorf("--pack is required: one of %s", packNames())
	}
	p, ok := packs[packName]
	if !ok {
		return 0, emit.UsageErrorf("unknown pack %q: valid packs are %s", packName, packNames())
	}
	window := inv.Scope.Since
	if window == 0 {
		window = p.window
	}

	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	backend, ok := provider.Metrics()
	if !ok {
		return emitUnavailable(inv, provider, "perf probe --pack="+packName)
	}

	now := deps.now()
	w := cloud.TimeWindow{Start: now.Add(-window), End: now}
	scanned := 0
	for _, q := range p.queries {
		series, err := backend.QuerySeries(ctx, cloud.SeriesQuery{
			Metric:     q.metric,
			Matchers:   q.matchers,
			Window:     w,
			Step:       q.step,
			GroupBy:    q.groupBy,
			Percentile: q.percentile,
			Rate:       q.rate,
		})
		if errors.Is(err, cloud.ErrMetricAbsent) {
			// §5: detect and degrade with an explicit
			// pack_unavailable finding, not silence — and keep
			// running the pack's other queries.
			if err := inv.Out.Emit(packUnavailableFinding(packName, q)); err != nil {
				return 0, err
			}
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("pack %s: %s query: %w", packName, q.metric, err)
		}
		kept := excludeSeries(series, q.exclude)
		scanned += len(kept)
		sortSeries(kept)
		for _, s := range kept {
			if f, breached := evaluate(packName, q, s, window); breached {
				if err := inv.Out.Emit(f); err != nil {
					return 0, err
				}
			}
		}
	}
	if err := inv.Out.Note("pack", packName); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("window", window.String()); err != nil {
		return 0, err
	}
	return scanned, nil
}

// emitUnavailable is the §2 no-provider/no-capability degradation:
// one explicit cloud.unavailable finding, the summary marker, exit 0
// with scanned=0 (mirrors cloudcheck.emitUnavailable).
func emitUnavailable(inv emit.Invocation, p cloud.Provider, what string) (int, error) {
	u := cloud.Unavailable(p, cloud.CapabilityMetrics)
	if err := inv.Out.Emit(emit.Finding{
		Kind:     "cloud.unavailable",
		Severity: emit.SeverityInfo,
		Reason:   "CapabilityUnavailable",
		Message:  fmt.Sprintf("%s needs the provider %s capability: %s", what, u.Capability, u.Reason),
		Details: []emit.Field{
			{Key: "capability", Value: string(u.Capability)},
			{Key: "provider", Value: u.Provider},
		},
	}); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("unavailable", u.Reason); err != nil {
		return 0, err
	}
	return 0, nil
}

// packUnavailableFinding is the per-query absence degradation:
// warning severity because the operator asked for this pack by name.
func packUnavailableFinding(packName string, q packQuery) emit.Finding {
	return emit.Finding{
		Kind:     "perf.pack_unavailable",
		Severity: emit.SeverityWarning,
		Reason:   "MetricAbsent",
		Message: fmt.Sprintf("pack %s: metric %s is not in the project's metrics workspace — enable GKE control-plane metrics for the API server / etcd components; the pack's other queries still ran",
			packName, q.metric),
		Details: []emit.Field{
			{Key: "pack", Value: packName},
			{Key: "metric", Value: q.metric},
		},
	}
}

// excludeSeries applies the query's client-side label exclusions
// (see packQuery.exclude for why this cannot be a matcher).
func excludeSeries(series []cloud.Series, exclude map[string][]string) []cloud.Series {
	if len(exclude) == 0 {
		return series
	}
	var kept []cloud.Series
	for _, s := range series {
		drop := false
		for label, values := range exclude {
			got, ok := s.Labels[label]
			if !ok {
				continue
			}
			for _, v := range values {
				if got == v {
					drop = true
					break
				}
			}
		}
		if !drop {
			kept = append(kept, s)
		}
	}
	return kept
}

// sortSeries orders series by their label values in labelDetailKeys
// order, so output is deterministic regardless of backend ordering.
func sortSeries(series []cloud.Series) {
	key := func(s cloud.Series) string {
		var b strings.Builder
		for _, k := range labelDetailKeys {
			b.WriteString(s.Labels[k])
			b.WriteByte('\x00')
		}
		return b.String()
	}
	sort.Slice(series, func(i, j int) bool { return key(series[i]) < key(series[j]) })
}

// evaluate applies the breach semantics (packs.go package comment)
// to one series: critical at max >= crit, warning at max > warn.
func evaluate(packName string, q packQuery, s cloud.Series, window time.Duration) (emit.Finding, bool) {
	if len(s.Points) == 0 {
		return emit.Finding{}, false
	}
	observed := s.Points[0].Value
	for _, p := range s.Points {
		if p.Value > observed {
			observed = p.Value
		}
	}
	latest := s.Points[len(s.Points)-1].Value

	var severity string
	threshold := q.warn
	switch {
	case observed >= q.crit:
		severity = emit.SeverityCritical
		threshold = q.crit
	case observed > q.warn:
		severity = emit.SeverityWarning
	default:
		return emit.Finding{}, false // zero nominal state
	}

	f := emit.Finding{
		Kind:     q.findingKind,
		Severity: severity,
		Reason:   q.reason,
		Details: []emit.Field{
			{Key: "pack", Value: packName},
			{Key: "metric", Value: q.metric},
		},
	}
	// Group labels from the series, then matcher-pinned ones the
	// reduction stripped (e.g. code=429 after the total-rate
	// collapse), in fixed declared-key order.
	var labelParts []string
	for _, k := range labelDetailKeys {
		v, ok := s.Labels[k]
		if !ok {
			v, ok = q.matchers[k]
		}
		if ok && v != "" {
			f.Details = append(f.Details, emit.Field{Key: k, Value: v})
			labelParts = append(labelParts, k+"="+v)
		}
	}
	f.Details = append(f.Details,
		emit.Field{Key: "observed", Value: q.format(observed)},
		emit.Field{Key: "latest", Value: q.format(latest)},
		emit.Field{Key: "threshold", Value: q.format(threshold)},
		emit.Field{Key: "window", Value: window.String()},
	)
	labelText := ""
	if len(labelParts) > 0 {
		labelText = " for " + strings.Join(labelParts, " ")
	}
	f.Message = fmt.Sprintf("%s peaked at %s%s in the last %s — above the %s threshold %s: %s",
		q.what, q.format(observed), labelText, window, severity, q.format(threshold), q.why)
	if q.trend {
		if trend, ok := trendPct(s); ok {
			f.Details = append(f.Details, emit.Field{Key: "trend", Value: trend})
		}
	}
	return f, true
}

// trendPct is the §5 "P95 trend": split the window's points in
// half, compare the halves' means, report the delta percent with an
// explicit sign ("+34%"). Needs both halves populated and a nonzero
// baseline; otherwise the field is omitted (zero nominal state
// applies to fields too).
func trendPct(s cloud.Series) (string, bool) {
	n := len(s.Points)
	if n < 2 {
		return "", false
	}
	mean := func(points []cloud.Point) float64 {
		sum := 0.0
		for _, p := range points {
			sum += p.Value
		}
		return sum / float64(len(points))
	}
	first := mean(s.Points[:n/2])
	second := mean(s.Points[n/2:])
	if first <= 0 {
		return "", false
	}
	return fmt.Sprintf("%+d%%", int(math.Round((second-first)/first*100))), true
}
