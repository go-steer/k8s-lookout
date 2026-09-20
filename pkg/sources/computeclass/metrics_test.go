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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	corev1 "k8s.io/api/core/v1"
)

// promHarness is §8.4's export pipeline in miniature: OTEL-native instruments
// on a MeterProvider whose pull reader is registered into a Prometheus
// registry the process already owns.
type promHarness struct {
	registry *prometheus.Registry
	source   *Source
}

// newPromHarness builds a source whose state exercises EVERY instrument. A
// series with no observation is not exported at all, so anything left out here
// silently drops out of the name and docs assertions below.
func newPromHarness(t *testing.T) *promHarness {
	t.Helper()
	reg := prometheus.NewRegistry()
	exporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutTargetInfo(),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		t.Fatalf("otelprom.New: %v", err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	s := newTestSource(t)
	s.cfg.Meter = mp.Meter(MeterName)
	if err := s.startMetrics(); err != nil {
		t.Fatalf("startMetrics: %v", err)
	}
	t.Cleanup(func() { _ = s.closeMetrics() })

	now := t0
	s.now = func() time.Time { return now }

	s.UpsertClass("n4-preferred", spec(t, n4PreferredSpec), t0)

	// One node per condition the source can report.
	s.UpsertNode(node("at-rank-0", "n4-preferred", "n4", "0"), t0)   // clean
	s.UpsertNode(node("disagreeing", "n4-preferred", "c3", "0"), t0) // annotation != inferred
	s.UpsertNode(node("pending", "n4-preferred", "e2", ""), t0)      // no rank yet, unmatched
	s.UpsertNode(node("stale", "n4-preferred", "n4", "9"), t0)       // index the class lost
	s.UpsertNode(node("unfit", "n4-preferred", "e2", "ccc_no_rule_matching"), t0)
	s.UpsertNode(node("outside", "n4-preferred", "e2", "ccc_scale_up_anyway"), t0)
	s.UpsertNode(node("unreadable", "broken", "n4", "0"), t0)

	// A rule field pkg/leeway does not model, so the matcher fails closed and
	// names the field.
	s.UpsertClass("future", map[string]any{
		"priorities": []any{
			map[string]any{"machineFamily": "n4"},
			map[string]any{"quantumEntanglement": "required"},
		},
	}, t0)
	s.UpsertNode(node("future-node", "future", "n4", ""), t0)

	// A partially-scored class: no well-defined order, so every node on it
	// reports axis_invalid.
	s.UpsertClass("half-scored", map[string]any{
		"priorities": []any{
			map[string]any{"machineFamily": "n4", "priorityScore": int64(10)},
			map[string]any{"machineFamily": "c3"},
		},
	}, t0)
	s.UpsertNode(node("confused", "half-scored", "n4", "0"), t0)

	// A class that will not decode, so decode_errors_total has a row.
	s.UpsertClass("broken", map[string]any{"priorities": "not a list"}, t0)

	// Two nodes matched by more than one rule.
	s.UpsertClass("ambiguous", map[string]any{
		"priorities": []any{
			map[string]any{"minCores": int64(1)},
			map[string]any{"machineFamily": "n4"},
		},
	}, t0)
	s.UpsertNode(node("both", "ambiguous", "n4", ""), t0)

	// Pods, so the pod-second counters and the pods gauge have rows, and a
	// node move so transitions_total does.
	for _, n := range []string{"at-rank-0", "unfit", "outside", "pending"} {
		s.UpsertPod(pod("pod-"+n, n, corev1.PodRunning), t0)
	}
	now = at(30 * time.Second)
	s.UpsertNode(node("at-rank-0", "n4-preferred", "c3", "1"), now)

	// One unbalanced Leave, so tracker_underflows_total is exported. It is the
	// one series we hope never moves in production, which means it has to move
	// here or it cannot be checked at all.
	s.tracker.Leave(s.nodes["at-rank-0"].axis, 2, now)

	now = at(60 * time.Second)
	return &promHarness{registry: reg, source: s}
}

func (h *promHarness) names(t *testing.T) []string {
	t.Helper()
	families, err := h.registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make([]string, 0, len(families))
	for _, f := range families {
		out = append(out, f.GetName())
	}
	slices.Sort(out)
	return out
}

func (h *promHarness) family(t *testing.T, name string) *dto.MetricFamily {
	t.Helper()
	families, err := h.registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

// TestInstrumentNames_PrometheusSpelling pins the derivation the exporter
// performs, which §8.4 relies on rather than hand-maintaining two lists.
//
// The piece that is easy to get wrong is the unit: it is injected into the
// MIDDLE of the name, ahead of any `_total`, so `pod_time` with unit `s`
// reaches /metrics as `pod_time_seconds_total` — which is the name we want and
// the reason the instrument is not called `pod_time_seconds`.
func TestInstrumentNames_PrometheusSpelling(t *testing.T) {
	h := newPromHarness(t)
	want := []string{
		"lookout_leeway_preference_ambiguous",
		"lookout_leeway_preference_axis_info",
		"lookout_leeway_preference_axis_invalid",
		"lookout_leeway_preference_decode_errors_total",
		"lookout_leeway_preference_disagreement",
		"lookout_leeway_preference_no_rule_matching",
		"lookout_leeway_preference_nodes",
		"lookout_leeway_preference_off_axis",
		// Unit "s" lands ahead of the _total on both pod-second counters.
		"lookout_leeway_preference_pod_time_seconds_total",
		"lookout_leeway_preference_pods",
		"lookout_leeway_preference_out_of_range",
		"lookout_leeway_preference_rank_pending",
		"lookout_leeway_preference_rank_weighted_time_seconds_total",
		// No unit, so `_total` is simply appended.
		"lookout_leeway_preference_tracker_underflows_total",
		"lookout_leeway_preference_transitions_total",
		"lookout_leeway_preference_unmatched",
		"lookout_leeway_preference_unreadable_class_nodes",
		"lookout_leeway_preference_unsupported_rules",
	}
	slices.Sort(want)
	if got := h.names(t); !slices.Equal(got, want) {
		t.Errorf("exported metric names:\n got %q\nwant %q", got, want)
	}
}

// TestMetricDocs_MatchTheExporter is what makes MetricDocs safe to publish.
// The docs generator cannot derive these rows — there is no
// prometheus.Collector to Describe — so names, types, help and labels are
// hand-written, and this holds them to what actually came out.
func TestMetricDocs_MatchTheExporter(t *testing.T) {
	h := newPromHarness(t)

	docs := MetricDocs()
	names := make([]string, 0, len(docs))
	for _, d := range docs {
		names = append(names, d.Name)
	}
	slices.Sort(names)
	if got := h.names(t); !slices.Equal(got, names) {
		t.Fatalf("MetricDocs names:\n got %q\nwant %q", names, got)
	}

	promType := map[dto.MetricType]string{
		dto.MetricType_GAUGE:     "gauge",
		dto.MetricType_COUNTER:   "counter",
		dto.MetricType_HISTOGRAM: "histogram",
	}
	for _, d := range docs {
		t.Run(d.Name, func(t *testing.T) {
			fam := h.family(t, d.Name)
			if fam == nil {
				t.Fatalf("not exported")
			}
			if got := promType[fam.GetType()]; got != d.Type {
				t.Errorf("type = %q, doc says %q", got, d.Type)
			}
			if got := fam.GetHelp(); got != d.Help {
				t.Errorf("help =\n %q\ndoc says\n %q", got, d.Help)
			}
			var got []string
			for _, lp := range fam.GetMetric()[0].GetLabel() {
				got = append(got, lp.GetName())
			}
			want := slices.Clone(d.Labels)
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("labels = %q, doc says %q", got, want)
			}
		})
	}
}

// TestMetricHelpHasNoDoubleQuote. The docs generator renders help strings into
// a table and the exporter writes them into /metrics; a double quote breaks
// one of the two, and which one depends on the renderer.
func TestMetricHelpHasNoDoubleQuote(t *testing.T) {
	for _, d := range MetricDocs() {
		if strings.Contains(d.Help, `"`) {
			t.Errorf("%s: help contains a double quote:\n%s", d.Name, d.Help)
		}
	}
}

// TestObservedValues checks the numbers, not just the shapes — the harness
// state is contrived, but each of these is a different way the accounting
// could be wrong without any name or label changing.
func TestObservedValues(t *testing.T) {
	h := newPromHarness(t)

	// Four pods on n4-preferred nodes, thirty seconds before the one at rank 0
	// fell to rank 1, sixty seconds total.
	if got := sampleValue(t, h, "lookout_leeway_preference_pod_time_seconds_total", map[string]string{"rank": "0"}); got != 30 {
		t.Errorf("rank-0 pod-seconds = %v, want 30", got)
	}
	if got := sampleValue(t, h, "lookout_leeway_preference_pod_time_seconds_total", map[string]string{"rank": "1"}); got != 30 {
		t.Errorf("rank-1 pod-seconds = %v, want 30", got)
	}
	// Mean achieved rank over the tiers: 30 pod-seconds at rank 1 and 30 at
	// rank 0 is a weighted total of 30.
	if got := sampleValue(t, h, "lookout_leeway_preference_rank_weighted_time_seconds_total",
		map[string]string{"axis": "n4-preferred"}); got != 30 {
		t.Errorf("rank-weighted seconds = %v, want 30", got)
	}
	// The pending node's pod never resolves, so it sits at rank `unknown` for
	// the whole window. One pod, sixty seconds.
	if got := sampleValue(t, h, "lookout_leeway_preference_pod_time_seconds_total", map[string]string{"rank": "unknown"}); got != 60 {
		t.Errorf("unknown-rank pod-seconds = %v, want 60", got)
	}

	checks := []struct {
		metric string
		axis   string
		want   float64
	}{
		// Two: the c3 node annotated 0, and the stale node annotated 9 on a
		// class whose n4 rule inference placed it at. An out-of-range index is
		// a disagreement as well as an out-of-range — inference named a rule,
		// the annotation named a different one, and the fact that the different
		// one no longer exists does not make the two agree.
		{"lookout_leeway_preference_disagreement", "n4-preferred", 2},
		{"lookout_leeway_preference_rank_pending", "n4-preferred", 1},
		{"lookout_leeway_preference_out_of_range", "n4-preferred", 1},
		{"lookout_leeway_preference_no_rule_matching", "n4-preferred", 1},
		{"lookout_leeway_preference_off_axis", "n4-preferred", 1},
		{"lookout_leeway_preference_axis_invalid", "half-scored", 1},
		{"lookout_leeway_preference_ambiguous", "ambiguous", 1},
	}
	for _, c := range checks {
		if got := sampleValue(t, h, c.metric, map[string]string{"axis": c.axis}); got != c.want {
			t.Errorf("%s{axis=%q} = %v, want %v", c.metric, c.axis, got, c.want)
		}
	}

	// The unsupported-rule row names the offending field, which is the whole
	// reason the matcher fails closed rather than ignoring what it cannot read.
	if got := sampleValue(t, h, "lookout_leeway_preference_unsupported_rules",
		map[string]string{"field": "quantumEntanglement"}); got != 1 {
		t.Errorf("unsupported_rules{field=quantumEntanglement} = %v, want 1", got)
	}
	// A node on a class that did not decode is unreadable, not rank 0.
	if got := sampleValue(t, h, "lookout_leeway_preference_unreadable_class_nodes",
		map[string]string{"class": "broken"}); got != 1 {
		t.Errorf("unreadable_class_nodes{class=broken} = %v, want 1", got)
	}
	if got := sampleValue(t, h, "lookout_leeway_preference_decode_errors_total",
		map[string]string{"class": "broken"}); got != 1 {
		t.Errorf("decode_errors_total{class=broken} = %v, want 1", got)
	}
	if got := sampleValue(t, h, "lookout_leeway_preference_tracker_underflows_total", nil); got != 1 {
		t.Errorf("tracker_underflows_total = %v, want 1", got)
	}
}

// TestAxisInfoCarriesTheOrdering pins the label an operator reads to know
// whether rank came from list position or from priorityScore — and the
// declared-versus-assumed distinction §7.7.4 gates a finding on.
func TestAxisInfoCarriesTheOrdering(t *testing.T) {
	h := newPromHarness(t)
	labels := sampleLabels(t, h, "lookout_leeway_preference_axis_info", map[string]string{"axis": "n4-preferred"})
	for key, want := range map[string]string{
		"provider":         "gke-computeclass",
		"ordering":         "list-position",
		"rules":            "3",
		"tiers":            "3",
		"scale_up":         "scale-up-anyway",
		"active_migration": "true",
	} {
		if got := labels[key]; got != want {
			t.Errorf("axis_info %s = %q, want %q", key, got, want)
		}
	}
	// A partially-scored class is exported too, reading `invalid` — the point
	// is that somebody can see it, not that it scores.
	if got := sampleLabels(t, h, "lookout_leeway_preference_axis_info",
		map[string]string{"axis": "half-scored"})["ordering"]; got != "invalid" {
		t.Errorf("half-scored ordering = %q, want invalid", got)
	}
	// And whenUnsatisfiable absent reads as `unset`, not as GKE's documented
	// default.
	if got := sampleLabels(t, h, "lookout_leeway_preference_axis_info",
		map[string]string{"axis": "ambiguous"})["scale_up"]; got != "unset" {
		t.Errorf("undeclared scale_up = %q, want unset", got)
	}
}

// TestNodesSeriesKeepsRuleIdentityApartFromRank. rank is the tier, rule_index
// is the raw annotation, and a node with neither reads `none` rather than 0 —
// which is exactly what a naive default would invent, and 0 is the most
// preferred position.
func TestNodesSeriesKeepsRuleIdentityApartFromRank(t *testing.T) {
	h := newPromHarness(t)
	fam := h.family(t, "lookout_leeway_preference_nodes")
	if fam == nil {
		t.Fatal("nodes series not exported")
	}

	seen := map[string]string{} // rule_index -> rank
	for _, m := range fam.GetMetric() {
		l := labelsOf(m)
		if l["axis"] != "n4-preferred" {
			continue
		}
		seen[l["rule_index"]] = l["rank"]
	}
	// The stale node's annotation names index 9, which the class no longer
	// has: identity preserved, rank unknowable.
	if got := seen["9"]; got != "unknown" {
		t.Errorf("rule_index 9 reports rank %q, want unknown — a stale index has no rank", got)
	}
	// The sentinel nodes carry no rule at all.
	if got := seen["none"]; got != "unsatisfiable" && got != "off-axis" {
		t.Errorf("rule_index none reports rank %q, want a sentinel rank", got)
	}
	// And the rendered rule is there for the nodes that have one.
	for _, m := range fam.GetMetric() {
		l := labelsOf(m)
		if l["axis"] == "n4-preferred" && l["rule_index"] == "1" && l["rule"] == "" {
			t.Error("a resolved node reported an empty rendered rule")
		}
	}
}

func labelsOf(m *dto.Metric) map[string]string {
	out := map[string]string{}
	for _, lp := range m.GetLabel() {
		out[lp.GetName()] = lp.GetValue()
	}
	return out
}

// sampleValue sums every sample of one family whose labels match the filter.
func sampleValue(t *testing.T, h *promHarness, name string, match map[string]string) float64 {
	t.Helper()
	fam := h.family(t, name)
	if fam == nil {
		t.Fatalf("%s: not exported", name)
	}
	var total float64
	var found bool
	for _, m := range fam.GetMetric() {
		if !matches(labelsOf(m), match) {
			continue
		}
		found = true
		if g := m.GetGauge(); g != nil {
			total += g.GetValue()
		}
		if c := m.GetCounter(); c != nil {
			total += c.GetValue()
		}
	}
	if !found {
		t.Fatalf("%s: no sample matching %v", name, match)
	}
	return total
}

// sampleLabels returns the labels of the single sample matching the filter.
func sampleLabels(t *testing.T, h *promHarness, name string, match map[string]string) map[string]string {
	t.Helper()
	fam := h.family(t, name)
	if fam == nil {
		t.Fatalf("%s: not exported", name)
	}
	for _, m := range fam.GetMetric() {
		if l := labelsOf(m); matches(l, match) {
			return l
		}
	}
	t.Fatalf("%s: no sample matching %v", name, match)
	return nil
}

func matches(labels, want map[string]string) bool {
	for k, v := range want {
		if labels[k] != v {
			return false
		}
	}
	return true
}
