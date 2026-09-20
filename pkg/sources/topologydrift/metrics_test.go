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

package topologydrift

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	v1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// promHarness is the §8.4 export pipeline in miniature: OTEL-native
// instruments on a MeterProvider whose pull reader is registered into a
// Prometheus registry the process already owns.
type promHarness struct {
	registry *prometheus.Registry
	in       *instruments
}

func newPromHarness(t *testing.T, opts metricsOptions) *promHarness {
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

	opts.Meter = mp.Meter(MeterName)
	in, err := newInstruments(opts)
	if err != nil {
		t.Fatalf("newInstruments: %v", err)
	}
	t.Cleanup(func() { _ = in.Close() })
	return &promHarness{registry: reg, in: in}
}

// names returns the metric names the registry currently exposes.
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

// family returns the gathered family with the given name.
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

// fullOptionsDrift is the drift the harness's one subject reports. Above
// DefaultPerDomainSeriesMinDrift, so the default posture — no All, the shipped
// floor — still exports its per-domain series; the gate tests move the floor
// rather than the subject.
const fullOptionsDrift = 0.2

func fullOptions() metricsOptions {
	return metricsOptions{
		PerDomain: PerDomainGate{All: true},
		SubjectCounts: func() map[leeway.SubjectKind]int64 {
			return map[leeway.SubjectKind]int64{leeway.SubjectDeployment: 3, leeway.SubjectDaemonSet: 1}
		},
		DomainNodes: func() map[leeway.TopologyKey]map[leeway.Domain]int64 {
			return map[leeway.TopologyKey]map[leeway.Domain]int64{
				zoneKey: {"us-central1-a": 7, "us-central1-b": 5},
			}
		},
		// The stub applies the gate, as Source.domainObjects does: a harness
		// whose walker yielded unconditionally would make every gate assertion
		// below pass for the wrong reason.
		DomainObjects: func(g PerDomainGate, yield countObserver) {
			if !g.Admits(fullOptionsDrift, true) {
				return
			}
			yield(subA, zoneKey, "us-central1-a", leeway.StateRunning, 4)
			yield(subA, zoneKey, "us-central1-b", leeway.StatePending, 1)
		},
		Evaluations: func(yield evalObserver) {
			yield(subA, &Evaluation{
				Key: zoneKey,
				// Under a transient, so that the §7.6 gauge has a row: the
				// series only exists when something is being suppressed or
				// relaxed, and fullOptions has to exercise every series
				// MetricDocs claims.
				Suppression: leeway.Suppression{
					State:      leeway.TransientRollout,
					Relax:      true,
					Multiplier: 2.5,
					Reason:     "a rollout is in progress",
				},
				Scores: leeway.Scores{
					Domains:        []leeway.Domain{"us-central1-a", "us-central1-b"},
					Actual:         []int64{4, 1},
					Expected:       []int64{3, 2},
					Total:          5,
					ObservedSkew:   3,
					ExcessSkew:     2,
					Relocation:     1,
					Drift:          fullOptionsDrift,
					MaxDomainShare: 0.8,
					Evaluable:      true,
				},
			})
		},
		Alerts: func(yield alertObserver) {
			yield(subA, zoneKey, leeway.AlertState{Phase: leeway.PhaseFiring}, leeway.TierB)
		},
		Baselines: func() baselineStats {
			return baselineStats{
				Tracked: 9, Mature: 4, Frozen: 2,
				Outcomes: map[leeway.ObserveOutcome]int64{
					leeway.ObserveApplied: 120,
					leeway.ObserveReset:   1,
				},
			}
		},
		Intents: func(yield intentObserver) {
			skew := int32(1)
			yield(subA, zoneKey, &leeway.Intent{
				TopologyKey:       zoneKey,
				Mode:              leeway.ModeSpread,
				Source:            leeway.SourceTopologySpreadConstraint,
				MaxSkew:           &skew,
				WhenUnsatisfiable: v1.DoNotSchedule,
				Confidence:        leeway.ConfidenceDeclared,
				Weighting:         leeway.WeightEqual,
			})
		},
	}
}

// TestInstrumentNames_PrometheusSpelling pins the derived Prometheus names
// against the real exporter.
//
// §8.4 declares instruments in OpenTelemetry form and lets the exporter derive
// the Prometheus spelling, on the grounds that hand-maintaining two lists
// guarantees drift. The derivation is a dependency's behaviour, and the one
// piece of it that is easy to get wrong is the unit: it is injected into the
// *middle* of the name, ahead of any `_total`, so an instrument that sets
// WithUnit gets a name nobody chose. This test is what turns that from a
// paragraph in a design document into something a dependency bump cannot break
// silently.
func TestInstrumentNames_PrometheusSpelling(t *testing.T) {
	h := newPromHarness(t, fullOptions())
	ctx := context.Background()
	h.in.recordEvent(ctx, resourcePod, time.Unix(1_700_000_000, 0))
	h.in.recordEvaluation(ctx, leeway.SubjectDeployment, 250*time.Millisecond)
	// A counter with no Add is not exported at all, so the one metric we hope
	// never moves in production has to move here or it cannot be checked.
	h.in.recordMismatch(ctx, leeway.SubjectDeployment)

	want := []string{
		"lookout_leeway_domain_objects",
		"lookout_leeway_domain_expected",
		"lookout_leeway_domain_ready_nodes",
		"lookout_leeway_intent_info",
		// The §7.3 scores. None of them sets a unit — ρ and a share are
		// dimensionless, and a skew and a relocation distance are counts of
		// objects, which has no UCUM spelling worth injecting into the middle
		// of the name.
		"lookout_leeway_observed_skew",
		"lookout_leeway_excess_skew",
		"lookout_leeway_relocation_distance",
		"lookout_leeway_drift",
		"lookout_leeway_max_domain_share",
		// Unit "s" lands before nothing at all on a gauge, so the name reads
		// as the Prometheus convention for a unix timestamp.
		"lookout_leeway_last_event_timestamp_seconds",
		"lookout_leeway_evaluation_duration_seconds",
		"lookout_leeway_subjects_tracked",
		// §8.2's episodes. A gauge rather than a counter, because the question
		// it answers is "what is firing now", not "how many ever did".
		"lookout_leeway_alert_state",
		"lookout_leeway_transient_subjects",
		// The other half of the unit hazard: `_total` is appended to a
		// monotonic counter, and it is appended AFTER any unit. This one sets
		// no unit, so the declared name simply gains the suffix.
		"lookout_leeway_counter_mismatch_total",
		// §7.5's two. The gauge is unconditional — it is how an operator
		// watches maturity climb, so a zero has to be a zero and not an
		// absence — and the counter is the one series whose sustained
		// non-zero reading (outcome="reset") is a silent failure.
		"lookout_leeway_baselines",
		"lookout_leeway_baseline_samples_total",
	}
	slices.Sort(want)

	if got := h.names(t); !slices.Equal(got, want) {
		t.Errorf("exported metric names:\n got %q\nwant %q", got, want)
	}
}

// TestMetricDocs_MatchTheExporter is what makes MetricDocs safe to publish.
//
// The docs generator cannot derive these rows the way it derives every other
// sentinel metric — there is no prometheus.Collector to Describe — so the
// names, types and labels are written by hand. This gathers a real bridged
// registry with every optional series turned on and holds the hand-written
// rows to what actually came out.
func TestMetricDocs_MatchTheExporter(t *testing.T) {
	h := newPromHarness(t, fullOptions())
	ctx := context.Background()
	h.in.recordEvent(ctx, resourcePod, time.Unix(1_700_000_000, 0))
	h.in.recordEvaluation(ctx, leeway.SubjectDeployment, 250*time.Millisecond)
	// A counter with no Add is not exported at all, so the one metric we hope
	// never moves in production has to move here or it cannot be checked.
	h.in.recordMismatch(ctx, leeway.SubjectDeployment)

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

// TestMetricDocs_OptionalMatchesTheGate pins the Optional column: exactly the
// rows marked optional are the ones §8.4's per-domain gate can withhold.
func TestMetricDocs_OptionalMatchesTheGate(t *testing.T) {
	opts := fullOptions()
	// A floor no subject can reach — drift is R/n, so it never exceeds 1.
	opts.PerDomain = PerDomainGate{MinDrift: 2}
	h := newPromHarness(t, opts)
	ctx := context.Background()
	h.in.recordEvent(ctx, resourcePod, time.Unix(1_700_000_000, 0))
	h.in.recordEvaluation(ctx, leeway.SubjectDeployment, 250*time.Millisecond)
	// A counter with no Add is not exported at all, so the one metric we hope
	// never moves in production has to move here or it cannot be checked.
	h.in.recordMismatch(ctx, leeway.SubjectDeployment)

	exported := h.names(t)
	for _, d := range MetricDocs() {
		present := slices.Contains(exported, d.Name)
		if present == d.Optional {
			t.Errorf("%s: exported=%v below the per-domain floor, but Optional=%v", d.Name, present, d.Optional)
		}
	}
}

func TestInstruments_ObservableGaugesCarryTheirAttributes(t *testing.T) {
	h := newPromHarness(t, fullOptions())

	t.Run("subjects_tracked is split by kind", func(t *testing.T) {
		f := h.family(t, "lookout_leeway_subjects_tracked")
		if f == nil {
			t.Fatal("family absent")
		}
		got := map[string]float64{}
		for _, m := range f.GetMetric() {
			got[labelValue(m, "subject_kind")] = m.GetGauge().GetValue()
		}
		if got["Deployment"] != 3 || got["DaemonSet"] != 1 {
			t.Errorf("subjects_tracked = %v, want Deployment 3 and DaemonSet 1", got)
		}
	})

	t.Run("domain_ready_nodes is split by axis and domain", func(t *testing.T) {
		f := h.family(t, "lookout_leeway_domain_ready_nodes")
		if f == nil {
			t.Fatal("family absent")
		}
		if len(f.GetMetric()) != 2 {
			t.Fatalf("got %d series, want 2", len(f.GetMetric()))
		}
		for _, m := range f.GetMetric() {
			if labelValue(m, "topology_key") != string(zoneKey) {
				t.Errorf("topology_key = %q, want %q", labelValue(m, "topology_key"), zoneKey)
			}
			if d := labelValue(m, "domain"); d != "us-central1-a" && d != "us-central1-b" {
				t.Errorf("unexpected domain %q", d)
			}
		}
	})

	t.Run("domain_objects carries the subject and the state", func(t *testing.T) {
		f := h.family(t, "lookout_leeway_domain_objects")
		if f == nil {
			t.Fatal("family absent")
		}
		var running *dto.Metric
		for _, m := range f.GetMetric() {
			if labelValue(m, "state") == "running" {
				running = m
			}
		}
		if running == nil {
			t.Fatal("no running series")
		}
		for key, want := range map[string]string{
			"subject_kind": "Deployment",
			"namespace":    "prod",
			"subject":      "a",
			"domain":       "us-central1-a",
		} {
			if got := labelValue(running, key); got != want {
				t.Errorf("%s = %q, want %q", key, got, want)
			}
		}
		if running.GetGauge().GetValue() != 4 {
			t.Errorf("value = %v, want 4", running.GetGauge().GetValue())
		}
	})
}

func TestInstruments_AlertStateEncodesThePhaseAndKeepsIt(t *testing.T) {
	// §8.4's 0/1/2. Resolving deliberately reads 2 rather than a fourth value:
	// the finding is still outstanding, so `alert_state == 2` counts exactly
	// the episodes somebody has been told about and not yet told are over. The
	// phase itself stays reachable as a label.
	episodes := []struct {
		phase leeway.AlertPhase
		want  float64
	}{
		{leeway.PhasePending, 1},
		{leeway.PhaseFiring, 2},
		{leeway.PhaseResolving, 2},
	}

	opts := fullOptions()
	opts.Alerts = func(yield alertObserver) {
		for i, e := range episodes {
			sub := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: fmt.Sprintf("web-%d", i)}
			yield(sub, zoneKey, leeway.AlertState{Phase: e.phase}, leeway.TierA)
		}
	}
	h := newPromHarness(t, opts)

	f := h.family(t, "lookout_leeway_alert_state")
	if f == nil {
		t.Fatal("family absent")
	}
	if len(f.GetMetric()) != len(episodes) {
		t.Fatalf("got %d series, want one per episode (%d)", len(f.GetMetric()), len(episodes))
	}

	byPhase := map[string]*dto.Metric{}
	for _, m := range f.GetMetric() {
		byPhase[labelValue(m, "phase")] = m
	}
	for _, e := range episodes {
		m := byPhase[e.phase.String()]
		if m == nil {
			t.Errorf("no series for phase %q", e.phase)
			continue
		}
		if got := m.GetGauge().GetValue(); got != e.want {
			t.Errorf("%s = %v, want %v", e.phase, got, e.want)
		}
	}

	firing := byPhase[leeway.PhaseFiring.String()]
	for key, want := range map[string]string{
		"subject_kind": "Deployment",
		"namespace":    "prod",
		"subject":      "web-1",
		"topology_key": string(zoneKey),
		"tier":         leeway.TierA.String(),
	} {
		if got := labelValue(firing, key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

// The §7.6 gauge counts subject-axes, not subjects: the whole point of the
// series is to be readable during an outage, when the per-subject series are
// exactly what nobody can page through.
func TestInstruments_TheTransientGaugeCountsAxesNotSubjects(t *testing.T) {
	suppressed := func(state leeway.TransientState) leeway.Suppression {
		return leeway.Suppression{State: state, Suppress: true, Reason: "because"}
	}
	opts := fullOptions()
	opts.Evaluations = func(yield evalObserver) {
		for i := range 7 {
			sub := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: fmt.Sprintf("web-%d", i)}
			ev := &Evaluation{Key: zoneKey, Suppression: suppressed(leeway.TransientDomainOutage)}
			switch i {
			case 6:
				// One subject on a different axis, and a different state.
				ev = &Evaluation{Key: regionKey, Suppression: suppressed(leeway.TransientWarmup)}
			case 5:
				// And one that is fine, which contributes no row at all.
				ev = &Evaluation{Key: zoneKey}
			}
			yield(sub, ev)
		}
	}

	f := newPromHarness(t, opts).family(t, "lookout_leeway_transient_subjects")
	if f == nil {
		t.Fatal("family absent")
	}
	got := map[string]float64{}
	for _, m := range f.GetMetric() {
		got[labelValue(m, "topology_key")+"/"+labelValue(m, "transient")] = m.GetGauge().GetValue()
	}
	want := map[string]float64{
		string(zoneKey) + "/domain-outage":    5,
		string(regionKey) + "/cluster-warmup": 1,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d series %v, want %d", len(got), got, len(want))
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s = %v, want %v", k, got[k], w)
		}
	}
}

func TestInstruments_AnUntroubledEstateHasNoTransientSeries(t *testing.T) {
	// Zero rows rather than a row reading zero, so that `transient_subjects`
	// present at all is the alert condition.
	opts := fullOptions()
	opts.Evaluations = func(yield evalObserver) {
		yield(subA, &Evaluation{Key: zoneKey})
	}
	if f := newPromHarness(t, opts).family(t, "lookout_leeway_transient_subjects"); f != nil {
		t.Errorf("exported %d series with nothing suppressed", len(f.GetMetric()))
	}
}

func TestAlertLevel_AQuietEpisodeReadsZero(t *testing.T) {
	// Not reachable through the observer — a subject in PhaseOK has no entry to
	// walk — but the encoding is a published contract, and "0 means nothing is
	// wrong" is the half of it a dashboard's `== 0` depends on.
	if got := alertLevel(leeway.PhaseOK); got != 0 {
		t.Errorf("alertLevel(ok) = %d, want 0", got)
	}
}

func TestInstruments_NoAlertObserverMeansNoAlertSeries(t *testing.T) {
	// The embedder that never wires the machine — and the shape of every other
	// optional observer in this file.
	opts := fullOptions()
	opts.Alerts = nil
	if f := newPromHarness(t, opts).family(t, "lookout_leeway_alert_state"); f != nil {
		t.Errorf("exported %d series with no observer", len(f.GetMetric()))
	}
}

// labelValue reads one label off a gathered metric.
func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func TestInstruments_ThePerDomainGateDecidesWhoGetsABreakdown(t *testing.T) {
	// §8.4: at §6.6's baseline row the per-domain breakdown is ~480k series
	// against ~3.5k for everything else, and under managed Prometheus that is
	// a bill rather than a failure. So it is exported for the subjects
	// somebody is about to go and look at, and withheld for the rest — and
	// the aggregates never move either way.
	for _, tc := range []struct {
		name string
		gate PerDomainGate
		want bool
	}{
		{"the shipped floor admits a drifting subject", PerDomainGate{MinDrift: DefaultPerDomainSeriesMinDrift}, true},
		{"a floor above it does not", PerDomainGate{MinDrift: 2}, false},
		{"All overrides the floor", PerDomainGate{All: true, MinDrift: 2}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := fullOptions()
			opts.PerDomain = tc.gate
			names := newPromHarness(t, opts).names(t)

			for _, m := range []string{"lookout_leeway_domain_objects", "lookout_leeway_domain_expected"} {
				if got := slices.Contains(names, m); got != tc.want {
					t.Errorf("%s exported = %v, want %v", m, got, tc.want)
				}
			}
			for _, m := range []string{"lookout_leeway_subjects_tracked", "lookout_leeway_drift"} {
				if !slices.Contains(names, m) {
					t.Errorf("%s went missing: the gate does not reach the aggregates", m)
				}
			}
		})
	}
}

func TestPerDomainGate_ASubjectWithNoScoreIsNeverAdmittedByTheFloor(t *testing.T) {
	// A zero floor admits every *scored* subject, and an unscored one has no
	// drift figure for a floor to compare against — admitting it on a zero
	// would put the whole estate's breakdown on the wire the moment somebody
	// set the floor to zero meaning "everything that is measured".
	if (PerDomainGate{}).Admits(0, false) {
		t.Error("a zero floor admitted an unscored subject")
	}
	if !(PerDomainGate{All: true}).Admits(0, false) {
		t.Error("All must admit an unscored subject: it is the every-subject switch")
	}
	if !(PerDomainGate{}).Admits(0, true) {
		t.Error("a zero floor must admit a scored subject that is not drifting")
	}
}

func TestInstruments_AGatedSubjectHasNoScoreSeries(t *testing.T) {
	// §7.5 gates a subject below MinReplicasForScoring, one with no eligible
	// domains, and one told to ignore the axis. None of them has a drift figure,
	// and the zero is not one: five series per axis asserting a perfectly
	// balanced workload for every single-replica Deployment is both the wrong
	// statement and, at §6.6's baseline, the bulk of the cardinality.
	opts := fullOptions()
	opts.Evaluations = func(yield evalObserver) {
		yield(subA, &Evaluation{
			Key:    zoneKey,
			Scores: leeway.Scores{Domains: []leeway.Domain{"us-central1-a"}, Gate: leeway.GateBelowMinReplicas},
		})
		// And a nil evaluation is a walker bug, not a scrape failure.
		yield(subA, nil)
	}
	names := newPromHarness(t, opts).names(t)

	for _, m := range []string{
		"lookout_leeway_drift",
		"lookout_leeway_observed_skew",
		"lookout_leeway_excess_skew",
		"lookout_leeway_max_domain_share",
		"lookout_leeway_relocation_distance",
		"lookout_leeway_domain_expected",
	} {
		if slices.Contains(names, m) {
			t.Errorf("%s was exported for a gated subject", m)
		}
	}
}

func TestInstruments_ThePerDomainBreakdownStopsAtTheShorterSlice(t *testing.T) {
	// Domains and Expected are built together and are the same length, so this
	// is a guard rather than a case. It is worth holding anyway: the loop reads
	// one slice by the other's index, and the failure mode of getting that
	// wrong is a panic inside a scrape callback, which takes down /metrics for
	// every other source in the process.
	opts := fullOptions()
	opts.Evaluations = func(yield evalObserver) {
		yield(subA, &Evaluation{
			Key: zoneKey,
			Scores: leeway.Scores{
				Domains:   []leeway.Domain{"us-central1-a", "us-central1-b"},
				Expected:  []int64{3},
				Evaluable: true,
			},
		})
	}
	h := newPromHarness(t, opts)

	f := h.family(t, "lookout_leeway_domain_expected")
	if f == nil {
		t.Fatal("family absent")
	}
	if len(f.GetMetric()) != 1 {
		t.Errorf("got %d series, want the one domain the expectation covers", len(f.GetMetric()))
	}
}

func TestInstruments_MissingCallbacksAreNotAPanic(t *testing.T) {
	// A source constructed without state wired in — which is what a partially
	// built process looks like — must scrape clean rather than crash the
	// handler serving /metrics for every other source.
	h := newPromHarness(t, metricsOptions{PerDomain: PerDomainGate{All: true}})
	if got := h.names(t); len(got) != 0 {
		t.Errorf("names = %q, want nothing observed", got)
	}
}

func TestInstruments_NilMeterUsesTheNoopProvider(t *testing.T) {
	in, err := newInstruments(metricsOptions{})
	if err != nil {
		t.Fatalf("newInstruments with a nil meter: %v", err)
	}
	// Recording against the no-op provider must be safe, since this is the
	// shape every unit test and every telemetry-less process runs in.
	in.recordEvent(context.Background(), resourceNode, time.Now())
	in.recordEvaluation(context.Background(), leeway.SubjectJob, time.Second)
	if err := in.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestInstruments_CloseIsIdempotentAndNilSafe(t *testing.T) {
	var nilIn *instruments
	if err := nilIn.Close(); err != nil {
		t.Errorf("Close on a nil *instruments: %v", err)
	}
	// Run's deferred Close can fire on a path where the source already closed.
	h := newPromHarness(t, metricsOptions{})
	if err := h.in.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := h.in.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestInstruments_NilRecordersAreSafe(t *testing.T) {
	var in *instruments
	in.recordEvent(context.Background(), resourcePod, time.Now())
	in.recordEvaluation(context.Background(), leeway.SubjectDeployment, time.Second)
	in.recordMismatch(context.Background(), leeway.SubjectDeployment)
}

// brokenMeter fails to declare one named instrument and otherwise behaves like
// the no-op meter. It exists because every declaration in newInstruments
// returns an error that no configuration can trigger, and an unexercised error
// path in startup code is where a nil-pointer panic hides.
type brokenMeter struct {
	metric.Meter
	fail string
}

var errDeclare = errors.New("declare failed")

func (m brokenMeter) Int64Gauge(name string, opts ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	if name == m.fail {
		return nil, errDeclare
	}
	return m.Meter.Int64Gauge(name, opts...)
}

func (m brokenMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if name == m.fail {
		return nil, errDeclare
	}
	return m.Meter.Float64Histogram(name, opts...)
}

func (m brokenMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if name == m.fail {
		return nil, errDeclare
	}
	return m.Meter.Int64Counter(name, opts...)
}

func (m brokenMeter) Int64ObservableGauge(name string, opts ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	if name == m.fail {
		return nil, errDeclare
	}
	return m.Meter.Int64ObservableGauge(name, opts...)
}

func (m brokenMeter) RegisterCallback(f metric.Callback, insts ...metric.Observable) (metric.Registration, error) {
	if m.fail == "callback" {
		return nil, errDeclare
	}
	return m.Meter.RegisterCallback(f, insts...)
}

func TestNewInstruments_ReportsDeclarationFailures(t *testing.T) {
	for _, fail := range []string{
		metricLastEvent,
		metricEvalDuration,
		metricCounterMismatch,
		metricSubjectsTracked,
		metricDomainReadyNodes,
		metricDomainObjects,
		"callback",
	} {
		t.Run(fail, func(t *testing.T) {
			meter := brokenMeter{Meter: noop.NewMeterProvider().Meter(MeterName), fail: fail}
			in, err := newInstruments(metricsOptions{Meter: meter})
			if err == nil {
				t.Fatalf("newInstruments succeeded with %s failing", fail)
			}
			if in != nil {
				t.Error("a failed newInstruments returned a non-nil result")
			}
			if !errors.Is(err, errDeclare) {
				t.Errorf("error %v does not wrap the cause", err)
			}
			if !strings.Contains(err.Error(), "topologydrift") {
				t.Errorf("error %q does not name the source", err)
			}
		})
	}
}

// TestSkewLabel: a required podAntiAffinity is a ceiling of one per domain and
// not a skew of one, so the label has to be able to say which bound it is
// reporting — and "none" rather than a number it invented when there is neither.
func TestSkewLabel(t *testing.T) {
	for name, tc := range map[string]struct {
		in   *leeway.Intent
		want string
	}{
		"max skew":       {&leeway.Intent{MaxSkew: ptr(int32(3))}, "3"},
		"max per domain": {&leeway.Intent{MaxPerDomain: ptr(int64(1))}, "max-per-domain=1"},
		"neither":        {&leeway.Intent{}, "none"},
		// An intent carrying both is a spread constraint on an axis that also
		// bounds a domain; the skew is the tighter statement and wins.
		"both": {&leeway.Intent{MaxSkew: ptr(int32(2)), MaxPerDomain: ptr(int64(1))}, "2"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := skewLabel(tc.in); got != tc.want {
				t.Errorf("skewLabel = %q, want %q", got, tc.want)
			}
		})
	}
}
