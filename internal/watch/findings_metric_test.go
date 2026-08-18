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

package watch

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// TestFindingsMetric_CountsDetectionNotDelivery (#288): every
// dispatcher terminal counts the same — one increment per distinct
// finding, keyed by kind and severity, regardless of where the
// routing stages then send it. That is the whole point of the
// collector: lookout_events_injected_total already measures delivery,
// and a warning batched into the watchboard or an info-class signal
// that is only stored is still something the sentinel found.
func TestFindingsMetric_CountsDetectionNotDelivery(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	ctx := context.Background()

	crash := crashLoopSignal()
	d.DispatchSignal(ctx, crash)             // critical → its own session
	d.DispatchSignal(ctx, crashLoopSignal()) // duplicate → suppressed
	d.DispatchSignal(ctx, warningSignal(1))  // warning → watchboard
	d.DispatchSignal(ctx, infoSignal())      // info → stored only

	for _, tc := range []struct {
		kind, severity string
		want           float64
		why            string
	}{
		{crash.Kind, string(crash.Severity), 1, "the injected critical, counted once — the duplicate is the same finding, not a second one"},
		{"objectstate.restart_burst", string(engine.SeverityWarning), 1, "a watchboard-batched warning is a finding, though it opens no session"},
		{"custom.heartbeat", string(engine.SeverityInfo), 1, "an info-class signal is a finding, though it is never injected anywhere"},
	} {
		got := testutil.ToFloat64(d.metrics.findings.WithLabelValues(tc.kind, tc.severity))
		if got != tc.want {
			t.Errorf("findings_total{kind=%q,severity=%q} = %v, want %v — %s", tc.kind, tc.severity, got, tc.want, tc.why)
		}
	}
}

// TestFindingsMetric_HasNoNamespaceLabel: the deliberate cardinality
// choice, pinned. kind is bounded by the declared ledger and severity
// is three values, but namespace is unbounded — a series per (kind,
// severity, namespace) on a large cluster is exactly how a findings
// metric turns into an outage of the monitoring stack it feeds.
func TestFindingsMetric_HasNoNamespaceLabel(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newMetricsFor(reg, "prod-us")
	m.findings.WithLabelValues("pod.crashloop", "critical").Inc()

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var labels []string
	for _, fam := range fams {
		if fam.GetName() != "lookout_findings_total" {
			continue
		}
		for _, l := range fam.GetMetric()[0].GetLabel() {
			labels = append(labels, l.GetName())
		}
	}
	if len(labels) == 0 {
		t.Fatal("lookout_findings_total not registered")
	}
	want := map[string]bool{"cluster": true, "kind": true, "severity": true}
	for _, l := range labels {
		if !want[l] {
			t.Errorf("unexpected label %q on lookout_findings_total (labels: %v)", l, labels)
		}
		delete(want, l)
	}
	for l := range want {
		t.Errorf("missing label %q on lookout_findings_total (labels: %v)", l, labels)
	}
}
