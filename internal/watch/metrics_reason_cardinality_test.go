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

// Issue #109 — /metrics label cardinality unbounded from free-form
// Event.reason.
//
// dispatch.go stamps sig.Key.Reason straight onto the "reason" label of
// eventsSeen/eventsDedupSuppress/eventsInjected/injectErrors. That value
// is free-form (pkg/sources/k8sevents passes raw ev.Reason through; some
// capacity/quota sources echo scheduler-predicate text), so the "reason"
// label is an unbounded Prometheus dimension — one series per distinct
// reason string, growing without limit and blowing up scrape/memory.
//
// FIX (runtime distinct-value cap, not a static allowlist): keep the
// reason label but bound its cardinality — the first reasonLabelCap
// distinct reasons keep their real value; any further NEW reason
// collapses to reasonOther. Cardinality is then <= reasonLabelCap+1.
//
// ---------------------------------------------------------------------
// Seam under test (the coder adds these):
//
//	const reasonLabelCap = 100
//	const reasonOther    = "other"
//
//	// on *metrics:
//	reasonMu   sync.Mutex
//	reasonSeen map[string]struct{}   // make()d in newMetrics
//
//	// boundReason caps the cardinality of the free-form reason label
//	// (#109): the first reasonLabelCap distinct reasons keep their
//	// value; any further NEW reason collapses to reasonOther.
//	func (m *metrics) boundReason(reason string) string
//
// Semantics: already-seen reason -> return it unchanged (does NOT
// consume a new slot); else if len(reasonSeen) >= reasonLabelCap ->
// return reasonOther; else record it and return it. Thread-safe
// (dispatch runs from source callbacks). Every free-form reason label
// site in dispatch.go/storm.go is then wrapped as
// m.boundReason(sig.Key.Reason).
//
// Fails to COMPILE until boundReason/reasonLabelCap/reasonOther exist;
// then a naive stub returning reason unconditionally fails the
// "returns other after the cap" assertions; passes only with the cap.

import (
	"fmt"
	"testing"
)

func TestBoundReason_CapsLabelCardinality(t *testing.T) {
	t.Parallel()

	m := newMetrics()

	// The first reasonLabelCap distinct reasons keep their real value.
	for i := 0; i < reasonLabelCap; i++ {
		r := fmt.Sprintf("reason-%d", i)
		if got := m.boundReason(r); got != r {
			t.Fatalf("boundReason(%q) = %q, want %q (first %d distinct reasons keep their value)",
				r, got, r, reasonLabelCap)
		}
	}

	// A previously-seen reason is stable and must NOT consume a slot.
	if got := m.boundReason("reason-0"); got != "reason-0" {
		t.Fatalf("boundReason(%q) on a seen reason = %q, want %q (seen reasons stay verbatim)",
			"reason-0", got, "reason-0")
	}

	// A NEW reason once the cap is reached collapses to reasonOther.
	if got := m.boundReason("reason-overflow"); got != reasonOther {
		t.Fatalf("boundReason(%q) after cap = %q, want %q (new reasons past the cap collapse)",
			"reason-overflow", got, reasonOther)
	}

	// EVERY further distinct new reason collapses too — cardinality of
	// the source values is bounded, not just clamped for one string.
	for i := 0; i < 50; i++ {
		r := fmt.Sprintf("late-%d", i)
		if got := m.boundReason(r); got != reasonOther {
			t.Fatalf("boundReason(%q) past the cap = %q, want %q (all overflow reasons collapse)",
				r, got, reasonOther)
		}
	}

	// End-to-end: driving many distinct reasons through the wrapped
	// label site must yield <= reasonLabelCap+1 eventsSeen series, i.e.
	// the /metrics dimension is actually bounded.
	for i := 0; i < reasonLabelCap*5; i++ {
		m.eventsSeen.WithLabelValues(m.boundReason(fmt.Sprintf("wire-%d", i)), "ns").Inc()
	}
	if series := countSeries(t, m, "k8s_event_watcher_events_seen_total"); series > reasonLabelCap+1 {
		t.Fatalf("eventsSeen series = %d, want <= %d (reasonLabelCap+1) — reason label is unbounded",
			series, reasonLabelCap+1)
	}
}

// countSeries gathers the metrics registry and counts the label-set
// series for the named metric family.
func countSeries(t *testing.T, m *metrics, name string) int {
	t.Helper()
	fams, err := m.registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() == name {
			return len(f.GetMetric())
		}
	}
	return 0
}
