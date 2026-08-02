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

package capacity

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

var ft0 = time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)

// forecastCfg mirrors saturation's shipped geometry scaled onto this
// sub-source's knobs, so the exact-ETA walk below can reuse the known
// numbers: window 90m (minSpan 45m), warn 60m, crit 15m, 8 samples.
func forecastCfg() Config {
	return Config{
		ForecastWindow:     90 * time.Minute,
		ForecastMinSamples: 8,
		ForecastWarnETA:    60 * time.Minute,
		ForecastCritETA:    15 * time.Minute,
	}
}

// smallForecastCfg keeps hysteresis simulations short: window 10m
// (minSpan/reobserve 5m), warn 10m (clear 20m), crit 2m.
func smallForecastCfg() Config {
	return Config{
		ForecastWindow:     10 * time.Minute,
		ForecastMinSamples: 5,
		ForecastWarnETA:    10 * time.Minute,
		ForecastCritETA:    2 * time.Minute,
	}
}

func newForecastSource(cfg Config) *Source {
	s := New(fake.NewSimpleClientset(), cloud.NoProvider, cfg)
	s.logf = func(string, ...any) {}
	return s
}

// forecastNode builds a Ready, schedulable node with the given
// allocatable (CPU in millicores, memory in bytes; 0 omits the
// dimension entirely).
func forecastNode(name string, lbls map[string]string, cpuMilli, memBytes int64) *corev1.Node {
	alloc := corev1.ResourceList{}
	if cpuMilli > 0 {
		alloc[corev1.ResourceCPU] = *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI)
	}
	if memBytes > 0 {
		alloc[corev1.ResourceMemory] = *resource.NewQuantity(memBytes, resource.BinarySI)
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: lbls},
		Status: corev1.NodeStatus{
			Allocatable: alloc,
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

// forecastPod builds a Running pod bound to node with one container
// requesting the given CPU/memory.
func forecastPod(name, node string, cpuMilli, memBytes int64) *corev1.Pod {
	req := corev1.ResourceList{}
	if cpuMilli > 0 {
		req[corev1.ResourceCPU] = *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI)
	}
	if memBytes > 0 {
		req[corev1.ResourceMemory] = *resource.NewQuantity(memBytes, resource.BinarySI)
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: name},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{{
				Name:      "app",
				Resources: corev1.ResourceRequirements{Requests: req},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// TestClusterForecast_ExactETA_CrossingsAndHysteresis walks a pure
// linear fill (saturation's TestForecast_ExactETA geometry mapped
// onto the ratio): one unlabeled node (domain "cluster") with 400
// allocatable memory units, requests growing 2/min from 200. The
// warning must fire the moment the span reaches window/2 (ETA then
// exactly 55m), never re-fire at the same severity, and escalate to
// critical when the ETA reaches 15m.
func TestClusterForecast_ExactETA_CrossingsAndHysteresis(t *testing.T) {
	t.Parallel()
	s := newForecastSource(forecastCfg())
	nodes := []*corev1.Node{forecastNode("n1", nil, 0, 400)}

	var fired []engine.Signal
	step := 30 * time.Second
	for i := 0; i <= 172; i++ {
		now := ft0.Add(time.Duration(i) * step)
		pods := []*corev1.Pod{forecastPod("web", "n1", 0, 200+int64(i))}
		fired = append(fired, s.sampleCluster(pods, nodes, now)...)

		switch {
		case i < 90 && len(fired) != 0:
			t.Fatalf("i=%d (span %.1fm): fired %d signals before window/2 span — the insufficient-window gate leaked", i, float64(i)*0.5, len(fired))
		case i == 90:
			// span = 45m = window/2; ratio = 290/400; ETA = 55m.
			if len(fired) != 1 {
				t.Fatalf("i=90: signals = %d, want the warning to fire exactly at the span gate", len(fired))
			}
			sig := fired[0]
			if sig.Kind != KindClusterForecast || sig.Severity != engine.SeverityWarning {
				t.Fatalf("kind=%q severity=%q, want %s/warning", sig.Kind, sig.Severity, KindClusterForecast)
			}
			if sig.Forecast == nil {
				t.Fatal("forecast attachment missing (§8)")
			}
			wantETA := now.Add(55 * time.Minute)
			if d := sig.Forecast.ETA.Sub(wantETA); d < -2*time.Second || d > 2*time.Second {
				t.Errorf("ETA = %v, want %v ±2s (known slope 2 units/min, headroom 110 units)", sig.Forecast.ETA, wantETA)
			}
			if sig.Forecast.ConfidenceBasis != "linear-90m-window" {
				t.Errorf("ConfidenceBasis = %q, want linear-90m-window (§8)", sig.Forecast.ConfidenceBasis)
			}
			if sig.Key.UID != "nodegroup:cluster" || sig.Key.Reason != "cluster_forecast" {
				t.Errorf("dedup key = %+v, want uid=nodegroup:cluster reason=cluster_forecast", sig.Key)
			}
			if sig.KindOfObject != "NodeGroup" || sig.Name != "cluster" || sig.Namespace != "" {
				t.Errorf("object identity wrong: %+v", sig.TriageEvent)
			}
			for _, want := range []string{"scheduling domain cluster", "memory requests at 72.5%", "(linear-90m window)"} {
				if !strings.Contains(sig.Message, want) {
					t.Errorf("message %q missing evidence %q", sig.Message, want)
				}
			}
		case i > 90 && i < 170 && len(fired) != 1:
			// ETA stays inside (15m, 60m]: warning is latched,
			// nothing re-fires.
			t.Fatalf("i=%d: signals = %d, want the warning latched (no same-severity re-fire)", i, len(fired))
		case i == 172:
			// ETA ≤ 15m since ~i=170: escalation must have fired.
			if len(fired) != 2 {
				t.Fatalf("i=172: signals = %d, want the critical escalation to fire", len(fired))
			}
			if fired[1].Severity != engine.SeverityCritical {
				t.Errorf("escalation severity = %q, want critical", fired[1].Severity)
			}
		}
	}
}

func TestClusterForecast_NoFire_InsufficientSamples(t *testing.T) {
	t.Parallel()
	s := newForecastSource(smallForecastCfg()) // MinSamples 5
	nodes := []*corev1.Node{forecastNode("n1", nil, 0, 1000)}
	// 4 samples spanning 6m (span gate passes; count gate must not).
	for i := 0; i < 4; i++ {
		pods := []*corev1.Pod{forecastPod("web", "n1", 0, 500+int64(i)*100)}
		if got := s.sampleCluster(pods, nodes, ft0.Add(time.Duration(i)*2*time.Minute)); len(got) != 0 {
			t.Fatalf("emitted %d signal(s) from %d samples, want none below ForecastMinSamples", len(got), i+1)
		}
	}
}

func TestClusterForecast_NoFire_InsufficientSpan(t *testing.T) {
	t.Parallel()
	s := newForecastSource(smallForecastCfg()) // minSpan 5m
	nodes := []*corev1.Node{forecastNode("n1", nil, 0, 1000)}
	// 12 rapid samples spanning 110s with a dire slope: count gate
	// passes, span gate must not.
	for i := 0; i < 12; i++ {
		pods := []*corev1.Pod{forecastPod("web", "n1", 0, 400+int64(i)*50)}
		if got := s.sampleCluster(pods, nodes, ft0.Add(time.Duration(i)*10*time.Second)); len(got) != 0 {
			t.Fatalf("emitted %d signal(s) over a %ds span, want none below window/2", len(got), i*10)
		}
	}
}

func TestClusterForecast_NoFire_NonPositiveSlope(t *testing.T) {
	t.Parallel()
	s := newForecastSource(smallForecastCfg())
	nodes := []*corev1.Node{forecastNode("n1", nil, 0, 1000)}
	// Draining requests: high ratio, falling — a ratio near full
	// without a positive slope is not a forecast.
	for i := 0; i < 20; i++ {
		pods := []*corev1.Pod{forecastPod("web", "n1", 0, 900-int64(i)*10)}
		if got := s.sampleCluster(pods, nodes, ft0.Add(time.Duration(i)*30*time.Second)); len(got) != 0 {
			t.Fatalf("emitted %d signal(s) on a falling series, want none (non-positive slope)", len(got))
		}
	}
}

// TestClusterForecast_RecedeReleasesLatch_ThenRefires drives the full
// hysteresis loop: climb to a warning, flatten — as the steep samples
// prune out, the fitted slope shrinks so the ETA recedes past
// 2×WarnETA (recededSince set, latch released) and then turns
// non-positive — and finally climb again: the fresh approach must
// fire a second time, which only a released latch allows.
func TestClusterForecast_RecedeReleasesLatch_ThenRefires(t *testing.T) {
	t.Parallel()
	s := newForecastSource(smallForecastCfg()) // warn 10m, clear 20m, reobserve 5m
	nodes := []*corev1.Node{forecastNode("n1", nil, 0, 1000)}
	key := domainKey{domain: clusterDomain, resource: forecastResourceMemory}
	step := 30 * time.Second
	var fired []engine.Signal
	tick := func(i int, memReq int64) {
		pods := []*corev1.Pod{forecastPod("web", "n1", 0, memReq)}
		fired = append(fired, s.sampleCluster(pods, nodes, ft0.Add(time.Duration(i)*step))...)
	}

	// Climb: 503 + 10/tick (20 units/min). ETA drops below warn (10m)
	// strictly between ticks 29 and 30 (9.85m at i=30 — off the
	// boundary on purpose).
	for i := 0; i <= 30; i++ {
		tick(i, 503+int64(i)*10)
	}
	if len(fired) != 1 || fired[0].Severity != engine.SeverityWarning {
		t.Fatalf("climb fired %d signal(s) (last %+v), want exactly one warning", len(fired), fired)
	}

	// Flatten at 803 for 50 ticks (25m — well past the 10m window):
	// the slope decays through small-positive (ETA recedes past
	// clear) to zero. No signal may fire; the latch must release,
	// and the recede path must be the one observed doing it.
	sawRecede := false
	for i := 31; i <= 80; i++ {
		tick(i, 803)
		if ser := s.domains[key]; ser != nil && !ser.recededSince.IsZero() && ser.firedSeverity == "" {
			sawRecede = true
		}
	}
	if len(fired) != 1 {
		t.Fatalf("flat phase fired %d extra signal(s), want none", len(fired)-1)
	}
	if !sawRecede {
		t.Error("latch never released via the recede path (ETA past 2×WarnETA) during the flat phase")
	}
	if ser := s.domains[key]; ser.firedSeverity != "" {
		t.Fatalf("latch still %q after 25m flat, want released", ser.firedSeverity)
	}

	// Fresh approach: steep re-climb (40 units/min). A released latch
	// must fire again.
	for i := 81; i <= 89; i++ {
		tick(i, 803+int64(i-80)*20)
	}
	if len(fired) < 2 {
		t.Fatal("re-climb after latch release never re-fired — release must re-arm the forecast")
	}
	if sev := fired[len(fired)-1].Severity; sev != engine.SeverityWarning && sev != engine.SeverityCritical {
		t.Errorf("re-fire severity = %q", sev)
	}
}

// TestClusterForecast_PerDomainIndependence: two nodepools, one
// filling, one idle — only the filling domain fires, keyed and named
// by its own nodepool.
func TestClusterForecast_PerDomainIndependence(t *testing.T) {
	t.Parallel()
	s := newForecastSource(forecastCfg())
	nodes := []*corev1.Node{
		forecastNode("a1", map[string]string{nodepoolLabel: "pool-a"}, 0, 400),
		forecastNode("b1", map[string]string{nodepoolLabel: "pool-b"}, 0, 400),
	}
	var fired []engine.Signal
	for i := 0; i <= 100; i++ {
		pods := []*corev1.Pod{
			forecastPod("hot", "a1", 0, 200+int64(i)),
			forecastPod("cold", "b1", 0, 100),
		}
		fired = append(fired, s.sampleCluster(pods, nodes, ft0.Add(time.Duration(i)*30*time.Second))...)
	}
	if len(fired) == 0 {
		t.Fatal("the filling domain never fired")
	}
	for _, sig := range fired {
		if sig.Name != "pool-a" || sig.Key.UID != "nodegroup:pool-a" {
			t.Errorf("signal for %s/%s, want every fire on pool-a only", sig.Name, sig.Key.UID)
		}
	}
	bKey := domainKey{domain: "pool-b", resource: forecastResourceMemory}
	if ser := s.domains[bKey]; ser == nil {
		t.Error("idle domain's series must still be tracked")
	} else if ser.firedSeverity != "" {
		t.Errorf("idle domain latched %q", ser.firedSeverity)
	}
}

// TestClusterForecast_DomainDerivation pins the label precedence:
// GKE nodepool > stable zone > legacy-beta zone > "cluster"; cordoned
// and NotReady nodes contribute no domain at all.
func TestClusterForecast_DomainDerivation(t *testing.T) {
	t.Parallel()
	s := newForecastSource(forecastCfg())
	cordoned := forecastNode("n5", map[string]string{nodepoolLabel: "pool-cordoned"}, 0, 400)
	cordoned.Spec.Unschedulable = true
	notReady := forecastNode("n6", map[string]string{nodepoolLabel: "pool-notready"}, 0, 400)
	notReady.Status.Conditions[0].Status = corev1.ConditionFalse
	nodes := []*corev1.Node{
		forecastNode("n1", map[string]string{nodepoolLabel: "pool-x", zoneLabel: "us-east1-b"}, 0, 400),
		forecastNode("n2", map[string]string{zoneLabel: "us-east1-c"}, 0, 400),
		forecastNode("n3", map[string]string{zoneLabelBeta: "us-east1-d"}, 0, 400),
		forecastNode("n4", nil, 0, 400),
		cordoned,
		notReady,
	}
	s.sampleCluster(nil, nodes, ft0)
	for _, want := range []string{"pool-x", "us-east1-c", "us-east1-d", "cluster"} {
		if s.domains[domainKey{domain: want, resource: forecastResourceMemory}] == nil {
			t.Errorf("domain %q missing from the sample", want)
		}
	}
	for _, absent := range []string{"pool-cordoned", "pool-notready"} {
		if s.domains[domainKey{domain: absent, resource: forecastResourceMemory}] != nil {
			t.Errorf("excluded node's domain %q was sampled (cordoned/NotReady nodes are not packable capacity)", absent)
		}
	}
}

// TestClusterForecast_PodFiltering: only bound, non-terminal pods on
// known schedulable nodes count toward the requests sum.
func TestClusterForecast_PodFiltering(t *testing.T) {
	t.Parallel()
	s := newForecastSource(forecastCfg())
	nodes := []*corev1.Node{forecastNode("n1", nil, 0, 100)}

	unbound := forecastPod("unbound", "", 0, 40)
	succeeded := forecastPod("done", "n1", 0, 30)
	succeeded.Status.Phase = corev1.PodSucceeded
	failed := forecastPod("dead", "n1", 0, 20)
	failed.Status.Phase = corev1.PodFailed
	ghost := forecastPod("ghost", "gone-node", 0, 10)

	pods := []*corev1.Pod{forecastPod("web", "n1", 0, 50), unbound, succeeded, failed, ghost}
	s.sampleCluster(pods, nodes, ft0)
	ser := s.domains[domainKey{domain: clusterDomain, resource: forecastResourceMemory}]
	if ser == nil || len(ser.samples) != 1 {
		t.Fatalf("series = %+v, want one sample", ser)
	}
	if ser.samples[0].v != 0.5 {
		t.Errorf("ratio = %v, want 0.5 — only the bound Running pod's 50/100 counts", ser.samples[0].v)
	}
}

// TestClusterForecast_DimensionsIndependent: CPU fills while memory
// stays flat — only the CPU dimension fires, and the message names
// it. Both dimensions ride the domain's one dedup key.
func TestClusterForecast_DimensionsIndependent(t *testing.T) {
	t.Parallel()
	s := newForecastSource(forecastCfg())
	nodes := []*corev1.Node{forecastNode("n1", nil, 4000, 400)}
	var fired []engine.Signal
	for i := 0; i <= 100; i++ {
		pods := []*corev1.Pod{forecastPod("web", "n1", 2000+int64(i)*10, 100)}
		fired = append(fired, s.sampleCluster(pods, nodes, ft0.Add(time.Duration(i)*30*time.Second))...)
	}
	if len(fired) == 0 {
		t.Fatal("the filling CPU dimension never fired")
	}
	for _, sig := range fired {
		if !strings.Contains(sig.Message, "cpu requests") {
			t.Errorf("message %q must name the cpu dimension", sig.Message)
		}
		if sig.Key.UID != "nodegroup:cluster" || sig.Key.Reason != "cluster_forecast" {
			t.Errorf("dedup key = %+v, want the domain key shared by both dimensions", sig.Key)
		}
	}
	memKey := domainKey{domain: clusterDomain, resource: forecastResourceMemory}
	if ser := s.domains[memKey]; ser == nil {
		t.Error("memory series must still be tracked")
	} else if ser.firedSeverity != "" {
		t.Errorf("flat memory dimension latched %q", ser.firedSeverity)
	}
}

// TestClusterForecast_Clearance walks the §7.4 predicate: unclaimed
// reasons and the pre-first-sample state decline; a latched domain
// reads symptomatic; a flattened one clears as recovered; a vanished
// domain clears as object_deleted.
func TestClusterForecast_Clearance(t *testing.T) {
	t.Parallel()
	s := newForecastSource(smallForecastCfg())
	inc := engine.Incident{Key: engine.EventKey{UID: "nodegroup:cluster", Reason: "cluster_forecast"}}

	if _, ok := s.Clearance(engine.Incident{Key: engine.EventKey{UID: "nodegroup:cluster", Reason: "stockout"}}); ok {
		t.Error("claimed a non-forecast capacity incident")
	}
	if _, ok := s.Clearance(inc); ok {
		t.Error("judged before the first sample cycle — an empty map at startup must not read as deleted")
	}

	// Drive a warning latch (the recede test's climb geometry).
	nodes := []*corev1.Node{forecastNode("n1", nil, 0, 1000)}
	step := 30 * time.Second
	cur := ft0
	s.now = func() time.Time { return cur }
	for i := 0; i <= 30; i++ {
		cur = ft0.Add(time.Duration(i) * step)
		s.sampleCluster([]*corev1.Pod{forecastPod("web", "n1", 0, 503+int64(i)*10)}, nodes, cur)
	}
	verdict, ok := s.Clearance(inc)
	if !ok || verdict.Cleared {
		t.Fatalf("latched domain: (cleared=%v, ok=%v), want claimed and symptomatic", verdict.Cleared, ok)
	}

	// Flatten well past the recede + re-observation thresholds.
	for i := 31; i <= 80; i++ {
		cur = ft0.Add(time.Duration(i) * step)
		s.sampleCluster([]*corev1.Pod{forecastPod("web", "n1", 0, 803)}, nodes, cur)
	}
	verdict, ok = s.Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("flattened domain: %+v (ok=%v), want cleared/recovered", verdict, ok)
	}
	if verdict.StableSince.IsZero() {
		t.Error("recovered clearance must carry StableSince (the recede/turn instant)")
	}

	// The domain disappears (nodepool scaled away): object_deleted.
	cur = cur.Add(step)
	s.sampleCluster(nil, nil, cur)
	verdict, ok = s.Clearance(inc)
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Fatalf("vanished domain: %+v (ok=%v), want cleared/object_deleted", verdict, ok)
	}
}
