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

package autoscaling

// §13 conventions mirrored from the workload source's suite: the
// source is driven directly through its handlers (no informers) with
// a settable fake clock; the sweep is called by hand with the same
// clock. Run/informer plumbing is covered by the end-to-end path
// everywhere else.

import (
	"strings"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

var testStart = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

// newTestSource returns an armed source with a settable fake clock.
func newTestSource(t *testing.T, cfg Config) (*Source, *time.Time) {
	t.Helper()
	s := New(fake.NewSimpleClientset(), cfg)
	now := testStart
	clock := &now
	s.now = func() time.Time { return *clock }
	s.armed = true
	s.armedAt = testStart
	return s, clock
}

func int32Ptr(v int32) *int32 { return &v }

// hpa builds an HPA fixture targeting Deployment/web.
func hpa(uid, ns, name string, max, current, desired int32, conds ...autoscalingv2.HorizontalPodAutoscalerCondition) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Namespace: ns, Name: name},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: "web"},
			MaxReplicas:    max,
		},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{
			CurrentReplicas: current,
			DesiredReplicas: desired,
			Conditions:      conds,
		},
	}
}

// withCPU attaches a cpu utilization metric spec/status pair.
func withCPU(h *autoscalingv2.HorizontalPodAutoscaler, currentPct, targetPct int32) *autoscalingv2.HorizontalPodAutoscaler {
	h.Spec.Metrics = []autoscalingv2.MetricSpec{{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name:   corev1.ResourceCPU,
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: int32Ptr(targetPct)},
		},
	}}
	h.Status.CurrentMetrics = []autoscalingv2.MetricStatus{{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricStatus{
			Name:    corev1.ResourceCPU,
			Current: autoscalingv2.MetricValueStatus{AverageUtilization: int32Ptr(currentPct)},
		},
	}}
	return h
}

func cond(t autoscalingv2.HorizontalPodAutoscalerConditionType, status corev1.ConditionStatus, reason, message string, ltt time.Time) autoscalingv2.HorizontalPodAutoscalerCondition {
	return autoscalingv2.HorizontalPodAutoscalerCondition{
		Type: t, Status: status, Reason: reason, Message: message,
		LastTransitionTime: metav1.Time{Time: ltt},
	}
}

func scalingLimited(status corev1.ConditionStatus, reason string) autoscalingv2.HorizontalPodAutoscalerCondition {
	return cond(autoscalingv2.ScalingLimited, status, reason, "", testStart)
}

func scalingActive(status corev1.ConditionStatus, reason string, ltt time.Time) autoscalingv2.HorizontalPodAutoscalerCondition {
	return cond(autoscalingv2.ScalingActive, status, reason, "the HPA was unable to compute the replica count", ltt)
}

// pinnedHPA is the canonical pinned fixture: at max, capped by the
// controller's own verdict, cpu 87% vs target 60%.
func pinnedHPA(uid string) *autoscalingv2.HorizontalPodAutoscaler {
	return withCPU(hpa(uid, "prod", "web-hpa", 10, 10, 10,
		scalingLimited(corev1.ConditionTrue, reasonTooManyReplicas),
		scalingActive(corev1.ConditionTrue, "ValidMetricFound", testStart)), 87, 60)
}

// healthyHPA is the same HPA scaled back inside its range.
func healthyHPA(uid string) *autoscalingv2.HorizontalPodAutoscaler {
	return withCPU(hpa(uid, "prod", "web-hpa", 10, 6, 6,
		scalingLimited(corev1.ConditionFalse, "DesiredWithinRange"),
		scalingActive(corev1.ConditionTrue, "ValidMetricFound", testStart)), 45, 60)
}

// ---- hpa_pinned ----

func TestPinnedSustainedFiresOnceWithIdentity(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	s.onHPA(pinnedHPA("h1"))

	// Inside the window: silence.
	*clock = testStart.Add(9 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("fired inside PinnedSustain: %+v", sigs)
	}
	// Past PinnedSustain: exactly one warning with full identity.
	*clock = testStart.Add(12 * time.Minute)
	sigs := s.sweep(*clock)
	if len(sigs) != 1 {
		t.Fatalf("got %d signals, want 1: %+v", len(sigs), sigs)
	}
	sig := sigs[0]
	if sig.Kind != KindPinned || sig.Key.UID != "h1" || sig.Key.Reason != ReasonPinned {
		t.Errorf("signal identity = %s/%s/%s", sig.Kind, sig.Key.Reason, sig.Key.UID)
	}
	if sig.KindOfObject != hpaKind || sig.Namespace != "prod" || sig.Name != "web-hpa" {
		t.Errorf("object identity = %s %s/%s", sig.KindOfObject, sig.Namespace, sig.Name)
	}
	if sig.Severity != engine.SeverityWarning {
		t.Errorf("severity = %s, want warning", sig.Severity)
	}
	for _, want := range []string{"Deployment web", "maxReplicas=10", "cpu at 87% vs target 60%"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("message %q missing %q", sig.Message, want)
		}
	}
	if !sig.FirstSeen.Equal(testStart) {
		t.Errorf("FirstSeen = %v, want the episode start %v", sig.FirstSeen, testStart)
	}
	// Still pinned, still inside the warning band: no re-fire.
	*clock = testStart.Add(20 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Errorf("warning re-fired within the episode: %+v", sigs)
	}
}

func TestPinnedEscalatesToCriticalOnce(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	s.onHPA(pinnedHPA("h1"))
	*clock = testStart.Add(12 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 1 {
		t.Fatalf("setup: expected the warning")
	}

	*clock = testStart.Add(31 * time.Minute)
	sigs := s.sweep(*clock)
	if len(sigs) != 1 {
		t.Fatalf("escalation: got %d signals, want 1: %+v", len(sigs), sigs)
	}
	if sigs[0].Severity != engine.SeverityCritical {
		t.Errorf("escalation severity = %s, want critical", sigs[0].Severity)
	}
	if sigs[0].Key.UID != "h1" || sigs[0].Key.Reason != ReasonPinned {
		t.Errorf("escalation must keep the episode's dedup key, got %+v", sigs[0].Key)
	}
	// One escalation per episode.
	*clock = testStart.Add(2 * time.Hour)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Errorf("critical re-fired within the episode: %+v", sigs)
	}
}

func TestPinnedDropBelowMaxMidWindowResetsEpisode(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	s.onHPA(pinnedHPA("h1"))

	// Scales back in before the window elapses: no fire.
	*clock = testStart.Add(5 * time.Minute)
	s.onHPA(healthyHPA("h1"))
	*clock = testStart.Add(15 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("fired after the episode ended mid-window: %+v", sigs)
	}

	// A NEW episode starts its own window and fires again.
	repin := testStart.Add(20 * time.Minute)
	*clock = repin
	s.onHPA(pinnedHPA("h1"))
	*clock = repin.Add(9 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("new episode fired early (old window leaked): %+v", sigs)
	}
	*clock = repin.Add(11 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 1 {
		t.Fatalf("new episode: got %d signals, want 1", len(sigs))
	}
}

// TestAtMaxButNotLimitedNeverFires is the load-bearing false-positive
// pin: an HPA sitting at max with its metric satisfied — the
// minReplicas==maxReplicas sizing, or load that plateaued exactly at
// capacity — reports ScalingLimited != True/TooManyReplicas and must
// stay silent forever.
func TestAtMaxButNotLimitedNeverFires(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	h := withCPU(hpa("h1", "prod", "web-hpa", 4, 4, 4,
		scalingLimited(corev1.ConditionFalse, "DesiredWithinRange"),
		scalingActive(corev1.ConditionTrue, "ValidMetricFound", testStart)), 40, 60)
	s.onHPA(h)
	*clock = testStart.Add(2 * time.Hour)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("maxed-but-satisfied HPA fired: %+v", sigs)
	}

	// Same for min==max sizing where the controller reports the OTHER
	// ScalingLimited reason.
	h2 := withCPU(hpa("h2", "prod", "pinned-size", 3, 3, 3,
		scalingLimited(corev1.ConditionTrue, "TooFewReplicas"),
		scalingActive(corev1.ConditionTrue, "ValidMetricFound", testStart)), 10, 60)
	s.onHPA(h2)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("min==max sized HPA fired: %+v", sigs)
	}
}

// TestPinnedFallbackPredicate covers the secondary arm: no
// ScalingLimited condition at all, but desired>=max with the metric
// over target still counts as pinned.
func TestPinnedFallbackPredicate(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	h := withCPU(hpa("h1", "prod", "web-hpa", 10, 10, 10), 87, 60)
	s.onHPA(h)
	*clock = testStart.Add(11 * time.Minute)
	sigs := s.sweep(*clock)
	if len(sigs) != 1 {
		t.Fatalf("fallback predicate: got %d signals, want 1", len(sigs))
	}
	// And its inverse: metric NOT over target, no condition → no fire.
	s.onHPA(withCPU(hpa("h2", "prod", "other-hpa", 10, 10, 10), 40, 60))
	*clock = testStart.Add(2 * time.Hour)
	for _, sig := range s.sweep(*clock) {
		if sig.Key.UID == "h2" {
			t.Fatalf("fallback fired without the metric over target: %+v", sig)
		}
	}
}

// ---- hpa_metrics_dead ----

func deadHPA(uid string, ltt time.Time) *autoscalingv2.HorizontalPodAutoscaler {
	return hpa(uid, "prod", "web-hpa", 10, 4, 4,
		scalingActive(corev1.ConditionFalse, "FailedGetResourceMetric", ltt))
}

func TestMetricsDeadSustainedWarning(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	s.onHPA(deadHPA("h1", testStart))

	*clock = testStart.Add(14 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("fired inside MetricsDeadSustain: %+v", sigs)
	}
	*clock = testStart.Add(16 * time.Minute)
	sigs := s.sweep(*clock)
	if len(sigs) != 1 {
		t.Fatalf("got %d signals, want 1: %+v", len(sigs), sigs)
	}
	sig := sigs[0]
	if sig.Kind != KindMetricsDead || sig.Key.Reason != ReasonMetricsDead || sig.Key.UID != "h1" {
		t.Errorf("signal identity = %s/%s/%s", sig.Kind, sig.Key.Reason, sig.Key.UID)
	}
	if sig.Severity != engine.SeverityWarning {
		t.Errorf("severity = %s — metrics-dead is warning-only (leading indicator)", sig.Severity)
	}
	if !strings.Contains(sig.Message, "FailedGetResourceMetric") || !strings.Contains(sig.Message, "Deployment web") {
		t.Errorf("message = %q", sig.Message)
	}
	// Warning-only, once per episode: hours later, still nothing more.
	*clock = testStart.Add(3 * time.Hour)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Errorf("metrics-dead re-fired or escalated: %+v", sigs)
	}
}

// TestMetricsDeadCountsFromConditionTransition: a pipeline already
// dead when first observed (initial LIST) counts its window from the
// condition's lastTransitionTime, not from the observation.
func TestMetricsDeadCountsFromConditionTransition(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	// Dead since an hour before the sentinel saw it.
	s.onHPA(deadHPA("h1", testStart.Add(-time.Hour)))
	*clock = testStart.Add(time.Minute)
	sigs := s.sweep(*clock)
	if len(sigs) != 1 {
		t.Fatalf("pre-existing dead pipeline: got %d signals, want 1 at first post-arm sweep", len(sigs))
	}
	if !sigs[0].FirstSeen.Equal(testStart.Add(-time.Hour)) {
		t.Errorf("FirstSeen = %v, want the condition transition", sigs[0].FirstSeen)
	}
}

func TestScalingDisabledNeverFires(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	h := hpa("h1", "prod", "web-hpa", 10, 0, 0,
		scalingActive(corev1.ConditionFalse, reasonScalingDisabled, testStart.Add(-2*time.Hour)))
	s.onHPA(h)
	*clock = testStart.Add(2 * time.Hour)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("ScalingDisabled (replicas=0, deliberate) fired: %+v", sigs)
	}
}

func TestMetricsDeadRecoveryResetsAndRefires(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	s.onHPA(deadHPA("h1", testStart))
	*clock = testStart.Add(16 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 1 {
		t.Fatalf("setup: expected the first episode's warning")
	}

	// The pipeline comes back...
	*clock = testStart.Add(20 * time.Minute)
	s.onHPA(healthyHPA("h1"))
	*clock = testStart.Add(40 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 0 {
		t.Fatalf("fired after recovery: %+v", sigs)
	}

	// ...and dies again: a NEW episode with its own window and fire.
	redead := testStart.Add(time.Hour)
	*clock = redead
	s.onHPA(deadHPA("h1", redead))
	*clock = redead.Add(16 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 1 {
		t.Fatalf("new episode: got %d signals, want 1", len(sigs))
	}
}

// ---- arming ----

func TestPreArmSilence(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	s.armed = false
	s.onHPA(pinnedHPA("h1"))
	s.onHPA(deadHPA("h2", testStart.Add(-2*time.Hour)))
	*clock = testStart.Add(2 * time.Hour)
	if sigs := s.sweep(*clock); sigs != nil {
		t.Fatalf("un-armed sweep emitted: %+v", sigs)
	}
}

// ---- §7.4 clearance ----

func pinnedIncident(uid string) engine.Incident {
	return engine.Incident{Key: engine.EventKey{UID: uid, Reason: ReasonPinned}}
}

func deadIncident(uid string) engine.Incident {
	return engine.Incident{Key: engine.EventKey{UID: uid, Reason: ReasonMetricsDead}}
}

func TestPinnedClearance(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	s.onHPA(pinnedHPA("h1"))
	*clock = testStart.Add(12 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 1 {
		t.Fatalf("setup: expected the warning")
	}

	cl, ok := s.Clearance(pinnedIncident("h1"))
	if !ok || cl.Cleared {
		t.Fatalf("still pinned: clearance = %+v ok=%v, want held open", cl, ok)
	}

	// Scales back inside the range: cleared, stable since the OBSERVED
	// lift.
	*clock = testStart.Add(25 * time.Minute)
	liftObserved := *clock
	s.onHPA(healthyHPA("h1"))
	cl, ok = s.Clearance(pinnedIncident("h1"))
	if !ok || !cl.Cleared || cl.Resolution != engine.ResolutionRecovered {
		t.Fatalf("below max: clearance = %+v ok=%v", cl, ok)
	}
	if !cl.StableSince.Equal(liftObserved) {
		t.Errorf("StableSince = %v, want the lift OBSERVATION %v", cl.StableSince, liftObserved)
	}
}

func TestMetricsDeadClearance(t *testing.T) {
	s, clock := newTestSource(t, Config{})
	s.onHPA(deadHPA("h1", testStart))
	*clock = testStart.Add(16 * time.Minute)
	if sigs := s.sweep(*clock); len(sigs) != 1 {
		t.Fatalf("setup: expected the warning")
	}

	cl, ok := s.Clearance(deadIncident("h1"))
	if !ok || cl.Cleared {
		t.Fatalf("still dead: clearance = %+v ok=%v", cl, ok)
	}

	// ScalingActive=False with a NON-FailedGet reason ends the episode
	// but must NOT clear — the pipeline never proved itself alive.
	*clock = testStart.Add(20 * time.Minute)
	s.onHPA(hpa("h1", "prod", "web-hpa", 10, 0, 0,
		scalingActive(corev1.ConditionFalse, reasonScalingDisabled, *clock)))
	cl, ok = s.Clearance(deadIncident("h1"))
	if !ok || cl.Cleared {
		t.Fatalf("ScalingDisabled cleared a dead-pipeline incident: %+v ok=%v", cl, ok)
	}

	// ScalingActive back to True: cleared, stable since the observed
	// transition.
	*clock = testStart.Add(30 * time.Minute)
	aliveObserved := *clock
	s.onHPA(healthyHPA("h1"))
	cl, ok = s.Clearance(deadIncident("h1"))
	if !ok || !cl.Cleared || cl.Resolution != engine.ResolutionRecovered {
		t.Fatalf("pipeline alive: clearance = %+v ok=%v", cl, ok)
	}
	if !cl.StableSince.Equal(aliveObserved) {
		t.Errorf("StableSince = %v, want the alive OBSERVATION %v", cl.StableSince, aliveObserved)
	}
}

func TestClearanceObjectDeleted(t *testing.T) {
	s, _ := newTestSource(t, Config{})
	s.onHPA(pinnedHPA("h1"))
	s.onHPADelete(pinnedHPA("h1"))
	for _, inc := range []engine.Incident{pinnedIncident("h1"), deadIncident("h1")} {
		cl, ok := s.Clearance(inc)
		if !ok || !cl.Cleared || cl.Resolution != engine.ResolutionObjectDeleted {
			t.Fatalf("deleted HPA: clearance(%s) = %+v ok=%v", inc.Key.Reason, cl, ok)
		}
	}
}

func TestClearanceForeignReason(t *testing.T) {
	s, _ := newTestSource(t, Config{})
	if _, ok := s.Clearance(engine.Incident{Key: engine.EventKey{UID: "x", Reason: "rollout_stall"}}); ok {
		t.Error("claimed a foreign reason")
	}
}

func TestClearanceUnarmed(t *testing.T) {
	s, _ := newTestSource(t, Config{})
	s.armed = false
	if _, ok := s.Clearance(pinnedIncident("h1")); ok {
		t.Error("judged against an unsynced mirror")
	}
	if _, ok := s.Clearance(deadIncident("h1")); ok {
		t.Error("judged against an unsynced mirror")
	}
}

// ---- contract pins ----

func TestSourceContract(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), Config{})
	if s.Name() != "autoscaling" {
		t.Errorf("Name() = %q, want autoscaling (§7.2 table)", s.Name())
	}
	if s.Scope() != sources.ScopeCluster {
		t.Errorf("Scope() = %v, want cluster", s.Scope())
	}
	var _ sources.Source = s
	var _ sources.AccessDeclarer = s
	var _ engine.ClearanceObserver = s
}

// TestRequiredAccess pins the §11 declaration. Matches the
// autoscaling-group rule in deploy/12-clusterrole-watcher.yaml.
func TestRequiredAccess(t *testing.T) {
	t.Parallel()
	s := New(fake.NewSimpleClientset(), Config{})
	want := map[string]bool{
		"autoscaling/horizontalpodautoscalers/list":  true,
		"autoscaling/horizontalpodautoscalers/watch": true,
	}
	for _, r := range s.RequiredAccess() {
		key := r.Group + "/" + r.Resource + "/" + r.Verb
		if !want[key] {
			t.Errorf("unexpected requirement %s", key)
		}
		delete(want, key)
	}
	for key := range want {
		t.Errorf("missing requirement %s", key)
	}
}

// TestKindsAreFrozenStrings pins the §7.3 kind names — playbooks and
// fleet consumers match on these exact strings.
func TestKindsAreFrozenStrings(t *testing.T) {
	t.Parallel()
	frozen := map[string]string{
		KindPinned:      "autoscaling.hpa_pinned",
		KindMetricsDead: "autoscaling.hpa_metrics_dead",
	}
	for got, want := range frozen {
		if got != want {
			t.Errorf("kind %q, want %q (frozen)", got, want)
		}
	}
	// The dedup reasons are the kind suffixes and map to themselves.
	for _, reason := range []string{ReasonPinned, ReasonMetricsDead} {
		if engine.CanonicalReason(reason) != reason {
			t.Errorf("reason %q must map to itself under CanonicalReason", reason)
		}
	}
}

func TestConfigNormalize(t *testing.T) {
	t.Parallel()
	d := Config{}.normalize()
	if d != DefaultConfig() {
		t.Errorf("zero Config normalized to %+v, want the defaults %+v", d, DefaultConfig())
	}
	// A PinnedSustain raised past PinnedCritSustain never escalates
	// before it warns.
	c := Config{PinnedSustain: time.Hour, PinnedCritSustain: 30 * time.Minute}.normalize()
	if c.pinnedCritSustain() != time.Hour {
		t.Errorf("effective crit sustain = %v, want clamped to PinnedSustain", c.pinnedCritSustain())
	}
}
