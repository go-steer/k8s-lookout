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

package saturation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

const mib = float64(1 << 20)

// scriptedPods is a PodUsageFetcher whose next return is set by the
// test between sampleOnce calls (§13: synthetic series, known slopes).
type scriptedPods struct {
	samples []ContainerSample
	err     error
}

func (f *scriptedPods) FetchPodUsage(context.Context) ([]ContainerSample, error) {
	return f.samples, f.err
}

type scriptedVolumes struct {
	samples []VolumeSample
	err     error
	calls   int
}

func (f *scriptedVolumes) FetchVolumeUsage(context.Context) ([]VolumeSample, error) {
	f.calls++
	return f.samples, f.err
}

// memSample builds the standard one-container fixture.
func memSample(used, limit float64) ContainerSample {
	return ContainerSample{
		Namespace: "prod", Pod: "web-1", PodUID: "u1", Container: "app", Node: "n1",
		Resource: ResourceMemory, Used: used, Limit: limit,
	}
}

func testSource(cfg Config, pods PodUsageFetcher, vols VolumeUsageFetcher) (*Source, *[]string) {
	s := New(cfg, pods, vols)
	var logs []string
	s.logf = func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }
	return s, &logs
}

// smallCfg keeps clearance/hysteresis simulations short: window 10m
// (minSpan/reobserve 5m), warn 10m (clear 20m), crit 2m.
func smallCfg() Config {
	return Config{Interval: 30 * time.Second, Window: 10 * time.Minute, WarnETA: 10 * time.Minute,
		CritETA: 2 * time.Minute, MinSamples: 5, StaleAfter: time.Hour}
}

var t0 = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// TestForecast_ExactETA_CrossingsAndHysteresis walks the shipped
// defaults through a pure linear series (slope exactly 1MiB/min,
// limit 200MiB from 100MiB): the warning must fire the moment the
// span reaches window/2 (ETA then exactly 55m), never re-fire at the
// same severity, and escalate to critical exactly when the ETA drops
// below 15m.
func TestForecast_ExactETA_CrossingsAndHysteresis(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(DefaultConfig(), pods, nil)
	ctx := context.Background()

	var fired []engine.Signal
	step := 30 * time.Second
	slopePerMin := 1 * mib
	for i := 0; i <= 172; i++ {
		now := t0.Add(time.Duration(i) * step)
		minutes := float64(i) / 2
		pods.samples = []ContainerSample{memSample(100*mib+minutes*slopePerMin, 200*mib)}
		got := s.sampleOnce(ctx, now)
		fired = append(fired, got...)

		switch {
		case i < 90 && len(fired) != 0:
			t.Fatalf("i=%d (span %.1fm): fired %d signals before window/2 span — the insufficient-window gate leaked", i, float64(i)*0.5, len(fired))
		case i == 90:
			// span = 45m = window/2; current = 145MiB; ETA = 55m.
			if len(fired) != 1 {
				t.Fatalf("i=90: signals = %d, want the warning to fire exactly at the span gate", len(fired))
			}
			sig := fired[0]
			if sig.Kind != KindForecast || sig.Severity != engine.SeverityWarning {
				t.Fatalf("kind=%q severity=%q, want %s/warning", sig.Kind, sig.Severity, KindForecast)
			}
			if sig.Forecast == nil {
				t.Fatal("forecast attachment missing (§8)")
			}
			wantETA := now.Add(55 * time.Minute)
			if d := sig.Forecast.ETA.Sub(wantETA); d < -2*time.Second || d > 2*time.Second {
				t.Errorf("ETA = %v, want %v ±2s (known slope 1MiB/min, headroom 55MiB)", sig.Forecast.ETA, wantETA)
			}
			if sig.Forecast.ConfidenceBasis != "linear-90m-window" {
				t.Errorf("ConfidenceBasis = %q, want linear-90m-window (§8)", sig.Forecast.ConfidenceBasis)
			}
			if sig.Key.UID != "u1/app" || sig.Key.Reason != "forecast_memory" {
				t.Errorf("dedup key = %+v, want uid=u1/app reason=forecast_memory", sig.Key)
			}
			if sig.KindOfObject != "Pod" || sig.Container != "app" || sig.Namespace != "prod" || sig.Name != "web-1" {
				t.Errorf("object identity wrong: %+v", sig.TriageEvent)
			}
			for _, want := range []string{"current=145.0MiB", "limit=200.0MiB", "slope_per_min=1.0MiB"} {
				if !strings.Contains(sig.Message, want) {
					t.Errorf("message %q missing evidence %q", sig.Message, want)
				}
			}
		case i > 90 && i <= 170 && len(fired) != 1:
			// ETA is inside (15m, 60m) until i=170: warning is
			// latched, nothing re-fires.
			t.Fatalf("i=%d: signals = %d, want the warning latched (no same-severity re-fire)", i, len(fired))
		case i == 172:
			// current = 186MiB → ETA = 14m < 15m: escalation.
			if len(fired) != 2 {
				t.Fatalf("i=172: signals = %d, want the critical escalation to fire", len(fired))
			}
			if fired[1].Severity != engine.SeverityCritical {
				t.Errorf("escalation severity = %q, want critical", fired[1].Severity)
			}
			wantETA := now.Add(14 * time.Minute)
			if d := fired[1].Forecast.ETA.Sub(wantETA); d < -2*time.Second || d > 2*time.Second {
				t.Errorf("escalation ETA = %v, want %v ±2s", fired[1].Forecast.ETA, wantETA)
			}
		}
	}
}

func TestNoForecast_InsufficientSamples(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(smallCfg(), pods, nil) // MinSamples 5
	ctx := context.Background()
	// 4 samples spanning 6m (span gate passes; count gate must not).
	for i := 0; i < 4; i++ {
		pods.samples = []ContainerSample{memSample(float64(i)*10*mib, 45*mib)}
		if got := s.sampleOnce(ctx, t0.Add(time.Duration(i)*2*time.Minute)); len(got) != 0 {
			t.Fatalf("emitted %d signal(s) from %d samples, want none below MinSamples", len(got), i+1)
		}
	}
}

func TestNoForecast_InsufficientSpan(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(smallCfg(), pods, nil) // minSpan 5m
	ctx := context.Background()
	// 12 rapid samples spanning 110s with a dire slope: count gate
	// passes, span gate must not (§13 insufficient window).
	for i := 0; i < 12; i++ {
		pods.samples = []ContainerSample{memSample(float64(i)*5*mib, 70*mib)}
		if got := s.sampleOnce(ctx, t0.Add(time.Duration(i)*10*time.Second)); len(got) != 0 {
			t.Fatalf("emitted %d signal(s) over a %ds span, want none below window/2", len(got), i*10)
		}
	}
}

func TestNoForecast_NonPositiveSlope(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(smallCfg(), pods, nil)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		pods.samples = []ContainerSample{memSample(100*mib-float64(i)*mib, 110*mib)}
		if got := s.sampleOnce(ctx, t0.Add(time.Duration(i)*30*time.Second)); len(got) != 0 {
			t.Fatalf("emitted %d signal(s) on a falling series, want none (non-positive slope)", len(got))
		}
	}
}

func TestNoForecast_NoLimit(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(smallCfg(), pods, nil)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		pods.samples = []ContainerSample{memSample(float64(i)*50*mib, 0)} // no limit
		if got := s.sampleOnce(ctx, t0.Add(time.Duration(i)*30*time.Second)); len(got) != 0 {
			t.Fatalf("emitted %d signal(s) for a limitless container, want none (no ceiling → no forecast)", len(got))
		}
	}
}

// fireWarning drives the small config to a latched warning and
// returns the source, fetcher, and the next sample index/time.
func fireWarning(t *testing.T, pods *scriptedPods, s *Source) (i int, now time.Time) {
	t.Helper()
	ctx := context.Background()
	for i = 0; i < 60; i++ {
		now = t0.Add(time.Duration(i) * 30 * time.Second)
		// slope 2MiB/min toward a 40MiB limit: ETA = 20m - m,
		// crossing warn (10m) at m > 10.
		pods.samples = []ContainerSample{memSample(float64(i)*mib, 40*mib)}
		if got := s.sampleOnce(ctx, now); len(got) > 0 {
			if got[0].Severity != engine.SeverityWarning {
				t.Fatalf("first fire severity = %q, want warning", got[0].Severity)
			}
			return i + 1, now
		}
	}
	t.Fatal("warning never fired")
	return 0, time.Time{}
}

func satIncident() engine.Incident {
	return engine.Incident{
		Key: engine.EventKey{UID: "u1/app", Reason: "forecast_memory"},
		Ref: engine.IncidentRef{Namespace: "prod", KindOfObject: "Pod", Name: "web-1", Container: "app"},
	}
}

func TestClearance_FlatteningTrendClearsAndReleasesLatch(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(smallCfg(), pods, nil)
	ctx := context.Background()
	i, _ := fireWarning(t, pods, s)

	// While latched and still trending up: claimed, not cleared.
	if verdict, ok := s.Clearance(satIncident()); !ok || verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want claimed + symptomatic", verdict, ok)
	}

	// The leak stops: usage holds flat. The fitted slope shrinks as
	// the rising history ages out of the window, so the ETA recedes
	// beyond 2×warn (and eventually the slope itself goes
	// non-positive) — either way the clearance contract reports the
	// symptom absent, with no re-fires along the way.
	flat := pods.samples[0].Used
	var clearedAt time.Time
	var refires int
	for j := 0; j < 80; j++ {
		now := t0.Add(time.Duration(i+j) * 30 * time.Second)
		pods.samples = []ContainerSample{memSample(flat, 40*mib)}
		refires += len(s.sampleOnce(ctx, now))
		s.now = func() time.Time { return now }
		if verdict, ok := s.Clearance(satIncident()); ok && verdict.Cleared {
			clearedAt = now
			if verdict.Resolution != engine.ResolutionRecovered {
				t.Fatalf("Resolution = %q, want recovered", verdict.Resolution)
			}
			if verdict.StableSince.IsZero() {
				t.Fatal("cleared verdict carries no StableSince")
			}
			break
		}
	}
	if refires != 0 {
		t.Errorf("flat series re-fired %d time(s), want none", refires)
	}
	if clearedAt.IsZero() {
		t.Fatal("clearance never reported the symptom absent on a flat series")
	}

	// Latch released: a fresh approach fires again.
	cur := flat
	var refired bool
	for j := 0; j < 60 && !refired; j++ {
		now := clearedAt.Add(time.Duration(j+1) * 30 * time.Second)
		cur += 1.5 * mib // 3MiB/min — back toward the limit
		pods.samples = []ContainerSample{memSample(cur, 60*mib)}
		refired = len(s.sampleOnce(ctx, now)) > 0
	}
	if !refired {
		t.Error("a fresh approach after clearance never re-fired (latch stuck)")
	}
}

// TestClearance_NonPositiveSlope_ReobservePeriodMath pins the second
// clearance rule at the state level (deterministically — on smooth
// series the recede rule usually wins first): a non-positive slope
// clears only after it has HELD for a full re-observation period
// (window/2), stable since the slope turned.
func TestClearance_NonPositiveSlope_ReobservePeriodMath(t *testing.T) {
	t.Parallel()
	s, _ := testSource(smallCfg(), &scriptedPods{}, nil)
	s.fetched = true
	turned := t0
	s.series[targetKey{uid: "u1/app", resource: ResourceMemory}] = &series{
		kindOfObject: "Pod", namespace: "prod", name: "web-1", container: "app",
		firedSeverity: engine.SeverityWarning,
		nonPosSince:   turned,
		lastSample:    turned,
	}

	early := turned.Add(s.cfg.reobserve() - time.Second)
	s.now = func() time.Time { return early }
	if verdict, ok := s.Clearance(satIncident()); !ok || verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want claimed + NOT cleared one second before the re-observation period elapses", verdict, ok)
	}

	due := turned.Add(s.cfg.reobserve())
	s.now = func() time.Time { return due }
	verdict, ok := s.Clearance(satIncident())
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("verdict = %+v ok=%v, want cleared/recovered after a full re-observation period", verdict, ok)
	}
	if !verdict.StableSince.Equal(turned) {
		t.Errorf("StableSince = %v, want the instant the slope turned (%v)", verdict.StableSince, turned)
	}
}

func TestClearance_ETARecedesBeyondTwiceWarn(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(smallCfg(), pods, nil)
	ctx := context.Background()
	i, _ := fireWarning(t, pods, s)

	// The operator raises the limit (a real fix): same slope, but the
	// ETA jumps far beyond 2×warn on the next evaluation → immediate
	// recede-clear.
	now := t0.Add(time.Duration(i) * 30 * time.Second)
	pods.samples = []ContainerSample{memSample(float64(i)*mib, 400*mib)}
	if got := s.sampleOnce(ctx, now); len(got) != 0 {
		t.Fatalf("emitted %d signal(s) on the receded evaluation, want none", len(got))
	}
	verdict, ok := s.Clearance(satIncident())
	if !ok || !verdict.Cleared || verdict.Resolution != engine.ResolutionRecovered {
		t.Fatalf("verdict = %+v ok=%v, want cleared/recovered on recede", verdict, ok)
	}
	if !verdict.StableSince.Equal(now) {
		t.Errorf("StableSince = %v, want the recede instant %v", verdict.StableSince, now)
	}
}

func TestClearance_TargetGone_ObjectDeleted(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	cfg := smallCfg()
	cfg.StaleAfter = 2 * time.Minute
	s, _ := testSource(cfg, pods, nil)
	ctx := context.Background()

	// Before any cycle: cannot judge (an empty map is not "deleted").
	if _, ok := s.Clearance(satIncident()); ok {
		t.Fatal("observer must decline before the first successful cycle")
	}

	i, now := fireWarning(t, pods, s)
	_ = i
	// Pod gone: its samples stop; the series goes stale and is pruned.
	pods.samples = nil
	s.sampleOnce(ctx, now.Add(3*time.Minute))
	verdict, ok := s.Clearance(satIncident())
	if !ok || !verdict.Cleared {
		t.Fatalf("verdict = %+v ok=%v, want cleared for a vanished target", verdict, ok)
	}
	if verdict.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("Resolution = %q, want object_deleted (a deletion is not a fix, §9.3)", verdict.Resolution)
	}
}

func TestClearance_ClaimsForecastIncidentsAwayFromPodObservers(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{}
	s, _ := testSource(smallCfg(), pods, nil)
	fireWarning(t, pods, s)
	// The incident's object is a READY pod (a leak doesn't flip
	// readiness) — the saturation observer must claim it (ok=true,
	// not cleared) so an ordering mistake with a pod-readiness
	// observer can't wrongly resolve it.
	verdict, ok := s.Clearance(satIncident())
	if !ok {
		t.Fatal("observer must claim its own forecast incidents even while symptomatic")
	}
	if verdict.Cleared {
		t.Fatal("symptomatic forecast incident reported cleared")
	}
	// Foreign incidents are declined.
	if _, ok := s.Clearance(engine.Incident{Key: engine.EventKey{UID: "p9", Reason: "CrashLoopBackOff"}, Ref: engine.IncidentRef{KindOfObject: "Pod"}}); ok {
		t.Error("observer must not claim non-saturation incidents")
	}
}

func TestPVCPath_ForecastFromKubeletStats(t *testing.T) {
	t.Parallel()
	vols := &scriptedVolumes{}
	pods := &scriptedPods{} // empty but healthy
	s, _ := testSource(smallCfg(), pods, vols)
	ctx := context.Background()

	// 1GiB PVC filling at ~51MiB/min: ETA crosses warn(10m) quickly
	// once the span gate opens.
	var fired []engine.Signal
	for i := 0; i < 30 && len(fired) == 0; i++ {
		used := 500*mib + float64(i)*25.5*mib
		vols.samples = []VolumeSample{{Namespace: "prod", ClaimName: "data-web-0", UsedBytes: used, CapacityBytes: 1024 * mib}}
		fired = append(fired, s.sampleOnce(ctx, t0.Add(time.Duration(i)*30*time.Second))...)
	}
	if len(fired) != 1 {
		t.Fatalf("signals = %d, want one PVC forecast", len(fired))
	}
	sig := fired[0]
	if sig.Kind != KindForecast || sig.KindOfObject != "PersistentVolumeClaim" {
		t.Errorf("kind=%q object=%q, want %s on a PersistentVolumeClaim", sig.Kind, sig.KindOfObject, KindForecast)
	}
	if sig.Key.UID != "pvc:prod/data-web-0" || sig.Key.Reason != "forecast_pvc" {
		t.Errorf("dedup key = %+v, want uid=pvc:prod/data-web-0 reason=forecast_pvc", sig.Key)
	}
	if sig.Forecast == nil || sig.Forecast.ConfidenceBasis != "linear-10m-window" {
		t.Errorf("forecast = %+v, want linear-10m-window basis", sig.Forecast)
	}
}

func TestKubeletUnreachable_OneLoudLogAndPVCDimensionSkipped(t *testing.T) {
	t.Parallel()
	vols := &scriptedVolumes{err: errors.New("nodes \"n1\" is forbidden: proxy blocked")}
	pods := &scriptedPods{}
	s, logs := testSource(smallCfg(), pods, vols)
	ctx := context.Background()

	// Many failing cycles: exactly ONE loud log, container dimension
	// unaffected (prove it by driving a memory series to a fire).
	for i := 0; i < 40; i++ {
		pods.samples = []ContainerSample{memSample(float64(i)*mib, 40*mib)}
		s.sampleOnce(ctx, t0.Add(time.Duration(i)*30*time.Second))
	}
	var loud int
	for _, l := range *logs {
		if strings.Contains(l, "kubelet stats summary unreachable") {
			loud++
			if !strings.Contains(l, "SKIPPING the PVC dimension") {
				t.Errorf("unreachable log %q does not say the PVC dimension is skipped", l)
			}
		}
	}
	if loud != 1 {
		t.Fatalf("kubelet-unreachable logged %d times over 40 failing cycles, want exactly once", loud)
	}
	if vols.calls != 40 {
		t.Errorf("volume fetcher called %d times, want every cycle (the dimension resumes when reachable)", vols.calls)
	}

	// It comes back: one recovery log, samples flow again.
	vols.err = nil
	vols.samples = []VolumeSample{{Namespace: "prod", ClaimName: "data-0", UsedBytes: 1, CapacityBytes: 100}}
	s.sampleOnce(ctx, t0.Add(21*time.Minute))
	var resumed int
	for _, l := range *logs {
		if strings.Contains(l, "PVC dimension resumed") {
			resumed++
		}
	}
	if resumed != 1 {
		t.Errorf("resume logged %d times, want once", resumed)
	}
}

func TestRun_MetricsAPIUnavailableAtStartup_FailsLoudly(t *testing.T) {
	t.Parallel()
	pods := &scriptedPods{err: errors.New("the server could not find the requested resource")}
	s, _ := testSource(DefaultConfig(), pods, nil)
	err := s.Run(context.Background(), func(engine.Signal) {})
	if err == nil {
		t.Fatal("Run must fail loudly when metrics.k8s.io is unavailable at startup (§11)")
	}
	if !strings.Contains(err.Error(), "metrics.k8s.io") || !strings.Contains(err.Error(), "disable the source") {
		t.Errorf("error %q should name the API and the way out", err)
	}
}

func TestRequiredAccess_DeclaresMetricsAndNodeProxy(t *testing.T) {
	t.Parallel()
	s, _ := testSource(DefaultConfig(), &scriptedPods{}, nil)
	got := make(map[string]bool)
	for _, req := range s.RequiredAccess() {
		got[req.String()] = true
	}
	for _, want := range []string{
		"get pods.metrics.k8s.io cluster-wide",
		"list pods.metrics.k8s.io cluster-wide",
		"list pods cluster-wide",
		"list nodes cluster-wide",
		"get nodes/proxy cluster-wide",
	} {
		if !got[want] {
			t.Errorf("RequiredAccess missing %q (have %v)", want, got)
		}
	}
}

func TestProbe_MissingGrantNamesThisSource(t *testing.T) {
	t.Parallel()
	s, _ := testSource(DefaultConfig(), &scriptedPods{}, nil)
	err := sources.Probe(context.Background(), denyReviewer{}, s)
	if err == nil {
		t.Fatal("Probe must fail loudly when a grant is missing (§11)")
	}
	if !strings.Contains(err.Error(), Name) {
		t.Errorf("error %q should name the source", err)
	}
}

type denyReviewer struct{}

func (denyReviewer) Allowed(context.Context, sources.Requirement) (bool, error) { return false, nil }
