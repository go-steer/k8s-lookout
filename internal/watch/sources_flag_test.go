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
	"encoding/json"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/degradation"
	"github.com/go-steer/k8s-lookout/pkg/sources/expiry"
	"github.com/go-steer/k8s-lookout/pkg/sources/ingress"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"

	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
)

// TestSourcesFlag_DefaultIsAuto pins the --sources default. DEFAULT
// CHANGED DELIBERATELY on 2026-07-27 under the zero-deployed-users
// policy (one post-M0 pin change, recorded in the CHANGELOG): the
// default is now "auto" — probe-and-enable across the portable set,
// resolved by resolveSourcesAuto (auto_test.go) — where it had been
// k8s-events only (the frozen M0 surface) since the flag landed. The
// M0 flag surface itself (TestFlagSurfaceFrozen) predates --sources
// and is untouched. --sources=k8s-events reproduces the pre-auto
// default byte-for-byte, pinned below.
func TestSourcesFlag_DefaultIsAuto(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.sources != autoValue {
		t.Fatalf("default --sources = %q, want %q (the 2026-07-27 default change)", f.sources, autoValue)
	}

	// The old default, pinned explicitly: --sources=k8s-events builds
	// exactly the M0 surface — one source, no typed handles.
	f, err = parseFlags([]string{"--sources=k8s-events", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	bs, err := buildSources(f, "", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if bs.objState != nil {
		t.Error("object-state must NOT be constructed with --sources=k8s-events")
	}
	if bs.rollout != nil || bs.saturation != nil {
		t.Error("rollout/saturation must NOT be constructed with --sources=k8s-events")
	}
	if bs.degradation != nil || bs.expiry != nil {
		t.Error("degradation/expiry must NOT be constructed with --sources=k8s-events")
	}
	all := bs.registry.All()
	if len(all) != 1 || all[0].Name() != k8sevents.Name {
		t.Errorf("--sources=k8s-events registry = %d sources, want just k8s-events", len(all))
	}
}

// TestSourcesFlag_AutoMixRejected: "auto" is a whole-value sentinel,
// not a list member — mixing it with named sources is a config error,
// never a guess.
func TestSourcesFlag_AutoMixRejected(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=auto,object-state", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	err = f.validate()
	if err == nil {
		t.Fatal("--sources=auto,<name> must be a config error")
	}
	if !strings.Contains(err.Error(), "auto cannot be combined") {
		t.Errorf("error should explain the auto/list conflict: %v", err)
	}
}

func TestSourcesFlag_ObjectStateEnabled(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=k8s-events,object-state", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	bs, err := buildSources(f, "", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if bs.objState == nil {
		t.Fatal("object-state enabled but not returned for recovery wiring")
	}
	if _, ok := bs.registry.Lookup(objectstate.Name); !ok {
		t.Error("object-state not registered")
	}
	if _, ok := bs.registry.Lookup(k8sevents.Name); !ok {
		t.Error("k8s-events not registered")
	}
}

// TestSourcesFlag_DegradationAndExpiryEnabled: the two M3 leading-
// indicator sources register additively and come back as typed
// handles for §7.4 observer + shared-factory wiring.
func TestSourcesFlag_DegradationAndExpiryEnabled(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=k8s-events,degradation,expiry", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	built, err := buildSources(f, "", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if built.degradation == nil {
		t.Fatal("degradation enabled but not returned for observer wiring")
	}
	if built.expiry == nil {
		t.Fatal("expiry enabled but not returned for observer wiring")
	}
	if _, ok := built.registry.Lookup(degradation.Name); !ok {
		t.Error("degradation not registered")
	}
	if _, ok := built.registry.Lookup(expiry.Name); !ok {
		t.Error("expiry not registered")
	}
}

// TestSourcesFlag_DegradationExpiryDefaults pins the new ADDITIVE
// flags' defaults (§7.2 rows 5–6 normative values) and their bounds.
func TestSourcesFlag_DegradationExpiryDefaults(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.degradationWindow != 15*time.Minute {
		t.Errorf("default degradation-window = %v, want 15m", f.degradationWindow)
	}
	if f.degradationDrop != 0.3 {
		t.Errorf("default degradation-drop = %v, want 0.3", f.degradationDrop)
	}
	if f.expiryInterval != time.Hour {
		t.Errorf("default expiry-interval = %v, want 1h", f.expiryInterval)
	}
	if f.expiryWarn != 336*time.Hour {
		t.Errorf("default expiry-warn = %v, want 336h (14d)", f.expiryWarn)
	}
	if f.expiryNamespaces != "" {
		t.Errorf("default expiry-namespaces = %q, want empty (all)", f.expiryNamespaces)
	}

	for _, bad := range [][]string{
		{"--degradation-window=0s", "--dry-run"},
		{"--degradation-drop=0", "--dry-run"},
		{"--degradation-drop=1.5", "--dry-run"},
		{"--expiry-interval=0s", "--dry-run"},
		{"--expiry-warn=71h", "--dry-run"}, // below the design-fixed 72h critical
	} {
		f, err := parseFlags(bad)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", bad, err)
		}
		if err := f.validate(); err == nil {
			t.Errorf("validate(%v) accepted a nonsensical value", bad)
		}
	}
}

// TestDispatchSignal_ForecastSerialized: a Signal carrying the §8
// Forecast (trend/countdown sources) surfaces it as the ADDITIVE
// "forecast" payload field; signals without one keep the frozen shape
// (pinned byte-exact by TestDispatchSignal_SourcePathWireShapeFrozen).
func TestDispatchSignal_ForecastSerialized(t *testing.T) {
	t.Parallel()
	base, injects, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{
		filter:    engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1)),
		dedup:     dedup,
		injector:  inj,
		metrics:   newMetrics(),
		cluster:   "prod-us-central1",
		mode:      "shared",
		targetSid: "sess-shared",
	}
	disp.DispatchSignal(context.Background(), engine.Signal{
		Kind:     expiry.KindWarning,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "sec-1", Reason: "warning"},
			Namespace:    "prod",
			KindOfObject: "Secret",
			Name:         "api-tls",
			Message:      "certificate expires in 72h",
			FirstSeen:    time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
			LastSeen:     time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
			Count:        1,
		},
		Forecast: &engine.Forecast{ETA: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC), ConfidenceBasis: expiry.ConfidenceBasis},
	})
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte((*injects)[0]), &envelope); err != nil {
		t.Fatalf("captured body isn't the inject envelope: %v", err)
	}
	if !strings.Contains(envelope.Message, `"forecast":{"eta":"2026-08-08T12:00:00Z","confidence_basis":"certificate-notAfter"}`) {
		t.Errorf("payload missing the §8 forecast field:\n%s", envelope.Message)
	}
}

func TestSourcesFlag_UnknownSourceRejected(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=k8s-events,vessel", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	err = f.validate()
	if err == nil {
		t.Fatal("unknown source name must be a config error, not a silent skip")
	}
	if !strings.Contains(err.Error(), "vessel") || !strings.Contains(err.Error(), objectstate.Name) {
		t.Errorf("error %q should name the bad source and the known ones", err)
	}
}

func TestSourcesFlag_EmptyRejected(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err == nil {
		t.Fatal("--sources= (no sources) must be a config error")
	}
}

// TestSetupRecovery_ObjectStateObserverAbsorbed: when object-state is
// enabled, recovery wires the tracker to the SOURCE's clearance
// observer — no standalone pod informer, no separate RBAC probe (the
// §11 source probe already covered pods list/watch).
func TestSetupRecovery_ObjectStateObserverAbsorbed(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset() // fake SSAR would DENY — proving no probe runs on this path
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{filter: engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1)), dedup: dedup, metrics: newMetrics(), dryRun: true}
	objState := objectstate.New(client, objectstate.DefaultConfig())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := &flags{recoveryStableFor: 5 * time.Minute}
	if err := setupRecovery(ctx, f, client, dedup, disp, newMetrics(), &builtSources{objState: objState}); err != nil {
		t.Fatalf("setupRecovery: %v", err)
	}
	if disp.tracker == nil {
		t.Fatal("recovery not enabled with the object-state observer")
	}
}

// TestSetupRecovery_FallbackKeepsZeroConfigBehavior: without
// object-state, the standalone observer path runs its own RBAC
// probe and — as shipped in the recovery PR — disables recovery
// loudly (nil tracker, nil error) when pods list/watch is missing,
// instead of crash-looping an M0 deployment.
func TestSetupRecovery_FallbackKeepsZeroConfigBehavior(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset() // fake SSAR returns not-allowed
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{filter: engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1)), dedup: dedup, metrics: newMetrics(), dryRun: true}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := &flags{recoveryStableFor: 5 * time.Minute}
	if err := setupRecovery(ctx, f, client, dedup, disp, newMetrics(), &builtSources{}); err != nil {
		t.Fatalf("setupRecovery must not fail on missing RBAC in fallback mode: %v", err)
	}
	if disp.tracker != nil {
		t.Fatal("recovery must be DISABLED (not enabled, not fatal) when the fallback observer lacks RBAC")
	}
}

// TestSourcesFlag_RolloutAndSaturationEnabled: the two M3 trend/as-it-
// happens sources register under their §7.2 names and come back as
// typed handles for shared-factory + clearance wiring.
func TestSourcesFlag_RolloutAndSaturationEnabled(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=k8s-events,rollout,saturation", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	bs, err := buildSources(f, "", fake.NewSimpleClientset(), nil, metricsfake.NewSimpleClientset(), nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if bs.rollout == nil || bs.saturation == nil {
		t.Fatalf("rollout=%v saturation=%v, want both constructed", bs.rollout, bs.saturation)
	}
	for _, name := range []string{rollout.Name, saturation.Name, k8sevents.Name} {
		if _, ok := bs.registry.Lookup(name); !ok {
			t.Errorf("source %q not registered", name)
		}
	}
}

// TestSourcesFlag_SaturationWithoutMetricsClientIsAnError: buildSources
// must refuse rather than construct a saturation source that would
// nil-pointer at first fetch.
func TestSourcesFlag_SaturationWithoutMetricsClient(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=saturation", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if _, err := buildSources(f, "", fake.NewSimpleClientset(), nil, nil, nil); err == nil {
		t.Fatal("buildSources must fail when saturation is enabled without a metrics client")
	}
}

// TestTrendFlags_DefaultsAndBounds pins the ADDITIVE M3 flag surface:
// defaults per DESIGN.md §7.2 rows 3-4, nonsensical values rejected in
// every mode.
func TestTrendFlags_DefaultsAndBounds(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.rolloutObserve != 3*time.Minute {
		t.Errorf("default --rollout-observe = %v, want 3m", f.rolloutObserve)
	}
	if f.saturationInterval != 30*time.Second {
		t.Errorf("default --saturation-interval = %v, want 30s", f.saturationInterval)
	}
	if f.saturationWindow != 90*time.Minute {
		t.Errorf("default --saturation-window = %v, want 90m (the §8 linear-90m-window basis)", f.saturationWindow)
	}
	if f.saturationWarn != 60*time.Minute {
		t.Errorf("default --saturation-warn = %v, want 60m", f.saturationWarn)
	}
	for _, bad := range [][]string{
		{"--rollout-observe=0s", "--dry-run"},
		{"--saturation-interval=0s", "--dry-run"},
		{"--saturation-window=10s", "--saturation-interval=30s", "--dry-run"},
		{"--saturation-warn=0s", "--dry-run"},
	} {
		f, err := parseFlags(bad)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", bad, err)
		}
		if err := f.validate(); err == nil {
			t.Errorf("validate(%v) accepted a nonsensical bound", bad)
		}
	}
}

// TestSetupRecovery_TrendObserversWithoutPodRBAC: rollout/saturation
// clearance observers keep the §7.4 loop alive even when the fallback
// pod observer's RBAC is missing (fake SSAR denies) — pod clearance is
// disabled loudly, the tracker still runs for the sources' own kinds.
func TestSetupRecovery_TrendObserversWithoutPodRBAC(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset() // fake SSAR returns not-allowed
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{filter: engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1)), dedup: dedup, metrics: newMetrics(), dryRun: true}
	bs := &builtSources{
		rollout:    rollout.New(client, rollout.DefaultConfig()),
		saturation: saturation.New(saturation.DefaultConfig(), nil, nil),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := &flags{recoveryStableFor: 5 * time.Minute}
	if err := setupRecovery(ctx, f, client, dedup, disp, newMetrics(), bs); err != nil {
		t.Fatalf("setupRecovery: %v", err)
	}
	if disp.tracker == nil {
		t.Fatal("recovery must stay ENABLED via the rollout/saturation observers when only pod RBAC is missing")
	}
}

// TestSourcesFlag_CapacityEnabled: the M4 capacity source registers
// under its §7.2 name, receives the provider handle (here nil →
// cloud.NoProvider inside the source: §2 explicit degradation), and
// stays OFF by default.
func TestSourcesFlag_CapacityEnabled(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=k8s-events,capacity", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	bs, err := buildSources(f, "", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if bs.capacity == nil {
		t.Fatal("capacity enabled but not returned as a typed handle")
	}
	if _, ok := bs.registry.Lookup(capacity.Name); !ok {
		t.Error("capacity not registered")
	}

	// Explicit lists stay literal: a list without capacity must NOT
	// construct it (the pre-auto default surface, pinned).
	fDefault, err := parseFlags([]string{"--sources=k8s-events", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	bsDefault, err := buildSources(fDefault, "", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources(k8s-events only): %v", err)
	}
	if bsDefault.capacity != nil {
		t.Error("capacity must NOT be constructed without being named")
	}
}

// TestSourcesFlag_IngressEnabled: the post-M5 #135 ingress source
// registers under its §7.2 name (no cloud provider, no typed handle —
// it declares no clearance observer and rides no shared factory), and
// an explicit list without it must not construct it.
func TestSourcesFlag_IngressEnabled(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=k8s-events,ingress", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	bs, err := buildSources(f, "", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if _, ok := bs.registry.Lookup(ingress.Name); !ok {
		t.Error("ingress not registered")
	}

	fDefault, err := parseFlags([]string{"--sources=k8s-events", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	bsDefault, err := buildSources(fDefault, "", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources(k8s-events only): %v", err)
	}
	if _, ok := bsDefault.registry.Lookup(ingress.Name); ok {
		t.Error("ingress must NOT be constructed without being named")
	}
}

// TestCapacityFlags_DefaultsAndBounds pins the ADDITIVE M4 flag
// surface: --capacity-poll 60s, --pending-age 5m, nonsensical values
// rejected in every mode.
func TestCapacityFlags_DefaultsAndBounds(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.capacityPoll != 60*time.Second {
		t.Errorf("default --capacity-poll = %v, want 60s", f.capacityPoll)
	}
	if f.pendingAge != 5*time.Minute {
		t.Errorf("default --pending-age = %v, want 5m", f.pendingAge)
	}
	for _, bad := range [][]string{
		{"--capacity-poll=0s", "--dry-run"},
		{"--capacity-poll=-10s", "--dry-run"},
		{"--pending-age=0s", "--dry-run"},
	} {
		f, err := parseFlags(bad)
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", bad, err)
		}
		if err := f.validate(); err == nil {
			t.Errorf("validate(%v) accepted a nonsensical value", bad)
		}
	}
}
