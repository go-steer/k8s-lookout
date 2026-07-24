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
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
)

// TestSourcesFlag_DefaultIsK8sEventsOnly pins the ADDITIVE flag
// contract: --sources defaults to exactly the M0 surface, so an
// existing deployment that never sets it keeps identical behavior
// (the frozen-flag test stays untouched by design).
func TestSourcesFlag_DefaultIsK8sEventsOnly(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.sources != k8sevents.Name {
		t.Fatalf("default --sources = %q, want %q", f.sources, k8sevents.Name)
	}
	registry, objState, err := buildSources(f, fake.NewSimpleClientset())
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if objState != nil {
		t.Error("object-state must NOT be constructed by default")
	}
	all := registry.All()
	if len(all) != 1 || all[0].Name() != k8sevents.Name {
		t.Errorf("default registry = %d sources, want just k8s-events", len(all))
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
	registry, objState, err := buildSources(f, fake.NewSimpleClientset())
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if objState == nil {
		t.Fatal("object-state enabled but not returned for recovery wiring")
	}
	if _, ok := registry.Lookup(objectstate.Name); !ok {
		t.Error("object-state not registered")
	}
	if _, ok := registry.Lookup(k8sevents.Name); !ok {
		t.Error("k8s-events not registered")
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
	disp := &dispatcher{filter: engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)), dedup: dedup, metrics: newMetrics(), dryRun: true}
	objState := objectstate.New(client, objectstate.DefaultConfig())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := &flags{recoveryStableFor: 5 * time.Minute}
	if err := setupRecovery(ctx, f, client, dedup, disp, newMetrics(), objState); err != nil {
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
	disp := &dispatcher{filter: engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)), dedup: dedup, metrics: newMetrics(), dryRun: true}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := &flags{recoveryStableFor: 5 * time.Minute}
	if err := setupRecovery(ctx, f, client, dedup, disp, newMetrics(), nil); err != nil {
		t.Fatalf("setupRecovery must not fail on missing RBAC in fallback mode: %v", err)
	}
	if disp.tracker != nil {
		t.Fatal("recovery must be DISABLED (not enabled, not fatal) when the fallback observer lacks RBAC")
	}
}
