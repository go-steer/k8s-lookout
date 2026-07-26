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
	"github.com/go-steer/k8s-lookout/pkg/sources/tokenburn"
)

// TestSourcesFlag_TokenBurnEnabled: the M5 token-burn source (§7.2
// row 9, §12) registers additively under its table name, comes back
// as a typed handle for §7.4 observer wiring, defaults to the
// injector's daemon URL for its cost-stack endpoint, and stays OFF
// by default.
func TestSourcesFlag_TokenBurnEnabled(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=k8s-events,token-burn", "--daemon-url=http://daemon.local:8420", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	bs, err := buildSources(f, "tok", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if bs.tokenBurn == nil {
		t.Fatal("token-burn enabled but not returned as a typed handle")
	}
	if _, ok := bs.registry.Lookup(tokenburn.Name); !ok {
		t.Error("token-burn not registered")
	}

	// Default surface: token-burn must NOT be constructed.
	fDefault, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	bsDefault, err := buildSources(fDefault, "", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources(default): %v", err)
	}
	if bsDefault.tokenBurn != nil {
		t.Error("token-burn must NOT be constructed by default")
	}
}

// TestSourcesFlag_TokenBurnEndpointOverride: --token-endpoint wins
// over --daemon-url, and enabling the source with NEITHER is a loud
// config error naming the source (§11 posture) — the only way to hit
// it is --dry-run, which skips --daemon-url.
func TestSourcesFlag_TokenBurnEndpointOverride(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--sources=token-burn", "--token-endpoint=http://cost.local:8420", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if bs, err := buildSources(f, "", fake.NewSimpleClientset(), nil, nil, nil); err != nil || bs.tokenBurn == nil {
		t.Fatalf("buildSources with --token-endpoint = (%v, %v), want the source built", bs, err)
	}

	f, err = parseFlags([]string{"--sources=token-burn", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	err = f.validate()
	if err == nil {
		t.Fatal("token-burn without --daemon-url or --token-endpoint must be a config error")
	}
	if !strings.Contains(err.Error(), tokenburn.Name) {
		t.Errorf("error %q should name the source", err)
	}
}

// TestTokenBurnFlags_DefaultsAndBounds pins the ADDITIVE M5 flag
// surface: --token-poll 60s, --burn-multiple 4, --burn-eta 30m,
// --token-budget-usd 0 (budget unknown → trigger disarmed),
// --token-endpoint empty (ride --daemon-url); nonsensical values
// rejected in every mode.
func TestTokenBurnFlags_DefaultsAndBounds(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.tokenPoll != 60*time.Second {
		t.Errorf("default --token-poll = %v, want 60s", f.tokenPoll)
	}
	if f.burnMultiple != 4 {
		t.Errorf("default --burn-multiple = %v, want 4", f.burnMultiple)
	}
	if f.burnETA != 30*time.Minute {
		t.Errorf("default --burn-eta = %v, want 30m", f.burnETA)
	}
	if f.tokenBudgetUSD != 0 {
		t.Errorf("default --token-budget-usd = %v, want 0 (unknown)", f.tokenBudgetUSD)
	}
	if f.tokenEndpoint != "" {
		t.Errorf("default --token-endpoint = %q, want empty (ride --daemon-url)", f.tokenEndpoint)
	}
	for _, bad := range [][]string{
		{"--token-poll=0s", "--dry-run"},
		{"--token-poll=-10s", "--dry-run"},
		{"--burn-multiple=1", "--dry-run"},
		{"--burn-multiple=0.5", "--dry-run"},
		{"--burn-eta=0s", "--dry-run"},
		{"--token-budget-usd=-1", "--dry-run"},
		{"--token-endpoint=http://cost.local/", "--dry-run"},
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

// TestSetupRecovery_TokenBurnObserverWithoutPodRBAC: the token-burn
// clearance observer keeps the §7.4 loop alive even when the
// fallback pod observer's RBAC is missing — same posture as the
// other source-specific observers.
func TestSetupRecovery_TokenBurnObserverWithoutPodRBAC(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset() // fake SSAR returns not-allowed
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	disp := &dispatcher{filter: engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0)), dedup: dedup, metrics: newMetrics(), dryRun: true}
	bs := &builtSources{
		tokenBurn: tokenburn.New(tokenburn.NewHTTPClient("http://daemon.local:8420", ""), tokenburn.DefaultConfig()),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := &flags{recoveryStableFor: 5 * time.Minute}
	if err := setupRecovery(ctx, f, client, dedup, disp, newMetrics(), bs); err != nil {
		t.Fatalf("setupRecovery: %v", err)
	}
	if disp.tracker == nil {
		t.Fatal("recovery must stay ENABLED via the token-burn observer when only pod RBAC is missing")
	}
}
