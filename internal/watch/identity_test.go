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

// §8 deployment-identity wiring: zone/project resolve by the
// documented precedence (explicit flag > provider metadata > empty)
// and the dispatcher stamps them onto every signal, so
// source-namespaced payloads and fingerprints carry real zones
// in-cluster. The M0-frozen k8s-event pair stays byte-identical (its
// wire pin is untouched by design); empty-zone deployments keep
// producing the zone-less fingerprints they always did.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
)

// identityProvider is NoProvider plus a cloud.Identity surface — the
// shape of a gke-tagged provider that resolved its metadata.
type identityProvider struct {
	cloud.Provider
	project, location string
}

func (p identityProvider) Project() string  { return p.project }
func (p identityProvider) Location() string { return p.location }

// TestIdentityPrecedence pins the documented resolution order:
// explicit flag > provider metadata > empty.
func TestIdentityPrecedence(t *testing.T) {
	t.Parallel()
	detected := identityProvider{Provider: cloud.NoProvider, project: "meta-proj", location: "us-central1-b"}
	cases := []struct {
		name                  string
		provider              cloud.Provider
		flagProject, flagZone string
		wantProject, wantZone string
	}{
		{"flags win over metadata", detected, "flag-proj", "flag-zone", "flag-proj", "flag-zone"},
		{"metadata fills blank flags", detected, "", "", "meta-proj", "us-central1-b"},
		{"mixed: flag zone, metadata project", detected, "", "flag-zone", "meta-proj", "flag-zone"},
		{"mixed: flag project, metadata zone", detected, "flag-proj", "", "flag-proj", "us-central1-b"},
		{"no identity surface: flags only", cloud.NoProvider, "flag-proj", "", "flag-proj", ""},
		{"nothing anywhere: empty (zone-less fingerprints)", cloud.NoProvider, "", "", "", ""},
		{"undetectable metadata stays empty", identityProvider{Provider: cloud.NoProvider}, "", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			project, zone := identityFromProvider(tc.provider, tc.flagProject, tc.flagZone)
			if project != tc.wantProject || zone != tc.wantZone {
				t.Errorf("identityFromProvider = (%q, %q), want (%q, %q)", project, zone, tc.wantProject, tc.wantZone)
			}
		})
	}
}

// TestIdentityFlagsAdditive pins the two new flags as ADDITIVE: they
// parse, and their defaults are empty — a deployment that sets
// nothing keeps the M0 behavior (and zone-less fingerprints)
// byte-identical. Companion to TestFlagSurfaceFrozen.
func TestIdentityFlagsAdditive(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--project=p1", "--zone=us-east1-c"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.project != "p1" || f.zone != "us-east1-c" {
		t.Errorf("parsed (project, zone) = (%q, %q), want (p1, us-east1-c)", f.project, f.zone)
	}
	f, err = parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.project != "" || f.zone != "" {
		t.Errorf("default (project, zone) = (%q, %q), want empty (additive contract)", f.project, f.zone)
	}
}

// identitySignal is a source-namespaced signal (the §8 stamped wire
// shape — the frozen k8s-event pair is excluded from stamping).
func identitySignal(ts time.Time) engine.Signal {
	return engine.Signal{
		Kind:     saturation.KindForecast,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "u9/app", Reason: "forecast_memory"},
			Namespace:    "prod",
			KindOfObject: "Pod",
			Name:         "web-9",
			Container:    "app",
			Message:      "memory saturation forecast",
			FirstSeen:    ts,
			LastSeen:     ts,
			Count:        1,
		},
	}
}

func identityDispatcher(t *testing.T, project, zone string) (*dispatcher, *[]string) {
	t.Helper()
	base, injects, _ := newFakeDaemon(t)
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "sre@example.com"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	return &dispatcher{
		filter:    engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1, 0)),
		dedup:     dedup,
		injector:  inj,
		metrics:   newMetrics(),
		cluster:   "prod-us-central1",
		project:   project,
		zone:      zone,
		mode:      "shared",
		targetSid: "sess-shared",
	}, injects
}

// TestDispatchSignal_StampsZoneIntoPayloadAndFingerprint: with a
// resolved identity, a source-namespaced payload carries the real
// zone/project and its fingerprint hashes the zone — the
// (fingerprint, cluster/project/zone) fleet join key, complete.
func TestDispatchSignal_StampsZoneIntoPayloadAndFingerprint(t *testing.T) {
	t.Parallel()
	disp, injects := identityDispatcher(t, "prod-project", "us-central1-b")
	ts := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	disp.DispatchSignal(context.Background(), identitySignal(ts))
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte((*injects)[0]), &envelope); err != nil {
		t.Fatalf("captured body isn't the inject envelope: %v", err)
	}
	var payload inject.Payload
	if err := json.Unmarshal([]byte(envelope.Message), &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if payload.Project != "prod-project" || payload.Zone != "us-central1-b" {
		t.Errorf("payload (project, zone) = (%q, %q), want (prod-project, us-central1-b)", payload.Project, payload.Zone)
	}
	want := engine.Fingerprint(saturation.KindForecast, engine.CanonicalReason("forecast_memory"), "Pod", "us-central1-b")
	if payload.Fingerprint != want {
		t.Errorf("fingerprint = %s, want the zone-hashed %s", payload.Fingerprint, want)
	}
	// The zone participates in the hash: the same class in another
	// zone (or with none) is a different fleet-rollup key.
	if zoneless := engine.Fingerprint(saturation.KindForecast, engine.CanonicalReason("forecast_memory"), "Pod", ""); payload.Fingerprint == zoneless {
		t.Error("zone did not participate in the fingerprint hash")
	}
}

// TestDispatchSignal_EmptyZoneFallback: a deployment with no flags
// and no provider metadata keeps the pre-wiring behavior exactly —
// zone/project absent from the wire (omitempty) and the zone-less
// fingerprint, still stable across push and scan.
func TestDispatchSignal_EmptyZoneFallback(t *testing.T) {
	t.Parallel()
	disp, injects := identityDispatcher(t, "", "")
	ts := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	disp.DispatchSignal(context.Background(), identitySignal(ts))
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte((*injects)[0]), &envelope); err != nil {
		t.Fatalf("captured body isn't the inject envelope: %v", err)
	}
	if strings.Contains(envelope.Message, `"zone"`) || strings.Contains(envelope.Message, `"project"`) {
		t.Errorf("empty identity leaked onto the wire (omitempty contract): %s", envelope.Message)
	}
	var payload inject.Payload
	if err := json.Unmarshal([]byte(envelope.Message), &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	want := engine.Fingerprint(saturation.KindForecast, engine.CanonicalReason("forecast_memory"), "Pod", "")
	if payload.Fingerprint != want {
		t.Errorf("fingerprint = %s, want the zone-less %s", payload.Fingerprint, want)
	}
}

// TestGKEProviderImplementsIdentity pins the compile-time contract
// resolveIdentity depends on: any provider that can say where it
// runs must satisfy cloud.Identity, and the sentinel's stub here
// mirrors the gke provider's exported surface (Project/Location).
// The gke package itself asserts the same under its build tag.
func TestGKEProviderImplementsIdentity(t *testing.T) {
	t.Parallel()
	var _ cloud.Identity = identityProvider{}
	if _, ok := cloud.NoProvider.(cloud.Identity); ok {
		t.Error("NoProvider must not implement cloud.Identity — vanilla deployments stamp flags only")
	}
}
