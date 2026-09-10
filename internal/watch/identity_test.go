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

// §8 deployment-identity wiring: project/region/zone resolve by the
// documented precedence (explicit flag > provider metadata > empty)
// and the dispatcher stamps them onto every signal, so payloads and
// fingerprints carry a real location in-cluster.
//
// Region and zone are two properties of one cluster, not two names for
// one (amended 2026-09-10, issue #389): a zonal cluster has both, a
// regional cluster has a region and no zone. The M0-frozen k8s-event
// pair now carries the identity block too — a cluster NAME is unique
// only within a (project, location) pair, so `cluster` alone does not
// identify anything in a fleet — but every field is omitempty, so a
// deployment that stamps no identity still emits the M0 bytes exactly
// and keeps producing the domain-less fingerprints it always did.

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
	project, location, region string
}

func (p identityProvider) Project() string  { return p.project }
func (p identityProvider) Location() string { return p.location }
func (p identityProvider) Region() string   { return p.region }

// TestIdentityPrecedence pins the documented resolution order:
// explicit flag > provider metadata > empty.
func TestIdentityPrecedence(t *testing.T) {
	t.Parallel()
	// A ZONAL cluster: the provider's location is the zone, and the
	// region is the same place one level up.
	zonal := identityProvider{Provider: cloud.NoProvider, project: "meta-proj", location: "us-central1-b", region: "us-central1"}
	// A REGIONAL cluster: location and region are the same string, and
	// the cluster has no zone of its own (issue #389).
	regional := identityProvider{Provider: cloud.NoProvider, project: "meta-proj", location: "us-central1", region: "us-central1"}
	cases := []struct {
		name                              string
		provider                          cloud.Provider
		flagProject, flagRegion, flagZone string
		wantProject, wantRegion, wantZone string
	}{
		{"flags win over metadata", zonal, "flag-proj", "flag-region", "flag-zone", "flag-proj", "flag-region", "flag-zone"},
		{"metadata fills blank flags", zonal, "", "", "", "meta-proj", "us-central1", "us-central1-b"},
		{"regional metadata leaves the zone empty", regional, "", "", "", "meta-proj", "us-central1", ""},
		{"mixed: flag location, metadata project", zonal, "", "flag-region", "flag-zone", "meta-proj", "flag-region", "flag-zone"},
		{"mixed: flag project, metadata location", zonal, "flag-proj", "", "", "flag-proj", "us-central1", "us-central1-b"},
		// Region and zone resolve as a pair: a flagged region means the
		// operator is describing this cluster's location, so metadata
		// must not supply a zone from somewhere else — and a zonal flag
		// alone is a zone whose region we were not told.
		{"a flagged region suppresses the metadata zone", zonal, "", "flag-region", "", "meta-proj", "flag-region", ""},
		{"a flagged zone suppresses the metadata region", zonal, "", "", "flag-zone", "meta-proj", "", "flag-zone"},
		{"no identity surface: flags only", cloud.NoProvider, "flag-proj", "", "", "flag-proj", "", ""},
		{"nothing anywhere: empty (domain-less fingerprints)", cloud.NoProvider, "", "", "", "", "", ""},
		{"undetectable metadata stays empty", identityProvider{Provider: cloud.NoProvider}, "", "", "", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			project, region, zone := identityFromProvider(tc.provider, tc.flagProject, tc.flagRegion, tc.flagZone)
			if project != tc.wantProject || region != tc.wantRegion || zone != tc.wantZone {
				t.Errorf("identityFromProvider = (%q, %q, %q), want (%q, %q, %q)",
					project, region, zone, tc.wantProject, tc.wantRegion, tc.wantZone)
			}
		})
	}
}

// TestFailureDomainUnchangedByTheRegionZoneSplit is the reason the
// 2026-09-10 split (issue #389) moved no fingerprint: whatever the old
// single zone field held — the provider's location, a zone for zonal
// clusters and a region for regional ones — is exactly what
// engine.FailureDomain reconstructs from the pair.
func TestFailureDomainUnchangedByTheRegionZoneSplit(t *testing.T) {
	t.Parallel()
	for _, location := range []string{"us-central1-b", "us-central1", ""} {
		p := identityProvider{Provider: cloud.NoProvider, location: location, region: "us-central1"}
		if location == "" {
			p.region = ""
		}
		_, region, zone := identityFromProvider(p, "", "", "")
		if got := engine.FailureDomain(region, zone); got != location {
			t.Errorf("location %q → region=%q zone=%q → failure domain %q, want the location unchanged",
				location, region, zone, got)
		}
	}
}

// TestIdentityFlagsAdditive pins the two new flags as ADDITIVE: they
// parse, and their defaults are empty — a deployment that sets
// nothing keeps the M0 behavior (and zone-less fingerprints)
// byte-identical. Companion to TestFlagSurfaceFrozen.
func TestIdentityFlagsAdditive(t *testing.T) {
	t.Parallel()
	f, err := parseFlags([]string{"--project=p1", "--region=us-east1", "--zone=us-east1-c"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.project != "p1" || f.region != "us-east1" || f.zone != "us-east1-c" {
		t.Errorf("parsed (project, region, zone) = (%q, %q, %q), want (p1, us-east1, us-east1-c)", f.project, f.region, f.zone)
	}
	f, err = parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.project != "" || f.region != "" || f.zone != "" {
		t.Errorf("default (project, region, zone) = (%q, %q, %q), want empty (additive contract)", f.project, f.region, f.zone)
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

func identityDispatcher(t *testing.T, project, region, zone string) (*dispatcher, *[]string) {
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
		region:    region,
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
	disp, injects := identityDispatcher(t, "prod-project", "us-central1", "us-central1-b")
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
	if payload.Project != "prod-project" || payload.Region != "us-central1" || payload.Zone != "us-central1-b" {
		t.Errorf("payload (project, region, zone) = (%q, %q, %q), want (prod-project, us-central1, us-central1-b)",
			payload.Project, payload.Region, payload.Zone)
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
	disp, injects := identityDispatcher(t, "", "", "")
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
	for _, key := range []string{`"zone"`, `"region"`, `"project"`} {
		if strings.Contains(envelope.Message, key) {
			t.Errorf("empty identity leaked %s onto the wire (omitempty contract): %s", key, envelope.Message)
		}
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

// TestFrozenPairCarriesIdentity is the 2026-09-10 amendment (issue
// #389): the M0-frozen k8s-event payload now carries project/region/
// zone. A cluster NAME is unique only within a (project, location)
// pair, so in a fleet process two clusters called `prod` were
// indistinguishable on the wire and nothing downstream could build a
// context to reach either.
//
// It carries the identity block ONLY. Source, severity and fingerprint
// stay off the frozen pair: those are our pipeline's verdicts about a
// signal, not facts about where it happened, and the M0 contract keeps
// them in-process.
func TestFrozenPairCarriesIdentity(t *testing.T) {
	t.Parallel()
	disp, injects := identityDispatcher(t, "prod-project", "us-central1", "us-central1-b")
	ts := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	sig := identitySignal(ts)
	sig.Kind = engine.KindK8sEvent
	sig.Key.Reason = "CrashLoopBackOff"
	disp.DispatchSignal(context.Background(), sig)
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
	if payload.Project != "prod-project" || payload.Region != "us-central1" || payload.Zone != "us-central1-b" {
		t.Errorf("frozen-kind payload (project, region, zone) = (%q, %q, %q), want the stamped identity",
			payload.Project, payload.Region, payload.Zone)
	}
	if payload.Source != "" || payload.Severity != "" || payload.Fingerprint != "" {
		t.Errorf("frozen-kind payload leaked a pipeline verdict: source=%q severity=%q fingerprint=%q",
			payload.Source, payload.Severity, payload.Fingerprint)
	}
}

// TestFrozenPairIdentityIsOmitEmpty is the other half of the
// amendment's contract, and the reason it needed no migration: the
// single-cluster deployment that stamps no identity emits the M0
// bytes, key for key.
func TestFrozenPairIdentityIsOmitEmpty(t *testing.T) {
	t.Parallel()
	disp, injects := identityDispatcher(t, "", "", "")
	ts := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	sig := identitySignal(ts)
	sig.Kind = engine.KindK8sEvent
	sig.Key.Reason = "CrashLoopBackOff"
	disp.DispatchSignal(context.Background(), sig)
	if len(*injects) != 1 {
		t.Fatalf("expected 1 inject; got %d", len(*injects))
	}
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte((*injects)[0]), &envelope); err != nil {
		t.Fatalf("captured body isn't the inject envelope: %v", err)
	}
	for _, key := range []string{`"project"`, `"region"`, `"zone"`, `"pull_cause"`} {
		if strings.Contains(envelope.Message, key) {
			t.Errorf("unstamped %s reached the frozen k8s-event wire: %s", key, envelope.Message)
		}
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
