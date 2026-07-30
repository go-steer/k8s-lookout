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

//go:build gke || allproviders

package gke

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// Tagged counterpart of pkg/cloud's default-build conformance test:
// under -tags gke/allproviders the provider must self-register.
func TestGKERegisteredUnderTag(t *testing.T) {
	if !slices.Contains(cloud.Registered(), Name) {
		t.Errorf("Registered() = %v, want it to contain %q", cloud.Registered(), Name)
	}
}

func TestNewSelectsGKEAsSingleRegisteredProvider(t *testing.T) {
	t.Setenv(cloud.ProviderEnv, "")
	t.Setenv(metadataHostEnv, "localhost:1") // keep detection off the network
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("CLOUDSDK_CORE_PROJECT", "")
	p, err := cloud.New(context.Background(), cloud.Config{})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if p.Name() != Name {
		t.Errorf("New selected %q, want %q (single registered provider)", p.Name(), Name)
	}
}

func TestIdentityFromConfigPinsWins(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "env-project")
	p, err := New(context.Background(), cloud.Config{
		Project: "pinned-project", Location: "us-east1", Cluster: "prod",
	})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	g := p.(*Provider)
	if g.Project() != "pinned-project" || g.Location() != "us-east1" || g.Cluster() != "prod" {
		t.Errorf("identity = %q/%q/%q, want config pins to win", g.Project(), g.Location(), g.Cluster())
	}
}

func TestIdentityFromEnvAndMetadata(t *testing.T) {
	md := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing Metadata-Flavor", http.StatusForbidden)
			return
		}
		w.Header().Set("Metadata-Flavor", "Google")
		switch strings.TrimPrefix(r.URL.Path, "/computeMetadata/v1/") {
		case "instance/attributes/cluster-name":
			_, _ = w.Write([]byte("md-cluster"))
		case "instance/attributes/cluster-location":
			_, _ = w.Write([]byte("europe-west1-b\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer md.Close()

	t.Setenv("GOOGLE_CLOUD_PROJECT", "env-project")
	t.Setenv(metadataHostEnv, strings.TrimPrefix(md.URL, "http://"))

	p, err := New(context.Background(), cloud.Config{})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	g := p.(*Provider)
	if g.Project() != "env-project" {
		t.Errorf("Project() = %q, want env-project (from GOOGLE_CLOUD_PROJECT)", g.Project())
	}
	if g.Cluster() != "md-cluster" {
		t.Errorf("Cluster() = %q, want md-cluster (from metadata)", g.Cluster())
	}
	if g.Location() != "europe-west1-b" {
		t.Errorf("Location() = %q, want europe-west1-b (trimmed, from metadata)", g.Location())
	}
}

func TestOffGCEDetectionIsBestEffort(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("CLOUDSDK_CORE_PROJECT", "")
	t.Setenv(metadataHostEnv, "localhost:1")
	p, err := New(context.Background(), cloud.Config{})
	if err != nil {
		t.Fatalf("New off-GCE must not error, got: %v", err)
	}
	g := p.(*Provider)
	if g.Project() != "" || g.Location() != "" || g.Cluster() != "" {
		t.Errorf("identity = %q/%q/%q, want empty off-GCE", g.Project(), g.Location(), g.Cluster())
	}
}

// TestCapabilityAvailability pins the per-capability availability
// judgment as of M5: every implemented capability (metrics joined
// the project-scoped set with the M5 backend) is available exactly
// when the identity it needs is resolved, with the §2 explicit
// reason otherwise; only workload identity stays deferred (its M5
// track lands separately). Getters must mirror Capabilities()
// exactly.
func TestCapabilityAvailability(t *testing.T) {
	t.Setenv(metadataHostEnv, "localhost:1")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("CLOUDSDK_CORE_PROJECT", "")

	newProvider := func(cfg cloud.Config) cloud.Provider {
		t.Helper()
		p, err := New(context.Background(), cfg)
		if err != nil {
			t.Fatalf("New error: %v", err)
		}
		return p
	}
	statusOf := func(p cloud.Provider, c cloud.Capability) cloud.CapabilityStatus {
		t.Helper()
		for _, s := range p.Capabilities() {
			if s.Capability == c {
				return s
			}
		}
		t.Fatalf("capability %s missing from Capabilities()", c)
		return cloud.CapabilityStatus{}
	}
	getterOK := func(p cloud.Provider, c cloud.Capability) bool {
		switch c {
		case cloud.CapabilityMetrics:
			_, ok := p.Metrics()
			return ok
		case cloud.CapabilityCapacity:
			_, ok := p.Capacity()
			return ok
		case cloud.CapabilityQuota:
			_, ok := p.Quota()
			return ok
		case cloud.CapabilityOrphans:
			_, ok := p.Orphans()
			return ok
		case cloud.CapabilityIPSpace:
			_, ok := p.IPSpace()
			return ok
		case cloud.CapabilityStockout:
			_, ok := p.Stockouts()
			return ok
		case cloud.CapabilityAudit:
			_, ok := p.Audit()
			return ok
		default:
			_, ok := p.WorkloadIdentity()
			return ok
		}
	}

	full := newProvider(cloud.Config{Project: "p", Location: "us-east1-b", Cluster: "prod"})
	projectOnly := newProvider(cloud.Config{Project: "p"})
	empty := newProvider(cloud.Config{})

	cases := []struct {
		capability  cloud.Capability
		fullOK      bool
		projOK      bool
		projReason  string
		emptyReason string
	}{
		{cloud.CapabilityMetrics, true, true, "", reasonNoProject},
		{cloud.CapabilityCapacity, true, true, "", reasonNoProject},
		{cloud.CapabilityQuota, true, true, "", reasonNoProject},
		{cloud.CapabilityOrphans, true, true, "", reasonNoProject},
		{cloud.CapabilityIPSpace, true, false, reasonNoClusterIdentity, reasonNoClusterIdentity},
		{cloud.CapabilityStockout, true, true, "", reasonNoProject},
		{cloud.CapabilityWorkloadIdentity, true, true, "", reasonNoProject},
		{cloud.CapabilityAudit, true, false, reasonNoClusterIdentity, reasonNoClusterIdentity},
	}
	if len(cases) != len(cloud.AllCapabilities()) {
		t.Fatalf("test table covers %d capabilities, boundary defines %d", len(cases), len(cloud.AllCapabilities()))
	}
	for _, tc := range cases {
		if s := statusOf(full, tc.capability); s.Available != tc.fullOK {
			t.Errorf("full identity: %s available=%v, want %v (reason %q)", tc.capability, s.Available, tc.fullOK, s.Reason)
		}
		if got := getterOK(full, tc.capability); got != tc.fullOK {
			t.Errorf("full identity: %s getter ok=%v diverges from Capabilities()", tc.capability, got)
		}
		s := statusOf(projectOnly, tc.capability)
		if s.Available != tc.projOK || (!tc.projOK && s.Reason != tc.projReason) {
			t.Errorf("project only: %s = %+v, want available=%v reason %q", tc.capability, s, tc.projOK, tc.projReason)
		}
		if got := getterOK(projectOnly, tc.capability); got != tc.projOK {
			t.Errorf("project only: %s getter ok=%v diverges from Capabilities()", tc.capability, got)
		}
		s = statusOf(empty, tc.capability)
		if s.Available || s.Reason != tc.emptyReason {
			t.Errorf("no identity: %s = %+v, want unavailable reason %q", tc.capability, s, tc.emptyReason)
		}
	}

	// The §2 marker for a missing-identity capability names the fix.
	u := cloud.Unavailable(empty, cloud.CapabilityQuota)
	if u.Reason != reasonNoProject {
		t.Errorf("Unavailable reason = %q, want %q", u.Reason, reasonNoProject)
	}
}

// Without a resolvable project the capacity capability degrades with
// the explicit §2 reason instead of a client that cannot query.
func TestCapacityUnavailableWithoutProject(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("CLOUDSDK_CORE_PROJECT", "")
	t.Setenv(metadataHostEnv, "localhost:1")
	p, err := New(context.Background(), cloud.Config{})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if _, ok := p.Capacity(); ok {
		t.Fatal("Capacity() available without a project")
	}
	u := cloud.Unavailable(p, cloud.CapabilityCapacity)
	if u.Reason != reasonNoProject {
		t.Errorf("reason = %q, want %q", u.Reason, reasonNoProject)
	}
}
