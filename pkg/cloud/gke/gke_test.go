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

func TestCapabilities_CapacityLiveRestDeferred(t *testing.T) {
	t.Setenv(metadataHostEnv, "localhost:1")
	p, err := New(context.Background(), cloud.Config{Project: "p"})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	statuses := p.Capabilities()
	if len(statuses) != len(cloud.AllCapabilities()) {
		t.Fatalf("Capabilities() reports %d entries, want %d", len(statuses), len(cloud.AllCapabilities()))
	}
	for _, status := range statuses {
		if status.Capability == cloud.CapabilityCapacity {
			if !status.Available || status.Reason != "" {
				t.Errorf("capacity status = %+v, want available with project set (M4)", status)
			}
			continue
		}
		if status.Available {
			t.Errorf("capability %s reported available before its milestone", status.Capability)
		}
		if status.Reason != reasonDeferred {
			t.Errorf("capability %s reason = %q, want %q", status.Capability, status.Reason, reasonDeferred)
		}
	}

	if _, ok := p.Metrics(); ok {
		t.Error("Metrics() available before M5")
	}
	if _, ok := p.Quota(); ok {
		t.Error("Quota() available before its M4 change lands")
	}
	if api, ok := p.Capacity(); !ok || api == nil {
		t.Error("Capacity() unavailable with a project set (M4 landed it)")
	}
	u := cloud.Unavailable(p, cloud.CapabilityQuota)
	if want := `unavailable reason="not implemented until M4/M5"`; u.Marker() != want {
		t.Errorf("Marker() = %s, want %s", u.Marker(), want)
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
