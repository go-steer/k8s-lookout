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

	"github.com/prometheus/client_golang/prometheus"
)

func TestParseClusters(t *testing.T) {
	refs, err := parseClusters("prod-us=abc.us-central1.gke.goog, prod-eu=def.europe-west1.gke.goog")
	if err != nil {
		t.Fatalf("parseClusters: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("got %d refs, want 2", len(refs))
	}
	if refs[0].Name != "prod-us" || refs[0].Endpoint != "abc.us-central1.gke.goog" {
		t.Errorf("ref[0] = %+v", refs[0])
	}
	if refs[1].Name != "prod-eu" || refs[1].Endpoint != "def.europe-west1.gke.goog" {
		t.Errorf("ref[1] = %+v", refs[1])
	}
}

func TestParseClustersBareEndpointDerivesName(t *testing.T) {
	refs, err := parseClusters("uid123.us-central1.gke.goog")
	if err != nil {
		t.Fatalf("parseClusters: %v", err)
	}
	if len(refs) != 1 || refs[0].Name != "uid123" {
		t.Fatalf("bare endpoint should derive first-label name, got %+v", refs)
	}
}

func TestParseClustersErrors(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{"empty", "", "empty"},
		{"no endpoint", "prod-us=", "no endpoint"},
		{"no name", "=abc.gke.goog", "no cluster name"},
		{"duplicate", "a=x.gke.goog,a=y.gke.goog", "duplicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseClusters(tc.in)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parseClusters(%q) err = %v, want contains %q", tc.in, err, tc.want)
			}
		})
	}
}

func TestParseClustersFrom(t *testing.T) {
	if p, l := parseClustersFrom("my-proj"); p != "my-proj" || l != "" {
		t.Errorf("project only: got (%q,%q)", p, l)
	}
	if p, l := parseClustersFrom("my-proj/us-central1"); p != "my-proj" || l != "us-central1" {
		t.Errorf("project/location: got (%q,%q)", p, l)
	}
}

func TestDropProjectTierSources(t *testing.T) {
	// quota + notifications drop; the cluster-tier sources stay, in order.
	got := dropProjectTierSources("k8s-events,quota,capacity,notifications,expiry")
	if got != "k8s-events,capacity,expiry" {
		t.Errorf("dropProjectTierSources = %q, want k8s-events,capacity,expiry", got)
	}
	// auto is untouched (it never enables project-tier sources).
	if got := dropProjectTierSources(autoValue); got != autoValue {
		t.Errorf("auto should pass through, got %q", got)
	}
}

// The single-cluster default (no --clusters/--clusters-from) resolves to
// exactly one runner for --cluster-name, with no fleet config.
func TestResolveRunnersDefaultSingle(t *testing.T) {
	f := &flags{clusterName: "prod", sink: sinkCoreAgent}
	reg := prometheus.NewRegistry()
	runners, err := resolveRunners(context.Background(), f, nil, "", reg)
	if err != nil {
		t.Fatalf("resolveRunners: %v", err)
	}
	if len(runners) != 1 {
		t.Fatalf("got %d runners, want 1", len(runners))
	}
	if runners[0].clusterName != "prod" || runners[0].restCfg != nil {
		t.Errorf("default runner = %+v, want name=prod restCfg=nil", runners[0])
	}
}

// On a build with no Fleet-capable provider (the default/untagged build
// resolves the NoProvider sentinel), multi-cluster mode fails loudly and
// names the fix rather than silently degrading.
func TestResolveRunnersMultiClusterNeedsFleet(t *testing.T) {
	f := &flags{clusters: "a=x.gke.goog", sink: sinkCoreAgent}
	reg := prometheus.NewRegistry()
	_, err := resolveRunners(context.Background(), f, nil, "", reg)
	if err == nil || !strings.Contains(err.Error(), "-tags gke") {
		t.Fatalf("resolveRunners err = %v, want a loud 'needs a Fleet-capable provider (-tags gke)' error", err)
	}
}
