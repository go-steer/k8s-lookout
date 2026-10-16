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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fleetKubeconfig writes a kubeconfig with two reachable contexts and,
// optionally, a third that points at a cluster the file never defines
// — a stale entry, which is the everyday shape of #388's "one cluster
// cannot be resolved".
func fleetKubeconfig(t *testing.T, withStale bool) string {
	t.Helper()
	cfg := `apiVersion: v1
kind: Config
current-context: prod-eu
clusters:
- name: eu
  cluster:
    server: https://eu.example.test:443
- name: us
  cluster:
    server: https://us.example.test:443
contexts:
- name: prod-eu
  context:
    cluster: eu
    user: alice
- name: prod-us
  context:
    cluster: us
    user: alice
`
	if withStale {
		cfg += `- name: torn-down
  context:
    cluster: gone
    user: alice
`
	}
	cfg += `users:
- name: alice
  user:
    token: t0ken
`
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

// TestResolveRunnersFromKubeconfig is #388 part 1 end to end: one
// runner per context, each with its own credentials, on a build with
// no cloud provider at all.
func TestResolveRunnersFromKubeconfig(t *testing.T) {
	f := &flags{clustersFrom: "kubeconfig:" + fleetKubeconfig(t, false), sink: sinkCoreAgent}
	runners, err := resolveRunners(context.Background(), f, nil, "", prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("resolveRunners: %v", err)
	}
	if len(runners) != 2 {
		t.Fatalf("got %d runners, want 2", len(runners))
	}
	want := map[string]string{
		"prod-eu": "https://eu.example.test:443",
		"prod-us": "https://us.example.test:443",
	}
	for _, r := range runners {
		if r.restCfg == nil {
			t.Fatalf("runner %q has no rest.Config", r.clusterName)
		}
		if got := r.restCfg.Host; got != want[r.clusterName] {
			t.Errorf("runner %q host = %q, want %q — the runners share one cluster's credentials", r.clusterName, got, want[r.clusterName])
		}
	}
}

// TestResolveRunnersSkipsUnresolvableCluster is #388 part 2: one
// cluster that cannot be resolved costs that cluster and nothing else.
// resolveRunners runs before any runner starts, so the old behavior —
// return the error — meant a single torn-down cluster in a discovery
// listing left the whole fleet unwatched.
func TestResolveRunnersSkipsUnresolvableCluster(t *testing.T) {
	reg := prometheus.NewRegistry()
	f := &flags{clustersFrom: "kubeconfig:" + fleetKubeconfig(t, true), sink: sinkCoreAgent}
	runners, err := resolveRunners(context.Background(), f, nil, "", reg)
	if err != nil {
		t.Fatalf("resolveRunners aborted on one bad cluster: %v", err)
	}
	if len(runners) != 2 {
		t.Fatalf("got %d runners, want the 2 resolvable clusters", len(runners))
	}
	for _, r := range runners {
		if r.clusterName == "torn-down" {
			t.Error("the unresolvable cluster got a runner")
		}
	}
	// A skipped cluster is loud on /metrics, because it is a coverage
	// gap: nothing watches it and its silence means nothing.
	const want = `
# HELP lookout_cluster_resolve_errors_total Total clusters this process was told to watch and did not, by cluster and cause (issues #388, #410). credentials: the cluster could not be resolved into a client. duplicate_name: two clusters in the fleet share this name, which is the only handle the sentinel has on a cluster, so neither is watched. The cluster is SKIPPED, not fatal, so the rest of the fleet still runs — which means a non-zero value is a coverage gap: nothing is watching that cluster and its silence means nothing. Counted at startup, so it moves on process restart and on nothing else.
# TYPE lookout_cluster_resolve_errors_total counter
lookout_cluster_resolve_errors_total{cause="credentials",cluster="torn-down"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "lookout_cluster_resolve_errors_total"); err != nil {
		t.Error(err)
	}
}

// TestResolveRunnersEveryClusterUnresolvableIsFatal: degrading is for
// one cluster's bad day. Nothing resolvable is credentials, a build
// tag or a kubeconfig naming nothing reachable — and supervising an
// empty fleet would report ready while watching no cluster at all.
func TestResolveRunnersEveryClusterUnresolvableIsFatal(t *testing.T) {
	const cfg = `apiVersion: v1
kind: Config
contexts:
- name: gone-a
  context:
    cluster: missing-a
    user: alice
- name: gone-b
  context:
    cluster: missing-b
    user: alice
users:
- name: alice
  user:
    token: t0ken
`
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	f := &flags{clustersFrom: "kubeconfig:" + path, sink: sinkCoreAgent}
	_, err := resolveRunners(context.Background(), f, nil, "", prometheus.NewRegistry())
	if err == nil {
		t.Fatal("a fleet with no resolvable cluster started anyway")
	}
	for _, want := range []string{"gone-a", "gone-b", "none of the 2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// An empty kubeconfig is a config error, not a zero-cluster fleet:
// same posture as a --clusters-from project that matched nothing.
func TestResolveRunnersEmptyKubeconfigIsFatal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte("apiVersion: v1\nkind: Config\n"), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	f := &flags{clustersFrom: "kubeconfig:" + path, sink: sinkCoreAgent}
	_, err := resolveRunners(context.Background(), f, nil, "", prometheus.NewRegistry())
	if err == nil || !strings.Contains(err.Error(), "matched no clusters") {
		t.Fatalf("err = %v, want the no-clusters refusal", err)
	}
}
