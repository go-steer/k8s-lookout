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
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// TestPerClusterStorePaths is #410 at the wiring: --store is a stem in
// multi-cluster mode and every runner gets its own file. Sharing one
// SQLite path across runners is not merely untidy — two of the store's
// tables carry no cluster column (triage_status is keyed (fingerprint,
// resource_key); graph history by epoch and time), so one file lets one
// cluster's triage record and topology answer for another's.
func TestPerClusterStorePaths(t *testing.T) {
	f := &flags{
		clustersFrom: "kubeconfig:" + fleetKubeconfig(t, false),
		sink:         sinkCoreAgent,
		store:        "/var/lib/lookout/lookout.db",
		dedupPersist: "/var/lib/lookout/dedup.json",
	}
	runners, err := resolveRunners(context.Background(), f, nil, "", prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("resolveRunners: %v", err)
	}
	want := map[string]string{
		"prod-eu": "/var/lib/lookout/lookout-prod-eu.db",
		"prod-us": "/var/lib/lookout/lookout-prod-us.db",
	}
	seen := map[string]string{}
	for _, r := range runners {
		if got := r.f.store; got != want[r.clusterName] {
			t.Errorf("runner %q store = %q, want %q", r.clusterName, got, want[r.clusterName])
		}
		if prev, dup := seen[r.f.store]; dup {
			t.Errorf("runners %q and %q share the store %q", prev, r.clusterName, r.f.store)
		}
		seen[r.f.store] = r.clusterName
		// The snapshot (#386) keeps its own per-cluster path alongside.
		if got := r.f.dedupPersist; !strings.Contains(got, r.clusterName) {
			t.Errorf("runner %q dedup snapshot = %q, want the cluster in the name", r.clusterName, got)
		}
	}
	// The stem itself must never be opened: it is a name for a family of
	// files, and a runner writing to it would be a fleet-wide store again.
	if seen[f.store] != "" {
		t.Errorf("a runner opened the stem %q itself", f.store)
	}
}

// The single-cluster default is untouched: the store stays exactly where
// the operator put it. Deriving there would move an existing deployment's
// database on upgrade — losing its triage records and finding state, and
// stranding every `--store` a playbook already hard-codes.
func TestSingleClusterStorePathIsLiteral(t *testing.T) {
	f := &flags{clusterName: "prod", sink: sinkCoreAgent, store: "/var/lib/lookout/lookout.db"}
	runners, err := resolveRunners(context.Background(), f, nil, "", prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("resolveRunners: %v", err)
	}
	if got := runners[0].f.store; got != "/var/lib/lookout/lookout.db" {
		t.Errorf("single-cluster --store = %q, want the literal flag value", got)
	}
}

// TestDuplicateClusterNamesAreSkipped: a cluster's NAME is the only
// handle this sentinel has on it — the metrics label, the wire field, the
// /readyz entry, and now the store and dedup files. Discovery across a
// project can legitimately return the same name twice (two locations),
// and watching both would silently merge them everywhere at once. Both
// are skipped; the rest of the fleet runs.
//
// The helper is driven directly because no real fleet source can be
// asked for the pair: --clusters refuses a repeated name at parse time,
// and a kubeconfig's contexts are a map.
func TestDuplicateClusterNamesAreSkipped(t *testing.T) {
	reg := prometheus.NewRegistry()
	refs := []cloud.ClusterRef{
		{Name: "prod", Project: "p", Location: "us-central1", Endpoint: "a.example.test"},
		{Name: "prod", Project: "p", Location: "europe-west1", Endpoint: "b.example.test"},
		{Name: "staging", Project: "p", Location: "us-central1", Endpoint: "c.example.test"},
	}
	skipped := skipDuplicateNames(refs, newFleetMetrics(reg))
	if len(skipped) != 1 || skipped[0] != "prod" {
		t.Fatalf("skipped = %q, want only prod — an ambiguous name is watched by neither, an unambiguous one still runs", skipped)
	}
	// Counted once per cluster dropped, not once per name: the coverage
	// gap is two clusters wide.
	const want = `
# HELP lookout_cluster_resolve_errors_total Total clusters this process was told to watch and did not, by cluster and cause (issues #388, #410). credentials: the cluster could not be resolved into a client. duplicate_name: two clusters in the fleet share this name, which is the only handle the sentinel has on a cluster, so neither is watched. The cluster is SKIPPED, not fatal, so the rest of the fleet still runs — which means a non-zero value is a coverage gap: nothing is watching that cluster and its silence means nothing. Counted at startup, so it moves on process restart and on nothing else.
# TYPE lookout_cluster_resolve_errors_total counter
lookout_cluster_resolve_errors_total{cause="duplicate_name",cluster="prod"} 2
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "lookout_cluster_resolve_errors_total"); err != nil {
		t.Error(err)
	}
}

// A fleet that is ALL duplicates leaves resolveRunners nothing to build,
// which its existing empty-fleet refusal turns into a fatal error (see
// TestResolveRunnersEveryClusterUnresolvableIsFatal): supervising an
// empty fleet would report ready while watching no cluster at all.
func TestEveryClusterNameDuplicateIsSkipped(t *testing.T) {
	refs := []cloud.ClusterRef{
		{Name: "prod", Location: "us-central1"},
		{Name: "prod", Location: "europe-west1"},
	}
	skipped := skipDuplicateNames(refs, newFleetMetrics(prometheus.NewRegistry()))
	if len(skipped) != 1 || skipped[0] != "prod" {
		t.Fatalf("skipped = %q, want prod — with nothing left, resolveRunners refuses the fleet", skipped)
	}
}

// The two skip causes are distinguishable on /metrics, because they need
// different fixes: one is credentials, the other is a naming collision
// only the operator can break. Each cause is produced by the path that
// really raises it, so the labels cannot drift apart in the source.
func TestSkipCausesAreLabelledSeparately(t *testing.T) {
	dupReg := prometheus.NewRegistry()
	skipDuplicateNames([]cloud.ClusterRef{
		{Name: "prod", Location: "us-central1"},
		{Name: "prod", Location: "europe-west1"},
	}, newFleetMetrics(dupReg))

	credReg := prometheus.NewRegistry()
	f := &flags{clustersFrom: "kubeconfig:" + fleetKubeconfig(t, true), sink: sinkCoreAgent}
	if _, err := resolveRunners(context.Background(), f, nil, "", credReg); err != nil {
		t.Fatalf("resolveRunners: %v", err)
	}

	for _, tc := range []struct {
		reg            *prometheus.Registry
		cluster, cause string
		want           float64
	}{
		{dupReg, "prod", resolveSkipDuplicateName, 2},
		{dupReg, "prod", resolveSkipCredentials, 0},
		{credReg, "torn-down", resolveSkipCredentials, 1},
		{credReg, "torn-down", resolveSkipDuplicateName, 0},
		{credReg, "prod-us", resolveSkipCredentials, 0},
	} {
		if got := resolveErrorCount(t, tc.reg, tc.cluster, tc.cause); got != tc.want {
			t.Errorf("resolve errors{cluster=%q,cause=%q} = %v, want %v", tc.cluster, tc.cause, got, tc.want)
		}
	}
}

// resolveErrorCount reads one series out of the gathered registry. It
// gathers rather than re-building the bundle because newFleetMetrics
// registers, and registering the same collector twice panics.
func resolveErrorCount(t *testing.T, reg *prometheus.Registry, cluster, cause string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "lookout_cluster_resolve_errors_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["cluster"] == cluster && labels["cause"] == cause {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0 // never incremented: a CounterVec has no series until then
}
