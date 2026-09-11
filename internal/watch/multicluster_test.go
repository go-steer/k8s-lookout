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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
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
	f := &flags{clusterName: "prod", sink: sinkCoreAgent, dedupPersist: "/data/dedup.json"}
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
	// The per-cluster suffixing (#386) is a multi-cluster affordance and
	// must not move an existing single-cluster deployment's snapshot: on
	// upgrade that file would silently read as empty and the sentinel
	// would re-alert on every open incident.
	if got := runners[0].f.dedupPersist; got != "/data/dedup.json" {
		t.Errorf("single-cluster --dedup-persist = %q, want the literal flag value", got)
	}
}

// Multi-cluster mode used to refuse both --dedup-persist and --store
// outright, because one path across N runners would have been silently
// shared. Now that resolveRunners derives a path per cluster from each
// flag as a stem (#386 for the snapshot, #410 for the store), both are
// accepted.
func TestMultiClusterAcceptsPerClusterStatePaths(t *testing.T) {
	validateWith := func(extra ...string) error {
		f, err := parseFlags(append([]string{"--dry-run", "--clusters=a=x.gke.goog,b=y.gke.goog"}, extra...))
		if err != nil {
			t.Fatalf("parseFlags(%v): %v", extra, err)
		}
		return f.validate()
	}
	if err := validateWith("--dedup-persist=/data/dedup.json"); err != nil {
		t.Errorf("multi-cluster --dedup-persist rejected: %v", err)
	}
	if err := validateWith("--store=/data/lookout.db"); err != nil {
		t.Errorf("multi-cluster --store rejected: %v", err)
	}
	if err := validateWith("--dedup-persist=/data/dedup.json", "--store=/data/lookout.db"); err != nil {
		t.Errorf("multi-cluster --dedup-persist + --store rejected: %v", err)
	}
}

// TestPerClusterDedupSnapshotsDoNotClobber is the whole of #386 end to
// end at the cache: N runners' worth of derived paths, each snapshotted
// and each reloaded, and no cluster ever sees another's entries. Before
// the fix all N shared one path, so the last renamer of a tick won and
// every runner reloaded whatever that cluster had.
func TestPerClusterDedupSnapshotsDoNotClobber(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "dedup.json")
	// Distinct names, because the name alone is what the path is keyed
	// on (#410) and resolveRunners refuses a fleet that returns two
	// clusters sharing one.
	refs := []cloud.ClusterRef{
		{Name: "prod-us", Project: "p", Location: "us-central1"},
		{Name: "prod-eu", Project: "p", Location: "europe-west1"},
		{Name: "staging", Project: "q", Location: "us-central1"},
	}

	key := func(i int) engine.EventKey {
		return engine.EventKey{UID: fmt.Sprintf("uid-%d", i), Reason: "BackOff"}
	}
	paths := make([]string, len(refs))
	for i, ref := range refs {
		paths[i] = emit.PerClusterPath(base, ref.Name)
		cache, err := engine.NewDedupCache(time.Hour, paths[i])
		if err != nil {
			t.Fatalf("cluster %d: NewDedupCache: %v", i, err)
		}
		// One bound incident per cluster, keyed so a leak is
		// identifiable by which cluster's session comes back.
		if got := cache.Observe(key(i), time.Now()); got.Kind != engine.DedupNewIncident {
			t.Fatalf("cluster %d: first Observe = %v, want a new incident", i, got.Kind)
		}
		cache.BindSession(key(i), fmt.Sprintf("sess-%d", i))
		if err := cache.Snapshot(); err != nil {
			t.Fatalf("cluster %d: Snapshot: %v", i, err)
		}
	}

	// N distinct files, all of them actually written.
	if len(slices.Compact(slices.Clone(paths))) != len(refs) {
		t.Fatalf("derived paths are not distinct: %q", paths)
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("snapshot %s: %v", p, err)
		}
	}

	// Restart: each runner reloads its own file and only its own. A
	// cluster that inherited a sibling's entries would suppress that
	// sibling's incident as a duplicate it never actually emitted.
	for i, p := range paths {
		cache, err := engine.NewDedupCache(time.Hour, p)
		if err != nil {
			t.Fatalf("cluster %d: reload: %v", i, err)
		}
		if got := cache.Len(); got != 1 {
			t.Errorf("cluster %d reloaded %d entries, want 1 (its own)", i, got)
		}
		sid, ok := cache.LookupSession(key(i))
		if !ok || sid != fmt.Sprintf("sess-%d", i) {
			t.Errorf("cluster %d reloaded session %q (ok=%t), want its own — the binding did not survive the restart", i, sid, ok)
		}
		for j := range refs {
			if j == i {
				continue
			}
			if sid, ok := cache.LookupSession(key(j)); ok {
				t.Errorf("cluster %d reloaded cluster %d's binding (%q) — the snapshots are still shared", i, j, sid)
			}
		}
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
