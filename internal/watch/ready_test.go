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
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TestReadiness_LifecycleOfOneRunner walks the states a single-cluster
// sentinel passes through: process start (nothing resolved), runners
// resolved but not started, sources syncing, watching, and the restart
// gap after a runner exits.
func TestReadiness_LifecycleOfOneRunner(t *testing.T) {
	t.Parallel()
	rd := newReadiness()

	if ok, why := rd.ready(); ok || !strings.Contains(why, "starting") {
		t.Fatalf("before expect: ready=%v why=%q, want not ready and a starting reason", ok, why)
	}

	rd.expect([]string{"prod-east"})
	ok, why := rd.ready()
	if ok || !strings.Contains(why, "not started") {
		t.Fatalf("expected-but-unstarted: ready=%v why=%q, want not ready naming the unstarted runner", ok, why)
	}
	if !strings.Contains(why, "prod-east") {
		t.Errorf("reason %q does not name the cluster", why)
	}

	var synced atomic.Bool
	rd.set("prod-east", synced.Load)
	if ok, why := rd.ready(); ok || !strings.Contains(why, "syncing") {
		t.Fatalf("started-but-unsynced: ready=%v why=%q, want not ready with a syncing reason", ok, why)
	}

	synced.Store(true)
	if ok, why := rd.ready(); !ok || why != "" {
		t.Fatalf("synced: ready=%v why=%q, want ready with no reason", ok, why)
	}

	// A runner that exits withdraws its probe: between supervisor
	// restarts nobody is watching this cluster.
	rd.clear("prod-east")
	if ok, why := rd.ready(); ok || !strings.Contains(why, "not started") {
		t.Fatalf("after clear: ready=%v why=%q, want not ready again", ok, why)
	}
}

// TestReadiness_UnnamedSingleClusterReadsAsAPhase (#321): the default
// deployment — one sentinel, one cluster, no --cluster-name — has no
// name for its cluster, and the per-cluster list rendered the hole:
// `waiting on 1 of 1 cluster(s): [ (not started)]`. The phase is the
// whole answer there, so say only that.
func TestReadiness_UnnamedSingleClusterReadsAsAPhase(t *testing.T) {
	t.Parallel()
	rd := newReadiness()
	rd.expect([]string{""})

	ok, why := rd.ready()
	if ok || why != "cluster runner not started" {
		t.Fatalf("unstarted: ready=%v why=%q, want the bare phase", ok, why)
	}

	var synced atomic.Bool
	rd.set("", synced.Load)
	if ok, why := rd.ready(); ok || why != "informer caches syncing" {
		t.Fatalf("unsynced: ready=%v why=%q, want the bare phase", ok, why)
	}

	synced.Store(true)
	if ok, why := rd.ready(); !ok || why != "" {
		t.Fatalf("synced: ready=%v why=%q, want ready with no reason", ok, why)
	}
}

// TestReadiness_NamedSingleClusterStillListsIt: --cluster-name is the
// operator saying the name matters — a one-cluster fleet is still a
// fleet — so the list form stays. Only the *unnamed* case collapses.
func TestReadiness_NamedSingleClusterStillListsIt(t *testing.T) {
	t.Parallel()
	rd := newReadiness()
	rd.expect([]string{"prod-east"})
	if ok, why := rd.ready(); ok || !strings.Contains(why, "1 of 1") || !strings.Contains(why, "prod-east (not started)") {
		t.Errorf("ready=%v why=%q, want the named list form", ok, why)
	}
}

// TestReadiness_PartialFleetIsNotReady is the multi-cluster case the
// naive "every registered probe is happy" implementation gets wrong: a
// process responsible for three clusters that has started one is not
// two-thirds ready, it is not ready.
func TestReadiness_PartialFleetIsNotReady(t *testing.T) {
	t.Parallel()
	rd := newReadiness()
	rd.expect([]string{"a", "b", "c"})
	rd.set("a", func() bool { return true })

	ok, why := rd.ready()
	if ok {
		t.Fatal("one of three clusters watching must not read as ready")
	}
	if !strings.Contains(why, "2 of 3") || !strings.Contains(why, "b") || !strings.Contains(why, "c") {
		t.Errorf("reason %q should count the stragglers and name them", why)
	}

	rd.set("b", func() bool { return true })
	rd.set("c", func() bool { return false })
	if ok, why := rd.ready(); ok || !strings.Contains(why, "c (syncing)") {
		t.Errorf("ready=%v why=%q, want the one unsynced cluster named", ok, why)
	}

	rd.set("c", func() bool { return true })
	if ok, _ := rd.ready(); !ok {
		t.Error("all three watching should be ready")
	}
}

// TestReadiness_ProbesRunOutsideTheLock guards the deadlock this would
// otherwise invite: a source's HasSynced takes the source's own lock,
// so holding the tracker's while calling it lets one slow source block
// every other cluster's readiness answer.
func TestReadiness_ProbesRunOutsideTheLock(t *testing.T) {
	t.Parallel()
	rd := newReadiness()
	rd.expect([]string{"slow"})

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	rd.set("slow", func() bool {
		once.Do(func() { close(entered) })
		<-release
		return true
	})

	done := make(chan bool, 1)
	go func() { ok, _ := rd.ready(); done <- ok }()
	<-entered

	// The tracker must still be usable while a probe is in flight.
	rd.set("other", func() bool { return true })
	close(release)

	select {
	case ok := <-done:
		if !ok {
			t.Error("probe returned true; ready() should agree")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ready() never returned — the tracker lock is held across the probe")
	}
}

// TestServeMetrics_ReadyzTracksTheRunners is the wire-level contract
// the kubelet sees: 503 with a reason while syncing, 200 once watching,
// and /healthz flatly 200 throughout — the two probes must not be the
// same answer, which is the whole of issue #285.
func TestServeMetrics_ReadyzTracksTheRunners(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	rd := newReadiness()
	rd.expect([]string{"prod"})
	var synced atomic.Bool
	rd.set("prod", synced.Load)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- serveMetrics(ctx, addr, prometheus.NewRegistry(), rd) }()

	get := func(path string) (int, string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			resp, gerr := http.Get("http://" + addr + path) //nolint:noctx // short-lived test probe
			if gerr != nil {
				if time.Now().After(deadline) {
					t.Fatalf("GET %s: %v", path, gerr)
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return resp.StatusCode, string(body)
		}
	}

	code, body := get("/readyz")
	if code != http.StatusServiceUnavailable {
		t.Errorf("unsynced /readyz = %d, want 503", code)
	}
	if !strings.Contains(body, "syncing") {
		t.Errorf("unsynced /readyz body = %q, want it to say what it is waiting on", body)
	}
	if code, _ := get("/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d while syncing, want 200 — liveness is a different question", code)
	}

	synced.Store(true)
	if code, body := get("/readyz"); code != http.StatusOK || !strings.Contains(body, "ok") {
		t.Errorf("synced /readyz = %d %q, want 200 ok", code, body)
	}

	cancel()
	if err := <-served; err != nil {
		t.Errorf("serveMetrics returned %v, want nil on ctx cancel", err)
	}
}
