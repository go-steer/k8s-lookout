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

// M5 exit checks (DESIGN.md §14): the §9.3 corpus-harvester contract
// validated end-to-end against the REAL dispatcher, the push/pull
// fingerprint parity of the v1 schema freeze, and the multi-cluster
// stockout rollup simulated as the fleet-side fingerprint join
// (docs/signal-schema-v1.md; evidence recorded in
// docs/milestones/M5.md).

package watch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/corpus"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// captureDaemon is a fake core-agent daemon that logs every request
// in dev/drills/stub-daemon.py's EXACT line format, so its capture
// feeds pkg/corpus the way `kubectl logs` of the drill stub does.
type captureDaemon struct {
	mu      sync.Mutex
	lines   []string
	counter int
}

func (c *captureDaemon) appendLine(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, line)
}

func (c *captureDaemon) capture() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n") + "\n"
}

func newCaptureDaemon(t *testing.T) (*captureDaemon, string) {
	t.Helper()
	c := &captureDaemon{lines: []string{"stub-daemon listening on :7777"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 0, 4096)
		if r.Body != nil {
			buf := make([]byte, 4096)
			for {
				n, err := r.Body.Read(buf)
				body = append(body, buf[:n]...)
				if err != nil {
					break
				}
			}
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sessions":
			c.mu.Lock()
			c.counter++
			sid := fmt.Sprintf("stub-sess-%04d", c.counter)
			c.lines = append(c.lines, fmt.Sprintf("SESSION-CREATE sid=%s caller=%s token=present", sid, r.Header.Get("X-Asserted-Caller")))
			c.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"sessionID":%q}`, sid)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sessions/") && strings.HasSuffix(r.URL.Path, "/inject"):
			sid := strings.Split(r.URL.Path, "/")[2]
			var envelope struct {
				Message string `json:"message"`
			}
			kind := ""
			if json.Unmarshal(body, &envelope) == nil {
				var k struct {
					Kind string `json:"kind"`
				}
				if json.Unmarshal([]byte(envelope.Message), &k) == nil {
					kind = k.Kind
				}
			}
			c.appendLine(fmt.Sprintf("INJECT sid=%s kind=%s token=present body=%s", sid, kind, string(body)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return c, srv.URL
}

func newCaptureDispatcher(t *testing.T, base, cluster string) *dispatcher {
	t.Helper()
	inj, err := inject.NewInjector(inject.Config{DaemonURL: base, BearerToken: "tok", AssertedCaller: "lookout"})
	if err != nil {
		t.Fatalf("NewInjector: %v", err)
	}
	dedup, _ := engine.NewDedupCache(5*time.Minute, "")
	return &dispatcher{
		filter:   engine.NewFilter(engine.NewFilterConfig(nil, nil, nil, 0, 1, 0)),
		dedup:    dedup,
		injector: inj,
		metrics:  newMetrics(),
		cluster:  cluster,
		mode:     "per-incident",
	}
}

// TestFingerprintParity_PushAndScan is the schema-freeze parity
// check: the SAME failure class produces the SAME fingerprint via the
// sentinel push path (the dispatcher's §8 stamp, read back from the
// §9.1 store) and via the scan-source mapping read-path findings
// carry (engine.ScanFingerprint — `lookout health` / `triage delta`
// stamping). The literal below also appears in
// pkg/checks/health/testdata/health-broken.golden's pod.crashloop
// line: one key across the wire, the store, and the scan surfaces.
func TestFingerprintParity_PushAndScan(t *testing.T) {
	t.Parallel()
	const crashloopPodFP = "sha256:e2957792a0b3ad9e29db2051dbc69ff01dfe3a52da8dbb6d1331aa44fe946f8b"

	// Scan side: the recipe read-path findings are stamped with,
	// canonicalization included (ErrImagePull folds into the
	// ImagePullBackOff family exactly like the push path's dedup).
	if got := engine.ScanFingerprint("CrashLoopBackOff", "Pod", ""); got != crashloopPodFP {
		t.Errorf("ScanFingerprint(CrashLoopBackOff, Pod) = %s, want the pinned %s", got, crashloopPodFP)
	}
	if engine.ScanFingerprint("ErrImagePull", "Pod", "") != engine.ScanFingerprint("ImagePullBackOff", "Pod", "") {
		t.Error("scan fingerprints must fold reason families like the push path does")
	}

	// Push side: dispatch the incident through the real pipeline and
	// read the stamped fingerprint back from the occurrence store.
	_, base := newCaptureDaemon(t)
	d := newCaptureDispatcher(t, base, "prod-east")
	st, err := store.Open(t.TempDir() + "/lookout.db")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer func() { _ = st.Close() }()
	d.store = st

	sig := crashLoopSignal()
	sig.Namespace = "prod"
	sig.Name = "api-0"
	d.DispatchSignal(context.Background(), sig)
	st.Flush()

	occ, err := st.RecentByFingerprint(context.Background(), crashloopPodFP, time.Time{})
	if err != nil {
		t.Fatalf("RecentByFingerprint: %v", err)
	}
	if len(occ) != 1 {
		t.Fatalf("push-path occurrence not found under the scan fingerprint: got %d rows — push and pull have diverged", len(occ))
	}
}

// TestDrill_CorpusHarvest_EndToEnd is the §9.3 exit check: a scripted
// full incident lifecycle through the REAL dispatcher against a fake
// daemon capture (stub-daemon line format), with the §9.4 records an
// incident playbook would write via `lookout triage status` exported
// into the same capture — and the harvester extracts exactly ONE
// complete labeled trajectory by pure schema walks.
//
// This validates the CONTRACT with fixtures per the standing drill
// policy; the "one harvested labeled trajectory from a REAL incident"
// half of the M5 exit remains a human step (docs/milestones/M5.md).
func TestDrill_CorpusHarvest_EndToEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	daemon, base := newCaptureDaemon(t)
	d := newCaptureDispatcher(t, base, "prod-east")

	st, err := store.Open(t.TempDir() + "/lookout.db")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer func() { _ = st.Close() }()
	d.store = st
	d.triage = newTriageOverrides(st, d.metrics, 3)

	// Stage 1 — symptom: the critical inject opens a session, warm
	// (the §7.6 bundle attached the way the enrichment stage does).
	sig := crashLoopSignal()
	sig.Namespace = "prod"
	sig.Name = "api-0"
	sig.Enrichment = &engine.Enrichment{Bundle: "kind=bundle.target severity=info namespace=prod kind_of_object=Deployment name=api"}
	d.DispatchSignal(ctx, sig)

	// A duplicate inside the dedup window: suppressed, store-only.
	dup := crashLoopSignal()
	dup.Namespace = "prod"
	dup.Name = "api-0"
	dup.LastSeen = dup.LastSeen.Add(time.Minute)
	d.DispatchSignal(ctx, dup)

	// Stages 2+3 — diagnosis, action: the incident playbook writes
	// §9.4 records at each material transition (`lookout triage
	// status` → store), and the drill exports each written record
	// into the capture as a JSON line.
	fp := engine.ScanFingerprint("CrashLoopBackOff", "Pod", "") // from the scan finding the agent held
	resourceKey := memory.ResourceKey("Pod", "prod", "api-0")
	for _, rec := range []memory.TriageStatusRecord{
		{Fingerprint: fp, ResourceKey: resourceKey, Session: "stub-sess-0001", Status: memory.StatusTriaged,
			RootCauseHypothesis: "DB connection string invalid in checkout-config", SeverityOverride: "warning"},
		{Fingerprint: fp, ResourceKey: resourceKey, Session: "stub-sess-0001", Status: memory.StatusActioned,
			RootCauseHypothesis: "DB connection string invalid in checkout-config",
			Action:              "PR #402 opened; config rollout pending"},
	} {
		written, err := st.UpsertTriageStatus(ctx, rec)
		if err != nil {
			t.Fatalf("UpsertTriageStatus(%s): %v", rec.Status, err)
		}
		line, err := json.Marshal(written)
		if err != nil {
			t.Fatal(err)
		}
		daemon.appendLine(string(line))
	}

	// Stage 4 — externally verified outcome: the recovery observer's
	// resolved signal routes into the bound session and flips the
	// §9.4 record to resolved (the automatic lifecycle).
	res := resolvedSignalFor(sig, engine.KindResolved)
	res.Fingerprint = fp
	res.Cluster = "prod-east"
	res.Namespace = "prod"
	res.Name = "api-0"
	d.DispatchSignal(ctx, res)

	// Harvest the capture.
	trajectories, err := corpus.Harvest(strings.NewReader(daemon.capture()))
	if err != nil {
		t.Fatalf("Harvest: %v", err)
	}
	if len(trajectories) != 1 {
		t.Fatalf("got %d trajectories, want exactly 1: %+v", len(trajectories), trajectories)
	}
	tr := trajectories[0]
	if !tr.Complete {
		t.Fatalf("trajectory incomplete — a §9.3 stage is missing: %+v", tr)
	}
	if tr.Session != "stub-sess-0001" || tr.Cluster != "prod-east" || tr.Fingerprint != fp {
		t.Errorf("identity wrong: session=%q cluster=%q fingerprint=%q", tr.Session, tr.Cluster, tr.Fingerprint)
	}
	if tr.Symptom.Kind != "k8s-event" || tr.Symptom.Reason != "CrashLoopBackOff" ||
		tr.Symptom.KindOfObject != "Pod" || tr.Symptom.Name != "api-0" {
		t.Errorf("symptom stage wrong: %+v", tr.Symptom)
	}
	if !tr.Diagnosis.EnrichmentBundle || tr.Diagnosis.TriageStatus != "triaged" ||
		tr.Diagnosis.RootCause != "DB connection string invalid in checkout-config" {
		t.Errorf("diagnosis stage wrong: %+v", tr.Diagnosis)
	}
	if tr.Action.Status != "actioned" || !strings.Contains(tr.Action.Action, "PR #402") {
		t.Errorf("action stage wrong: %+v", tr.Action)
	}
	if tr.Outcome.Kind != "resolved" || tr.Outcome.Resolution != "recovered" ||
		tr.Outcome.ClearedAfter != "2m30s" || tr.Outcome.ObservedStableFor != "5m0s" {
		t.Errorf("outcome stage wrong: %+v", tr.Outcome)
	}
	if tr.Label != "recovered" {
		t.Errorf("label = %q, want the structured ground truth \"recovered\"", tr.Label)
	}

	// Drill evidence (docs/milestones/M5.md): the harvested
	// trajectory, verbatim.
	evidence, _ := json.Marshal(tr)
	t.Logf("harvested trajectory: %s", evidence)

	// The §9.4 record joined the corpus: flipped to resolved in the
	// store by the recovery dispatch, no manual TTL bookkeeping.
	records, err := st.TriageStatuses(ctx, memory.TriageQuery{Fingerprint: fp})
	if err != nil {
		t.Fatalf("TriageStatuses: %v", err)
	}
	if len(records) != 1 || records[0].Status != memory.StatusResolved {
		t.Errorf("triage-status record did not flip to resolved: %+v", records)
	}
}

// stockoutSignal is the scripted §10.1 stockout shape both simulated
// clusters observe: same zone-scoped failure class, cluster-local
// object identity.
func stockoutSignal(kind, uid, reason, objectClass, name, zone string) engine.Signal {
	ts := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	return engine.Signal{
		Kind:     kind,
		Source:   engine.SourceSentinel,
		Severity: engine.SeverityCritical,
		Zone:     zone,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: uid, Reason: reason},
			KindOfObject: objectClass,
			Name:         name,
			Message:      "autoscaler noScaleUp decision for nodegroup " + name,
			FirstSeen:    ts,
			LastSeen:     ts,
			Count:        1,
		},
	}
}

// TestDrill_MultiClusterRollup_Stockout simulates the M5 exit's
// fleet half: TWO sentinel dispatcher instances (cluster=prod-east /
// prod-west) observe the same staged zonal stockout (plus the
// quota_blocked class), and the captured inject streams roll up
// fleet-side as a pure fingerprint join — identical fingerprints,
// differing cluster identity, no payload parsing.
func TestDrill_MultiClusterRollup_Stockout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const zone = "us-east1-b"

	clusters := []string{"prod-east", "prod-west"}
	streams := map[string][]inject.Payload{}
	for _, cluster := range clusters {
		daemon, base := newCaptureDaemon(t)
		d := newCaptureDispatcher(t, base, cluster)
		// The same scripted signal shapes on both clusters: a zonal
		// stockout and a project-scoped quota block (§10.1 remedy-
		// disjoint classes → distinct fingerprints).
		d.DispatchSignal(ctx, stockoutSignal("capacity.stockout", "nodegroup:pool-a", "stockout", "NodeGroup", "pool-a", zone))
		d.DispatchSignal(ctx, stockoutSignal("capacity.quota_blocked", "quota:CPUS/us-east1", "quota_blocked", "Quota", "CPUS", ""))

		for _, line := range strings.Split(daemon.capture(), "\n") {
			if !strings.HasPrefix(line, "INJECT ") {
				continue
			}
			var envelope struct {
				Message string `json:"message"`
			}
			if err := json.Unmarshal([]byte(line[strings.Index(line, " body=")+len(" body="):]), &envelope); err != nil {
				t.Fatalf("%s capture: %v", cluster, err)
			}
			var p inject.Payload
			if err := json.Unmarshal([]byte(envelope.Message), &p); err != nil {
				t.Fatalf("%s payload: %v", cluster, err)
			}
			streams[cluster] = append(streams[cluster], p)
		}
		if len(streams[cluster]) != 2 {
			t.Fatalf("%s: got %d injects, want 2", cluster, len(streams[cluster]))
		}
	}

	// Per-kind cross-cluster assertions: identical fingerprint,
	// differing cluster, shared zone, full §8 identity on the wire.
	for i, kind := range []string{"capacity.stockout", "capacity.quota_blocked"} {
		east, west := streams["prod-east"][i], streams["prod-west"][i]
		if east.Kind != kind || west.Kind != kind {
			t.Fatalf("stream order drifted: %s vs %s (want %s)", east.Kind, west.Kind, kind)
		}
		if east.Fingerprint == "" || east.Fingerprint != west.Fingerprint {
			t.Errorf("%s: fingerprints differ across clusters (east=%s west=%s) — the fleet rollup join breaks", kind, east.Fingerprint, west.Fingerprint)
		}
		if east.Cluster != "prod-east" || west.Cluster != "prod-west" {
			t.Errorf("%s: cluster identity wrong: %q / %q", kind, east.Cluster, west.Cluster)
		}
		if east.Zone != west.Zone {
			t.Errorf("%s: zone differs: %q / %q", kind, east.Zone, west.Zone)
		}
		if east.Source != "sentinel" || east.Severity != "critical" {
			t.Errorf("%s: §8 identity fields missing on the wire: source=%q severity=%q", kind, east.Source, east.Severity)
		}
	}
	if streams["prod-east"][0].Zone != zone {
		t.Errorf("stockout zone = %q, want %s", streams["prod-east"][0].Zone, zone)
	}

	// The fleet-side rollup: group BOTH streams by fingerprint alone.
	// The staged stockout collapses to ONE fleet-level group with a
	// member per cluster; so does quota_blocked; nothing else exists.
	groups := map[string][]string{}
	for cluster, payloads := range streams {
		for _, p := range payloads {
			groups[p.Fingerprint] = append(groups[p.Fingerprint], cluster)
		}
	}
	if len(groups) != 2 {
		t.Fatalf("fingerprint join yielded %d groups, want 2 (stockout + quota_blocked): %v", len(groups), groups)
	}
	for fp, members := range groups {
		if len(members) != 2 {
			t.Errorf("group %s has members %v, want one per cluster", fp, members)
		}
	}

	// Drill evidence (docs/milestones/M5.md): the fleet join,
	// verbatim.
	for fp, members := range groups {
		t.Logf("fleet group %s → clusters %v", fp, members)
	}
	for _, cluster := range clusters {
		wire, _ := json.Marshal(streams[cluster][0])
		t.Logf("%s stockout wire payload: %s", cluster, wire)
	}

	// Kind-level storm coverage: the storm fingerprint recipe is
	// cluster-free by construction — the same node-failure storm in
	// two clusters of one zone joins, a different zone does not
	// (zone-scoped causes are exactly what fleet rollup must group).
	stormEast := engine.Fingerprint(engine.KindStorm, "NodeNotReady", "Node", zone)
	stormWest := engine.Fingerprint(engine.KindStorm, "NodeNotReady", "Node", zone)
	if stormEast != stormWest {
		t.Error("storm fingerprints must be identical across clusters of one zone")
	}
	if other := engine.Fingerprint(engine.KindStorm, "NodeNotReady", "Node", "us-east1-c"); other == stormEast {
		t.Error("storm fingerprints must differ across zones")
	}
}
