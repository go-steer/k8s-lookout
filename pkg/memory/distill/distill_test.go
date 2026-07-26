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

package distill

// §13 conventions: every predicate is exercised over a SEEDED real
// store (pkg/store on a temp file — the databases are ordinary
// SQLite), with a fires case and a doesn't-fire case per predicate,
// through a fake memory.FactWriter that captures writes.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

var t0 = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// fakeWriter is the memory.FactWriter fake: it records upserts in
// call order and can inject failures.
type fakeWriter struct {
	facts []memory.DistilledFact
	err   error
}

func (w *fakeWriter) UpsertFact(_ context.Context, f memory.DistilledFact) (memory.DistilledFact, error) {
	if w.err != nil {
		return memory.DistilledFact{}, w.err
	}
	w.facts = append(w.facts, f)
	return f, nil
}

func (w *fakeWriter) byClass(class string) []memory.DistilledFact {
	var out []memory.DistilledFact
	for _, f := range w.facts {
		if f.Class == class {
			out = append(out, f)
		}
	}
	return out
}

// seededStore opens a real store with a settable clock and returns
// both plus a helper that records a signal at the clock's current
// time and flushes.
func seededStore(t *testing.T) (*store.Store, *clock, func(engine.Signal, store.Outcome)) {
	t.Helper()
	c := &clock{now: t0.Add(-6 * 24 * time.Hour)} // seed inside the 7d window
	s, err := store.Open(filepath.Join(t.TempDir(), "lookout.db"),
		store.WithClock(c.Now), store.WithLogf(t.Logf))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, c, func(sig engine.Signal, out store.Outcome) {
		s.Record(sig, out)
		s.Flush()
	}
}

type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) Advance(d time.Duration) { c.now = c.now.Add(d) }
func (c *clock) At(t time.Time)          { c.now = t }
func passConfig(overrides ...func(*Config)) Config {
	cfg := Config{Now: func() time.Time { return t0 }}
	for _, o := range overrides {
		o(&cfg)
	}
	return cfg
}

func stockoutSignal(nodegroup, zone string) engine.Signal {
	return engine.Signal{
		Kind:        "capacity.stockout",
		Source:      engine.SourceSentinel,
		Severity:    engine.SeverityCritical,
		Fingerprint: "sha256:stockout",
		Cluster:     "prod-east",
		Project:     "acme-prod",
		Zone:        zone,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "nodegroup:" + nodegroup, Reason: "stockout"},
			KindOfObject: "NodeGroup",
			Name:         nodegroup,
			Message:      "autoscaler noScaleUp decision for nodegroup " + nodegroup + ": GCE_STOCKOUT",
		},
	}
}

func crashSignal(uid, pod, controller string) engine.Signal {
	return engine.Signal{
		Kind:        "k8s-event",
		Source:      engine.SourceSentinel,
		Severity:    engine.SeverityCritical,
		Fingerprint: "sha256:crash",
		Cluster:     "prod-east",
		TriageEvent: engine.TriageEvent{
			Key:           engine.EventKey{UID: uid, Reason: "CrashLoopBackOff"},
			Namespace:     "prod",
			KindOfObject:  "Pod",
			Name:          pod,
			Message:       "back-off restarting failed container",
			ControllerRef: controller,
		},
	}
}

func renewalSignal(uid, cert, issuer string) engine.Signal {
	return engine.Signal{
		Kind:        "expiry.warning",
		Source:      engine.SourceSentinel,
		Severity:    engine.SeverityCritical,
		Fingerprint: "sha256:renewal",
		Cluster:     "prod-east",
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: uid, Reason: "warning"},
			Namespace:    "prod",
			KindOfObject: "Certificate",
			Name:         cert,
			Message: "certificate expires in 48h0m0s (notAfter 2026-07-27T12:00:00Z); subject=" + cert +
				" issuer=" + issuer + " days_left=2; source=cert-manager renewal=FAILED last_failure=2026-07-24T00:00:00Z",
		},
	}
}

// --- predicate: capacity.stockout.recurrence -------------------------------

// TestStockout_Fires: three stockouts for the same (zone, nodegroup)
// inside the window — suppressed duplicates included — produce one
// fact with a deterministic statement and scope.
func TestStockout_Fires(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	first := c.Now()
	record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteInjected, SessionID: "sid-1"})
	c.Advance(24 * time.Hour)
	record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteSuppressed, SessionID: "sid-1"})
	c.Advance(24 * time.Hour)
	record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteInjected, SessionID: "sid-2"})
	last := c.Now()

	w := &fakeWriter{}
	stats, err := Run(context.Background(), s, w, passConfig())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.Scanned != 3 {
		t.Errorf("scanned = %d, want 3", stats.Scanned)
	}
	facts := w.byClass(ClassStockout)
	if len(facts) != 1 {
		t.Fatalf("stockout facts = %d, want 1", len(facts))
	}
	f := facts[0]
	wantScope := map[string]string{
		memory.ScopeProject:   "acme-prod",
		memory.ScopeCluster:   "prod-east",
		memory.ScopeZone:      "us-east1-b",
		memory.ScopeNodeGroup: "n2d-pool",
	}
	if memory.ScopeKey(f.Scope) != memory.ScopeKey(wantScope) {
		t.Errorf("scope = %v, want %v", f.Scope, wantScope)
	}
	wantStatement := fmt.Sprintf("us-east1-b nodegroup n2d-pool: 3 stockouts in 7d (first %s, last %s)",
		first.UTC().Format(time.RFC3339), last.UTC().Format(time.RFC3339))
	if f.Statement != wantStatement {
		t.Errorf("statement:\n got %q\nwant %q", f.Statement, wantStatement)
	}
	if f.Occurrences != 3 || f.DistinctObjects != 1 {
		t.Errorf("evidence: occurrences=%d distinct=%d, want 3/1", f.Occurrences, f.DistinctObjects)
	}
	if len(f.SourceFingerprints) != 1 || f.SourceFingerprints[0] != "sha256:stockout" {
		t.Errorf("fingerprints = %v", f.SourceFingerprints)
	}
	if !f.WindowStart.Equal(t0.Add(-DefaultWindow)) || !f.WindowEnd.Equal(t0) {
		t.Errorf("window = %s..%s", f.WindowStart, f.WindowEnd)
	}
}

// TestStockout_BelowThresholdAndOutsideWindow: two in-window
// occurrences don't fire; a third one OLDER than the window doesn't
// rescue the count.
func TestStockout_BelowThresholdAndOutsideWindow(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	c.At(t0.Add(-8 * 24 * time.Hour)) // outside the 7d window
	record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteInjected})
	c.At(t0.Add(-2 * 24 * time.Hour))
	record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteInjected})
	c.Advance(24 * time.Hour)
	record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteInjected})

	w := &fakeWriter{}
	stats, err := Run(context.Background(), s, w, passConfig())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.Scanned != 2 {
		t.Errorf("scanned = %d, want 2 (window excludes the old row)", stats.Scanned)
	}
	if len(w.facts) != 0 {
		t.Errorf("below-threshold pattern wrote %d fact(s): %+v", len(w.facts), w.facts)
	}
}

// TestStockout_DistinctZonesAreDistinctFacts: the zone is a scope
// dimension — the same nodegroup name stocking out in two zones is
// two facts (that separation is what makes "us-east1-c clean" a
// readable absence).
func TestStockout_DistinctZonesAreDistinctFacts(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	for range 3 {
		record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteInjected})
		record(stockoutSignal("n2d-pool", "us-east1-c"), store.Outcome{Route: store.RouteInjected})
		c.Advance(time.Hour)
	}
	w := &fakeWriter{}
	if _, err := Run(context.Background(), s, w, passConfig()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := w.byClass(ClassStockout); len(got) != 2 {
		t.Errorf("facts = %d, want 2 (one per zone)", len(got))
	}
}

// --- predicate: workload.crashloop.recurrence ------------------------------

// TestCrashloop_Fires: two FRESH incidents plus suppressed followups
// for pods of the same workload distill into one per-workload fact.
func TestCrashloop_Fires(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	// Incident 1: pod A crashes, injects, then two dedup followups.
	record(crashSignal("uid-a", "payment-7d5b9c6f4-x2k9q", "ReplicaSet/payment-7d5b9c6f4"), store.Outcome{Route: store.RouteInjected, SessionID: "sid-1"})
	c.Advance(time.Minute)
	record(crashSignal("uid-a", "payment-7d5b9c6f4-x2k9q", "ReplicaSet/payment-7d5b9c6f4"), store.Outcome{Route: store.RouteSuppressed, SessionID: "sid-1"})
	c.Advance(time.Minute)
	record(crashSignal("uid-a", "payment-7d5b9c6f4-x2k9q", "ReplicaSet/payment-7d5b9c6f4"), store.Outcome{Route: store.RouteSuppressed, SessionID: "sid-1"})
	// Incident 2, next day: replacement pod B crashes again.
	c.Advance(24 * time.Hour)
	record(crashSignal("uid-b", "payment-7d5b9c6f4-m8t2z", "ReplicaSet/payment-7d5b9c6f4"), store.Outcome{Route: store.RouteInjected, SessionID: "sid-2"})
	c.Advance(time.Minute)
	record(crashSignal("uid-b", "payment-7d5b9c6f4-m8t2z", "ReplicaSet/payment-7d5b9c6f4"), store.Outcome{Route: store.RouteSuppressed, SessionID: "sid-2"})

	w := &fakeWriter{}
	if _, err := Run(context.Background(), s, w, passConfig()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	facts := w.byClass(ClassCrashloop)
	if len(facts) != 1 {
		t.Fatalf("crashloop facts = %d (%+v), want 1", len(facts), w.facts)
	}
	f := facts[0]
	if f.Scope[memory.ScopeWorkload] != "ReplicaSet/payment-7d5b9c6f4" {
		t.Errorf("workload scope = %q, want the ControllerRef", f.Scope[memory.ScopeWorkload])
	}
	if f.Scope[memory.ScopeReason] != "CrashLoopBackOff" || f.Scope[memory.ScopeNamespace] != "prod" {
		t.Errorf("scope = %v", f.Scope)
	}
	if f.Occurrences != 5 || f.DistinctObjects != 2 {
		t.Errorf("evidence: occurrences=%d distinct=%d, want 5/2", f.Occurrences, f.DistinctObjects)
	}
	want := "prod/ReplicaSet/payment-7d5b9c6f4: 2 fresh incidents, 5 CrashLoopBackOff occurrences across 2 pods in 7d"
	if f.Statement != want {
		t.Errorf("statement:\n got %q\nwant %q", f.Statement, want)
	}
}

// TestCrashloop_OneIncidentDoesNotFire: five occurrences inside ONE
// incident (one injected + four suppressed) is a long incident, not
// a recurring pattern — no fact.
func TestCrashloop_OneIncidentDoesNotFire(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	record(crashSignal("uid-a", "payment-7d5b9c6f4-x2k9q", ""), store.Outcome{Route: store.RouteInjected, SessionID: "sid-1"})
	for range 4 {
		c.Advance(time.Minute)
		record(crashSignal("uid-a", "payment-7d5b9c6f4-x2k9q", ""), store.Outcome{Route: store.RouteSuppressed, SessionID: "sid-1"})
	}
	w := &fakeWriter{}
	if _, err := Run(context.Background(), s, w, passConfig()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(w.facts) != 0 {
		t.Errorf("single incident wrote %d fact(s): %+v", len(w.facts), w.facts)
	}
}

// TestCrashloop_ResolvedRowsExcluded: §7.4 outcome records carry the
// original reason but are outcomes, not symptoms — they must not
// count toward recurrence.
func TestCrashloop_ResolvedRowsExcluded(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	record(crashSignal("uid-a", "payment-7d5b9c6f4-x2k9q", ""), store.Outcome{Route: store.RouteInjected, SessionID: "sid-1"})
	for range 4 {
		c.Advance(time.Hour)
		resolved := crashSignal("uid-a", "payment-7d5b9c6f4-x2k9q", "")
		resolved.Kind = engine.KindResolved
		record(resolved, store.Outcome{Route: store.RouteResolved, SessionID: "sid-1"})
	}
	c.Advance(time.Hour)
	record(crashSignal("uid-a", "payment-7d5b9c6f4-x2k9q", ""), store.Outcome{Route: store.RouteInjected, SessionID: "sid-2"})

	w := &fakeWriter{}
	if _, err := Run(context.Background(), s, w, passConfig()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 2 fresh incidents but only 2 symptom occurrences (< 5): the 4
	// resolved rows must not have been counted.
	if len(w.byClass(ClassCrashloop)) != 0 {
		t.Errorf("resolved rows counted toward recurrence: %+v", w.facts)
	}
}

// TestCrashloop_FallbackWorkloadKey: without a ControllerRef or app
// labels, pod-name suffix stripping groups replacement pods of the
// same ReplicaSet together.
func TestCrashloop_FallbackWorkloadKey(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	record(crashSignal("uid-a", "payment-7d5b9c6f4-x2k9q", ""), store.Outcome{Route: store.RouteInjected, SessionID: "sid-1"})
	c.Advance(24 * time.Hour)
	record(crashSignal("uid-b", "payment-7d5b9c6f4-m8t2z", ""), store.Outcome{Route: store.RouteInjected, SessionID: "sid-2"})
	for i := range 3 {
		c.Advance(time.Minute)
		uid := []string{"uid-a", "uid-b", "uid-b"}[i]
		pod := []string{"payment-7d5b9c6f4-x2k9q", "payment-7d5b9c6f4-m8t2z", "payment-7d5b9c6f4-m8t2z"}[i]
		record(crashSignal(uid, pod, ""), store.Outcome{Route: store.RouteSuppressed})
	}
	w := &fakeWriter{}
	if _, err := Run(context.Background(), s, w, passConfig()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	facts := w.byClass(ClassCrashloop)
	if len(facts) != 1 {
		t.Fatalf("crashloop facts = %d, want 1 (suffix stripping should merge replacement pods)", len(facts))
	}
	if got := facts[0].Scope[memory.ScopeWorkload]; got != "payment" {
		t.Errorf("fallback workload key = %q, want %q", got, "payment")
	}
}

func TestStripPodSuffix(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"payment-7d5b9c6f4-x2k9q": "payment",       // Deployment pod: hash + random suffix
		"payment-0":               "payment",       // StatefulSet ordinal
		"payment-12":              "payment",       // StatefulSet ordinal, two digits
		"node-exporter-x2k9q":     "node-exporter", // DaemonSet pod: random suffix only
		"payment":                 "payment",       // bare name untouched
	}
	for in, want := range tests {
		if got := stripPodSuffix(in); got != want {
			t.Errorf("stripPodSuffix(%q) = %q, want %q", in, got, want)
		}
	}
}

// --- predicate: expiry.renewal_failure.recurrence --------------------------

// TestRenewal_FiresAcrossCertificates: three renewal-failure
// occurrences across two certificates under one issuer distill into
// one per-issuer fact.
func TestRenewal_FiresAcrossCertificates(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	record(renewalSignal("uid-c1", "api-tls", "letsencrypt-prod"), store.Outcome{Route: store.RouteInjected, SessionID: "sid-1"})
	c.Advance(time.Hour)
	record(renewalSignal("uid-c2", "web-tls", "letsencrypt-prod"), store.Outcome{Route: store.RouteInjected, SessionID: "sid-2"})
	c.Advance(time.Hour)
	record(renewalSignal("uid-c1", "api-tls", "letsencrypt-prod"), store.Outcome{Route: store.RouteSuppressed, SessionID: "sid-1"})

	w := &fakeWriter{}
	if _, err := Run(context.Background(), s, w, passConfig()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	facts := w.byClass(ClassRenewalFailure)
	if len(facts) != 1 {
		t.Fatalf("renewal facts = %d, want 1", len(facts))
	}
	f := facts[0]
	if f.Scope[memory.ScopeIssuer] != "letsencrypt-prod" {
		t.Errorf("issuer scope = %q", f.Scope[memory.ScopeIssuer])
	}
	want := "issuer letsencrypt-prod: 3 cert-renewal failures across 2 certificates in 7d"
	if f.Statement != want {
		t.Errorf("statement:\n got %q\nwant %q", f.Statement, want)
	}
}

// TestRenewal_SingleCertificateDoesNotFire: one certificate failing
// repeatedly is an incident about that certificate, not a fact about
// the issuer — the distinct-object floor holds it back. And healthy
// expiry countdowns (no renewal=FAILED marker) never count at all.
func TestRenewal_SingleCertificateDoesNotFire(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	for range 4 {
		record(renewalSignal("uid-c1", "api-tls", "letsencrypt-prod"), store.Outcome{Route: store.RouteSuppressed})
		c.Advance(time.Hour)
	}
	healthy := renewalSignal("uid-c3", "ok-tls", "letsencrypt-prod")
	healthy.Message = "certificate expires in 300h0m0s (notAfter 2026-08-07T00:00:00Z); subject=ok-tls issuer=letsencrypt-prod days_left=12; source=cert-manager renewal=ok"
	record(healthy, store.Outcome{Route: store.RouteWatchboard})

	w := &fakeWriter{}
	if _, err := Run(context.Background(), s, w, passConfig()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(w.facts) != 0 {
		t.Errorf("wrote %d fact(s): %+v", len(w.facts), w.facts)
	}
}

func TestIssuerOf(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"subject=x issuer=letsencrypt-prod days_left=2": "letsencrypt-prod",
		"subject=x issuer=- days_left=2":                "unknown",
		"subject=x issuer=selfsigned; source=webhook":   "selfsigned",
		"no issuer token at all":                        "unknown",
	}
	for in, want := range tests {
		if got := issuerOf(in); got != want {
			t.Errorf("issuerOf(%q) = %q, want %q", in, got, want)
		}
	}
}

// --- pass mechanics ---------------------------------------------------------

// TestRun_WriteFailureAborts: a writer failure surfaces; the pass is
// re-derivable so aborting loses freshness only.
func TestRun_WriteFailureAborts(t *testing.T) {
	t.Parallel()
	s, _, record := seededStore(t)
	for range 3 {
		record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteInjected})
	}
	w := &fakeWriter{err: errors.New("disk full")}
	if _, err := Run(context.Background(), s, w, passConfig()); err == nil {
		t.Error("expected write failure to surface")
	}
}

// TestRun_NilStoreIsQuiet: the disabled (nil) store yields nothing —
// a pass over it scans and writes zero.
func TestRun_NilStoreIsQuiet(t *testing.T) {
	t.Parallel()
	var s *store.Store
	w := &fakeWriter{}
	stats, err := Run(context.Background(), s, w, passConfig())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.Scanned != 0 || len(w.facts) != 0 {
		t.Errorf("nil store pass: scanned=%d facts=%d", stats.Scanned, len(w.facts))
	}
}

// TestRun_EndToEndUpsertDedupe: two passes over the same evidence
// through the REAL store-backed writer leave exactly one fact row
// (the §9.2 dedupe contract, integration-level).
func TestRun_EndToEndUpsertDedupe(t *testing.T) {
	t.Parallel()
	s, c, record := seededStore(t)
	for range 3 {
		record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteInjected})
		c.Advance(time.Hour)
	}
	ctx := context.Background()
	if _, err := Run(ctx, s, s, passConfig()); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	// New evidence arrives; the next pass updates the same fact.
	record(stockoutSignal("n2d-pool", "us-east1-b"), store.Outcome{Route: store.RouteInjected})
	if _, err := Run(ctx, s, s, passConfig(func(cfg *Config) {
		cfg.Now = func() time.Time { return t0.Add(time.Hour) }
	})); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	facts, err := s.Facts(ctx, memory.FactQuery{Class: ClassStockout})
	if err != nil {
		t.Fatalf("Facts: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("facts after two passes = %d, want 1 (upsert, not duplicate)", len(facts))
	}
	if facts[0].Occurrences != 4 {
		t.Errorf("occurrences = %d, want 4 after refresh", facts[0].Occurrences)
	}
}
