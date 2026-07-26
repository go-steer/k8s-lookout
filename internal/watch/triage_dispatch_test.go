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

// §9.4 severity-routing tests: open triage-status records refine the
// class the dispatcher routes on — a downgraded incident stops
// re-paging, escalated pins critical — and the §7.4 recovery inject
// flips the record to resolved (the automatic lifecycle: after the
// flip the override no longer applies).

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// stampedFingerprint reproduces what DispatchSignal stamps on a
// source signal (kind + canonical reason + object class, zone "").
func stampedFingerprint(sig engine.Signal) string {
	return engine.Fingerprint(sig.Kind, engine.CanonicalReason(sig.Key.Reason), sig.KindOfObject, sig.Zone)
}

func triageDispatcher(t *testing.T, base string) (*dispatcher, *store.Store) {
	t.Helper()
	d, _ := newBoardDispatcher(t, base, 100, time.Minute, 200)
	s := openDispatchStore(t, d)
	d.triage = newTriageOverrides(s, d.metrics, 3)
	return d, s
}

func writeRecord(t *testing.T, s *store.Store, rec memory.TriageStatusRecord) {
	t.Helper()
	if _, err := s.UpsertTriageStatus(context.Background(), rec); err != nil {
		t.Fatalf("UpsertTriageStatus: %v", err)
	}
}

// TestRoutingHonorsDowngrade: a critical signal whose incident an
// agent downgraded (severity_override=warning) routes to the
// watchboard instead of opening a fresh per-incident session — the
// downgraded incident stops re-paging.
func TestRoutingHonorsDowngrade(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)
	ctx := context.Background()

	sig := crashLoopSignal()
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint:      stampedFingerprint(sig),
		ResourceKey:      memory.ResourceKey("Pod", "checkout", "checkout-svc-7b9d-x4kzq"),
		Session:          "sid-original",
		Status:           memory.StatusTriaged,
		SeverityOverride: "warning",
	})

	d.DispatchSignal(ctx, sig)
	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteWatchboard]) != 1 {
		t.Errorf("routes = %v, want the downgraded signal on the watchboard", routes(byRoute))
	}
	if len(byRoute[store.RouteInjected]) != 0 || len(*injects) != 0 {
		t.Errorf("downgraded incident still paged: injected rows=%d injects=%d",
			len(byRoute[store.RouteInjected]), len(*injects))
	}
	if got := testutil.ToFloat64(d.metrics.triageOverrides.WithLabelValues("downgraded")); got != 1 {
		t.Errorf("triage_overrides_total{action=downgraded} = %v, want 1", got)
	}
}

// TestRoutingHonorsControllerKeyedRecord: a record the playbook keyed
// by the WORKLOAD (controller) instead of the pod still matches the
// pod's signals via ControllerRef.
func TestRoutingHonorsControllerKeyedRecord(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)

	sig := crashLoopSignal() // ControllerRef: ReplicaSet/checkout-svc-7b9d
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint:      stampedFingerprint(sig),
		ResourceKey:      memory.ResourceKey("ReplicaSet", "checkout", "checkout-svc-7b9d"),
		Status:           memory.StatusActioned,
		SeverityOverride: "info",
	})
	d.DispatchSignal(context.Background(), sig)
	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteInfoStored]) != 1 {
		t.Errorf("routes = %v, want info-stored via the controller-keyed record", routes(byRoute))
	}
}

// TestRoutingEscalatedPinsCritical: status=escalated keeps the
// incident hot — a warning-class signal bypasses the watchboard and
// opens a per-incident session, whatever severity_override says.
func TestRoutingEscalatedPinsCritical(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)

	sig := warningSignal(1)
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint:      stampedFingerprint(sig),
		ResourceKey:      memory.ResourceKey("Pod", "shop", "cart-1"),
		Status:           memory.StatusEscalated,
		SeverityOverride: "info", // escalated outranks any leftover override
	})
	d.DispatchSignal(context.Background(), sig)
	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteInjected]) != 1 || len(*injects) == 0 {
		t.Errorf("routes = %v, want an escalated per-incident inject (watchboard bypassed)", routes(byRoute))
	}
	if len(byRoute[store.RouteWatchboard]) != 0 {
		t.Error("escalated signal landed on the watchboard")
	}
	if got := testutil.ToFloat64(d.metrics.triageOverrides.WithLabelValues("escalated")); got != 1 {
		t.Errorf("triage_overrides_total{action=escalated} = %v, want 1", got)
	}
}

// TestRoutingIgnoresOtherIncidents: the fingerprint is class-level
// (§8) — a record for ANOTHER object of the same class must not
// steer this signal (no resource match, no override), and a resolved
// record must not either.
func TestRoutingIgnoresOtherIncidents(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)

	sig := crashLoopSignal()
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint:      stampedFingerprint(sig),
		ResourceKey:      memory.ResourceKey("Pod", "checkout", "some-other-pod"),
		Status:           memory.StatusTriaged,
		SeverityOverride: "warning",
	})
	d.DispatchSignal(context.Background(), sig)
	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteInjected]) != 1 || len(*injects) == 0 {
		t.Errorf("routes = %v, want per-incident (another object's record must not downgrade this one)", routes(byRoute))
	}
}

// TestRecoveryFlipExpiresOverride: the M4 lifecycle round-trip —
// (1) downgraded incident routes to the watchboard; (2) the §7.4
// kind=resolved outcome flips the record to resolved (write-through,
// counted); (3) the next occurrence of the class on the same object
// pages at its own severity again: the override expired WITH the
// symptom, no TTL bookkeeping anywhere.
func TestRecoveryFlipExpiresOverride(t *testing.T) {
	t.Parallel()
	base, _ := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)
	// A nanosecond dedup window: every dispatch is a fresh incident,
	// standing in for the window expiry between (1) and (3).
	d.dedup, _ = engine.NewDedupCache(time.Nanosecond, "")
	ctx := context.Background()

	sig := crashLoopSignal()
	fp := stampedFingerprint(sig)
	key := memory.ResourceKey("Pod", "checkout", "checkout-svc-7b9d-x4kzq")
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint:      fp,
		ResourceKey:      key,
		Status:           memory.StatusTriaged,
		SeverityOverride: "warning",
	})

	// (1) downgraded → watchboard.
	d.DispatchSignal(ctx, sig)
	if byRoute := occurrencesByRoute(t, s); len(byRoute[store.RouteWatchboard]) != 1 {
		t.Fatalf("routes = %v, want watchboard first", routes(byRoute))
	}

	// (2) symptom clears: the resolved outcome carries the original
	// incident's fingerprint; the record flips.
	resolved := resolvedSignalFor(sig, engine.KindResolved)
	resolved.Fingerprint = fp
	d.DispatchSignal(ctx, resolved)
	open, err := s.TriageStatuses(ctx, memory.TriageQuery{OpenOnly: true})
	if err != nil {
		t.Fatalf("TriageStatuses: %v", err)
	}
	if len(open) != 0 {
		t.Fatalf("open records after recovery = %+v, want none (flipped to resolved)", open)
	}
	if got := testutil.ToFloat64(d.metrics.triageFlips); got != 1 {
		t.Errorf("triage_resolved_flips_total = %v, want 1", got)
	}

	// (3) the symptom recurs on the same pod after the dedup window
	// (newer event activity + lapsed 1ns cooldown = the retry safety
	// net): with the record resolved, routing follows the signal's
	// own class again — it pages.
	time.Sleep(time.Millisecond) // let the 1ns window lapse
	recurrence := sig
	recurrence.LastSeen = sig.LastSeen.Add(time.Hour)
	d.DispatchSignal(ctx, recurrence)
	byRoute := occurrencesByRoute(t, s)
	if len(byRoute[store.RouteInjected]) != 1 {
		t.Errorf("routes after flip = %v, want a fresh per-incident inject (override expired with the symptom)", routes(byRoute))
	}
}

// TestSharedModeSkipsOverrides: --mode=shared keeps its frozen
// contract — every severity routes to --target-session, records or
// not.
func TestSharedModeSkipsOverrides(t *testing.T) {
	t.Parallel()
	base, injects := newRoutingFakeDaemon(t)
	d, s := triageDispatcher(t, base)
	d.mode = "shared"
	d.targetSid = "shared-1"
	d.board = nil

	sig := crashLoopSignal()
	writeRecord(t, s, memory.TriageStatusRecord{
		Fingerprint:      stampedFingerprint(sig),
		ResourceKey:      memory.ResourceKey("Pod", "checkout", "checkout-svc-7b9d-x4kzq"),
		Status:           memory.StatusTriaged,
		SeverityOverride: "info",
	})
	d.DispatchSignal(context.Background(), sig)
	if len(*injects) != 1 || (*injects)[0].SessionID != "shared-1" {
		t.Errorf("shared mode: injects = %+v, want exactly one to shared-1", *injects)
	}
}

func routes(m map[store.RouteOutcome][]store.Occurrence) map[store.RouteOutcome]int {
	out := map[store.RouteOutcome]int{}
	for k, v := range m {
		out[k] = len(v)
	}
	return out
}
