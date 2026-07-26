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
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// triageRefreshInterval is how stale the routing stage's view of the
// §9.4 triage-status records may get before the next signal reloads
// it. Deliberately not a flag: any value well under an incident's
// lifetime behaves identically, and the reload is one indexed SELECT
// over a low-volume table.
const triageRefreshInterval = 30 * time.Second

// triageOverrides is the severity-routing consumer of §9.4: a cached
// view of the OPEN triage-status records, consulted per dispatched
// signal and refreshed on the interval above. Two effects, per the
// design:
//
//   - severity_override: an incident the agent downgraded stops
//     re-paging on followups (and on the re-page after a dedup
//     window expires) — the signal routes at the agent's class
//     (warning → watchboard, info → store) instead of its own.
//   - status=escalated: pins critical — bypasses the watchboard
//     regardless of any override or source-stamped class.
//
// Matching is (fingerprint, resource_key): the fingerprint alone is
// class-level (§8 — it spans objects and clusters), so honoring it
// without the resource pin would let one downgraded incident silence
// every same-class incident on the cluster. The resource key is
// matched against the signal's object and, when the signal carries a
// ControllerRef, its controller — records are usually written
// against the payload's object, but a playbook may reasonably key
// the workload instead.
//
// Lifecycle is §9.4-automatic: the cache only loads OPEN records,
// and resolve() flips records to resolved when the §7.4 recovery
// inject fires (write-through: the cached view drops the record
// immediately, not at the next refresh). No manual TTL bookkeeping —
// an override "expires" by the symptom clearing, or by the agent
// rewriting the record.
type triageOverrides struct {
	store   *store.Store
	metrics *metrics
	now     func() time.Time

	mu      sync.Mutex
	loaded  time.Time
	records []memory.TriageStatusRecord
}

func newTriageOverrides(st *store.Store, m *metrics) *triageOverrides {
	return &triageOverrides{store: st, metrics: m, now: time.Now}
}

// Apply returns the signal's effective severity after honoring any
// open triage-status record for its incident. Unmatched signals keep
// their class unchanged.
func (t *triageOverrides) Apply(ctx context.Context, sig engine.Signal) engine.Severity {
	rec, ok := t.match(ctx, sig)
	if !ok {
		return sig.Severity
	}
	next := sig.Severity
	action := ""
	switch {
	case rec.Status == memory.StatusEscalated:
		next = engine.SeverityCritical
		action = "escalated"
	case rec.SeverityOverride != "":
		next = engine.Severity(rec.SeverityOverride)
		if !next.Valid() { // defensive: store validates on write
			return sig.Severity
		}
		switch {
		case severityWeight(next) < severityWeight(sig.Severity):
			action = "downgraded"
		case severityWeight(next) > severityWeight(sig.Severity):
			action = "upgraded"
		}
	}
	if next == sig.Severity || action == "" {
		return sig.Severity
	}
	t.metrics.triageOverrides.WithLabelValues(action).Inc()
	log.Printf("triage-status: %s %s %s/%s %s → %s (status=%s, session=%s)",
		action, sig.Kind, sig.Namespace, sig.Name, sig.Severity, next, rec.Status, rec.Session)
	return next
}

// resolve is the §9.4 automatic lifecycle: a §7.4 kind=resolved
// outcome flips the incident's open record(s) to resolved so they
// join the §9.3 corpus and stop steering routing/scans. A
// kind=resolved.reverted deliberately does NOT restore the record:
// the fix did not stick, and the next inject should page at its own
// class until the agent re-triages.
func (t *triageOverrides) resolve(ctx context.Context, sig engine.Signal) {
	flipped, err := t.store.ResolveTriageStatus(ctx, sig.Fingerprint, signalResourceKeys(sig)...)
	if err != nil {
		log.Printf("triage-status: resolve flip for %s %s/%s: %v", sig.Kind, sig.Namespace, sig.Name, err)
		return
	}
	if flipped == 0 {
		return
	}
	t.metrics.triageFlips.Add(float64(flipped))
	log.Printf("triage-status: %d record(s) flipped to resolved for %s/%s (fingerprint=%s) — joined the §9.3 corpus",
		flipped, sig.Namespace, sig.Name, sig.Fingerprint)
	// Write-through: drop the flipped records from the cached view
	// now; a routing decision between here and the next refresh must
	// not honor a resolved record.
	t.mu.Lock()
	t.loaded = time.Time{}
	t.mu.Unlock()
}

// match refreshes the cache when stale and finds the freshest open
// record for the signal's incident.
func (t *triageOverrides) match(ctx context.Context, sig engine.Signal) (memory.TriageStatusRecord, bool) {
	if sig.Fingerprint == "" {
		return memory.TriageStatusRecord{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if t.loaded.IsZero() || now.Sub(t.loaded) >= triageRefreshInterval {
		recs, err := t.store.TriageStatuses(ctx, memory.TriageQuery{OpenOnly: true})
		if err != nil {
			// Keep the previous view; routing must not flap on a
			// transient read failure.
			log.Printf("triage-status: refresh failed (keeping %d cached record(s)): %v", len(t.records), err)
		} else {
			t.records = recs // already newest-updated first
			t.loaded = now
		}
	}
	keys := signalResourceKeys(sig)
	for _, rec := range t.records {
		if rec.Fingerprint != sig.Fingerprint {
			continue
		}
		for _, key := range keys {
			if rec.MatchesResource(key) {
				return rec, true
			}
		}
	}
	return memory.TriageStatusRecord{}, false
}

// signalResourceKeys composes the §9.4 resource keys a signal's
// incident can be recorded under: the object itself and, when known,
// its controller (ControllerRef is "Kind/name"; the controller
// shares the object's namespace).
func signalResourceKeys(sig engine.Signal) []string {
	keys := []string{memory.ResourceKey(sig.KindOfObject, sig.Namespace, sig.Name)}
	if kind, name, ok := strings.Cut(sig.ControllerRef, "/"); ok && kind != "" && name != "" {
		keys = append(keys, memory.ResourceKey(kind, sig.Namespace, name))
	}
	return keys
}

// severityWeight orders the §7.7 classes for the
// downgraded/upgraded distinction (info < warning < critical).
func severityWeight(s engine.Severity) int {
	switch s {
	case engine.SeverityCritical:
		return 2
	case engine.SeverityWarning:
		return 1
	default:
		return 0
	}
}
