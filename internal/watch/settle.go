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
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

const (
	// defaultSettleHoldEntries caps held keys and cleared tombstones per
	// cluster (#834). Losing an entry under extreme churn costs only one
	// self_resolved count, never a forwarded incident.
	defaultSettleHoldEntries = 4096
	// settleQuietSlack is how long past --settle a held key must stay
	// unsighted before being counted self_resolved: kubelet's restart
	// back-off caps at 5m (300s), so a failure still in progress is
	// re-sighted inside it.
	settleQuietSlack = 5 * time.Minute
	// settleSweepInterval is how often the background sweep checks for
	// self-resolved holds.
	settleSweepInterval = 30 * time.Second
	// defaultSettleClearedTTL bounds how long a self-resolved blip's
	// tombstone is kept to clamp repeat blips inside the Event's TTL.
	defaultSettleClearedTTL = 24 * time.Hour

	settleOutcomeForwarded    = "forwarded"
	settleOutcomeSelfResolved = "self_resolved"
)

type settleHoldKey struct {
	Namespace string
	UID       string
	Reason    string // canonical reason
}

type settleHold struct {
	LastSeen      time.Time // watcher clock at last held sighting (aged by Sweep)
	EventLastSeen time.Time // Event's own LastSeen (saved onto tombstone on Sweep)
	Start         time.Time // clamped blip start when this is a repeat blip
}

type settleCleared struct {
	EventLastSeen time.Time
	At            time.Time // watcher clock when swept (aged by clearedTTL)
}

// settleTracker tracks keys currently held by engine.GateSettle (#834) so the
// dispatcher can:
//   - count lookout_watch_gate_outcomes_total{gate="settle",outcome="forwarded"}
//     when a held key later lasts past --settle and passes the gate;
//   - sweep keys unsighted for --settle + 5m as outcome="self_resolved" and
//     record a cleared tombstone;
//   - clamp repeat blips (after a self_resolved tombstone OR a read-only
//     DedupCache.LastRetiredTS lookup on a resolved incident) to the new
//     blip's first sighting rather than the reused Event's old FirstTimestamp.
//
// Memory-only (like #816 tombstones): a sentinel restart fails open on
// repeat-blip start, falling back to the Event's own FirstTimestamp.
// Safe for concurrent use across DispatchSignal and runSettleSweepLoop.
type settleTracker struct {
	mu         sync.Mutex
	entries    map[settleHoldKey]settleHold
	cleared    map[settleHoldKey]settleCleared
	max        int
	quiet      time.Duration
	clearedTTL time.Duration
	now        func() time.Time
}

func newSettleTracker(settle, clearedTTL time.Duration) *settleTracker {
	if clearedTTL <= 0 {
		clearedTTL = defaultSettleClearedTTL
	}
	return &settleTracker{
		entries:    make(map[settleHoldKey]settleHold),
		cleared:    make(map[settleHoldKey]settleCleared),
		max:        defaultSettleHoldEntries,
		quiet:      settle + settleQuietSlack,
		clearedTTL: clearedTTL,
		now:        time.Now,
	}
}

func (t *settleTracker) setClock(fn func() time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if fn == nil {
		t.now = time.Now
		return
	}
	t.now = fn
}

func (t *settleTracker) clockLocked() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

func makeSettleHoldKey(sig engine.Signal, canonicalReason string) settleHoldKey {
	return settleHoldKey{
		Namespace: sig.Namespace,
		UID:       sig.Key.UID,
		Reason:    canonicalReason,
	}
}

// BlipStart computes the effective FirstSeen for sig when a standing hold,
// cleared-blip tombstone, or resolved-incident EventLastTS (from read-only
// DedupCache.LastRetiredTS) indicates this sighting is a repeat blip on a
// reused Event object. Strictly read-only: never mutates entries or cleared.
func (t *settleTracker) BlipStart(sig engine.Signal, canonicalReason string, retiredTS time.Time, hasRetired bool) (time.Time, bool) {
	if t == nil || sig.LastSeen.IsZero() {
		return time.Time{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	hk := makeSettleHoldKey(sig, canonicalReason)
	if h, ok := t.entries[hk]; ok && !h.Start.IsZero() {
		return h.Start, true
	}
	now := t.clockLocked()
	if c, ok := t.cleared[hk]; ok && now.Sub(c.At) <= t.clearedTTL {
		if !sig.LastSeen.After(c.EventLastSeen) {
			// Informer relist replay of the cleared blip — do not clamp.
			return time.Time{}, false
		}
		return sig.LastSeen, true
	}
	if hasRetired && !retiredTS.IsZero() && sig.LastSeen.After(retiredTS) {
		return sig.LastSeen, true
	}
	return time.Time{}, false
}

// Hold records a sighting rejected by engine.GateSettle. An informer relist
// replay of an already-cleared blip (Event LastSeen not strictly after the
// cleared tombstone) opens no hold so one blip never double-counts
// self_resolved.
func (t *settleTracker) Hold(sig engine.Signal, canonicalReason string, blipStart time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clockLocked()
	hk := makeSettleHoldKey(sig, canonicalReason)
	prev, held := t.entries[hk]

	if c, ok := t.cleared[hk]; ok {
		if now.Sub(c.At) > t.clearedTTL {
			delete(t.cleared, hk)
		} else if !held && !sig.LastSeen.After(c.EventLastSeen) {
			return
		} else if sig.LastSeen.After(c.EventLastSeen) {
			delete(t.cleared, hk)
		}
	}

	if !held && len(t.entries) >= t.max {
		var oldestKey settleHoldKey
		var oldest time.Time
		first := true
		for k, h := range t.entries {
			if first || h.LastSeen.Before(oldest) {
				oldestKey, oldest, first = k, h.LastSeen, false
			}
		}
		delete(t.entries, oldestKey)
	}

	start := prev.Start
	if start.IsZero() && !blipStart.IsZero() {
		start = blipStart
	}
	t.entries[hk] = settleHold{
		LastSeen:      now,
		EventLastSeen: sig.LastSeen,
		Start:         start,
	}
}

// Release forgets a held key that just passed the settle gate, returning true
// when the key was actively held (so the caller increments outcome="forwarded").
func (t *settleTracker) Release(sig engine.Signal, canonicalReason string) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	hk := makeSettleHoldKey(sig, canonicalReason)
	_, ok := t.entries[hk]
	delete(t.entries, hk)
	if c, hasCleared := t.cleared[hk]; hasCleared && sig.LastSeen.After(c.EventLastSeen) {
		delete(t.cleared, hk)
	}
	return ok
}

// Sweep expires every hold unsighted for at least quiet (--settle + 5m),
// records a cleared tombstone for each, prunes expired/overflowing tombstones,
// and returns how many holds resolved without paging.
func (t *settleTracker) Sweep() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clockLocked()
	expired := 0
	for hk, h := range t.entries {
		if now.Sub(h.LastSeen) >= t.quiet {
			delete(t.entries, hk)
			t.cleared[hk] = settleCleared{
				EventLastSeen: h.EventLastSeen,
				At:            now,
			}
			expired++
		}
	}
	for hk, c := range t.cleared {
		if now.Sub(c.At) > t.clearedTTL {
			delete(t.cleared, hk)
		}
	}
	for len(t.cleared) > t.max {
		var oldestKey settleHoldKey
		var oldest time.Time
		first := true
		for k, c := range t.cleared {
			if first || c.At.Before(oldest) {
				oldestKey, oldest, first = k, c.At, false
			}
		}
		delete(t.cleared, oldestKey)
	}
	return expired
}

// Len reports the number of keys currently held (test helper).
func (t *settleTracker) Len() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}

func runSettleSweepLoop(ctx context.Context, tracker *settleTracker, m *metrics) {
	ticker := time.NewTicker(settleSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := tracker.Sweep(); n > 0 && m != nil {
				m.gateOutcomes.WithLabelValues(engine.GateSettle, settleOutcomeSelfResolved).Add(float64(n))
			}
		}
	}
}
