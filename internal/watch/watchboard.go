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
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// Watchboard (DESIGN.md §7.7 + §15 Q2, decided): the shared session
// warning-class signals batch into as a rolling digest, so leading
// indicators never each open a per-incident session at page priority.
// The watchboard is the per-incident-mode answer to warning noise —
// in `--mode=shared` it is disabled entirely and ALL severities keep
// routing to `--target-session` (frozen contract).
//
// Lifecycle (size-based rotation, §15 Q2): the session is created
// lazily — POST /sessions with X-Asserted-Caller = `--owner`, same as
// per-incident sessions — at the first digest flush; there is no
// watchboard session until the cluster produces a warning. After
// `--watchboard-rotate` digest injects, the NEXT flush opens a fresh
// session and the old one is closed with a final
// kind=watchboard.rotated lineage inject. Dedup bindings into the old
// session stay valid: followups and §7.4 outcomes for an incident
// keep routing to where the incident lives; only NEW warnings go to
// the successor. Sessions are identified in-band (no name parameter
// on POST /sessions): every inject is kind=watchboard.*, each digest
// carries (board_generation, sequence), and rotation links successor
// ← predecessor. See docs/watchboard-rotation-design.md.

// watchboardConfig is the flag-shaped construction input.
type watchboardConfig struct {
	// injector is the agent sink digests flush through (the field
	// keeps its historical name). When it carries the
	// inject.SessionOpener capability (the core-agent sink), rotation
	// keeps the frozen §15 Q2 wire order — empty successor session
	// first, lineage pointer into the closed session, then the digest.
	// A stateless sink (webhook) opens the successor WITH the digest
	// and appends the lineage pointer afterwards.
	injector      inject.Sink
	metrics       *metrics
	cluster       string
	dryRun        bool
	batch         int           // --watchboard-batch: flush at this many buffered warnings
	flushInterval time.Duration // --watchboard-flush: flush this long after the first buffered warning
	rotateAfter   int           // --watchboard-rotate: digests per session before rotating
}

// boardEntry is one buffered warning: the signal plus its dedup
// running count (stamped by the dispatcher, same as the per-incident
// payload's count).
type boardEntry struct {
	sig   engine.Signal
	count int
}

// watchboard batches warning-class signals into rolling digest
// injects on a managed shared session. Safe for concurrent use: the
// dispatcher Adds, the tick goroutine flushes on age.
type watchboard struct {
	watchboardConfig

	// clock is injectable for the byte-exact wire pins.
	clock func() time.Time
	// bind is called after a successful digest inject, once per
	// flushed entry, so the dispatcher can bind the incident to the
	// watchboard session (dedup binding + §7.4 recovery tracking).
	// Called with the watchboard's lock held — must not call back
	// into the watchboard.
	bind func(sig engine.Signal, sessionID string)

	mu sync.Mutex
	// sid is the current watchboard session ("" until the first
	// non-dry-run flush).
	sid string
	// generation counts watchboard sessions opened by this sentinel
	// (1-based; the digest payload's board_generation).
	generation int
	// injects counts digests injected into the CURRENT session — the
	// §15 Q2 rotation counter and the digest sequence source.
	injects int
	buffer  []boardEntry
	// opened is when the current buffer received its first entry —
	// the interval-flush anchor and the digest's window_start.
	opened time.Time
}

func newWatchboard(cfg watchboardConfig) *watchboard {
	return &watchboard{watchboardConfig: cfg, clock: time.Now}
}

// Add buffers one warning-class signal and flushes immediately when
// the count threshold is reached (`--watchboard-batch`); otherwise
// the interval tick flushes it within `--watchboard-flush`.
func (b *watchboard) Add(ctx context.Context, sig engine.Signal, count int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buffer) == 0 {
		b.opened = b.clock()
	}
	b.buffer = append(b.buffer, boardEntry{sig: sig, count: count})
	b.metrics.watchboardEntries.WithLabelValues(sig.Kind).Inc()
	b.metrics.watchboardBuffered.Set(float64(len(b.buffer)))
	log.Printf("watchboard: buffered %s %s/%s (severity=warning, buffered=%d/%d)",
		sig.Kind, sig.Namespace, sig.Name, len(b.buffer), b.batch)
	if len(b.buffer) >= b.batch {
		b.flushLocked(ctx)
	}
}

// Tick flushes the buffer if its oldest entry has aged past the flush
// interval. Driven by run()'s ticker; worst-case flush latency is
// flushInterval + the tick period.
func (b *watchboard) Tick(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buffer) == 0 {
		return
	}
	if b.clock().Sub(b.opened) >= b.flushInterval {
		b.flushLocked(ctx)
	}
}

// FlushNow flushes any buffered warnings regardless of age — the
// shutdown path, so a terminating sentinel doesn't drop its buffer.
func (b *watchboard) FlushNow(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buffer) > 0 {
		b.flushLocked(ctx)
	}
}

// run drives interval flushes until ctx is cancelled, then does a
// final best-effort flush. The tick period is derived from the flush
// interval (never a flag): any value well below it behaves
// identically, mirroring recoveryTickInterval's rationale.
func (b *watchboard) run(ctx context.Context) {
	tick := b.flushInterval / 4
	if tick < time.Second {
		tick = time.Second
	}
	if tick > 5*time.Second {
		tick = 5 * time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			b.FlushNow(flushCtx)
			cancel()
			return
		case <-t.C:
			b.Tick(ctx)
		}
	}
}

// flushLocked injects the buffered warnings as one digest, rotating
// the session first when the §15 Q2 threshold is reached. Caller
// holds b.mu.
func (b *watchboard) flushLocked(ctx context.Context) {
	now := b.clock()
	if b.dryRun {
		if b.generation == 0 {
			b.generation = 1
		}
	} else if b.sid == "" || b.injects >= b.rotateAfter {
		opener, ok := b.injector.(inject.SessionOpener)
		if !ok {
			// Stateless sink: the successor incident opens WITH the
			// digest as its payload — the whole flush happens there.
			b.openStatelessLocked(ctx, now)
			return
		}
		if !b.openSessionLocked(ctx, opener, now) {
			return
		}
	}

	payload := b.digestPayloadLocked(now)
	if b.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", b.sid, string(out))
	} else if err := b.injector.Append(ctx, b.sid, payload); err != nil {
		// Dropped, consistent with a failed per-incident inject: the
		// entries were counted (watchboard_entries_total) and remain
		// visible in the dedup cache; retrying a stateful buffer
		// against a down daemon would grow without bound.
		b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
		log.Printf("watchboard: digest inject (sid=%s, entries=%d): %v", b.sid, len(b.buffer), err)
		b.clearBufferLocked()
		return
	}
	b.finishFlushLocked()
}

// openSessionLocked creates (or rotates onto) the watchboard session
// through the core-agent sink's SessionOpener capability, keeping the
// frozen §15 Q2 wire order: empty successor first, kind=
// watchboard.rotated into the closed session, digest afterwards (the
// caller's flush). Returns false when the flush must stop (no session
// to flush into — buffer already dropped). Caller holds b.mu.
func (b *watchboard) openSessionLocked(ctx context.Context, opener inject.SessionOpener, now time.Time) bool {
	rotating := b.sid != ""
	newSid, err := opener.CreateSession(ctx)
	if err != nil {
		b.metrics.sessionCreates.WithLabelValues("error").Inc()
		if rotating {
			// Rotation deferred, not data lost: keep flushing into
			// the over-threshold session and retry the rotation on
			// the next flush.
			log.Printf("watchboard: create successor session failed (rotation deferred to next flush): %v", err)
			return true
		}
		// No board session at all — drop the buffer, consistent
		// with a failed per-incident CreateSession dropping its
		// event. The next warning starts a fresh buffer.
		b.metrics.injectErrors.WithLabelValues("watchboard", "session_create").Inc()
		log.Printf("watchboard: create session failed — dropping %d buffered warning(s): %v", len(b.buffer), err)
		b.clearBufferLocked()
		return false
	}
	b.metrics.sessionCreates.WithLabelValues("ok").Inc()
	if rotating {
		rotated := inject.WatchboardRotatedPayload{
			Kind:               inject.KindWatchboardRotated,
			Cluster:            b.cluster,
			BoardGeneration:    b.generation,
			SuccessorSessionID: newSid,
			InjectsCount:       b.injects,
			RotatedAt:          now,
		}
		if rerr := b.injector.Append(ctx, b.sid, rotated); rerr != nil {
			// The lineage pointer is best-effort: the successor
			// is already the live board, and the digest lineage
			// stays reconstructable from (generation, sequence).
			b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
			log.Printf("watchboard: rotated lineage inject into %s failed: %v", b.sid, rerr)
		}
		b.metrics.watchboardRotations.Inc()
		log.Printf("watchboard: rotated after %d digest inject(s): %s → %s (generation %d → %d)",
			b.injects, b.sid, newSid, b.generation, b.generation+1)
	}
	b.sid = newSid
	b.generation++
	b.injects = 0
	return true
}

// openStatelessLocked is the flush + rotation path for sinks WITHOUT
// the SessionOpener capability (the webhook sink): an incident at a
// stateless receiver exists only by receiving its first payload, so
// the successor opens WITH the digest and the kind=watchboard.rotated
// lineage pointer is appended to the CLOSED incident afterwards — the
// one place the webhook wire order differs from the core-agent
// sink's. Caller holds b.mu.
func (b *watchboard) openStatelessLocked(ctx context.Context, now time.Time) {
	rotating := b.sid != ""
	prevSid, prevGen, prevInjects := b.sid, b.generation, b.injects
	// Successor lineage coordinates ride the opening digest:
	// (generation+1, sequence 1).
	b.generation++
	b.injects = 0
	payload := b.digestPayloadLocked(now)
	newSid, err := b.injector.OpenIncident(ctx, payload)
	if newSid == "" {
		b.sid, b.generation, b.injects = prevSid, prevGen, prevInjects
		b.metrics.sessionCreates.WithLabelValues("error").Inc()
		if rotating {
			// Rotation deferred like the SessionOpener path — but the
			// digest rode the failed open, so deliver it into the
			// over-threshold incident instead.
			log.Printf("watchboard: open successor incident failed (rotation deferred to next flush): %v", err)
			payload = b.digestPayloadLocked(now)
			if aerr := b.injector.Append(ctx, b.sid, payload); aerr != nil {
				b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
				log.Printf("watchboard: digest inject (sid=%s, entries=%d): %v", b.sid, len(b.buffer), aerr)
				b.clearBufferLocked()
				return
			}
			b.finishFlushLocked()
			return
		}
		b.metrics.injectErrors.WithLabelValues("watchboard", "session_create").Inc()
		log.Printf("watchboard: create session failed — dropping %d buffered warning(s): %v", len(b.buffer), err)
		b.clearBufferLocked()
		return
	}
	b.metrics.sessionCreates.WithLabelValues("ok").Inc()
	b.sid = newSid
	if rotating {
		rotated := inject.WatchboardRotatedPayload{
			Kind:               inject.KindWatchboardRotated,
			Cluster:            b.cluster,
			BoardGeneration:    prevGen,
			SuccessorSessionID: newSid,
			InjectsCount:       prevInjects,
			RotatedAt:          now,
		}
		if rerr := b.injector.Append(ctx, prevSid, rotated); rerr != nil {
			// Best-effort lineage, same as the SessionOpener path.
			b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
			log.Printf("watchboard: rotated lineage inject into %s failed: %v", prevSid, rerr)
		}
		b.metrics.watchboardRotations.Inc()
		log.Printf("watchboard: rotated after %d digest inject(s): %s → %s (generation %d → %d)",
			prevInjects, prevSid, newSid, prevGen, prevGen+1)
	}
	if err != nil {
		// Incident opened but the digest delivery failed (a partial
		// open — not something the webhook sink produces today, but
		// the Sink contract allows it): consistent with a failed
		// digest inject, the buffer is dropped after counting.
		b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
		log.Printf("watchboard: digest inject (sid=%s, entries=%d): %v", b.sid, len(b.buffer), err)
		b.clearBufferLocked()
		return
	}
	b.finishFlushLocked()
}

// finishFlushLocked is the common success tail of a digest delivery:
// counters, per-entry session binding, the flush log, buffer reset.
// Caller holds b.mu.
func (b *watchboard) finishFlushLocked() {
	b.injects++
	b.metrics.watchboardDigests.Inc()
	// Bind AFTER the successful inject: followups and §7.4 outcomes
	// for these incidents now route to this watchboard session.
	if b.bind != nil {
		for _, e := range b.buffer {
			b.bind(e.sig, b.sid)
		}
	}
	log.Printf("watchboard: digest %d entry(ies) → sid=%s (generation=%d, injects=%d/%d, mode=per-incident)",
		len(b.buffer), b.sid, b.generation, b.injects, b.rotateAfter)
	b.clearBufferLocked()
}

// digestPayloadLocked composes the schema-stable kind=watchboard.digest
// wire body from the buffer. Caller holds b.mu.
func (b *watchboard) digestPayloadLocked(now time.Time) inject.WatchboardDigestPayload {
	entries := make([]inject.WatchboardEntry, 0, len(b.buffer))
	for _, e := range b.buffer {
		entries = append(entries, inject.WatchboardEntry{
			Kind:         e.sig.Kind,
			Fingerprint:  e.sig.Fingerprint,
			Reason:       e.sig.Key.Reason,
			Namespace:    e.sig.Namespace,
			KindOfObject: e.sig.KindOfObject,
			Name:         e.sig.Name,
			UID:          e.sig.Key.UID,
			Count:        e.count,
			FirstSeen:    e.sig.FirstSeen,
			LastSeen:     e.sig.LastSeen,
		})
	}
	return inject.WatchboardDigestPayload{
		Kind:            inject.KindWatchboardDigest,
		Cluster:         b.cluster,
		BoardGeneration: b.generation,
		Sequence:        b.injects + 1,
		WindowStart:     b.opened,
		WindowEnd:       now,
		Entries:         entries,
	}
}

func (b *watchboard) clearBufferLocked() {
	b.buffer = b.buffer[:0]
	b.metrics.watchboardBuffered.Set(0)
}

// bindWatchboardIncident is the watchboard's bind callback: after a
// successful digest inject each flushed warning is bound to the
// watchboard session, so dedup followups and §7.4 outcomes route
// there. If the incident is ALREADY bound — e.g. a §7.5 storm claimed
// it while it sat in the buffer — the existing binding wins: bindings
// are per-incident and point at wherever the incident lives.
func (d *dispatcher) bindWatchboardIncident(sig engine.Signal, sid string) {
	if _, ok := d.dedup.LookupSession(sig.Key); ok {
		return
	}
	d.dedup.BindIncident(sig.Key, sid, sig.IncidentRef())
	if d.tracker != nil {
		d.tracker.Track(engine.Incident{
			Key:       sig.Key,
			SessionID: sid,
			FirstSeen: sig.FirstSeen,
			Ref:       sig.IncidentRef(),
		})
	}
}
