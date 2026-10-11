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
	// injectMaxBytes is --inject-max-bytes, the sink's per-inject wire
	// ceiling, applied to the digest before it goes out (issue #337).
	// <= 0 means no ceiling, matching dispatcher.fitInject.
	injectMaxBytes int
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
	// reattach, when non-nil, is the §7.7 ancestor-reattachment stage
	// (issue #220), consulted per entry at the TOP of a flush: a true
	// return means the dispatcher delivered the warning into an
	// existing per-incident session sharing its blast-radius ancestor,
	// and the entry must not become a digest entry. Nil when no
	// topology resolver is available — every entry then digests as it
	// did pre-#220. Same lock contract as bind.
	reattach func(ctx context.Context, sig engine.Signal, count int) bool

	flushMu sync.Mutex
	mu      sync.Mutex
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
	if len(b.buffer) == 0 {
		b.opened = b.clock()
	}
	b.buffer = append(b.buffer, boardEntry{sig: sig, count: count})
	b.metrics.watchboardEntries.WithLabelValues(sig.Kind).Inc()
	b.metrics.watchboardBuffered.Set(float64(len(b.buffer)))
	log.Printf("watchboard: buffered %s %s/%s (severity=warning, buffered=%d/%d)",
		sig.Kind, sig.Namespace, sig.Name, len(b.buffer), b.batch)
	needFlush := len(b.buffer) >= b.batch
	b.mu.Unlock()
	if needFlush {
		b.flush(ctx)
	}
}

// Tick flushes the buffer if its oldest entry has aged past the flush
// interval. Driven by run()'s ticker; worst-case flush latency is
// flushInterval + the tick period.
func (b *watchboard) Tick(ctx context.Context) {
	b.mu.Lock()
	needFlush := len(b.buffer) > 0 && b.clock().Sub(b.opened) >= b.flushInterval
	b.mu.Unlock()
	if needFlush {
		b.flush(ctx)
	}
}

// FlushNow flushes any buffered warnings regardless of age — the
// shutdown path, so a terminating sentinel doesn't drop its buffer.
func (b *watchboard) FlushNow(ctx context.Context) {
	b.mu.Lock()
	needFlush := len(b.buffer) > 0
	b.mu.Unlock()
	if needFlush {
		b.flush(ctx)
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

// startBoard runs the watchboard flush loop in a goroutine and returns
// a wait func that STOPS the board and blocks until run has returned —
// i.e. until the final shutdown FlushNow has completed. wait is never
// nil. The wiring shutdown path calls wait so a terminating sentinel
// does not exit before the board's last flush lands (issue #108).
//
// The board gets its own cancel scope rather than running on the
// caller's ctx, and wait cancels it. That is what makes the join
// unconditional: the caller reaches it on two paths, and on only one
// of them is the parent ctx already cancelled. Running on the caller's
// ctx meant a runner exiting because a SOURCE failed joined a board
// that had been given no reason to stop, and the process wedged with
// its diagnosis unprinted (issue #364). The final FlushNow does its
// delivery on a fresh background ctx, so cancelling here does not cost
// the buffered warnings #108 exists to keep.
func startBoard(ctx context.Context, board *watchboard) (wait func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		board.run(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

// flush snapshots buffered entries under b.mu and executes all network
// I/O (reattachment, session creation/rotation, digest Append) outside
// b.mu while serialized by b.flushMu (#642).
func (b *watchboard) flush(ctx context.Context) {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	if len(b.buffer) == 0 {
		b.mu.Unlock()
		return
	}
	entries := b.buffer
	windowStart := b.opened
	b.buffer = nil
	b.metrics.watchboardBuffered.Set(0)
	sid := b.sid
	gen := b.generation
	injects := b.injects
	b.mu.Unlock()

	now := b.clock()
	entries = b.reattachBatch(ctx, entries)
	if len(entries) == 0 {
		return
	}
	if b.dryRun {
		if gen == 0 {
			gen = 1
		}
	} else if sid == "" || injects >= b.rotateAfter {
		opener, ok := b.sessionOpener()
		if !ok {
			newSid, newGen, newInjects := b.openStateless(ctx, now, windowStart, entries, sid, gen, injects)
			b.mu.Lock()
			b.sid, b.generation, b.injects = newSid, newGen, newInjects
			b.mu.Unlock()
			return
		}
		var okOpen bool
		sid, gen, injects, okOpen = b.openSession(ctx, opener, now, entries, sid, gen, injects)
		if !okOpen {
			b.mu.Lock()
			b.sid, b.generation, b.injects = sid, gen, injects
			b.mu.Unlock()
			return
		}
	}

	payload := b.digestPayload(now, windowStart, gen, injects, entries)
	if b.dryRun {
		out, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("--- dry-run payload for session %q ---\n%s\n", sid, string(out))
	} else if err := b.injector.Append(ctx, sid, payload); err != nil {
		b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
		log.Printf("watchboard: digest inject (sid=%s, entries=%d): %v", sid, len(entries), err)
		b.mu.Lock()
		b.sid, b.generation, b.injects = sid, gen, injects
		b.mu.Unlock()
		return
	}
	injects++
	b.mu.Lock()
	b.sid, b.generation, b.injects = sid, gen, injects
	b.mu.Unlock()
	b.finishFlush(entries, sid, gen, injects)
}

// sessionOpener returns how this board opens an empty session, if its
// sink can: CreateSessionKeyed for a sink that places incidents by
// lookout's identity (the ax sink: one stable watchboard task per
// cluster), else the plain SessionOpener capability (core-agent).
// ok=false is a stateless sink (webhook).
func (b *watchboard) sessionOpener() (func(context.Context) (string, error), bool) {
	if ko, ok := b.injector.(inject.KeyedOpener); ok {
		key := inject.IncidentKey{Cluster: b.cluster, Kind: inject.IncidentKeyWatchboard}
		return func(ctx context.Context) (string, error) { return ko.CreateSessionKeyed(ctx, key) }, true
	}
	if so, ok := b.injector.(inject.SessionOpener); ok {
		return so.CreateSession, true
	}
	return nil, false
}

// openSession creates (or rotates onto) the watchboard session outside
// b.mu (serialized by b.flushMu).
func (b *watchboard) openSession(ctx context.Context, opener func(context.Context) (string, error), now time.Time, entries []boardEntry, sid string, gen, injects int) (string, int, int, bool) {
	rotating := sid != ""
	newSid, err := opener(ctx)
	if err != nil {
		b.metrics.sessionCreates.WithLabelValues("error").Inc()
		if rotating {
			log.Printf("watchboard: create successor session failed (rotation deferred to next flush): %v", err)
			return sid, gen, injects, true
		}
		b.metrics.injectErrors.WithLabelValues("watchboard", "session_create").Inc()
		log.Printf("watchboard: create session failed — dropping %d buffered warning(s): %v", len(entries), err)
		return sid, gen, injects, false
	}
	b.metrics.sessionCreates.WithLabelValues("ok").Inc()
	if rotating {
		rotated := inject.WatchboardRotatedPayload{
			Kind:               inject.KindWatchboardRotated,
			Cluster:            b.cluster,
			BoardGeneration:    gen,
			SuccessorSessionID: newSid,
			InjectsCount:       injects,
			RotatedAt:          now,
		}
		if rerr := b.injector.Append(ctx, sid, rotated); rerr != nil {
			b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
			log.Printf("watchboard: rotated lineage inject into %s failed: %v", sid, rerr)
		}
		b.metrics.watchboardRotations.Inc()
		log.Printf("watchboard: rotated after %d digest inject(s): %s → %s (generation %d → %d)",
			injects, sid, newSid, gen, gen+1)
	}
	return newSid, gen + 1, 0, true
}

// openStateless handles flush + rotation for sinks without SessionOpener
// outside b.mu (serialized by b.flushMu).
func (b *watchboard) openStateless(ctx context.Context, now, windowStart time.Time, entries []boardEntry, prevSid string, prevGen, prevInjects int) (string, int, int) {
	rotating := prevSid != ""
	nextGen := prevGen + 1
	payload := b.digestPayload(now, windowStart, nextGen, 0, entries)
	newSid, err := b.injector.OpenIncident(ctx, payload)
	if newSid == "" {
		b.metrics.sessionCreates.WithLabelValues("error").Inc()
		if rotating {
			log.Printf("watchboard: open successor incident failed (rotation deferred to next flush): %v", err)
			payload = b.digestPayload(now, windowStart, prevGen, prevInjects, entries)
			if aerr := b.injector.Append(ctx, prevSid, payload); aerr != nil {
				b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
				log.Printf("watchboard: digest inject (sid=%s, entries=%d): %v", prevSid, len(entries), aerr)
				return prevSid, prevGen, prevInjects
			}
			b.finishFlush(entries, prevSid, prevGen, prevInjects+1)
			return prevSid, prevGen, prevInjects + 1
		}
		b.metrics.injectErrors.WithLabelValues("watchboard", "session_create").Inc()
		log.Printf("watchboard: create session failed — dropping %d buffered warning(s): %v", len(entries), err)
		return prevSid, prevGen, prevInjects
	}
	b.metrics.sessionCreates.WithLabelValues("ok").Inc()
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
			b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
			log.Printf("watchboard: rotated lineage inject into %s failed: %v", prevSid, rerr)
		}
		b.metrics.watchboardRotations.Inc()
		log.Printf("watchboard: rotated after %d digest inject(s): %s → %s (generation %d → %d)",
			prevInjects, prevSid, newSid, prevGen, nextGen)
	}
	if err != nil {
		b.metrics.injectErrors.WithLabelValues("watchboard", "inject").Inc()
		log.Printf("watchboard: digest inject (sid=%s, entries=%d): %v", newSid, len(entries), err)
		return newSid, nextGen, 0
	}
	b.finishFlush(entries, newSid, nextGen, 1)
	return newSid, nextGen, 1
}

func (b *watchboard) reattachBatch(ctx context.Context, entries []boardEntry) []boardEntry {
	if b.reattach == nil || len(entries) == 0 {
		return entries
	}
	kept := entries[:0]
	for _, e := range entries {
		if b.reattach(ctx, e.sig, e.count) {
			continue
		}
		kept = append(kept, e)
	}
	return kept
}

func (b *watchboard) finishFlush(entries []boardEntry, sid string, gen, injects int) {
	b.metrics.watchboardDigests.Inc()
	if b.bind != nil {
		for _, e := range entries {
			b.bind(e.sig, sid)
		}
	}
	log.Printf("watchboard: digest %d entry(ies) → sid=%s (generation=%d, injects=%d/%d, mode=per-incident)",
		len(entries), sid, gen, injects, b.rotateAfter)
}

func (b *watchboard) digestPayload(now, windowStart time.Time, gen, injects int, raw []boardEntry) inject.WatchboardDigestPayload {
	entries := make([]inject.WatchboardEntry, 0, len(raw))
	for _, e := range raw {
		entries = append(entries, inject.WatchboardEntry{
			Kind:         e.sig.Kind,
			Fingerprint:  e.sig.Fingerprint,
			Reason:       e.sig.Key.Reason,
			ReasonClass:  engine.ReasonClass(e.sig.Kind, e.sig.Key.Reason, e.sig.Message),
			Namespace:    e.sig.Namespace,
			KindOfObject: e.sig.KindOfObject,
			Name:         e.sig.Name,
			UID:          e.sig.Key.UID,
			Count:        e.count,
			FirstSeen:    e.sig.FirstSeen,
			LastSeen:     e.sig.LastSeen,
		})
	}
	payload := inject.WatchboardDigestPayload{
		Kind:            inject.KindWatchboardDigest,
		Cluster:         b.cluster,
		BoardGeneration: gen,
		Sequence:        injects + 1,
		WindowStart:     windowStart,
		WindowEnd:       now,
		Entries:         entries,
	}
	if b.injectMaxBytes > 0 {
		if shed := payload.FitTo(b.injectMaxBytes); len(shed) > 0 {
			log.Printf("watchboard: digest exceeded --inject-max-bytes=%d; dropped the %d oldest of %d entries to fit "+
				"(--watchboard-batch=%d is too large for this sink's inject ceiling)",
				b.injectMaxBytes, payload.EntriesDropped, len(entries), b.batch)
			b.metrics.injectShrinks.WithLabelValues("watchboard_entries").Inc()
		}
	}
	return payload
}

// bindWatchboardIncident is the watchboard's bind callback: after a
// successful digest inject each flushed warning is bound to the
// watchboard session, so dedup followups and §7.4 outcomes route
// there. If the incident is ALREADY bound — e.g. a §7.5 storm claimed
// it while it sat in the buffer — the existing binding wins: bindings
// are per-incident and point at wherever the incident lives.
func (d *dispatcher) bindWatchboardIncident(sig engine.Signal, sid string) {
	// Same canonical pipeline key DispatchSignal used for this
	// signal's dedup slot (message-aware — see TriageEvent.CanonicalKey).
	key := sig.CanonicalKey()
	if _, ok := d.dedup.LookupSession(key); ok {
		return
	}
	d.dedup.BindIncident(key, sid, sig.IncidentRef())
	if d.tracker != nil {
		d.tracker.Track(engine.Incident{
			Key:       key,
			SessionID: sid,
			FirstSeen: sig.FirstSeen,
			Ref:       sig.IncidentRef(),
		})
	}
}
