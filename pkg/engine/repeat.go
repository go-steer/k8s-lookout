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

package engine

import (
	"hash/fnv"
	"strconv"
	"strings"
	"time"
)

// The repeat schedule (issue #599, DESIGN.md §7.4 "Repeats of an open
// incident"): what happens when an incident's dedup window lapses
// while the incident is still open.
//
// Before #599 every lapse opened the incident again. The scheduler
// re-queues an unschedulable pod about every five minutes, just past
// the 5m default --dedup-window, so one stuck pod opened 225 sessions
// in a 24h soak and its agent re-diagnosed an unchanged failure every
// time. Now the lapse is a followup into the bound session:
//
//   - a pure reminder (repeat_count N > 0) when nothing material
//     changed, spaced on a doubling schedule — the first one window
//     after the open, then 2, 4, 8 windows after the previous one,
//     capped at repeatIntervalCap (5m, 10m, 20m, 40m, 1h, 1h, … at
//     the default window). Occurrences in between are absorbed:
//     counted, never injected.
//   - a material-change followup (repeat_count 0, omitted on the
//     wire) when the signal changes — a class the incident has not
//     shown before, or a jump in its occurrence rate. A class change
//     goes in at the first occurrence more than max(2 windows, the
//     next reminder's interval) after the last inject — one seen
//     inside the first window included — and further changes fold
//     into it, so a failure rotating through message shapes never
//     injects more often than its reminder schedule. The cost: a lone
//     real change late in the backoff waits up to that interval (1h at
//     most). A rate jump has its own floor and goes in at once. The
//     schedule restarts from one window after either.
//   - a re-send of the last inject when it did not reach the session
//     (MarkUndelivered) — including the open itself, so an agent whose
//     session was created but never got the incident does not meet it
//     first as a pure reminder.
//
// The first fire is untouched (still an immediate open), and a
// resolved incident's recurrence is still a fresh open: only an
// incident ObserveSignal's case 2 calls open ever reaches here.

const (
	// repeatIntervalCap is the longest gap between two reminders,
	// fixed by the #599 decision (no flag). A --dedup-window longer
	// than this is its own cap: reminders never come faster than the
	// window.
	repeatIntervalCap = time.Hour
	// repeatJumpFactor and repeatJumpMin define a count jump: the
	// occurrences since the last inject arrive at repeatJumpFactor
	// times the reference rate or faster, and there are at least
	// repeatJumpMin of them.
	//
	// Why 4×: rates that move by 2× are routine for a failure that is
	// not changing — kubelet's crash backoff and image-pull backoff
	// each double their delay step by step, the scheduler's retry
	// cadence jitters around its period, and the reference rate is
	// measured over intervals that themselves double. Two doublings
	// is outside that envelope. Why 10: a rate taken over a handful of
	// occurrences is noise (one informer re-list replays an event per
	// Event object, which counts), and ten is more than the stock
	// steady cadences deliver in any one reminder interval up to the
	// 40m step (a 5m retry period gives 8), so a steady failure cannot
	// clear the floor and the factor at once.
	repeatJumpFactor = 4
	repeatJumpMin    = 10
	// maxRepeatClasses bounds dedupEntry.Classes. Real incidents show
	// a handful (kubelet's pull cycle is four); beyond the cap the
	// oldest is forgotten, which at worst reads one returning class as
	// a change.
	maxRepeatClasses = 16
)

// repeatInterval is the gap before reminder n (n >= 1), measured from
// the previous inject: window·2^(n-1), capped at repeatIntervalCap or
// the window itself, whichever is longer.
func repeatInterval(window time.Duration, n int) time.Duration {
	limit := max(repeatIntervalCap, window)
	d := window
	for i := 1; i < n && d < limit; i++ {
		d *= 2
	}
	return min(d, limit)
}

// repeat decides an event for an incident that is still open (case 2
// of ObserveSignal). Called with the cache lock held; lapsed is
// whether the dedup window had expired since the entry's LastSeen.
//
// In order:
//
//  1. An undelivered last inject (MarkUndelivered) is re-sent now,
//     schedule or not: the session must not miss an open or a change.
//     A stale reminder re-sends as the class change pending since.
//  2. A rate jump is a RepeatCount-0 followup at once.
//  3. A pending class change is a RepeatCount-0 followup once more
//     than max(2 windows, the next reminder's interval) has passed
//     since the last inject; until then it is held (absorbed), and it
//     supersedes a reminder that comes due meanwhile.
//  4. Inside the incident's first window (no repeat inject yet, the
//     window not lapsed) it is a plain duplicate.
//  5. A reminder that has come due is the next RepeatCount.
//  6. Anything else is absorbed.
//
// The first lapse always finds the first reminder due, since the open
// is at least a window back.
func (c *DedupCache) repeat(e *dedupEntry, now, eventLastTS time.Time, class string, lapsed bool) DedupResult {
	if e.LastInjectAt.IsZero() {
		// Legacy entry (pre-#599 snapshot): the open was the last
		// inject, and every occurrence so far was before it.
		e.LastInjectAt = e.FirstSeen
		e.InjectCount = e.Count
	}
	inRepeat := e.LastInjectAt.After(e.FirstSeen)
	e.Count++
	e.LastSeen = now
	e.EventLastTS = eventLastTS
	if e.noteClass(class) && now.Sub(e.FirstSeen) > c.window {
		// A class first seen within a window of the open is part of the
		// picture the open described (kubelet's pull cycle puts four
		// messages on one key within seconds); only a later one is
		// news, held until announced (see the PendingChange floor).
		e.PendingChange = true
	}
	result := DedupResult{Kind: DedupRepeat, SessionID: e.SessionID, Storm: e.Storm}
	if e.Undelivered {
		e.Undelivered = false
		result.Change = e.LastChange
		if result.Change == "" && e.PendingChange {
			// The undelivered reminder is stale: a class change arrived
			// since. Re-send as the change, which carries the reminder's
			// news too, rather than letting markInject swallow it.
			result.Change = RepeatChangeClass
			e.RepeatCount = 0
			e.LastChange = RepeatChangeClass
		}
		if result.Change == "" {
			result.RepeatCount = e.RepeatCount
		}
		e.markInject(now)
		result.Count = e.Count
		return result
	}
	sinceInject := now.Sub(e.LastInjectAt)
	next := e.RepeatCount + 1
	interval := repeatInterval(c.window, next)
	dup := DedupResult{Kind: DedupDuplicate, SessionID: e.SessionID, Count: e.Count, Storm: e.Storm}
	if inRepeat && e.countJump(now) {
		// A rate jump has its own floor (repeatJumpMin) and goes in at
		// once, carrying any held class change with it. The jumped
		// rate is the new normal: measure the next jump against it,
		// not against the quiet rate it replaced.
		e.RatePerHour = e.rateSinceInject(now)
		e.RepeatCount = 0
		e.LastChange = RepeatChangeRate
		e.markInject(now)
		result.Change = RepeatChangeRate
		result.Count = e.Count
		return result
	}
	if e.PendingChange {
		// A class change is announced no sooner than the later of two
		// windows and the next reminder's interval after the last
		// inject, so an incident rotating through shapes can never
		// inject more often than its reminder schedule would. Until
		// then it is held — and it supersedes a reminder that comes
		// due meanwhile, so that reminder cannot swallow it.
		if sinceInject > max(2*c.window, interval) {
			e.RepeatCount = 0
			e.LastChange = RepeatChangeClass
			e.markInject(now)
			result.Change = RepeatChangeClass
			result.Count = e.Count
			return result
		}
		dup.Absorbed = inRepeat || lapsed
		return dup
	}
	if !inRepeat && !lapsed {
		// Still the first window: an ordinary duplicate.
		return dup
	}
	if sinceInject <= interval {
		dup.Absorbed = true
		return dup
	}
	e.RatePerHour = e.rateSinceInject(now)
	e.RepeatCount = next
	e.LastChange = ""
	e.markInject(now)
	result.RepeatCount = next
	result.Count = e.Count
	return result
}

// markInject records that the incident just spoke into its session;
// whatever was pending is in that inject.
func (e *dedupEntry) markInject(now time.Time) {
	e.LastInjectAt = now
	e.InjectCount = e.Count
	e.PendingChange = false
}

// rateSinceInject is the occurrence rate, per hour, since the last
// inject; zero when no time has passed.
func (e *dedupEntry) rateSinceInject(now time.Time) float64 {
	hours := now.Sub(e.LastInjectAt).Hours()
	if hours <= 0 {
		return 0
	}
	return float64(e.Count-e.InjectCount) / hours
}

// countJump reports a count jump since the last inject (see
// repeatJumpFactor). Needs a reference rate, so it is inert until the
// first reminder has measured one.
func (e *dedupEntry) countJump(now time.Time) bool {
	if e.RatePerHour <= 0 || e.Count-e.InjectCount < repeatJumpMin {
		return false
	}
	if !now.After(e.LastInjectAt) {
		return true // repeatJumpMin occurrences in no time at all
	}
	return e.rateSinceInject(now) >= repeatJumpFactor*e.RatePerHour
}

// noteClass adds class to the incident's known classes and reports
// whether it is a material change: new to an entry that already knew
// some. An entry restored from a pre-#599 snapshot knows none and
// adopts its first class silently.
func (e *dedupEntry) noteClass(class string) bool {
	for _, known := range e.Classes {
		if known == class {
			return false
		}
	}
	changed := len(e.Classes) > 0
	e.Classes = append(e.Classes, class)
	if len(e.Classes) > maxRepeatClasses {
		e.Classes = e.Classes[len(e.Classes)-maxRepeatClasses:]
	}
	return changed
}

// repeatClass is a signal's class for the material-change test: its
// kind, its severity and the shape of its message, hashed. The reason
// is not an input because it is already the key — a different reason
// family is a different incident, and the raw spellings inside one
// family (ErrImagePull, ImagePullBackOff) are kubelet's retry cycle,
// not a change. Only the hash is persisted, never message text.
func repeatClass(kind string, sev Severity, message string) string {
	h := fnv.New64a()
	h.Write([]byte(kind))
	h.Write([]byte{0})
	h.Write([]byte(sev))
	h.Write([]byte{0})
	h.Write([]byte(messageShape(message)))
	return strconv.FormatUint(h.Sum64(), 16)
}

// messageShape is a message with every run of digits replaced by a
// single '#': the counts, durations, ages and timestamps a repeating
// event varies ("0/6 nodes are available: 3 node(s) didn't match …",
// "back-off 5m0s", "pending for 12m") read as one shape, while a new
// clause — a different scheduling predicate failing, a different
// registry error — is a new one.
func messageShape(msg string) string {
	var b strings.Builder
	b.Grow(len(msg))
	digits := false
	for i := 0; i < len(msg); i++ {
		ch := msg[i]
		if ch >= '0' && ch <= '9' {
			if !digits {
				b.WriteByte('#')
			}
			digits = true
			continue
		}
		digits = false
		b.WriteByte(ch)
	}
	return b.String()
}
