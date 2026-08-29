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

package inject

import "encoding/json"

// MaxInjectBytes is the core-agent daemon's per-inject request-body
// ceiling: POST /sessions/<sid>/inject rejects a larger body with 400
// "request body too large (max 8192 bytes)". It is a DAEMON contract —
// nothing in this repo enforces it — kept here as the default the
// dispatcher fits payloads to (overridable via --inject-max-bytes, so
// operators can track the daemon without a rebuild). Without the fit an
// over-limit inject is dropped whole and a new incident lands as an
// empty session (issue #198).
const MaxInjectBytes = 8192

// truncMarker terminates a Message that FitTo shortened, so a reader
// (and the playbook) sees the text was cut rather than genuinely ending
// there.
const truncMarker = "…[truncated]"

// WireSize returns the exact number of bytes injectJSON POSTs for
// payload: the payload marshaled to JSON, wrapped as a string in the
// {"message":"…"} envelope, and marshaled again — the double-encoding
// the daemon's inject endpoint expects, and the byte count its
// body-size limit measures. Returns -1 if payload cannot marshal (it
// would never reach the wire).
func WireSize(payload any) int {
	body, err := json.Marshal(payload)
	if err != nil {
		return -1
	}
	wrapped, err := json.Marshal(injectMessageRequest{Message: string(body)})
	if err != nil {
		return -1
	}
	return len(wrapped)
}

// FitTo shrinks the payload in place so its WireSize is <= maxBytes,
// returning a short list of what it shed (nil when the payload already
// fit, so the frozen-wire pins stay byte-identical on normal payloads).
// The daemon drops an over-limit inject entirely (issue #198), so a
// lossy-but-delivered incident beats a silent empty session. Shrink
// order is least-signal-first:
//
//  1. drop Enrichment — the §7.6 warm-session bundle, best-effort and
//     reproducible via the `lookout` commands its overflow trailers name.
//  2. truncate Message to fit, with a … marker.
//
// Identity and routing fields (reason, uid, fingerprint, cluster,
// context, counts, timestamps) are never touched: a shrunk incident
// still routes, dedups, and closes. If even an empty Message won't fit
// (identity alone exceeds maxBytes — not reachable with a sane
// --inject-max-bytes) FitTo leaves the smallest form it produced and
// lets the caller POST it, so the daemon's own error stays the honest
// signal.
func (p *Payload) FitTo(maxBytes int) []string {
	if WireSize(p) <= maxBytes {
		return nil
	}
	var shed []string
	if p.Enrichment != nil {
		p.Enrichment = nil
		shed = append(shed, "enrichment")
		if WireSize(p) <= maxBytes {
			return shed
		}
	}
	if p.Message != "" {
		if shrinkText(p.Message, maxBytes,
			func(s string) { p.Message = s },
			func() int { return WireSize(p) }) {
			shed = append(shed, "message")
		}
	}
	return shed
}

// FitTo is the StormPayload counterpart of Payload.FitTo: a storm's
// initial inject carries a (radius-only, so usually smaller) enrichment
// bundle and an aggregate message, and breaches the same daemon ceiling
// the same way. Same never-touch-identity guarantee, with one extra
// step wedged in the middle of the shrink order (issue #336).
//
// MemberFingerprints holds one entry per member and is otherwise
// uncapped, so on a large storm it — not the enrichment, not the
// message — IS the payload. Shedding enrichment and truncating a
// ~150-byte message cannot claw back a 22 KiB body, so before #336 a
// storm past ~330 members stayed over the ceiling after a full fit, the
// daemon 400d the initial inject, and the storm landed as a
// bound-but-empty session — the exact #198 failure the guard exists to
// prevent. It is shed BEFORE Message for that reason: the message is a
// generated summary derivable from the structured fields, but shedding
// it first buys almost nothing and leaves the payload over anyway.
//
// Truncation keeps the earliest arrivals (the leading symptom, the same
// end representatives are drawn from) and sets
// MemberFingerprintsTruncated so a reader can tell a cut list from a
// short storm. AffectedCount still carries the true member count.
func (p *StormPayload) FitTo(maxBytes int) []string {
	if WireSize(p) <= maxBytes {
		return nil
	}
	var shed []string
	if p.Enrichment != nil {
		p.Enrichment = nil
		shed = append(shed, "enrichment")
		if WireSize(p) <= maxBytes {
			return shed
		}
	}
	if len(p.MemberFingerprints) > 0 {
		full := p.MemberFingerprints
		if shrinkFingerprints(full, maxBytes,
			func(fps []string) {
				p.MemberFingerprints = fps
				p.MemberFingerprintsTruncated = len(fps) < len(full)
			},
			func() int { return WireSize(p) }) {
			shed = append(shed, "member_fingerprints")
		}
		if WireSize(p) <= maxBytes {
			return shed
		}
	}
	if p.Message != "" {
		if shrinkText(p.Message, maxBytes,
			func(s string) { p.Message = s },
			func() int { return WireSize(p) }) {
			shed = append(shed, "message")
		}
	}
	return shed
}

// FitTo shrinks a watchboard digest under maxBytes by dropping its
// OLDEST entries, returning what it shed (issue #337). The digest has
// no enrichment and no prose message — its entries ARE the payload —
// so there is exactly one thing to shed.
//
// Newest-wins, the opposite of StormPayload.FitTo's keep-the-earliest:
// a storm's first arrivals are the leading symptom of one event, while
// the watchboard is a rolling board of independent warnings where the
// current state is what an operator acts on. The count dropped is
// reported in EntriesDropped rather than silently, and the entries
// were already counted in watchboard_entries_total.
//
// Without this the digest went to the wire unfitted: a
// --watchboard-batch past roughly 22 (or fewer, with long
// namespace/name pairs) made every flush 400 and drop its whole
// buffer, permanently, from a legal flag value.
func (p *WatchboardDigestPayload) FitTo(maxBytes int) []string {
	if WireSize(p) <= maxBytes {
		return nil
	}
	if len(p.Entries) == 0 {
		return nil
	}
	full := p.Entries
	// Binary-search the largest SUFFIX (the newest k entries) that fits.
	lo, hi, best := 0, len(full), -1
	for lo <= hi {
		mid := (lo + hi) / 2
		p.Entries = full[len(full)-mid:]
		p.EntriesDropped = len(full) - mid
		if WireSize(p) <= maxBytes {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if best < 0 {
		best = 0
	}
	p.Entries = full[len(full)-best:]
	p.EntriesDropped = len(full) - best
	if p.EntriesDropped == 0 {
		return nil
	}
	return []string{"entries"}
}

// shrinkText binary-searches the largest rune prefix of text that, once
// written back through set and re-measured through size, keeps the wire
// within maxBytes, appending truncMarker to any text it shortened. The
// double-JSON escaping makes a rune's byte cost nonlinear, so it
// converges by measurement (set/size reflect the surrounding payload)
// rather than arithmetic. Reports whether it changed the text.
// Rune-sliced so it never cuts a multi-byte character in half.
func shrinkText(text string, maxBytes int, set func(string), size func() int) bool {
	runes := []rune(text)
	lo, hi, best := 0, len(runes), -1
	for lo <= hi {
		mid := (lo + hi) / 2
		set(string(runes[:mid]) + truncMarker)
		if size() <= maxBytes {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if best < 0 {
		// Even the marker alone won't fit — drop the text entirely and
		// let the residual (identity only) go as-is.
		set("")
		return text != ""
	}
	out := string(runes[:best]) + truncMarker
	set(out)
	return out != text
}

// shrinkFingerprints is the slice counterpart of shrinkText (issue
// #336): it binary-searches the largest PREFIX of fps that, written
// back through set and re-measured through size, keeps the wire within
// maxBytes. A prefix, not a sample, so the members kept are the
// earliest arrivals. Converges by measurement for the same reason
// shrinkText does — the double-JSON escaping makes each entry's byte
// cost nonlinear, and the surrounding payload counts too.
//
// Reports whether it dropped anything. An empty result (not even one
// fingerprint fits) is a legitimate outcome: AffectedCount survives, so
// the storm still says how big it was.
func shrinkFingerprints(fps []string, maxBytes int, set func([]string), size func() int) bool {
	lo, hi, best := 0, len(fps), -1
	for lo <= hi {
		mid := (lo + hi) / 2
		set(fps[:mid])
		if size() <= maxBytes {
			best = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if best < 0 {
		best = 0
	}
	set(fps[:best])
	return best < len(fps)
}
