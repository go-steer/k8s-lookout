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

import (
	"encoding/json"
	"slices"
)

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

// FitPayload fits any Open or Append wire payload (by value or pointer)
// within maxBytes, returning a fitted value and the list of shed fields.
// When maxBytes <= 0 or payload already fits, payload is returned unchanged
// with a nil shed slice.
func FitPayload(payload any, maxBytes int) (any, []string) {
	if maxBytes <= 0 {
		return payload, nil
	}
	switch p := payload.(type) {
	case Payload:
		shed := p.FitTo(maxBytes)
		return p, shed
	case *Payload:
		if p == nil {
			return payload, nil
		}
		cp := *p
		shed := cp.FitTo(maxBytes)
		return &cp, shed
	case StormPayload:
		shed := p.FitTo(maxBytes)
		return p, shed
	case *StormPayload:
		if p == nil {
			return payload, nil
		}
		cp := *p
		shed := cp.FitTo(maxBytes)
		return &cp, shed
	case WatchboardDigestPayload:
		shed := p.FitTo(maxBytes)
		return p, shed
	case *WatchboardDigestPayload:
		if p == nil {
			return payload, nil
		}
		cp := *p
		shed := cp.FitTo(maxBytes)
		return &cp, shed
	case ResolvedPayload:
		shed := p.FitTo(maxBytes)
		return p, shed
	case *ResolvedPayload:
		if p == nil {
			return payload, nil
		}
		cp := *p
		shed := cp.FitTo(maxBytes)
		return &cp, shed
	case TriageRegressedPayload:
		shed := p.FitTo(maxBytes)
		return p, shed
	case *TriageRegressedPayload:
		if p == nil {
			return payload, nil
		}
		cp := *p
		shed := cp.FitTo(maxBytes)
		return &cp, shed
	case StormMemberPayload:
		shed := p.FitTo(maxBytes)
		return p, shed
	case *StormMemberPayload:
		if p == nil {
			return payload, nil
		}
		cp := *p
		shed := cp.FitTo(maxBytes)
		return &cp, shed
	case StormUpdatePayload:
		shed := p.FitTo(maxBytes)
		return p, shed
	case *StormUpdatePayload:
		if p == nil {
			return payload, nil
		}
		cp := *p
		shed := cp.FitTo(maxBytes)
		return &cp, shed
	case FamilyMemberPayload:
		shed := p.FitTo(maxBytes)
		return p, shed
	case *FamilyMemberPayload:
		if p == nil {
			return payload, nil
		}
		cp := *p
		shed := cp.FitTo(maxBytes)
		return &cp, shed
	case WatchboardRotatedPayload:
		shed := p.FitTo(maxBytes)
		return p, shed
	case *WatchboardRotatedPayload:
		if p == nil {
			return payload, nil
		}
		cp := *p
		shed := cp.FitTo(maxBytes)
		return &cp, shed
	default:
		return payload, nil
	}
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
//  2. bound Context.Labels if labels alone force the payload over ceiling,
//     preserving a modest Message.
//  3. truncate Message to fit, with a … marker.
//  4. bound Context.Labels if still over ceiling.
//
// Identity and routing fields (reason, uid, fingerprint, cluster,
// controller_ref, node, counts, timestamps) are never touched: a shrunk
// incident still routes, dedups, and closes.
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
	if len(p.Context.Labels) > 0 {
		// Check whether even a empty/short Message leaves the payload over
		// maxBytes because Context.Labels itself is oversized.
		origMsg := p.Message
		if len(p.Message) > 128 {
			p.Message = p.Message[:128]
		}
		labelsOverflow := WireSize(p) > maxBytes
		p.Message = origMsg
		if labelsOverflow {
			if shrinkLabels(p.Context.Labels, maxBytes,
				func(m map[string]string) { p.Context.Labels = m },
				func() int {
					saved := p.Message
					if len(p.Message) > 128 {
						p.Message = p.Message[:128]
					}
					sz := WireSize(p)
					p.Message = saved
					return sz
				}) {
				shed = append(shed, "labels")
			}
			if WireSize(p) <= maxBytes {
				return shed
			}
		}
	}
	if p.Message != "" {
		if shrinkText(p.Message, maxBytes,
			func(s string) { p.Message = s },
			func() int { return WireSize(p) }) {
			shed = append(shed, "message")
		}
		if WireSize(p) <= maxBytes {
			return shed
		}
	}
	if len(p.Context.Labels) > 0 {
		if shrinkLabels(p.Context.Labels, maxBytes,
			func(m map[string]string) { p.Context.Labels = m },
			func() int { return WireSize(p) }) {
			if !slices.Contains(shed, "labels") {
				shed = append(shed, "labels")
			}
		}
	}
	return shed
}

// FitTo is the StormPayload counterpart of Payload.FitTo.
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
	if len(p.Context.Labels) > 0 {
		if shrinkLabels(p.Context.Labels, maxBytes,
			func(m map[string]string) { p.Context.Labels = m },
			func() int { return WireSize(p) }) {
			shed = append(shed, "labels")
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
// OLDEST entries, returning what it shed (issue #337).
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

// FitTo bounds a ResolvedPayload under maxBytes by shrinking Context.Labels
// while keeping all outcome identity fields intact.
func (p *ResolvedPayload) FitTo(maxBytes int) []string {
	if WireSize(p) <= maxBytes {
		return nil
	}
	if len(p.Context.Labels) > 0 {
		if shrinkLabels(p.Context.Labels, maxBytes,
			func(m map[string]string) { p.Context.Labels = m },
			func() int { return WireSize(p) }) {
			return []string{"labels"}
		}
	}
	return nil
}

// FitTo bounds a TriageRegressedPayload under maxBytes by shrinking
// Context.Labels and truncating Message while keeping routing identity.
func (p *TriageRegressedPayload) FitTo(maxBytes int) []string {
	if WireSize(p) <= maxBytes {
		return nil
	}
	var shed []string
	if len(p.Context.Labels) > 0 {
		if shrinkLabels(p.Context.Labels, maxBytes,
			func(m map[string]string) { p.Context.Labels = m },
			func() int { return WireSize(p) }) {
			shed = append(shed, "labels")
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

// FitTo bounds a StormMemberPayload under maxBytes by truncating Message.
func (p *StormMemberPayload) FitTo(maxBytes int) []string {
	if WireSize(p) <= maxBytes {
		return nil
	}
	if p.Message != "" {
		if shrinkText(p.Message, maxBytes,
			func(s string) { p.Message = s },
			func() int { return WireSize(p) }) {
			return []string{"message"}
		}
	}
	return nil
}

// FitTo bounds a StormUpdatePayload under maxBytes by truncating Message.
func (p *StormUpdatePayload) FitTo(maxBytes int) []string {
	if WireSize(p) <= maxBytes {
		return nil
	}
	if p.Message != "" {
		if shrinkText(p.Message, maxBytes,
			func(s string) { p.Message = s },
			func() int { return WireSize(p) }) {
			return []string{"message"}
		}
	}
	return nil
}

// FitTo bounds a FamilyMemberPayload under maxBytes by truncating Message.
func (p *FamilyMemberPayload) FitTo(maxBytes int) []string {
	if WireSize(p) <= maxBytes {
		return nil
	}
	if p.Message != "" {
		if shrinkText(p.Message, maxBytes,
			func(s string) { p.Message = s },
			func() int { return WireSize(p) }) {
			return []string{"message"}
		}
	}
	return nil
}

// FitTo is a no-op on WatchboardRotatedPayload (fixed small lineage fields).
func (p *WatchboardRotatedPayload) FitTo(_ int) []string {
	return nil
}

// shrinkLabels deterministically trims map entries (keeping sorted keys
// prefix) so size() <= maxBytes, returning true if any entry was shed.
func shrinkLabels(labels map[string]string, maxBytes int, set func(map[string]string), size func() int) bool {
	if len(labels) == 0 {
		return false
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	build := func(n int) map[string]string {
		if n <= 0 {
			return nil
		}
		m := make(map[string]string, n)
		for _, k := range keys[:n] {
			m[k] = labels[k]
		}
		return m
	}

	lo, hi, best := 0, len(keys), -1
	for lo <= hi {
		mid := (lo + hi) / 2
		set(build(mid))
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
	set(build(best))
	return best < len(keys)
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
