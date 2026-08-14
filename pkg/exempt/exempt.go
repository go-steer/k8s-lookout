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

// Package exempt implements the third suppression axis (issue #234):
// an owner-declared, git-reviewed, expiring statement that a finding
// is intentional here.
//
// # Exempt must not mean absent
//
// The whole case for deterministic detectors is that SOP-driven
// auditing permits fabricated all-clears and unverifiable coverage. An
// opt-out that makes a finding DISAPPEAR reintroduces exactly that
// through the front door — "the audit found nothing" becomes
// unfalsifiable again, with the omission now living in a YAML file
// nobody reads instead of a model's transcript.
//
// So this package never removes anything. It answers one question —
// "is this finding covered by a reviewed exemption?" — and the emit
// Writer ANNOTATES the finding with the reason and expiry, counts it,
// and reports the count on the terminating summary line. Filtering is
// the consumer's job, done on data it can see.
//
// # Why a file, and why every entry expires
//
// The file is passed on the command line (`--exemptions`), which makes
// it a git artifact reviewable in a PR by someone other than its
// beneficiary. The two alternatives both fail that test: an in-cluster
// ConfigMap can be edited without review, and object annotations let
// the team requesting an exemption grant it to itself.
//
// Every entry MUST carry a reason and an expiry, and a missing one is a
// load error rather than a warning. A permanent exemption is
// indistinguishable from a check nobody wrote, which is how posture
// programs rot: the file accretes entries, nothing ever forces a
// re-read, and five years later no one can say which lines still
// describe reality. An expiry converts that from an archaeology problem
// into a calendar one — and `lookout audit exemptions` turns the
// calendar into findings.
//
// An expired entry simply stops matching. It does not annotate, it is
// not an error, and the finding it used to cover emits unqualified —
// which is the correct default, since the statement backing it has
// lapsed.
//
// # Not an ack, and not a severity override
//
// Three suppression axes now exist and must stay distinguishable:
//
//   - `findings ack` (#212) is OPERATOR-owned, transient and expiring:
//     "known, I'm on it." It lives in the sentinel's store, not in git.
//   - §9.4 `severity_override` is AGENT-owned and standing: it asserts
//     a diagnosis about how bad something is.
//   - An exemption is OWNER-owned, durable and reviewed in git:
//     "intentional here, by design." It asserts nothing about severity
//     and does not expire on its own schedule the way an ack does.
//
// An exemption is also NOT the remedy for a false positive. It says "we
// accept this finding"; a finding that is factually wrong needs the
// detector fixed.
package exempt

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// Entry is one reviewed exemption.
//
// Matching is by finding fields only — kind, namespace, name — because
// the matcher sees a finding, not a live object. Label selectors were
// considered and rejected for that reason: honoring one would mean a
// cluster read at annotation time, on every surface including the ones
// that have no cluster (`findings diff` re-reading a piped report).
type Entry struct {
	// Kind is the finding kind this entry covers, matched exactly
	// (e.g. "audit.no_pdb"). Required: an entry that matched every
	// kind would be an off switch, not an exemption.
	Kind string `json:"kind"`
	// Namespace narrows the entry to one namespace. Empty matches any
	// namespace, which is the right grain for a cluster-scoped
	// finding and the wrong one for most workload posture.
	Namespace string `json:"namespace,omitempty"`
	// Name narrows the entry to one object, matched EXACTLY — no
	// normalization, no globbing. An exemption names what a reviewer
	// actually approved; a pattern would silently widen over time as
	// new objects came to match it. For findings whose subject is a
	// pod with a generated name, the practical grain is
	// kind+namespace, and that is deliberate.
	Name string `json:"name,omitempty"`
	// Reason is why this is intentional. Required, free text, carried
	// onto the annotated finding so the justification travels with
	// the output rather than staying in a file the reader may not
	// have.
	Reason string `json:"reason"`
	// Expires is when the entry stops applying: an RFC3339 timestamp,
	// or a bare `YYYY-MM-DD` date meaning 00:00:00Z at the START of
	// that day. Required.
	Expires string `json:"expires"`
	// Owner is who to ask about it. Optional, but the field exists
	// because "expired, and nobody knows whose it was" is the
	// predictable end state of a file without one.
	Owner string `json:"owner,omitempty"`

	// expires is Expires parsed at load time.
	expires time.Time
}

// ExpiresAt returns the parsed expiry instant.
func (e Entry) ExpiresAt() time.Time { return e.expires }

// Subject renders the entry's match scope for diagnostics and
// findings: `<kind>` widened to `<kind> in <ns>` / `<kind> on
// <ns>/<name>` as it narrows.
func (e Entry) Subject() string {
	switch {
	case e.Namespace != "" && e.Name != "":
		return fmt.Sprintf("%s on %s/%s", e.Kind, e.Namespace, e.Name)
	case e.Namespace != "":
		return fmt.Sprintf("%s in %s", e.Kind, e.Namespace)
	default:
		return e.Kind
	}
}

// file is the on-disk shape. A top-level key rather than a bare list
// so the format has somewhere to grow (and so a reader can tell a
// truncated file from an empty one).
type file struct {
	Exemptions []Entry `json:"exemptions"`
}

// Set is a loaded exemption file, bound to the instant it was loaded
// at. Binding the clock once per invocation is what makes expiry
// deterministic: every finding in one scan is judged against the same
// "now", so a long scan cannot annotate its first finding and decline
// its last.
//
// The zero value and a nil *Set are usable and match nothing — a
// command invoked without --exemptions behaves exactly as it did
// before this package existed.
type Set struct {
	now     time.Time
	entries []Entry
	// byKind indexes live entries; expired ones are held in entries
	// (so `audit exemptions` can report them) but never indexed.
	byKind map[string][]Entry
}

// Load reads and validates an exemption file, binding it to now.
//
// Validation is strict and total: the first bad entry fails the load
// rather than being skipped. A silently-dropped exemption is worse
// than no exemption file, because the operator believes a finding is
// annotated when it is not.
func Load(path string, now time.Time) (*Set, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read exemptions: %w", err)
	}
	var f file
	// UnmarshalStrict so a typo'd key ("expiry:", "namespaces:") is an
	// error instead of an entry that quietly never matches.
	if err := yaml.UnmarshalStrict(raw, &f); err != nil {
		return nil, fmt.Errorf("parse exemptions %s: %w", path, err)
	}
	s := &Set{now: now, byKind: map[string][]Entry{}}
	for i := range f.Exemptions {
		e := f.Exemptions[i]
		if err := validate(&e); err != nil {
			return nil, fmt.Errorf("exemptions %s: entry %d (%s): %w", path, i+1, e.Kind, err)
		}
		s.entries = append(s.entries, e)
		if !e.expired(now) {
			s.byKind[e.Kind] = append(s.byKind[e.Kind], e)
		}
	}
	return s, nil
}

// validate enforces the two mandatory fields and parses the expiry.
func validate(e *Entry) error {
	if strings.TrimSpace(e.Kind) == "" {
		return fmt.Errorf("no kind: an exemption must name the finding kind it covers")
	}
	if strings.TrimSpace(e.Reason) == "" {
		return fmt.Errorf("no reason: an exemption without a stated justification cannot be reviewed")
	}
	if strings.TrimSpace(e.Expires) == "" {
		return fmt.Errorf("no expires: a permanent exemption is indistinguishable from a check nobody wrote")
	}
	t, err := parseExpiry(e.Expires)
	if err != nil {
		return err
	}
	e.expires = t
	return nil
}

// parseExpiry accepts a bare date or a full RFC3339 timestamp. The
// date form is what a human writes; it resolves to the START of that
// UTC day, so "expires: 2026-12-31" means the entry is inactive
// throughout 2026-12-31 — earlier rather than later, which is the safe
// direction for something that suppresses nothing but does quiet a
// reader.
func parseExpiry(s string) (time.Time, error) {
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("expires %q is neither YYYY-MM-DD nor RFC3339", s)
	}
	return t.UTC(), nil
}

func (e Entry) expired(now time.Time) bool { return !now.Before(e.expires) }

// Now returns the instant the set was bound to.
func (s *Set) Now() time.Time {
	if s == nil {
		return time.Time{}
	}
	return s.now
}

// Entries returns every loaded entry, expired ones included, sorted by
// expiry then kind. `audit exemptions` reports on this; matching does
// not use it.
func (s *Set) Entries() []Entry {
	if s == nil {
		return nil
	}
	out := append([]Entry(nil), s.entries...)
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].expires.Equal(out[j].expires) {
			return out[i].expires.Before(out[j].expires)
		}
		return out[i].Subject() < out[j].Subject()
	})
	return out
}

// Expired reports whether an entry has lapsed as of the set's instant.
func (s *Set) Expired(e Entry) bool {
	if s == nil {
		return false
	}
	return e.expired(s.now)
}

// Exempt implements emit.Exempter: it reports whether a live entry
// covers a finding, and returns the reason and expiry to annotate it
// with.
//
// The most specific matching entry wins, so a namespace-wide exemption
// and a single-object one can coexist and the reader sees the
// justification that actually applies. Specificity is
// name-then-namespace; ties break on the reason text so the answer is
// deterministic when a file lists two entries of the same shape.
func (s *Set) Exempt(kind, namespace, name string) (reason, expires string, ok bool) {
	if s == nil {
		return "", "", false
	}
	var best *Entry
	for i := range s.byKind[kind] {
		e := &s.byKind[kind][i]
		if e.Namespace != "" && e.Namespace != namespace {
			continue
		}
		if e.Name != "" && e.Name != name {
			continue
		}
		if best == nil || moreSpecific(*e, *best) {
			best = e
		}
	}
	if best == nil {
		return "", "", false
	}
	return best.Reason, best.Expires, true
}

// moreSpecific ranks two matching entries: a name beats no name, a
// namespace beats no namespace, and the reason text is the tiebreak.
func moreSpecific(a, b Entry) bool {
	if (a.Name != "") != (b.Name != "") {
		return a.Name != ""
	}
	if (a.Namespace != "") != (b.Namespace != "") {
		return a.Namespace != ""
	}
	return a.Reason < b.Reason
}
