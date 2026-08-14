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

package exempt

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// at is the instant every test binds its Set to.
var at = time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)

// write drops a file in a temp dir and returns its path.
func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "exemptions.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

// load loads a fixture that is expected to be valid.
func load(t *testing.T, body string) *Set {
	t.Helper()
	s, err := Load(write(t, body), at)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return s
}

func TestExemptMatchGrain(t *testing.T) {
	t.Parallel()
	s := load(t, `
exemptions:
  - kind: audit.no_pdb
    namespace: batch
    reason: batch jobs are restartable by design
    expires: 2026-12-01
  - kind: audit.no_pdb
    namespace: prod
    name: legacy-api
    reason: single-replica vendor appliance, replacement tracked in PLAT-8812
    expires: 2026-10-15
    owner: platform
`)
	cases := []struct {
		name                 string
		kind, namespace, obj string
		wantOK               bool
		wantReasonSubstring  string
	}{
		{name: "namespace-scoped entry covers any object in it", kind: "audit.no_pdb", namespace: "batch", obj: "nightly-etl", wantOK: true, wantReasonSubstring: "restartable"},
		{name: "object-scoped entry covers exactly its object", kind: "audit.no_pdb", namespace: "prod", obj: "legacy-api", wantOK: true, wantReasonSubstring: "PLAT-8812"},
		{name: "sibling object in the same namespace is not covered", kind: "audit.no_pdb", namespace: "prod", obj: "checkout", wantOK: false},
		{name: "another namespace is not covered", kind: "audit.no_pdb", namespace: "staging", obj: "nightly-etl", wantOK: false},
		{name: "another kind is not covered", kind: "audit.singleton", namespace: "batch", obj: "nightly-etl", wantOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reason, expires, ok := s.Exempt(tc.kind, tc.namespace, tc.obj)
			if ok != tc.wantOK {
				t.Fatalf("Exempt(%q, %q, %q) ok = %v, want %v", tc.kind, tc.namespace, tc.obj, ok, tc.wantOK)
			}
			if !ok {
				if reason != "" || expires != "" {
					t.Errorf("no match must return empty annotations, got reason=%q expires=%q", reason, expires)
				}
				return
			}
			if !strings.Contains(reason, tc.wantReasonSubstring) {
				t.Errorf("reason = %q, want it to contain %q", reason, tc.wantReasonSubstring)
			}
			if expires == "" {
				t.Error("a match must carry its expiry so the annotation is self-describing")
			}
		})
	}
}

// A single-object entry and a namespace-wide one can both match; the
// reader must be shown the justification that actually applies.
func TestExemptMostSpecificWins(t *testing.T) {
	t.Parallel()
	s := load(t, `
exemptions:
  - kind: audit.no_pdb
    reason: cluster-wide grace period during the rollout
    expires: 2026-09-01
  - kind: audit.no_pdb
    namespace: prod
    reason: namespace-wide, prod is behind a regional LB
    expires: 2026-09-01
  - kind: audit.no_pdb
    namespace: prod
    name: legacy-api
    reason: vendor appliance
    expires: 2026-09-01
`)
	for _, tc := range []struct {
		namespace, name, want string
	}{
		{"prod", "legacy-api", "vendor appliance"},
		{"prod", "checkout", "namespace-wide, prod is behind a regional LB"},
		{"staging", "checkout", "cluster-wide grace period during the rollout"},
	} {
		reason, _, ok := s.Exempt("audit.no_pdb", tc.namespace, tc.name)
		if !ok {
			t.Fatalf("%s/%s: expected a match", tc.namespace, tc.name)
		}
		if reason != tc.want {
			t.Errorf("%s/%s: reason = %q, want %q", tc.namespace, tc.name, reason, tc.want)
		}
	}
}

// Expiry is the load-bearing property: a lapsed entry stops annotating,
// silently and without erroring, and the finding emits unqualified.
func TestExemptExpiry(t *testing.T) {
	t.Parallel()
	s := load(t, `
exemptions:
  - kind: audit.live
    reason: still current
    expires: 2026-09-01
  - kind: audit.lapsed
    reason: lapsed last month
    expires: 2026-07-01
  - kind: audit.today
    reason: expires at the start of today
    expires: 2026-08-14
`)
	if _, _, ok := s.Exempt("audit.live", "", ""); !ok {
		t.Error("an entry expiring in the future must match")
	}
	if _, _, ok := s.Exempt("audit.lapsed", "", ""); ok {
		t.Error("an expired entry must not match")
	}
	// A bare date resolves to the START of that UTC day, so an entry
	// dated today has already lapsed by noon.
	if _, _, ok := s.Exempt("audit.today", "", ""); ok {
		t.Error("a bare date means 00:00Z that day; it must be inactive throughout the day itself")
	}
	// Expired entries are still LOADED — `audit exemptions` reports on
	// them, which is the mechanism that keeps the file from rotting.
	if got := len(s.Entries()); got != 3 {
		t.Errorf("Entries() = %d, want all 3 including the expired one", got)
	}
}

func TestExpiryFormats(t *testing.T) {
	t.Parallel()
	s := load(t, `
exemptions:
  - kind: audit.date
    reason: bare date
    expires: 2026-12-31
  - kind: audit.stamp
    reason: full timestamp
    expires: 2026-12-31T18:30:00Z
  - kind: audit.offset
    reason: non-UTC offset
    expires: 2026-12-31T18:30:00+02:00
`)
	byKind := map[string]time.Time{}
	for _, e := range s.Entries() {
		byKind[e.Kind] = e.ExpiresAt()
	}
	want := map[string]time.Time{
		"audit.date":   time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
		"audit.stamp":  time.Date(2026, 12, 31, 18, 30, 0, 0, time.UTC),
		"audit.offset": time.Date(2026, 12, 31, 16, 30, 0, 0, time.UTC),
	}
	for kind, w := range want {
		if got := byKind[kind]; !got.Equal(w) {
			t.Errorf("%s expires at %s, want %s", kind, got, w)
		}
	}
}

// Validation is strict and TOTAL: one bad entry fails the load. A
// silently-skipped exemption is worse than no file at all, because the
// operator believes a finding is annotated when it is not.
func TestLoadRejects(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, body, wantSubstring string
	}{
		{
			name:          "missing kind",
			body:          "exemptions:\n  - reason: why\n    expires: 2026-12-01\n",
			wantSubstring: "no kind",
		},
		{
			name:          "missing reason",
			body:          "exemptions:\n  - kind: audit.no_pdb\n    expires: 2026-12-01\n",
			wantSubstring: "no reason",
		},
		{
			name:          "missing expires",
			body:          "exemptions:\n  - kind: audit.no_pdb\n    reason: why\n",
			wantSubstring: "no expires",
		},
		{
			name:          "blank reason",
			body:          "exemptions:\n  - kind: audit.no_pdb\n    reason: \"   \"\n    expires: 2026-12-01\n",
			wantSubstring: "no reason",
		},
		{
			name:          "unparseable expiry",
			body:          "exemptions:\n  - kind: audit.no_pdb\n    reason: why\n    expires: next quarter\n",
			wantSubstring: "neither YYYY-MM-DD nor RFC3339",
		},
		{
			name:          "typo'd key is not silently ignored",
			body:          "exemptions:\n  - kind: audit.no_pdb\n    reason: why\n    expiry: 2026-12-01\n",
			wantSubstring: "parse exemptions",
		},
		{
			name:          "not a list",
			body:          "exemptions: audit.no_pdb\n",
			wantSubstring: "parse exemptions",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := Load(write(t, tc.body), at)
			if err == nil {
				t.Fatalf("Load succeeded, want an error (got %d entries)", len(s.Entries()))
			}
			if !strings.Contains(err.Error(), tc.wantSubstring) {
				t.Errorf("error = %q, want it to mention %q", err, tc.wantSubstring)
			}
		})
	}
}

// The failing entry is identified by position AND kind: a file with
// thirty entries needs to say which one.
func TestLoadErrorLocatesTheEntry(t *testing.T) {
	t.Parallel()
	_, err := Load(write(t, `
exemptions:
  - kind: audit.first
    reason: fine
    expires: 2026-12-01
  - kind: audit.second
    reason: fine
    expires: 2026-12-01
  - kind: audit.third
    expires: 2026-12-01
`), at)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "entry 3") || !strings.Contains(err.Error(), "audit.third") {
		t.Errorf("error = %q, want it to name entry 3 (audit.third)", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	t.Parallel()
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml"), at); err == nil {
		t.Fatal("Load of a missing file must fail — an exemption file the operator named but that is not there is not the same as no exemptions")
	}
}

func TestLoadEmptyFile(t *testing.T) {
	t.Parallel()
	s := load(t, "")
	if got := len(s.Entries()); got != 0 {
		t.Errorf("Entries() = %d, want 0", got)
	}
	if _, _, ok := s.Exempt("audit.no_pdb", "prod", "api"); ok {
		t.Error("an empty file must match nothing")
	}
}

// A nil *Set is what every command without --exemptions carries. It
// must behave exactly as the surface did before this package existed.
func TestNilSetMatchesNothing(t *testing.T) {
	t.Parallel()
	var s *Set
	if _, _, ok := s.Exempt("audit.no_pdb", "prod", "api"); ok {
		t.Error("nil Set matched")
	}
	if s.Entries() != nil {
		t.Error("nil Set returned entries")
	}
	if !s.Now().IsZero() {
		t.Error("nil Set returned a clock")
	}
	if s.Expired(Entry{}) {
		t.Error("nil Set reported an entry expired")
	}
}

// Entries() is sorted by expiry so `audit exemptions` output is stable
// and reads soonest-first.
func TestEntriesSortedByExpiry(t *testing.T) {
	t.Parallel()
	s := load(t, `
exemptions:
  - kind: audit.c
    reason: last
    expires: 2027-01-01
  - kind: audit.a
    reason: first
    expires: 2026-06-01
  - kind: audit.b
    reason: middle
    expires: 2026-09-01
`)
	var got []string
	for _, e := range s.Entries() {
		got = append(got, e.Kind)
	}
	want := []string{"audit.a", "audit.b", "audit.c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Entries() order = %v, want %v", got, want)
		}
	}
	if !s.Expired(s.Entries()[0]) {
		t.Error("audit.a expired 2026-06-01 and the set is bound to 2026-08-14")
	}
	if s.Expired(s.Entries()[2]) {
		t.Error("audit.c expires 2027-01-01 and must be live")
	}
}

func TestSubjectRendering(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		entry Entry
		want  string
	}{
		{Entry{Kind: "audit.no_pdb"}, "audit.no_pdb"},
		{Entry{Kind: "audit.no_pdb", Namespace: "prod"}, "audit.no_pdb in prod"},
		{Entry{Kind: "audit.no_pdb", Namespace: "prod", Name: "api"}, "audit.no_pdb on prod/api"},
		// A name without a namespace is legal and matches that name in
		// any namespace; the rendering must not imply otherwise.
		{Entry{Kind: "audit.no_pdb", Name: "api"}, "audit.no_pdb"},
	} {
		if got := tc.entry.Subject(); got != tc.want {
			t.Errorf("Subject() = %q, want %q", got, tc.want)
		}
	}
}

// Names are matched exactly: no generated-suffix normalization, no
// globbing. An exemption names what a reviewer actually approved.
func TestNamesMatchExactly(t *testing.T) {
	t.Parallel()
	s := load(t, `
exemptions:
  - kind: audit.no_pdb
    namespace: prod
    name: api
    reason: approved for this object
    expires: 2026-12-01
`)
	for _, name := range []string{"api-7d9f8", "api-canary", "apiserver", "AP I"} {
		if _, _, ok := s.Exempt("audit.no_pdb", "prod", name); ok {
			t.Errorf("name %q must not match the exempted name \"api\"", name)
		}
	}
	if _, _, ok := s.Exempt("audit.no_pdb", "prod", "api"); !ok {
		t.Error("the exact name must match")
	}
}
