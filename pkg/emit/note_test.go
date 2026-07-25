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

package emit

import (
	"bytes"
	"testing"
	"time"
)

// TestSummaryNotes: Note keys ride the summary line after the
// mandatory three, in first-set order, last value wins, both formats
// (§6.6 "answer live-only and say so in the summary line").
func TestSummaryNotes(t *testing.T) {
	var out bytes.Buffer
	w, err := NewWriter(&out, FormatLogfmt)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Note("source", "live"); err != nil {
		t.Fatal(err)
	}
	if err := w.Note("at", "2026-07-25T10:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := w.Note("source", "history"); err != nil { // replaces in place
		t.Fatal(err)
	}
	if err := w.Summary(7, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	want := "scanned=7 findings=0 elapsed=100ms source=history at=2026-07-25T10:00:00Z\n"
	if out.String() != want {
		t.Errorf("summary = %q, want %q", out.String(), want)
	}

	out.Reset()
	w, err = NewWriter(&out, FormatJSON)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Note("source", "live"); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(3, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	wantJSON := "{\"scanned\":3,\"findings\":0,\"elapsed\":\"100ms\",\"source\":\"live\"}\n"
	if out.String() != wantJSON {
		t.Errorf("summary = %q, want %q", out.String(), wantJSON)
	}
}

// TestSummaryWithoutNotesUnchanged: no notes means the historical
// byte-exact summary — nothing downstream re-parses.
func TestSummaryWithoutNotesUnchanged(t *testing.T) {
	for format, want := range map[Format]string{
		FormatLogfmt: "scanned=5 findings=0 elapsed=1s\n",
		FormatJSON:   "{\"scanned\":5,\"findings\":0,\"elapsed\":\"1s\"}\n",
	} {
		var out bytes.Buffer
		w, err := NewWriter(&out, format)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Summary(5, time.Second); err != nil {
			t.Fatal(err)
		}
		if out.String() != want {
			t.Errorf("%s summary = %q, want %q", format, out.String(), want)
		}
	}
}

// TestNoteRejectsBadKeys: charset contract plus the mandatory keys
// themselves.
func TestNoteRejectsBadKeys(t *testing.T) {
	w, err := NewWriter(&bytes.Buffer{}, FormatLogfmt)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "Bad-Key", "scanned", "findings", "elapsed"} {
		if err := w.Note(key, "x"); err == nil {
			t.Errorf("Note(%q) must be rejected", key)
		}
	}
}
