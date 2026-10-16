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
	"strings"
	"testing"
	"time"
)

// onlyNamespace is a Visibility that shows findings in one namespace.
type onlyNamespace string

func (o onlyNamespace) Visible(ns string) bool { return ns == string(o) }

func visFinding(ns, name string) Finding {
	return Finding{Kind: "x.y", Severity: SeverityInfo, Namespace: ns, KindOfObject: "Pod", Name: name, Reason: "R"}
}

// TestWriterWithholdsWhatTheCallerCannotSee: a finding the Visibility
// refuses is not written, not counted in findings=, not seen by a
// watcher, and is counted on the summary as hidden=.
func TestWriterWithholdsWhatTheCallerCannotSee(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatLogfmt, WithVisibility(onlyNamespace("team-a")))
	if err != nil {
		t.Fatal(err)
	}
	var watched []string
	w.watchers = append(w.watchers, func(f Finding) { watched = append(watched, f.Name) })
	for _, f := range []Finding{visFinding("team-a", "mine"), visFinding("team-b", "theirs"), visFinding("", "node-1")} {
		if err := w.Emit(f); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Summary(3, time.Second); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "name=mine") || strings.Contains(out, "theirs") || strings.Contains(out, "node-1") {
		t.Errorf("visibility not applied:\n%s", out)
	}
	if !strings.Contains(out, "findings=1") || !strings.Contains(out, "hidden=2") {
		t.Errorf("summary does not account for the withheld rows:\n%s", out)
	}
	if len(watched) != 1 || watched[0] != "mine" {
		t.Errorf("a watcher saw a withheld finding: %v", watched)
	}
}

// TestWriterWithoutVisibilityHasNoHiddenField: the summary is unchanged
// for every invocation that does not run as its caller.
func TestWriterWithoutVisibilityHasNoHiddenField(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, FormatLogfmt)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Emit(visFinding("team-b", "theirs")); err != nil {
		t.Fatal(err)
	}
	if err := w.Summary(1, time.Second); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); strings.Contains(out, "hidden=") || !strings.Contains(out, "findings=1") {
		t.Errorf("a writer with no Visibility changed:\n%s", out)
	}
}

func TestHiddenIsAWriterOwnedSummaryKey(t *testing.T) {
	t.Parallel()
	found := false
	for _, k := range SummaryNoteFields() {
		found = found || k == "hidden"
	}
	if !found {
		t.Error("hidden is not declared as a Writer-owned summary key")
	}
}
