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

// Package findings implements the `lookout findings` command group
// (docs/findings-diff-design.md, issue #212): the run-to-run
// transition surface an unattended agent needs so consecutive scans
// report what CHANGED rather than re-listing the same forty findings
// every fifteen minutes.
//
// Two commands, both talking to the §9.1 store:
//
//   - `findings diff` consumes a §4.2 report on stdin (or a file),
//     classifies each subject against the persisted state, emits the
//     transitions, and advances the state.
//   - `findings ack` opens a time-boxed suppression window on one
//     subject.
//
// The boundary with go-steer/mast is a WIRE contract, not a Go
// import: mast pipes a report in and reads transition records out, and
// never learns the Kubernetes-shaped schema. That is why the whole
// surface is a plain CLI/MCP command over the §4.2 envelope.
//
// This group is unusual among read-path commands in that it does not
// talk to a cluster at all: its input is another command's output.
// That is deliberate — any producer of §4.2 findings can be diffed,
// including one that does not exist yet.
package findings

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	findingstate "github.com/go-steer/k8s-lookout/pkg/findings"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

func init() {
	checks.Register(DiffCommand(Deps{}))
	checks.Register(AckCommand(Deps{}))
}

// Deps are the injectable dependencies of the findings commands. The
// zero value gives production behavior; tests inject a fixed clock.
type Deps struct {
	// Now is the clock that anchors the diff's ack boundary and an
	// ack's expiry. Nil means time.Now. It is read ONCE per
	// invocation, so every subject in one run is classified against
	// the same instant.
	Now func() time.Time
}

func (d Deps) now() time.Time {
	if d.Now == nil {
		return time.Now()
	}
	return d.Now()
}

// storeHint is the usage-error tail: this surface is durable by
// design (#212 scope), so there is no store-less mode to fall back to.
const storeHint = "finding state lives in the sentinel's --store SQLite file (§9.1); a diff with nowhere to persist would report everything new on every run"

// openReport resolves --report to a stream: "-" (the default) is
// stdin, anything else is a file path. The returned closer is nil for
// stdin — the runner owns the process stream.
func openReport(inv emit.Invocation, path string) (io.Reader, io.Closer, error) {
	if path == "" {
		return nil, nil, emit.UsageErrorf("--report is required: `-` reads the report on stdin (the usual `lookout health | lookout findings diff --report -`), or give a file path")
	}
	if path == "-" {
		return inv.In, nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open report: %w", err)
	}
	return f, f, nil
}

// openStore opens the store read-write, translating the two errors an
// operator actually hits into advice.
func openStore(path string) (*store.Store, error) {
	if path == "" {
		return nil, emit.UsageErrorf("--store is required: %s", storeHint)
	}
	st, err := store.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open store %s: %w", path, err)
	}
	return st, nil
}

// parseTransitions parses the --transitions allow-list. Empty means
// "every class", which is the honest default: a consumer that wants
// only the changed classes should say so rather than have the tool
// decide what is interesting on its behalf.
func parseTransitions(spec string) (map[findingstate.Transition]bool, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	known := map[findingstate.Transition]bool{
		findingstate.TransitionNew:        true,
		findingstate.TransitionOngoing:    true,
		findingstate.TransitionEscalated:  true,
		findingstate.TransitionResolved:   true,
		findingstate.TransitionSuppressed: true,
	}
	out := map[findingstate.Transition]bool{}
	for _, tok := range strings.Split(spec, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		t := findingstate.Transition(tok)
		if !known[t] {
			return nil, emit.UsageErrorf("--transitions=%q: %q is not a transition class (want %s)", spec, tok, transitionList())
		}
		out[t] = true
	}
	if len(out) == 0 {
		return nil, emit.UsageErrorf("--transitions=%q selects nothing", spec)
	}
	return out, nil
}

// transitionList renders the wire enum for help and error text, in
// the fixed order the docs use.
func transitionList() string {
	return strings.Join([]string{
		string(findingstate.TransitionNew),
		string(findingstate.TransitionOngoing),
		string(findingstate.TransitionEscalated),
		string(findingstate.TransitionResolved),
		string(findingstate.TransitionSuppressed),
	}, "|")
}
