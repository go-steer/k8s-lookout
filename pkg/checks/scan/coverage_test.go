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

package scan_test

import (
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/scan"

	_ "github.com/go-steer/k8s-lookout/pkg/checks/all"
)

// TestScanCoversEveryRegisteredCommand is the guardian: adding a check
// without deciding whether a bare `lookout scan` should run it fails
// CI. Every visible command in the default registry has to be in
// scan's stage-1 set, in an opt-in group, or in the exclusion table
// with a stated reason.
//
// This test is the entire reason scan will not rot the way `bundle`
// and `health` did — both compose a hand-written list of Go calls, and
// neither has picked up a check added since it was written. There is
// no way to be quietly absent from scan.
func TestScanCoversEveryRegisteredCommand(t *testing.T) {
	reg := checks.Default()

	// Where each command is run from, if it is run at all.
	runFrom := map[string]string{}
	for _, name := range scan.Stage1 {
		runFrom[name] = "scan's stage-1 set"
	}
	for _, group := range scan.OptionalGroups {
		for _, c := range reg.GroupCommands(group) {
			if _, excluded := scan.Excluded[c.Name]; excluded {
				continue
			}
			if runFrom[c.Name] == "" {
				runFrom[c.Name] = "--include=" + group
			}
		}
	}

	for _, c := range reg.All() {
		if c.Hidden {
			continue
		}
		reason, isExcluded := scan.Excluded[c.Name]
		where := runFrom[c.Name]
		switch {
		case where == "" && !isExcluded:
			t.Errorf("command %q is neither run by `lookout scan` nor excluded from it.\n"+
				"\tDecide, in pkg/checks/scan/scan.go:\n"+
				"\t  - a target-free INCIDENT check a bare scan should run → add it to stage1, in the order it should emit;\n"+
				"\t  - otherwise → add it to excluded with the reason a zero-argument scan does not run it.\n"+
				"\tThe reason string is read by the next contributor and by the response doc; write a sentence, not a shrug.",
				c.Name)
		case where != "" && isExcluded:
			t.Errorf("command %q is both run (%s) and excluded (%q) — pick one", c.Name, where, reason)
		case isExcluded && strings.TrimSpace(reason) == "":
			t.Errorf("command %q is excluded from scan with an empty reason", c.Name)
		}
	}
}

// TestScanTablesNameRealCommands is the other direction: a renamed or
// deleted command must not leave a dangling entry behind. A stale
// stage1 entry is the worse of the two — scan skips names it cannot
// resolve, so the check would silently stop running.
func TestScanTablesNameRealCommands(t *testing.T) {
	reg := checks.Default()
	check := func(table, name string) {
		t.Helper()
		c, ok := reg.Lookup(name)
		if !ok {
			t.Errorf("%s names %q, which is not a registered command", table, name)
			return
		}
		if c.Hidden {
			t.Errorf("%s names %q, which is hidden — hidden commands are test scaffolding", table, name)
		}
	}
	for _, name := range scan.Stage1 {
		check("scan's stage1", name)
	}
	for name := range scan.Excluded {
		check("scan's exclusion table", name)
	}
	for _, group := range scan.OptionalGroups {
		if checks.GroupSummary(group) == "" {
			t.Errorf("scan's optionalGroups names %q, which is not a §4.1 group", group)
		}
		if len(reg.GroupCommands(group)) == 0 {
			t.Errorf("scan's optionalGroups names %q, which has no visible commands", group)
		}
	}
}

// TestScanGlossaryCoversItsStages: scan emits its stages' findings
// verbatim, so its output glossary must be the union of theirs. It is
// built from the registry rather than by hand, and this asserts the
// build actually happened — a glossary that is only scan's own six
// keys would fail every stage's contract check at runtime.
func TestScanGlossaryCoversItsStages(t *testing.T) {
	reg := checks.Default()
	c, ok := reg.Lookup("scan")
	if !ok {
		t.Fatal("scan is not registered — pkg/checks/all/all.go registers it; did the init go away?")
	}
	declared := map[string]bool{}
	for _, f := range c.Output {
		declared[f.Name] = true
	}
	for _, name := range scan.Stage1 {
		stage, ok := reg.Lookup(name)
		if !ok {
			continue // reported by TestScanTablesNameRealCommands
		}
		for _, f := range stage.Output {
			if !declared[f.Name] {
				t.Errorf("stage %q declares output field %q, which scan's glossary does not carry", name, f.Name)
			}
		}
	}
}
