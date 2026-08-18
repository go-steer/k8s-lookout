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

package checks_test

import (
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
)

// TestCommandHelpGolden pins the generated --help text: it is an
// agent-facing surface (§4.4.1), so accidental reformatting is a
// contract change, not cosmetics.
func TestCommandHelpGolden(t *testing.T) {
	checktest.Golden(t, "testdata/demo-help.golden", checktest.DemoCommand().Help())
}

func TestGroupHelpGolden(t *testing.T) {
	r := checks.NewRegistry()
	demo := checktest.DemoCommand()
	demo.Hidden = false // visible so the listing has content
	r.Register(demo)
	logs := demo
	logs.Name = "triage logs"
	logs.MCPName = "k8s_triage_logs"
	logs.Summary = "Condense a workload's logs by template fingerprint; kubectl logs, but ~350 tokens."
	r.Register(logs)
	checktest.Golden(t, "testdata/group-help.golden", checks.GroupHelp(r, "triage"))
}

// TestHelpRunsThroughRunner proves `--help` on a mounted command
// prints the generated metadata help, stdout-only, exit 0.
func TestHelpRunsThroughRunner(t *testing.T) {
	demo := checktest.DemoCommand()
	res := checktest.Run(t, demo, "--help")
	if res.Code != 0 {
		t.Fatalf("exit = %d, stderr: %s", res.Code, res.Stderr)
	}
	if res.Stdout != demo.Help() {
		t.Errorf("--help output diverges from generated help:\n%s", res.Stdout)
	}
	if res.Stderr != "" {
		t.Errorf("stderr should be empty, got %q", res.Stderr)
	}
}
