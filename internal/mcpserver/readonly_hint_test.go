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

package mcpserver

// Regression drill for issue #105 (code half): New() hard-codes
// readOnly := true and stamps ReadOnlyHint:true on EVERY tool — the
// whole surface is assumed to be the read path. But k8s_triage_status
// (triage.StatusCommand) is an actual SQLite WRITE: `lookout triage
// status --status=...` upserts a triage-status record through the §9.4
// TriageWriter. A convention-following MCP client reads ReadOnlyHint
// and auto-approves the write with no operator confirmation.
//
// The invariant: a tool that mutates state must advertise
// ReadOnlyHint:false, while genuine read tools keep ReadOnlyHint:true.
// The test asserts the observable MCP annotation (what a client sees),
// not any internal marker — the fix's shape (a Writes bool on
// checks.Command, or otherwise) is the coder's choice.

import (
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/triage"
)

// readOnlyHint reports a tool's ReadOnlyHint and whether an annotation
// block is present at all.
func readOnlyHint(tool *mcp.Tool) (hint, present bool) {
	if tool == nil || tool.Annotations == nil {
		return false, false
	}
	return tool.Annotations.ReadOnlyHint, true
}

// TestReadOnlyHint_WriteToolIsNotReadOnly builds the server from the
// REAL commands (so the coder's marker on triage.StatusCommand flips
// this green) and asserts the write tool is not advertised read-only
// while a genuine read tool still is.
func TestReadOnlyHint_WriteToolIsNotReadOnly(t *testing.T) {
	reg := checks.NewRegistry()
	reg.Register(triage.StatusCommand())              // k8s_triage_status — a WRITE
	reg.Register(triage.RadiusCommand(triage.Deps{})) // k8s_blast_radius — a genuine read

	cs := connect(t, New(reg, "test"))
	byName := map[string]*mcp.Tool{}
	for _, tool := range listTools(t, cs) {
		byName[tool.Name] = tool
	}

	status, ok := byName["k8s_triage_status"]
	if !ok {
		t.Fatal("k8s_triage_status was not served as a tool")
	}
	if hint, present := readOnlyHint(status); !present || hint {
		t.Errorf("k8s_triage_status ReadOnlyHint = %v (present=%v), want false — it is a SQLite WRITE (triage-status upsert), but MCP advertises it as read-only so a convention-following client auto-approves it (issue #105)",
			hint, present)
	}

	read, ok := byName["k8s_blast_radius"]
	if !ok {
		t.Fatal("k8s_blast_radius was not served as a tool")
	}
	if hint, present := readOnlyHint(read); !present || !hint {
		t.Errorf("k8s_blast_radius ReadOnlyHint = %v (present=%v), want true — a genuine read tool must stay read-only after the write-marker fix",
			hint, present)
	}
}
