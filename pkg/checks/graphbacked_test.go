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

package checks

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func graphBackedCmd() Command {
	return Command{
		Name:        "triage radius-probe",
		MCPName:     "k8s_triage_radius_probe",
		Summary:     "Synthetic graph-backed command for §6.6 flag tests.",
		GraphBacked: true,
		Hidden:      true,
		Kinds:       []KindField{Kind("test.finding", "synthetic; test scaffolding only", emit.SeverityInfo)},
		Run:         func(ctx context.Context, inv emit.Invocation) (int, error) { return 0, nil },
	}
}

// TestGraphBacked_ValidateReservesHistoryFlags: a graph-backed
// command cannot declare its own --at/--store; a live-only command
// still can.
func TestGraphBacked_ValidateReservesHistoryFlags(t *testing.T) {
	c := graphBackedCmd()
	if err := c.Validate(); err != nil {
		t.Fatalf("plain graph-backed command must validate: %v", err)
	}
	c.Flags = []emit.FlagSpec{{Name: "at", Type: emit.FlagString, Help: "clash"}}
	if err := c.Validate(); err == nil {
		t.Error("graph-backed command shadowing --at must fail validation")
	}
	live := graphBackedCmd()
	live.GraphBacked = false
	live.Flags = []emit.FlagSpec{{Name: "at", Type: emit.FlagString, Help: "own flag"}}
	if err := live.Validate(); err != nil {
		t.Errorf("live-only command may own --at: %v", err)
	}
}

// TestGraphBacked_HelpAndRunConfig: --help documents the §6.6 flags
// exactly for graph-backed commands, and RunConfig carries the bit
// so emit.Run registers them.
func TestGraphBacked_HelpAndRunConfig(t *testing.T) {
	c := graphBackedCmd()
	help := c.Help()
	if !strings.Contains(help, "Graph history flags") || !strings.Contains(help, "--at=") {
		t.Errorf("graph-backed --help must document --at, got:\n%s", help)
	}
	live := graphBackedCmd()
	live.GraphBacked = false
	if h := live.Help(); strings.Contains(h, "--at=") || strings.Contains(h, "Graph history flags") {
		t.Errorf("live-only --help must not advertise --at, got:\n%s", h)
	}
	if rc := c.RunConfig(io.Discard, io.Discard); !rc.GraphBacked {
		t.Error("RunConfig must carry GraphBacked")
	}
}
