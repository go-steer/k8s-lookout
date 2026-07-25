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

import (
	"context"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// TestInputSchema_GraphBackedGetsHistoryFlags: the MCP schema is
// derived from the same metadata as --help, so graph-backed commands
// expose at/store properties and live-only commands do not (§4.4.3:
// one source, no drift).
func TestInputSchema_GraphBackedGetsHistoryFlags(t *testing.T) {
	t.Parallel()
	base := checks.Command{
		Name:    "fake-graph",
		MCPName: "k8s_fake_graph",
		Summary: "Synthetic graph-backed command; test scaffolding only.",
		Run:     func(context.Context, emit.Invocation) (int, error) { return 0, nil },
	}

	live := inputSchema(base)
	for _, prop := range []string{"at", "store"} {
		if _, ok := live.Properties[prop]; ok {
			t.Errorf("live-only schema must not expose %q", prop)
		}
	}

	gb := base
	gb.GraphBacked = true
	schema := inputSchema(gb)
	for _, prop := range []string{"at", "store"} {
		if _, ok := schema.Properties[prop]; !ok {
			t.Errorf("graph-backed schema missing %q", prop)
		}
	}

	// argv mapping accepts the properties and produces the flags.
	args, err := argv(gb, []byte(`{"at":"20m","store":"/var/lib/lookout/lookout.db"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--at=20m", "--store=/var/lib/lookout/lookout.db"}
	if len(args) != len(want) || args[0] != want[0] || args[1] != want[1] {
		t.Errorf("argv = %v, want %v", args, want)
	}
	// ...and rejects them for live-only commands.
	if _, err := argv(base, []byte(`{"at":"20m"}`)); err == nil {
		t.Error("live-only argv must reject the at property")
	}
}
