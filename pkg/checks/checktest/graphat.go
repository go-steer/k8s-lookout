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

package checktest

import (
	"context"
	"strconv"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// GraphAtCommand returns the hidden GraphBacked probe that proves
// the §6.6 --at plumbing end-to-end: flag registration/gating in
// emit.Run, Scope.At/Store resolution, read-only store open, and
// store.GraphAt nearest-snapshot + replay. No production command
// consumes --at yet (triage radius --at and triage changes come
// next); this is the seam they will use, exercised by tests. Never
// registered in the default registry.
func GraphAtCommand() checks.Command {
	return checks.Command{
		Name:        "triage graph-at",
		MCPName:     "k8s_triage_graph_at",
		Summary:     "Resolve the topology graph as of --at from a sentinel store; test scaffolding only.",
		Hidden:      true,
		GraphBacked: true,
		Output: []checks.OutputField{
			{Name: "nodes", Doc: "node count of the resolved point-in-time graph"},
			{Name: "edges", Doc: "edge count of the resolved point-in-time graph"},
			{Name: "generation", Doc: "graph generation the resolution landed on"},
		},
		Examples: []string{
			"lookout triage graph-at --at=20m --store=/var/lib/lookout/lookout.db",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			if inv.Scope.At.IsZero() {
				// Live-only invocation: nothing to resolve. Real
				// graph-backed commands answer live here (and their
				// summary line says so); the probe just scans nothing.
				return 0, nil
			}
			st, err := store.OpenRead(inv.Scope.Store)
			if err != nil {
				return 0, err
			}
			defer func() { _ = st.Close() }()
			snap, err := st.GraphAt(ctx, inv.Scope.At)
			if err != nil {
				return 0, err
			}
			f := emit.Finding{
				Kind:     "graph.at",
				Severity: emit.SeverityInfo,
				Message:  "point-in-time topology resolved",
				Details: []emit.Field{
					{Key: "nodes", Value: strconv.Itoa(snap.NumNodes())},
					{Key: "edges", Value: strconv.Itoa(snap.NumEdges())},
					{Key: "generation", Value: strconv.FormatUint(snap.Generation(), 10)},
				},
			}
			if err := inv.Out.Emit(f); err != nil {
				return 0, err
			}
			return snap.NumNodes(), nil
		},
	}
}
