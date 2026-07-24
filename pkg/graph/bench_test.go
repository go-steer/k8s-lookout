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

package graph

// Benchmarks that SET the §15 Q5 gate: at what measured scale does
// the plain-map COW graph justify the CSR + interning rewrite behind
// the same interface? Current numbers and the trigger thresholds
// derived from them are recorded in docs/graph-q5-gate.md — update
// that file when these move materially.
//
// Run: go test ./pkg/graph -bench . -benchmem -run '^$'

import (
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

var benchSizes = []int{1_000, 10_000}

func mustBuild(b *testing.B, objs []any) *Graph {
	b.Helper()
	g := New(Options{SwapInterval: -1})
	if err := g.Writer().FromObjects(slices.Values(objs)); err != nil {
		b.Fatal(err)
	}
	return g
}

// BenchmarkInitialBuild measures the §6.3 initial-sync path: full
// FromObjects build + single swap. heap-MB is the retained cost of
// one built graph (snapshot + writer maintenance state + interner),
// measured once outside the timed loop.
func BenchmarkInitialBuild(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("pods=%d", n), func(b *testing.B) {
			objs := synthCluster(42, n)

			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			hold := mustBuild(b, objs)
			runtime.GC()
			runtime.ReadMemStats(&after)
			snap, _ := hold.Snapshot()

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				mustBuild(b, objs)
			}
			b.StopTimer()
			runtime.KeepAlive(hold)
			// After StopTimer: ResetTimer would have deleted these.
			b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/1e6, "heap-MB")
			b.ReportMetric(float64(snap.NumNodes()), "nodes")
			b.ReportMetric(float64(snap.NumEdges()), "edges")
		})
	}
}

// BenchmarkRadius measures blast-radius queries (depth 3, random pod
// origins) on a fixed snapshot, reporting p50/p99 tail latency —
// the number the Q5 gate keys on.
func BenchmarkRadius(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("pods=%d", n), func(b *testing.B) {
			g := mustBuild(b, synthCluster(42, n))
			s, _ := g.Snapshot()
			var pods []NodeID
			for id, info := range s.nodes {
				if info.kind == KindPod {
					pods = append(pods, id)
				}
			}
			slices.Sort(pods)

			durs := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for i := range b.N {
				start := time.Now()
				r := s.Radius(pods[i%len(pods)], 3)
				durs = append(durs, time.Since(start))
				if len(r.Down) == 0 {
					b.Fatal("empty radius on a live pod")
				}
			}
			b.StopTimer()
			slices.Sort(durs)
			b.ReportMetric(float64(durs[len(durs)/2].Nanoseconds()), "p50-ns")
			b.ReportMetric(float64(durs[len(durs)*99/100].Nanoseconds()), "p99-ns")
		})
	}
}

// relabelDeltas pre-generates alternating label-toggle updates for
// every pod so the timed loops below measure graph work, not object
// construction.
func relabelDeltas(objs []any) []Delta {
	var deltas []Delta
	for _, o := range objs {
		p, ok := o.(*corev1.Pod)
		if !ok {
			continue
		}
		on := p.DeepCopy()
		on.Labels["canary"] = "true"
		deltas = append(deltas, Delta{Op: OpUpdate, Object: on}, Delta{Op: OpUpdate, Object: p.DeepCopy()})
	}
	return deltas
}

// BenchmarkDeltaApply measures steady-state ingest throughput:
// pod-update deltas applied one at a time with a swap every 200
// deltas (≈ one 300ms batching window at realistic informer rates,
// §6.2). ns/op is the amortized per-delta cost including swaps.
func BenchmarkDeltaApply(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("pods=%d", n), func(b *testing.B) {
			objs := synthCluster(42, n)
			g := mustBuild(b, objs)
			w := g.Writer()
			deltas := relabelDeltas(objs)

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if err := w.Apply(deltas[i%len(deltas)]); err != nil {
					b.Fatal(err)
				}
				if i%200 == 199 {
					if err := w.Flush(); err != nil {
						b.Fatal(err)
					}
				}
			}
			if err := w.Flush(); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "deltas/s")
		})
	}
}

// BenchmarkSnapshotSwap isolates the swap itself — the full-map
// clone is the headline plain-map COW cost and the most likely Q5
// trigger. One delta per flush = worst-case swap amortization.
func BenchmarkSnapshotSwap(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("pods=%d", n), func(b *testing.B) {
			objs := synthCluster(42, n)
			g := mustBuild(b, objs)
			w := g.Writer()
			deltas := relabelDeltas(objs)

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if err := w.Apply(deltas[i%len(deltas)]); err != nil {
					b.Fatal(err)
				}
				if err := w.Flush(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
