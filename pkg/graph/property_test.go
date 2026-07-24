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

// Property tests per DESIGN.md §13:
//
//   - every delta applied is reflected in the next snapshot —
//     verified as "incremental maintenance ≡ from-scratch rebuild of
//     the same object set" after every flush;
//   - radius answers are stable across COW swaps — readers hammer
//     snapshots while the writer churns, under -race.
//
// Cluster sizes here are CI-sized (the suite runs with -race); the
// full 10k-pod scale lives in the benchmarks.

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// objSet tracks the "world" of live objects so it can be rebuilt
// from scratch and compared against the incrementally maintained
// graph.
type objSet map[string]any

func objKey(o any) string {
	m := o.(metav1.Object)
	return fmt.Sprintf("%T/%s/%s", o, m.GetNamespace(), m.GetName())
}

func newObjSet(objs []any) objSet {
	s := make(objSet, len(objs))
	for _, o := range objs {
		s[objKey(o)] = o
	}
	return s
}

// sortedValues returns the objects in deterministic (key-sorted)
// order, optionally filtered by key prefix.
func (s objSet) sortedValues(prefix string) []any {
	keys := make([]string, 0, len(s))
	for k := range s {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := make([]any, len(keys))
	for i, k := range keys {
		out[i] = s[k]
	}
	return out
}

// mutate applies one random world mutation, updating both the
// object set and the graph writer. Returns a description for
// failure messages.
func mutate(t *testing.T, rng *rand.Rand, world objSet, w *Writer) string {
	t.Helper()
	pick := func(prefix string) any {
		vals := world.sortedValues(prefix)
		if len(vals) == 0 {
			return nil
		}
		return vals[rng.IntN(len(vals))]
	}
	apply := func(op Op, o any) {
		t.Helper()
		if err := w.Apply(Delta{Op: op, Object: o}); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	switch n := rng.IntN(100); {
	case n < 40: // toggle a label on a random pod
		o := pick("*v1.Pod")
		if o == nil {
			return "noop"
		}
		p := o.(*corev1.Pod).DeepCopy()
		if p.Labels == nil {
			p.Labels = map[string]string{}
		}
		if _, on := p.Labels["canary"]; on {
			delete(p.Labels, "canary")
		} else {
			p.Labels["canary"] = "true"
		}
		world[objKey(p)] = p
		apply(OpUpdate, p)
		return "relabel " + p.Name
	case n < 60: // delete a random pod
		o := pick("*v1.Pod")
		if o == nil {
			return "noop"
		}
		p := o.(*corev1.Pod)
		delete(world, objKey(p))
		apply(OpDelete, p)
		return "delete pod " + p.Name
	case n < 75: // add a pod cloned from an existing one
		o := pick("*v1.Pod")
		if o == nil {
			return "noop"
		}
		p := o.(*corev1.Pod).DeepCopy()
		p.Name = fmt.Sprintf("%s-cl%04d", p.Name, rng.IntN(10000))
		if _, dup := world[objKey(p)]; dup {
			return "noop"
		}
		world[objKey(p)] = p
		apply(OpAdd, p)
		return "add pod " + p.Name
	case n < 85: // rewrite a random service's selector
		o := pick("*v1.Service")
		if o == nil {
			return "noop"
		}
		svc := o.(*corev1.Service).DeepCopy()
		if svc.Spec.Selector == nil {
			svc.Spec.Selector = map[string]string{}
		}
		if svc.Spec.Selector["app"] == "nothing-matches" {
			svc.Spec.Selector["app"] = svc.Name
		} else {
			svc.Spec.Selector["app"] = "nothing-matches"
		}
		world[objKey(svc)] = svc
		apply(OpUpdate, svc)
		return "reselect svc " + svc.Name
	case n < 93: // delete a random configmap (dangling mounts appear)
		o := pick("*v1.ConfigMap")
		if o == nil {
			return "noop"
		}
		cm := o.(*corev1.ConfigMap)
		delete(world, objKey(cm))
		apply(OpDelete, cm)
		return "delete cm " + cm.Name
	default: // delete a random service
		o := pick("*v1.Service")
		if o == nil {
			return "noop"
		}
		svc := o.(*corev1.Service)
		delete(world, objKey(svc))
		apply(OpDelete, svc)
		return "delete svc " + svc.Name
	}
}

// diffDumps fails the test with the symmetric difference of two
// edge/node dumps.
func diffDumps(t *testing.T, what, step string, got, want map[string]bool) {
	t.Helper()
	for k := range want {
		if !got[k] {
			t.Errorf("%s after %q: missing %s", what, step, k)
		}
	}
	for k := range got {
		if !want[k] {
			t.Errorf("%s after %q: unexpected %s", what, step, k)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

// TestEveryDeltaReflectedInNextSnapshot flushes after every single
// mutation and requires the snapshot to be indistinguishable from a
// from-scratch rebuild of the current object set.
func TestEveryDeltaReflectedInNextSnapshot(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	world := newObjSet(synthCluster(7, 200))
	g := New(Options{SwapInterval: -1})
	w := g.Writer()
	if err := w.FromObjects(slices.Values(world.sortedValues(""))); err != nil {
		t.Fatal(err)
	}
	for step := range 120 {
		desc := mutate(t, rng, world, w)
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		s, err := g.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		checkInvariants(t, s)
		_, fresh := buildGraph(t, world.sortedValues(""))
		diffDumps(t, "edges", fmt.Sprintf("step %d: %s", step, desc), dumpEdges(s), dumpEdges(fresh))
		diffDumps(t, "observed nodes", fmt.Sprintf("step %d: %s", step, desc), dumpObserved(s), dumpObserved(fresh))
	}
}

// TestIncrementalEqualsRebuildAfterHeavyChurn runs a larger world
// through batched churn (multiple deltas per flush, the production
// shape) and compares once at the end.
func TestIncrementalEqualsRebuildAfterHeavyChurn(t *testing.T) {
	rng := rand.New(rand.NewPCG(13, 17))
	world := newObjSet(synthCluster(13, 1500))
	g := New(Options{SwapInterval: -1})
	w := g.Writer()
	if err := w.FromObjects(slices.Values(world.sortedValues(""))); err != nil {
		t.Fatal(err)
	}
	for step := range 400 {
		mutate(t, rng, world, w)
		if step%25 == 24 {
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	s, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, s)
	_, fresh := buildGraph(t, world.sortedValues(""))
	diffDumps(t, "edges", "heavy churn", dumpEdges(s), dumpEdges(fresh))
	diffDumps(t, "observed nodes", "heavy churn", dumpObserved(s), dumpObserved(fresh))
}

// TestRadiusStableAcrossConcurrentSwaps runs readers against
// snapshots while the writer churns and swaps continuously. On any
// single snapshot, repeated queries must agree exactly and every
// returned NodeID must resolve — regardless of what the writer is
// doing meanwhile. Run with -race (CI does).
func TestRadiusStableAcrossConcurrentSwaps(t *testing.T) {
	world := newObjSet(synthCluster(23, 600))
	g := New(Options{SwapInterval: -1})
	w := g.Writer()
	if err := w.FromObjects(slices.Values(world.sortedValues(""))); err != nil {
		t.Fatal(err)
	}
	s0, _ := g.Snapshot()

	// Collect stable query origins up front (NodeIDs survive
	// delete/re-create; Radius on a since-deleted origin is defined
	// as empty).
	var origins []NodeID
	for id, info := range s0.nodes {
		if info.kind == KindPod {
			origins = append(origins, id)
		}
	}
	slices.Sort(origins)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writer: continuous churn + swap.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(29, 31))
		for {
			select {
			case <-stop:
				return
			default:
			}
			mutate(t, rng, world, w)
			if err := w.Flush(); err != nil {
				return
			}
		}
	}()

	// Readers: consistency of answers on a fixed snapshot.
	for r := range 4 {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed+1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				s, err := g.Snapshot()
				if err != nil {
					t.Errorf("reader: %v", err)
					return
				}
				origin := origins[rng.IntN(len(origins))]
				r1 := s.Radius(origin, 3)
				r2 := s.Radius(origin, 3)
				if !slices.Equal(r1.Up, r2.Up) || !slices.Equal(r1.Down, r2.Down) || !slices.Equal(r1.Lateral, r2.Lateral) {
					t.Error("radius answer changed on a fixed snapshot")
					return
				}
				for _, hits := range [][]Hit{r1.Up, r1.Down, r1.Lateral} {
					for _, h := range hits {
						if _, ok := s.Resolve(h.ID); !ok {
							t.Errorf("radius returned unresolvable node %d", h.ID)
							return
						}
					}
				}
				// Exercise the other queries for -race coverage.
				_ = s.OwnerChain(origin)
				_ = s.CommonAncestors(origin, origins[rng.IntN(len(origins))])
				_ = s.WorkloadEdges(origin)
			}
		}(uint64(100 + r))
	}

	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()

	sFinal, _ := g.Snapshot()
	checkInvariants(t, sFinal)
}
