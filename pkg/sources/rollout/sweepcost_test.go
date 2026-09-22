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

package rollout

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"
)

// What this source costs, and the two shapes that made it cost too much
// (issue #493). Taking the Phase 8 scale numbers on a 403-node fleet —
// 2,883 Deployments, 6,318 ReplicaSets — the sentinel never reached
// /readyz: one core, flat, forever, with the only runnable goroutine in
// a SIGQUIT dump sitting in evalDeployment's scan of every ReplicaSet in
// the cluster, called from a sweep of every Deployment in the cluster,
// called from the handler for a single ReplicaSet status write.
//
// Two independent multipliers, and removing either one alone still
// leaves an unusable source:
//
//   - per-sweep cost was O(D×R + S×P), because ownership was a predicate
//     re-evaluated against every candidate rather than an index;
//   - sweeps happened per event, because three of the four informer
//     handlers swept inline.
//
// Both guards below are structural on purpose. A timing assertion is the
// obvious thing to write here and it would be the wrong thing: it
// measures the runner, not the code, so it is either loose enough to
// pass a quadratic on a small fixture or tight enough to flake.

// seedFleet loads n Deployments, each mid-rollout with an old healthy
// ReplicaSet and a new one whose single pod is crash-looping — the §7.2
// fixture, n times over, with distinct owners.
func seedFleet(s *Source, n int) {
	for i := range n {
		d := fmt.Sprintf("d%04d", i)
		s.onReplicaSet(replicaSet("rs-old-"+d, "prod", "web-"+d+"-a1", d, "1", 3, 3))
		s.onReplicaSet(replicaSet("rs-new-"+d, "prod", "web-"+d+"-b2", d, "2", 1, 0))
		s.onPod(rolloutPod("p-"+d, "prod", "web-"+d+"-b2-x1", "rs-new-"+d, "", false, "CrashLoopBackOff"))
		s.onDeployment(deployment(d, "prod", "web-"+d, 3, 1, 3, 4))
	}
}

// TestSweep_JudgesEveryWorkloadAgainstItsOwnReplicaSetsOnly is the
// correctness half of the index: with hundreds of Deployments in one
// namespace, each verdict must rest on that Deployment's own
// ReplicaSets. A wrong index does not fail loudly — it produces a
// plausible verdict from someone else's revision — so this asserts the
// count AND the evidence.
func TestSweep_JudgesEveryWorkloadAgainstItsOwnReplicaSetsOnly(t *testing.T) {
	t.Parallel()
	const fleet = 300
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	seedFleet(s, fleet)
	s.settle(*clock)
	if got := col.kinds(); len(got) != 0 {
		t.Fatalf("fired %d before the observe window elapsed", len(got))
	}

	*clock = clock.Add(3 * time.Minute)
	s.send(s.sweep(*clock))

	sigs := col.all()
	if len(sigs) != fleet {
		t.Fatalf("signals = %d, want one stall per Deployment (%d)", len(sigs), fleet)
	}
	seen := make(map[string]bool, fleet)
	for _, sig := range sigs {
		seen[sig.Name] = true
		// The new ReplicaSet named in the evidence must be this
		// Deployment's own, not whichever one a whole-collection scan
		// happened to reach last.
		want := "web-" + sig.Key.UID + "-b2"
		if !strings.Contains(sig.Message, want) {
			t.Fatalf("%s cited %q, want its own new ReplicaSet %q", sig.Name, sig.Message, want)
		}
		if !strings.Contains(sig.Message, "old_ready=3/3") {
			t.Fatalf("%s: old-revision evidence %q, want its own 3/3", sig.Name, sig.Message)
		}
	}
	if len(seen) != fleet {
		t.Errorf("%d distinct Deployments fired, want %d", len(seen), fleet)
	}
}

// TestEval_ResolvesOwnershipByIndexNotByScan is the cost half, asserted
// structurally: the per-workload evaluators must reach ownership through
// the sweep's index and never range over the source's whole ReplicaSet
// or pod collection. One `for _, e := range s.replicasets` inside
// evalDeployment is all it takes to put the quadratic back, and it would
// pass every behavioural test in this package.
func TestEval_ResolvesOwnershipByIndexNotByScan(t *testing.T) {
	t.Parallel()
	// The collections that are sized by the cluster rather than by the
	// object being judged. Ranging over either of these, inside a
	// function called once per workload per sweep, is the bug.
	forbidden := map[string]string{
		"replicasets": "every ReplicaSet in the cluster — use ownerIndex.replicasets[uid]",
		"pods":        "every pod in the cluster — use ownerIndex.pods[uid]",
	}
	guarded := map[string]bool{
		"evalDeployment":  true,
		"evalStatefulSet": true,
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "rollout.go", nil, 0)
	if err != nil {
		t.Fatalf("parse rollout.go: %v", err)
	}
	checked := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || !guarded[fn.Name.Name] {
			continue
		}
		checked++
		ast.Inspect(fn, func(n ast.Node) bool {
			rng, ok := n.(*ast.RangeStmt)
			if !ok {
				return true
			}
			sel, ok := rng.X.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "s" {
				return true
			}
			if why, bad := forbidden[sel.Sel.Name]; bad {
				t.Errorf("%s ranges over s.%s at %s: that is %s",
					fn.Name.Name, sel.Sel.Name, fset.Position(rng.Pos()), why)
			}
			return true
		})
	}
	if checked != len(guarded) {
		t.Fatalf("guarded %d of %d evaluators — a rename silently retired this test", checked, len(guarded))
	}
}

// TestWorkloadEvent_DoesNotSweepInline pins the second multiplier: a
// workload event records state and asks for a sweep, it does not perform
// one. The fixture is already past its observe window, so the old inline
// sweep would emit here and the coalesced one does not.
func TestWorkloadEvent_DoesNotSweepInline(t *testing.T) {
	t.Parallel()
	s, col, clock := newTestSource(t, Config{Observe: 3 * time.Minute})
	seedBadDeploy(s)
	*clock = clock.Add(10 * time.Minute)

	drain(s) // the seed's own request, so the test observes this event's

	for _, tc := range []struct {
		name string
		fire func()
	}{
		{"deployment", func() { s.onDeployment(deployment("d1", "prod", "web", 3, 1, 3, 4)) }},
		{"replicaset", func() { s.onReplicaSet(replicaSet("rs-new", "prod", "web-7b9", "d1", "2", 1, 0)) }},
		{"statefulset", func() { s.onStatefulSet(statefulSet("s1", "prod", "db", 3, 2, "rev-a", "rev-b")) }},
	} {
		tc.fire()
		if got := col.kinds(); len(got) != 0 {
			t.Fatalf("%s event swept inline and emitted %v", tc.name, got)
		}
		select {
		case <-s.dirty:
		default:
			t.Fatalf("%s event did not ask for a sweep — the stall would wait for the tick", tc.name)
		}
	}

	// And the sweep it asked for still finds the stall.
	s.settle(*clock)
	if got := col.kinds(); len(got) != 1 {
		t.Fatalf("signals = %v, want the stall on the coalesced sweep", got)
	}
}

// TestPodEvent_DoesNotEvenAskForASweep: pod churn is the highest-volume
// stream of the four and a pod update cannot start a stall on its own,
// so it stays off the sweep path entirely rather than merely coalescing
// onto it. onPod has said so in a comment since before #493; this makes
// the comment fail when it stops being true.
func TestPodEvent_DoesNotEvenAskForASweep(t *testing.T) {
	t.Parallel()
	s, _, _ := newTestSource(t, Config{})
	drain(s)
	s.onPod(rolloutPod("p-new", "prod", "web-7b9-x1", "rs-new", "", false, "CrashLoopBackOff"))
	select {
	case <-s.dirty:
		t.Fatal("a pod update asked for a sweep")
	default:
	}
}

// drain empties the coalescing request, so a test can attribute the next
// one to the event it just delivered.
func drain(s *Source) {
	select {
	case <-s.dirty:
	default:
	}
}

// BenchmarkSweep is the number behind the guards, at roughly the fleet
// that found the bug. Run it with -benchtime=1x if you want the shape of
// a cold sweep rather than a warm one.
func BenchmarkSweep(b *testing.B) {
	for _, fleet := range []int{100, 400, 1600} {
		b.Run(fmt.Sprintf("deployments=%d", fleet), func(b *testing.B) {
			s := New(nil, Config{})
			now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
			s.now = func() time.Time { return now }
			s.armed = true
			seedFleet(s, fleet)
			b.ResetTimer()
			for range b.N {
				s.sweep(now)
			}
		})
	}
}
