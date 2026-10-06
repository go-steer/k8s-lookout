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

package topologydrift

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// The tests in this file answer issue #543: a Deployment spreads on
// kubernetes.io/hostname with a pod anti-affinity, meaning to spread across
// zones; the scheduler honours the anti-affinity and still puts 8 of 10
// replicas in one zone. Which tier fires on the zone axis, and does it reach
// the wire?
//
// The hostname term says nothing about zones, so no inference source puts an
// intent on the zone axis, and §5.1's assumed cluster default (zone maxSkew
// 5, ScheduleAnyway) is what the subject is judged against. The 2026-10-01
// amendment that drops that default does not apply: it is for a *required
// podAffinity* colocation, and an anti-affinity colocates nothing. So the
// answer is §8.1 Tier B — `leeway.placement_drift` at `warning` — which §8.3
// routes to the watchboard digest. It opens a session only if a storm takes it
// in, or if the operator declares the zone spread as a contract.
//
// It is a regression guard for that answer, in both directions: the finding
// must not go silent (an unconstrained workload piled into one zone is a good
// finding, Gari 2026-10-06) and must not get louder than Tier B without a
// declaration somebody wrote down.

// hostSpreadZones is three zones of perZone nodes each, labelled with their
// hostname so the anti-affinity's axis exists on the nodes.
func hostSpreadZones(perZone map[string]int) []*corev1.Node {
	var out []*corev1.Node
	for _, z := range []string{"zone-a", "zone-b", "zone-c"} {
		for i := range perZone[z] {
			name := fmt.Sprintf("%s-%d", z, i)
			out = append(out, node(name, z, onHost(name)))
		}
	}
	return out
}

func hostSpreadInventory(nodes []*corev1.Node) *Inventory {
	inv := NewInventory(DefaultTopologyKeys)
	for _, n := range nodes {
		inv.Upsert(n)
	}
	return inv
}

// eightOneOne is 85% in one zone on a ten-replica subject: 8/1/1.
//
// A required hostname anti-affinity allows one replica per node, so zone-a
// needs eight nodes; the preferred term allows doubling up, so four will do.
var eightOneOne = []struct {
	name    string
	rep     *corev1.Pod
	perZone map[string]int
	nodes   []string
}{
	{
		name:    "required anti-affinity on hostname, 8 nodes per zone",
		rep:     affPod(antiRequired(selfTerm(hostKey))),
		perZone: map[string]int{"zone-a": 8, "zone-b": 8, "zone-c": 8},
		nodes: []string{"zone-a-0", "zone-a-1", "zone-a-2", "zone-a-3", "zone-a-4", "zone-a-5", "zone-a-6", "zone-a-7",
			"zone-b-0", "zone-c-0"},
	},
	{
		// The capacity a required term needs to reach 8/1/1 on a lopsided
		// cluster. The zone expectation stays even ([4 3 3]): a spread
		// constraint counts domains, not nodes.
		name:    "required anti-affinity on hostname, 8/2/2 nodes",
		rep:     affPod(antiRequired(selfTerm(hostKey))),
		perZone: map[string]int{"zone-a": 8, "zone-b": 2, "zone-c": 2},
		nodes: []string{"zone-a-0", "zone-a-1", "zone-a-2", "zone-a-3", "zone-a-4", "zone-a-5", "zone-a-6", "zone-a-7",
			"zone-b-0", "zone-c-0"},
	},
	{
		name:    "preferred anti-affinity on hostname, 4 nodes per zone",
		rep:     affPod(antiPreferred(100, selfTerm(hostKey))),
		perZone: map[string]int{"zone-a": 4, "zone-b": 4, "zone-c": 4},
		nodes: []string{"zone-a-0", "zone-a-1", "zone-a-2", "zone-a-3", "zone-a-0", "zone-a-1", "zone-a-2", "zone-a-3",
			"zone-b-0", "zone-c-0"},
	},
}

// TestHostnameAntiAffinityZoneSkew_IsTierBUnderTheAssumedDefault is the
// verdict half of #543, on the shipped resolve and scoring pass with
// --topology-cluster-defaults unset.
func TestHostnameAntiAffinityZoneSkew_IsTierBUnderTheAssumedDefault(t *testing.T) {
	for _, c := range eightOneOne {
		t.Run(c.name, func(t *testing.T) {
			inv := hostSpreadInventory(hostSpreadZones(c.perZone))
			res, ev := zoneVerdict(inv, c.rep, replicas(c.rep, c.nodes...), ResolveConfig{})

			// The anti-affinity yields no zone intent of its own, and the
			// #536 rule for required colocation does not unseat the default.
			if in := ev.Intent; in == nil || in.Source != leeway.SourceClusterDefaultAssumed || in.Confidence != leeway.ConfidenceAssumed {
				t.Fatalf("zone intent = %+v, want the assumed cluster default", ev.Intent)
			}
			for key, in := range res.Intents {
				if in != nil && key != hostKey && (in.Source == leeway.SourcePodAntiAffinityRequired || in.Source == leeway.SourcePodAntiAffinityPreferred) {
					t.Errorf("a hostname anti-affinity produced an intent on %s: %+v", key, in)
				}
			}

			if !slices.Equal(ev.Scores.Actual, []int64{8, 1, 1}) || !slices.Equal(ev.Scores.Expected, []int64{4, 3, 3}) {
				t.Fatalf("actual/expected = %v/%v, want [8 1 1]/[4 3 3]", ev.Scores.Actual, ev.Scores.Expected)
			}
			v := ev.Verdict
			if !v.Breached || v.Suppressed || v.Tier != leeway.TierB || v.Kind != leeway.BreachDrift || v.Severity != string(engine.SeverityWarning) {
				t.Fatalf("verdict = %+v, want an unsuppressed Tier B drift breach at warning", v)
			}
			if want := "normalised drift over threshold"; v.Reason != want {
				t.Errorf("reason = %q, want %q", v.Reason, want)
			}
			if !v.Escalated {
				t.Errorf("max domain share %.2f did not escalate", ev.Scores.MaxDomainShare)
			}
			if d := v.Route(false); !d.Signal || d.Severity != string(engine.SeverityWarning) {
				t.Errorf("route = %+v, want a warning signal", d)
			}
			if got := engine.RouteFor(engine.Severity(v.Severity)); got != engine.RouteWatchboard {
				t.Errorf("§7.7 route = %v, want the watchboard: Tier B opens no session of its own", got)
			}
		})
	}
}

// TestHostnameAntiAffinityZoneSkew_WhatMakesItCritical is the other half of
// the question: which declarations turn the same placement into Tier A.
//
// Only a zone constraint with DoNotSchedule does — on the pod, or as an
// operator-declared cluster default, which §8.1 does not cap. A LeewayPolicy
// declares intent but no contract (its shipped schema has no maxSkew), so it
// stays Tier B, as does a declared ScheduleAnyway default.
func TestHostnameAntiAffinityZoneSkew_WhatMakesItCritical(t *testing.T) {
	base := eightOneOne[0]
	inv := hostSpreadInventory(hostSpreadZones(base.perZone))

	withTSC := base.rep.DeepCopy()
	withTSC.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{zoneSpread(1, corev1.DoNotSchedule)}
	declared := func(action corev1.UnsatisfiableConstraintAction) *[]corev1.TopologySpreadConstraint {
		return &[]corev1.TopologySpreadConstraint{{TopologyKey: corev1.LabelTopologyZone, MaxSkew: 1, WhenUnsatisfiable: action}}
	}
	policy := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: LeewayPolicy
metadata: { name: api, namespace: payments }
spec:
  selector: { matchLabels: { app: api } }
  topologyKeys:
    - { key: topology.kubernetes.io/zone, mode: Spread }
`, false)

	for _, c := range []struct {
		name   string
		rep    *corev1.Pod
		cfg    ResolveConfig
		source leeway.IntentSource
		tier   leeway.Tier
		kind   leeway.BreachKind
	}{
		{"zone TSC maxSkew 1 DoNotSchedule on the pod", withTSC, ResolveConfig{}, leeway.SourceTopologySpreadConstraint, leeway.TierA, leeway.BreachMaxSkew},
		{"declared cluster default zone=1:DoNotSchedule", base.rep, ResolveConfig{ClusterDefaults: declared(corev1.DoNotSchedule)}, leeway.SourceClusterDefaultDeclared, leeway.TierA, leeway.BreachMaxSkew},
		{"declared cluster default zone=1:ScheduleAnyway", base.rep, ResolveConfig{ClusterDefaults: declared(corev1.ScheduleAnyway)}, leeway.SourceClusterDefaultDeclared, leeway.TierB, leeway.BreachDrift},
		{"LeewayPolicy zone Spread", base.rep, ResolveConfig{Policy: policy}, leeway.SourcePolicyCRD, leeway.TierB, leeway.BreachDrift},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, ev := zoneVerdict(inv, c.rep, replicas(c.rep, base.nodes...), c.cfg)
			if ev.Intent == nil || ev.Intent.Source != c.source {
				t.Fatalf("zone intent = %+v, want source %v", ev.Intent, c.source)
			}
			if v := ev.Verdict; !v.Breached || v.Tier != c.tier || v.Kind != c.kind {
				t.Errorf("verdict = %+v, want tier %v kind %v", v, c.tier, c.kind)
			}
		})
	}
}

// TestHostnameAntiAffinityZoneSkew_ReachesTheWireAsAWarning runs the
// required-term cluster through Run with the shipped defaults (zone and
// region tracked, cluster defaults unset, Tier C signals off) and reads what
// comes out.
func TestHostnameAntiAffinityZoneSkew_ReachesTheWireAsAWarning(t *testing.T) {
	c := eightOneOne[0]
	objs := []runtime.Object{replicaSet("api-7f9", "payments", "api")}
	for _, n := range hostSpreadZones(c.perZone) {
		objs = append(objs, n)
	}
	for i, p := range replicas(c.rep, c.nodes...) {
		p.Name = fmt.Sprintf("api-7f9-%d", i)
		p.UID = types.UID("payments/" + p.Name)
		p.Status.Phase = corev1.PodRunning
		ownedBy("ReplicaSet", "api-7f9")(p)
		objs = append(objs, p)
	}

	cfg := quickAlerts()
	cfg.TopologyKeys = DefaultTopologyKeys
	s := New(fake.NewSimpleClientset(objs...), cfg)
	s.logf = func(string, ...any) {}

	var mu sync.Mutex
	var got []sources.Signal
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx, func(sig sources.Signal) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, sig)
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancellation")
		}
	})
	signals := func() []sources.Signal {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
	waitFor(t, "the finding to reach the wire", func() bool { return len(signals()) > 0 })
	time.Sleep(50 * time.Millisecond)

	sigs := signals()
	if len(sigs) != 1 {
		t.Fatalf("%d signal(s), want exactly one zone finding: %v", len(sigs), sigs)
	}
	sig := sigs[0]
	if sig.Kind != leeway.KindPlacementDrift || sig.Severity != engine.SeverityWarning {
		t.Errorf("signal = %s at %s, want %s at warning", sig.Kind, sig.Severity, leeway.KindPlacementDrift)
	}
	// The assumed default is a ScheduleAnyway maxSkew 5 and the observed skew
	// is 7, so the cause is the soft constraint being ignored.
	if sig.Key.Reason != string(leeway.CauseConstraintIgnored) {
		t.Errorf("reason (suspected cause) = %q, want %q", sig.Key.Reason, leeway.CauseConstraintIgnored)
	}
	for _, want := range []string{"tier B", "topology.kubernetes.io/zone", "zone-a 8/4", "observed skew 7", "intent cluster-default-assumed"} {
		if !strings.Contains(sig.Message, want) {
			t.Errorf("message %q does not contain %q", sig.Message, want)
		}
	}
	t.Logf("wire: kind=%s severity=%s reason=%s message=%q", sig.Kind, sig.Severity, sig.Key.Reason, sig.Message)
}
