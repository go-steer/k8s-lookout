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

package engine

import (
	"fmt"
	"testing"
	"time"
)

// This file covers the cluster fallback (issue #334): the
// simultaneity tier that groups Node incidents nothing else groups.
// The motivating measurement was a kwok fleet drill — thirty fake
// nodes lost their kubelet in one second and the sentinel opened
// thirty sessions, while `lookout health` reported the same outage in
// a single line.

// nodeSignal fabricates a critical NotReady incident on one node, the
// shape a node signal has when it reaches the correlator.
func nodeSignal(i int, cluster string) Signal {
	name := fmt.Sprintf("node-%02d", i)
	return Signal{
		Kind:        KindK8sEvent,
		Source:      SourceSentinel,
		Severity:    SeverityCritical,
		Cluster:     cluster,
		Fingerprint: Fingerprint(KindK8sEvent, "NodeNotReady", "Node", ""),
		TriageEvent: TriageEvent{
			Key:          EventKey{UID: fmt.Sprintf("node-uid-%d", i), Reason: "NodeNotReady"},
			KindOfObject: "Node",
			Name:         name,
			FirstSeen:    time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC),
			LastSeen:     time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC),
		},
	}
}

// nodeOnlyResolver answers every Node incident with the node itself
// and nothing else — the topology a fleet with no zone labels has,
// and the exact shape that produced one session per node.
type nodeOnlyResolver struct{}

func (nodeOnlyResolver) Ancestors(ref ObjectRef) []Ancestor {
	if ref.Kind != "Node" {
		return nil
	}
	return []Ancestor{{Kind: "Node", Name: ref.Name}}
}

// fallbackCorrelator is a correlator with the cluster fallback armed
// over a fixed fleet size.
func fallbackCorrelator(t *testing.T, fleet int) (*StormCorrelator, *time.Time) {
	t.Helper()
	c, err := NewStormCorrelator(DefaultStormWindow, DefaultStormMin, nodeOnlyResolver{})
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	if err := c.EnableClusterFallback(func() int { return fleet }); err != nil {
		t.Fatalf("EnableClusterFallback: %v", err)
	}
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	return c, &now
}

// fail drives n node incidents through the correlator, advancing the
// clock by step between each, and reports how many storms formed and
// the last one.
func fail(c *StormCorrelator, now *time.Time, n int, step time.Duration, cluster string) (int, StormInfo) {
	var formed int
	var last StormInfo
	for i := range n {
		if i > 0 {
			*now = now.Add(step)
		}
		if v := c.Observe(nodeSignal(i, cluster)); v.Kind == StormFormed {
			formed++
			last = v.Storm
		}
	}
	return formed, last
}

// TestClusterFallback_OffWithoutAFleetSize is the default posture: an
// engine caller that installs no fleet-size hook gets the old
// behaviour exactly — one session per node, no coarse key invented
// out of a denominator nobody supplied.
func TestClusterFallback_OffWithoutAFleetSize(t *testing.T) {
	t.Parallel()
	c, err := NewStormCorrelator(DefaultStormWindow, DefaultStormMin, nodeOnlyResolver{})
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }

	if formed, _ := fail(c, &now, 30, time.Second/10, "prod"); formed != 0 {
		t.Errorf("storms formed = %d, want 0 (no fleet size installed)", formed)
	}
}

// TestClusterFallback_GroupsASimultaneousFleetOutage is the #334
// measurement, inverted: thirty of forty nodes in one second is ONE
// storm, not thirty sessions.
func TestClusterFallback_GroupsASimultaneousFleetOutage(t *testing.T) {
	t.Parallel()
	c, now := fallbackCorrelator(t, 40)

	formed, storm := fail(c, now, 30, time.Second/10, "prod-us-east1")
	if formed != 1 {
		t.Fatalf("storms formed = %d, want 1", formed)
	}
	if want := (Ancestor{Kind: ClusterAncestorKind, Name: "prod-us-east1"}); storm.Ancestor != want {
		t.Errorf("ancestor = %+v, want %+v", storm.Ancestor, want)
	}
	if storm.KeySource != KeySourceSimultaneity {
		t.Errorf("KeySource = %q, want %q", storm.KeySource, KeySourceSimultaneity)
	}
	// Formation happens at the threshold (a fifth of 40 = 8); the rest
	// attach, so the storm ends up holding all thirty.
	if storm.AffectedCount != 8 {
		t.Errorf("AffectedCount at formation = %d, want 8", storm.AffectedCount)
	}
}

// TestClusterFallback_NeedsAFifthOfTheFleet is the fraction test: the
// same three-node failure is a cluster event in a fleet of ten and
// three unlucky nodes in a fleet of a hundred.
func TestClusterFallback_NeedsAFifthOfTheFleet(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		fleet    int
		failures int
		want     int
	}{
		{"a fifth of a small fleet", 10, 3, 1},
		{"three of a hundred is not a cluster event", 100, 3, 0},
		{"a fifth of a hundred is", 100, 20, 1},
		{"never fewer than three, however small the fleet", 4, 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, now := fallbackCorrelator(t, tc.fleet)
			if formed, _ := fail(c, now, tc.failures, time.Second/10, "prod"); formed != tc.want {
				t.Errorf("%d/%d nodes: storms formed = %d, want %d",
					tc.failures, tc.fleet, formed, tc.want)
			}
		})
	}
}

// TestClusterFallback_NeedsTheSameSecond is the window test, and the
// reason the tier is safe to leave on: a rolling upgrade taking nodes
// out one at a time never accumulates into a cluster-wide page, even
// though the same nodes fall inside --storm-window.
func TestClusterFallback_NeedsTheSameSecond(t *testing.T) {
	t.Parallel()
	c, now := fallbackCorrelator(t, 40)

	// 30 nodes, one every 5s: every pair is within the 60s declared
	// window, but never 8 of them inside 20s.
	if formed, _ := fail(c, now, 30, 5*time.Second, "prod"); formed != 0 {
		t.Errorf("storms formed = %d, want 0 (drained one at a time)", formed)
	}
}

// TestClusterFallback_NeverOutranksAModelledKey: when the topology
// answers with anything coarser than the node itself — a Zone, which
// is what the fleet tier joins on — the modelled key forms and the
// fallback is never even offered.
func TestClusterFallback_NeverOutranksAModelledKey(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	zone := Ancestor{Kind: "Zone", Name: "us-east1-b"}
	for i := range 30 {
		sig := nodeSignal(i, "prod")
		res.byObject[sig.ref()] = []Ancestor{{Kind: "Node", Name: sig.Name}, zone}
	}
	c, err := NewStormCorrelator(DefaultStormWindow, DefaultStormMin, res)
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	if err := c.EnableClusterFallback(func() int { return 40 }); err != nil {
		t.Fatalf("EnableClusterFallback: %v", err)
	}
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }

	formed, storm := fail(c, &now, 30, time.Second/10, "prod")
	if formed != 1 {
		t.Fatalf("storms formed = %d, want 1", formed)
	}
	if storm.Ancestor != zone {
		t.Errorf("ancestor = %+v, want the modelled zone %+v", storm.Ancestor, zone)
	}
	if storm.KeySource != KeySourceTopology {
		t.Errorf("KeySource = %q, want %q", storm.KeySource, KeySourceTopology)
	}
	// Three nodes is enough for a zone (the declared min); the fraction
	// test belongs to the fallback alone.
	if storm.AffectedCount != DefaultStormMin {
		t.Errorf("AffectedCount at formation = %d, want %d", storm.AffectedCount, DefaultStormMin)
	}
}

// TestClusterFallback_PodsAreNeverGrouped: the fallback is a NODE
// tier. A burst of pod incidents that share nothing is thirty
// incidents, exactly as before — grouping them on "the cluster" would
// be the fan-in equivalent of paging on the whole world.
func TestClusterFallback_PodsAreNeverGrouped(t *testing.T) {
	t.Parallel()
	c, now := fallbackCorrelator(t, 40)

	var formed int
	for i := range 30 {
		*now = now.Add(time.Second / 10)
		sig := podSignal(i, fmt.Sprintf("ns-%d", i))
		sig.Cluster = "prod"
		if v := c.Observe(sig); v.Kind == StormFormed {
			formed++
		}
	}
	if formed != 0 {
		t.Errorf("storms formed = %d, want 0 (pods are not cluster-keyed)", formed)
	}
}

// TestClusterFallback_ExpiresFasterThanAModelledStorm: the coarsest
// key in the system gets the shortest leash. Five idle minutes after
// the outage, a node failing for an unrelated reason opens its own
// session instead of being absorbed into an hours-old cluster storm.
func TestClusterFallback_ExpiresFasterThanAModelledStorm(t *testing.T) {
	t.Parallel()
	c, now := fallbackCorrelator(t, 40)

	if formed, _ := fail(c, now, 8, time.Second/10, "prod"); formed != 1 {
		t.Fatalf("storms formed = %d, want 1", formed)
	}
	// Well inside stormIdleTTL (30m), well past simultaneityIdleTTL.
	*now = now.Add(simultaneityIdleTTL + time.Minute)
	v := c.Observe(nodeSignal(99, "prod"))
	if v.Kind != StormNone {
		t.Errorf("verdict = %v, want StormNone (the cluster storm should have expired)", v.Kind)
	}
	// And a modelled storm is untouched by the shorter TTL.
	if got := stormIdleTTL; got <= simultaneityIdleTTL {
		t.Errorf("stormIdleTTL = %s, must stay longer than the fallback's %s", got, simultaneityIdleTTL)
	}
}

// TestClusterFallback_UnnamedCluster: --cluster-name is optional, and
// a fleet outage in a cluster nobody named still pages once. The
// ancestor says so rather than inventing an identity.
func TestClusterFallback_UnnamedCluster(t *testing.T) {
	t.Parallel()
	c, now := fallbackCorrelator(t, 10)

	formed, storm := fail(c, now, 3, time.Second/10, "")
	if formed != 1 {
		t.Fatalf("storms formed = %d, want 1", formed)
	}
	if want := (Ancestor{Kind: ClusterAncestorKind, Name: unnamedCluster}); storm.Ancestor != want {
		t.Errorf("ancestor = %+v, want %+v", storm.Ancestor, want)
	}
}

// TestEnableClusterFallback_RejectsANilFleetSize: the denominator is
// the whole safety argument, so refusing it is a config error, not a
// silent downgrade to "always form".
func TestEnableClusterFallback_RejectsANilFleetSize(t *testing.T) {
	t.Parallel()
	c, err := NewStormCorrelator(DefaultStormWindow, DefaultStormMin, nodeOnlyResolver{})
	if err != nil {
		t.Fatalf("NewStormCorrelator: %v", err)
	}
	if err := c.EnableClusterFallback(nil); err == nil {
		t.Error("nil fleet-size function must be rejected")
	}
}

// TestClusterFallback_ZeroFleetSizeNeverForms: before the topology
// index has published a snapshot the fleet size is 0. A correlator
// that treated "unknown" as "tiny" would fire the coarsest key it has
// during exactly the window where it knows least.
func TestClusterFallback_ZeroFleetSizeNeverForms(t *testing.T) {
	t.Parallel()
	c, now := fallbackCorrelator(t, 0)

	if formed, _ := fail(c, now, 30, time.Second/10, "prod"); formed != 0 {
		t.Errorf("storms formed = %d, want 0 (fleet size unknown)", formed)
	}
}
