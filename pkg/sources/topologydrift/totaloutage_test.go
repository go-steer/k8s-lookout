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
	"fmt"
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// The tests in this file are the total outage: every node in a zone NotReady,
// still present, with pods still bound to them. The partial outage is in
// transient_test.go and the removed zone in domain_test.go; this is the case
// that falls between them, because a zone with no Ready node drops out of the
// eligible set and an outage test walking only that set never sees it.

func TestSource_AZoneWithEveryNodeNotReadyIsStillAnOutage(t *testing.T) {
	cfg := Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}
	s := armedSource(cfg,
		node("n-a1", "us-central1-a"), node("n-a2", "us-central1-a"),
		node("n-b", "us-central1-b"), node("n-c", "us-central1-c"))
	s.inv.SampleReady(t0, cfg.readyRetention())

	s.inv.Upsert(node("n-a1", "us-central1-a", notReady()))
	s.inv.Upsert(node("n-a2", "us-central1-a", notReady()))
	now := t0.Add(time.Minute)
	s.inv.SampleReady(now, cfg.readyRetention())

	// The real eligibility, not a hand-built one: the point is what §7.1
	// does to the dead zone before §7.6 is asked about it.
	e := leeway.EligibleDomains(s.inv.NodeViews(zoneKey, Constraints{}), leeway.DefaultEligibilityOptions())
	if slices.Contains(e.Domains, "us-central1-a") {
		t.Fatalf("eligible = %v: a zone with no Ready node should have left the eligible set", e.Domains)
	}
	if !slices.Contains(e.Unready, "us-central1-a") {
		t.Fatalf("Unready = %v, want the dead zone named", e.Unready)
	}

	if got := s.suppression(anySubject, now)(zoneKey, e); !got.Suppress || got.State != leeway.TransientDomainOutage {
		t.Errorf("suppression = %+v, want a suppressing domain-outage", got)
	}
	if got := s.nodeGroupSuppression(now)(zoneKey, e); !got.Suppress || got.State != leeway.TransientDomainOutage {
		t.Errorf("node-group suppression = %+v, want a suppressing domain-outage", got)
	}

	// Bounded by §7.6's window like every other outage. A zone dead for longer
	// than that is the cluster's shape now, and the domain_unavailable episode
	// is what keeps reporting it.
	late := now.Add(leeway.DefaultTransientConfig().OutageWindow + time.Minute)
	s.inv.SampleReady(late, cfg.readyRetention())
	if got := s.suppression(anySubject, late)(zoneKey, e); got.State != leeway.TransientNone {
		t.Errorf("suppression past the window = %+v, want no transient", got)
	}
}

// End to end through Run, against domain_test.go's removed zone: the same one
// domain finding, with the cause the zone actually has, and the workloads
// quiet because §7.6 holds them rather than only because the expectation
// happens to shrink to something they still meet.
func TestSource_ANotReadyZoneIsOneFindingAndEveryWorkloadIsHeld(t *testing.T) {
	objs := []runtime.Object{
		node("n-a", "us-central1-a"), node("n-b", "us-central1-b"),
		node("n-c1", "us-central1-c"), node("n-c2", "us-central1-c"),
	}
	const workloads = 20
	for w := range workloads {
		name := fmt.Sprintf("app-%d", w)
		objs = append(objs, replicaSet(name+"-7c9f", "prod", name))
		for i, n := range []string{"n-a", "n-b", "n-c1", "n-c2"} {
			objs = append(objs, pod(fmt.Sprintf("%s-%d", name, i), "prod", n, ownedBy("ReplicaSet", name+"-7c9f")))
		}
	}
	s, signals := runDomainSource(t, objs...)

	waitFor(t, "every workload to be scored", func() bool { return scoredAxes(s) == workloads })
	waitFor(t, "the zone to be latched", func() bool {
		return slices.Contains(domainNames(s, zoneKey), "us-central1-c")
	})

	// The nodes stay, and so do the pods bound to them: a kubelet that stops
	// reporting leaves its pods Running in the API until somebody evicts them.
	s.onNode(node("n-c1", "us-central1-c", notReady()))
	s.onNode(node("n-c2", "us-central1-c", notReady()))

	waitFor(t, "the outage to reach every evaluation", func() bool {
		return suppressedAxes(s) == workloads
	})
	waitFor(t, "the domain finding to reach the wire", func() bool { return len(signals()) > 0 })
	time.Sleep(100 * time.Millisecond)

	got := signals()
	if len(got) != 1 {
		t.Fatalf("%d signal(s) for one dead zone, want exactly 1: %v", len(got), got)
	}
	if got[0].Kind != leeway.KindDomainUnavailable || got[0].Name != "us-central1-c" {
		t.Errorf("signal = %s %s, want %s for us-central1-c", got[0].Kind, got[0].Name, leeway.KindDomainUnavailable)
	}
	if got[0].Key.Reason != string(leeway.CauseDomainOutage) {
		t.Errorf("reason = %q, want %q: the nodes are present and none is Ready", got[0].Key.Reason, leeway.CauseDomainOutage)
	}
}

// The defect the suppression exists for. §7.5 resets a baseline whose domain
// set changes, and freezes one whose evaluation is suppressed; a total outage
// that is not suppressed shrinks the set, so every learned baseline on the
// axis was thrown away when the zone died and again when it came back — six
// hours of maturity, twice, for a flap.
func TestSource_ABaselineSurvivesAZoneGoingWhollyNotReadyAndBack(t *testing.T) {
	cfg := quickLearning()
	cfg.ReadySampleInterval = 5 * time.Millisecond
	cfg.EligibilitySweepInterval = 5 * time.Millisecond
	objs := append([]runtime.Object{
		replicaSet("web-7c9f", "prod", "web"),
		node("n-a", "us-central1-a"), node("n-b", "us-central1-b"),
		node("n-c1", "us-central1-c"), node("n-c2", "us-central1-c"),
	}, webPods(6, "n-a", "n-b", "n-c1")...)
	s, _ := runAlerting(t, cfg, nil, objs...)

	key := baselineKey{Subject: webSub, Key: zoneKey}
	waitFor(t, "the baseline to mature", func() bool {
		return s.baselines.Intent(key, time.Now(), s.cfg.Baselines) != nil
	})
	firstSeen, domains := baselineShape(s, key)
	resets := baselineResets(s)

	s.onNode(node("n-c1", "us-central1-c", notReady()))
	s.onNode(node("n-c2", "us-central1-c", notReady()))
	// Waiting on the eligible set rather than on the suppression, so that
	// without the fix this fails on the reset it is about rather than on a
	// timeout.
	waitFor(t, "the dead zone to leave the eligible set", func() bool { return scoredDomains(s, webSub) == 2 })
	// Several baseline ticks at two milliseconds, so that "no reset" means the
	// sampler ran against the outage and held rather than had not run.
	time.Sleep(50 * time.Millisecond)

	s.onNode(node("n-c1", "us-central1-c"))
	s.onNode(node("n-c2", "us-central1-c"))
	waitFor(t, "the zone to return to the eligible set", func() bool { return scoredDomains(s, webSub) == 3 })
	time.Sleep(50 * time.Millisecond)

	if n := baselineResets(s) - resets; n != 0 {
		t.Errorf("%d baseline reset(s) across the outage, want none", n)
	}
	gotFirst, gotDomains := baselineShape(s, key)
	if !gotFirst.Equal(firstSeen) || !slices.Equal(gotDomains, domains) {
		t.Errorf("baseline = %v over %v after the outage, want it still %v over %v", gotFirst, gotDomains, firstSeen, domains)
	}
}

// scoredDomains is how many domains sub's zone-axis evaluation was scored
// over, or -1 before it has one.
func scoredDomains(s *Source, sub leeway.SubjectRef) int {
	for _, ev := range s.state.EvaluationsOf(sub) {
		if ev.Key == zoneKey {
			return len(ev.Scores.Domains)
		}
	}
	return -1
}

// baselineShape reads the two fields a reset moves.
func baselineShape(s *Source, k baselineKey) (time.Time, []leeway.Domain) {
	s.baselines.mu.Lock()
	defer s.baselines.mu.Unlock()
	b := s.baselines.sets[k]
	if b == nil {
		return time.Time{}, nil
	}
	return b.FirstSeen, slices.Clone(b.Domains)
}

// baselineResets is the §6.5 counter of domain-set resets.
func baselineResets(s *Source) int64 {
	s.baselines.mu.Lock()
	defer s.baselines.mu.Unlock()
	return s.baselines.outcomes[leeway.ObserveReset]
}
