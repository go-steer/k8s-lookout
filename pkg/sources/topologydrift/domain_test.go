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
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// hostAxis is kubernetes.io/hostname as a topology axis: one node per domain.
const hostAxis = leeway.TopologyKey(corev1.LabelHostname)

// onHost labels a node with its own hostname, which is what makes
// kubernetes.io/hostname a topology axis with one node per domain.
func onHost(name string) nodeOpt { return withLabel(corev1.LabelHostname, name) }

// domainNames is the latched domain set for one axis, as strings.
func domainNames(s *Source, key leeway.TopologyKey) []string {
	var out []string
	for _, d := range s.knownDomains(key) {
		out = append(out, string(d))
	}
	return out
}

// breachedDomains is the domains one pass judged unavailable, on any axis.
func breachedDomains(s *Source) []string {
	var out []string
	for _, j := range s.domainJudgements(time.Now()) {
		if j.Breached {
			out = append(out, j.Subject.Name)
		}
	}
	slices.Sort(out)
	return out
}

// The latch is the memory that keeps a dead zone reported after the evidence
// for it has aged out, and the two conditions it forgets on are what keep it
// bounded. Both halves are here because the first one alone would be a leak.
func TestSampleDomains_LatchesWhatTheClusterHasAndForgetsWhatAgedOut(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	s, _ := nodeGroupSource(t, Config{},
		node("n-a", "us-central1-a"), node("n-b", "us-central1-b"), node("n-c", "us-central1-c"))

	s.inv.SampleReady(t0, s.cfg.readyRetention())
	s.sampleDomains(t0)
	if got, want := domainNames(s, zoneKey), []string{"us-central1-a", "us-central1-b", "us-central1-c"}; !slices.Equal(got, want) {
		t.Fatalf("latched %v, want %v", got, want)
	}

	// The zone is deleted outright: no nodes, no census row, nothing but the
	// ready series to say it was ever there.
	s.inv.Remove("n-c")
	t1 := t0.Add(time.Minute)
	s.inv.SampleReady(t1, s.cfg.readyRetention())
	s.sampleDomains(t1)
	if got := domainNames(s, zoneKey); !slices.Contains(got, "us-central1-c") {
		t.Errorf("latched %v after the zone was deleted, want us-central1-c still there: dropping it here is the finding resolving itself while the zone is still gone", got)
	}
	if got, want := breachedDomains(s), []string{"us-central1-c"}; !slices.Equal(got, want) {
		t.Errorf("breached = %v, want %v", got, want)
	}

	// Past the retention, the series is empty and there is nothing left to
	// distinguish a zone deleted on purpose from one that failed. The entry
	// goes, which is what bounds the map.
	t2 := t1.Add(2 * s.cfg.readyRetention())
	s.inv.SampleReady(t2, s.cfg.readyRetention())
	s.sampleDomains(t2)
	if got := domainNames(s, zoneKey); slices.Contains(got, "us-central1-c") {
		t.Errorf("latched %v once the history aged out, want us-central1-c forgotten — the latch is unbounded otherwise", got)
	}
	if got := breachedDomains(s); len(got) != 0 {
		t.Errorf("breached = %v, want none once the domain is forgotten", got)
	}
}

// A process that starts while a zone is already down has no fall to have
// watched. The nodes are there and none of them can take a pod, which is the
// whole claim, and refusing to make it would tie the finding to whether lookout
// happened to be running an hour ago.
func TestSampleDomains_ADomainFirstSeenAlreadyDeadIsStillKnown(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	s, _ := nodeGroupSource(t, Config{},
		node("n-a", "us-central1-a"),
		node("n-c1", "us-central1-c", notReady()), node("n-c2", "us-central1-c", notReady()))

	s.sampleDomains(now)
	if got, want := breachedDomains(s), []string{"us-central1-c"}; !slices.Equal(got, want) {
		t.Errorf("breached = %v, want %v", got, want)
	}
}

// Every known domain is judged, not only the broken ones: Alerts.Pass reads the
// absence of a judgement as "this subject is gone" and abandons the episode
// without resolving it, so a zone that came back would have its finding dropped
// on the floor rather than cleared.
func TestDomainJudgements_HealthyDomainsAreJudgedToo(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	s, _ := nodeGroupSource(t, Config{},
		node("n-a", "us-central1-a"), node("n-b", "us-central1-b"))
	s.sampleDomains(now)

	js := s.domainJudgements(now)
	if len(js) != 2 {
		t.Fatalf("judged %d domain(s), want 2 — a healthy domain has to be present and saying so", len(js))
	}
	for _, j := range js {
		if j.Subject.Kind != leeway.SubjectDomain {
			t.Errorf("subject kind = %q, want %q", j.Subject.Kind, leeway.SubjectDomain)
		}
		if j.Breached {
			t.Errorf("%s judged breached, want healthy", j.Subject.Name)
		}
		if j.Tier != leeway.TierB {
			t.Errorf("%s tier = %v, want B (§2.3's table)", j.Subject.Name, j.Tier)
		}
	}
}

// The defect this default exists to prevent. kubernetes.io/hostname is a
// topology key like any other and every node is its own domain on it, so an
// unguarded detector turns a rolling upgrade into one finding per node — an
// observation objectstate already makes, with the node as the subject.
func TestDomainAxes_APerNodeAxisIsNotJudgedByDefault(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	nodes := []*corev1.Node{
		node("n-a", "us-central1-a", onHost("n-a")),
		node("n-b", "us-central1-b", onHost("n-b"), notReady()),
	}
	cfg := Config{TopologyKeys: []leeway.TopologyKey{zoneKey, hostAxis}}

	s, _ := nodeGroupSource(t, cfg, nodes...)
	s.sampleDomains(now)
	if got, want := s.domainAxes(), []leeway.TopologyKey{zoneKey}; !slices.Equal(got, want) {
		t.Fatalf("domainAxes() = %v, want %v — the hostname axis is scored but must not be judged", got, want)
	}
	// One dead node, one finding, and it names the zone rather than the node.
	if got, want := breachedDomains(s), []string{"us-central1-b"}; !slices.Equal(got, want) {
		t.Errorf("breached = %v, want %v", got, want)
	}

	// Named explicitly it is honoured — the list is configuration, because a
	// rack or a cell label is a real failure domain on some estates. What the
	// default refuses is doing this to an operator who asked for nothing.
	cfg.DomainUnavailableKeys = []leeway.TopologyKey{hostAxis}
	s2, _ := nodeGroupSource(t, cfg, nodes...)
	s2.sampleDomains(now)
	if got, want := breachedDomains(s2), []string{"n-b"}; !slices.Equal(got, want) {
		t.Errorf("breached = %v, want %v once the hostname axis is named", got, want)
	}
}

// Nil means "take the default" everywhere else in Config, so the off switch has
// to be the empty slice — and it has to survive normalization to mean anything.
func TestDomainAxes_AnEmptyKeyListIsTheOffSwitch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	s, _ := nodeGroupSource(t, Config{DomainUnavailableKeys: []leeway.TopologyKey{}},
		node("n-a", "us-central1-a"), node("n-b", "us-central1-b", notReady()))

	s.sampleDomains(now)
	if got := s.domainAxes(); len(got) != 0 {
		t.Errorf("domainAxes() = %v, want none", got)
	}
	if got := s.domainJudgements(now); got != nil {
		t.Errorf("judgements = %v, want none — the detector is off", got)
	}
	if got := s.unavailableDomains(); got != nil {
		t.Errorf("gauge = %v, want no series at all when the detector is off", got)
	}
	// And the latch is never written, so the off switch costs no memory
	// either.
	if got := domainNames(s, zoneKey); len(got) != 0 {
		t.Errorf("latched %v with the detector off, want nothing", got)
	}
}

// A configured axis the inventory does not index has no domains and no history.
// Judging it would be judging the empty set on every pass.
func TestDomainAxes_AnUnindexedAxisIsSkipped(t *testing.T) {
	t.Parallel()

	s, _ := nodeGroupSource(t, Config{
		TopologyKeys:          []leeway.TopologyKey{zoneKey},
		DomainUnavailableKeys: []leeway.TopologyKey{zoneKey, "topology.kubernetes.io/region"},
	}, node("n-a", "us-central1-a"))

	if got, want := s.domainAxes(), []leeway.TopologyKey{zoneKey}; !slices.Equal(got, want) {
		t.Errorf("domainAxes() = %v, want %v", got, want)
	}
}

// Zero is the reading that matters: a gauge that only appears during an outage
// cannot be alerted on before the first one.
func TestUnavailableDomains_ReportsZeroForEveryConfiguredAxis(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	s, _ := nodeGroupSource(t, Config{}, node("n-a", "us-central1-a"), node("n-b", "us-central1-b"))
	s.sampleDomains(now)

	got := s.unavailableDomains()
	if len(got) != 1 || got[zoneKey] != 0 {
		t.Fatalf("gauge = %v, want one series for the zone axis at 0", got)
	}

	s.inv.Upsert(node("n-b", "us-central1-b", notReady()))
	if got := s.unavailableDomains(); got[zoneKey] != 1 {
		t.Errorf("gauge = %v, want 1 on the zone axis", got)
	}
}

// §2.3's subject holds no objects by construction, so the pod index is the
// wrong question: Tracked is false for every domain, and the workload branch
// would report a zone that is still down as recovered-object_deleted on the
// first observation.
func TestClearance_ADomainSubjectIsNotReportedObjectDeleted(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	s, _ := nodeGroupSource(t, Config{Dwell: leeway.Dwell{For: time.Nanosecond, Resolve: time.Hour}},
		node("n-a", "us-central1-a"), node("n-c", "us-central1-c", notReady()))
	s.sampleDomains(now)

	sub := leeway.SubjectRef{Kind: leeway.SubjectDomain, Name: "us-central1-c"}
	inc := engine.Incident{Key: engine.EventKey{UID: findingUID(sub, zoneKey)}}

	// Two passes to cross the one-nanosecond dwell.
	s.alerts.Pass(s.domainJudgements(now), now, 0)
	s.alerts.Pass(s.domainJudgements(now), now.Add(time.Second), 0)

	c, ok := s.Clearance(inc)
	if !ok {
		t.Fatal("Clearance declined a leeway domain incident, want it owned")
	}
	if c.Cleared {
		t.Errorf("clearance = %+v, want not cleared: the zone is still down", c)
	}

	// The zone comes back. The episode resolves through the machine, which is
	// the clearance — not a second signal.
	s.inv.Upsert(node("n-c", "us-central1-c"))
	later := now.Add(time.Minute)
	s.alerts.Pass(s.domainJudgements(later), later, 0)
	// Still not cleared: PhaseResolving reports firing, which is §8.2's
	// resolve dwell and not a second one of this observer's.
	if c, _ := s.Clearance(inc); c.Cleared {
		t.Errorf("clearance = %+v inside the resolve dwell, want the episode still held open", c)
	}
	past := later.Add(2 * time.Hour)
	s.alerts.Pass(s.domainJudgements(past), past, 0)

	c, ok = s.Clearance(inc)
	if !ok || !c.Cleared {
		t.Fatalf("clearance = %+v (owned=%v), want cleared once the zone is back", c, ok)
	}
	if c.Resolution == engine.ResolutionObjectDeleted {
		t.Errorf("resolution = %q, want recovered: a domain has no object to delete", c.Resolution)
	}
}

// runDomainSource is runAlerting with the emitted signals collected, and with
// the ready tick short enough that the latch refreshes inside a test.
func runDomainSource(t *testing.T, objs ...runtime.Object) (*Source, func() []sources.Signal) {
	t.Helper()
	cfg := quickAlerts()
	cfg.ReadySampleInterval = 5 * time.Millisecond
	// Tier C would be metrics-only by default, which would make "no workload
	// emitted" true for the wrong reason. With the opt-in on, the only thing
	// keeping the twenty quiet is §7.6.
	cfg.TierCSignals = true

	s := New(fake.NewSimpleClientset(objs...), cfg)
	s.logf = func(string, ...any) {}

	var mu sync.Mutex
	var got []sources.Signal
	emit := func(sig sources.Signal) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, sig)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, emit) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v, want nil on cancellation", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancellation")
		}
	})
	waitFor(t, "the source to sync", s.HasSynced)

	return s, func() []sources.Signal {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
}

// Phase 7's exit criterion, end to end through Run: a zone whose nodes all go
// away produces exactly one leeway.domain_unavailable, and the workloads that
// drifted because of it stay suppressed rather than each emitting their own.
func TestSource_ADeadZoneIsExactlyOneFindingAndTheWorkloadsStayQuiet(t *testing.T) {
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
	if got := signals(); len(got) != 0 {
		t.Fatalf("%d signal(s) before anything happened: %v", len(got), got)
	}

	// The zone goes, and every workload's pods there land in the survivors.
	// Each of the twenty is now 2/2/0 against an expectation it can no longer
	// meet, in the same second.
	s.inv.Remove("n-c1")
	s.inv.Remove("n-c2")
	for w := range workloads {
		name := fmt.Sprintf("app-%d", w)
		for i, n := range []string{"n-a", "n-b"} {
			s.state.OnPodAdd(pod(fmt.Sprintf("%s-%d", name, i+2), "prod", n, ownedBy("ReplicaSet", name+"-7c9f")))
		}
	}

	waitFor(t, "the domain finding to reach the wire", func() bool {
		return len(signals()) > 0
	})
	// Long enough for several alert ticks at the five-millisecond interval, so
	// that "one signal" means the rest were declined rather than pending.
	time.Sleep(100 * time.Millisecond)

	got := signals()
	if len(got) != 1 {
		t.Fatalf("%d signal(s) for one dead zone, want exactly 1: %v", len(got), got)
	}
	sig := got[0]
	if sig.Kind != leeway.KindDomainUnavailable {
		t.Errorf("kind = %q, want %q", sig.Kind, leeway.KindDomainUnavailable)
	}
	if sig.KindOfObject != string(leeway.SubjectDomain) || sig.Name != "us-central1-c" {
		t.Errorf("subject = %s/%s, want Domain/us-central1-c — the subject is the domain, not a workload", sig.KindOfObject, sig.Name)
	}
	if sig.Key.Reason != string(leeway.CauseConsolidation) {
		t.Errorf("reason = %q, want %q: the nodes were removed", sig.Key.Reason, leeway.CauseConsolidation)
	}
	if sig.Severity != engine.Severity(leeway.TierB.Severity()) {
		t.Errorf("severity = %q, want %q", sig.Severity, leeway.TierB.Severity())
	}
}

// The other half of the exit criterion, and the reason for the rename: the same
// kind with a different suspected cause. Every node in the zone is cordoned —
// all present, all Ready, none able to take a pod.
func TestSource_ACordonedZoneIsTheSameKindWithADifferentCause(t *testing.T) {
	objs := []runtime.Object{
		node("n-a", "us-central1-a"), node("n-b", "us-central1-b"),
		node("n-c1", "us-central1-c"), node("n-c2", "us-central1-c"),
	}
	s, signals := runDomainSource(t, objs...)

	waitFor(t, "the zone to be latched", func() bool {
		return slices.Contains(domainNames(s, zoneKey), "us-central1-c")
	})

	s.inv.Upsert(node("n-c1", "us-central1-c", cordoned()))
	s.inv.Upsert(node("n-c2", "us-central1-c", cordoned()))

	waitFor(t, "the domain finding to reach the wire", func() bool { return len(signals()) > 0 })

	got := signals()[0]
	if got.Kind != leeway.KindDomainUnavailable {
		t.Fatalf("kind = %q, want %q", got.Kind, leeway.KindDomainUnavailable)
	}
	if got.Key.Reason != string(leeway.CauseTaintExclusion) {
		t.Errorf("reason = %q, want %q: a cordon is the unschedulable taint, and every node is still Ready", got.Key.Reason, leeway.CauseTaintExclusion)
	}
}
