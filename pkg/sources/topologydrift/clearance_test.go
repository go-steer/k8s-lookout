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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

var _ engine.ClearanceObserver = (*Source)(nil)

// clearanceSource is a synced source holding one counted pod for webSubject, so
// the observer's "does this workload still exist" test has something to find.
func clearanceSource(t *testing.T) *Source {
	t.Helper()
	objs := []runtime.Object{
		node("n-a", "us-central1-a"),
		node("n-b", "us-central1-b"),
		replicaSet("web-7c9f", "prod", "web"),
		pod("web-1", "prod", "n-a", ownedBy("ReplicaSet", "web-7c9f")),
	}
	s, _ := runSource(t, Config{TopologyKeys: []leeway.TopologyKey{zoneKey}}, objs...)
	return s
}

// incident is an engine.Incident carrying one of this source's synthetic UIDs.
func incident(sub leeway.SubjectRef, key leeway.TopologyKey) engine.Incident {
	return engine.Incident{Key: engine.EventKey{UID: findingUID(sub, key), Reason: string(leeway.CauseUnknown)}}
}

func TestSource_ClearanceDeclinesForeignIncidents(t *testing.T) {
	s := clearanceSource(t)
	for _, uid := range []string{"nodegroup:default-pool", "5f3c2e10-0000-4000-8000-000000000000", ""} {
		if _, ok := s.Clearance(engine.Incident{Key: engine.EventKey{UID: uid}}); ok {
			t.Errorf("claimed incident %q, which this source did not raise", uid)
		}
	}
}

func TestSource_ClearanceDeclinesBeforeTheCachesSync(t *testing.T) {
	// The one wrong answer that is also irreversible: an empty mirror makes
	// every subject look deleted, and object_deleted is a terminal resolution.
	s := New(fake.NewSimpleClientset(), Config{TopologyKeys: []leeway.TopologyKey{zoneKey}})
	if _, ok := s.Clearance(incident(webSubject, zoneKey)); ok {
		t.Error("an unsynced source judged an incident")
	}
}

func TestSource_ClearanceHoldsAFiringEpisodeOpen(t *testing.T) {
	s := clearanceSource(t)
	now := t0
	s.alerts.Pass([]Judgement{judged(zoneKey, true)}, now, 0)
	now = now.Add(leeway.DefaultDwell().For + time.Minute)
	out := only(t, s.alerts.Pass([]Judgement{judged(zoneKey, true)}, now, 0))
	if out.Transition != leeway.TransitionFiring {
		t.Fatalf("the episode did not fire: %+v", out)
	}

	c, ok := s.Clearance(incident(webSubject, zoneKey))
	if !ok {
		t.Fatal("the source declined its own incident")
	}
	if c.Cleared {
		t.Errorf("a firing episode was reported clear: %+v", c)
	}
}

func TestSource_ClearanceHoldsThroughTheResolveDwell(t *testing.T) {
	// §8.2 keeps the finding outstanding for Dwell.Resolve after the breach
	// stops, and PhaseResolving reports Firing. This observer must agree for
	// the whole of it — the recovery tracker's own stability window runs on
	// top, it does not replace this one.
	s := clearanceSource(t)
	now := t0
	s.alerts.Pass([]Judgement{judged(zoneKey, true)}, now, 0)
	now = now.Add(leeway.DefaultDwell().For + time.Minute)
	s.alerts.Pass([]Judgement{judged(zoneKey, true)}, now, 0)

	now = now.Add(time.Minute)
	s.alerts.Pass([]Judgement{judged(zoneKey, false)}, now, 0)
	if st, live := s.alerts.StateOf(webSubject, zoneKey); !live || !st.Phase.Firing() {
		t.Fatalf("the episode left the machine as soon as the breach stopped: %+v live=%v", st, live)
	}

	c, _ := s.Clearance(incident(webSubject, zoneKey))
	if c.Cleared {
		t.Errorf("cleared inside the resolve dwell: %+v", c)
	}
}

func TestSource_ClearanceClearsAResolvedEpisode(t *testing.T) {
	s := clearanceSource(t)
	now := t0
	s.alerts.Pass([]Judgement{judged(zoneKey, true)}, now, 0)
	now = now.Add(leeway.DefaultDwell().For + time.Minute)
	s.alerts.Pass([]Judgement{judged(zoneKey, true)}, now, 0)
	now = now.Add(time.Minute)
	s.alerts.Pass([]Judgement{judged(zoneKey, false)}, now, 0)
	now = now.Add(leeway.DefaultDwell().Resolve + time.Minute)
	s.alerts.Pass([]Judgement{judged(zoneKey, false)}, now, 0)

	if _, live := s.alerts.StateOf(webSubject, zoneKey); live {
		t.Fatal("the episode is still in the machine after its resolve dwell")
	}

	c, ok := s.Clearance(incident(webSubject, zoneKey))
	if !ok || !c.Cleared {
		t.Fatalf("a resolved episode was not cleared: %+v ok=%v", c, ok)
	}
	if c.Resolution != engine.ResolutionRecovered {
		t.Errorf("Resolution = %q, want recovered — the drift went away, the workload did not", c.Resolution)
	}
	if !c.StableSince.IsZero() {
		t.Errorf("StableSince = %v, want the zero time; the ClearSince left with the episode", c.StableSince)
	}
}

func TestSource_ClearanceReportsADeletedSubject(t *testing.T) {
	s := clearanceSource(t)
	gone := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "never-existed"}

	c, ok := s.Clearance(incident(gone, zoneKey))
	if !ok || !c.Cleared {
		t.Fatalf("an untracked subject was not cleared: %+v ok=%v", c, ok)
	}
	if c.Resolution != engine.ResolutionObjectDeleted {
		t.Errorf("Resolution = %q, want object_deleted", c.Resolution)
	}
}

func TestSource_ClearanceIsPerAxis(t *testing.T) {
	// Two axes, one subject, two episodes. Clearing the region finding must not
	// clear the zone one — which is exactly what the axis in the UID buys.
	s := clearanceSource(t)
	now := t0
	s.alerts.Pass([]Judgement{judged(zoneKey, true)}, now, 0)
	now = now.Add(leeway.DefaultDwell().For + time.Minute)
	s.alerts.Pass([]Judgement{judged(zoneKey, true)}, now, 0)

	if c, _ := s.Clearance(incident(webSubject, zoneKey)); c.Cleared {
		t.Error("the zone episode was cleared while firing")
	}
	if c, _ := s.Clearance(incident(webSubject, regionKey)); !c.Cleared {
		t.Error("the region axis, which never had an episode, was held open")
	}
}
