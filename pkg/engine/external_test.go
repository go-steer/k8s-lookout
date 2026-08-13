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
	"strings"
	"testing"
	"time"
)

// TestExternalAncestor_KeySourceIsAttributed pins the storm's answer to
// "why are these one incident?". Without it a storm keyed on a registry
// and a storm keyed on a namespace are indistinguishable downstream,
// and tier-3 mined keys would have nothing to gate on.
func TestExternalAncestor_KeySourceIsAttributed(t *testing.T) {
	t.Parallel()

	t.Run("external extractor", func(t *testing.T) {
		t.Parallel()
		res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
		c, now := newTestCorrelator(t, DefaultStormWindow, DefaultStormMin, res)
		var storm StormInfo
		for i := 0; i < DefaultStormMin; i++ {
			*now = now.Add(time.Second)
			sig := retryablePullSignal(i, "kube-system", *now)
			if v := c.Observe(sig); v.Kind == StormFormed {
				storm = v.Storm
			}
		}
		if storm.KeySource != KeySourceRegistry {
			t.Errorf("KeySource = %q, want %q", storm.KeySource, KeySourceRegistry)
		}
	})

	t.Run("topology ancestor", func(t *testing.T) {
		t.Parallel()
		res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
		var sigs []Signal
		for i := 0; i < DefaultStormMin; i++ {
			s := podSignal(i, "shop")
			res.byObject[s.ref()] = []Ancestor{ancNode}
			sigs = append(sigs, s)
		}
		c, now := newTestCorrelator(t, DefaultStormWindow, DefaultStormMin, res)
		var storm StormInfo
		for _, s := range sigs {
			*now = now.Add(time.Second)
			if v := c.Observe(s); v.Kind == StormFormed {
				storm = v.Storm
			}
		}
		if storm.KeySource != KeySourceTopology {
			t.Errorf("KeySource = %q, want %q", storm.KeySource, KeySourceTopology)
		}
		if storm.Ancestor != ancNode {
			t.Errorf("ancestor = %+v, want %+v", storm.Ancestor, ancNode)
		}
	})
}

// TestExternalAncestor_NewFaultClassNeedsNoCorrelatorChange is the
// whole point of the seam (#225). It adds a blast-radius key for a
// fault class the correlator has never heard of — a shared cloud API
// endpoint — as ONE list entry, and correlates on it. Nothing in
// storm.go knows this key exists.
//
// If a future fault class cannot be expressed this way, that is the
// signal the seam is the wrong shape; this test is where it shows.
func TestExternalAncestor_NewFaultClassNeedsNoCorrelatorChange(t *testing.T) {
	t.Parallel()

	const keySourceCloudAPI = "cloud-api"
	// A wholly invented fault class: pods failing against one cloud
	// metadata endpoint, which is in no topology graph anywhere.
	cloudAPI := ExternalAncestor{
		Name: keySourceCloudAPI,
		Extract: func(sig Signal) (Ancestor, bool) {
			const marker = "endpoint "
			i := strings.Index(sig.Message, marker)
			if i < 0 {
				return Ancestor{}, false
			}
			host := sig.Message[i+len(marker):]
			if j := strings.IndexByte(host, ' '); j >= 0 {
				host = host[:j]
			}
			if host == "" {
				return Ancestor{}, false
			}
			return Ancestor{Kind: "CloudAPI", Name: host}, true
		},
	}

	// Topology deliberately empty: the new key must stand alone.
	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	c, now := newTestCorrelator(t, DefaultStormWindow, DefaultStormMin, res)
	c.external = append([]ExternalAncestor{cloudAPI}, DefaultExternalAncestors...)

	var storm StormInfo
	var formed int
	for i := 0; i < DefaultStormMin; i++ {
		*now = now.Add(time.Second)
		sig := podSignal(i, "shop")
		sig.Message = "failed to reach endpoint metadata.google.internal after 3 tries"
		if v := c.Observe(sig); v.Kind == StormFormed {
			formed++
			storm = v.Storm
		}
	}
	if formed != 1 {
		t.Fatalf("storms formed = %d, want 1", formed)
	}
	want := Ancestor{Kind: "CloudAPI", Name: "metadata.google.internal"}
	if storm.Ancestor != want {
		t.Errorf("ancestor = %+v, want %+v", storm.Ancestor, want)
	}
	if storm.KeySource != keySourceCloudAPI {
		t.Errorf("KeySource = %q, want %q", storm.KeySource, keySourceCloudAPI)
	}
}

// TestExternalAncestor_OutranksTopology: an external key spans
// workloads, so it must win over any Kubernetes ancestor the incidents
// also share. Here every incident shares BOTH a registry and a node —
// keying on the node would split a cluster-wide registry fault across
// however many nodes it touched.
func TestExternalAncestor_OutranksTopology(t *testing.T) {
	t.Parallel()

	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	var sigs []Signal
	for i := 0; i < DefaultStormMin; i++ {
		s := retryablePullSignal(i, "kube-system", time.Time{})
		res.byObject[s.ref()] = []Ancestor{ancNode, ancNS}
		sigs = append(sigs, s)
	}
	c, now := newTestCorrelator(t, DefaultStormWindow, DefaultStormMin, res)

	var storm StormInfo
	for _, s := range sigs {
		*now = now.Add(time.Second)
		s.FirstSeen, s.LastSeen = *now, *now
		if v := c.Observe(s); v.Kind == StormFormed {
			storm = v.Storm
		}
	}
	if storm.Ancestor.Kind != AncestorKindRegistry {
		t.Errorf("ancestor = %+v, want the registry to outrank Node and Namespace", storm.Ancestor)
	}
}

// TestExternalAncestor_DecliningExtractorIsSkipped: an extractor that
// does not apply must contribute no key at all, rather than an empty
// one that would correlate every unrelated incident together.
func TestExternalAncestor_DecliningExtractorIsSkipped(t *testing.T) {
	t.Parallel()

	never := ExternalAncestor{
		Name:    "never",
		Extract: func(Signal) (Ancestor, bool) { return Ancestor{Kind: "Bogus"}, false },
	}
	keys, sources := externalKeys([]ExternalAncestor{never}, podSignal(0, "shop"))
	if len(keys) != 0 || len(sources) != 0 {
		t.Errorf("declining extractor produced keys=%v sources=%v, want none", keys, sources)
	}
}

// retryablePullSignal builds a pull-failure signal already classified
// retryable, as the dispatcher stamps it before the correlator sees it.
func retryablePullSignal(i int, ns string, at time.Time) Signal {
	s := podSignal(i, ns)
	s.Key.Reason = "BackOff"
	s.Message = `Back-off pulling image "` + arHost + `/gke-release/app:v1"`
	s.PullClass = PullClassRetryable
	if !at.IsZero() {
		s.FirstSeen, s.LastSeen = at, at
	}
	return s
}
