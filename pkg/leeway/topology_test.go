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

package leeway

import (
	"reflect"
	"testing"
)

func TestSubjectRef_String(t *testing.T) {
	tests := []struct {
		in   SubjectRef
		want string
	}{
		{SubjectRef{Kind: SubjectDeployment, Namespace: "prod", Name: "api"}, "Deployment/prod/api"},
		{SubjectRef{Kind: SubjectNodeGroup, Name: "pool-1"}, "NodeGroup/pool-1"},
		{SubjectRef{Kind: SubjectStatefulSet, Namespace: "db", Name: "pg"}, "StatefulSet/db/pg"},
	}
	for _, tc := range tests {
		if got := tc.in.String(); got != tc.want {
			t.Errorf("SubjectRef%+v.String() = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseSubjectRef_RoundTripsEverythingStringCanWrite(t *testing.T) {
	// §9.1 keys the persisted alert state by the rendered subject, so this is
	// the only way a restored row gets back to a SubjectRef. Anything String
	// can produce has to survive the trip.
	for _, want := range []SubjectRef{
		{Kind: SubjectDeployment, Namespace: "prod", Name: "api"},
		{Kind: SubjectNodeGroup, Name: "pool-1"},
		{Kind: SubjectStatefulSet, Namespace: "db", Name: "pg"},
	} {
		got, ok := ParseSubjectRef(want.String())
		if !ok || got != want {
			t.Errorf("ParseSubjectRef(%q) = %+v, %v; want %+v, true", want.String(), got, ok, want)
		}
	}
}

func TestParseSubjectRef_RefusesWhatItCannotRead(t *testing.T) {
	// A row written by a different build, or a truncated one. Refusing is the
	// §9.3 policy — a record that cannot be read back to a subject would
	// otherwise dwell forever against a subject nothing can match it to.
	for _, in := range []string{
		"",
		"Deployment",
		"Deployment/prod/api/extra",
		"/prod/api",
		"Deployment//api",
		"Deployment/prod/",
		"/api",
		"Deployment/",
	} {
		if got, ok := ParseSubjectRef(in); ok {
			t.Errorf("ParseSubjectRef(%q) = %+v, true; want a refusal", in, got)
		}
	}
}

func TestCountState_String(t *testing.T) {
	want := map[CountState]string{
		StateRunning:       "running",
		StatePending:       "pending",
		StateUnschedulable: "unschedulable",
		StateTerminating:   "terminating",
	}
	seen := map[string]bool{}
	for s, w := range want {
		got := s.String()
		if got != w {
			t.Errorf("CountState(%d) = %q, want %q", s, got, w)
		}
		if seen[got] {
			t.Errorf("duplicate state label %q", got)
		}
		seen[got] = true
	}
	if got := CountState(9).String(); got != "unknown" {
		t.Errorf("out-of-range state = %q, want unknown", got)
	}
}

// TestDomainCount_PinnedOverlapsRatherThanPartitions is the invariant the
// Pinned doc comment claims. If Total counted it, a distribution of pinned pods
// would double every tally and the apportionment would be scored against twice
// the replica count.
func TestDomainCount_PinnedOverlapsRatherThanPartitions(t *testing.T) {
	var c DomainCount
	c.Add(StateRunning, true)
	c.Add(StateRunning, false)
	c.Add(StatePending, false)
	c.Add(StateUnschedulable, true)
	c.Add(StateTerminating, false)

	want := DomainCount{Running: 2, Pending: 1, Unschedulable: 1, Terminating: 1, Pinned: 2}
	if c != want {
		t.Errorf("DomainCount = %+v, want %+v", c, want)
	}
	if got := c.Total(); got != 5 {
		t.Errorf("Total() = %d, want 5 — Pinned must not be added on top of the states", got)
	}
}

func TestDomainCount_AddIgnoresUnknownState(t *testing.T) {
	var c DomainCount
	c.Add(CountState(42), false)
	if c.Total() != 0 {
		t.Errorf("an unrecognised state incremented something: %+v", c)
	}
	// Pinned is orthogonal to the state switch, so it still lands. That is the
	// right behaviour: the pin is a fact about the pod, not about its phase.
	c.Add(CountState(42), true)
	if c.Pinned != 1 {
		t.Errorf("Pinned = %d, want 1", c.Pinned)
	}
}

func TestDistribution_AddAndTotal(t *testing.T) {
	d := NewDistribution()
	d.Add("a", StateRunning, false)
	d.Add("a", StateRunning, false)
	d.Add("b", StatePending, true)

	if d.Total != 3 {
		t.Errorf("Total = %d, want 3", d.Total)
	}
	if got := d.ByDomain["a"].Running; got != 2 {
		t.Errorf("a.Running = %d, want 2", got)
	}
	if got := d.ByDomain["b"].Pinned; got != 1 {
		t.Errorf("b.Pinned = %d, want 1", got)
	}
}

// TestDistribution_AddRemoveRoundTrip is the property the delta rules depend
// on: Remove with the same arguments undoes Add exactly, Pinned included.
// Pinned overlaps the state counters rather than partitioning them, so an
// asymmetry between the two would not show up as a wrong answer on the next
// event — it would show up as a counter that drifts one per move and is
// meaningfully wrong a week later.
func TestDistribution_AddRemoveRoundTrip(t *testing.T) {
	states := []CountState{StateRunning, StatePending, StateUnschedulable, StateTerminating}
	for _, state := range states {
		for _, pinned := range []bool{false, true} {
			d := NewDistribution()
			d.Add("a", state, pinned)
			d.Remove("a", state, pinned)

			if d.Total != 0 {
				t.Errorf("state %v pinned %v: Total = %d, want 0", state, pinned, d.Total)
			}
			if len(d.ByDomain) != 0 {
				t.Errorf("state %v pinned %v: ByDomain = %+v, want empty", state, pinned, d.ByDomain)
			}
		}
	}
}

// TestDistribution_RemoveDropsEmptyDomains: a subject rescheduled around the
// cluster over months must not accumulate a zero row per domain it ever
// touched. Dropping them is safe because the eligible domain list comes from
// the node inventory and Counts already yields zero for an absent domain.
func TestDistribution_RemoveDropsEmptyDomains(t *testing.T) {
	d := NewDistribution()
	d.Add("a", StateRunning, false)
	d.Add("a", StatePending, false)
	d.Add("b", StateRunning, false)

	d.Remove("a", StateRunning, false)
	if _, present := d.ByDomain["a"]; !present {
		t.Error("a domain that still holds a pending pod was dropped")
	}
	d.Remove("a", StatePending, false)
	if _, present := d.ByDomain["a"]; present {
		t.Errorf("an emptied domain survived as a zero row: %+v", d.ByDomain)
	}

	if d.Total != 1 || d.ByDomain["b"].Running != 1 {
		t.Errorf("the untouched domain moved: %+v", d)
	}
	if got := d.Counts([]Domain{"a", "b"}, StateRunning); got[0] != 0 {
		t.Errorf("Counts on a dropped domain = %d, want 0", got[0])
	}
}

// TestDistribution_RemoveWhatWasNeverAdded: unreachable through the delta rules,
// which decrement from a stored Placement. Guarded anyway because an informer
// handler is the wrong place to turn a bookkeeping slip into a negative count
// every downstream score then inherits.
func TestDistribution_RemoveWhatWasNeverAdded(t *testing.T) {
	d := NewDistribution()
	d.Add("a", StateRunning, false)

	d.Remove("b", StateRunning, false)
	d.Remove("", StateRunning, false)

	if d.Total != 1 {
		t.Errorf("Total = %d, want 1", d.Total)
	}
	if len(d.ByDomain) != 1 {
		t.Errorf("ByDomain = %+v, want just a", d.ByDomain)
	}

	var zero Distribution
	zero.Remove("a", StateRunning, false)
	if zero.Total != 0 {
		t.Errorf("a zero-value Distribution went negative: %+v", zero)
	}
}

// TestDistribution_RemoveNormalisesTheEmptyDomain mirrors Add: the two must
// agree on which bucket "" means, or a pod added to DomainUnknown could never
// be removed.
func TestDistribution_RemoveNormalisesTheEmptyDomain(t *testing.T) {
	d := NewDistribution()
	d.Add("", StateRunning, false)
	d.Remove(DomainUnknown, StateRunning, false)

	if d.Total != 0 || len(d.ByDomain) != 0 {
		t.Errorf("the unknown bucket did not round-trip: %+v", d)
	}
}

func TestDomainCount_SubMirrorsAdd(t *testing.T) {
	var c DomainCount
	c.Add(StateRunning, true)
	c.Sub(StateRunning, true)
	if !c.IsZero() {
		t.Errorf("Sub did not undo Add: %+v", c)
	}

	// An out-of-range state is ignored by both, symmetrically.
	c.Add(CountState(99), false)
	c.Sub(CountState(99), false)
	if !c.IsZero() {
		t.Errorf("an unknown state left a residue: %+v", c)
	}

	// IsZero counts Pinned, which is the case that matters: a pinned pod's
	// state counter and its Pinned counter must both come back to zero before
	// the domain row is dropped.
	c.Add(StateRunning, true)
	c.Sub(StateRunning, false)
	if c.IsZero() {
		t.Error("IsZero ignored a stranded Pinned count")
	}
}

func TestDistribution_Clone(t *testing.T) {
	// §6.5's verifier compares a snapshot against counts that are still moving
	// underneath it.
	d := NewDistribution()
	d.Add("a", StateRunning, true)
	d.Add("b", StatePending, false)

	clone := d.Clone()
	d.Add("a", StateRunning, false)
	d.Add("c", StateRunning, false)

	if got := clone.ByDomain["a"].Running; got != 1 {
		t.Errorf("clone followed the original: a.Running = %d, want 1", got)
	}
	if clone.Total != 2 {
		t.Errorf("clone.Total = %d, want 2", clone.Total)
	}
	if _, present := clone.ByDomain["c"]; present {
		t.Error("a domain added after the clone appeared in it")
	}
}

func TestDistribution_Equal(t *testing.T) {
	build := func(f func(*Distribution)) *Distribution {
		d := NewDistribution()
		f(d)
		return d
	}
	base := build(func(d *Distribution) {
		d.Add("a", StateRunning, false)
		d.Add("b", StatePending, true)
	})

	tests := []struct {
		name  string
		other *Distribution
		want  bool
	}{
		{"same counts", base.Clone(), true},
		{
			"different state in the same domain",
			build(func(d *Distribution) {
				d.Add("a", StateTerminating, false)
				d.Add("b", StatePending, true)
			}),
			false,
		},
		{
			// The case the verifier exists to catch: the totals agree and the
			// distribution does not.
			"same total, different domains",
			build(func(d *Distribution) {
				d.Add("a", StateRunning, false)
				d.Add("a", StatePending, true)
			}),
			false,
		},
		{
			"pinned differs only",
			build(func(d *Distribution) {
				d.Add("a", StateRunning, false)
				d.Add("b", StatePending, false)
			}),
			false,
		},
		{"an extra domain", build(func(d *Distribution) {
			d.Add("a", StateRunning, false)
			d.Add("b", StatePending, true)
			d.Add("c", StateRunning, false)
		}), false},
		{"empty", NewDistribution(), false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := base.Equal(tc.other); got != tc.want {
				t.Errorf("Equal = %v, want %v", got, tc.want)
			}
			if tc.other != nil {
				if got := tc.other.Equal(base); got != tc.want {
					t.Errorf("reversed Equal = %v, want %v", got, tc.want)
				}
			}
		})
	}

	var nilDist *Distribution
	if !nilDist.Equal(nil) {
		t.Error("two absent distributions are not equal")
	}
	if nilDist.Equal(base) {
		t.Error("an absent distribution equals a populated one")
	}
}

// TestDistribution_EmptyDomainNormalises: a caller that forgets to normalise
// must not create a second, invisible bucket keyed by "" that never appears in
// any domain list and silently drops pods out of the distribution.
func TestDistribution_EmptyDomainNormalises(t *testing.T) {
	d := NewDistribution()
	d.Add("", StateRunning, false)
	d.Add(DomainUnknown, StateRunning, false)

	if len(d.ByDomain) != 1 {
		t.Fatalf("ByDomain has %d buckets, want 1: %v", len(d.ByDomain), d.ByDomain)
	}
	if got := d.ByDomain[DomainUnknown].Running; got != 2 {
		t.Errorf("unknown bucket = %d, want 2", got)
	}
}

// TestDistribution_ZeroValueIsUsable: Distribution is reachable as a struct
// literal from other packages, and a nil map panicking on the first Add would
// be a crash in the source layer rather than a compile error here.
func TestDistribution_ZeroValueIsUsable(t *testing.T) {
	var d Distribution
	d.Add("a", StateRunning, false)
	if d.Total != 1 || d.ByDomain["a"].Running != 1 {
		t.Errorf("zero-value Distribution mishandled Add: %+v", d)
	}
}

// TestDistribution_CountsProjectsOntoEligibleDomains is the seam between
// counting and scoring. The projection must honour the *eligible* domain list,
// not the distribution's own keys.
func TestDistribution_CountsProjectsOntoEligibleDomains(t *testing.T) {
	d := NewDistribution()
	d.Add("a", StateRunning, false)
	d.Add("a", StatePending, false)
	d.Add("c", StateRunning, false)

	// "b" is eligible and empty; "c" holds a pod but is not eligible.
	got := d.Counts([]Domain{"a", "b"}, StateRunning)
	if want := []int64{1, 0}; !reflect.DeepEqual(got, want) {
		t.Errorf("Counts = %v, want %v", got, want)
	}

	// An eligible domain holding nothing is the most important cell in the
	// calculation; dropping it would make an empty zone indistinguishable from
	// a zone that does not exist.
	if len(got) != 2 {
		t.Errorf("Counts returned %d cells for 2 domains", len(got))
	}
}

func TestDistribution_CountsSumsRequestedStates(t *testing.T) {
	d := NewDistribution()
	d.Add("a", StateRunning, false)
	d.Add("a", StatePending, false)
	d.Add("a", StateTerminating, false)
	d.Add("a", StateUnschedulable, false)

	tests := []struct {
		name   string
		states []CountState
		want   int64
	}{
		{"running only", []CountState{StateRunning}, 1},
		{"running and pending", []CountState{StateRunning, StatePending}, 2},
		{"all four", []CountState{StateRunning, StatePending, StateUnschedulable, StateTerminating}, 4},
		// §7.6: excluding Terminating is how a rollout stops looking like drift.
		{"steady state", []CountState{StateRunning, StatePending, StateUnschedulable}, 3},
		{"none requested", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := d.Counts([]Domain{"a"}, tc.states...); got[0] != tc.want {
				t.Errorf("Counts(%v) = %d, want %d", tc.states, got[0], tc.want)
			}
		})
	}
}

func TestDistribution_CountsNoDomains(t *testing.T) {
	d := NewDistribution()
	d.Add("a", StateRunning, false)
	if got := d.Counts(nil, StateRunning); len(got) != 0 {
		t.Errorf("Counts(nil) = %v, want empty", got)
	}
}

func TestDistribution_DomainsAreCanonicallyOrdered(t *testing.T) {
	d := NewDistribution()
	for _, dom := range []Domain{"z", DomainUnknown, "a", "m"} {
		d.Add(dom, StateRunning, false)
	}
	want := []Domain{"a", "m", "z", DomainUnknown}
	// Repeated because the bug this guards against is map-iteration order,
	// which a single call can pass by luck.
	for i := 0; i < 50; i++ {
		if got := d.Domains(); !reflect.DeepEqual(got, want) {
			t.Fatalf("Domains = %v, want %v", got, want)
		}
	}
}

func TestSortDomains(t *testing.T) {
	tests := []struct {
		name string
		in   []Domain
		want []Domain
	}{
		{"unknown sorts last", []Domain{DomainUnknown, "b", "a"}, []Domain{"a", "b", DomainUnknown}},
		{
			// The sentinel starts with an underscore, which sorts *before*
			// every lowercase letter. Plain lexicographic ordering would put it
			// first and let it displace a real domain in a finding body that
			// lists the worst few.
			"lexicographic order alone would put it first",
			[]Domain{"us-central1-a", DomainUnknown},
			[]Domain{"us-central1-a", DomainUnknown},
		},
		{"already sorted", []Domain{"a", "b", "c"}, []Domain{"a", "b", "c"}},
		{"reversed", []Domain{"c", "b", "a"}, []Domain{"a", "b", "c"}},
		{"single", []Domain{"a"}, []Domain{"a"}},
		{"empty", []Domain{}, []Domain{}},
		{"only unknown", []Domain{DomainUnknown}, []Domain{DomainUnknown}},
		{
			"synthetic domains keep a stable relative order",
			[]Domain{syntheticDomain(2), "a", syntheticDomain(0), DomainUnknown},
			[]Domain{syntheticDomain(0), syntheticDomain(2), "a", DomainUnknown},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := make([]Domain, len(tc.in))
			copy(got, tc.in)
			SortDomains(got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("SortDomains(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestSortDomains_IsAStrictWeakOrdering: sort.Slice is entitled to do anything,
// including loop, if the comparator is inconsistent. The unknown-last special
// case is exactly the kind of clause that breaks transitivity if written
// carelessly.
func TestSortDomains_IsAStrictWeakOrdering(t *testing.T) {
	r := propRand()
	pool := []Domain{"a", "b", "z", DomainUnknown, syntheticDomain(0), "us-central1-a", ""}

	for i := 0; i < 2000; i++ {
		in := make([]Domain, 1+r.IntN(8))
		for j := range in {
			in[j] = pool[r.IntN(len(pool))]
		}

		first := append([]Domain(nil), in...)
		SortDomains(first)

		// Shuffling the input must not change the output, which is what
		// "total order over the values" means in practice.
		shuffled := append([]Domain(nil), in...)
		r.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
		SortDomains(shuffled)

		if !reflect.DeepEqual(first, shuffled) {
			t.Fatalf("order depends on input permutation: %v vs %v (from %v)", first, shuffled, in)
		}

		// And DomainUnknown, if present, is last.
		for j, d := range first {
			if d == DomainUnknown && j != len(first)-1 && first[j+1] != DomainUnknown {
				t.Fatalf("%v: unknown at %d is not in the trailing run", first, j)
			}
		}
	}
}
