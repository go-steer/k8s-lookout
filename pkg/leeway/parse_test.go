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

import "testing"

// The three round-trip tests below walk the enum by value rather than by a
// hand-written list, counting up until String stops recognising the value.
// That is what makes them a guard: adding a Weighting with a String arm but no
// ParseWeighting arm fails here, so the CRD cannot silently start rejecting a
// spelling its own schema advertises.

func TestParseIntentMode_RoundTripsEveryDeclaredMode(t *testing.T) {
	var n int
	for i := 0; i < 64; i++ {
		m := IntentMode(i)
		if m.String() == "Unknown" {
			break
		}
		n++
		got, ok := ParseIntentMode(m.String())
		if !ok {
			t.Errorf("ParseIntentMode(%q) not recognised", m.String())
			continue
		}
		if got != m {
			t.Errorf("ParseIntentMode(%q) = %v, want %v", m.String(), got, m)
		}
	}
	if n != 3 {
		t.Errorf("walked %d modes, want 3 — update this test with the new one", n)
	}
}

func TestParseWeighting_RoundTripsEveryDeclaredWeighting(t *testing.T) {
	var n int
	for i := 0; i < 64; i++ {
		w := Weighting(i)
		if w.String() == "Unknown" {
			break
		}
		n++
		got, ok := ParseWeighting(w.String())
		if !ok {
			t.Errorf("ParseWeighting(%q) not recognised", w.String())
			continue
		}
		if got != w {
			t.Errorf("ParseWeighting(%q) = %v, want %v", w.String(), got, w)
		}
	}
	if n != 4 {
		t.Errorf("walked %d weightings, want 4 — update this test with the new one", n)
	}
}

func TestParseIntentSource_RoundTripsEverySourceInBothSpellings(t *testing.T) {
	sources := IntentSources()
	if len(sources) != 10 {
		t.Fatalf("IntentSources() has %d entries, want 10", len(sources))
	}
	// IntentSources doubles as the precedence list the CRD validates against,
	// so it has to hold every declared source and hold them in order.
	for i, s := range sources {
		if int(s) != i {
			t.Errorf("IntentSources()[%d] = %v (value %d): the list is out of precedence order", i, s, s)
		}
		if s.String() == "unknown" {
			t.Errorf("IntentSources()[%d] has no String arm", i)
		}
		for _, spelling := range []string{s.String(), s.APIName()} {
			got, ok := ParseIntentSource(spelling)
			if !ok {
				t.Errorf("ParseIntentSource(%q) not recognised", spelling)
				continue
			}
			if got != s {
				t.Errorf("ParseIntentSource(%q) = %v, want %v", spelling, got, s)
			}
		}
	}
	// And nothing beyond the list is declared.
	if IntentSource(len(sources)).String() != "unknown" {
		t.Errorf("IntentSource(%d) has a String arm but is not in IntentSources()", len(sources))
	}
}

func TestIntentSource_APIName(t *testing.T) {
	// The CamelCase spellings §10.1's inference.sources list uses. These are
	// the strings the shipped CRD's enum has to contain, so they are pinned
	// here rather than derived in the test.
	want := map[IntentSource]string{
		SourcePolicyCRD:                "PolicyCRD",
		SourceWorkloadAnnotation:       "WorkloadAnnotation",
		SourceTopologySpreadConstraint: "TopologySpreadConstraint",
		SourcePodAntiAffinityRequired:  "PodAntiAffinityRequired",
		SourcePodAffinityRequired:      "PodAffinityRequired",
		SourceClusterDefaultDeclared:   "ClusterDefaultDeclared",
		SourceClusterDefaultAssumed:    "ClusterDefaultAssumed",
		SourcePodAntiAffinityPreferred: "PodAntiAffinityPreferred",
		SourcePodAffinityPreferred:     "PodAffinityPreferred",
		SourceLearnedBaseline:          "LearnedBaseline",
	}
	for src, w := range want {
		if got := src.APIName(); got != w {
			t.Errorf("%v.APIName() = %q, want %q", src, got, w)
		}
	}
}

func TestParse_RejectsJunk(t *testing.T) {
	if _, ok := ParseIntentMode("Sprea"); ok {
		t.Error("ParseIntentMode accepted a truncated mode")
	}
	if _, ok := ParseWeighting("AllocatableDisk"); ok {
		t.Error("ParseWeighting accepted a weighting that does not exist")
	}
	if _, ok := ParseIntentSource(""); ok {
		t.Error("ParseIntentSource accepted the empty string")
	}
	// "unknown" is the String sentinel, not a source. Parsing it back would
	// turn every unrecognised value into a valid one.
	if _, ok := ParseIntentSource("unknown"); ok {
		t.Error("ParseIntentSource accepted the String sentinel")
	}
}

func TestParse_IsSpellingTolerant(t *testing.T) {
	// Folding exists so an operator can copy the kebab-case spelling off a
	// dashboard into a CRD that documents the CamelCase one.
	for _, spelling := range []string{
		"pod-anti-affinity-required",
		"PodAntiAffinityRequired",
		"pod_anti_affinity_required",
		"  podantiaffinityrequired  ",
	} {
		got, ok := ParseIntentSource(spelling)
		if !ok || got != SourcePodAntiAffinityRequired {
			t.Errorf("ParseIntentSource(%q) = %v, %v", spelling, got, ok)
		}
	}
	if got, ok := ParseWeighting("allocatablecpu"); !ok || got != WeightAllocatableCPU {
		t.Errorf("ParseWeighting(allocatablecpu) = %v, %v", got, ok)
	}
}
