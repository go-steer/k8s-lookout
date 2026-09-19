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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// noClusterDefaults is the DECLARED EMPTY state: an operator asserting this
// cluster configures no PodTopologySpread defaults. Tests about anything other
// than FR-9 use it so that a cluster default cannot quietly supply the intent
// they meant to derive from the pod.
var noClusterDefaults = &[]corev1.TopologySpreadConstraint{}

// declaredDefaults is the DECLARED state.
func declaredDefaults(cs ...corev1.TopologySpreadConstraint) *[]corev1.TopologySpreadConstraint {
	return &cs
}

// bareBod is a pod declaring no topologySpreadConstraints — the only
// population cluster defaults reach.
func barePod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web-1"}}
}

func TestClusterDefaultIntents_UnsetAssumesTheUpstreamSet(t *testing.T) {
	// The S4 rule: not being told is not the same as being told there is
	// nothing, and the difference has to be visible on the intent.
	got := ClusterDefaultIntents(barePod(), nil)
	if len(got) != len(SystemDefaultConstraints) {
		t.Fatalf("got %d intents, want %d", len(got), len(SystemDefaultConstraints))
	}
	for _, in := range got {
		if in.Source != leeway.SourceClusterDefaultAssumed {
			t.Errorf("%s: source = %v, want cluster-default-assumed", in.TopologyKey, in.Source)
		}
		if in.Confidence != leeway.ConfidenceAssumed {
			t.Errorf("%s: confidence = %v, want assumed", in.TopologyKey, in.Confidence)
		}
		// The cap that makes the whole assumption safe to ship.
		if in.HardContract() {
			t.Errorf("%s: an assumed cluster default reported a hard contract — §8.1 caps it below Tier A", in.TopologyKey)
		}
		if len(in.Evidence) != 1 || !strings.Contains(in.Evidence[0].Detail, "ASSUMED") {
			t.Errorf("%s: evidence does not say the number is a guess: %+v", in.TopologyKey, in.Evidence)
		}
	}
}

func TestSystemDefaultConstraints_MatchUpstream(t *testing.T) {
	// kube-scheduler's PodTopologySpread systemDefaultConstraints, and the
	// pairing is the part worth pinning: hostname is the TIGHT one at 3 and
	// zone the loose one at 5, which reads backwards if you expect the
	// coarser axis to be the stricter. Swapping them would score every
	// undeclared workload in the fleet against the wrong numbers on both axes
	// at once, and nothing else in the system would notice.
	want := map[string]int32{
		corev1.LabelHostname:     3,
		corev1.LabelTopologyZone: 5,
	}
	if len(SystemDefaultConstraints) != len(want) {
		t.Fatalf("got %d constraints, want %d", len(SystemDefaultConstraints), len(want))
	}
	for _, c := range SystemDefaultConstraints {
		if skew, ok := want[c.TopologyKey]; !ok || c.MaxSkew != skew {
			t.Errorf("%s: maxSkew = %d, want %d", c.TopologyKey, c.MaxSkew, want[c.TopologyKey])
		}
		if c.WhenUnsatisfiable != corev1.ScheduleAnyway {
			t.Errorf("%s: whenUnsatisfiable = %s, want ScheduleAnyway — the upstream defaults are soft, which is what keeps an assumption off Tier A",
				c.TopologyKey, c.WhenUnsatisfiable)
		}
	}
}

func TestClusterDefaultIntents_DeclaredEmptyIsAnAnswer(t *testing.T) {
	if got := ClusterDefaultIntents(barePod(), noClusterDefaults); got != nil {
		t.Errorf("declared-empty produced %+v, want no intents", got)
	}
}

func TestClusterDefaultIntents_DeclaredIsTrusted(t *testing.T) {
	// An operator who declares a DoNotSchedule default is making an assertion
	// of their own, so unlike the assumed case this one CAN reach Tier A.
	got := ClusterDefaultIntents(barePod(), declaredDefaults(corev1.TopologySpreadConstraint{
		TopologyKey:       string(zoneKey),
		MaxSkew:           2,
		WhenUnsatisfiable: corev1.DoNotSchedule,
	}))
	if len(got) != 1 {
		t.Fatalf("got %d intents, want 1", len(got))
	}
	in := got[0]
	if in.Source != leeway.SourceClusterDefaultDeclared {
		t.Errorf("source = %v, want cluster-default-declared", in.Source)
	}
	if in.Confidence != leeway.ConfidenceDeclared {
		t.Errorf("confidence = %v, want declared", in.Confidence)
	}
	if !in.HardContract() {
		t.Error("a declared DoNotSchedule default is not a hard contract, but the assertion is the operator's own")
	}
	if in.MaxSkew == nil || *in.MaxSkew != 2 {
		t.Errorf("maxSkew = %v, want 2", in.MaxSkew)
	}
}

func TestClusterDefaultIntents_APodWithItsOwnConstraintIgnoresThem(t *testing.T) {
	// The blast-radius argument, and the reason a wrong assumption cannot
	// corrupt a workload that has actually expressed intent: one constraint on
	// the pod and the scheduler drops the cluster defaults entirely.
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	if got := ClusterDefaultIntents(pod, nil); got != nil {
		t.Errorf("a pod with its own constraint got cluster defaults: %+v", got)
	}
	if got := ClusterDefaultIntents(pod, declaredDefaults(corev1.TopologySpreadConstraint{
		TopologyKey: string(zoneKey), MaxSkew: 9, WhenUnsatisfiable: corev1.DoNotSchedule,
	})); got != nil {
		t.Errorf("a pod with its own constraint got declared defaults: %+v", got)
	}
}

func TestClusterDefaultIntents_SkipsAKeylessConstraint(t *testing.T) {
	got := ClusterDefaultIntents(barePod(), declaredDefaults(
		corev1.TopologySpreadConstraint{MaxSkew: 2, WhenUnsatisfiable: corev1.ScheduleAnyway},
	))
	if got != nil {
		t.Errorf("a constraint with no topologyKey produced %+v", got)
	}
}

func TestClusterDefaultIntents_NilPod(t *testing.T) {
	if got := ClusterDefaultIntents(nil, nil); got != nil {
		t.Errorf("nil pod produced %+v", got)
	}
}

func TestResolve_ClusterDefaultLosesToThePodsOwnIntent(t *testing.T) {
	// Precedence end to end: §5.1 puts a TopologySpreadConstraint above both
	// cluster-default sources, and the pod's own constraint is also the reason
	// ClusterDefaultIntents returns nothing here in the first place. Either
	// mechanism alone would pass this; both are meant to hold.
	res := Resolve(tscPod(zoneSpread(1, corev1.DoNotSchedule)), threeZones(t),
		ResolveConfig{ClusterDefaults: declaredDefaults(corev1.TopologySpreadConstraint{
			TopologyKey: string(zoneKey), MaxSkew: 9, WhenUnsatisfiable: corev1.ScheduleAnyway,
		})})

	in := res.Intents[zoneKey]
	if in == nil {
		t.Fatal("no zone intent")
	}
	if in.Source != leeway.SourceTopologySpreadConstraint {
		t.Errorf("source = %v, want topology-spread-constraint", in.Source)
	}
	if in.MaxSkew == nil || *in.MaxSkew != 1 {
		t.Errorf("maxSkew = %v, want the pod's 1 rather than the default's 9", in.MaxSkew)
	}
}

func TestResolve_AssumedDefaultReachesABarePod(t *testing.T) {
	// The population cluster defaults exist for: a workload that declared
	// nothing at all still gets an expectation, and the expectation says
	// openly that nobody asked for it.
	res := Resolve(barePod(), threeZones(t), ResolveConfig{ClusterDefaults: nil})

	in := res.Intents[zoneKey]
	if in == nil {
		t.Fatal("a bare pod got no zone intent from the assumed cluster default")
	}
	if in.Source != leeway.SourceClusterDefaultAssumed {
		t.Errorf("source = %v, want cluster-default-assumed", in.Source)
	}
	if in.MaxSkew == nil || *in.MaxSkew != 5 {
		t.Errorf("maxSkew = %v, want the upstream zone default of 5", in.MaxSkew)
	}
	// The eligible set is computed for every tracked axis whether or not it
	// carries an intent, so the default intent should have picked one up.
	if in.EligibleDomains.Len() != 3 {
		t.Errorf("eligible domains = %v, want all three zones", in.EligibleDomains)
	}
}

func TestResolve_AnAssumedDefaultOnAnUntrackedAxisStaysOffTheWire(t *testing.T) {
	// The upstream set names hostname as well as zone, and hostname is not an
	// axis this source counts by default. An intent nobody declared, on an
	// axis nobody scores, would be a series on every subject in the cluster
	// and nothing else — see onlyTrackedAxes.
	res := Resolve(barePod(), threeZones(t), ResolveConfig{ClusterDefaults: nil})

	if _, ok := res.Intents[leeway.TopologyKey(corev1.LabelHostname)]; ok {
		t.Error("an assumed cluster default reached intent_info on an untracked axis")
	}
	if res.Intents[zoneKey] == nil {
		t.Error("the tracked axis lost its assumed intent too — the filter is too wide")
	}
}

func TestParseClusterDefaults(t *testing.T) {
	cases := []struct {
		name  string
		raw   string
		check func(*testing.T, *[]corev1.TopologySpreadConstraint)
	}{{
		name: "empty is unset",
		raw:  "",
		check: func(t *testing.T, got *[]corev1.TopologySpreadConstraint) {
			if got != nil {
				t.Errorf("got %+v, want nil (unset)", got)
			}
		},
	}, {
		name: "none is declared empty",
		raw:  "none",
		check: func(t *testing.T, got *[]corev1.TopologySpreadConstraint) {
			if got == nil || len(*got) != 0 {
				t.Errorf("got %+v, want a non-nil empty slice", got)
			}
		},
	}, {
		name: "one constraint, action omitted",
		raw:  "topology.kubernetes.io/zone=4",
		check: func(t *testing.T, got *[]corev1.TopologySpreadConstraint) {
			if got == nil || len(*got) != 1 {
				t.Fatalf("got %+v, want one constraint", got)
			}
			c := (*got)[0]
			if c.TopologyKey != "topology.kubernetes.io/zone" || c.MaxSkew != 4 {
				t.Errorf("got %+v", c)
			}
			// Deliberately NOT the API's DoNotSchedule default: a typo in a
			// flag must not promote a cluster-wide assumption into something
			// that can raise a critical finding.
			if c.WhenUnsatisfiable != corev1.ScheduleAnyway {
				t.Errorf("whenUnsatisfiable = %s, want ScheduleAnyway", c.WhenUnsatisfiable)
			}
		},
	}, {
		name: "two constraints with explicit actions",
		raw:  "kubernetes.io/hostname=3:ScheduleAnyway, topology.kubernetes.io/zone=2:DoNotSchedule",
		check: func(t *testing.T, got *[]corev1.TopologySpreadConstraint) {
			if got == nil || len(*got) != 2 {
				t.Fatalf("got %+v, want two constraints", got)
			}
			if (*got)[1].WhenUnsatisfiable != corev1.DoNotSchedule {
				t.Errorf("second action = %s, want DoNotSchedule", (*got)[1].WhenUnsatisfiable)
			}
		},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseClusterDefaults(tc.raw)
			if err != nil {
				t.Fatalf("ParseClusterDefaults(%q) = %v", tc.raw, err)
			}
			tc.check(t, got)
		})
	}
}

func TestParseClusterDefaults_Rejects(t *testing.T) {
	// Every one of these is a flag a human could plausibly write, and every
	// one of them must stop startup rather than fall through to the assumed
	// defaults — silently scoring the fleet against numbers the operator
	// thought they had replaced is the S4 failure wearing a different hat.
	for _, raw := range []string{
		"topology.kubernetes.io/zone",         // no maxSkew
		"=3",                                  // no key
		"topology.kubernetes.io/zone=many",    // maxSkew not a number
		"topology.kubernetes.io/zone=0",       // below the API's minimum
		"topology.kubernetes.io/zone=-1",      // ditto
		"topology.kubernetes.io/zone=3:Maybe", // unknown action
		" , ",                                 // declares nothing, and "none" is how you say that
	} {
		t.Run(raw, func(t *testing.T) {
			if got, err := ParseClusterDefaults(raw); err == nil {
				t.Errorf("ParseClusterDefaults(%q) = %+v, want an error", raw, got)
			}
		})
	}
}

func TestDescribeClusterDefaults(t *testing.T) {
	if got := DescribeClusterDefaults(nil); !strings.Contains(got, "assumed") {
		t.Errorf("unset renders as %q, want it to say the numbers are assumed", got)
	}
	if got := DescribeClusterDefaults(noClusterDefaults); !strings.Contains(got, "declared empty") {
		t.Errorf("declared-empty renders as %q", got)
	}
	got := DescribeClusterDefaults(declaredDefaults(corev1.TopologySpreadConstraint{
		TopologyKey: string(zoneKey), MaxSkew: 2, WhenUnsatisfiable: corev1.DoNotSchedule,
	}))
	if !strings.Contains(got, "declared") || !strings.Contains(got, "maxSkew=2") {
		t.Errorf("declared renders as %q", got)
	}
}
