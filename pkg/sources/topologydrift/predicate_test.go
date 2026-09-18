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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// term builds a single-expression NodeSelectorTerm.
func term(key string, op corev1.NodeSelectorOperator, values ...string) corev1.NodeSelectorTerm {
	return corev1.NodeSelectorTerm{
		MatchExpressions: []corev1.NodeSelectorRequirement{{Key: key, Operator: op, Values: values}},
	}
}

func requiredAffinity(terms ...corev1.NodeSelectorTerm) *corev1.NodeSelector {
	return &corev1.NodeSelector{NodeSelectorTerms: terms}
}

func TestConstraintsOf(t *testing.T) {
	// The three shapes of "no node affinity" are all reachable from a real
	// object graph — no Affinity at all, an Affinity carrying only pod
	// affinity, and a NodeAffinity carrying only the preferred form — and each
	// one dereferences a different nil if the extraction is careless.
	cases := map[string]*corev1.Affinity{
		"no affinity block":     nil,
		"affinity, no node":     {PodAntiAffinity: &corev1.PodAntiAffinity{}},
		"node affinity, no req": {NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{Weight: 1}}}},
	}
	for name, aff := range cases {
		t.Run(name, func(t *testing.T) {
			c := ConstraintsOf(&corev1.Pod{Spec: corev1.PodSpec{Affinity: aff}})
			if c.RequiredNodeAffinity != nil {
				t.Errorf("RequiredNodeAffinity = %+v, want nil", c.RequiredNodeAffinity)
			}
			if !c.MatchesNode("n1", nil) {
				t.Error("an unconstrained pod must match a node with no labels at all")
			}
		})
	}

	t.Run("populated", func(t *testing.T) {
		want := requiredAffinity(term("pool", corev1.NodeSelectorOpIn, "a"))
		pod := &corev1.Pod{Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"disk": "ssd"},
			Tolerations:  []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists}},
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: want,
			}},
		}}
		c := ConstraintsOf(pod)
		if c.NodeSelector["disk"] != "ssd" || len(c.Tolerations) != 1 || c.RequiredNodeAffinity != want {
			t.Errorf("ConstraintsOf = %+v", c)
		}
	})

	t.Run("preferred node affinity is not an eligibility bar", func(t *testing.T) {
		// A preferred term ranks nodes the scheduler may still ignore. Reading
		// it as a requirement would shrink the eligible domain set on a
		// preference and suppress exactly the drift the subsystem reports.
		pod := &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
				{Weight: 100, Preference: term("pool", corev1.NodeSelectorOpIn, "fast")},
			},
		}}}}
		if !ConstraintsOf(pod).MatchesNode("n1", map[string]string{"pool": "slow"}) {
			t.Error("a preferred affinity excluded a node")
		}
	})
}

func TestConstraints_MatchesNode(t *testing.T) {
	labels := map[string]string{
		"topology.kubernetes.io/zone": "zone-a",
		"pool":                        "general",
		"cores":                       "16",
		"empty":                       "",
	}

	cases := []struct {
		name string
		c    Constraints
		want bool
	}{
		{"zero value matches", Constraints{}, true},

		{"nodeSelector hit", Constraints{NodeSelector: map[string]string{"pool": "general"}}, true},
		{"nodeSelector miss", Constraints{NodeSelector: map[string]string{"pool": "gpu"}}, false},
		{"nodeSelector on an absent key", Constraints{NodeSelector: map[string]string{"nope": "x"}}, false},
		// A label set to the empty string is not the same as an absent label,
		// and a nodeSelector asking for "" must not be satisfied by every node
		// that has never heard of the key.
		{"nodeSelector empty value matches an empty label", Constraints{NodeSelector: map[string]string{"empty": ""}}, true},
		{"nodeSelector empty value misses an absent label", Constraints{NodeSelector: map[string]string{"absent": ""}}, false},
		{"nodeSelector ANDs", Constraints{NodeSelector: map[string]string{"pool": "general", "cores": "8"}}, false},

		{"In hit", Constraints{RequiredNodeAffinity: requiredAffinity(term("pool", corev1.NodeSelectorOpIn, "gpu", "general"))}, true},
		{"In miss", Constraints{RequiredNodeAffinity: requiredAffinity(term("pool", corev1.NodeSelectorOpIn, "gpu"))}, false},
		{"In on an absent key", Constraints{RequiredNodeAffinity: requiredAffinity(term("absent", corev1.NodeSelectorOpIn, "x"))}, false},
		{"NotIn hit", Constraints{RequiredNodeAffinity: requiredAffinity(term("pool", corev1.NodeSelectorOpNotIn, "gpu"))}, true},
		{"NotIn miss", Constraints{RequiredNodeAffinity: requiredAffinity(term("pool", corev1.NodeSelectorOpNotIn, "general"))}, false},
		// An absent label satisfies NotIn: the node does not have the value,
		// which is what was asked. kube-scheduler agrees, and the opposite
		// reading would exclude every unlabelled node from every NotIn
		// workload.
		{"NotIn on an absent key", Constraints{RequiredNodeAffinity: requiredAffinity(term("absent", corev1.NodeSelectorOpNotIn, "x"))}, true},
		{"Exists", Constraints{RequiredNodeAffinity: requiredAffinity(term("pool", corev1.NodeSelectorOpExists))}, true},
		{"Exists on an absent key", Constraints{RequiredNodeAffinity: requiredAffinity(term("absent", corev1.NodeSelectorOpExists))}, false},
		{"Exists on an empty value", Constraints{RequiredNodeAffinity: requiredAffinity(term("empty", corev1.NodeSelectorOpExists))}, true},
		{"DoesNotExist", Constraints{RequiredNodeAffinity: requiredAffinity(term("absent", corev1.NodeSelectorOpDoesNotExist))}, true},
		{"DoesNotExist on a present key", Constraints{RequiredNodeAffinity: requiredAffinity(term("pool", corev1.NodeSelectorOpDoesNotExist))}, false},

		{"Gt", Constraints{RequiredNodeAffinity: requiredAffinity(term("cores", corev1.NodeSelectorOpGt, "8"))}, true},
		{"Gt is strict", Constraints{RequiredNodeAffinity: requiredAffinity(term("cores", corev1.NodeSelectorOpGt, "16"))}, false},
		{"Lt", Constraints{RequiredNodeAffinity: requiredAffinity(term("cores", corev1.NodeSelectorOpLt, "32"))}, true},
		{"Lt is strict", Constraints{RequiredNodeAffinity: requiredAffinity(term("cores", corev1.NodeSelectorOpLt, "16"))}, false},
		{"Gt against a non-numeric label", Constraints{RequiredNodeAffinity: requiredAffinity(term("pool", corev1.NodeSelectorOpGt, "8"))}, false},
		{"Gt against a non-numeric bound", Constraints{RequiredNodeAffinity: requiredAffinity(term("cores", corev1.NodeSelectorOpGt, "lots"))}, false},
		{"Gt with two values", Constraints{RequiredNodeAffinity: requiredAffinity(term("cores", corev1.NodeSelectorOpGt, "1", "2"))}, false},
		{"Gt on an absent key", Constraints{RequiredNodeAffinity: requiredAffinity(term("absent", corev1.NodeSelectorOpGt, "1"))}, false},
		{"unknown operator", Constraints{RequiredNodeAffinity: requiredAffinity(term("pool", corev1.NodeSelectorOperator("Approximately"), "general"))}, false},

		{"terms are ORed", Constraints{RequiredNodeAffinity: requiredAffinity(
			term("pool", corev1.NodeSelectorOpIn, "gpu"),
			term("pool", corev1.NodeSelectorOpIn, "general"),
		)}, true},
		{"expressions within a term are ANDed", Constraints{RequiredNodeAffinity: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
				{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"general"}},
				{Key: "cores", Operator: corev1.NodeSelectorOpIn, Values: []string{"8"}},
			}}},
		}}, false},

		// An empty term selects nothing rather than everything, and a
		// NodeSelector whose only term is empty therefore matches no node.
		// "Select every node" and "select no node" are the two readings of an
		// empty term and only one of them is upstream's.
		{"an empty term is skipped", Constraints{RequiredNodeAffinity: requiredAffinity(corev1.NodeSelectorTerm{})}, false},
		{"an empty term does not rescue a failing one", Constraints{RequiredNodeAffinity: requiredAffinity(
			term("pool", corev1.NodeSelectorOpIn, "gpu"),
			corev1.NodeSelectorTerm{},
		)}, false},
		{"no terms at all", Constraints{RequiredNodeAffinity: &corev1.NodeSelector{}}, false},

		{"matchFields on metadata.name", Constraints{RequiredNodeAffinity: requiredAffinity(corev1.NodeSelectorTerm{
			MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"n1"}}},
		})}, true},
		{"matchFields naming another node", Constraints{RequiredNodeAffinity: requiredAffinity(corev1.NodeSelectorTerm{
			MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"n2"}}},
		})}, false},
		{"an unsupported field does not match", Constraints{RequiredNodeAffinity: requiredAffinity(corev1.NodeSelectorTerm{
			MatchFields: []corev1.NodeSelectorRequirement{{Key: "spec.providerID", Operator: corev1.NodeSelectorOpExists}},
		})}, false},
		{"matchFields and matchExpressions are ANDed", Constraints{RequiredNodeAffinity: requiredAffinity(corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu"}}},
			MatchFields:      []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"n1"}}},
		})}, false},

		// nodeSelector and required affinity are ANDed with each other, which
		// is easy to get wrong by evaluating whichever one is set.
		{"selector and affinity are ANDed", Constraints{
			NodeSelector:         map[string]string{"pool": "general"},
			RequiredNodeAffinity: requiredAffinity(term("topology.kubernetes.io/zone", corev1.NodeSelectorOpIn, "zone-b")),
		}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.c.MatchesNode("n1", labels); got != tc.want {
				t.Errorf("MatchesNode = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestConstraints_ToleratesNode(t *testing.T) {
	noSchedule := corev1.Taint{Key: "dedicated", Value: "payments", Effect: corev1.TaintEffectNoSchedule}
	noExecute := corev1.Taint{Key: "dedicated", Value: "payments", Effect: corev1.TaintEffectNoExecute}
	preferNo := corev1.Taint{Key: "spot", Value: "true", Effect: corev1.TaintEffectPreferNoSchedule}

	cases := []struct {
		name   string
		tols   []corev1.Toleration
		taints []corev1.Taint
		want   bool
	}{
		{"no taints", nil, nil, true},
		{"untolerated NoSchedule", nil, []corev1.Taint{noSchedule}, false},
		{"untolerated NoExecute", nil, []corev1.Taint{noExecute}, false},
		// PreferNoSchedule is a scoring input to kube-scheduler, not a filter.
		// Treating it as an eligibility bar would remove domains the scheduler
		// was perfectly willing to use and report a workload as pinned when it
		// is not.
		{"untolerated PreferNoSchedule", nil, []corev1.Taint{preferNo}, true},

		{"Equal on key and value", []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "payments", Effect: corev1.TaintEffectNoSchedule}}, []corev1.Taint{noSchedule}, true},
		{"Equal with the wrong value", []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "search", Effect: corev1.TaintEffectNoSchedule}}, []corev1.Taint{noSchedule}, false},
		// An unset operator defaults to Equal. The API server defaults it, but
		// this code also runs against hand-written fixtures.
		{"unset operator defaults to Equal", []corev1.Toleration{{Key: "dedicated", Value: "payments"}}, []corev1.Taint{noSchedule}, true},
		{"unset operator with the wrong value", []corev1.Toleration{{Key: "dedicated", Value: "search"}}, []corev1.Taint{noSchedule}, false},
		{"Exists ignores the value", []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}, []corev1.Taint{noSchedule}, true},
		{"unknown operator", []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOperator("Roughly"), Value: "payments"}}, []corev1.Taint{noSchedule}, false},

		{"empty effect tolerates every effect", []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}, []corev1.Taint{noSchedule, noExecute}, true},
		{"a NoSchedule toleration does not cover NoExecute", []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}, []corev1.Taint{noExecute}, false},

		// The DaemonSet / system add-on wildcard.
		{"empty key with Exists tolerates everything", []corev1.Toleration{{Operator: corev1.TolerationOpExists}}, []corev1.Taint{noSchedule, noExecute, preferNo}, true},
		{"empty key with Equal tolerates nothing", []corev1.Toleration{{Operator: corev1.TolerationOpEqual, Value: "payments"}}, []corev1.Taint{noSchedule}, false},

		// Every barring taint must be tolerated, not just one of them — the
		// GPU case from S3, where a node carries two NoSchedule taints and GKE
		// injects one toleration per taint.
		{"one of two taints tolerated", []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}, []corev1.Taint{noSchedule, {Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule}}, false},
		{"both taints tolerated", []corev1.Toleration{
			{Key: "dedicated", Operator: corev1.TolerationOpExists},
			{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists},
		}, []corev1.Taint{noSchedule, {Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule}}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Constraints{Tolerations: tc.tols}
			if got := c.ToleratesNode(tc.taints); got != tc.want {
				t.Errorf("ToleratesNode = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInventory_NodeViewsApplyConstraints(t *testing.T) {
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("a1", "zone-a", withLabel("pool", "gpu"), withTaint("nvidia.com/gpu", "present", corev1.TaintEffectNoSchedule)))
	inv.Upsert(node("b1", "zone-b", withLabel("pool", "general")))

	c := ConstraintsOf(&corev1.Pod{Spec: corev1.PodSpec{
		NodeSelector: map[string]string{"pool": "gpu"},
		Tolerations:  []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists}},
	}})
	views := inv.NodeViews(zoneKey, leeway.WeightEqual, c)
	if len(views) != 2 {
		t.Fatalf("got %d views, want 2", len(views))
	}
	if !views[0].MatchesSelector || !views[0].Tolerated {
		t.Errorf("the gpu node should be usable by this pod: %+v", views[0])
	}
	if views[1].MatchesSelector {
		t.Errorf("the general node should not match a pool=gpu selector: %+v", views[1])
	}

	// Tolerated is reported per node whatever the policy is; it is §7.1's
	// NodeTaintsPolicy that decides whether it bars eligibility. A workload
	// with no toleration for the GPU taint still *matches* the selector.
	bare := inv.NodeViews(zoneKey, leeway.WeightEqual, Constraints{NodeSelector: map[string]string{"pool": "gpu"}})
	if !bare[0].MatchesSelector || bare[0].Tolerated {
		t.Errorf("untolerated GPU node = %+v, want matching but not tolerated", bare[0])
	}
}

func TestEligibility_ClassPinnedWorkloadIsNotDrifting(t *testing.T) {
	// The §7.7.6 false positive, end to end, and the single largest
	// false-positive class in the tool: a compute-class-pinned Deployment
	// whose class has nodes in one zone out of three. Naive zone counting
	// calls it maximally skewed. With FR-7 the eligible set is one zone, the
	// expectation is "all of it there", and the drift is zero.
	//
	// The taint half is what makes this test worth having twice over: the
	// class nodes carry `cloud.google.com/compute-class=<name>:NoSchedule` and
	// GKE injects the matching toleration at admission, so the pod's own
	// manifest — and the Deployment's template — carry neither the selector
	// nor the toleration that make this arrangement legal.
	const class = "n4-preferred"
	inv := NewInventory([]leeway.TopologyKey{zoneKey})
	inv.Upsert(node("f1", "us-central1-f",
		withLabel("cloud.google.com/compute-class", class),
		withTaint("cloud.google.com/compute-class", class, corev1.TaintEffectNoSchedule)))
	inv.Upsert(node("a1", "us-central1-a"))
	inv.Upsert(node("b1", "us-central1-b"))

	admitted := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: "leeway"},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"cloud.google.com/compute-class": class},
			Tolerations: []corev1.Toleration{{
				Key: "cloud.google.com/compute-class", Operator: corev1.TolerationOpEqual,
				Value: class, Effect: corev1.TaintEffectNoSchedule,
			}},
		},
	}

	opts := leeway.DefaultEligibilityOptions()
	// Honour taints, which is not the default: a class-pinned workload is
	// exactly the case where the taint is load-bearing, and the point of the
	// test is that honouring it still yields the right answer.
	opts.Policies.NodeTaintsPolicy = corev1.NodeInclusionPolicyHonor

	el := leeway.EligibleDomains(inv.NodeViews(zoneKey, leeway.WeightEqual, ConstraintsOf(admitted)), opts)
	if !slices.Equal(el.Domains, []leeway.Domain{"us-central1-f"}) {
		t.Fatalf("eligible domains = %v, want [us-central1-f]", el.Domains)
	}

	// Three replicas, all in the one eligible zone: expected equals actual, so
	// the skew a naive counter would call maximal is zero.
	exp := leeway.Apportion(3, el.Weights(leeway.WeightEqual), nil)
	if !slices.Equal(exp.Expected, []int64{3}) || exp.Unplaceable != 0 {
		t.Fatalf("expected distribution = %+v, want all three in the one eligible zone", exp)
	}
}
