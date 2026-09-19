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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// Fixtures are YAML rather than composed maps so that what the test asserts
// about is the document an operator writes, not a hand-built approximation of
// one. A schema mistake that only shows up on the wire shows up here too.
func policyFrom(t *testing.T, doc string) *unstructured.Unstructured {
	t.Helper()
	var obj map[string]any
	if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
		t.Fatalf("fixture is not valid YAML: %v", err)
	}
	return &unstructured.Unstructured{Object: obj}
}

func decode(t *testing.T, doc string, clusterScoped bool) *Policy {
	t.Helper()
	p, err := DecodePolicy(policyFrom(t, doc), clusterScoped)
	if err != nil {
		t.Fatalf("DecodePolicy: %v", err)
	}
	return p
}

// §10.1's worked example, minus the thresholds, baseline and exclusions blocks
// that belong to later phases and are deliberately not in the shipped schema.
const fullPolicy = `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: LeewayPolicy
metadata:
  name: payments-api
  namespace: payments
spec:
  selector:
    matchLabels: { app: api }
  subjectKinds: [Deployment]
  topologyKeys:
    - key: topology.kubernetes.io/zone
      mode: Spread
      weighting: AllocatableCPU
      expectedDistribution:
        us-east-1a: 40
        us-east-1b: 40
        us-east-1c: 20
  inference:
    enabled: true
    sources: [TopologySpreadConstraint, PodAntiAffinityRequired]
`

func TestDecodePolicy_FullDocument(t *testing.T) {
	p := decode(t, fullPolicy, false)

	if p.Name != "payments-api" || p.Namespace != "payments" {
		t.Errorf("identity = %s/%s", p.Namespace, p.Name)
	}
	if p.ClusterScoped() {
		t.Error("ClusterScoped() = true for a LeewayPolicy")
	}
	if !p.Selector.Matches(labels.Set{"app": "api"}) {
		t.Error("selector does not match its own example labels")
	}
	if p.Selector.Matches(labels.Set{"app": "web"}) {
		t.Error("selector matches a workload it does not name")
	}
	if len(p.SubjectKinds) != 1 || p.SubjectKinds[0] != leeway.SubjectKind("Deployment") {
		t.Errorf("SubjectKinds = %v", p.SubjectKinds)
	}

	key, ok := p.Keys["topology.kubernetes.io/zone"]
	if !ok {
		t.Fatalf("zone key absent, have %v", p.Keys)
	}
	if key.Mode != leeway.ModeSpread {
		t.Errorf("Mode = %v", key.Mode)
	}
	if key.Weighting != leeway.WeightAllocatableCPU {
		t.Errorf("Weighting = %v", key.Weighting)
	}
	want := map[leeway.Domain]float64{"us-east-1a": 40, "us-east-1b": 40, "us-east-1c": 20}
	for d, w := range want {
		if key.ExpectedDistribution[d] != w {
			t.Errorf("ExpectedDistribution[%s] = %v, want %v", d, key.ExpectedDistribution[d], w)
		}
	}

	if !p.Inference.Enabled {
		t.Error("Inference.Enabled = false")
	}
	if !p.Inference.Allows(leeway.SourceTopologySpreadConstraint) {
		t.Error("an allowlisted source is not allowed")
	}
	if p.Inference.Allows(leeway.SourcePodAffinityPreferred) {
		t.Error("a source outside the allowlist is allowed")
	}
	// The allowlist is over every inference source, not only the pod-level
	// ones §10.1's example happens to list — enumerating the sources you trust
	// should not quietly admit one you did not name.
	if p.Inference.Allows(leeway.SourceClusterDefaultAssumed) {
		t.Error("an unlisted cluster default slipped through the allowlist")
	}
}

func TestDecodePolicy_NoSelectorMatchesEverything(t *testing.T) {
	// LabelSelectorAsSelector maps a nil selector to labels.Nothing(), which is
	// right for a workload's own selector and exactly wrong for a policy's
	// scope. Getting it backwards makes every selector-less policy inert,
	// which looks like the feature not working at all.
	p := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: fleet-wide }
spec:
  topologyKeys:
    - key: topology.kubernetes.io/zone
`, true)
	if !p.Selector.Matches(labels.Set{"anything": "at-all"}) {
		t.Error("a policy with no selector matched nothing")
	}
	if !p.Selector.Matches(labels.Set{}) {
		t.Error("a policy with no selector did not match an unlabelled pod")
	}
}

func TestDecodePolicy_Defaults(t *testing.T) {
	p := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: minimal }
spec:
  topologyKeys:
    - key: topology.kubernetes.io/zone
`, true)

	if !p.Inference.Enabled {
		t.Error("omitting inference: disabled inference — the one default that must not be off")
	}
	if p.Inference.Sources != nil {
		t.Errorf("Sources = %v, want nil (every source) when unlisted", p.Inference.Sources)
	}
	if !p.Inference.Allows(leeway.SourceLearnedBaseline) {
		t.Error("a nil allowlist excluded a source")
	}
	key := p.Keys["topology.kubernetes.io/zone"]
	if key.Mode != leeway.ModeSpread {
		t.Errorf("Mode = %v, want Spread by default", key.Mode)
	}
	if key.Weighting != leeway.WeightEqual {
		t.Errorf("Weighting = %v, want Equal by default", key.Weighting)
	}
	if key.ExpectedDistribution != nil {
		t.Errorf("ExpectedDistribution = %v, want nil", key.ExpectedDistribution)
	}
	if len(p.SubjectKinds) != 0 {
		t.Errorf("SubjectKinds = %v, want empty (every kind)", p.SubjectKinds)
	}
}

func TestDecodePolicy_DeclaredEmptySourceListIsAnAllowlistOfNothing(t *testing.T) {
	// The same three-state problem the cluster defaults have: absent means
	// "all", and declared-empty means "none" — a legitimate way to say "this
	// policy and nothing else" without switching the inference machinery off.
	p := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: only-me }
spec:
  inference:
    sources: []
`, true)
	if p.Inference.Sources == nil {
		t.Fatal("a declared-empty list decoded as absent")
	}
	if !p.Inference.Enabled {
		t.Error("Enabled = false: an empty allowlist is not the same as disabling inference")
	}
	for _, s := range leeway.IntentSources() {
		if s == leeway.SourcePolicyCRD {
			continue
		}
		if p.Inference.Allows(s) {
			t.Errorf("%v allowed under an empty allowlist", s)
		}
	}
}

func TestDecodePolicy_InferenceDisabled(t *testing.T) {
	p := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: declared-only }
spec:
  inference: { enabled: false }
`, true)
	for _, s := range leeway.IntentSources() {
		if p.Inference.Allows(s) {
			t.Errorf("%v allowed with inference disabled", s)
		}
	}
}

func TestDecodePolicy_ClusterScopedRefusesANamespace(t *testing.T) {
	// Namespace is what decides specificity when two policies match, so a
	// cluster-scoped object carrying one would silently win a comparison it
	// should lose.
	p := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: fleet-wide, namespace: somehow-set }
spec: {}
`, true)
	if p.Namespace != "" {
		t.Errorf("Namespace = %q, want empty", p.Namespace)
	}
	if !p.ClusterScoped() {
		t.Error("ClusterScoped() = false")
	}
}

func TestDecodePolicy_Rejects(t *testing.T) {
	cases := []struct {
		name          string
		doc           string
		clusterScoped bool
		want          string
	}{
		{
			name: "namespaced policy with no namespace",
			doc: `
metadata: { name: orphan }
spec: {}`,
			want: "no namespace",
		},
		{
			name: "topology key with no key",
			doc: `
metadata: { name: x }
spec:
  topologyKeys:
    - mode: Spread`,
			clusterScoped: true,
			want:          "empty key",
		},
		{
			name: "unknown mode",
			doc: `
metadata: { name: x }
spec:
  topologyKeys:
    - { key: zone, mode: Scatter }`,
			clusterScoped: true,
			want:          "unknown mode",
		},
		{
			name: "unknown weighting",
			doc: `
metadata: { name: x }
spec:
  topologyKeys:
    - { key: zone, weighting: AllocatableDisk }`,
			clusterScoped: true,
			want:          "unknown weighting",
		},
		{
			name: "unknown inference source",
			doc: `
metadata: { name: x }
spec:
  inference:
    sources: [Vibes]`,
			clusterScoped: true,
			want:          "unknown source",
		},
		{
			name: "duplicate topology key",
			doc: `
metadata: { name: x }
spec:
  topologyKeys:
    - { key: zone, mode: Spread }
    - { key: zone, mode: Ignore }`,
			clusterScoped: true,
			want:          "duplicate key",
		},
		{
			name: "negative share",
			doc: `
metadata: { name: x }
spec:
  topologyKeys:
    - key: zone
      expectedDistribution: { a: -1 }`,
			clusterScoped: true,
			want:          "negative share",
		},
		{
			name: "every share zero",
			doc: `
metadata: { name: x }
spec:
  topologyKeys:
    - key: zone
      expectedDistribution: { a: 0, b: 0 }`,
			clusterScoped: true,
			want:          "every share is zero",
		},
		{
			name: "distribution on an Ignore key",
			doc: `
metadata: { name: x }
spec:
  topologyKeys:
    - key: zone
      mode: Ignore
      expectedDistribution: { a: 1 }`,
			clusterScoped: true,
			want:          "Ignore key",
		},
		{
			name: "empty subject kind",
			doc: `
metadata: { name: x }
spec:
  subjectKinds: ["", Deployment]`,
			clusterScoped: true,
			want:          "empty subjectKinds",
		},
		{
			name: "malformed selector",
			doc: `
metadata: { name: x }
spec:
  selector:
    matchExpressions:
      - { key: app, operator: Nonsense, values: [a] }`,
			clusterScoped: true,
			want:          "selector",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodePolicy(policyFrom(t, tc.doc), tc.clusterScoped)
			if err == nil {
				t.Fatalf("accepted an invalid document")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestDecodePolicy_NilObject(t *testing.T) {
	if _, err := DecodePolicy(nil, false); err == nil {
		t.Error("DecodePolicy(nil) = nil error")
	}
}

func TestPolicy_Matches(t *testing.T) {
	deployment := leeway.SubjectRef{Kind: "Deployment", Namespace: "payments", Name: "api"}
	p := decode(t, fullPolicy, false)

	if !p.Matches(deployment, labels.Set{"app": "api"}) {
		t.Error("the policy does not match the subject it was written for")
	}
	if p.Matches(deployment, labels.Set{"app": "web"}) {
		t.Error("matched a pod the selector excludes")
	}
	if p.Matches(leeway.SubjectRef{Kind: "Deployment", Namespace: "other", Name: "api"}, labels.Set{"app": "api"}) {
		t.Error("a namespaced policy matched a subject in another namespace")
	}
	if p.Matches(leeway.SubjectRef{Kind: "StatefulSet", Namespace: "payments", Name: "api"}, labels.Set{"app": "api"}) {
		t.Error("matched a subject kind the policy does not name")
	}
	// Controller-added labels must not break a selector written against the
	// template's labels — which is the normal case, since the pod carries
	// pod-template-hash and the workload does not.
	if !p.Matches(deployment, labels.Set{"app": "api", "pod-template-hash": "7d9f"}) {
		t.Error("an extra controller label broke the match")
	}
}

func TestPolicy_MatchesEveryKindWhenUnrestricted(t *testing.T) {
	p := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: fleet-wide }
spec: {}
`, true)
	for _, kind := range []leeway.SubjectKind{"Deployment", "StatefulSet", "DaemonSet"} {
		sub := leeway.SubjectRef{Kind: kind, Namespace: "anywhere", Name: "x"}
		if !p.Matches(sub, labels.Set{}) {
			t.Errorf("a cluster-wide policy with no restrictions missed %s", kind)
		}
	}
}

func TestPolicy_Intents(t *testing.T) {
	p := decode(t, fullPolicy, false)
	got := p.Intents()
	if len(got) != 1 {
		t.Fatalf("Intents() = %d intents, want 1", len(got))
	}
	in := got[0]
	if in.Source != leeway.SourcePolicyCRD {
		t.Errorf("Source = %v", in.Source)
	}
	if in.Confidence != leeway.ConfidenceDeclared {
		t.Errorf("Confidence = %v, want declared", in.Confidence)
	}
	if in.Mode != leeway.ModeSpread {
		t.Errorf("Mode = %v", in.Mode)
	}
	if in.Weighting != leeway.WeightAllocatableCPU {
		t.Errorf("Weighting = %v", in.Weighting)
	}
	if len(in.ExplicitShares) != 3 {
		t.Errorf("ExplicitShares = %v", in.ExplicitShares)
	}
	// A policy describes where objects belong; it does not report a promise
	// Kubernetes made, so it raises no Tier A finding on its own.
	if in.HardContract() {
		t.Error("a policy intent claims a hard contract")
	}
	if len(in.Evidence) == 0 || !strings.Contains(in.Evidence[0].Detail, "payments-api") {
		t.Errorf("Evidence = %v, want it to name the policy", in.Evidence)
	}
}

func TestPolicy_IntentsAreOrderedByKey(t *testing.T) {
	// These become metric labels and evidence strings, and map iteration is
	// not stable. Resolution is per-key so order cannot change the outcome,
	// but a diff of two evidence lists should not depend on the runtime.
	p := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: many }
spec:
  topologyKeys:
    - { key: topology.kubernetes.io/zone }
    - { key: kubernetes.io/hostname }
    - { key: topology.kubernetes.io/region }
`, true)
	want := []leeway.TopologyKey{
		"kubernetes.io/hostname",
		"topology.kubernetes.io/region",
		"topology.kubernetes.io/zone",
	}
	for i := 0; i < 20; i++ {
		got := p.Intents()
		for j := range want {
			if got[j].TopologyKey != want[j] {
				t.Fatalf("Intents()[%d].TopologyKey = %s, want %s", j, got[j].TopologyKey, want[j])
			}
		}
	}
}

func TestPolicy_DeclaringNoKeysAssertsNoIntent(t *testing.T) {
	// A policy that only switches inference off still has an effect, but it
	// has not said where anything belongs. Inventing a neutral intent here
	// would put a row on the wire claiming the operator asked for something.
	p := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: quiet }
spec:
  inference: { enabled: false }
`, true)
	if got := p.Intents(); len(got) != 0 {
		t.Errorf("Intents() = %v, want none", got)
	}
}
