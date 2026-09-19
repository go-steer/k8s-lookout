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
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// FR-10: explicit declaration of intent, overriding inference (§10.1).
//
// **The CRD is optional and that is the whole design constraint.** lookout is
// not otherwise a CRD-installing product and topology-drift is default-on, so
// a cluster with no policies installed is the normal case, not a degraded one.
// This inverts the gateway source's precedent, which treats a missing CRD as a
// configuration error worth failing on: there the user named a source that
// watches Gateway API objects, so an empty watch would be a coverage lie,
// whereas here inference is the product and a policy is an override.
//
// **Only the fields listed below are honoured.** §10.1 also sketches
// `thresholds`, `baseline` and `exclusions`, which belong to the findings and
// baseline phases and do nothing yet. They are deliberately absent from the
// shipped CRD schema rather than accepted and ignored — a structural schema
// prunes what it does not declare, so an operator who writes `thresholds:`
// today sees it vanish from `kubectl get -o yaml` instead of believing a
// threshold is in force that nothing reads. Adding them later is an additive
// schema change, which is the safe direction.

// Policy CRD group and version.
const (
	PolicyGroup   = "leeway.lookout.go-steer.io"
	PolicyVersion = "v1alpha1"

	// PolicyKind is the namespaced kind; ClusterPolicyKind is cluster-scoped.
	PolicyKind        = "LeewayPolicy"
	ClusterPolicyKind = "ClusterLeewayPolicy"
)

var (
	policyGV = schema.GroupVersion{Group: PolicyGroup, Version: PolicyVersion}

	// policyGVR is the namespaced resource, clusterPolicyGVR the cluster-scoped
	// one. Both carry the same schema; only the scope differs.
	policyGVR        = policyGV.WithResource("leewaypolicies")
	clusterPolicyGVR = policyGV.WithResource("clusterleewaypolicies")
)

// Policy is one decoded, validated policy document.
type Policy struct {
	Name string
	// Namespace is empty for a ClusterLeewayPolicy. It is also what decides
	// precedence between two matching policies: a namespaced one is more
	// specific than a cluster-scoped one.
	Namespace string

	// Selector matches the *pod's* labels, not the owning workload's. The
	// source never reads the owner object — it resolves subjects through the
	// owner chain on the pod's own metadata (§6.2) — so the pod's labels are
	// the only labels in hand. In practice they are the template's labels plus
	// controller-added ones like pod-template-hash, so a selector written
	// against the workload matches; a selector written against a label that
	// exists only on the Deployment object does not, and that is documented
	// rather than worked around, because fixing it means watching workloads.
	//
	// A nil selector in the document becomes labels.Everything().
	Selector labels.Selector

	// SubjectKinds restricts the policy to those subject kinds. Empty means
	// every kind.
	SubjectKinds []leeway.SubjectKind

	// Keys is the declared intent per topology key.
	Keys map[leeway.TopologyKey]PolicyKey

	// Inference decides which inferred sources may still contribute on keys
	// this policy does not declare.
	Inference InferenceConfig
}

// PolicyKey is one entry of spec.topologyKeys.
type PolicyKey struct {
	Key       leeway.TopologyKey
	Mode      leeway.IntentMode
	Weighting leeway.Weighting

	// ExpectedDistribution is relative weights per domain, nil when the
	// operator left it to the weighting mode.
	ExpectedDistribution map[leeway.Domain]float64
}

// InferenceConfig is spec.inference.
type InferenceConfig struct {
	// Enabled false means the policy is the only intent this subject has;
	// every key it does not declare has no intent at all rather than an
	// inferred one. The zero value of this struct is therefore not usable —
	// decodePolicy defaults it to true, because omitting `inference:`
	// entirely must not silently disable inference.
	Enabled bool

	// Sources is an allowlist over the inference sources, nil meaning all of
	// them. It is applied to *every* inference source and not only the five
	// §10.1's example lists, so an operator who enumerates the pod-level
	// sources also excludes the cluster defaults. That is the literal reading
	// of an allowlist and the useful one: a list of sources you trust should
	// not quietly admit one you did not name.
	Sources map[leeway.IntentSource]bool
}

// Allows reports whether an inferred source may contribute under this policy.
func (c InferenceConfig) Allows(s leeway.IntentSource) bool {
	if !c.Enabled {
		return false
	}
	if c.Sources == nil {
		return true
	}
	return c.Sources[s]
}

// ---- decoding ----

// policyObject is the wire shape. Decoding through a typed struct rather than
// unstructured.Nested* accessors is the opposite choice from the gateway
// source, and for the opposite reason: that source reads four fields out of a
// large foreign schema, while this one owns its schema and reads nearly all of
// it.
type policyObject struct {
	metav1.ObjectMeta `json:"metadata"`
	Spec              policySpec `json:"spec"`
}

type policySpec struct {
	Selector     *metav1.LabelSelector `json:"selector"`
	SubjectKinds []string              `json:"subjectKinds"`
	TopologyKeys []policyTopologyKey   `json:"topologyKeys"`
	Inference    *policyInference      `json:"inference"`
}

type policyTopologyKey struct {
	Key       string `json:"key"`
	Mode      string `json:"mode"`
	Weighting string `json:"weighting"`
	// ExpectedDistribution is integer-valued because CRD schemas cannot carry
	// a float without the API machinery complaining, and because §10.1's
	// worked example is three whole numbers. They are weights, not percentages
	// — nothing requires them to sum to 100.
	ExpectedDistribution map[string]int64 `json:"expectedDistribution"`
}

type policyInference struct {
	Enabled *bool    `json:"enabled"`
	Sources []string `json:"sources"`
}

// DecodePolicy converts one unstructured policy object into a validated
// Policy. clusterScoped says which kind it came from, because the namespaced
// and cluster-scoped kinds share a schema and only the caller knows which
// informer delivered it.
//
// An invalid document is an error rather than a partially-applied policy. A
// policy that half-applies is worse than one that does not apply: the operator
// who wrote it believes their intent is in force, and the half that silently
// vanished is the half that stops a finding firing.
func DecodePolicy(u *unstructured.Unstructured, clusterScoped bool) (*Policy, error) {
	if u == nil {
		return nil, fmt.Errorf("nil policy object")
	}
	var obj policyObject
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &obj); err != nil {
		return nil, fmt.Errorf("decode %s/%s: %w", u.GetNamespace(), u.GetName(), err)
	}

	p := &Policy{
		Name:      obj.Name,
		Namespace: obj.Namespace,
		Keys:      map[leeway.TopologyKey]PolicyKey{},
		Inference: InferenceConfig{Enabled: true},
	}
	if clusterScoped {
		// A ClusterLeewayPolicy has no namespace; refuse to carry one even if
		// the object somehow arrived with it set, because Namespace is what
		// decides specificity when two policies match.
		p.Namespace = ""
	} else if p.Namespace == "" {
		return nil, fmt.Errorf("%s %q: namespaced policy with no namespace", PolicyKind, p.Name)
	}

	sel, err := metav1.LabelSelectorAsSelector(obj.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("policy %s: selector: %w", p.ref(), err)
	}
	// LabelSelectorAsSelector maps a nil selector to labels.Nothing(), which
	// is right for a workload's own selector and exactly wrong here: a policy
	// that omits a selector is scoping itself to everything in reach, not to
	// nothing. Getting this backwards would make every selector-less policy
	// inert, which is the failure that looks like the feature not working.
	if obj.Spec.Selector == nil {
		sel = labels.Everything()
	}
	p.Selector = sel

	for _, k := range obj.Spec.SubjectKinds {
		if k == "" {
			return nil, fmt.Errorf("policy %s: empty subjectKinds entry", p.ref())
		}
		p.SubjectKinds = append(p.SubjectKinds, leeway.SubjectKind(k))
	}

	for i := range obj.Spec.TopologyKeys {
		key, err := decodePolicyKey(&obj.Spec.TopologyKeys[i])
		if err != nil {
			return nil, fmt.Errorf("policy %s: topologyKeys[%d]: %w", p.ref(), i, err)
		}
		if _, dup := p.Keys[key.Key]; dup {
			return nil, fmt.Errorf("policy %s: topologyKeys: duplicate key %q", p.ref(), key.Key)
		}
		p.Keys[key.Key] = key
	}

	if inf := obj.Spec.Inference; inf != nil {
		if inf.Enabled != nil {
			p.Inference.Enabled = *inf.Enabled
		}
		if inf.Sources != nil {
			// A declared-but-empty list is an allowlist of nothing, which is
			// a legitimate way to say "this policy and nothing else" without
			// also turning off the inference machinery. Distinguishing it
			// from an absent list is the same three-state problem the cluster
			// defaults have, and it is a nil check for the same reason.
			p.Inference.Sources = map[leeway.IntentSource]bool{}
			for _, name := range inf.Sources {
				src, ok := leeway.ParseIntentSource(name)
				if !ok {
					return nil, fmt.Errorf("policy %s: inference.sources: unknown source %q", p.ref(), name)
				}
				p.Inference.Sources[src] = true
			}
		}
	}
	return p, nil
}

func decodePolicyKey(in *policyTopologyKey) (PolicyKey, error) {
	out := PolicyKey{Key: leeway.TopologyKey(in.Key)}
	if in.Key == "" {
		return out, fmt.Errorf("empty key")
	}
	out.Mode = leeway.ModeSpread
	if in.Mode != "" {
		m, ok := leeway.ParseIntentMode(in.Mode)
		if !ok {
			return out, fmt.Errorf("unknown mode %q", in.Mode)
		}
		out.Mode = m
	}
	out.Weighting = leeway.WeightEqual
	if in.Weighting != "" {
		w, ok := leeway.ParseWeighting(in.Weighting)
		if !ok {
			return out, fmt.Errorf("unknown weighting %q", in.Weighting)
		}
		out.Weighting = w
	}
	for domain, share := range in.ExpectedDistribution {
		if domain == "" {
			return out, fmt.Errorf("expectedDistribution: empty domain")
		}
		if share < 0 {
			return out, fmt.Errorf("expectedDistribution[%q]: negative share %d", domain, share)
		}
		if out.ExpectedDistribution == nil {
			out.ExpectedDistribution = map[leeway.Domain]float64{}
		}
		out.ExpectedDistribution[leeway.Domain(domain)] = float64(share)
	}
	// An all-zero distribution names domains but asks for nothing in any of
	// them, which Apportion would read as "no information" and fall back to
	// equal shares on. Rejecting it at decode means the operator hears about
	// it, rather than getting equal spread and wondering why their declaration
	// did nothing.
	if out.ExpectedDistribution != nil {
		var sum float64
		for _, v := range out.ExpectedDistribution {
			sum += v
		}
		if sum == 0 {
			return out, fmt.Errorf("expectedDistribution: every share is zero")
		}
	}
	if out.Mode == leeway.ModeIgnore && out.ExpectedDistribution != nil {
		return out, fmt.Errorf("expectedDistribution set on an Ignore key")
	}
	return out, nil
}

// ref renders the policy for an error message.
func (p *Policy) ref() string {
	if p.Namespace == "" {
		return ClusterPolicyKind + "/" + p.Name
	}
	return PolicyKind + "/" + p.Namespace + "/" + p.Name
}

// ClusterScoped reports whether this came from the cluster-scoped kind.
func (p *Policy) ClusterScoped() bool { return p.Namespace == "" }

// Matches reports whether the policy applies to a subject with the given pod
// labels.
func (p *Policy) Matches(sub leeway.SubjectRef, podLabels labels.Set) bool {
	if p.Namespace != "" && p.Namespace != sub.Namespace {
		return false
	}
	if len(p.SubjectKinds) > 0 {
		var found bool
		for _, k := range p.SubjectKinds {
			if k == sub.Kind {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return p.Selector.Matches(podLabels)
}

// Intents renders the policy's declared keys as intents.
//
// Only keys the policy actually declares produce one. A policy that names a
// selector and nothing else still has an effect — it can switch inference off
// — but it asserts no intent, and inventing a neutral one here would put a row
// on the wire claiming the operator asked for something they did not.
func (p *Policy) Intents() []leeway.Intent {
	keys := make([]leeway.TopologyKey, 0, len(p.Keys))
	for k := range p.Keys {
		keys = append(keys, k)
	}
	// Map iteration order is not stable and these become metric labels and
	// evidence, so sort. Resolution is per-key so order cannot change the
	// outcome, but a diff of two evidence lists should not depend on the runtime.
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	out := make([]leeway.Intent, 0, len(keys))
	for _, k := range keys {
		pk := p.Keys[k]
		in := leeway.Intent{
			TopologyKey:    pk.Key,
			Mode:           pk.Mode,
			Source:         leeway.SourcePolicyCRD,
			Confidence:     leeway.ConfidenceDeclared,
			Weighting:      pk.Weighting,
			ExplicitShares: pk.ExpectedDistribution,
		}
		// No MaxSkew, and therefore no hard contract of its own. A policy is
		// the operator's description of where objects belong, not a promise
		// Kubernetes made and broke, and §8.1 reserves Tier A for the latter.
		// A DoNotSchedule TSC on the same key does not lose its contract by
		// being outranked, though — leeway.ResolveIntents carries it onto the
		// winner, because the scheduler's guarantee is a fact about what
		// happened rather than an opinion a policy can overrule.
		in.Evidence = []leeway.EvidenceItem{{
			Source: leeway.SourcePolicyCRD,
			Detail: p.ref() + " declares " + pk.Mode.String() + " on " + string(pk.Key),
		}}
		out = append(out, in)
	}
	return out
}
