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
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// policyListKinds is what the dynamic fake needs to serve a List on a resource
// it has no compiled-in type for.
var policyListKinds = map[schema.GroupVersionResource]string{
	policyGVR:        PolicyKind + "List",
	clusterPolicyGVR: ClusterPolicyKind + "List",
}

// logCapture collects log lines so a test can assert what an operator is told.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (c *logCapture) logf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, strings.TrimSpace(fmt.Sprintf(format, args...)))
}

func (c *logCapture) all() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n")
}

// sourceWithPolicies builds a source whose discovery serves the named policy
// resources and whose dynamic client holds objs.
func sourceWithPolicies(t *testing.T, served []string, objs ...runtime.Object) (*Source, *logCapture) {
	t.Helper()
	client := fake.NewSimpleClientset()
	if len(served) > 0 {
		var rs []metav1.APIResource
		for _, name := range served {
			rs = append(rs, metav1.APIResource{Name: name, Kind: PolicyKind, Namespaced: name == policyGVR.Resource})
		}
		client.Resources = []*metav1.APIResourceList{{GroupVersion: policyGV.String(), APIResources: rs}}
	}
	s := New(client, Config{})
	s.WithDynamic(dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), policyListKinds, objs...))
	logs := &logCapture{}
	s.logf = logs.logf
	return s, logs
}

// policyObj renders a fixture as the unstructured object an informer delivers.
func policyObj(t *testing.T, doc string) *unstructured.Unstructured {
	t.Helper()
	return policyFrom(t, doc)
}

func TestPolicyWatch_NoDynamicClientIsSilentAndHarmless(t *testing.T) {
	// The embedder that never calls WithDynamic — every unit test in this
	// package, and any consumer with no dynamic client.
	s := New(fake.NewSimpleClientset(), Config{})
	synced, err := s.startPolicyWatch(context.Background())
	if err != nil || synced != nil {
		t.Errorf("startPolicyWatch() = %v, %v, want no barriers and no error", synced, err)
	}
}

func TestPolicyWatch_AbsentCRDsLogAndContinue(t *testing.T) {
	// The whole reason this source does not copy the gateway source's
	// hard-fail: topology-drift is default-on and complete without a policy,
	// so a cluster that never installed the CRD must not fail to start.
	s, logs := sourceWithPolicies(t, nil)
	synced, err := s.startPolicyWatch(context.Background())
	if err != nil {
		t.Fatalf("startPolicyWatch() errored on an absent CRD: %v", err)
	}
	if synced != nil {
		t.Errorf("registered %d barriers with no CRD served", len(synced))
	}
	if !strings.Contains(logs.all(), "not installed") {
		t.Errorf("nothing told the operator why policies are ignored: %q", logs.all())
	}
	// And the source still resolves intent, from inference alone.
	if got := s.policyFor(apiSubject(), &corev1.Pod{}); got != nil {
		t.Errorf("policyFor() = %v with no CRD", got)
	}
}

func TestPolicyWatch_OnlyOneKindServed(t *testing.T) {
	// A partial install is a real state — someone applied half the file, or
	// pruned the cluster-scoped kind on purpose — and it should cost exactly
	// the kind that is missing.
	s, logs := sourceWithPolicies(t, []string{policyGVR.Resource})
	synced, err := s.startPolicyWatch(context.Background())
	if err != nil {
		t.Fatalf("startPolicyWatch() = %v", err)
	}
	if len(synced) != 1 {
		t.Errorf("registered %d barriers, want 1 for the one served kind", len(synced))
	}
	if !strings.Contains(logs.all(), clusterPolicyGVR.Resource) {
		t.Errorf("the unserved kind was dropped silently: %q", logs.all())
	}
}

func TestPolicyWatch_DeliversPoliciesIntoTheStore(t *testing.T) {
	obj := policyObj(t, nsPolicy("api"))
	s, _ := sourceWithPolicies(t, []string{policyGVR.Resource, clusterPolicyGVR.Resource}, obj)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	synced, err := s.startPolicyWatch(ctx)
	if err != nil {
		t.Fatalf("startPolicyWatch() = %v", err)
	}
	if len(synced) != 2 {
		t.Fatalf("registered %d barriers, want 2", len(synced))
	}
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		t.Fatal("policy caches never synced")
	}
	if s.policies.Len() != 1 {
		t.Fatalf("store holds %d policies after sync, want 1", s.policies.Len())
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Labels: apiLabels()}}
	got := s.policyFor(apiSubject(), pod)
	if got == nil || got.Name != "api" {
		t.Errorf("policyFor() = %v, want the delivered policy", got)
	}
}

func TestPolicyWatch_AnUndecodablePolicyKeepsThePreviousOne(t *testing.T) {
	// Replacing a policy with nothing on a decode failure means a typo in one
	// field silently reverts a subject to inferred intent, and the operator
	// sees their policy stop applying with nothing to explain it.
	s, logs := sourceWithPolicies(t, nil)
	s.onPolicy(policyObj(t, nsPolicy("api")), false)
	if s.policies.Len() != 1 {
		t.Fatal("the good policy did not land")
	}

	// An all-zero expectedDistribution: valid against the structural schema
	// (every value is a non-negative integer) and rejected by the decoder,
	// which is exactly the class of failure that reaches this handler.
	s.onPolicy(policyObj(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: LeewayPolicy
metadata: { name: api, namespace: payments }
spec:
  topologyKeys:
    - key: topology.kubernetes.io/zone
      expectedDistribution: { zone-a: 0, zone-b: 0 }
`), false)

	if s.policies.Len() != 1 {
		t.Errorf("store holds %d policies, want the previous one retained", s.policies.Len())
	}
	if !strings.Contains(logs.all(), "ignoring") {
		t.Errorf("the rejected policy was dropped silently: %q", logs.all())
	}
}

func TestPolicyWatch_DeleteAndTombstone(t *testing.T) {
	s, _ := sourceWithPolicies(t, nil)
	obj := policyObj(t, nsPolicy("api"))
	s.onPolicy(obj, false)

	// The informer delivers a tombstone when it missed the delete itself —
	// a resync gap, which is the case a handler is most likely to get wrong.
	s.onPolicyDelete(cache.DeletedFinalStateUnknown{Key: "payments/api", Obj: obj})
	if s.policies.Len() != 0 {
		t.Errorf("a tombstoned delete left %d policies", s.policies.Len())
	}

	s.onPolicy(obj, false)
	s.onPolicyDelete(obj)
	if s.policies.Len() != 0 {
		t.Errorf("a plain delete left %d policies", s.policies.Len())
	}
}

func TestPolicyWatch_IgnoresObjectsThatAreNotUnstructured(t *testing.T) {
	// Defensive, but the handler signature is `any` and a shared informer can
	// hand over a tombstone wrapping something unexpected.
	s, _ := sourceWithPolicies(t, nil)
	s.onPolicy("not an object", false)
	s.onPolicyDelete("not an object")
	s.onPolicyDelete(cache.DeletedFinalStateUnknown{Key: "x", Obj: nil})
	if s.policies.Len() != 0 {
		t.Errorf("store holds %d", s.policies.Len())
	}
}

func TestPolicyFor_AmbiguityWarnsOncePerCompetingSet(t *testing.T) {
	// The condition persists until someone edits a policy, and policyFor runs
	// on every coalesced evaluation of every matched subject — so the warning
	// has to be deduplicated or it is a log flood, not a signal.
	s, logs := sourceWithPolicies(t, nil)
	s.onPolicy(policyObj(t, nsPolicy("alpha")), false)
	s.onPolicy(policyObj(t, nsPolicy("zeta")), false)

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Labels: apiLabels()}}
	for i := 0; i < 20; i++ {
		if got := s.policyFor(apiSubject(), pod); got == nil || got.Name != "alpha" {
			t.Fatalf("policyFor() = %v, want the deterministic winner", got)
		}
	}
	if n := strings.Count(logs.all(), "no defensible ordering"); n != 1 {
		t.Errorf("warned %d times over 20 evaluations, want exactly 1", n)
	}
	// It must still name both policies and the winner, or it is not actionable.
	for _, want := range []string{"payments/alpha", "payments/zeta"} {
		if !strings.Contains(logs.all(), want) {
			t.Errorf("the warning does not name %s: %q", want, logs.all())
		}
	}
}

func TestResolve_PolicyOverridesInferredIntent(t *testing.T) {
	// The end-to-end shape FR-10 exists for: the pod says spread on zone, the
	// operator says colocate, and the operator wins.
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	policy := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: fleet }
spec:
  topologyKeys:
    - { key: topology.kubernetes.io/zone, mode: Colocate }
`, true)

	res := Resolve(pod, threeZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults, Policy: policy})
	got := res.Intents[zoneKey]
	if got == nil || got.Source != leeway.SourcePolicyCRD || got.Mode != leeway.ModeColocate {
		t.Fatalf("intent = %+v, want the policy's Colocate", got)
	}
	// The DoNotSchedule contract survives the override — the scheduler is
	// still enforcing it whatever the operator's policy says about intent.
	if got.MaxSkew == nil || !got.HardContract() {
		t.Errorf("the pod's DoNotSchedule contract was erased by the policy: %+v", got)
	}
	// And the overridden constraint is still quotable as evidence.
	var sawTSC bool
	for _, e := range got.Evidence {
		if e.Source == leeway.SourceTopologySpreadConstraint {
			sawTSC = true
		}
	}
	if !sawTSC {
		t.Error("the superseded TSC left no evidence")
	}
}

func TestResolve_PolicyExcludesAnInferenceSourceEntirely(t *testing.T) {
	// An excluded source must not survive as demoted evidence either: evidence
	// is what a finding quotes to justify itself, and quoting a term the policy
	// said to disregard would make the finding unanswerable.
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	policy := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: fleet }
spec:
  inference:
    sources: [PodAntiAffinityRequired]
`, true)

	res := Resolve(pod, threeZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults, Policy: policy})
	if got := res.Intents[zoneKey]; got != nil {
		t.Errorf("intent = %+v, want none: the only source present was excluded", got)
	}
	// Eligibility is still computed for every tracked axis — the policy said
	// nothing about where the pod may go, only about what to infer from.
	if _, ok := res.Eligible[zoneKey]; !ok {
		t.Error("excluding an inference source also dropped the eligible set")
	}
}

func TestResolve_PolicyWithInferenceOffAssertsOnlyWhatItDeclares(t *testing.T) {
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	policy := decode(t, `
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: ClusterLeewayPolicy
metadata: { name: fleet }
spec:
  inference: { enabled: false }
  topologyKeys:
    - { key: topology.kubernetes.io/region, mode: Spread }
`, true)

	res := Resolve(pod, threeZones(t), ResolveConfig{ClusterDefaults: noClusterDefaults, Policy: policy})
	if got := res.Intents[zoneKey]; got != nil {
		t.Errorf("zone intent = %+v, want none with inference disabled", got)
	}
	if got := res.Intents["topology.kubernetes.io/region"]; got == nil || got.Source != leeway.SourcePolicyCRD {
		t.Errorf("region intent = %+v, want the declared one", got)
	}
}

func TestResolve_NoPolicyIsUnchanged(t *testing.T) {
	// The default deployment. Threading a nil policy through must be exactly
	// the same computation as before FR-10 existed.
	pod := tscPod(zoneSpread(1, corev1.DoNotSchedule))
	inv := threeZones(t)
	with := Resolve(pod, inv, ResolveConfig{ClusterDefaults: noClusterDefaults, Policy: nil})
	if got := with.Intents[zoneKey]; got == nil || got.Source != leeway.SourceTopologySpreadConstraint {
		t.Errorf("intent = %+v, want the pod's own TSC", got)
	}
}
