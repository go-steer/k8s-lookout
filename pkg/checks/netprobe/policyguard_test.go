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

package netprobe

import (
	"os"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// testUser is the identity deploy-probe/ grants and guards.
const testUser = "system:serviceaccount:agent-triage:lookout-watch"

// shippedGuard decodes the policy and binding exactly as
// deploy-probe/probe-from.yaml ships them, so the guard is tested
// against the real manifest rather than a hand-made copy of it (the
// chart is held to the same file by dev/tools/verify-helm-parity).
func shippedGuard(t testing.TB) (*admissionv1.ValidatingAdmissionPolicy, *admissionv1.ValidatingAdmissionPolicyBinding) {
	t.Helper()
	vap, vapb, err := loadShippedGuard()
	if err != nil {
		t.Fatal(err)
	}
	return vap, vapb
}

func loadShippedGuard() (*admissionv1.ValidatingAdmissionPolicy, *admissionv1.ValidatingAdmissionPolicyBinding, error) {
	raw, err := os.ReadFile("../../../deploy-probe/probe-from.yaml")
	if err != nil {
		return nil, nil, err
	}
	var vap *admissionv1.ValidatingAdmissionPolicy
	var vapb *admissionv1.ValidatingAdmissionPolicyBinding
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		var meta metav1.TypeMeta
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			return nil, nil, err
		}
		switch meta.Kind {
		case "ValidatingAdmissionPolicy":
			vap = &admissionv1.ValidatingAdmissionPolicy{}
			if err := yaml.UnmarshalStrict([]byte(doc), vap); err != nil {
				return nil, nil, err
			}
		case "ValidatingAdmissionPolicyBinding":
			vapb = &admissionv1.ValidatingAdmissionPolicyBinding{}
			if err := yaml.UnmarshalStrict([]byte(doc), vapb); err != nil {
				return nil, nil, err
			}
		}
	}
	if vap == nil || vapb == nil {
		return nil, nil, errorString("deploy-probe/probe-from.yaml lacks the policy or its binding")
	}
	return vap, vapb, nil
}

// guardPolicy and guardBinding are the shipped objects, for seeding
// fake clusters.
func guardPolicy() *admissionv1.ValidatingAdmissionPolicy {
	vap, _, err := loadShippedGuard()
	if err != nil {
		panic(err)
	}
	return vap
}

func guardBinding() *admissionv1.ValidatingAdmissionPolicyBinding {
	_, vapb, err := loadShippedGuard()
	if err != nil {
		panic(err)
	}
	return vapb
}

func TestShippedPolicyCoversTheGrantedIdentity(t *testing.T) {
	vap, vapb := shippedGuard(t)
	if vap.Labels[GuardLabel] != GuardValue || vapb.Labels[GuardLabel] != GuardValue {
		t.Errorf("shipped policy/binding lack %s=%s", GuardLabel, GuardValue)
	}
	for _, user := range []string{testUser, "system:serviceaccount:team-x:lookout-watch"} {
		if !policyCovers(*vap, user) {
			t.Errorf("the shipped policy does not cover %s (re-namespacing must not unhook it)", user)
		}
	}
	for _, user := range []string{"system:serviceaccount:agent-triage:other", "alice@example.com", "system:serviceaccount:x:not-lookout-watch"} {
		if policyCovers(*vap, user) && !strings.HasSuffix(user, ":lookout-watch") {
			t.Errorf("the shipped policy claims to cover %s", user)
		}
	}
	if !bindingEnforces(*vapb) || vapb.Spec.PolicyName != vap.Name {
		t.Errorf("the shipped binding does not enforce the shipped policy: %+v", vapb.Spec)
	}
}

func runGuarded(t *testing.T, mutate func(fc *fakeCluster)) (*fakeCluster, checktest.Result) {
	t.Helper()
	fc := newFakeCluster(t, runningPod())
	if mutate != nil {
		mutate(fc)
	}
	res := checktest.Run(t, NewFrom(fc.deps(), FromConfig{}), podFlag, imageFlag, "--tcp=cart.shop.svc:7070")
	return fc, res
}

func deletePolicyObjects(t *testing.T, fc *fakeCluster, policy, binding bool) {
	t.Helper()
	vap, vapb := shippedGuard(t)
	if policy {
		if err := fc.cs.Tracker().Delete(admissionv1.SchemeGroupVersion.WithResource("validatingadmissionpolicies"), "", vap.Name); err != nil {
			t.Fatal(err)
		}
	}
	if binding {
		if err := fc.cs.Tracker().Delete(admissionv1.SchemeGroupVersion.WithResource("validatingadmissionpolicybindings"), "", vapb.Name); err != nil {
			t.Fatal(err)
		}
	}
}

func replaceObject(t *testing.T, fc *fakeCluster, gvr schema.GroupVersionResource, obj runtime.Object) {
	t.Helper()
	if err := fc.cs.Tracker().Update(gvr, obj, ""); err != nil {
		t.Fatal(err)
	}
}

func TestProbeFromRefusesWithoutThePolicy(t *testing.T) {
	vapGVR := admissionv1.SchemeGroupVersion.WithResource("validatingadmissionpolicies")
	vapbGVR := admissionv1.SchemeGroupVersion.WithResource("validatingadmissionpolicybindings")
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, fc *fakeCluster)
		want   string
	}{
		{"no policy", func(t *testing.T, fc *fakeCluster) { deletePolicyObjects(t, fc, true, true) }, "no ValidatingAdmissionPolicy labeled"},
		{"policy without binding", func(t *testing.T, fc *fakeCluster) { deletePolicyObjects(t, fc, false, true) }, "no binding enforces it"},
		{"binding only warns", func(t *testing.T, fc *fakeCluster) {
			b := guardBinding()
			b.Spec.ValidationActions = []admissionv1.ValidationAction{admissionv1.Warn}
			replaceObject(t, fc, vapbGVR, b)
		}, "no binding enforces it"},
		{"binding narrowed to some namespaces", func(t *testing.T, fc *fakeCluster) {
			b := guardBinding()
			b.Spec.MatchResources = &admissionv1.MatchResources{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"probe": "guarded"}}}
			replaceObject(t, fc, vapbGVR, b)
		}, "no binding enforces it"},
		{"policy fails open", func(t *testing.T, fc *fakeCluster) {
			p := guardPolicy()
			ignore := admissionv1.Ignore
			p.Spec.FailurePolicy = &ignore
			replaceObject(t, fc, vapGVR, p)
		}, "no ValidatingAdmissionPolicy labeled"},
		{"policy matches another identity", func(t *testing.T, fc *fakeCluster) {
			p := guardPolicy()
			p.Spec.MatchConditions[0].Expression = "request.userInfo.username == 'system:serviceaccount:other:someone'"
			replaceObject(t, fc, vapGVR, p)
		}, "covers " + testUser},
		{"policy with a condition the guard cannot verify", func(t *testing.T, fc *fakeCluster) {
			p := guardPolicy()
			p.Spec.MatchConditions = append(p.Spec.MatchConditions, admissionv1.MatchCondition{Name: "x", Expression: "request.namespace == 'nobody'"})
			replaceObject(t, fc, vapGVR, p)
		}, "no ValidatingAdmissionPolicy labeled"},
		{"policy not labeled", func(t *testing.T, fc *fakeCluster) {
			p := guardPolicy()
			delete(p.Labels, GuardLabel)
			replaceObject(t, fc, vapGVR, p)
		}, "no ValidatingAdmissionPolicy labeled"},
		{"cluster older than 1.30", func(t *testing.T, fc *fakeCluster) {
			fc.cs.PrependReactor("list", "validatingadmissionpolicies", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewNotFound(vapGVR.GroupResource(), "")
			})
		}, "does not serve admissionregistration.k8s.io/v1 ValidatingAdmissionPolicy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc, res := runGuarded(t, func(fc *fakeCluster) { tc.mutate(t, fc) })
			assertRefused(t, fc, res, "PolicyMissing", tc.want, "UNGUARDED", "deploy-probe/probe-from.yaml")
			if fc.patchCount() != 0 {
				t.Error("added a container although the grant is unguarded")
			}
		})
	}
}

func TestProbeFromGuardNeedsItsReadGrant(t *testing.T) {
	fc, res := runGuarded(t, func(fc *fakeCluster) {
		fc.cs.PrependReactor("list", "validatingadmissionpolicies", func(a k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(a.GetResource().GroupResource(), "", errorString(`User "u" cannot list resource "validatingadmissionpolicies" in API group "admissionregistration.k8s.io"`))
		})
	})
	assertRefused(t, fc, res, "Forbidden",
		checks.Refused("list", "admissionregistration.k8s.io", "validatingadmissionpolicies").String(), "deploy-probe/")
	if fc.patchCount() != 0 {
		t.Error("patched without confirming the policy")
	}
}

func TestProbeFromGuardNeedsTheCallerIdentity(t *testing.T) {
	fc, res := runGuarded(t, func(fc *fakeCluster) {
		fc.cs.PrependReactor("create", "selfsubjectreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewNotFound(a.GetResource().GroupResource(), "")
		})
	})
	assertRefused(t, fc, res, "PolicyMissing", "cannot confirm the admission policy covers this identity")
}

func TestProbeFromProceedsWhenGuarded(t *testing.T) {
	fc, res := runGuarded(t, func(fc *fakeCluster) { fc.log = innerLog(t, "--tcp=127.0.0.1:1") })
	if res.Code != emit.ExitData || fc.patchCount() != 1 {
		t.Fatalf("exit %d patches %d: %s%s", res.Code, fc.patchCount(), res.Stdout, res.Stderr)
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }
