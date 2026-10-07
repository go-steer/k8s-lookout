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

// The runtime half of the probe grant's guard. `patch
// pods/ephemeralcontainers` on its own is close to pods/exec: any
// image, with the pod's volumes mounted. deploy-probe/ (Helm
// rbac.probeFrom) ships it beside a ValidatingAdmissionPolicy that
// narrows it to the one inert probe container, but kubectl can apply
// the grant without the policy (a cluster older than 1.30 does not
// serve the policy kinds; a partial apply; someone deletes the
// policy). In that state lookout refuses to use the grant at all.
//
// What counts as "guarded": a policy labeled GuardLabel=net-probe-from
// that fails closed, matches UPDATE on pods/ephemeralcontainers, and
// whose match conditions are ones this code can verify cover the
// caller; plus a binding for it that denies and is not narrowed.

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// GuardLabel marks the ValidatingAdmissionPolicy (and its binding)
// that guards the probe grant; its value is GuardValue.
const (
	GuardLabel = "k8s-lookout.go-steer.dev/guards"
	GuardValue = "net-probe-from"
)

// The identity conditions the guard can verify, exactly as
// deploy-probe/ and the chart write them (or as an operator copying
// the policy for one named identity would).
var (
	identityBySAName = regexp.MustCompile(`^request\.userInfo\.username\.startsWith\('system:serviceaccount:'\) && request\.userInfo\.username\.endsWith\(':([^']+)'\)$`)
	identityExact    = regexp.MustCompile(`^request\.userInfo\.username == '([^']+)'$`)
)

const addsContainers = "has(object.spec.ephemeralContainers)"

// unguarded is the shared tail of every PolicyMissing message.
const unguarded = "the patch pods/ephemeralcontainers grant is UNGUARDED (on its own it can add any image to any pod, with the pod's volumes mounted), so lookout will not use it. Fix: apply deploy-probe/probe-from.yaml in full, or Helm rbac.probeFrom=true, on Kubernetes 1.30+ — or remove the grant"

// policyGuard reports why the probe must not proceed ("" when the
// grant is guarded), or an error for a failure that is neither a
// refusal nor a missing policy.
func policyGuard(ctx context.Context, c kubernetes.Interface, user string, userKnown bool) (reason, msg string, err error) {
	sel := metav1.ListOptions{LabelSelector: GuardLabel + "=" + GuardValue}
	policies, err := c.AdmissionregistrationV1().ValidatingAdmissionPolicies().List(ctx, sel)
	if err != nil {
		return guardReadError(err, "validatingadmissionpolicies")
	}
	if !userKnown {
		return "PolicyMissing", "cannot confirm the admission policy covers this identity: the server did not answer a SelfSubjectReview, so " + unguarded, nil
	}
	var covering []string
	for _, p := range policies.Items {
		if policyCovers(p, user) {
			covering = append(covering, p.Name)
		}
	}
	if len(covering) == 0 {
		return "PolicyMissing", fmt.Sprintf("no ValidatingAdmissionPolicy labeled %s=%s covers %s; %s", GuardLabel, GuardValue, user, unguarded), nil
	}
	bindings, err := c.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return guardReadError(err, "validatingadmissionpolicybindings")
	}
	for _, name := range covering {
		for _, b := range bindings.Items {
			if b.Spec.PolicyName == name && bindingEnforces(b) {
				return "", "", nil
			}
		}
	}
	return "PolicyMissing", fmt.Sprintf("ValidatingAdmissionPolicy %s exists but no binding enforces it (validationActions Deny, no matchResources or paramRef narrowing); %s", strings.Join(covering, ","), unguarded), nil
}

// guardReadError classifies a failed read of the policy kinds.
func guardReadError(err error, resource string) (string, string, error) {
	if r, ok := checks.ForbiddenRefusal(err); ok {
		r.Verb, r.Group, r.Resource = "list", "admissionregistration.k8s.io", resource
		return "Forbidden", r.String() + " (the deploy-probe/ overlay, or Helm rbac.probeFrom=true, grants it to lookout's ServiceAccount, so the command can confirm its grant is guarded)", nil
	}
	if apierrors.IsNotFound(err) {
		return "PolicyMissing", "this cluster does not serve admissionregistration.k8s.io/v1 ValidatingAdmissionPolicy (Kubernetes 1.30+), so " + unguarded, nil
	}
	return "", "", fmt.Errorf("reading %s to confirm the probe grant is guarded: %w", resource, err)
}

// policyCovers reports whether p fails closed, applies to adding
// ephemeral containers, and provably matches user.
func policyCovers(p admissionv1.ValidatingAdmissionPolicy, user string) bool {
	if fp := p.Spec.FailurePolicy; fp != nil && *fp != admissionv1.Fail {
		return false
	}
	if p.Spec.ParamKind != nil || p.Spec.MatchConstraints == nil || !rulesCoverEphemeral(p.Spec.MatchConstraints.ResourceRules) {
		return false
	}
	if len(p.Spec.Validations) == 0 {
		return false
	}
	identity := false
	for _, mc := range p.Spec.MatchConditions {
		expr := strings.TrimSpace(mc.Expression)
		switch {
		case expr == addsContainers:
			// Only skips requests that add nothing.
		case identityMatches(expr, user):
			identity = true
		default:
			// A condition this code cannot evaluate might exclude
			// the caller; it does not count as covering.
			return false
		}
	}
	return identity
}

func identityMatches(expr, user string) bool {
	if m := identityBySAName.FindStringSubmatch(expr); m != nil {
		return strings.HasPrefix(user, "system:serviceaccount:") && strings.HasSuffix(user, ":"+m[1])
	}
	if m := identityExact.FindStringSubmatch(expr); m != nil {
		return user == m[1]
	}
	return false
}

func rulesCoverEphemeral(rules []admissionv1.NamedRuleWithOperations) bool {
	for _, r := range rules {
		if len(r.ResourceNames) > 0 {
			continue
		}
		if containsAny(r.APIGroups, "", "*") && containsAny(r.Resources, "pods/ephemeralcontainers", "pods/*", "*/*") &&
			(len(r.Operations) == 0 || containsAny(opStrings(r.Operations), "UPDATE", "*")) {
			return true
		}
	}
	return false
}

func bindingEnforces(b admissionv1.ValidatingAdmissionPolicyBinding) bool {
	deny := false
	for _, a := range b.Spec.ValidationActions {
		if a == admissionv1.Deny {
			deny = true
		}
	}
	if !deny || b.Spec.ParamRef != nil {
		return false
	}
	if m := b.Spec.MatchResources; m != nil {
		if len(m.ResourceRules) > 0 || len(m.ExcludeResourceRules) > 0 ||
			!emptySelector(m.NamespaceSelector) || !emptySelector(m.ObjectSelector) {
			return false
		}
	}
	return true
}

func emptySelector(s *metav1.LabelSelector) bool {
	return s == nil || (len(s.MatchLabels) == 0 && len(s.MatchExpressions) == 0)
}

func opStrings(ops []admissionv1.OperationType) []string {
	out := make([]string, len(ops))
	for i, o := range ops {
		out[i] = string(o)
	}
	return out
}

func containsAny(have []string, want ...string) bool {
	for _, h := range have {
		for _, w := range want {
			if h == w {
				return true
			}
		}
	}
	return false
}
