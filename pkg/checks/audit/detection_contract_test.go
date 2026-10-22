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

package audit_test

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// TestAuditWorkloadsDetectionBoundary_PositiveAndNegative validates positive and
// negative detection boundaries for `audit workloads` (#654).
func TestAuditWorkloadsDetectionBoundary_PositiveAndNegative(t *testing.T) {
	labels := map[string]string{"app": "frontend"}
	healthyDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "frontend"},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(int32(3)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: spread(template(labels, container("app", true, true))),
		},
	}
	coveringPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "frontend-pdb"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: ptr(intstr.FromInt32(2)),
			Selector:     &metav1.LabelSelector{MatchLabels: labels},
		},
	}

	negOut := checktest.AssertEnvelopeContract(t, audit.WorkloadsCommand(testDeps(healthyDeploy, coveringPDB)), "-A")
	checktest.AssertNoFinding(t, negOut, checktest.FindingMatch{})

	unhealthyDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "single-unprobed"},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "single-unprobed"}},
			Template: template(map[string]string{"app": "single-unprobed"}, container("app", false, true)),
		},
	}
	posOut := checktest.AssertEnvelopeContract(t, audit.WorkloadsCommand(testDeps(healthyDeploy, coveringPDB, unhealthyDeploy)), "-A")
	checktest.AssertHasFinding(t, posOut, checktest.FindingMatch{
		Kind:         "audit.single_replica",
		Namespace:    "prod",
		KindOfObject: "Deployment",
		Name:         "single-unprobed",
		Severity:     emit.SeverityWarning,
		Reason:       "SingleReplica",
	})
	checktest.AssertHasFinding(t, posOut, checktest.FindingMatch{
		Kind:         "audit.no_readiness_probe",
		Namespace:    "prod",
		KindOfObject: "Deployment",
		Name:         "single-unprobed",
		Severity:     emit.SeverityWarning,
		Reason:       "NoReadinessProbe",
	})
	checktest.AssertNoFinding(t, posOut, checktest.FindingMatch{Name: "frontend"})
}

// TestAuditHardeningDetectionBoundary_PositiveAndNegative validates positive and
// negative detection boundaries for `audit hardening` (#654).
func TestAuditHardeningDetectionBoundary_PositiveAndNegative(t *testing.T) {
	enforcedNS := nsObj("prod", map[string]string{"pod-security.kubernetes.io/enforce": "restricted"})
	dedicatedSA := serviceAccount("prod", "app-sa", ptr(false))
	cleanTmpl := app("safe")
	cleanTmpl.Spec.ServiceAccountName = "app-sa"
	cleanDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "safe"},
		Spec:       appsv1.DeploymentSpec{Template: cleanTmpl},
	}

	negOut := checktest.AssertEnvelopeContract(t, audit.HardeningCommand(testDeps(enforcedNS, dedicatedSA, cleanDeploy)), "-A")
	checktest.AssertNoFinding(t, negOut, checktest.FindingMatch{})

	privContainer := container("root", true, true)
	privContainer.SecurityContext = &corev1.SecurityContext{Privileged: ptr(true)}
	privTmpl := template(map[string]string{"app": "risky"}, privContainer)
	privTmpl.Spec.ServiceAccountName = "app-sa"
	privDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "risky"},
		Spec:       appsv1.DeploymentSpec{Template: privTmpl},
	}

	posOut := checktest.AssertEnvelopeContract(t, audit.HardeningCommand(testDeps(enforcedNS, dedicatedSA, cleanDeploy, privDeploy)), "-A")
	checktest.AssertHasFinding(t, posOut, checktest.FindingMatch{
		Kind:         "audit.privileged_container",
		Namespace:    "prod",
		KindOfObject: "Deployment",
		Name:         "risky",
		Severity:     emit.SeverityWarning,
		Reason:       "PrivilegedContainer",
	})
	checktest.AssertNoFinding(t, posOut, checktest.FindingMatch{Name: "safe"})
}

// TestAuditRBACDetectionBoundary_PositiveAndNegative validates positive and
// negative detection boundaries for `audit rbac` (#654).
func TestAuditRBACDetectionBoundary_PositiveAndNegative(t *testing.T) {
	leastPrivRole := role("prod", "pod-reader", rule([]string{""}, []string{"pods"}, []string{"get", "list"}))
	negOut := checktest.AssertEnvelopeContract(t, audit.RBACCommand(testDeps(leastPrivRole)), "-A")
	checktest.AssertNoFinding(t, negOut, checktest.FindingMatch{})

	wildcardRole := clusterRole("danger-admin", nil, fullWildcard())
	clusterAdmin := clusterRole("cluster-admin", map[string]string{"kubernetes.io/bootstrapping": "rbac-defaults"}, fullWildcard())
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "danger-binding"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "cluster-admin"},
		Subjects:   []rbacv1.Subject{{Kind: "User", Name: "bob@example.com"}},
	}
	posOut := checktest.AssertEnvelopeContract(t, audit.RBACCommand(testDeps(leastPrivRole, wildcardRole, clusterAdmin, binding)), "-A")
	checktest.AssertHasFinding(t, posOut, checktest.FindingMatch{
		Kind:         "audit.cluster_admin_binding",
		KindOfObject: "ClusterRoleBinding",
		Name:         "danger-binding",
		Severity:     emit.SeverityWarning,
		Reason:       "ClusterAdminClusterWide",
	})
	checktest.AssertNoFinding(t, posOut, checktest.FindingMatch{Name: "pod-reader"})
}
