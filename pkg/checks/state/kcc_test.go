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

package state_test

import (
	"context"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

var (
	kccNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	sqlGV          = schema.GroupVersion{Group: "sql.cnrm.cloud.google.com", Version: "v1beta1"}
	sqlInstanceGVR = sqlGV.WithResource("sqlinstances")
	storageGV      = schema.GroupVersion{Group: "storage.cnrm.cloud.google.com", Version: "v1beta1"}
	storageGVR     = storageGV.WithResource("storagebuckets")
	iamGV          = schema.GroupVersion{Group: "iam.cnrm.cloud.google.com", Version: "v1beta1"}
	iamMemberGVR   = iamGV.WithResource("iampolicymembers")
)

func runKCC(t *testing.T, served bool, dynObjs []runtime.Object, args ...string) (checks.Command, checktest.Result) {
	t.Helper()
	cs := fake.NewClientset()
	if served {
		cs.Resources = []*metav1.APIResourceList{
			{
				GroupVersion: sqlGV.String(),
				APIResources: []metav1.APIResource{
					{Name: "sqlinstances", Kind: "SQLInstance", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
					{Name: "sqlinstances/status", Kind: "SQLInstance", Namespaced: true, Verbs: metav1.Verbs{"get"}},
				},
			},
			{
				GroupVersion: storageGV.String(),
				APIResources: []metav1.APIResource{
					{Name: "storagebuckets", Kind: "StorageBucket", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
				},
			},
			{
				GroupVersion: iamGV.String(),
				APIResources: []metav1.APIResource{
					{Name: "iampolicymembers", Kind: "IAMPolicyMember", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
				},
			},
		}
	}
	scheme := runtime.NewScheme()
	kinds := map[schema.GroupVersionResource]string{
		sqlInstanceGVR: "SQLInstanceList",
		storageGVR:     "StorageBucketList",
		iamMemberGVR:   "IAMPolicyMemberList",
	}
	dyn := fakedynamic.NewSimpleDynamicClientWithCustomListKinds(scheme, kinds, dynObjs...)
	cmd := state.KCCCommand(state.KCCDeps{
		Client:  func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Dynamic: func(context.Context) (dynamic.Interface, error) { return dyn, nil },
		Now:     func() time.Time { return kccNow },
	})
	return cmd, checktest.Run(t, cmd, args...)
}

func kccObj(gv schema.GroupVersion, kind, ns, name string, gen, obsGen int64, readyStatus, readyReason, readyMsg string, delTS *time.Time) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gv.String(),
		"kind":       kind,
		"metadata": map[string]any{
			"namespace":         ns,
			"name":              name,
			"generation":        gen,
			"creationTimestamp": kccNow.Add(-30 * time.Minute).Format(time.RFC3339),
		},
	}}
	if delTS != nil {
		u.Object["metadata"].(map[string]any)["deletionTimestamp"] = delTS.Format(time.RFC3339)
	}
	status := map[string]any{}
	if obsGen > 0 {
		status["observedGeneration"] = obsGen
	}
	if readyStatus != "" {
		status["conditions"] = []any{map[string]any{
			"type":    "Ready",
			"status":  readyStatus,
			"reason":  readyReason,
			"message": readyMsg,
		}}
	}
	if len(status) > 0 {
		u.Object["status"] = status
	}
	return u
}

func TestKCCUnavailableWhenCRDsAbsent(t *testing.T) {
	cmd, res := runKCC(t, false, nil, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr = %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "kind=kcc.unavailable") || !strings.Contains(res.Stdout, "reason=CRDNotInstalled") {
		t.Errorf("expected kcc.unavailable with CRDNotInstalled, got:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "unavailable=kcc-not-installed") {
		t.Errorf("expected unavailable=kcc-not-installed summary note, got:\n%s", res.Stdout)
	}
	checktest.VerifyContract(t, cmd, "-A")
}

func TestKCCHealthyIsSilent(t *testing.T) {
	objs := []runtime.Object{
		kccObj(sqlGV, "SQLInstance", "prod", "orders-db", 3, 3, "True", "UpToDate", "The resource is up to date", nil),
		kccObj(storageGV, "StorageBucket", "prod", "orders-archive", 1, 1, "True", "UpToDate", "The resource is up to date", nil),
	}
	_, res := runKCC(t, true, objs, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr = %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "scanned=2 findings=0") {
		t.Errorf("healthy KCC objects should be silent, got:\n%s", res.Stdout)
	}
}

func TestKCCDetectsFailureModes(t *testing.T) {
	delStuck := kccNow.Add(-10 * time.Minute)
	objs := []runtime.Object{
		// 1. UpToDateFailed
		kccObj(sqlGV, "SQLInstance", "prod", "main-db", 2, 2, "False", "UpdateFailed", "googleapi: Error 403: Cloud SQL Admin API has not been used", nil),
		// 2. DependencyNotReady
		kccObj(iamGV, "IAMPolicyMember", "prod", "db-writer", 1, 1, "False", "DependencyNotReady", "reference SQLInstance prod/main-db is not ready", nil),
		// 3. ManagementConflict
		kccObj(storageGV, "StorageBucket", "prod", "shared-bucket", 1, 1, "False", "ManagementConflict", "conflict on cnrm.cloud.google.com/management-conflict-prevention-policy", nil),
		// 4. DeletionBlocked (DeleteFailed + deletionTimestamp)
		kccObj(storageGV, "StorageBucket", "prod", "nonempty-bucket", 4, 4, "False", "DeleteFailed", "bucket is not empty", &delStuck),
		// 5. ReconcileStalled (observedGeneration < generation while Ready=True)
		kccObj(sqlGV, "SQLInstance", "prod", "replica-db", 5, 2, "True", "UpToDate", "The resource is up to date", nil),
	}

	cmd, res := runKCC(t, true, objs, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr = %s", res.Code, res.Stderr)
	}
	for _, want := range []string{
		"kind=kcc.not_ready severity=warning namespace=prod kind_of_object=SQLInstance name=main-db reason=UpToDateFailed",
		"condition_reason=UpdateFailed",
		"kind=kcc.not_ready severity=warning namespace=prod kind_of_object=IAMPolicyMember name=db-writer reason=DependencyNotReady",
		"condition_reason=DependencyNotReady",
		"kind=kcc.not_ready severity=warning namespace=prod kind_of_object=StorageBucket name=shared-bucket reason=ManagementConflict",
		"condition_reason=ManagementConflict",
		"kind=kcc.not_ready severity=warning namespace=prod kind_of_object=StorageBucket name=nonempty-bucket reason=DeletionBlocked",
		"condition_reason=DeleteFailed",
		"kind=kcc.not_ready severity=info namespace=prod kind_of_object=SQLInstance name=replica-db reason=ReconcileStalled",
		"observed_generation=2 generation=5",
		"scanned=5 findings=5",
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("missing %q in stdout:\n%s", want, res.Stdout)
		}
	}
	checktest.VerifyContract(t, cmd, "-A")
}

func TestKCCNamespaceScope(t *testing.T) {
	objs := []runtime.Object{
		kccObj(sqlGV, "SQLInstance", "prod", "prod-db", 1, 1, "False", "UpdateFailed", "quota exceeded", nil),
		kccObj(sqlGV, "SQLInstance", "staging", "stage-db", 1, 1, "False", "UpdateFailed", "quota exceeded", nil),
	}
	_, res := runKCC(t, true, objs, "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr = %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "name=prod-db") || strings.Contains(res.Stdout, "name=stage-db") {
		t.Errorf("expected only prod namespace finding, got:\n%s", res.Stdout)
	}
}

func TestKCCRefusalDegradesCleanly(t *testing.T) {
	cs := fake.NewClientset()
	cs.Resources = []*metav1.APIResourceList{{
		GroupVersion: sqlGV.String(),
		APIResources: []metav1.APIResource{
			{Name: "sqlinstances", Kind: "SQLInstance", Namespaced: true, Verbs: metav1.Verbs{"get", "list"}},
		},
	}}
	scheme := runtime.NewScheme()
	dyn := fakedynamic.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		sqlInstanceGVR: "SQLInstanceList",
	})
	dyn.PrependReactor("list", "sqlinstances", func(a k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: sqlGV.Group, Resource: "sqlinstances"}, "", nil)
	})
	cmd := state.KCCCommand(state.KCCDeps{
		Client:  func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Dynamic: func(context.Context) (dynamic.Interface, error) { return dyn, nil },
		Now:     func() time.Time { return kccNow },
	})
	res := checktest.Run(t, cmd, "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit = %d, stderr = %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "kind=read.unavailable") || !strings.Contains(res.Stdout, "sqlinstances.sql.cnrm.cloud.google.com") {
		t.Errorf("expected read.unavailable finding for refused sqlinstances list, got:\n%s", res.Stdout)
	}
}
