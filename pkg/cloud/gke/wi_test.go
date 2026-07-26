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

//go:build gke || allproviders

package gke

// §13: the recorded IAM policy fixture (authored from the API
// reference — provenance in the fixture header) replayed through the
// wiIAMClient small interface; no live-project calls.

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"
	iam "google.golang.org/api/iam/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// fakeIAMPolicyClient replays one canned policy (or error) and
// records the emails asked for.
type fakeIAMPolicyClient struct {
	policy *iam.Policy
	err    error
	emails []string
}

func (f *fakeIAMPolicyClient) GetServiceAccountIamPolicy(_ context.Context, email string) (*iam.Policy, error) {
	f.emails = append(f.emails, email)
	if f.err != nil {
		return nil, f.err
	}
	return f.policy, nil
}

const wiTestGSA = "api-runtime@my-project.iam.gserviceaccount.com"

func wiPolicyAPI(t *testing.T) (*wiAPI, *fakeIAMPolicyClient) {
	t.Helper()
	var policy iam.Policy
	loadJSON(t, "iam-gsa-policy.json", &policy)
	fake := &fakeIAMPolicyClient{policy: &policy}
	return &wiAPI{project: "my-project", client: fake}, fake
}

func TestVerifyBindingBound(t *testing.T) {
	api, fake := wiPolicyAPI(t)
	b, err := api.VerifyBinding(context.Background(), "prod", "api-sa", wiTestGSA)
	if err != nil {
		t.Fatalf("VerifyBinding: %v", err)
	}
	if !b.Bound || len(b.Problems) != 0 {
		t.Errorf("binding = %+v, want Bound with no problems (fixture grants the exact member)", b)
	}
	if b.Namespace != "prod" || b.ServiceAccount != "api-sa" || b.CloudIdentity != wiTestGSA {
		t.Errorf("identity echo = %+v, want prod/api-sa claiming %s", b, wiTestGSA)
	}
	if len(fake.emails) != 1 || fake.emails[0] != wiTestGSA {
		t.Errorf("policy read for %v, want exactly [%s]", fake.emails, wiTestGSA)
	}
}

func TestVerifyBindingNoBinding(t *testing.T) {
	api, _ := wiPolicyAPI(t)
	// staging/other-sa is not a member of the fixture's
	// workloadIdentityUser binding.
	b, err := api.VerifyBinding(context.Background(), "staging", "other-sa", wiTestGSA)
	if err != nil {
		t.Fatalf("VerifyBinding: %v", err)
	}
	if b.Bound {
		t.Fatal("Bound for a non-member, want unbound")
	}
	if len(b.Problems) != 1 || !strings.HasPrefix(b.Problems[0], cloud.WIProblemNoBinding) {
		t.Fatalf("Problems = %v, want [0] leading with %q", b.Problems, cloud.WIProblemNoBinding)
	}
	// The detail names the member looked for and the role, so the
	// operator sees exactly what was matched.
	for _, want := range []string{
		"serviceAccount:my-project.svc.id.goog[staging/other-sa]",
		wiUserRole,
	} {
		if !strings.Contains(b.Problems[0], want) {
			t.Errorf("problem detail %q missing %q", b.Problems[0], want)
		}
	}
	if b.CloudIdentity != wiTestGSA {
		t.Errorf("CloudIdentity = %q, want the claimed identity echoed", b.CloudIdentity)
	}
}

func TestVerifyBindingIdentityMissing(t *testing.T) {
	api := &wiAPI{project: "my-project", client: &fakeIAMPolicyClient{
		err: &googleapi.Error{Code: http.StatusNotFound, Message: "Service account does not exist."},
	}}
	b, err := api.VerifyBinding(context.Background(), "prod", "api-sa", wiTestGSA)
	if err != nil {
		t.Fatalf("404 must map to a verdict, not an error, got: %v", err)
	}
	if b.Bound {
		t.Fatal("Bound for a nonexistent GSA")
	}
	if len(b.Problems) != 1 || !strings.HasPrefix(b.Problems[0], cloud.WIProblemIdentityMissing) {
		t.Fatalf("Problems = %v, want [0] leading with %q", b.Problems, cloud.WIProblemIdentityMissing)
	}
	if !strings.Contains(b.Problems[0], wiTestGSA) {
		t.Errorf("problem detail %q does not name the missing GSA", b.Problems[0])
	}
}

// A 403 is ambiguous (GSA may exist while the caller lacks
// getIamPolicy) — it must surface as an error, never as a verdict.
func TestVerifyBindingPermissionErrorFailsLoudly(t *testing.T) {
	api := &wiAPI{project: "my-project", client: &fakeIAMPolicyClient{
		err: &googleapi.Error{Code: http.StatusForbidden, Message: "Permission denied."},
	}}
	if _, err := api.VerifyBinding(context.Background(), "prod", "api-sa", wiTestGSA); err == nil {
		t.Fatal("403 returned a verdict, want a loud error")
	}
}

// Without a resolvable project the workload-identity capability
// degrades with the explicit §2 reason (the expected member embeds
// the project's identity pool).
func TestWorkloadIdentityUnavailableWithoutProject(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("CLOUDSDK_CORE_PROJECT", "")
	t.Setenv(metadataHostEnv, "localhost:1")
	p, err := New(context.Background(), cloud.Config{})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if _, ok := p.WorkloadIdentity(); ok {
		t.Fatal("WorkloadIdentity() available without a project")
	}
	u := cloud.Unavailable(p, cloud.CapabilityWorkloadIdentity)
	if u.Reason != reasonNoProject {
		t.Errorf("reason = %q, want %q", u.Reason, reasonNoProject)
	}

	withProject, err := New(context.Background(), cloud.Config{Project: "my-project"})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if _, ok := withProject.WorkloadIdentity(); !ok {
		t.Error("WorkloadIdentity() unavailable with a project pinned")
	}
}
