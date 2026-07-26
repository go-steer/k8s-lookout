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

// WorkloadIdentityAPI implementation (`state wi`, DESIGN.md §5): one
// IAM read per claimed GSA. The KSA is authorized when the GSA's IAM
// policy grants roles/iam.workloadIdentityUser to the member
// serviceAccount:<project>.svc.id.goog[<ns>/<ksa>]. Same SDK choice
// as the rest of the M4/M5 capabilities (compute.go): the
// google.golang.org/api REST discovery client, whose JSON wire
// shapes are the recorded-fixture format.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"google.golang.org/api/googleapi"
	iam "google.golang.org/api/iam/v1"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// wiUserRole is the IAM role that authorizes a KSA to act as a GSA.
const wiUserRole = "roles/iam.workloadIdentityUser"

// wiIAMClient is the §13 small client interface over the one IAM
// call the verification needs; production is
// projects.serviceAccounts.getIamPolicy, tests replay recorded JSON
// policy fixtures.
type wiIAMClient interface {
	GetServiceAccountIamPolicy(ctx context.Context, email string) (*iam.Policy, error)
}

// wiAPI implements cloud.WorkloadIdentityAPI.
type wiAPI struct {
	project string
	client  wiIAMClient
}

func newWIAPI(p *Provider) *wiAPI {
	svc := lazyClient(func(ctx context.Context) (*iam.Service, error) {
		return iam.NewService(ctx)
	})
	return &wiAPI{
		project: p.project,
		client:  &iamPolicyClient{svc: svc},
	}
}

// iamPolicyClient is the production wiIAMClient.
type iamPolicyClient struct {
	svc func(ctx context.Context) (*iam.Service, error)
}

func (c *iamPolicyClient) GetServiceAccountIamPolicy(ctx context.Context, email string) (*iam.Policy, error) {
	svc, err := c.svc(ctx)
	if err != nil {
		return nil, fmt.Errorf("iam client: %w", err)
	}
	// "projects/-" resolves the owning project from the email, so
	// cross-project GSAs verify too.
	return svc.Projects.ServiceAccounts.GetIamPolicy("projects/-/serviceAccounts/" + email).Context(ctx).Do()
}

// VerifyBinding implements cloud.WorkloadIdentityAPI: is the cluster
// identity namespace/serviceAccount authorized to act as
// cloudIdentity (the GSA email the KSA's annotation claims)?
//
// Known limitation (documented, not silently papered over): only the
// exact svc.id.goog member is accepted. A workloadIdentityUser grant
// routed through a group or a wildcard member verifies fine in GCP
// but reports unbound here — the detail string carries the member
// looked for, so the operator can see exactly what was matched.
//
// A 404 on the GSA maps to WIProblemIdentityMissing (the claimed
// identity does not exist). A 403 is ambiguous — the GSA may exist
// while the caller lacks iam.serviceAccounts.getIamPolicy — so it
// surfaces as an error (§2 fail-loudly), never as a verdict.
func (a *wiAPI) VerifyBinding(ctx context.Context, namespace, serviceAccount, cloudIdentity string) (cloud.WIBinding, error) {
	b := cloud.WIBinding{
		Namespace:      namespace,
		ServiceAccount: serviceAccount,
		CloudIdentity:  cloudIdentity,
	}
	policy, err := a.client.GetServiceAccountIamPolicy(ctx, cloudIdentity)
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
		b.Problems = []string{fmt.Sprintf("%s: service account %s not found in IAM", cloud.WIProblemIdentityMissing, cloudIdentity)}
		return b, nil
	}
	if err != nil {
		return cloud.WIBinding{}, fmt.Errorf("reading IAM policy of %s: %w", cloudIdentity, err)
	}
	member := fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s/%s]", a.project, namespace, serviceAccount)
	for _, binding := range policy.Bindings {
		if binding == nil || binding.Role != wiUserRole {
			continue
		}
		if slices.Contains(binding.Members, member) {
			b.Bound = true
			return b, nil
		}
	}
	b.Problems = []string{fmt.Sprintf("%s: %s lacks %s on %s", cloud.WIProblemNoBinding, member, wiUserRole, cloudIdentity)}
	return b, nil
}
