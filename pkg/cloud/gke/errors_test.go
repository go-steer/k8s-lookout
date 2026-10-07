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

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// decodedAPIError runs a recorded error body through the same
// decoder the REST discovery clients use (googleapi.CheckResponse),
// so the classifier sees exactly the error production gets.
func decodedAPIError(t *testing.T, code int, body []byte) error {
	t.Helper()
	err := googleapi.CheckResponse(&http.Response{
		StatusCode: code,
		Header:     http.Header{},
		Body:       io.NopCloser(bytes.NewReader(body)),
	})
	if err == nil {
		t.Fatalf("CheckResponse(%d) returned nil", code)
	}
	return err
}

// refusingGCE is the recorded fixture client with per-call errors.
type refusingGCE struct {
	*fixtureGCE
	disksErr, rulesErr, addrsErr, backendErr error
}

func (r *refusingGCE) ListDisks(ctx context.Context) ([]*compute.Disk, error) {
	if r.disksErr != nil {
		return nil, r.disksErr
	}
	return r.fixtureGCE.ListDisks(ctx)
}

func (r *refusingGCE) ListForwardingRules(ctx context.Context) ([]*compute.ForwardingRule, error) {
	if r.rulesErr != nil {
		return nil, r.rulesErr
	}
	return r.fixtureGCE.ListForwardingRules(ctx)
}

func (r *refusingGCE) ListAddresses(ctx context.Context) ([]*compute.Address, error) {
	if r.addrsErr != nil {
		return nil, r.addrsErr
	}
	return r.fixtureGCE.ListAddresses(ctx)
}

func (r *refusingGCE) GetBackendService(ctx context.Context, scope, name string) (*compute.BackendService, error) {
	if r.backendErr != nil {
		return nil, r.backendErr
	}
	return r.fixtureGCE.GetBackendService(ctx, scope, name)
}

// TestOrphanAddresses403IsPermissionDenied: the recorded 403 on the
// address list becomes cloud.ErrPermissionDenied naming the
// permission, while the disk sweep on the same client is unaffected.
func TestOrphanAddresses403IsPermissionDenied(t *testing.T) {
	gce := &refusingGCE{fixtureGCE: newFixtureGCE(t)}
	gce.addrsErr = decodedAPIError(t, http.StatusForbidden, readFixture(t, "compute-addresses-403.json"))
	api := &orphanAPI{gce: gce}

	_, err := api.OrphanAddresses(context.Background())
	if !errors.Is(err, cloud.ErrPermissionDenied) {
		t.Fatalf("OrphanAddresses err = %v, want cloud.ErrPermissionDenied", err)
	}
	if perm, _ := cloud.PermissionDenied(err); perm != "compute.addresses.list" {
		t.Errorf("permission = %q, want compute.addresses.list (from the API message)", perm)
	}
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != http.StatusForbidden {
		t.Errorf("classified error lost the googleapi.Error: %v", err)
	}
	if _, err := api.OrphanDisks(context.Background()); err != nil {
		t.Errorf("OrphanDisks on the same client: %v", err)
	}
}

func TestClassifyPermission(t *testing.T) {
	bare403 := &googleapi.Error{Code: http.StatusForbidden, Message: "The caller does not have permission"}
	iamStyle := &googleapi.Error{Code: http.StatusForbidden, Message: "Permission 'compute.backendServices.get' denied on resource 'projects/p/global/backendServices/web-bs'"}
	throttled := &googleapi.Error{Code: http.StatusForbidden, Message: "Rate Limit Exceeded",
		Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded", Message: "Rate Limit Exceeded"}}}
	disabled := &googleapi.Error{Code: http.StatusForbidden, Message: "Compute Engine API has not been used in project",
		Errors: []googleapi.ErrorItem{{Reason: "accessNotConfigured"}}}
	internal := &googleapi.Error{Code: http.StatusInternalServerError, Message: "Internal Error",
		Errors: []googleapi.ErrorItem{{Reason: "backendError"}}}

	for _, tc := range []struct {
		name     string
		err      error
		hint     string
		denied   bool
		wantPerm string
	}{
		{"nil", nil, "compute.disks.list", false, ""},
		{"bare 403 falls back to the hint", bare403, "compute.disks.list", true, "compute.disks.list"},
		{"message beats the hint", iamStyle, "", true, "compute.backendServices.get"},
		{"rate limit 403 stays fatal", throttled, "compute.disks.list", false, ""},
		{"disabled API 403 stays fatal", disabled, "compute.disks.list", false, ""},
		{"500 stays fatal", internal, "compute.disks.list", false, ""},
		{"non-API error stays fatal", errors.New("dial tcp: i/o timeout"), "compute.disks.list", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyPermission(tc.err, tc.hint)
			perm, denied := cloud.PermissionDenied(got)
			if denied != tc.denied || perm != tc.wantPerm {
				t.Errorf("classify = (%q, %v), want (%q, %v)", perm, denied, tc.wantPerm, tc.denied)
			}
			if !tc.denied && got != tc.err {
				t.Errorf("unclassified error rewrapped: %v", got)
			}
		})
	}
}

// TestOrphanSweepsClassifyEveryClass: each class's list call names
// its own permission when the API message does not, and a refusal
// deep in LB resolution degrades the lb class too; a 500 is not
// classified.
func TestOrphanSweepsClassifyEveryClass(t *testing.T) {
	bare403 := &googleapi.Error{Code: http.StatusForbidden, Message: "Forbidden"}
	gce := &refusingGCE{fixtureGCE: newFixtureGCE(t), disksErr: bare403, rulesErr: bare403, addrsErr: bare403}
	api := &orphanAPI{gce: gce}
	ctx := context.Background()

	_, err := api.OrphanDisks(ctx)
	if perm, ok := cloud.PermissionDenied(err); !ok || perm != "compute.disks.list" {
		t.Errorf("disks: (%q, %v), want compute.disks.list", perm, ok)
	}
	_, err = api.OrphanLoadBalancers(ctx)
	if perm, ok := cloud.PermissionDenied(err); !ok || perm != "compute.forwardingRules.list" {
		t.Errorf("lbs: (%q, %v), want compute.forwardingRules.list", perm, ok)
	}
	_, err = api.OrphanAddresses(ctx)
	if perm, ok := cloud.PermissionDenied(err); !ok || perm != "compute.addresses.list" {
		t.Errorf("addresses: (%q, %v), want compute.addresses.list", perm, ok)
	}

	gce = &refusingGCE{fixtureGCE: newFixtureGCE(t), backendErr: &googleapi.Error{
		Code: http.StatusForbidden, Message: "Required 'compute.regionBackendServices.get' permission for 'projects/p/regions/us-east1/backendServices/empty-bs'"}}
	_, err = (&orphanAPI{gce: gce}).OrphanLoadBalancers(ctx)
	if perm, ok := cloud.PermissionDenied(err); !ok || perm != "compute.regionBackendServices.get" {
		t.Errorf("lb resolution: (%q, %v), want compute.regionBackendServices.get", perm, ok)
	}

	gce = &refusingGCE{fixtureGCE: newFixtureGCE(t), addrsErr: &googleapi.Error{Code: http.StatusInternalServerError, Message: "Internal Error"}}
	_, err = (&orphanAPI{gce: gce}).OrphanAddresses(ctx)
	if err == nil || errors.Is(err, cloud.ErrPermissionDenied) {
		t.Errorf("500: err = %v, want an unclassified runtime error", err)
	}
}
