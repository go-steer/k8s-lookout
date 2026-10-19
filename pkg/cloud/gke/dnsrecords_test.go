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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	dns "google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// recordedDNSServer serves the recorded Cloud DNS fixtures at the
// REST paths the production client requests: the zone list, each
// zone's record sets, and a 404 for gone-zone (deleted between the
// list and the read). It returns the server and the paths requested.
func recordedDNSServer(t *testing.T) (*httptest.Server, *[]string) {
	t.Helper()
	var paths []string
	const zones = "/dns/v1/projects/" + fixtureProject + "/managedZones"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case zones:
			_, _ = w.Write(readFixture(t, "dns-managedzones.json"))
		case zones + "/public-zone/rrsets":
			_, _ = w.Write(readFixture(t, "dns-rrsets-public-zone.json"))
		case zones + "/private-zone/rrsets":
			_, _ = w.Write(readFixture(t, "dns-rrsets-private-zone.json"))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":404,"message":"The 'parameters.managedZone' resource named 'gone-zone' does not exist.","errors":[{"reason":"notFound"}]}}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &paths
}

func recordedDNSClient(srv *httptest.Server) *cloudDNSClient {
	return &cloudDNSClient{
		project: fixtureProject,
		svc: func(ctx context.Context) (*dns.Service, error) {
			return dns.NewService(ctx, option.WithEndpoint(srv.URL+"/"), option.WithoutAuthentication())
		},
	}
}

// TestAddressDNSRecordsFromRecordedZones drives the production client
// over the recorded zones (#716): A/AAAA data, geo, WRR and
// primary-backup routing targets, a CNAME onto a wildcard, a CNAME
// across zones, a CNAME loop, a TXT carrying an IP (not a record that
// resolves to it), a zone deleted mid-sweep, and an AAAA written in
// another textual form than the address asked about.
func TestAddressDNSRecordsFromRecordedZones(t *testing.T) {
	srv, paths := recordedDNSServer(t)
	api := &orphanAPI{dns: recordedDNSClient(srv)}

	got, err := api.AddressDNSRecords(context.Background(), []string{"34.120.10.20", "35.196.1.2", "2001:db8::1", "198.51.100.1"})
	if err != nil {
		t.Fatalf("AddressDNSRecords: %v", err)
	}
	want := []cloud.DNSRecord{
		{Name: "v6.example.test.", Type: "AAAA", Zone: "public-zone", Address: "2001:db8::1"},
		{Name: "wrr.internal.example.test.", Type: "A", Zone: "private-zone", Address: "34.120.10.20"},
		{Name: "*.example.test.", Type: "A", Zone: "public-zone", Address: "34.120.10.20"},
		{Name: "www.example.test.", Type: "CNAME", Zone: "public-zone", Address: "34.120.10.20"},
		{Name: "api.internal.example.test.", Type: "CNAME", Zone: "private-zone", Address: "35.196.1.2"},
		{Name: "hc.internal.example.test.", Type: "A", Zone: "private-zone", Address: "35.196.1.2"},
		{Name: "geo.example.test.", Type: "A", Zone: "public-zone", Address: "35.196.1.2"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("records:\n got %+v\nwant %+v", got, want)
	}
	wantPaths := []string{
		"/dns/v1/projects/proj-1/managedZones",
		"/dns/v1/projects/proj-1/managedZones/gone-zone/rrsets",
		"/dns/v1/projects/proj-1/managedZones/private-zone/rrsets",
		"/dns/v1/projects/proj-1/managedZones/public-zone/rrsets",
	}
	if !slices.Equal(*paths, wantPaths) {
		t.Errorf("requested %v, want %v (zones in name order)", *paths, wantPaths)
	}
}

// TestOrphansCommandOverRecordedDNS runs `cloud orphans
// --only=addresses` through the provider's credential decorator over
// the recorded Compute and Cloud DNS fixtures: both idle addresses are
// referenced (old-ingress-ip by the public wildcard, spare-egress-ip
// by the geo record), so neither is recommended for release.
func TestOrphansCommandOverRecordedDNS(t *testing.T) {
	srv, _ := recordedDNSServer(t)
	api := credsOrphans{&orphanAPI{gce: newFixtureGCE(t), dns: recordedDNSClient(srv)}}
	p := orphansOnly{Provider: cloud.NoProvider, api: api}
	res := checktest.Run(t, cloudcheck.OrphansCommand(cloudcheck.Deps{
		Provider: func(context.Context) (cloud.Provider, error) { return p, nil },
	}), "--only=addresses", "--min-age=0s")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0 (stderr: %s)", res.Code, res.Stderr)
	}
	for _, want := range []string{
		`name=old-ingress-ip reason=ReferencedByDNS`,
		`dns_records="wrr.internal.example.test. A in zone private-zone, *.example.test. A in zone public-zone, www.example.test. CNAME in zone public-zone"`,
		`name=spare-egress-ip reason=ReferencedByDNS`,
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
		}
	}
	if strings.Contains(res.Stdout, "billed until released") {
		t.Errorf("a DNS-referenced address was recommended for release:\n%s", res.Stdout)
	}
}

// TestAddressDNSRecordsNothingToAsk: no parsable address, no call.
func TestAddressDNSRecordsNothingToAsk(t *testing.T) {
	srv, paths := recordedDNSServer(t)
	api := &orphanAPI{dns: recordedDNSClient(srv)}
	for _, in := range [][]string{nil, {"", "not-an-ip"}} {
		got, err := api.AddressDNSRecords(context.Background(), in)
		if err != nil || got != nil || len(*paths) != 0 {
			t.Errorf("AddressDNSRecords(%q) = %v, %v after %v requests; want nothing asked", in, got, err, *paths)
		}
	}
}

// refusingDNS fails one of the two list calls.
type refusingDNS struct {
	zonesErr, recordsErr error
}

func (r refusingDNS) ListManagedZones(context.Context) ([]*dns.ManagedZone, error) {
	if r.zonesErr != nil {
		return nil, r.zonesErr
	}
	return []*dns.ManagedZone{{Name: "public-zone"}}, nil
}

func (r refusingDNS) ListRecordSets(context.Context, string) ([]*dns.ResourceRecordSet, error) {
	return nil, r.recordsErr
}

// TestAddressDNSRecordsErrors: a refusal names the permission the
// refused call needs; a disabled API says so in one short line,
// never as a grant to fix; anything else stays the provider's error.
func TestAddressDNSRecordsErrors(t *testing.T) {
	ctx := context.Background()
	ask := []string{"34.120.10.20"}

	refused := decodedAPIError(t, http.StatusForbidden, readFixture(t, "dns-managedzones-403.json"))
	_, err := (&orphanAPI{dns: refusingDNS{zonesErr: refused}}).AddressDNSRecords(ctx, ask)
	if perm, ok := cloud.PermissionDenied(err); !ok || perm != permDNSZonesList {
		t.Errorf("zone list 403: (%q, %v), want %s", perm, ok, permDNSZonesList)
	}
	_, err = (&orphanAPI{dns: refusingDNS{recordsErr: refused}}).AddressDNSRecords(ctx, ask)
	if perm, ok := cloud.PermissionDenied(err); !ok || perm != permDNSRecordsList {
		t.Errorf("record list 403: (%q, %v), want %s", perm, ok, permDNSRecordsList)
	}

	disabled := decodedAPIError(t, http.StatusForbidden, readFixture(t, "dns-api-disabled-403.json"))
	_, err = (&orphanAPI{dns: refusingDNS{zonesErr: disabled}}).AddressDNSRecords(ctx, ask)
	if _, denied := cloud.PermissionDenied(err); denied {
		t.Errorf("disabled API classified as a permission refusal: %v", err)
	}
	var off *apiDisabledError
	if !errors.As(err, &off) {
		t.Fatalf("disabled API err = %v, want an apiDisabledError", err)
	}
	if msg := err.Error(); msg != "listing Cloud DNS managed zones: the Cloud DNS API (dns.googleapis.com) is not enabled in the project" {
		t.Errorf("disabled API message = %q", msg)
	}
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != http.StatusForbidden {
		t.Errorf("disabled API error lost the googleapi.Error: %v", err)
	}

	internal := &googleapi.Error{Code: http.StatusInternalServerError, Message: "Internal Error"}
	_, err = (&orphanAPI{dns: refusingDNS{recordsErr: internal}}).AddressDNSRecords(ctx, ask)
	if err == nil || errors.Is(err, cloud.ErrPermissionDenied) || !strings.Contains(err.Error(), "zone public-zone") {
		t.Errorf("500: err = %v, want an unclassified error naming the zone", err)
	}
}
