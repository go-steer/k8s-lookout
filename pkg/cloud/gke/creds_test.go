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
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// gkeTokenRefusal is the body the GKE metadata server answers a token
// request with when the pod's KSA may not act as its GSA — the shape
// of the #179/#180 UAT failure (#614).
const gkeTokenRefusal = "Unable to generate access token; IAM returned 403 Forbidden: Permission 'iam.serviceAccounts.getAccessToken' denied on resource (or it may not exist).\n" +
	"This error could be caused by a missing IAM policy binding on the target IAM service account.\n" +
	"For more information, refer to the Workload Identity documentation:\n" +
	"\thttps://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#authenticating_to\n"

// RefusingMetadataServer is a fake GKE metadata server that answers
// every token request with code/body and serves nothing else (404,
// which the auth stack reads as "not defined" and defaults around).
// It points the process's credential detection at itself and away
// from any developer ADC, so the production clients — built with no
// options, exactly as the Provider builds them — hit it. Exported
// (test-only) for the command-level tests in package gke_test.
func RefusingMetadataServer(t *testing.T, code int, body string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Metadata-Flavor", "Google")
		if strings.HasSuffix(r.URL.Path, "/token") {
			http.Error(w, body, code)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Setenv(metadataHostEnv, strings.TrimPrefix(srv.URL, "http://"))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_CONFIG", t.TempDir())
	t.Setenv("HOME", t.TempDir())
}

// PinnedProvider is a GKE provider with its identity pinned, so
// construction makes no metadata lookups.
func PinnedProvider(t *testing.T) cloud.Provider {
	t.Helper()
	p, err := New(context.Background(), cloud.Config{Project: "my-project", Location: "us-central1", Cluster: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func assertCredentialsUnavailable(t *testing.T, err error) {
	t.Helper()
	fix, ok := cloud.CredentialsUnavailable(err)
	if !ok {
		t.Fatalf("error not classified CredentialsUnavailable: %v", err)
	}
	if fix != credentialsFix {
		t.Errorf("fix = %q, want the GKE remedy", fix)
	}
	if _, denied := cloud.PermissionDenied(err); denied {
		t.Errorf("a refused token must not also read as PermissionDenied: %v", err)
	}
}

// TestCapabilities_MetadataTokenRefusal drives the real REST clients
// (IAM, Compute, Monitoring, Container) through the real credential
// stack against a metadata server refusing the token the way GKE
// does for a missing roles/iam.workloadIdentityUser binding: each
// capability must surface cloud.ErrCredentialsUnavailable rather
// than the raw multi-line auth error.
func TestCapabilities_MetadataTokenRefusal(t *testing.T) {
	RefusingMetadataServer(t, http.StatusForbidden, gkeTokenRefusal)
	p := PinnedProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	calls := map[string]func() error{
		"workload-identity": func() error {
			api, _ := p.WorkloadIdentity()
			_, err := api.VerifyBinding(ctx, "lookout-uat", "lookout-uat", "lookout@my-project.iam.gserviceaccount.com")
			return err
		},
		"orphans": func() error {
			api, _ := p.Orphans()
			_, err := api.OrphanDisks(ctx)
			return err
		},
		"quota": func() error {
			api, _ := p.Quota()
			_, err := api.Quotas(ctx)
			return err
		},
		"metrics": func() error {
			api, _ := p.Metrics()
			_, err := api.QuerySeries(ctx, cloud.SeriesQuery{
				Metric: "container/cpu/used_millicores",
				Window: cloud.TimeWindow{Start: time.Now().Add(-time.Hour), End: time.Now()},
				Step:   time.Minute,
			})
			return err
		},
		"cluster-config": func() error {
			api, _ := p.ClusterConfig()
			_, err := api.Config(ctx)
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			assertCredentialsUnavailable(t, err)
			// The raw cause stays reachable for logs and stderr.
			var mdErr *metadata.Error
			if !errors.As(err, &mdErr) || mdErr.Code != http.StatusForbidden {
				t.Errorf("metadata 403 not reachable through the chain: %v", err)
			}
		})
	}
}

// TestCapabilities_MetadataOutageStaysRuntime: a 5xx from the token
// endpoint is an outage, not a missing grant — it must stay a runtime
// error rather than degrade with a remedy that does not apply.
func TestCapabilities_MetadataOutageStaysRuntime(t *testing.T) {
	RefusingMetadataServer(t, http.StatusServiceUnavailable, "metadata server unavailable")
	p := PinnedProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	api, _ := p.Orphans()
	_, err := api.OrphanDisks(ctx)
	if err == nil {
		t.Fatal("want an error from a failing metadata server")
	}
	if _, ok := cloud.CredentialsUnavailable(err); ok {
		t.Errorf("5xx token failure classified CredentialsUnavailable: %v", err)
	}
}

// TestClassifyCredentials_OAuth2RetrieveError drives a real
// golang.org/x/oauth2 token source against a token endpoint that
// rejects the grant: the RetrieveError rides out through *url.Error.
func TestClassifyCredentials_OAuth2RetrieveError(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"error":"invalid_grant","error_description":"Token has been expired or revoked."}`)
	}))
	defer tokenSrv.Close()
	apiSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("the API must not be reached without a token")
	}))
	defer apiSrv.Close()

	cfg := &oauth2.Config{ClientID: "id", Endpoint: oauth2.Endpoint{TokenURL: tokenSrv.URL}}
	client := oauth2.NewClient(context.Background(), cfg.TokenSource(context.Background(), &oauth2.Token{RefreshToken: "r"}))
	resp, err := client.Get(apiSrv.URL)
	if resp != nil {
		_ = resp.Body.Close()
	}
	var rErr *oauth2.RetrieveError
	if !errors.As(err, &rErr) {
		t.Fatalf("precondition: want an oauth2.RetrieveError in the chain, got %v", err)
	}
	assertCredentialsUnavailable(t, classifyCredentials(fmt.Errorf("listing disks: %w", err)))
}

func TestClassifyCredentials_Shapes(t *testing.T) {
	grpcRefusal := status.Error(codes.Unauthenticated, grpcCredsFailedPrefix+
		"credentials: cannot fetch token: compute: Received 403 `"+gkeTokenRefusal+"`")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"metadata 403 via credentials and url.Error", &url.Error{Op: "Get", URL: "https://compute.googleapis.com/compute/v1/projects/p/aggregated/disks",
			Err: fmt.Errorf("credentials: cannot fetch token: %w", &metadata.Error{Code: 403, Message: gkeTokenRefusal})}, true},
		{"metadata 404", &metadata.Error{Code: 404, Message: "not found"}, true},
		{"metadata 500", &metadata.Error{Code: 500, Message: "boom"}, false},
		{"oauth2 RetrieveError 400", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 400}, ErrorCode: "invalid_grant"}, true},
		{"oauth2 RetrieveError 503", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 503}}, false},
		{"auth.Error 401", fmt.Errorf("auth: %w", &auth.Error{Response: &http.Response{StatusCode: 401}}), true},
		{"auth.Error 502", &auth.Error{Response: &http.Response{StatusCode: 502}}, false},
		{"metadata 408 is retryable", &metadata.Error{Code: 408}, false},
		{"metadata 429 is retryable", &metadata.Error{Code: 429}, false},
		{"oauth2 RetrieveError 429", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 429}}, false},
		{"auth.Error 408", &auth.Error{Response: &http.Response{StatusCode: 408}}, false},
		{"gRPC per-RPC creds failure, 5xx inside", status.Error(codes.Unauthenticated, grpcCredsFailedPrefix+"credentials: cannot fetch token: compute: Received 503 `unavailable`"), false},
		{"gRPC per-RPC creds failure, 429 inside", status.Error(codes.Unauthenticated, grpcCredsFailedPrefix+"auth: cannot fetch token: 429\nResponse: slow down"), false},
		{"gRPC per-RPC creds failure, oauth2 status form", status.Error(codes.Unauthenticated, grpcCredsFailedPrefix+"oauth2: cannot fetch token: 401 Unauthorized\nResponse: {}"), true},
		{"gRPC per-RPC creds failure, temporarily_unavailable code", status.Error(codes.Unauthenticated, grpcCredsFailedPrefix+`auth: "temporarily_unavailable"`), false},
		{"gRPC per-RPC creds failure, no marker", status.Error(codes.Unauthenticated, grpcCredsFailedPrefix+"context deadline exceeded"), false},
		{"gRPC per-RPC creds failure", grpcRefusal, true},
		{"gRPC per-RPC creds failure, wrapped", fmt.Errorf("quota preferences: %w", grpcRefusal), true},
		{"gRPC Unauthenticated from the server", status.Error(codes.Unauthenticated, "Request had invalid authentication credentials."), false},
		{"gRPC PermissionDenied", status.Error(codes.PermissionDenied, grpcCredsFailedPrefix+"x"), false},
		{"API 403 is PermissionDenied, not credentials", &googleapi.Error{Code: 403, Message: "Required 'compute.disks.list' permission for 'projects/p'"}, false},
		{"metric absent", cloud.ErrMetricAbsent, false},
		{"plain error mentioning tokens", errors.New("cannot fetch token: Unable to generate access token"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyCredentials(tt.err)
			_, ok := cloud.CredentialsUnavailable(got)
			if ok != tt.want {
				t.Fatalf("classified=%v, want %v (err %v)", ok, tt.want, tt.err)
			}
			if !ok && !errors.Is(got, tt.err) {
				t.Errorf("unclassified error rewritten: %v → %v", tt.err, got)
			}
			if ok && !errors.Is(got, tt.err) {
				t.Errorf("original error not reachable through the classification")
			}
		})
	}
}

func TestClassifyCredentials_Idempotent(t *testing.T) {
	once := classifyCredentials(&metadata.Error{Code: 403})
	if twice := classifyCredentials(once); twice != once {
		t.Errorf("re-classification wrapped again: %v", twice)
	}
}

// TestCredsDecorator_PassesOtherErrors: the decorator only adds the
// credentials classification — cloud.ErrMetricAbsent (the perf
// pack_unavailable path) survives it untouched.
func TestCredsDecorator_PassesOtherErrors(t *testing.T) {
	_, err := credsMetrics{inner: absentBackend{}}.QuerySeries(context.Background(), cloud.SeriesQuery{})
	if !errors.Is(err, cloud.ErrMetricAbsent) {
		t.Errorf("ErrMetricAbsent lost through the metrics decorator: %v", err)
	}
	if _, ok := cloud.CredentialsUnavailable(err); ok {
		t.Errorf("metric absence classified as credentials: %v", err)
	}
}

type absentBackend struct{}

func (absentBackend) QuerySeries(context.Context, cloud.SeriesQuery) ([]cloud.Series, error) {
	return nil, fmt.Errorf("query: %w", cloud.ErrMetricAbsent)
}

// failingRPCCreds is a per-RPC credential whose token source fails
// with err — the way a token failure reaches a gRPC client.
type failingRPCCreds struct{ err error }

func (c failingRPCCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return nil, c.err
}

func (failingRPCCreds) RequireTransportSecurity() bool { return false }

// grpcCallWith makes one RPC on a live grpc-go connection whose
// per-RPC credential fails with credErr, and returns the RPC's error.
func grpcCallWith(t *testing.T, credErr error) error {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(failingRPCCreds{err: credErr}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

// TestClassifyCredentials_GRPCTransport pins the one string match
// against the real grpc-go transport. grpc-go puts EVERY failing
// per-RPC credential behind the same Unauthenticated prefix, so only
// a token endpoint's refusal may classify: a 5xx, a 429, a dial error
// or a deadline inside the token fetch are outages and stay runtime
// errors (the gRPC clients — Cloud Quotas, Logging, Pub/Sub — lose
// the typed chain there, so the transport's text is all there is).
func TestClassifyCredentials_GRPCTransport(t *testing.T) {
	fetch := func(err error) error { return fmt.Errorf("credentials: cannot fetch token: %w", err) }
	for _, tc := range []struct {
		name string
		cred error
		want bool
	}{
		{"metadata 403 (missing binding)", fetch(&metadata.Error{Code: 403, Message: gkeTokenRefusal}), true},
		{"auth.Error 400 invalid_grant", fetch(&auth.Error{Response: &http.Response{StatusCode: 400}, Body: []byte(`{"error":"invalid_grant"}`)}), true},
		{"auth.Error 401 status form", fetch(&auth.Error{Response: &http.Response{StatusCode: 401}}), true},
		{"metadata 503", fetch(&metadata.Error{Code: 503, Message: "metadata server unavailable"}), false},
		{"metadata 429", fetch(&metadata.Error{Code: 429, Message: "slow down"}), false},
		{"auth.Error 500", fetch(&auth.Error{Response: &http.Response{StatusCode: 500}}), false},
		{"dial error", fetch(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}), false},
		{"deadline in the token fetch", fetch(&url.Error{Op: "Get", URL: "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token", Err: context.DeadlineExceeded}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := grpcCallWith(t, tc.cred)
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("precondition: want Unauthenticated from the transport, got %v", err)
			}
			got := classifyCredentials(fmt.Errorf("listing quota infos: %w", err))
			if tc.want {
				assertCredentialsUnavailable(t, got)
				return
			}
			if _, ok := cloud.CredentialsUnavailable(got); ok {
				t.Errorf("classified CredentialsUnavailable: %v", err)
			}
		})
	}
}

// TestCredsDecorator_EndedContextNotClassified: once the caller's
// context has ended, even a refusal-shaped error is returned as is —
// a --timeout expiring mid token fetch is the runtime error the
// output contract promises, never a degraded exit 0.
func TestCredsDecorator_EndedContextNotClassified(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := credsQuota{inner: refusedQuota{}}.Quotas(ctx)
	if _, ok := cloud.CredentialsUnavailable(err); ok {
		t.Errorf("classified after the context ended: %v", err)
	}
	if _, err := (credsQuota{inner: refusedQuota{}}).Quotas(context.Background()); err == nil {
		t.Fatal("want an error")
	} else {
		assertCredentialsUnavailable(t, err)
	}
}

type refusedQuota struct{}

func (refusedQuota) Quotas(context.Context) ([]cloud.QuotaUsage, error) {
	return nil, fmt.Errorf("credentials: cannot fetch token: %w", &metadata.Error{Code: 403, Message: gkeTokenRefusal})
}

func (refusedQuota) History(context.Context, string, string, cloud.TimeWindow) (cloud.QuotaHistory, error) {
	return cloud.QuotaHistory{}, nil
}

// TestOrphanLoadBalancers_RuleCredentialsEndsTheClass (#614 × #612):
// a refused token on one rule's resolution chain is not that rule's
// failure — every later GET would be refused too — so the class
// fails with it, and through the real `cloud orphans` command it is
// one CredentialsUnavailable record and zero RuleUnresolved (whose
// masked error text would carry the token endpoint's prose).
func TestOrphanLoadBalancers_RuleCredentialsEndsTheClass(t *testing.T) {
	refused := &url.Error{Op: "Get", URL: "https://compute.googleapis.com/compute/v1/projects/p/global/backendServices/x",
		Err: fmt.Errorf("credentials: cannot fetch token: %w", &metadata.Error{Code: 403, Message: gkeTokenRefusal})}
	newAPI := func() *orphanAPI {
		return &orphanAPI{gce: &failingGCE{fixtureGCE: newFixtureGCE(t), service: "hypercomputer-w1-traffic-backend", err: refused}}
	}

	sweep, err := newAPI().OrphanLoadBalancers(context.Background())
	assertCredentialsUnavailable(t, err)
	if len(sweep.Unresolved) != 0 || len(sweep.Orphans) != 0 {
		t.Errorf("sweep = %+v, want nothing returned beside the class error", sweep)
	}

	p := orphansOnly{Provider: cloud.NoProvider, api: credsOrphans{newAPI()}}
	res := checktest.Run(t, cloudcheck.OrphansCommand(cloudcheck.Deps{
		Provider: func(context.Context) (cloud.Provider, error) { return p, nil },
	}), "--only=lbs")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, want 0 (stderr: %s)", res.Code, res.Stderr)
	}
	if n := strings.Count(res.Stdout, "reason=CredentialsUnavailable"); n != 1 {
		t.Errorf("CredentialsUnavailable records = %d, want 1:\n%s", n, res.Stdout)
	}
	for _, bad := range []string{"RuleUnresolved", "SweepFailed", "Unable to generate", "https://"} {
		if strings.Contains(res.Stdout, bad) {
			t.Errorf("stdout carries %q:\n%s", bad, res.Stdout)
		}
	}
}

// orphansOnly is NoProvider plus one orphans capability.
type orphansOnly struct {
	cloud.Provider
	api cloud.OrphanAPI
}

func (p orphansOnly) Orphans() (cloud.OrphanAPI, bool) { return p.api, true }
