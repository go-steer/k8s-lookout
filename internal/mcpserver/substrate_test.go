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

package mcpserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/internal/fleet"
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func TestReviewAccessGETFallbackWhenSSARRefused(t *testing.T) {
	for _, tc := range []struct {
		name        string
		withProxy   bool
		ssarCode    int
		ssarContent string
		ssarBody    string
		podsCode    int
		podsContent string
		podsBody    string
		wantAccess  string
		wantPodsHit bool
	}{
		{
			name:        "plain-text 405 from GET-only credential proxy with 200 pod list",
			withProxy:   true,
			ssarCode:    http.StatusMethodNotAllowed,
			ssarContent: "text/plain",
			ssarBody:    "method POST not allowed on /v1/k8s/",
			podsCode:    http.StatusOK,
			podsContent: "application/json",
			podsBody:    `{"kind":"PodList","apiVersion":"v1","items":[]}`,
			wantAccess:  accessRead,
			wantPodsHit: true,
		},
		{
			name:        "JSON 405 Status from proxy with structured K8s RBAC 403 Status",
			withProxy:   true,
			ssarCode:    http.StatusMethodNotAllowed,
			ssarContent: "application/json",
			ssarBody:    `{"kind":"Status","apiVersion":"v1","status":"Failure","code":405,"reason":"MethodNotAllowed"}`,
			podsCode:    http.StatusForbidden,
			podsContent: "application/json",
			podsBody:    `{"kind":"Status","apiVersion":"v1","status":"Failure","code":403,"reason":"Forbidden"}`,
			wantAccess:  accessLimited,
			wantPodsHit: true,
		},
		{
			name:        "403 on POST SSAR under CREDENTIAL_PROXY_URL falls back to GET pods and classifies 200 as read",
			withProxy:   true,
			ssarCode:    http.StatusForbidden,
			ssarContent: "text/plain",
			ssarBody:    "POST forbidden by proxy",
			podsCode:    http.StatusOK,
			podsContent: "application/json",
			podsBody:    `{"kind":"PodList","apiVersion":"v1","items":[]}`,
			wantAccess:  accessRead,
			wantPodsHit: true,
		},
		{
			name:        "non-Kubernetes plain-text 403 on GET pods (proxy ACL / VPC-SC) stays unreachable, never limited",
			withProxy:   true,
			ssarCode:    http.StatusMethodNotAllowed,
			ssarContent: "text/plain",
			ssarBody:    "POST refused",
			podsCode:    http.StatusForbidden,
			podsContent: "text/plain",
			podsBody:    "VPC Service Controls perimeter denied access",
			wantAccess:  accessUnreachable,
			wantPodsHit: true,
		},
		{
			name:        "without CREDENTIAL_PROXY_URL, VPC-SC / IAM 403 on SSAR stays unreachable without extra GET probe",
			withProxy:   false,
			ssarCode:    http.StatusForbidden,
			ssarContent: "application/json",
			ssarBody:    `{"error":{"code":403,"message":"VPC Service Controls"}}`,
			podsCode:    http.StatusForbidden,
			podsContent: "application/json",
			podsBody:    `{"error":{"code":403,"message":"VPC Service Controls"}}`,
			wantAccess:  accessUnreachable,
			wantPodsHit: false,
		},
		{
			name:        "405 on POST SSAR with 502 on GET pods classifies unreachable",
			withProxy:   true,
			ssarCode:    http.StatusMethodNotAllowed,
			ssarContent: "text/plain",
			ssarBody:    "method not allowed",
			podsCode:    http.StatusBadGateway,
			podsContent: "text/plain",
			podsBody:    "upstream down",
			wantAccess:  accessUnreachable,
			wantPodsHit: true,
		},
		{
			name:        "200 allowed SSAR does not probe GET pods",
			withProxy:   true,
			ssarCode:    http.StatusOK,
			ssarContent: "application/json",
			ssarBody:    `{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1","status":{"allowed":true}}`,
			podsCode:    http.StatusInternalServerError,
			podsContent: "application/json",
			wantAccess:  accessRead,
			wantPodsHit: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.withProxy {
				t.Setenv(cloud.CredentialProxyURLEnv, "http://127.0.0.1:8080")
			} else {
				t.Setenv(cloud.CredentialProxyURLEnv, "")
			}
			var podsHits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/selfsubjectaccessreviews"):
					w.Header().Set("Content-Type", tc.ssarContent)
					w.WriteHeader(tc.ssarCode)
					_, _ = io.WriteString(w, tc.ssarBody)
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/pods":
					podsHits.Add(1)
					if r.URL.Query().Get("limit") != "1" {
						t.Errorf("pods query = %q, want limit=1", r.URL.RawQuery)
					}
					w.Header().Set("Content-Type", tc.podsContent)
					w.WriteHeader(tc.podsCode)
					_, _ = io.WriteString(w, tc.podsBody)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			got := reviewAccess(t.Context(), &rest.Config{Host: srv.URL})
			if got != tc.wantAccess {
				t.Errorf("reviewAccess = %q, want %q", got, tc.wantAccess)
			}
			if (podsHits.Load() > 0) != tc.wantPodsHit {
				t.Errorf("podsHits = %d, want hit=%t", podsHits.Load(), tc.wantPodsHit)
			}
		})
	}
}

type fakeKubeconfigSource struct {
	fakeFleet
	ctxClusters map[string]string
}

func (f *fakeKubeconfigSource) ContextClusters() map[string]string {
	return f.ctxClusters
}

func TestUniformClusterNormalizationAcrossModes(t *testing.T) {
	// 1. Cloud / proxy discovery mode.
	cloudSrc := &fakeFleet{sequence: [][]cloud.ClusterRef{{
		gref("proj-a", "us-central1", "prod"),
		gref("proj-b", "us-east1", "stage"),
	}}}
	cloudDir, _ := newTestClusters(cloudSrc, false)
	for _, spelling := range []string{
		"prod",
		"proj-a/us-central1/prod",
		"projects/proj-a/locations/us-central1/clusters/prod",
		"gke_proj-a_us-central1_prod",
		"prod@proj-a",
		"prod@proj-a/us-central1",
	} {
		e, err := cloudDir.Resolve(t.Context(), "", spelling)
		if err != nil {
			t.Fatalf("cloud Resolve(%q): %v", spelling, err)
		}
		if e.canonical != "proj-a/us-central1/prod" || e.gkeID == nil || e.gkeID.Project != "proj-a" {
			t.Errorf("cloud Resolve(%q) = %+v (gkeID=%+v)", spelling, e, e.gkeID)
		}
	}

	// 2. Kubeconfig fleet mode (both standard gke_<p>_<l>_<c> context and renamed context).
	kubeSrc := &fakeKubeconfigSource{
		fakeFleet: fakeFleet{sequence: [][]cloud.ClusterRef{{
			{Name: "gke_proj-a_us-central1_prod", Endpoint: "https://prod-a.example:6443"},
			{Name: "friendly-stage", Endpoint: "https://stage-b.example:6443"},
		}}},
		ctxClusters: map[string]string{
			"gke_proj-a_us-central1_prod": "gke_proj-a_us-central1_prod",
			"friendly-stage":              "gke_proj-b_us-east1_stage",
		},
	}
	kubeDir, _ := newTestClusters(kubeSrc, false)
	reg := testRegistry(t)
	reg.Register(targetEchoCommand("triage echotarget", "k8s_echo_target", false))
	cs := connect(t, New(reg, "test", WithClusters(kubeDir)))

	for _, tc := range []struct {
		arg      string
		wantProj string
		wantLoc  string
		wantName string
	}{
		{"gke_proj-a_us-central1_prod", "proj-a", "us-central1", "prod"},
		{"projects/proj-a/locations/us-central1/clusters/prod", "proj-a", "us-central1", "prod"},
		{"proj-a/us-central1/prod", "proj-a", "us-central1", "prod"},
		{"prod", "proj-a", "us-central1", "prod"},
		{"friendly-stage", "proj-b", "us-east1", "stage"},
		{"projects/proj-b/locations/us-east1/clusters/stage", "proj-b", "us-east1", "stage"},
		{"gke_proj-b_us-east1_stage", "proj-b", "us-east1", "stage"},
		{"stage", "proj-b", "us-east1", "stage"},
	} {
		res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
			Name:      "k8s_echo_target",
			Arguments: map[string]any{"cluster": tc.arg},
		})
		if err != nil || res.IsError {
			t.Fatalf("CallTool(%q): err=%v res=%+v", tc.arg, err, res)
		}
		out := text(t, res)
		for _, want := range []string{
			"target_name=" + tc.wantName,
			"project=" + tc.wantProj,
			"location=" + tc.wantLoc,
		} {
			if !strings.Contains(out, want) {
				t.Errorf("CallTool(%q) output %q missing %q", tc.arg, out, want)
			}
		}
	}

	// Wrong project for an existing bare cluster name must NOT collide in kubeconfig mode.
	if _, err := kubeDir.Resolve(t.Context(), "", "projects/wrong-proj/locations/us-central1/clusters/prod"); err == nil {
		t.Error("kubeconfig Resolve matched wrong-proj against gke_proj-a_us-central1_prod")
	}

	// 3. Explicit --clusters mode with GKE resource paths.
	refs, err := fleet.ParseClusters("projects/proj-a/locations/us-central1/clusters/prod=https://prod.us-central1.gke.goog")
	if err != nil {
		t.Fatalf("ParseClusters: %v", err)
	}
	staticDir, _ := newTestClusters(&fakeFleet{sequence: [][]cloud.ClusterRef{refs}}, false)
	for _, spelling := range []string{
		"prod",
		"proj-a/us-central1/prod",
		"projects/proj-a/locations/us-central1/clusters/prod",
		"gke_proj-a_us-central1_prod",
	} {
		if _, err := staticDir.Resolve(t.Context(), "", spelling); err != nil {
			t.Errorf("static --clusters Resolve(%q): %v", spelling, err)
		}
	}
}

type fakeWidenableFleet struct {
	fakeFleet
	dynamic   map[cloud.ClusterID]cloud.ClusterRef
	transient map[cloud.ClusterID]error
	resolves  int
	resolveMu sync.Mutex
}

func (f *fakeWidenableFleet) ResolveCluster(_ context.Context, id cloud.ClusterID) (cloud.ClusterRef, error) {
	f.resolveMu.Lock()
	defer f.resolveMu.Unlock()
	f.resolves++
	if err, ok := f.transient[id]; ok && err != nil {
		return cloud.ClusterRef{}, err
	}
	if ref, ok := f.dynamic[id]; ok {
		return ref, nil
	}
	return cloud.ClusterRef{}, cloud.ErrClusterNotFound
}

func TestLazyWidenableScopePreservationRateLimitAndWorkloadGuard(t *testing.T) {
	src := &fakeWidenableFleet{
		fakeFleet: fakeFleet{sequence: [][]cloud.ClusterRef{{
			gref("proj-a", "us-central1", "prod"),
		}}},
		dynamic: map[cloud.ClusterID]cloud.ClusterRef{
			{Project: "proj-b", Location: "us-east1", Cluster: "stage"}: gref("proj-b", "us-east1", "stage"),
			{Project: "proj-c", Location: "us-west1", Cluster: "prod"}:  gref("proj-c", "us-west1", "prod"),
		},
	}
	dir, now := newTestClusters(src, false)

	// Out-of-scope fully qualified cluster resolves on demand without restarting.
	e, err := dir.Resolve(t.Context(), "", "projects/proj-b/locations/us-east1/clusters/stage")
	if err != nil {
		t.Fatalf("dynamic Resolve(stage): %v", err)
	}
	if e.canonical != "proj-b/us-east1/stage" {
		t.Errorf("canonical = %q, want proj-b/us-east1/stage", e.canonical)
	}
	if e.storeName != "" {
		t.Errorf("widened cluster storeName = %q, want empty (must never borrow local store files)", e.storeName)
	}

	// Widening a same-named cluster in another project must NOT steal or erase
	// the in-scope cluster's storeName.
	if _, err := dir.Resolve(t.Context(), "", "gke_proj-c_us-west1_prod"); err != nil {
		t.Fatalf("dynamic Resolve(proj-c prod): %v", err)
	}
	inScopeEntry, err := dir.Resolve(t.Context(), "", "proj-a/us-central1/prod")
	if err != nil {
		t.Fatalf("Resolve(in-scope prod): %v", err)
	}
	if inScopeEntry.storeName != "prod" {
		t.Errorf("in-scope prod storeName = %q, want prod", inScopeEntry.storeName)
	}

	// k8s_list_clusters / List includes dynamically widened clusters, and they
	// survive periodic rediscovery of the startup scope.
	*now = now.Add(2 * time.Hour)
	listed, _, err := dir.List(t.Context(), "", "")
	if err != nil {
		t.Fatalf("List after refresh: %v", err)
	}
	if len(listed) != 3 {
		t.Fatalf("List entries after refresh = %d, want 3 (1 in-scope + 2 widened)", len(listed))
	}

	// Negative cache / rate limit: repeated misses for the same unknown cluster
	// hit ResolveCluster only once per cooldown window.
	resolvesBefore := src.resolves
	for range 3 {
		if _, err := dir.Resolve(t.Context(), "", "projects/proj-missing/locations/us-central1/clusters/nope"); err == nil {
			t.Fatal("Resolve succeeded for missing cluster")
		}
	}
	if got := src.resolves - resolvesBefore; got != 1 {
		t.Errorf("ResolveCluster calls across 3 rapid misses = %d, want 1", got)
	}
	*now = now.Add(missRediscoverEvery + time.Second)
	_, _ = dir.Resolve(t.Context(), "", "projects/proj-missing/locations/us-central1/clusters/nope")
	if got := src.resolves - resolvesBefore; got != 2 {
		t.Errorf("ResolveCluster calls after cooldown = %d, want 2", got)
	}

	// Transient runtime errors (e.g., 503 / unreadable token file) are NOT
	// misclassified as errUnknownCluster and are NOT negative-cached, so an
	// immediate retry succeeds once the upstream recovers.
	flakyID := cloud.ClusterID{Project: "proj-flaky", Location: "us-central1", Cluster: "flaky"}
	src.dynamic[flakyID] = gref("proj-flaky", "us-central1", "flaky")
	src.transient = map[cloud.ClusterID]error{flakyID: errors.New("proxy HTTP 503")}
	if _, err := dir.Resolve(t.Context(), "", flakyID.ResourcePath()); err == nil || errors.Is(err, errUnknownCluster) {
		t.Fatalf("transient 503 Resolve err = %v, want runtime error (not errUnknownCluster)", err)
	}
	delete(src.transient, flakyID)
	if _, err := dir.Resolve(t.Context(), "", flakyID.ResourcePath()); err != nil {
		t.Fatalf("immediate retry after transient 503 failed (negative-cached?): %v", err)
	}

	// maxDynamicClusters (64) eviction bounds BOTH sets[""].entries and configs.
	evictSrc := &fakeWidenableFleet{
		fakeFleet: fakeFleet{sequence: [][]cloud.ClusterRef{{gref("proj-base", "us-central1", "base")}}},
		dynamic:   map[cloud.ClusterID]cloud.ClusterRef{},
	}
	evictDir := NewClusters(evictSrc, false, time.Hour)
	evictDir.access = func(context.Context, *rest.Config) string { return accessRead }
	for i := range maxDynamicClusters + 5 {
		cid := cloud.ClusterID{Project: "proj-dyn", Location: "us-central1", Cluster: "c-" + string(rune('a'+i/26)) + string(rune('a'+i%26))}
		evictSrc.dynamic[cid] = gref(cid.Project, cid.Location, cid.Cluster)
		if _, err := evictDir.Resolve(t.Context(), "", cid.ResourcePath()); err != nil {
			t.Fatalf("Resolve(%s): %v", cid.ResourcePath(), err)
		}
	}
	evictList, _, err := evictDir.List(t.Context(), "", "")
	if err != nil {
		t.Fatalf("evictDir.List: %v", err)
	}
	if want := 1 + maxDynamicClusters; len(evictList) != want {
		t.Errorf("len(evictList) = %d, want bounded at %d (1 in-scope + %d dynamic)", len(evictList), want, maxDynamicClusters)
	}
	evictDir.mu.Lock()
	cfgCount := len(evictDir.configs)
	evictDir.mu.Unlock()
	if cfgCount > 1+maxDynamicClusters {
		t.Errorf("len(evictDir.configs) = %d, want <= %d after eviction", cfgCount, 1+maxDynamicClusters)
	}

	// Workload rules over real mTLS listener:
	// - spiffe://example.test/ns/agents/sa/bot-a granted bare "prod" (in-scope) must NOT dynamically resolve
	//   projects/proj-evil/locations/us-east1/clusters/prod OR proj-d/us-east1/allowed-dyn.
	// - spiffe://example.test/ns/agents/sa/bot-b granted canonical GKE resource path projects/proj-d/.../allowed-dyn MAY resolve it.
	const (
		botAID    = "spiffe://example.test/ns/agents/sa/bot-a"
		botBID    = "spiffe://example.test/ns/agents/sa/bot-b"
		rulesYAML = `rules:
- name: allow-inscope-prod
  identities: ["` + botAID + `"]
  tools: ["all"]
  clusters: ["prod"]
- name: allow-dyn-d
  identities: ["` + botBID + `"]
  tools: ["all"]
  clusters: ["projects/proj-d/locations/us-east1/clusters/allowed-dyn"]
`
	)
	src.dynamic[cloud.ClusterID{Project: "proj-evil", Location: "us-east1", Cluster: "prod"}] = gref("proj-evil", "us-east1", "prod")
	src.dynamic[cloud.ClusterID{Project: "proj-d", Location: "us-east1", Cluster: "allowed-dyn"}] = gref("proj-d", "us-east1", "allowed-dyn")

	reg := testRegistry(t)
	reg.Register(targetEchoCommand("triage echotarget", "k8s_echo_target", false))
	ws := startWorkloadServer(t, rulesYAML, reg, WithClusters(dir))

	botACS := ws.mustConnectAs(t, botAID)
	resolvesBeforeWL := src.resolves
	for _, forbidden := range []string{
		"projects/proj-d/locations/us-east1/clusters/allowed-dyn",
		"projects/proj-evil/locations/us-east1/clusters/prod",
	} {
		res, err := botACS.CallTool(t.Context(), &mcp.CallToolParams{
			Name:      "k8s_echo_target",
			Arguments: map[string]any{"cluster": forbidden},
		})
		if err == nil && (res == nil || !res.IsError) {
			t.Errorf("bot-a was allowed to dynamically resolve unauthorized cluster %q", forbidden)
		}
	}
	if src.resolves != resolvesBeforeWL {
		t.Errorf("unauthorized workload triggered %d ResolveCluster calls, want 0", src.resolves-resolvesBeforeWL)
	}

	botBCS := ws.mustConnectAs(t, botBID)
	res, err := botBCS.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "k8s_echo_target",
		Arguments: map[string]any{"cluster": "projects/proj-d/locations/us-east1/clusters/allowed-dyn"},
	})
	if err != nil || res.IsError {
		t.Errorf("bot-b failed to resolve authorized GKE resource-path cluster: err=%v res=%+v", err, res)
	}
}

func TestStdioServeCancelsInflightToolWithoutClosingStdin(t *testing.T) {
	toolStarted := make(chan struct{})
	toolCtxCancelled := make(chan struct{})

	reg := checks.NewRegistry()
	reg.Register(checks.Command{
		Name:    "triage block",
		MCPName: "k8s_block",
		Summary: "Blocks until its context is cancelled; test scaffolding only.",
		Kinds:   []checks.KindField{checks.Kind("fake.block", "test", emit.SeverityInfo)},
		Run: func(ctx context.Context, _ emit.Invocation) (int, error) {
			close(toolStarted)
			<-ctx.Done()
			close(toolCtxCancelled)
			return 0, ctx.Err()
		},
	})

	// Simulate an open os.Stdin pipe whose writer is NEVER closed before shutdown.
	inR, inW := io.Pipe()
	defer func() { _ = inW.Close() }()
	outR, outW := io.Pipe()
	defer func() { _ = outW.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- Serve(ctx, New(reg, "test"), ServeOptions{Stdin: inR, Stdout: outW})
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	cs, err := client.Connect(t.Context(), &mcp.IOTransport{Reader: outR, Writer: inW}, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}

	go func() {
		_, _ = cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "k8s_block"})
	}()

	select {
	case <-toolStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for k8s_block tool to start")
	}

	// Cancel server context (simulating SIGTERM) while inW remains completely open.
	cancel()

	select {
	case <-toolCtxCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight stdio tool context was not cancelled on server shutdown")
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Errorf("Serve error = %v, want nil on clean context shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve hung waiting on open stdin reader")
	}
}
