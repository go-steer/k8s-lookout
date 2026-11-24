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

package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

const (
	proxyDiscoveryTimeout = 15 * time.Second
	maxProxyDiscoveryBody = 4 << 20 // 4 MiB
)

// credentialProxySource resolves GKE clusters and their Kubernetes client
// configs through CREDENTIAL_PROXY_URL (+ optional CREDENTIAL_PROXY_TOKEN_FILE)
// without requiring a kubeconfig, kubectl, or GCE metadata server (#858).
//
// Google Container API discovery routes through:
//
//	GET <CREDENTIAL_PROXY_URL>/v1/gcp/container.googleapis.com/v1/projects/<p>/locations/<l>/clusters
//
// Per-cluster Kubernetes API traffic routes through:
//
//	<CREDENTIAL_PROXY_URL>/v1/k8s/<p>/<l>/<c>
type credentialProxySource struct {
	proxy  *cloud.CredentialProxy
	scoped cloud.ScopedFleet // optional: handles fleet:/folders//organizations/ under -tags gke
	scopes []cloud.Scope
	static []cloud.ClusterRef
	auto   bool
}

var _ SingleClusterSource = (*credentialProxySource)(nil)

func (s *credentialProxySource) Describe() string {
	return "credential proxy " + s.proxy.URL
}

func (s *credentialProxySource) HTTPClient() *http.Client {
	return &http.Client{
		Timeout:       proxyDiscoveryTimeout,
		Transport:     s.proxy.WrapRoundTripper(http.DefaultTransport),
		CheckRedirect: s.proxy.CheckRedirect,
	}
}

func (s *credentialProxySource) RESTConfig(_ context.Context, ref cloud.ClusterRef) (*rest.Config, error) {
	if ref.Project != "" && ref.Location != "" && ref.Name != "" {
		host, err := s.proxy.K8sHost(cloud.ClusterID{
			Project:  ref.Project,
			Location: ref.Location,
			Cluster:  ref.Name,
		})
		if err != nil {
			return nil, err
		}
		cfg := &rest.Config{Host: host}
		if s.proxy.TokenFile != "" {
			cfg.Wrap(s.proxy.WrapRoundTripper)
		}
		return cfg, nil
	}
	if id, ok := cloud.ParseClusterRef(ref.Name); ok {
		host, err := s.proxy.K8sHost(id)
		if err != nil {
			return nil, err
		}
		cfg := &rest.Config{Host: host}
		if s.proxy.TokenFile != "" {
			cfg.Wrap(s.proxy.WrapRoundTripper)
		}
		return cfg, nil
	}
	if strings.HasPrefix(ref.Endpoint, "http://") || strings.HasPrefix(ref.Endpoint, "https://") {
		cfg := &rest.Config{Host: ref.Endpoint}
		if s.proxy.TokenFile != "" {
			cfg.Wrap(s.proxy.WrapRoundTripper)
		}
		return cfg, nil
	}
	return nil, fmt.Errorf("cluster %q: CREDENTIAL_PROXY_URL requires project/location/cluster identity to route /v1/k8s/<project>/<location>/<cluster>", ref.Name)
}

func (s *credentialProxySource) Discover(ctx context.Context) (Result, error) {
	if len(s.static) > 0 {
		return Result{Clusters: append([]cloud.ClusterRef(nil), s.static...)}, nil
	}
	if len(s.scopes) == 0 {
		return Result{}, nil
	}
	var (
		res    Result
		seen   = map[string]bool{}
		errs   []error
		failed int
	)
	for _, scope := range s.scopes {
		disc, err := s.discoverScope(ctx, scope)
		sr := ScopeResult{Scope: scope.String(), Via: disc.Via, Fallback: disc.Fallback, Skipped: disc.Skipped}
		if err != nil {
			if ctx.Err() != nil {
				return Result{}, ctx.Err()
			}
			failed++
			sr.Err = err
			errs = append(errs, fmt.Errorf("%s: %w", scope, err))
			res.Scopes = append(res.Scopes, sr)
			continue
		}
		for _, ref := range disc.Clusters {
			sr.Clusters++
			k := ref.Project + "/" + ref.Location + "/" + ref.Name
			if seen[k] {
				sr.Duplicates++
				continue
			}
			seen[k] = true
			res.Clusters = append(res.Clusters, ref)
		}
		res.Scopes = append(res.Scopes, sr)
	}
	if failed == len(s.scopes) {
		if s.auto {
			return res, nil
		}
		return res, errors.Join(errs...)
	}
	return res, nil
}

func (s *credentialProxySource) discoverScope(ctx context.Context, scope cloud.Scope) (cloud.Discovery, error) {
	if scope.Kind == cloud.ScopeProject {
		refs, err := s.discoverProjectViaProxy(ctx, scope.Project, scope.Location)
		return cloud.Discovery{Clusters: refs, Via: "credential-proxy"}, err
	}
	if s.scoped != nil {
		return s.scoped.Discover(ctx, scope)
	}
	return cloud.Discovery{}, fmt.Errorf("scope %q via CREDENTIAL_PROXY_URL requires building lookout with -tags gke (project scopes work on every build)", scope)
}

type proxyClusterRecord struct {
	Name                        string `json:"name"`
	Location                    string `json:"location"`
	ControlPlaneEndpointsConfig *struct {
		DNSEndpointConfig *struct {
			Endpoint string `json:"endpoint"`
		} `json:"dnsEndpointConfig"`
	} `json:"controlPlaneEndpointsConfig"`
}

func (r proxyClusterRecord) dnsEndpoint() string {
	if r.ControlPlaneEndpointsConfig != nil && r.ControlPlaneEndpointsConfig.DNSEndpointConfig != nil {
		return r.ControlPlaneEndpointsConfig.DNSEndpointConfig.Endpoint
	}
	return ""
}

func (s *credentialProxySource) discoverProjectViaProxy(ctx context.Context, project, location string) ([]cloud.ClusterRef, error) {
	loc := location
	if loc == "" {
		loc = "-"
	}
	if !cloud.ValidClusterID(project, "loc", "clus") || (loc != "-" && !cloud.ValidClusterID("proj", loc, "clus")) {
		return nil, fmt.Errorf("invalid project/location %q/%q", project, loc)
	}
	endpoint := fmt.Sprintf("%s/v1/gcp/container.googleapis.com/v1/projects/%s/locations/%s/clusters",
		s.proxy.URL, url.PathEscape(project), url.PathEscape(loc))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil) //nolint:gosec // operator-configured CREDENTIAL_PROXY_URL
	if err != nil {
		return nil, err
	}
	resp, err := s.HTTPClient().Do(req) //nolint:gosec // operator-configured CREDENTIAL_PROXY_URL
	if err != nil {
		return nil, fmt.Errorf("discover clusters via %s: %w", cloud.CredentialProxyURLEnv, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProxyDiscoveryBody))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discover clusters via %s: HTTP %d", cloud.CredentialProxyURLEnv, resp.StatusCode)
	}
	var parsed struct {
		Clusters []proxyClusterRecord `json:"clusters"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("parse cluster list from %s: %w", cloud.CredentialProxyURLEnv, err)
	}
	out := make([]cloud.ClusterRef, 0, len(parsed.Clusters))
	for _, c := range parsed.Clusters {
		cloc := c.Location
		if cloc == "" && loc != "-" {
			cloc = loc
		}
		if !cloud.ValidClusterID(project, cloc, c.Name) {
			continue
		}
		out = append(out, cloud.ClusterRef{
			Name:     c.Name,
			Project:  project,
			Location: cloc,
			Region:   gkeLocationRegion(cloc),
			Endpoint: c.dnsEndpoint(),
		})
	}
	return out, nil
}

// ResolveCluster resolves one fully qualified GKE cluster on demand through
// CREDENTIAL_PROXY_URL when a caller names a cluster outside the startup scope.
func (s *credentialProxySource) ResolveCluster(ctx context.Context, id cloud.ClusterID) (cloud.ClusterRef, error) {
	if len(s.static) > 0 {
		return cloud.ClusterRef{}, errors.New("static --clusters list cannot be widened dynamically")
	}
	if !cloud.ValidClusterID(id.Project, id.Location, id.Cluster) {
		return cloud.ClusterRef{}, fmt.Errorf("invalid GKE cluster reference %q", id.Triple())
	}
	client := s.HTTPClient()

	// 1. Try single-cluster Container API GET via /v1/gcp/.
	getURL := fmt.Sprintf("%s/v1/gcp/container.googleapis.com/v1/projects/%s/locations/%s/clusters/%s",
		s.proxy.URL, url.PathEscape(id.Project), url.PathEscape(id.Location), url.PathEscape(id.Cluster))
	if req, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil); err == nil { //nolint:gosec // operator-configured CREDENTIAL_PROXY_URL
		resp, err := client.Do(req) //nolint:gosec // operator-configured CREDENTIAL_PROXY_URL
		if err != nil {
			if ctx.Err() != nil || strings.Contains(err.Error(), cloud.CredentialProxyTokenFileEnv) {
				return cloud.ClusterRef{}, err
			}
		} else {
			body, rerr := io.ReadAll(io.LimitReader(resp.Body, maxProxyDiscoveryBody))
			_ = resp.Body.Close()
			if rerr == nil {
				switch {
				case resp.StatusCode == http.StatusOK:
					var rec proxyClusterRecord
					if json.Unmarshal(body, &rec) == nil {
						loc := rec.Location
						if loc == "" {
							loc = id.Location
						}
						name := rec.Name
						if name == "" {
							name = id.Cluster
						}
						return cloud.ClusterRef{
							Name:     name,
							Project:  id.Project,
							Location: loc,
							Region:   gkeLocationRegion(loc),
							Endpoint: rec.dnsEndpoint(),
						}, nil
					}
				case resp.StatusCode == http.StatusNotFound && isGoogleAPINotFound(body):
					// Authoritative Container API 404 (not a proxy route-disabled 404).
					return cloud.ClusterRef{}, fmt.Errorf("cluster %s: %w", id.Triple(), cloud.ErrClusterNotFound)
				case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
					return cloud.ClusterRef{}, fmt.Errorf("looking up cluster %s via %s: HTTP %d", id.Triple(), cloud.CredentialProxyURLEnv, resp.StatusCode)
				}
			}
		}
	}

	// 2. Fall back to location-scoped clusters.list (for proxies that only serve
	// the collection endpoint).
	refs, listErr := s.discoverProjectViaProxy(ctx, id.Project, id.Location)
	if listErr == nil {
		for _, r := range refs {
			if r.Name == id.Cluster {
				return r, nil
			}
		}
		return cloud.ClusterRef{}, fmt.Errorf("cluster %q under %s/%s: %w", id.Cluster, id.Project, id.Location, cloud.ErrClusterNotFound)
	}
	if ctx.Err() != nil || strings.Contains(listErr.Error(), cloud.CredentialProxyTokenFileEnv) {
		return cloud.ClusterRef{}, listErr
	}

	// 3. Fall back to a read-only pod-list probe on /v1/k8s/<p>/<l>/<c> for
	// proxies that grant Kubernetes cluster reads without Container API access.
	// Only genuine Kubernetes API responses (PodList on 200, or a structured
	// Kubernetes Status on 403 RBAC refusal) qualify; plain-text or Google RPC
	// 403/5xx responses from proxy ACLs or perimeters never fabricate a cluster.
	k8sHost, err := s.proxy.K8sHost(id)
	if err != nil {
		return cloud.ClusterRef{}, err
	}
	probeReq, err := http.NewRequestWithContext(ctx, http.MethodGet, k8sHost+"/api/v1/pods?limit=1", nil) //nolint:gosec // operator-configured CREDENTIAL_PROXY_URL
	if err != nil {
		return cloud.ClusterRef{}, err
	}
	probeResp, err := client.Do(probeReq) //nolint:gosec // operator-configured CREDENTIAL_PROXY_URL
	if err != nil {
		return cloud.ClusterRef{}, fmt.Errorf("probing cluster %s via %s: %w", id.Triple(), cloud.CredentialProxyURLEnv, err)
	}
	probeBody, rerr := io.ReadAll(io.LimitReader(probeResp.Body, 64*1024))
	_ = probeResp.Body.Close()
	if rerr != nil {
		return cloud.ClusterRef{}, fmt.Errorf("probing cluster %s via %s: %w", id.Triple(), cloud.CredentialProxyURLEnv, rerr)
	}
	switch {
	case probeResp.StatusCode == http.StatusOK && isKubernetesEnvelope(probeBody, "PodList"):
		return cloud.ClusterRef{
			Name:     id.Cluster,
			Project:  id.Project,
			Location: id.Location,
			Region:   gkeLocationRegion(id.Location),
		}, nil
	case probeResp.StatusCode == http.StatusForbidden && isKubernetesEnvelope(probeBody, "Status"):
		return cloud.ClusterRef{
			Name:     id.Cluster,
			Project:  id.Project,
			Location: id.Location,
			Region:   gkeLocationRegion(id.Location),
		}, nil
	case probeResp.StatusCode == http.StatusNotFound:
		return cloud.ClusterRef{}, fmt.Errorf("cluster %s: %w", id.Triple(), cloud.ErrClusterNotFound)
	default:
		return cloud.ClusterRef{}, fmt.Errorf("cluster %s is not reachable via %s (HTTP %d)", id.Triple(), cloud.CredentialProxyURLEnv, probeResp.StatusCode)
	}
}

func isGoogleAPINotFound(body []byte) bool {
	var env struct {
		Error *struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	return json.Unmarshal(body, &env) == nil && env.Error != nil &&
		(env.Error.Code == http.StatusNotFound || env.Error.Status == "NOT_FOUND")
}

func isKubernetesEnvelope(body []byte, wantKind string) bool {
	var env struct {
		Kind       string `json:"kind"`
		APIVersion string `json:"apiVersion"`
	}
	return json.Unmarshal(body, &env) == nil && env.Kind == wantKind && env.APIVersion != ""
}

// gkeLocationRegion strips a GKE single-letter zone suffix ("us-central1-a" ->
// "us-central1"), leaving regional locations ("us-central1") untouched.
func gkeLocationRegion(location string) string {
	if i := strings.LastIndexByte(location, '-'); i > 0 && i == len(location)-2 {
		c := location[i+1]
		if c >= 'a' && c <= 'z' {
			return location[:i]
		}
	}
	return location
}
