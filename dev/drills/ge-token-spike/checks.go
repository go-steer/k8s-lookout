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

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-steer/steward/googletoken"
)

// cloudScope is the scope the spike needs the token to carry.
const cloudScope = "https://www.googleapis.com/auth/cloud-platform"

// Verdict is one check's outcome, phrased for issue #698's questions.
type Verdict string

const (
	// Accepted: the API authenticated the person (200, or 403: the
	// token was accepted but IAM/RBAC said no, which is still a yes to
	// "does this token work as the user").
	Accepted Verdict = "ACCEPTED"
	// Rejected: the API refused the token itself (401).
	Rejected Verdict = "REJECTED"
	// Unreachable: no HTTP answer (network, private endpoint). Says
	// nothing about the token.
	Unreachable Verdict = "UNREACHABLE"
	// Info: a fact, not a pass/fail.
	Info Verdict = "INFO"
	// Skipped: a prerequisite check failed.
	Skipped Verdict = "SKIPPED"
)

// Result is one line of the report.
type Result struct {
	Check   string  `json:"check"`
	Verdict Verdict `json:"verdict"`
	Detail  string  `json:"detail"`
}

// Target is the cluster the GKE checks read.
type Target struct {
	Project, Location, Cluster string
}

// Endpoints are the Container API's answer about the cluster, needed by
// the GKE checks.
type Endpoints struct {
	DNS string // control-plane DNS endpoint host
	IP  string // public IP endpoint, empty for a private-only cluster
	CA  *x509.CertPool
}

// Spike runs the #698 checks with one person's token.
type Spike struct {
	Target Target
	// Bases replace Google's API hosts (tests). Empty means the real ones.
	ContainerBase, ComputeBase, MonitoringBase, LoggingBase string
	// HTTP calls Google APIs; IP endpoint calls get their own client
	// trusting the cluster CA.
	HTTP *http.Client
	// Scheme is "https" for real GKE endpoints; tests use "http".
	Scheme string
	Now    func() time.Time
}

func (s *Spike) base(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

// Run returns the report. token is the caller's own access token; it is
// used for API calls and never appears in a Result.
func (s *Spike) Run(ctx context.Context, id googletoken.Identity, token string) []Result {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	sum := sha256.Sum256([]byte(token))
	out := []Result{
		{"identity", Info, fmt.Sprintf("email=%s hd=%q sub=%s", id.Email, id.HD, id.Subject)},
		{"token.fingerprint", Info, "sha256:" + hex.EncodeToString(sum[:4]) +
			" (changes when Gemini Enterprise sends a refreshed token; the token itself is never shown)"},
		{"token.lifetime", Info, fmt.Sprintf("expires %s, %s from now",
			id.Expiry.UTC().Format(time.RFC3339), id.Expiry.Sub(now()).Round(time.Second))},
		scopeResult(id.Scopes),
	}

	ep, r := s.cluster(ctx, token)
	out = append(out, r)
	if ep != nil {
		out = append(out, s.whoAmI(ctx, "gke.dns-endpoint.authn", ep.DNS, nil, token))
		out = append(out, s.canList(ctx, "gke.dns-endpoint.rbac", ep.DNS, nil, token))
		if ep.IP == "" {
			out = append(out, Result{"gke.ip-endpoint.authn", Skipped, "the cluster has no public IP endpoint"})
		} else {
			out = append(out, s.whoAmI(ctx, "gke.ip-endpoint.authn", ep.IP, ep.CA, token))
		}
	} else {
		for _, c := range []string{"gke.dns-endpoint.authn", "gke.dns-endpoint.rbac", "gke.ip-endpoint.authn"} {
			out = append(out, Result{c, Skipped, "the Container API did not describe the cluster"})
		}
	}

	p := s.Target.Project
	out = append(out,
		s.api(ctx, "cloud.compute", http.MethodGet,
			s.base(s.ComputeBase, "https://compute.googleapis.com")+"/compute/v1/projects/"+p+"/zones?maxResults=1", nil, token),
		s.api(ctx, "cloud.monitoring", http.MethodGet,
			s.base(s.MonitoringBase, "https://monitoring.googleapis.com")+"/v3/projects/"+p+"/metricDescriptors?pageSize=1", nil, token),
		s.api(ctx, "cloud.logging", http.MethodPost,
			s.base(s.LoggingBase, "https://logging.googleapis.com")+"/v2/entries:list",
			map[string]any{"resourceNames": []string{"projects/" + p}, "pageSize": 1}, token),
	)
	return out
}

func scopeResult(scopes []string) Result {
	if slices.Contains(scopes, cloudScope) {
		return Result{"token.scopes", Accepted, strings.Join(scopes, " ")}
	}
	return Result{"token.scopes", Rejected, "missing cloud-platform: " + strings.Join(scopes, " ")}
}

// cluster asks the Container API about the target, as the person.
func (s *Spike) cluster(ctx context.Context, token string) (*Endpoints, Result) {
	t := s.Target
	u := fmt.Sprintf("%s/v1/projects/%s/locations/%s/clusters/%s",
		s.base(s.ContainerBase, "https://container.googleapis.com"), t.Project, t.Location, t.Cluster)
	var cl struct {
		Endpoint   string `json:"endpoint"`
		MasterAuth struct {
			ClusterCACertificate string `json:"clusterCaCertificate"`
		} `json:"masterAuth"`
		ControlPlaneEndpointsConfig struct {
			DNSEndpointConfig struct {
				Endpoint string `json:"endpoint"`
			} `json:"dnsEndpointConfig"`
			IPEndpointsConfig struct {
				PublicEndpoint string `json:"publicEndpoint"`
			} `json:"ipEndpointsConfig"`
		} `json:"controlPlaneEndpointsConfig"`
	}
	r := s.call(ctx, "cloud.container.clusters.get", s.HTTP, http.MethodGet, u, nil, token, &cl)
	if r.Verdict != Accepted || cl.ControlPlaneEndpointsConfig.DNSEndpointConfig.Endpoint == "" {
		if r.Verdict == Accepted {
			r.Detail += "; but no DNS endpoint is enabled on the cluster"
		}
		return nil, r
	}
	ep := &Endpoints{DNS: cl.ControlPlaneEndpointsConfig.DNSEndpointConfig.Endpoint}
	ep.IP = cl.ControlPlaneEndpointsConfig.IPEndpointsConfig.PublicEndpoint
	if ep.IP == "" {
		ep.IP = cl.Endpoint
	}
	if pem, err := base64.StdEncoding.DecodeString(cl.MasterAuth.ClusterCACertificate); err == nil {
		ep.CA = x509.NewCertPool()
		ep.CA.AppendCertsFromPEM(pem)
	}
	r.Detail += fmt.Sprintf("; dns=%s ip=%s", ep.DNS, ep.IP)
	return ep, r
}

// whoAmI asks the API server who the token belongs to
// (SelfSubjectReview): the direct answer to "does GKE authenticate this
// token as the person?".
func (s *Spike) whoAmI(ctx context.Context, name, host string, ca *x509.CertPool, token string) Result {
	var rev struct {
		Status struct {
			UserInfo struct {
				Username string   `json:"username"`
				Groups   []string `json:"groups"`
			} `json:"userInfo"`
		} `json:"status"`
	}
	body := map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview"}
	r := s.call(ctx, name, s.clientFor(ca), http.MethodPost,
		s.scheme()+"://"+host+"/apis/authentication.k8s.io/v1/selfsubjectreviews", body, token, &rev)
	if r.Verdict == Accepted && rev.Status.UserInfo.Username != "" {
		r.Detail = fmt.Sprintf("the API server sees username=%s groups=%v", rev.Status.UserInfo.Username, rev.Status.UserInfo.Groups)
	}
	return r
}

// canList asks whether the person may list pods cluster-wide
// (SelfSubjectAccessReview): shows RBAC is the person's, not the
// service's.
func (s *Spike) canList(ctx context.Context, name, host string, ca *x509.CertPool, token string) Result {
	var rev struct {
		Status struct {
			Allowed bool   `json:"allowed"`
			Reason  string `json:"reason"`
		} `json:"status"`
	}
	body := map[string]any{
		"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
		"spec": map[string]any{"resourceAttributes": map[string]any{"verb": "list", "resource": "pods"}},
	}
	r := s.call(ctx, name, s.clientFor(ca), http.MethodPost,
		s.scheme()+"://"+host+"/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", body, token, &rev)
	if r.Verdict == Accepted {
		r.Detail = fmt.Sprintf("list pods cluster-wide as this person: allowed=%v %s", rev.Status.Allowed, rev.Status.Reason)
	}
	return r
}

func (s *Spike) api(ctx context.Context, name, method, u string, body any, token string) Result {
	return s.call(ctx, name, s.HTTP, method, u, body, token, nil)
}

func (s *Spike) scheme() string {
	if s.Scheme != "" {
		return s.Scheme
	}
	return "https"
}

func (s *Spike) clientFor(ca *x509.CertPool) *http.Client {
	if ca == nil {
		return s.HTTP
	}
	return &http.Client{Timeout: s.HTTP.Timeout, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca, MinVersion: tls.VersionTLS12}}}
}

// call makes one request with the person's token and classifies the
// answer. Neither the token nor a response body beyond a short excerpt of
// an error ever lands in the Result.
func (s *Spike) call(ctx context.Context, name string, hc *http.Client, method, u string, body any, token string, out any) Result {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return Result{name, Unreachable, err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := hc.Do(req)
	if err != nil {
		return Result{name, Unreachable, err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return Result{name, Rejected, fmt.Sprintf("HTTP 401: %s", excerpt(b))}
	case resp.StatusCode == http.StatusForbidden:
		return Result{name, Accepted, fmt.Sprintf("HTTP 403 (token accepted, permission denied): %s", excerpt(b))}
	case resp.StatusCode/100 == 2:
		if out != nil {
			_ = json.Unmarshal(b, out)
		}
		return Result{name, Accepted, fmt.Sprintf("HTTP %d", resp.StatusCode)}
	default:
		return Result{name, Unreachable, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, excerpt(b))}
	}
}

func excerpt(b []byte) string {
	s := strings.Join(strings.Fields(string(b)), " ")
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

// Format renders the report as a fixed-width table for the assistant to
// show verbatim.
func Format(rs []Result) string {
	var b strings.Builder
	for _, r := range rs {
		fmt.Fprintf(&b, "%-30s %-12s %s\n", r.Check, r.Verdict, r.Detail)
	}
	return b.String()
}
