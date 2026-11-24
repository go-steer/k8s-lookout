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

	"golang.org/x/oauth2"
	"google.golang.org/api/option"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

type grpcClientKey struct{}

func withGRPCClient(ctx context.Context) context.Context {
	return context.WithValue(ctx, grpcClientKey{}, true)
}

func isGRPCClient(ctx context.Context) bool {
	v, _ := ctx.Value(grpcClientKey{}).(bool)
	return v
}

// grpcCredentialProxyErr fails fast before dialing a gRPC Google API endpoint
// when CREDENTIAL_PROXY_URL is active without a per-caller token, avoiding a
// hang on direct TCP egress in isolated task sandboxes.
func grpcCredentialProxyErr(ctx context.Context) error {
	if _, ok := kube.CallerTokenFrom(ctx); ok {
		return nil
	}
	if !cloud.HasCredentialProxy() {
		return nil
	}
	return &cloud.CredentialsUnavailableError{
		Fix: "CREDENTIAL_PROXY_URL proxies Google HTTP REST APIs (/v1/gcp/); gRPC Google APIs are unavailable through it",
		Err: errors.New("gRPC Google API is not supported via CREDENTIAL_PROXY_URL"),
	}
}

type failingTokenSource struct{ err error }

func (f failingTokenSource) Token() (*oauth2.Token, error) { return nil, f.err }

// clientOptions are the options every Google API client in this package
// is built with. When the invocation runs as its caller (#699,
// kube.WithCallerToken), that is the caller's token, so Compute,
// Container, Monitoring, Logging, IAM, Cloud Quotas and Pub/Sub answer
// with what the caller's IAM allows. When CREDENTIAL_PROXY_URL is set,
// Google REST APIs route through <CREDENTIAL_PROXY_URL>/v1/gcp/<host>/
// (#858). When injected Google auth is active, outbound calls carry the
// static placeholder bearer token for Substrate's egress gateway (#858).
// Otherwise it is nothing, and the client libraries use Application
// Default Credentials as before.
//
// Every client construction must pass it: a client built without it would
// read as lookout's own identity on a caller's behalf, which is exactly
// what per-caller identity exists to prevent. TestEveryClientUsesClientOptions
// fails the build for a construction site that does not.
//
// Clients are built lazily per Provider, and a Provider per invocation
// (cloud.New in each check), so a caller's token never outlives the call
// that brought it.
func clientOptions(ctx context.Context) []option.ClientOption {
	if ts, ok := kube.CallerTokenFrom(ctx); ok {
		return []option.ClientOption{option.WithTokenSource(ts)}
	}
	if cloud.HasCredentialProxy() {
		proxy, err := cloud.CredentialProxyFromEnv()
		if err != nil {
			return []option.ClientOption{option.WithTokenSource(failingTokenSource{err: err})}
		}
		if proxy != nil {
			if isGRPCClient(ctx) {
				return []option.ClientOption{option.WithTokenSource(failingTokenSource{
					err: &cloud.CredentialsUnavailableError{
						Fix: "CREDENTIAL_PROXY_URL proxies Google HTTP REST APIs (/v1/gcp/); gRPC Google APIs are unavailable through it",
						Err: errors.New("gRPC Google API is not supported via CREDENTIAL_PROXY_URL"),
					},
				})}
			}
			return []option.ClientOption{
				option.WithHTTPClient(&http.Client{
					Transport:     proxy.GCPTransport(nil),
					CheckRedirect: proxy.CheckRedirect,
				}),
				option.WithoutAuthentication(),
			}
		}
	}
	if cloud.IsInjectedGoogleAuth(ctx) {
		return []option.ClientOption{option.WithTokenSource(cloud.InjectedTokenSource())}
	}
	return nil
}
