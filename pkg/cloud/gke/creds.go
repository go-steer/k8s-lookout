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

// Token-acquisition classification (#614): the one place a refused
// token becomes the provider-neutral cloud.ErrCredentialsUnavailable.
// Every capability the Provider hands out is wrapped here, so no
// capability implementation — REST or gRPC, present or future — has
// to remember to classify at its call sites.
//
// Fleet discovery (fleet.go: DiscoverClusters/RESTConfig through
// cloud.Fleet) is deliberately NOT wrapped: it is not a capability,
// it runs once at `lookout watch` startup, and with no token there is
// no cluster to watch — failing startup loudly is the right answer,
// not a degraded record.

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// credentialsFix is the remedy the CredentialsUnavailable record
// names. Static text: it is rendered on stdout, so nothing from the
// error may flow into it.
const credentialsFix = "grant lookout's Kubernetes service account roles/iam.workloadIdentityUser on the Google service account its iam.gke.io/gcp-service-account annotation names; with Workload Identity off, check the node's service account and its access scopes"

// grpcCredsFailedPrefix is how grpc-go reports a per-RPC credentials
// failure: status.Errorf(codes.Unauthenticated, "transport: per-RPC
// creds failed due to error: %v", err) in internal/transport — the
// gRPC clients (Cloud Quotas, Logging, Pub/Sub) flatten the token
// source's typed error into that status text, so the chain the REST
// clients keep is gone. The one string match here, held narrow: the
// Unauthenticated code AND grpc-go's own fixed prefix, never the
// server prose after it.
//
// The prefix alone is not enough: grpc-go puts EVERY token-fetch
// failure behind it — a metadata 5xx, a dial error, a deadline — and
// those are outages, not a missing grant. So the text after the
// prefix must also carry a token endpoint's 4xx as the libraries
// render it (grpcTokenStatus), or a grant-refusal OAuth2 error code
// (grpcOAuthClientCode).
const grpcCredsFailedPrefix = "transport: per-RPC creds failed due to error: " //nolint:gosec // G101: gRPC error prefix string, not a credential

// grpcTokenStatus finds the token endpoint's HTTP status in a
// flattened token error, as each library renders it:
//
//	metadata: "compute: Received 403 `…`"
//	auth:     "auth: cannot fetch token: 403\nResponse: …"
//	oauth2:   "oauth2: cannot fetch token: 403 Forbidden\nResponse: …"
var grpcTokenStatus = regexp.MustCompile(`(?:compute: Received|auth: cannot fetch token:|oauth2: cannot fetch token:) (\d{3})\b`)

// grpcOAuthClientCode finds the OAuth2 error-code rendering of
// auth.Error and oauth2.RetrieveError (`auth: "invalid_grant" …`),
// held to the RFC 6749 §5.2 codes that mean the grant itself was
// refused — never temporarily_unavailable or server_error.
var grpcOAuthClientCode = regexp.MustCompile(`(?:auth|oauth2): "(?:invalid_request|invalid_client|invalid_grant|unauthorized_client|unsupported_grant_type|invalid_scope|access_denied)"`)

// classifyCredentials wraps err into a *cloud.CredentialsUnavailableError
// when it is a refused token; any other error (nil included) is
// returned unchanged. Recognised, typed first:
//
//   - *metadata.Error with a 4xx status — the (GKE) metadata server
//     refusing to mint a token, e.g. the 403 "Unable to generate
//     access token; IAM returned 403" of a missing
//     roles/iam.workloadIdentityUser binding. It reaches REST callers
//     wrapped by cloud.google.com/go/auth/credentials and *url.Error,
//     both of which unwrap;
//   - *oauth2.RetrieveError — a token endpoint answering with an
//     OAuth2 error (golang.org/x/oauth2 token sources);
//   - *auth.Error — the same from cloud.google.com/go/auth token
//     providers (and oauth2adapt's translation of a RetrieveError);
//   - a gRPC Unauthenticated status carrying grpc-go's per-RPC
//     credentials prefix AND a token-endpoint refusal in the text
//     after it (see grpcCredsFailedPrefix).
//
// A 5xx from a token endpoint, a 408 or a 429 (both retryable, as
// x/oauth2's google.AuthenticationError.Temporary() has them), a dial
// error or a deadline stays a runtime error: a transient outage is
// not a missing grant, and degrading it would name a fix that does
// not apply.
func classifyCredentials(err error) error {
	if err == nil || errors.Is(err, cloud.ErrCredentialsUnavailable) {
		return err
	}
	if tokenRefused(err) {
		return &cloud.CredentialsUnavailableError{Fix: credentialsFix, Err: err}
	}
	return err
}

func tokenRefused(err error) bool {
	var mdErr *metadata.Error
	if errors.As(err, &mdErr) {
		return clientError(mdErr.Code)
	}
	var rErr *oauth2.RetrieveError
	if errors.As(err, &rErr) {
		return rErr.Response == nil || clientError(rErr.Response.StatusCode)
	}
	var aErr *auth.Error
	if errors.As(err, &aErr) {
		return aErr.Response == nil || clientError(aErr.Response.StatusCode)
	}
	// The status itself, not status.FromError: on a wrapped error
	// FromError rewrites the message to the whole err.Error(), which
	// would put the caller's wrap text in front of grpc-go's prefix.
	var gs interface{ GRPCStatus() *status.Status }
	if errors.As(err, &gs) {
		if st := gs.GRPCStatus(); st != nil && st.Code() == codes.Unauthenticated {
			cause, ok := strings.CutPrefix(st.Message(), grpcCredsFailedPrefix)
			if !ok {
				return false
			}
			if m := grpcTokenStatus.FindStringSubmatch(cause); m != nil {
				code, _ := strconv.Atoi(m[1])
				return clientError(code)
			}
			return grpcOAuthClientCode.MatchString(cause)
		}
	}
	return false
}

// clientError: a 4xx from a token endpoint is a refusal, except 408
// and 429 — a timeout and a rate limit are worth retrying, not a
// grant to fix.
func clientError(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	return code >= http.StatusBadRequest && code < http.StatusInternalServerError
}

// classify is what every decorator returns: no classification once
// the caller's context has ended. A deadline or cancellation that
// interrupted the token fetch is the --timeout contract's runtime
// error, never a missing grant.
func classify(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return err
	}
	return classifyCredentials(err)
}

// The capability decorators: each forwards to the implementation and
// classifies the error. Explicit fields, not embedding, so a method
// added to a capability interface fails to compile here instead of
// passing through unclassified.

type credsMetrics struct{ inner cloud.MetricsBackend }

func (c credsMetrics) QuerySeries(ctx context.Context, q cloud.SeriesQuery) ([]cloud.Series, error) {
	s, err := c.inner.QuerySeries(ctx, q)
	return s, classify(ctx, err)
}

type credsCapacity struct{ inner cloud.CapacityAPI }

func (c credsCapacity) ScaleDecisions(ctx context.Context, w cloud.TimeWindow) ([]cloud.ScaleDecision, error) {
	d, err := c.inner.ScaleDecisions(ctx, w)
	return d, classify(ctx, err)
}

type credsQuota struct{ inner cloud.QuotaAPI }

func (c credsQuota) Quotas(ctx context.Context) ([]cloud.QuotaUsage, error) {
	q, err := c.inner.Quotas(ctx)
	return q, classify(ctx, err)
}

func (c credsQuota) History(ctx context.Context, name, scope string, w cloud.TimeWindow) (cloud.QuotaHistory, error) {
	h, err := c.inner.History(ctx, name, scope, w)
	return h, classify(ctx, err)
}

type credsOrphans struct{ inner cloud.OrphanAPI }

func (c credsOrphans) OrphanDisks(ctx context.Context) ([]cloud.OrphanDisk, error) {
	d, err := c.inner.OrphanDisks(ctx)
	return d, classify(ctx, err)
}

func (c credsOrphans) OrphanLoadBalancers(ctx context.Context) (cloud.LoadBalancerSweep, error) {
	l, err := c.inner.OrphanLoadBalancers(ctx)
	return l, classify(ctx, err)
}

func (c credsOrphans) OrphanAddresses(ctx context.Context) ([]cloud.OrphanAddress, error) {
	a, err := c.inner.OrphanAddresses(ctx)
	return a, classify(ctx, err)
}

type credsIPSpace struct{ inner cloud.IPSpaceAPI }

func (c credsIPSpace) SubnetUtilization(ctx context.Context) ([]cloud.SubnetUtilization, error) {
	u, err := c.inner.SubnetUtilization(ctx)
	return u, classify(ctx, err)
}

type credsStockout struct{ inner cloud.StockoutAPI }

func (c credsStockout) Stockouts(ctx context.Context, w cloud.TimeWindow) ([]cloud.Stockout, error) {
	s, err := c.inner.Stockouts(ctx, w)
	return s, classify(ctx, err)
}

type credsWI struct{ inner cloud.WorkloadIdentityAPI }

func (c credsWI) VerifyBinding(ctx context.Context, namespace, serviceAccount, cloudIdentity string) (cloud.WIBinding, error) {
	b, err := c.inner.VerifyBinding(ctx, namespace, serviceAccount, cloudIdentity)
	return b, classify(ctx, err)
}

type credsAudit struct{ inner cloud.AuditAPI }

func (c credsAudit) ObjectWrites(ctx context.Context, ref cloud.AuditRef, w cloud.TimeWindow) ([]cloud.ObjectWrite, error) {
	o, err := c.inner.ObjectWrites(ctx, ref, w)
	return o, classify(ctx, err)
}

type credsNotifications struct{ inner cloud.NotificationsAPI }

func (c credsNotifications) Receive(ctx context.Context, handle func(cloud.ClusterNotification)) error {
	return classify(ctx, c.inner.Receive(ctx, handle))
}

type credsClusterConfig struct{ inner cloud.ClusterConfigAPI }

func (c credsClusterConfig) Config(ctx context.Context) (cloud.ClusterConfig, error) {
	cfg, err := c.inner.Config(ctx)
	return cfg, classify(ctx, err)
}

func (c credsClusterConfig) UpgradeTargets(ctx context.Context, channel string) (cloud.UpgradeTargets, error) {
	t, err := c.inner.UpgradeTargets(ctx, channel)
	return t, classify(ctx, err)
}
