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

package kube

import (
	"context"

	"golang.org/x/oauth2"
)

// A read-path invocation can run as the person who asked for it rather
// than as lookout (#699): an MCP caller authenticated with their own
// Google access token, whose reads must be what their RBAC and IAM allow
// and nothing more. The caller's token travels in the invocation's
// context, beside the cluster Selection and independent of it:
//
//   - The Selection says which cluster; the caller token says who reads
//     it. A single-cluster endpoint reads lookout's own cluster as the
//     caller, so the token must not turn into a selection (which would,
//     after #694, also switch off the metadata server's answer for the
//     cloud identity).
//   - Every Kubernetes client built through OptionsFrom picks it up, and
//     the GKE provider builds every Google API client with it
//     (pkg/cloud/gke clientOptions), so no check has to know.
//
// There is no fallback. With a caller token present, a client either uses
// it or fails; nothing silently reads as lookout's own identity.
type callerTokenKey struct{}

// WithCallerToken returns a context whose Kubernetes and Google API
// clients authenticate with ts instead of lookout's own credentials. A
// nil ts leaves ctx unchanged.
func WithCallerToken(ctx context.Context, ts oauth2.TokenSource) context.Context {
	if ts == nil {
		return ctx
	}
	return context.WithValue(ctx, callerTokenKey{}, ts)
}

// CallerTokenFrom returns the caller token carried by ctx, if any.
func CallerTokenFrom(ctx context.Context) (oauth2.TokenSource, bool) {
	ts, ok := ctx.Value(callerTokenKey{}).(oauth2.TokenSource)
	return ts, ok && ts != nil
}
