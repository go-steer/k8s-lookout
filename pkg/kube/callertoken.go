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

// A caller token is either a person's own Google token (Google-token mode,
// #699) or a service account's token minted for a workload caller (#701,
// docs/mcp-multi-tenant-design.md "Caller kinds"). Both make every client
// read as that identity, but only a person's reads of lookout's own store
// and report files are filtered by their access: workload callers are not
// row-filtered, by design ("The store is not behind RBAC"), and their
// writes persist the way an operator's do.
type workloadCallerKey struct{}

// WithWorkloadCaller marks the invocation as running for a workload
// caller: a caller token it carries is a service account's, not a
// person's, so CallerFiltered is false.
func WithWorkloadCaller(ctx context.Context) context.Context {
	return context.WithValue(ctx, workloadCallerKey{}, true)
}

// CallerFiltered reports whether the invocation runs as a person whose
// rows from lookout's store and reports must pass their own access check:
// a caller token is present and the caller is not a workload.
func CallerFiltered(ctx context.Context) bool {
	if _, ok := CallerTokenFrom(ctx); !ok {
		return false
	}
	workload, _ := ctx.Value(workloadCallerKey{}).(bool)
	return !workload
}
