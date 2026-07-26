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

// Shared plumbing for the M4 capability implementations: lazy GCP
// client construction and resource-URL parsing.
//
// SDK choice (documented per the M4 plan): google.golang.org/api
// (the REST discovery clients) rather than cloud.google.com/go/*
// (the gRPC GAPICs). Rationale: the module is already in this
// repo's dependency graph via core-agent, the JSON-over-REST wire
// shape matches the §13 recorded-fixture convention exactly (a
// fixture IS a documented REST response body), and it links no
// additional gRPC/proto surface into the tagged build. Everything
// in this package remains behind the gke/allproviders build tags —
// the default build stays GCP-free (enforced by
// cmd/lookout/nogcp_default_test.go).

import (
	"context"
	"strings"
	"sync"
)

// lazyClient memoizes construction of one API client so capability
// getters stay cheap and side-effect free: credential/network
// errors surface at first call time (the command's runtime-error
// path, exit 1) instead of at provider construction, where they
// would break cloud-agnostic commands too.
func lazyClient[T any](build func(ctx context.Context) (*T, error)) func(ctx context.Context) (*T, error) {
	var (
		once sync.Once
		v    *T
		err  error
	)
	return func(ctx context.Context) (*T, error) {
		once.Do(func() { v, err = build(ctx) })
		return v, err
	}
}

// resourceTail returns the last path segment of a GCP resource URL
// or partial URL ("…/machineTypes/n2-standard-16" →
// "n2-standard-16"). Bare names pass through unchanged.
func resourceTail(u string) string {
	if i := strings.LastIndexByte(u, '/'); i >= 0 {
		return u[i+1:]
	}
	return u
}

// resourceScopeValue extracts the value following a scope collection
// segment in a resource URL: resourceScopeValue(u, "zones") on
// "…/zones/us-east1-b/disks/d1" returns "us-east1-b"; "" when the
// segment is absent.
func resourceScopeValue(u, collection string) string {
	parts := strings.Split(u, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == collection {
			return parts[i+1]
		}
	}
	return ""
}

// locationRegion derives the region from a GKE location, which may
// be a region ("us-east1") or a zone ("us-east1-b"): GCE zone names
// are region + "-<suffix>", i.e. three dash-separated parts.
func locationRegion(location string) string {
	parts := strings.Split(location, "-")
	if len(parts) == 3 {
		return strings.Join(parts[:2], "-")
	}
	return location
}
