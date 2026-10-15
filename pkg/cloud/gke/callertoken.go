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

	"google.golang.org/api/option"

	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// clientOptions are the options every Google API client in this package
// is built with. When the invocation runs as its caller (#699,
// kube.WithCallerToken), that is the caller's token, so Compute,
// Container, Monitoring, Logging, IAM, Cloud Quotas and Pub/Sub answer
// with what the caller's IAM allows. Otherwise it is nothing, and the
// client libraries use Application Default Credentials as before.
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
	return nil
}
