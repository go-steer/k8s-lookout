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
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/go-steer/k8s-lookout/internal/fleet"
	"github.com/go-steer/k8s-lookout/internal/mcpserver"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// multiClusterProblem checks the multi-cluster flags of `lookout mcp`
// (#700) against the rest, returning a usage problem or "".
func multiClusterProblem(multi, googleMode bool, clusters, clustersFrom, kubeconfig, contextName, store, report string, rediscover time.Duration, fs *flag.FlagSet) string {
	rediscoverSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "rediscover" {
			rediscoverSet = true
		}
	})
	if !multi {
		if rediscoverSet {
			return "--rediscover only applies to a multi-cluster endpoint; pass --clusters or --clusters-from as well"
		}
		return ""
	}
	switch {
	case clusters != "" && clustersFrom != "":
		return "--clusters and --clusters-from are mutually exclusive: list endpoints OR discover them, not both"
	case kubeconfig != "" || contextName != "":
		return "--kubeconfig and --context pin one cluster; a multi-cluster endpoint reaches each call's cluster through --clusters or --clusters-from (a kubeconfig fleet is --clusters-from=kubeconfig:<path>)"
	case googleMode && (store != "" || report != ""):
		// Store files are keyed by a cluster's bare name, which only a
		// discovery of the whole set can prove unique; each caller here
		// sees their own set, and a caller's rows would be access-checked
		// against a cluster that may not be the one they came from.
		return "--store and --report are not supported on a multi-cluster endpoint in Google-token mode: a store file is named by the cluster's bare name, which each caller's own cluster set cannot prove unique"
	case rediscover <= 0:
		return "--rediscover must be positive"
	}
	// In Google-token mode each caller's Google token is sent to the
	// cluster's API server, so only GKE control planes may be named
	// (#699): a kubeconfig, or a hand-written endpoint, could point
	// anywhere.
	const gkeOnly = "in Google-token mode only GKE clusters reached over their control-plane DNS endpoint (*.gke.goog) can be served: each caller's Google token is sent to the API server"
	if clusters != "" {
		refs, err := fleet.ParseClusters(clusters)
		if err != nil {
			return err.Error()
		}
		if googleMode {
			for _, r := range refs {
				if !mcpserver.IsGKEEndpoint(r.Endpoint) {
					return fmt.Sprintf("--clusters: %q: %s", r.Endpoint, gkeOnly)
				}
			}
		}
	} else {
		scopes, err := fleet.ParseScopes(clustersFrom)
		if err != nil {
			return err.Error()
		}
		if googleMode && scopes.Kubeconfig {
			return "--clusters-from=kubeconfig: " + gkeOnly
		}
	}
	return ""
}

// localClusterName is the canonical name of the cluster lookout runs in,
// for the one command (net probe) that measures from here. It is trusted
// only inside a pod, where the provider's identity comes from the
// metadata server: on a workstation the provider falls back to the
// kubeconfig's current context, and a probe from a laptop is from no
// cluster at all.
func localClusterName(ctx context.Context) string {
	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" {
		return ""
	}
	p, err := cloud.New(ctx, cloud.Config{})
	if err != nil {
		return ""
	}
	id, ok := p.(interface {
		Project() string
		Location() string
		Cluster() string
	})
	if !ok || id.Project() == "" || id.Location() == "" || id.Cluster() == "" {
		return ""
	}
	return id.Project() + "/" + id.Location() + "/" + id.Cluster()
}

// startClusters resolves the fleet and, outside Google-token mode,
// discovers it, logging what each scope found. It returns nil and the
// exit code on failure.
func startClusters(ctx context.Context, spec fleet.Spec, perCaller bool, rediscover time.Duration, defaultCluster, defaultProject string, stderr io.Writer) (*mcpserver.Clusters, int) {
	src, err := fleet.Resolve(ctx, spec)
	if err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
		return nil, 2
	}
	dir := mcpserver.NewClusters(src, perCaller, rediscover)
	dir.SetDefaults(defaultCluster, defaultProject)
	scopes, n, err := dir.Start(ctx)
	for _, note := range (fleet.Result{Scopes: scopes}).Notes() {
		fmt.Fprintf(stderr, "lookout mcp: multi-cluster: %s\n", note)
	}
	if err != nil {
		fmt.Fprintf(stderr, "lookout mcp: %v\n", err)
		return nil, 1
	}
	if perCaller {
		fmt.Fprintf(stderr, "lookout mcp: multi-cluster: each caller's clusters are discovered with their own token from %s\n", strconv.Quote(dir.Describe()))
	} else {
		fmt.Fprintf(stderr, "lookout mcp: multi-cluster: serving %d cluster(s) from %s, rediscovered every %s\n", n, strconv.Quote(dir.Describe()), rediscover)
	}
	return dir, 0
}
