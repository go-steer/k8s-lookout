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

package watch

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// clusterFleet is where resolveRunners gets its cluster list and each
// cluster's credentials. It is the two methods of cloud.Fleet plus a
// name for log lines, extracted as its own interface so multi-cluster
// is not a GKE-only feature (issue #388).
//
// Two implementations:
//
//   - fleetProvider — a cloud.Fleet provider (today: GKE behind the
//     gke build tag), which discovers clusters from the cloud's own
//     API and mints kubeconfig-free credentials from ADC.
//   - kubeconfigFleet — every context in a kubeconfig. No cloud API,
//     no build tag, and it works against EKS, on-prem, kind and a GKE
//     cluster whose credentials the operator already holds.
//
// The seam is deliberately small. Everything downstream of it —
// per-runner flags, the project-tier dedup, per-cluster dedup paths,
// supervision, readiness — already works off a []cloud.ClusterRef and
// a *rest.Config apiece, so a second implementation of exactly this
// pair is the whole of "watch a non-GKE fleet".
type clusterFleet interface {
	// Describe names the fleet source for log lines and errors.
	Describe() string
	// DiscoverClusters lists the clusters in scope. An empty result is
	// not an error here; the caller decides whether zero clusters is a
	// misconfiguration.
	DiscoverClusters(ctx context.Context) ([]cloud.ClusterRef, error)
	// RESTConfig resolves one cluster's credentials. A failure is
	// per-cluster and survivable — resolveRunners skips that cluster
	// and watches the rest (§11: one cluster's problem is not every
	// cluster's).
	RESTConfig(ctx context.Context, ref cloud.ClusterRef) (*rest.Config, error)
}

// fleetProvider adapts a cloud.Fleet provider to clusterFleet, adding
// only the provider's name for log lines.
type fleetProvider struct {
	cloud.Fleet
	name string
}

func (p fleetProvider) Describe() string { return "cloud provider " + p.name }

// kubeconfigFleetPrefix is the reserved --clusters-from value that
// selects the kubeconfig enumerator instead of cloud discovery:
// `kubeconfig` for client-go's default search, or
// `kubeconfig:<path>` for one explicit file. Checked before the
// project/location split, since a path contains slashes.
const kubeconfigFleetPrefix = "kubeconfig"

// kubeconfigFleet enumerates a fleet from a kubeconfig: one cluster
// per context, reached with that context's own credentials.
//
// Context names are the cluster identities. They are unique within a
// merged kubeconfig by construction, which is exactly the property
// per-cluster metric labels, dedup snapshot paths and readiness
// entries need — and it is why the enumeration keys on contexts rather
// than on cluster entries, several of which can share a server.
//
// The refs carry no project, location or region: a kubeconfig does not
// know them, and inventing them would put a wrong failure domain into
// the §8 fingerprint. Each runner then resolves identity the way a
// single-cluster sentinel does (explicit flag > provider metadata >
// empty).
type kubeconfigFleet struct {
	// path is an explicit kubeconfig file; empty means client-go's
	// default search ($KUBECONFIG, else ~/.kube/config).
	path string
}

func (k kubeconfigFleet) Describe() string {
	if k.path == "" {
		return "kubeconfig (default search: $KUBECONFIG, else ~/.kube/config)"
	}
	return "kubeconfig " + k.path
}

func (k kubeconfigFleet) DiscoverClusters(context.Context) ([]cloud.ClusterRef, error) {
	ctxs, err := kube.ListContexts(k.path)
	if err != nil {
		return nil, err
	}
	refs := make([]cloud.ClusterRef, 0, len(ctxs))
	for _, c := range ctxs {
		refs = append(refs, cloud.ClusterRef{Name: c.Name, Endpoint: c.Server})
	}
	return refs, nil
}

func (k kubeconfigFleet) RESTConfig(_ context.Context, ref cloud.ClusterRef) (*rest.Config, error) {
	return kube.ConfigForContext(k.path, ref.Name)
}

// parseKubeconfigFleet reports whether a --clusters-from value selects
// the kubeconfig enumerator, and with which explicit path ("" = the
// default search).
//
// `kubeconfig` is therefore reserved as a --clusters-from value and
// cannot name a cloud project. That is stated in the flag help; the
// alternative — a second flag whose only job is to disambiguate — is
// worse for a name nobody gives a project.
func parseKubeconfigFleet(s string) (path string, ok bool) {
	s = strings.TrimSpace(s)
	if s == kubeconfigFleetPrefix {
		return "", true
	}
	if p, found := strings.CutPrefix(s, kubeconfigFleetPrefix+":"); found {
		return strings.TrimSpace(p), true
	}
	return "", false
}

// resolveFleet picks the cluster source for this invocation:
// the kubeconfig enumerator when --clusters-from says so, else a
// Fleet-capable cloud provider.
func resolveFleet(ctx context.Context, f *flags) (clusterFleet, error) {
	if path, ok := parseKubeconfigFleet(f.clustersFrom); ok {
		return kubeconfigFleet{path: path}, nil
	}
	project, location := f.project, ""
	if f.clustersFrom != "" {
		project, location = parseClustersFrom(f.clustersFrom)
	}
	provider, err := cloud.New(ctx, cloud.Config{Project: project, Location: location})
	if err != nil {
		return nil, fmt.Errorf("multi-cluster: cloud provider: %w", err)
	}
	fleet, ok := provider.(cloud.Fleet)
	if !ok {
		return nil, fmt.Errorf("multi-cluster: provider %q cannot mint kubeconfig-free cluster credentials — build with -tags gke for GKE, or watch the fleet from a kubeconfig with --clusters-from=%s[:<path>] (one cluster per context, no cloud API needed)",
			provider.Name(), kubeconfigFleetPrefix)
	}
	return fleetProvider{Fleet: fleet, name: provider.Name()}, nil
}
