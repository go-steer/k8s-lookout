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

package cloud

import (
	"context"

	"k8s.io/client-go/rest"
)

// ClusterRef identifies one Kubernetes cluster a Fleet provider can
// connect to without a kubeconfig. Name is the human-readable cluster
// name the sentinel stamps as Signal.Cluster; Endpoint is the
// control-plane host the rest.Config dials (GKE: the *.gke.goog
// DNS endpoint).
type ClusterRef struct {
	Name     string
	Project  string
	Location string
	Endpoint string
}

// Fleet is the OPTIONAL provider surface for kubeconfig-free
// multi-cluster bootstrap (docs/multi-cluster-design.md). It mirrors
// the Identity pattern: a surface the sentinel type-asserts on the
// Provider, NOT a capability in the Metrics()/Quota() matrix — this is
// deployment bootstrap, resolved once at startup, not a per-signal
// facet. A provider that cannot mint credentials without a kubeconfig
// simply does not implement it, and the sentinel falls back to
// kube.Options (in-cluster / kubeconfig).
//
// The GKE implementation (pkg/cloud/gke, behind the gke build tag)
// discovers clusters via the Container API and mints an authenticated
// rest.Config from Application Default Credentials over each cluster's
// control-plane DNS endpoint — one identity for every cluster, no CA
// cert to pin.
type Fleet interface {
	// DiscoverClusters lists the clusters in the provider's configured
	// scope (GKE: projects/locations). An empty result is not an error
	// — it means nothing matched — but a missing project identity or a
	// failed API call is (§2 fail-loudly).
	DiscoverClusters(ctx context.Context) ([]ClusterRef, error)

	// RESTConfig mints an authenticated *rest.Config for one cluster.
	// The credential is the provider's own ADC identity; per-cluster
	// authorization is RBAC, bound to that identity in the target
	// cluster (docs/multi-cluster-design.md: authN ≠ authZ).
	RESTConfig(ctx context.Context, ref ClusterRef) (*rest.Config, error)
}
