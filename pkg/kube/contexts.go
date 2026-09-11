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
	"fmt"
	"sort"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ContextRef describes one context in a kubeconfig: enough to name a
// cluster and say where it is, without resolving credentials for it.
type ContextRef struct {
	// Name is the context name, which is what selects it. Context
	// names are unique within a merged kubeconfig by construction, so
	// this is a usable cluster identity.
	Name string
	// Cluster is the name of the cluster entry the context points at.
	// Not unique — several contexts (different users, namespaces) can
	// point at one cluster.
	Cluster string
	// Server is that cluster entry's API server URL, for logging and
	// for the operator to confirm the enumeration found what they
	// meant. Empty if the context names a cluster the file does not
	// define, which clientcmd only complains about at connect time.
	Server string
}

// ListContexts enumerates every context in a kubeconfig, sorted by
// name. It is the enumerate half of a kubeconfig-backed fleet
// (multi-cluster over clusters no cloud API knows about, issue #388):
// the caller turns each context into a cluster to watch and calls
// ConfigForContext to reach it.
//
// path is an explicit kubeconfig file; empty uses client-go's default
// loading rules ($KUBECONFIG, colon-separated and merged, else
// ~/.kube/config), which is also how an operator selects a subset —
// compose a KUBECONFIG of exactly the files they want watched.
//
// Deliberately never falls back to in-cluster credentials: those name
// exactly one cluster and have no contexts, so silently returning the
// pod's own cluster from a call that asked for a kubeconfig would
// watch something the operator did not list.
func ListContexts(path string) ([]ContextRef, error) {
	cfg, err := contextLoadingRules(path).Load()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig %s: %w", kubeconfigDesc(path), err)
	}
	refs := make([]ContextRef, 0, len(cfg.Contexts))
	for name, kctx := range cfg.Contexts {
		ref := ContextRef{Name: name, Cluster: kctx.Cluster}
		if cluster, ok := cfg.Clusters[kctx.Cluster]; ok {
			ref.Server = cluster.Server
		}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })
	return refs, nil
}

// ConfigForContext resolves the rest.Config for one named context of a
// kubeconfig — the credentials half of ListContexts.
//
// Unlike BuildConfig this never considers in-cluster credentials, for
// the reason ListContexts gives: a caller enumerating contexts is
// naming clusters explicitly, and the pod's own cluster is not one of
// them unless the kubeconfig says so.
func ConfigForContext(path, context string) (*rest.Config, error) {
	if context == "" {
		return nil, fmt.Errorf("kubeconfig %s: no context named", kubeconfigDesc(path))
	}
	cfg, err := loadKubeconfig(contextLoadingRules(path), context)
	if err != nil {
		return nil, fmt.Errorf("kubeconfig %s: context %q: %w", kubeconfigDesc(path), context, err)
	}
	return cfg, nil
}

// contextLoadingRules picks the explicit file when given one and
// client-go's default search otherwise.
func contextLoadingRules(path string) clientcmd.ClientConfigLoader {
	if path != "" {
		return &clientcmd.ClientConfigLoadingRules{ExplicitPath: path}
	}
	return clientcmd.NewDefaultClientConfigLoadingRules()
}

// kubeconfigDesc names the kubeconfig in an error message — the
// explicit path, or what the default search covers.
func kubeconfigDesc(path string) string {
	if path != "" {
		return path
	}
	return "(default search: $KUBECONFIG, else ~/.kube/config)"
}
