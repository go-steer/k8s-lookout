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

// Package kube provides Kubernetes client bootstrap shared by the
// lookout subcommands.
package kube

import (
	"fmt"
	"os"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Options selects how the Kubernetes client config is resolved.
type Options struct {
	// InCluster forces in-cluster service account credentials.
	InCluster bool
	// Kubeconfig is an explicit kubeconfig path. Used outside a pod.
	Kubeconfig string
	// Context names a context in the kubeconfig to use instead of its
	// current-context. It is a per-invocation override and never
	// writes back: selecting a cluster must not be a side effect that
	// outlives the process, because two concurrent invocations against
	// two clusters cannot both win a mutation of the shared file.
	//
	// Meaningless in-cluster — there is no kubeconfig to select from —
	// and BuildConfig says so rather than ignoring it.
	Context string
}

// BuildClient constructs a kubernetes.Interface from the options
// (see BuildConfig for resolution precedence).
func BuildClient(opts Options) (kubernetes.Interface, error) {
	cfg, err := BuildConfig(opts)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	return client, nil
}

// BuildDynamicClient constructs a dynamic.Interface from the same
// options, for reads of kinds outside the typed client surface
// (CRDs, aggregated APIs).
func BuildDynamicClient(opts Options) (dynamic.Interface, error) {
	cfg, err := BuildConfig(opts)
	if err != nil {
		return nil, err
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	return client, nil
}

// BuildClientFromConfig constructs a kubernetes.Interface from a
// rest.Config resolved elsewhere. It is the seam for multi-cluster
// bootstrap (docs/multi-cluster-design.md): a cloud.Fleet provider
// mints one config per cluster (GKE: ADC over the DNS endpoint), and
// those configs run through the same client construction as the
// kubeconfig/in-cluster path — pkg/kube stays cloud-free.
func BuildClientFromConfig(cfg *rest.Config) (kubernetes.Interface, error) {
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	return client, nil
}

// BuildDynamicClientFromConfig is BuildClientFromConfig for the dynamic
// client (CRDs, aggregated APIs), sharing one provider-supplied config.
func BuildDynamicClientFromConfig(cfg *rest.Config) (dynamic.Interface, error) {
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	return client, nil
}

// BuildConfig resolves the rest.Config the clients share.
// Precedence:
//  1. Explicit Kubeconfig always wins (out-of-cluster ops).
//  2. InCluster or auto-detected (KUBERNETES_SERVICE_HOST env
//     var is set inside a pod).
//  3. $KUBECONFIG env var → fallback to ~/.kube/config.
//
// Options.Context selects a context within whichever kubeconfig cases
// 1 and 3 land on. It is rejected against case 2: in-cluster
// credentials come from the pod's service account and there is no
// context to choose, so honoring the flag silently would report a
// cluster the invocation did not read.
func BuildConfig(opts Options) (*rest.Config, error) {
	var (
		cfg *rest.Config
		err error
	)
	switch {
	case opts.Kubeconfig != "":
		cfg, err = loadKubeconfig(&clientcmd.ClientConfigLoadingRules{ExplicitPath: opts.Kubeconfig}, opts.Context)
		if err != nil {
			return nil, fmt.Errorf("kubeconfig %s: %w", opts.Kubeconfig, err)
		}
	case opts.InCluster || os.Getenv("KUBERNETES_SERVICE_HOST") != "":
		if opts.Context != "" {
			return nil, fmt.Errorf("--context=%s is meaningless with in-cluster credentials: there is no kubeconfig to select a context from. Pass --kubeconfig=<path> as well, or drop --context", opts.Context)
		}
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("in-cluster config: %w", err)
		}
	default:
		// Fallback to default kubeconfig search (KUBECONFIG env,
		// then $HOME/.kube/config). Fine for local dev; a real
		// deployment always sets --in-cluster or --kubeconfig.
		cfg, err = loadKubeconfig(clientcmd.NewDefaultClientConfigLoadingRules(), opts.Context)
		if err != nil {
			return nil, fmt.Errorf("default kubeconfig: %w", err)
		}
	}
	return cfg, nil
}

// loadKubeconfig resolves one kubeconfig with an optional
// current-context override. The override struct has always been here
// and empty; kubeContext is what fills it.
func loadKubeconfig(loader clientcmd.ClientConfigLoader, kubeContext string) (*rest.Config, error) {
	overrides := &clientcmd.ConfigOverrides{CurrentContext: kubeContext}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
}
