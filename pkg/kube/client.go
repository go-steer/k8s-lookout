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

// BuildConfig resolves the rest.Config the clients share.
// Precedence:
//  1. Explicit Kubeconfig always wins (out-of-cluster ops).
//  2. InCluster or auto-detected (KUBERNETES_SERVICE_HOST env
//     var is set inside a pod).
//  3. $KUBECONFIG env var → fallback to ~/.kube/config.
func BuildConfig(opts Options) (*rest.Config, error) {
	var (
		cfg *rest.Config
		err error
	)
	switch {
	case opts.Kubeconfig != "":
		cfg, err = clientcmd.BuildConfigFromFlags("", opts.Kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("kubeconfig %s: %w", opts.Kubeconfig, err)
		}
	case opts.InCluster || os.Getenv("KUBERNETES_SERVICE_HOST") != "":
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("in-cluster config: %w", err)
		}
	default:
		// Fallback to default kubeconfig search (KUBECONFIG env,
		// then $HOME/.kube/config). Fine for local dev; a real
		// deployment always sets --in-cluster or --kubeconfig.
		loader := clientcmd.NewDefaultClientConfigLoadingRules()
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, &clientcmd.ConfigOverrides{}).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("default kubeconfig: %w", err)
		}
	}
	return cfg, nil
}
