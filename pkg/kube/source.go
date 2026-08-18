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

	"k8s.io/client-go/kubernetes"
)

// ClientSource yields the Kubernetes client for one invocation.
// Read-path commands take a ClientSource instead of a client so the
// (potentially failing) config resolution happens at run time under
// the invocation's context, and so tests substitute a
// fake.Clientset without touching command wiring.
type ClientSource func(ctx context.Context) (kubernetes.Interface, error)

// DefaultSource resolves the client with BuildClient's default
// precedence: in-cluster when running in a pod, $KUBECONFIG /
// ~/.kube/config otherwise.
func DefaultSource() ClientSource {
	return func(ctx context.Context) (kubernetes.Interface, error) {
		return BuildClient(OptionsFrom(ctx))
	}
}
