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

	"k8s.io/client-go/rest"
)

// The cluster a read-path invocation talks to is chosen once, by the
// caller, and then has to reach a client constructor that may be
// several packages away — every check group builds its own client
// behind a `Deps.client(ctx)` accessor. Threading two more strings
// through fourteen accessors and every fake in their tests would put
// cluster selection in the signature of code that has no opinion
// about it.
//
// The context is the right carrier because it is already the thing
// those accessors take and already scopes exactly one invocation. The
// alternative — a package-level default — would make two concurrent
// invocations against two clusters race, which is the ambient
// current-context problem this flag pair exists to remove.
type selectionKey struct{}

// Selection is the per-invocation cluster choice: the two values
// behind `--kubeconfig` and `--context`, or a Target named by the
// caller of a multi-cluster endpoint. The zero value means "resolve the
// way lookout always has" (in-cluster autodetect, then $KUBECONFIG,
// then ~/.kube/config, at its current-context).
type Selection struct {
	Kubeconfig string
	Context    string
	// Target, when set, is the cluster this invocation reads: it wins
	// over every other way of finding one.
	Target *Target
}

// Target is one cluster of a multi-cluster endpoint, chosen per call
// (#700): who it is, for the cloud provider, and the config that
// reaches it, for every Kubernetes client. The config is the fleet
// source's (a DNS endpoint with the server's credentials, or a
// kubeconfig context's); a caller token still replaces its credentials
// in BuildConfig, as it would any other config's.
type Target struct {
	// Name is the cluster's own name; Project and Location are empty
	// for a cluster a kubeconfig fleet names, which knows neither.
	Name     string
	Project  string
	Location string
	Config   *rest.Config
}

// WithTarget returns a context whose selection is the target cluster.
func WithTarget(ctx context.Context, t Target) context.Context {
	s := SelectionFrom(ctx)
	s.Target = &t
	return context.WithValue(ctx, selectionKey{}, s)
}

// IsZero reports whether nothing was selected.
func (s Selection) IsZero() bool { return s == Selection{} }

// WithSelection returns a context carrying the cluster selection.
// emit.Run calls it once per invocation, before the check runs.
func WithSelection(ctx context.Context, s Selection) context.Context {
	if s.IsZero() {
		return ctx
	}
	return context.WithValue(ctx, selectionKey{}, s)
}

// SelectionFrom returns the selection carried by ctx, or the zero
// value.
func SelectionFrom(ctx context.Context) Selection {
	s, _ := ctx.Value(selectionKey{}).(Selection)
	return s
}

// OptionsFrom is the accessor a Deps.client method uses instead of a
// bare Options{}: it starts from the invocation's selection so that
// `--kubeconfig` and `--context` reach the client without every check
// group having to know they exist.
//
// It takes no base Options because the read path has no other source
// of them — the sentinel, which does (`--in-cluster`), builds its
// config directly through BuildConfig.
func OptionsFrom(ctx context.Context) Options {
	s := SelectionFrom(ctx)
	ts, _ := CallerTokenFrom(ctx)
	return Options{Kubeconfig: s.Kubeconfig, Context: s.Context, Target: s.Target, CallerToken: ts}
}
