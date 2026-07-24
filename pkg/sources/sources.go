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

// Package sources defines the signal-source contract of the sentinel
// (DESIGN.md §7.2): pluggable sources feeding one shared pipeline —
// one resident process per cluster, never N sidecars. Each source is
// a package under pkg/sources implementing Source; the sentinel
// registers the enabled ones, probes their declared RBAC needs at
// startup (fail loudly, §11), and runs them against a single emit
// callback.
//
// Provider boundary (AGENTS.md): this package and its sources never
// import GCP SDKs; cloud-backed sources go through pkg/cloud.Provider.
package sources

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// Signal is the unit sources emit — an alias so the Source interface
// reads exactly as specified in DESIGN.md §7.2 while the type itself
// lives with the pipeline in pkg/engine.
type Signal = engine.Signal

// Source is a signal source (DESIGN.md §7.2, normative). Run blocks
// until ctx is cancelled (clean shutdown, return nil) or the source
// fails (return the error — the runner treats a failed source as
// fatal, never as a silent gap in coverage). emit may be called from
// any goroutine; it must not be called after Run returns.
//
// Sources leave deployment identity (Cluster/Project/Zone) and
// Fingerprint empty; the pipeline stamps them before inject.
//
// A source whose informers need RBAC beyond the bare minimum should
// also implement AccessDeclarer so the §11 startup probe can fail
// loudly instead of the source watching an empty stream.
type Source interface {
	Name() string // stable, used in signal schema + config
	Scope() Scope // Namespace | Cluster | Project (§11)
	Run(ctx context.Context, emit func(Signal)) error
}

// Scope is the deployment tier a source operates at (DESIGN.md §11).
// A deployment whose RBAC cannot support a source's scope gets an
// explicit startup error naming the source and the missing
// permission — never a silent empty watch.
type Scope int

const (
	// ScopeNamespace: the source works under a namespaced Role.
	ScopeNamespace Scope = iota
	// ScopeCluster: the source needs cluster-wide RBAC (nodes,
	// cluster-scoped informers).
	ScopeCluster
	// ScopeProject: one instance per GCP project regardless of
	// cluster count (e.g. the quota source).
	ScopeProject
)

// String returns the config-facing name of the scope.
func (s Scope) String() string {
	switch s {
	case ScopeNamespace:
		return "namespace"
	case ScopeCluster:
		return "cluster"
	case ScopeProject:
		return "project"
	}
	return fmt.Sprintf("Scope(%d)", int(s))
}

// Registry holds the sources enabled for one sentinel process, in
// registration order. Names are unique — a duplicate registration is
// a programming error surfaced at startup, not a silent overwrite.
type Registry struct {
	mu     sync.Mutex
	order  []Source
	byName map[string]Source
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byName: make(map[string]Source)}
}

// Register adds a source. Empty and duplicate names are rejected:
// Name() feeds the signal schema and config surface, so a collision
// would make signals unattributable.
func (r *Registry) Register(s Source) error {
	name := s.Name()
	if name == "" {
		return fmt.Errorf("sources: refusing to register a source with an empty name (%T)", s)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byName[name]; dup {
		return fmt.Errorf("sources: duplicate source name %q", name)
	}
	r.byName[name] = s
	r.order = append(r.order, s)
	return nil
}

// Lookup returns the source registered under name, if any.
func (r *Registry) Lookup(name string) (Source, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.byName[name]
	return s, ok
}

// All returns the registered sources in registration order.
func (r *Registry) All() []Source {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Source, len(r.order))
	copy(out, r.order)
	return out
}

// RunAll runs every source concurrently and blocks until all have
// returned. A clean shutdown (ctx cancelled, sources return nil)
// returns nil. If any source fails, the remaining sources are
// cancelled and the first failure is returned, wrapped with the
// source's name: a sentinel with a dead source is lying about its
// coverage, so we stop the whole process and let the restart policy
// bring it back (§7.2 — never a silent gap).
func RunAll(ctx context.Context, srcs []Source, emit func(Signal)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for _, s := range srcs {
		wg.Add(1)
		go func(s Source) {
			defer wg.Done()
			err := s.Run(ctx, emit)
			if err == nil || ctx.Err() != nil {
				return // clean exit, or failure during shutdown
			}
			mu.Lock()
			if firstErr == nil {
				firstErr = fmt.Errorf("source %q: %w", s.Name(), err)
			}
			mu.Unlock()
			cancel() // take the other sources down with us
		}(s)
	}
	wg.Wait()
	return firstErr
}
