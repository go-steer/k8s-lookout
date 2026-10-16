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

// Package fleet resolves the clusters a multi-cluster lookout serves,
// and their credentials, from --clusters or --clusters-from. It is
// shared by `lookout watch` and `lookout mcp` so the two cannot drift
// (docs/multi-cluster-design.md, docs/mcp-multi-tenant-design.md Part 1).
//
// Three sources:
//
//   - a cloud provider's fleet, discovered from one or more scopes:
//     projects, GKE fleets, folders and organizations, which may be mixed
//     on one --clusters-from;
//   - explicit name=endpoint pairs (--clusters), reached through the same
//     provider's credentials;
//   - every context in a kubeconfig (--clusters-from=kubeconfig[:path]),
//     with no cloud API and no build tag, so EKS, on-prem and kind fleets
//     work too.
package fleet

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// Source is where a multi-cluster process gets its clusters and each
// cluster's credentials.
type Source interface {
	// Describe names the source for log lines and errors.
	Describe() string
	// Discover lists the clusters in scope. An empty result is not an
	// error here; the caller decides whether zero clusters is a
	// misconfiguration. One scope failing among several is reported in
	// the Result, not returned: only every scope failing is an error.
	Discover(ctx context.Context) (Result, error)
	// RESTConfig resolves one cluster's credentials. A failure is
	// per-cluster and survivable.
	RESTConfig(ctx context.Context, ref cloud.ClusterRef) (*rest.Config, error)
}

// Result is a discovery: the clusters, and what each scope yielded and
// how, so a fallback or a partial failure is never silent.
type Result struct {
	Clusters []cloud.ClusterRef
	Scopes   []ScopeResult
}

// ScopeResult is one scope's part of a discovery.
type ScopeResult struct {
	// Scope is the scope as spelled on --clusters-from.
	Scope string
	// Clusters is how many this scope found; Duplicates how many of
	// those an earlier scope had already found (served once).
	Clusters   int
	Duplicates int
	// Via, Fallback and Skipped are cloud.Discovery's.
	Via      string
	Fallback string
	Skipped  map[string]int
	// Err is set when this scope failed and others did not.
	Err error
}

// Notes are the per-scope lines worth logging: every scope's count and
// source, and each fallback, skip count and failure. Outside text (scope
// names, API messages) is quoted, so a value cannot forge a log line.
func (r Result) Notes() []string {
	notes := make([]string, 0, len(r.Scopes))
	for _, s := range r.Scopes {
		if s.Err != nil {
			notes = append(notes, fmt.Sprintf("scope %q FAILED, its clusters are not served: %q", s.Scope, s.Err.Error()))
			continue
		}
		line := fmt.Sprintf("scope %q: %d cluster(s) via %s", s.Scope, s.Clusters, s.Via)
		if s.Duplicates > 0 {
			line += fmt.Sprintf(", %d already found by an earlier scope", s.Duplicates)
		}
		if s.Fallback != "" {
			line += fmt.Sprintf(" (fallback: %q)", s.Fallback)
		}
		for _, reason := range slices.Sorted(maps.Keys(s.Skipped)) {
			line += fmt.Sprintf("; skipped %d %s", s.Skipped[reason], reason)
		}
		notes = append(notes, line)
	}
	return notes
}

// Spec is what to resolve: the two flags, and the project a provider
// falls back on for explicit --clusters endpoints.
type Spec struct {
	Clusters     string
	ClustersFrom string
	Project      string
}

// KubeconfigScope is the reserved --clusters-from value that selects the
// kubeconfig enumerator instead of cloud discovery: `kubeconfig` for
// client-go's default search, or `kubeconfig:<path>` for one file.
const KubeconfigScope = "kubeconfig"

// Resolve picks the cluster source for a spec: the kubeconfig enumerator
// when --clusters-from says so, else a Fleet-capable cloud provider,
// discovering --clusters-from's scopes or reaching --clusters' endpoints.
func Resolve(ctx context.Context, spec Spec) (Source, error) {
	if spec.Clusters != "" && spec.ClustersFrom != "" {
		return nil, errors.New("--clusters and --clusters-from are mutually exclusive: list endpoints OR discover them, not both")
	}
	var (
		scopes []cloud.Scope
		static []cloud.ClusterRef
	)
	if spec.ClustersFrom != "" {
		parsed, err := ParseScopes(spec.ClustersFrom)
		if err != nil {
			return nil, err
		}
		if parsed.Kubeconfig {
			return kubeconfigSource{path: parsed.KubeconfigPath}, nil
		}
		scopes = parsed.Scopes
	} else {
		refs, err := ParseClusters(spec.Clusters)
		if err != nil {
			return nil, err
		}
		static = refs
	}
	cfg := cloud.Config{Project: spec.Project}
	if len(scopes) == 1 && scopes[0].Kind == cloud.ScopeProject {
		cfg = cloud.Config{Project: scopes[0].Project, Location: scopes[0].Location}
	}
	provider, err := cloud.New(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("multi-cluster: cloud provider: %w", err)
	}
	fl, ok := provider.(cloud.Fleet)
	if !ok {
		return nil, fmt.Errorf("multi-cluster: provider %q cannot mint kubeconfig-free cluster credentials — build with -tags gke for GKE, or use a kubeconfig with --clusters-from=%s[:<path>] (one cluster per context, no cloud API needed)",
			provider.Name(), KubeconfigScope)
	}
	src := cloudSource{fleet: fl, name: provider.Name(), scopes: scopes, static: static}
	if sf, ok := provider.(cloud.ScopedFleet); ok {
		src.scoped = sf
	} else if needsScoped(scopes) {
		return nil, fmt.Errorf("multi-cluster: provider %q discovers only its own project; %q needs a provider that can search other projects, fleets and folders", provider.Name(), spec.ClustersFrom)
	}
	return src, nil
}

// needsScoped reports whether scopes ask for more than one project's
// clusters, which a Fleet without ScopedFleet cannot discover.
func needsScoped(scopes []cloud.Scope) bool {
	return len(scopes) > 1 || (len(scopes) == 1 && scopes[0].Kind != cloud.ScopeProject)
}

// cloudSource is a cloud provider's fleet: discovered scopes, or
// explicit endpoints.
type cloudSource struct {
	fleet  cloud.Fleet
	scoped cloud.ScopedFleet // nil when the provider has only Fleet
	name   string
	scopes []cloud.Scope
	static []cloud.ClusterRef
}

func (s cloudSource) Describe() string { return "cloud provider " + s.name }

func (s cloudSource) RESTConfig(ctx context.Context, ref cloud.ClusterRef) (*rest.Config, error) {
	return s.fleet.RESTConfig(ctx, ref)
}

func (s cloudSource) Discover(ctx context.Context) (Result, error) {
	if len(s.scopes) == 0 {
		return Result{Clusters: append([]cloud.ClusterRef(nil), s.static...)}, nil
	}
	var (
		res    Result
		seen   = map[string]bool{}
		errs   []error
		failed int
	)
	for _, scope := range s.scopes {
		disc, err := s.discover(ctx, scope)
		sr := ScopeResult{Scope: scope.String(), Via: disc.Via, Fallback: disc.Fallback, Skipped: disc.Skipped}
		if err != nil {
			if ctx.Err() != nil {
				return Result{}, ctx.Err()
			}
			failed++
			sr.Err = err
			errs = append(errs, fmt.Errorf("%s: %w", scope, err))
			res.Scopes = append(res.Scopes, sr)
			continue
		}
		for _, ref := range disc.Clusters {
			// Scopes may overlap (a project inside a listed folder, a
			// fleet member in a listed project): one cluster, once.
			sr.Clusters++
			k := ref.Project + "/" + ref.Location + "/" + ref.Name
			if seen[k] {
				sr.Duplicates++
				continue
			}
			seen[k] = true
			res.Clusters = append(res.Clusters, ref)
		}
		res.Scopes = append(res.Scopes, sr)
	}
	if failed == len(s.scopes) {
		return res, errors.Join(errs...)
	}
	return res, nil
}

func (s cloudSource) discover(ctx context.Context, scope cloud.Scope) (cloud.Discovery, error) {
	if s.scoped != nil {
		return s.scoped.Discover(ctx, scope)
	}
	refs, err := s.fleet.DiscoverClusters(ctx)
	return cloud.Discovery{Clusters: refs, Via: s.name}, err
}

// kubeconfigSource enumerates a fleet from a kubeconfig: one cluster per
// context, reached with that context's own credentials.
//
// Context names are the cluster identities. They are unique within a
// merged kubeconfig by construction, which is exactly the property
// per-cluster metric labels, store paths and readiness entries need,
// and it is why the enumeration keys on contexts rather than on cluster
// entries, several of which can share a server.
//
// The refs carry no project, location or region: a kubeconfig does not
// know them, and inventing them would put a wrong failure domain into
// the §8 fingerprint.
type kubeconfigSource struct {
	// path is an explicit kubeconfig file; empty means client-go's
	// default search ($KUBECONFIG, else ~/.kube/config).
	path string
}

func (k kubeconfigSource) Describe() string {
	if k.path == "" {
		return "kubeconfig (default search: $KUBECONFIG, else ~/.kube/config)"
	}
	return "kubeconfig " + k.path
}

func (k kubeconfigSource) Discover(context.Context) (Result, error) {
	ctxs, err := kube.ListContexts(k.path)
	if err != nil {
		return Result{}, err
	}
	refs := make([]cloud.ClusterRef, 0, len(ctxs))
	for _, c := range ctxs {
		refs = append(refs, cloud.ClusterRef{Name: c.Name, Endpoint: c.Server})
	}
	return Result{Clusters: refs, Scopes: []ScopeResult{{Scope: k.Describe(), Clusters: len(refs), Via: "kubeconfig"}}}, nil
}

func (k kubeconfigSource) RESTConfig(_ context.Context, ref cloud.ClusterRef) (*rest.Config, error) {
	return kube.ConfigForContext(k.path, ref.Name)
}

// Scopes is a parsed --clusters-from: cloud scopes, or a kubeconfig.
type Scopes struct {
	Scopes         []cloud.Scope
	Kubeconfig     bool
	KubeconfigPath string
}

// ParseScopes parses --clusters-from: a comma-separated list of scopes,
// which may be mixed:
//
//	my-project, my-project/us-central1   one project, all or one location
//	fleet:<host-project>                 a GKE fleet's members
//	folders/<id>, organizations/<id>     everything under a folder or org
//
// or, alone, kubeconfig or kubeconfig:<path>. `kubeconfig`, `fleet:`,
// `folders/` and `organizations/` are therefore reserved and cannot name
// a project; no project is named any of them in practice.
func ParseScopes(s string) (Scopes, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Scopes{}, errors.New("--clusters-from is empty")
	}
	if path, ok := parseKubeconfig(s); ok {
		return Scopes{Kubeconfig: true, KubeconfigPath: path}, nil
	}
	var out Scopes
	seen := map[string]bool{}
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := parseKubeconfig(item); ok {
			return Scopes{}, fmt.Errorf("--clusters-from: %q cannot be combined with cloud scopes; a kubeconfig fleet is the whole value", item)
		}
		scope, err := parseScope(item)
		if err != nil {
			return Scopes{}, err
		}
		if k := scope.String(); !seen[k] {
			seen[k] = true
			out.Scopes = append(out.Scopes, scope)
		}
	}
	if len(out.Scopes) == 0 {
		return Scopes{}, errors.New("--clusters-from names no scope")
	}
	return out, nil
}

func parseScope(item string) (cloud.Scope, error) {
	if host, ok := strings.CutPrefix(item, "fleet:"); ok {
		if host = strings.TrimSpace(host); host == "" || strings.Contains(host, "/") {
			return cloud.Scope{}, fmt.Errorf("--clusters-from: %q: a fleet scope is fleet:<host-project>", item)
		}
		return cloud.Scope{Kind: cloud.ScopeFleet, Project: host}, nil
	}
	for prefix, kind := range map[string]cloud.ScopeKind{"folders/": cloud.ScopeFolder, "organizations/": cloud.ScopeOrganization} {
		if id, ok := strings.CutPrefix(item, prefix); ok {
			if !allDigits(id) {
				return cloud.Scope{}, fmt.Errorf("--clusters-from: %q: %s<id> takes the numeric ID", item, prefix)
			}
			return cloud.Scope{Kind: kind, ID: id}, nil
		}
	}
	project, location, _ := strings.Cut(item, "/")
	if project == "" || strings.Contains(location, "/") {
		return cloud.Scope{}, fmt.Errorf("--clusters-from: %q is not a project, project/location, fleet:<host>, folders/<id> or organizations/<id>", item)
	}
	return cloud.Scope{Kind: cloud.ScopeProject, Project: project, Location: location}, nil
}

// parseKubeconfig reports whether a --clusters-from value selects the
// kubeconfig enumerator, and with which explicit path ("" = default
// search).
func parseKubeconfig(s string) (path string, ok bool) {
	s = strings.TrimSpace(s)
	if s == KubeconfigScope {
		return "", true
	}
	if p, found := strings.CutPrefix(s, KubeconfigScope+":"); found {
		return strings.TrimSpace(p), true
	}
	return "", false
}

// ParseClusters parses --clusters: comma-separated name=endpoint pairs
// (or a bare endpoint, whose short name is its first DNS label). Refs
// come back in listed order; a duplicate name, empty endpoint, or empty
// name is a loud config error.
func ParseClusters(s string) ([]cloud.ClusterRef, error) {
	var items []string
	for _, it := range strings.Split(s, ",") {
		if it = strings.TrimSpace(it); it != "" {
			items = append(items, it)
		}
	}
	if len(items) == 0 {
		return nil, errors.New("--clusters is empty")
	}
	refs := make([]cloud.ClusterRef, 0, len(items))
	seen := map[string]bool{}
	for _, it := range items {
		var name, endpoint string
		if i := strings.IndexByte(it, '='); i >= 0 {
			name = strings.TrimSpace(it[:i])
			endpoint = strings.TrimSpace(it[i+1:])
		} else {
			endpoint = it
			name = shortClusterName(endpoint)
		}
		if endpoint == "" {
			return nil, fmt.Errorf("--clusters: entry %q has no endpoint", it)
		}
		if name == "" {
			return nil, fmt.Errorf("--clusters: entry %q has no cluster name", it)
		}
		if seen[name] {
			return nil, fmt.Errorf("--clusters: duplicate cluster name %q", name)
		}
		seen[name] = true
		refs = append(refs, cloud.ClusterRef{Name: name, Endpoint: endpoint})
	}
	return refs, nil
}

// shortClusterName derives a cluster name from a bare endpoint: its
// first DNS label (uid.us-central1.gke.goog → "uid"), or the whole
// string when there's no dot. Use name=endpoint for a friendly name.
func shortClusterName(endpoint string) string {
	if i := strings.IndexByte(endpoint, '.'); i > 0 {
		return endpoint[:i]
	}
	return endpoint
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
