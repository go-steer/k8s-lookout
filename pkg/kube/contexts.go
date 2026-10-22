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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
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
	if cfg.Dial == nil {
		cfg.Dial = newFailFastDialer(DefaultDialTimeout)
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

// FleetTarget is one resolved target cluster for a read-path fleet
// fan-out (--kubeconfig-dir / --contexts).
type FleetTarget struct {
	Kubeconfig string
	Context    string
	// Label is Context when unique across the resolved target set, or
	// <filename>:<Context> when two kubeconfig files share a context
	// name.
	Label string
}

// ParseContextsList parses a --contexts value: a comma-separated list
// of context names, or "*" to select every context in the target
// kubeconfig(s). Duplicate names are collapsed in listed order.
func ParseContextsList(raw string) (contexts []string, all bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, errors.New("--contexts names no contexts")
	}
	seen := map[string]bool{}
	for _, tok := range strings.Split(raw, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if tok == "*" {
			all = true
			continue
		}
		if !seen[tok] {
			seen[tok] = true
			contexts = append(contexts, tok)
		}
	}
	if all {
		return nil, true, nil
	}
	if len(contexts) == 0 {
		return nil, false, errors.New("--contexts names no contexts")
	}
	return contexts, false, nil
}

// ResolveFleetTargets resolves the ordered list of target clusters for
// a read-path fleet fan-out from --kubeconfig / --kubeconfig-dir and
// --contexts.
func ResolveFleetTargets(kubeconfig, kubeconfigDir, contextsRaw string) ([]FleetTarget, error) {
	var (
		wanted []string
		all    bool
		err    error
	)
	if strings.TrimSpace(contextsRaw) != "" {
		wanted, all, err = ParseContextsList(contextsRaw)
		if err != nil {
			return nil, err
		}
	}

	var targets []FleetTarget
	if kubeconfigDir != "" {
		files, err := discoverKubeconfigFiles(kubeconfigDir)
		if err != nil {
			return nil, err
		}
		matchedWanted := map[string]bool{}
		for _, f := range files {
			switch {
			case all:
				for _, name := range sortedContextNames(f.cfg) {
					targets = append(targets, FleetTarget{Kubeconfig: f.path, Context: name})
				}
			case len(wanted) > 0:
				for _, name := range wanted {
					if _, ok := f.cfg.Contexts[name]; ok {
						matchedWanted[name] = true
						targets = append(targets, FleetTarget{Kubeconfig: f.path, Context: name})
					}
				}
			default:
				if f.cfg.CurrentContext != "" {
					if _, ok := f.cfg.Contexts[f.cfg.CurrentContext]; ok {
						targets = append(targets, FleetTarget{Kubeconfig: f.path, Context: f.cfg.CurrentContext})
						continue
					}
				}
				for _, name := range sortedContextNames(f.cfg) {
					targets = append(targets, FleetTarget{Kubeconfig: f.path, Context: name})
				}
			}
		}
		if len(wanted) > 0 {
			for _, name := range wanted {
				if !matchedWanted[name] {
					return nil, fmt.Errorf("--kubeconfig-dir %q: context %q not found in any kubeconfig file", kubeconfigDir, name)
				}
			}
		}
	} else {
		if all {
			refs, err := ListContexts(kubeconfig)
			if err != nil {
				return nil, err
			}
			if len(refs) == 0 {
				return nil, fmt.Errorf("kubeconfig %s defines no contexts", kubeconfigDesc(kubeconfig))
			}
			for _, ref := range refs {
				targets = append(targets, FleetTarget{Kubeconfig: kubeconfig, Context: ref.Name})
			}
		} else {
			for _, name := range wanted {
				targets = append(targets, FleetTarget{Kubeconfig: kubeconfig, Context: name})
			}
		}
	}

	if len(targets) == 0 {
		return nil, errors.New("no target contexts resolved")
	}

	counts := make(map[string]int, len(targets))
	for _, t := range targets {
		counts[t.Context]++
	}
	for i := range targets {
		if counts[targets[i].Context] > 1 && targets[i].Kubeconfig != "" {
			targets[i].Label = filepath.Base(targets[i].Kubeconfig) + ":" + targets[i].Context
		} else {
			targets[i].Label = targets[i].Context
		}
	}
	return targets, nil
}

type loadedKubeconfig struct {
	path string
	cfg  *clientcmdapi.Config
}

func discoverKubeconfigFiles(dir string) ([]loadedKubeconfig, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("--kubeconfig-dir %q: %w", dir, err)
	}
	var (
		files    []loadedKubeconfig
		firstErr error
	)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || strings.HasPrefix(name, ".") {
			continue
		}
		fullPath := filepath.Join(dir, name)
		info, err := os.Stat(fullPath)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		knownExt := hasKubeconfigExt(name)
		cfg, err := clientcmd.LoadFromFile(fullPath)
		if err != nil {
			if knownExt {
				return nil, fmt.Errorf("kubeconfig %s: %w", fullPath, err)
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("kubeconfig %s: %w", fullPath, err)
			}
			continue
		}
		if len(cfg.Contexts) == 0 {
			if knownExt {
				return nil, fmt.Errorf("kubeconfig %s defines no contexts", fullPath)
			}
			continue
		}
		files = append(files, loadedKubeconfig{path: fullPath, cfg: cfg})
	}
	if len(files) == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, fmt.Errorf("--kubeconfig-dir %q contains no kubeconfig files", dir)
	}
	return files, nil
}

func hasKubeconfigExt(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".yaml", ".yml", ".conf", ".kubeconfig":
		return true
	default:
		return false
	}
}

func sortedContextNames(cfg *clientcmdapi.Config) []string {
	names := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
