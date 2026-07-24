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
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

// ProviderEnv is the environment variable that selects a provider by
// name when no explicit --cloud-provider flag is given.
const ProviderEnv = "LOOKOUT_CLOUD_PROVIDER"

// Config carries provider selection and optional identity pins.
// Commands populate it from flags/env; providers fill any blanks with
// their own detection (metadata server, well-known env vars).
type Config struct {
	// Provider explicitly selects a registered provider by name.
	// Empty means detect (ProviderEnv, then the single registered
	// provider, then none); NoProviderName forces the NoProvider
	// sentinel even when providers are compiled in.
	Provider string

	// Project, Location, Cluster pin the cloud identity. Optional.
	Project  string
	Location string
	Cluster  string
}

// Factory constructs a Provider from config. Implementations are
// registered from an init() in their build-tag-guarded package.
type Factory func(ctx context.Context, cfg Config) (Provider, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register makes a provider available for selection by name. Called
// from provider package init(); duplicate or empty names are
// programmer errors and panic.
func Register(name string, factory Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if name == "" || name == NoProviderName {
		panic(fmt.Sprintf("cloud: Register with reserved name %q", name))
	}
	if factory == nil {
		panic(fmt.Sprintf("cloud: Register(%q) with nil factory", name))
	}
	if _, dup := registry[name]; dup {
		panic(fmt.Sprintf("cloud: duplicate provider registration %q", name))
	}
	registry[name] = factory
}

// Registered returns the names of all compiled-in providers, sorted.
// The default (untagged) build returns an empty slice.
func Registered() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// New selects and constructs a Provider:
//
//  1. explicit cfg.Provider, else the ProviderEnv environment
//     variable — unknown names are a usage error;
//  2. else the single registered provider, if exactly one;
//  3. else the NoProvider sentinel (nothing registered, or several
//     registered with no selection — every capability lookup then
//     reports unavailable with the reason, per §2 fail-loudly).
//
// New never returns a nil Provider without an error: cloud-agnostic
// commands run untouched, cloud-dependent ones report unavailable.
func New(ctx context.Context, cfg Config) (Provider, error) {
	name := cfg.Provider
	if name == "" {
		name = os.Getenv(ProviderEnv)
	}
	if name == NoProviderName {
		return NoProvider, nil
	}
	if name != "" {
		registryMu.RLock()
		factory, ok := registry[name]
		registryMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("cloud: unknown provider %q (compiled in: %s)", name, registeredOrNone())
		}
		return factory(ctx, cfg)
	}

	registered := Registered()
	switch len(registered) {
	case 1:
		registryMu.RLock()
		factory := registry[registered[0]]
		registryMu.RUnlock()
		return factory(ctx, cfg)
	case 0:
		return NoProvider, nil
	default:
		return unconfigured{reason: fmt.Sprintf(
			"multiple cloud providers compiled in (%s); select one via --cloud-provider or %s",
			strings.Join(registered, ", "), ProviderEnv)}, nil
	}
}

// registeredOrNone renders the registry for error messages.
func registeredOrNone() string {
	registered := Registered()
	if len(registered) == 0 {
		return "none"
	}
	return strings.Join(registered, ", ")
}
