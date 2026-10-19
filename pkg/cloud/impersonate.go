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
	"errors"
	"sync"

	"golang.org/x/oauth2"
)

// Impersonator mints access tokens for a service account, using the
// process's own credentials (Application Default Credentials on GKE) as
// the impersonating identity. lookout mcp uses it to let a workload
// caller read as a service account configured for it (#701), rather than
// as lookout's own identity, and to check "group:" allow-list entries as
// the --google-groups-service-account.
//
// The tokens carry scopes; none means the cloud-platform scope.
//
// The returned token source refreshes itself and must outlive the call
// that asked for it, so an implementation must not tie it to ctx's
// cancellation.
type Impersonator func(ctx context.Context, serviceAccount string, scopes ...string) (oauth2.TokenSource, error)

// ErrNoImpersonator is returned by Impersonate in a build with no cloud
// provider compiled in that can impersonate a service account.
var ErrNoImpersonator = errors.New("impersonating a service account needs a build with the GKE provider (-tags gke)")

var (
	impersonatorMu sync.RWMutex
	impersonator   Impersonator
)

// RegisterImpersonator installs the impersonator a provider package
// supplies, from its build-tag-guarded init(). A second registration is a
// programmer error and panics.
func RegisterImpersonator(f Impersonator) {
	impersonatorMu.Lock()
	defer impersonatorMu.Unlock()
	if f == nil {
		panic("cloud: RegisterImpersonator with a nil impersonator")
	}
	if impersonator != nil {
		panic("cloud: duplicate impersonator registration")
	}
	impersonator = f
}

// CanImpersonate reports whether this build can impersonate a service
// account at all.
func CanImpersonate() bool {
	impersonatorMu.RLock()
	defer impersonatorMu.RUnlock()
	return impersonator != nil
}

// Impersonate returns a token source for serviceAccount carrying scopes
// (none: cloud-platform), minted with the process's own credentials, or
// ErrNoImpersonator.
func Impersonate(ctx context.Context, serviceAccount string, scopes ...string) (oauth2.TokenSource, error) {
	impersonatorMu.RLock()
	f := impersonator
	impersonatorMu.RUnlock()
	if f == nil {
		return nil, ErrNoImpersonator
	}
	return f(ctx, serviceAccount, scopes...)
}
