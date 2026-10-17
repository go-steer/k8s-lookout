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
	"net/http"
	"sync"
)

// ServiceClientFunc returns an HTTP client that calls Google APIs as the
// process's own identity (Application Default Credentials on GKE), with
// the given OAuth scopes. lookout mcp uses it for the parts of its OAuth
// authorization server (#702) that talk to Google as lookout: the Agent
// Identity credential vault that holds people's Google grants, and Secret
// Manager when the token-sealing keys live there.
//
// The client refreshes its own token and must outlive the call that asked
// for it, so an implementation must not tie it to ctx's cancellation.
type ServiceClientFunc func(ctx context.Context, scopes ...string) (*http.Client, error)

// ErrNoServiceClient is returned by ServiceClient in a build with no cloud
// provider compiled in that can call Google APIs as lookout.
var ErrNoServiceClient = errors.New("calling Google APIs as lookout's own identity needs a build with the GKE provider (-tags gke)")

var (
	serviceClientMu sync.RWMutex
	serviceClient   ServiceClientFunc
)

// RegisterServiceClient installs the client constructor a provider package
// supplies, from its build-tag-guarded init(). A second registration is a
// programmer error and panics.
func RegisterServiceClient(f ServiceClientFunc) {
	serviceClientMu.Lock()
	defer serviceClientMu.Unlock()
	if f == nil {
		panic("cloud: RegisterServiceClient with a nil constructor")
	}
	if serviceClient != nil {
		panic("cloud: duplicate service client registration")
	}
	serviceClient = f
}

// CanServiceClient reports whether this build can call Google APIs as
// lookout at all.
func CanServiceClient() bool {
	serviceClientMu.RLock()
	defer serviceClientMu.RUnlock()
	return serviceClient != nil
}

// ServiceClient returns an HTTP client authenticated as the process's own
// identity with scopes, or ErrNoServiceClient.
func ServiceClient(ctx context.Context, scopes ...string) (*http.Client, error) {
	serviceClientMu.RLock()
	f := serviceClient
	serviceClientMu.RUnlock()
	if f == nil {
		return nil, ErrNoServiceClient
	}
	return f(ctx, scopes...)
}
