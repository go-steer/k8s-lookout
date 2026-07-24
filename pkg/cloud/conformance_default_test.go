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

//go:build !gke && !allproviders

package cloud

import (
	"context"
	"testing"
)

// The default (untagged) build is the vanilla-Kubernetes build: no
// provider may be compiled in (DESIGN.md §2). The tagged counterpart
// asserting gke IS registered lives in pkg/cloud/gke.

func TestDefaultBuildHasNoProvidersRegistered(t *testing.T) {
	if got := Registered(); len(got) != 0 {
		t.Errorf("default build has providers registered: %v — the provider boundary leaks into the vanilla build", got)
	}
}

func TestDefaultBuildNewDetectsNone(t *testing.T) {
	t.Setenv(ProviderEnv, "") // isolate from the developer's shell
	p, err := New(context.Background(), Config{})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if p != NoProvider {
		t.Errorf("New in default build = %v, want the NoProvider sentinel", p)
	}
}
