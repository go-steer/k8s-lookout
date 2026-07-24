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

package main

import (
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// Build-tag conformance at the binary level: the default lookout
// build is the vanilla-Kubernetes build and must link no cloud
// provider (DESIGN.md §2). If this fails, an untagged file gained a
// pkg/cloud/gke import.
func TestDefaultBinaryLinksNoCloudProvider(t *testing.T) {
	if got := cloud.Registered(); len(got) != 0 {
		t.Errorf("default lookout build registers cloud providers: %v", got)
	}
}
