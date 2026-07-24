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

//go:build gke || allproviders

package main

// Blank-import the GKE provider so its init() registers it with
// pkg/cloud. This file carries the same build tags as pkg/cloud/gke:
// the default build links no cloud provider (DESIGN.md §2); release
// builds use -tags allproviders.
import (
	_ "github.com/go-steer/k8s-lookout/pkg/cloud/gke"
)
