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

// Package all blank-imports every read-path check package, so that
// one import populates the default registry with the complete command
// set:
//
//	import _ "github.com/go-steer/k8s-lookout/pkg/checks/all"
//
// Before this package existed the same import block was copied into
// five places — the binary, both doc generators, and both generator
// drift tests — and adding a check meant remembering all five. A
// forgotten site does not fail loudly: the command simply does not
// exist for that consumer, so `lookout --help` or a reference page
// silently omits it while every test still passes. TestAllPackages
// (all_test.go) is the guard: it walks pkg/checks/** for
// checks.Register calls and fails if this file does not import the
// package making them.
//
// Import this only from a main package or a test. A check package
// must not import it — that would be an import cycle in spirit
// (every check depending on every other) even where the compiler
// permits it.
package all

import (
	_ "github.com/go-steer/k8s-lookout/pkg/checks/audit"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/delta"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/events"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/findings"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/health"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/inventory"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/logs"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/netprobe"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/perf"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/stab"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/state"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/top"
	_ "github.com/go-steer/k8s-lookout/pkg/checks/triage"
)
