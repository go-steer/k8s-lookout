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

// Package audit implements the `lookout audit` command group
// (docs/fleet-audit-detectors-design.md, epic #182): deterministic
// best-practice POSTURE checks, as distinct from the incident groups
// that report what is currently broken.
//
// # Why posture is its own group and not its own binary
//
// A posture finding makes a different claim from every other group in
// this tree. `triage`, `state`, and `stab` say "something is wrong
// right now"; `audit` says "nothing is wrong right now, and there is no
// safety net for when it is". Those answer different questions, carry
// different urgency, and are read by different consumers, so they are
// separated — but by GROUP, not by binary.
//
// A sibling binary was the alternative and costs a second release
// artifact, a second image, a second RBAC surface, a second deploy, and
// a forked doc generator, permanently. The isolation ladder is
// group → build tag → separate binary, and every rung above the first
// is still walkable later if posture turns out to need it. Starting at
// the top rung is not.
//
// # The detectors do not share a runtime
//
// Unlike `triage`, the audit commands have no common cluster client or
// shared scan pass: a posture detector reads whatever API objects its
// own claim needs. What they DO share is the §4.2 envelope, the
// exemption seam (pkg/exempt, wired into emit.Writer for every command
// in the tree, not just this group), and the posture fingerprint recipe
// (engine.PostureFingerprint). Those three are the group's actual
// contract; see docs/audit-ingestion-contract.md for how a consumer
// ingests what comes out.
package audit

import "github.com/go-steer/k8s-lookout/pkg/checks"

func init() {
	checks.Register(ExemptionsCommand())
}
