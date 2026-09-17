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

// Package leeway is the placement-drift scoring engine: given where a
// subject's objects actually are and what its intent says they should be, it
// produces the numbers §7.3 of docs/leeway-design.md defines.
//
// # Purity
//
// This package does no I/O, holds no cluster connection, and imports no
// client-go. That is NFR-10, and purity_test.go enforces it transitively
// rather than by convention. Two things depend on it: `cmd/leeway` can be a
// thin standalone binary (§2.4), and every rule in here is testable by
// constructing a few slices instead of standing up a cluster — which is why
// the property tests can afford to run thousands of cases.
//
// The line is drawn at predicates, not at types. Kubernetes API types
// (k8s.io/api, k8s.io/apimachinery) are fine and are used where the design
// specifies them, because re-declaring UnsatisfiableConstraintAction locally
// would buy nothing but a translation layer. What is excluded is anything
// that reads a cluster. Where a rule genuinely needs to evaluate a scheduler
// predicate against a real Node — eligibility, in eligible.go — this package
// owns the *rule* and takes the *predicate* as a parameter, so the caller
// supplies node matching and leeway stays arithmetic.
//
// # Layout
//
//	topology.go    domain model (§5): keys, domains, subjects, distributions
//	intent.go      normalised Intent and precedence resolution (§5.1)
//	eligible.go    eligible-domain computation (§7.1)
//	apportion.go   Hamilton apportionment with water-filling caps (§7.2)
//	score.go       skew, R, ρ, Herfindahl, χ², small-n gating (§7.3, §7.4)
//
// baseline.go (§7.5, Tier C) and rank.go (§7.7, preference axes) are listed in
// the design's §2.2 layout but belong to phases 5 and 6; they are deliberately
// absent rather than stubbed.
//
// # Determinism
//
// Every function here is deterministic: the same inputs produce bit-identical
// outputs, including in the tie-breaks inside apportionment. This is not
// fastidiousness. These numbers reach finding bodies and fingerprints, so an
// apportionment that resolved a tie by map-iteration order would make findings
// flap between evaluations with nothing in the cluster having changed.
package leeway
