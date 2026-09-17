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

// Package topologydrift is the resident source for topology axes: it maintains
// the per-subject distributions that pkg/leeway scores (docs/leeway-design.md
// §6).
//
// The division of labour with pkg/leeway is strict and worth stating, because
// it is what keeps the engine testable without a cluster. pkg/leeway owns the
// rules — eligibility, apportionment, scoring — and imports no client-go. This
// package owns everything that touches an informer: resolving a Pod to a
// placement, resolving it to a subject, keeping the counts correct under a
// delta stream, and producing the pkg/leeway input shapes from cached objects.
//
// The counts are maintained incrementally rather than recomputed, because
// recomputing per evaluation is O(pods) per subject and the whole point of §6
// is to avoid that. Incremental counters are also the kind of code that is
// correct in tests and subtly wrong in production sixty days in, which is why
// §6.5's verifier is a shipped component rather than a debugging aid.
//
// Phase 2 deliberately emits nothing. Intent inference is Phase 3 and findings
// are Phase 4, so this package counts, exports metrics and produces no signals
// at all — which is what makes shipping it default-on tolerable before the
// detection half exists.
package topologydrift
