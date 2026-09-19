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

package topologydrift

import (
	"slices"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// TestTierSeverities_MatchEmit holds pkg/leeway's duplicated severity strings
// equal to pkg/emit's.
//
// pkg/leeway may not import pkg/emit — emit transitively reaches client-go and
// NFR-10 forbids that — so Tier.Severity returns string literals that are
// pkg/emit's constants retyped. This package imports both and is where the
// duplication is made safe. Without it a rename in emit would leave leeway
// emitting a severity the sentinel's routing has never heard of, which fails
// as silently as it is possible to fail: the finding is produced, accepted, and
// routed nowhere.
func TestTierSeverities_MatchEmit(t *testing.T) {
	want := map[leeway.Tier]string{
		leeway.TierA:    emit.SeverityCritical,
		leeway.TierB:    emit.SeverityWarning,
		leeway.TierC:    emit.SeverityInfo,
		leeway.TierNone: "",
	}
	for tier, sev := range want {
		if got := tier.Severity(); got != sev {
			t.Errorf("Tier %v severity = %q, want emit's %q", tier, got, sev)
		}
	}

	// And every severity a tier can produce is one the sentinel routes. A tier
	// mapping to a plausible-looking string emit does not know is the failure
	// this half catches.
	known := emit.Severities()
	for tier := range want {
		sev := tier.Severity()
		if sev == "" {
			continue
		}
		if !slices.Contains(known, sev) {
			t.Errorf("Tier %v routes to %q, which is not one of emit.Severities() %v", tier, sev, known)
		}
	}
}
