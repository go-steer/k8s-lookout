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

package state_test

import (
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// TestStateEdgesDetectionBoundary_PositiveAndNegative verifies positive and
// negative detection boundaries for `state edges` without brittle prose matching (#654).
func TestStateEdgesDetectionBoundary_PositiveAndNegative(t *testing.T) {
	clean := healthy(t)
	negOut := checktest.AssertEnvelopeContract(t, testCommand(clean.objects()...), "--workload=Deployment/prod/api")
	checktest.AssertNoFinding(t, negOut, checktest.FindingMatch{})

	broken := healthy(t)
	broken.cmApp = nil // remove referenced ConfigMap to trigger edge.missing_ref
	posOut := checktest.AssertEnvelopeContract(t, testCommand(broken.objects()...), "--workload=Deployment/prod/api")
	checktest.AssertHasFinding(t, posOut, checktest.FindingMatch{
		Kind:         "edge.missing_ref",
		Namespace:    "prod",
		KindOfObject: "ConfigMap",
		Name:         "app-config",
		Severity:     emit.SeverityCritical,
	})
}
