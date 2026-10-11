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

package delta

import (
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// TestDeltaDetectionBoundary_PositiveAndNegative verifies positive and negative
// detection boundaries using AssertEnvelopeContract + structured matchers (#654):
//   - negative boundary: healthy objects across all delta classes emit zero findings;
//   - positive boundary: unhealthy objects emit exact finding kinds/reasons without
//     coupling test assertions to prose message text.
func TestDeltaDetectionBoundary_PositiveAndNegative(t *testing.T) {
	negativeCmd := testCommand(
		healthyPod("prod", "web-0"),
		healthyDeployment("prod", "web", 1),
		systemDeployment("coredns", map[string]string{"k8s-app": "kube-dns"}, 2, 2),
		healthyNode("node-0"),
		pdb("prod", "web-pdb", 1, 3, 2, 3),
		quota("prod", "compute", map[string][2]string{"pods": {"10", "2"}}),
		healthyCronJob("prod", "backup"),
		healthyHPA("prod", "web"),
	)
	negOut := checktest.AssertEnvelopeContract(t, negativeCmd)
	checktest.AssertNoFinding(t, negOut, checktest.FindingMatch{})

	positiveCmd := testCommand(
		healthyPod("prod", "web-0"),
		crashloopPod("prod", "broken-api"),
	)
	posOut := checktest.AssertEnvelopeContract(t, positiveCmd)
	checktest.AssertHasFinding(t, posOut, checktest.FindingMatch{
		Kind:         "pod.crashloop",
		Namespace:    "prod",
		KindOfObject: "Pod",
		Name:         "broken-api",
		Severity:     emit.SeverityCritical,
		Reason:       "CrashLoopBackOff",
	})
	checktest.AssertNoFinding(t, posOut, checktest.FindingMatch{Name: "web-0"})
}
