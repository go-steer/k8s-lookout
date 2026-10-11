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

package cloudcheck_test

import (
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// TestCloudQuotaDetectionBoundary_PositiveAndNegative verifies positive and
// negative detection boundaries for `cloud quota` (#654):
//   - negative boundary: below-threshold quotas emit zero findings (`findings=0`);
//   - positive boundary: exhausted quota emits critical QuotaExhausted finding.
func TestCloudQuotaDetectionBoundary_PositiveAndNegative(t *testing.T) {
	healthyQuotas := &fakeQuota{quotas: []cloud.QuotaUsage{
		{Name: "CPUS", Scope: "us-central1", Usage: 120, Limit: 600},
		{Name: "NETWORKS", Scope: "global", Usage: 2, Limit: 15},
	}}
	negOut := checktest.AssertEnvelopeContract(t, cloudcheck.QuotaCommand(testDeps(quotaProvider{Provider: cloud.NoProvider, api: healthyQuotas})))
	checktest.AssertNoFinding(t, negOut, checktest.FindingMatch{})

	exhaustedQuotas := &fakeQuota{quotas: []cloud.QuotaUsage{
		{Name: "CPUS", Scope: "us-central1", Usage: 120, Limit: 600},
		{Name: "IN_USE_ADDRESSES", Scope: "us-central1", Usage: 16, Limit: 16},
	}}
	posOut := checktest.AssertEnvelopeContract(t, cloudcheck.QuotaCommand(testDeps(quotaProvider{Provider: cloud.NoProvider, api: exhaustedQuotas})))
	checktest.AssertHasFinding(t, posOut, checktest.FindingMatch{
		Kind:         "quota.pressure",
		KindOfObject: "Quota",
		Name:         "IN_USE_ADDRESSES",
		Severity:     emit.SeverityCritical,
		Reason:       "QuotaExhausted",
	})
	checktest.AssertNoFinding(t, posOut, checktest.FindingMatch{Name: "CPUS"})
}
