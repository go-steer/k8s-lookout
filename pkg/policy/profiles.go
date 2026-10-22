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

package policy

import (
	"fmt"
	"sort"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// Built-in profile names.
const (
	ProfileDefault      = "default"
	ProfileStrict       = "strict"
	ProfileOnboarding   = "onboarding"
	ProfileGKEAutopilot = "gke-autopilot"
)

// DefaultSystemNamespaces is the baseline set of system namespace patterns
// recognized by posture checks before any policy file additions.
var DefaultSystemNamespaces = []string{
	"kube-system",
	"kube-public",
	"kube-node-lease",
	"gke-*",
	"gmp-*",
	"istio-system",
	"asm-system",
	"config-management-system",
}

type profileSpec struct {
	name          string
	onlyAllowlist map[string]bool
	checks        map[string]CheckRule
}

func boolPtr(b bool) *bool { return &b }

var builtinProfiles = map[string]profileSpec{
	ProfileDefault: {
		name:   ProfileDefault,
		checks: map[string]CheckRule{},
	},
	ProfileStrict: {
		name: ProfileStrict,
		checks: map[string]CheckRule{
			"audit.no_liveness_probe": {
				Severity: emit.SeverityWarning,
			},
			"audit.no_spread": {
				Severity: emit.SeverityWarning,
			},
			"audit.image_floating_tag": {
				Severity: emit.SeverityWarning,
			},
			"audit.liveness_preempts_readiness/LivenessDeadlineTiesReadiness": {
				Severity: emit.SeverityWarning,
			},
			"audit.liveness_preempts_readiness/LivenessFiresFirst": {
				Severity: emit.SeverityWarning,
			},
		},
	},
	ProfileOnboarding: {
		name: ProfileOnboarding,
		onlyAllowlist: map[string]bool{
			"audit.podsecurity_gaps":          true,
			"audit.default_sa_automount":      true,
			"audit.no_network_policy":         true,
			"audit.netpol_missing":            true,
			"audit.no_resource_quota":         true,
			"audit.no_limit_range":            true,
			"audit.namespace_onboarding_gaps": true,
			"audit.cluster_admin_binding":     true,
			"audit.wildcard_grant":            true,
			"audit.wildcard_rbac":             true,
		},
		checks: map[string]CheckRule{
			"audit.podsecurity_gaps":          {Enabled: boolPtr(true)},
			"audit.default_sa_automount":      {Enabled: boolPtr(true)},
			"audit.no_network_policy":         {Enabled: boolPtr(true)},
			"audit.netpol_missing":            {Enabled: boolPtr(true)},
			"audit.no_resource_quota":         {Enabled: boolPtr(true)},
			"audit.no_limit_range":            {Enabled: boolPtr(true)},
			"audit.namespace_onboarding_gaps": {Enabled: boolPtr(true)},
			"audit.cluster_admin_binding":     {Enabled: boolPtr(true)},
			"audit.wildcard_grant":            {Enabled: boolPtr(true)},
			"audit.wildcard_rbac":             {Enabled: boolPtr(true)},
		},
	},
	ProfileGKEAutopilot: {
		name: ProfileGKEAutopilot,
		checks: map[string]CheckRule{
			"audit.nodepool_cannot_grow": {Enabled: boolPtr(false)},
			"audit.legacy_metadata":      {Enabled: boolPtr(false)},
			"audit.single_zone_nodepool": {Enabled: boolPtr(false)},
			"audit.privileged_container": {Enabled: boolPtr(false)},
			"audit.host_namespace":       {Enabled: boolPtr(false)},
			"audit.hostpath_mount":       {Enabled: boolPtr(false)},
		},
	},
}

// ProfileNames returns the sorted list of built-in profile names.
func ProfileNames() []string {
	out := make([]string, 0, len(builtinProfiles))
	for k := range builtinProfiles {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func lookupProfile(name string) (profileSpec, error) {
	if name == "" {
		name = ProfileDefault
	}
	p, ok := builtinProfiles[name]
	if !ok {
		return profileSpec{}, fmt.Errorf("unknown profile %q (valid profiles: %v)", name, ProfileNames())
	}
	return p, nil
}
