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

package audit

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/policy"
)

const kindPolicyRule = "audit.policy_rule"

// PolicyFlags returns the CLI/MCP flags shared by posture commands (`--policy`,
// `--profile`, `--only`, `--skip`, `--min-severity`, `--set`).
func PolicyFlags() []emit.FlagSpec {
	return []emit.FlagSpec{
		{
			Name:    "policy",
			Type:    emit.FlagString,
			Default: "",
			Help:    "path to a YAML posture policy file",
		},
		{
			Name:    "profile",
			Type:    emit.FlagString,
			Default: "",
			Help:    "built-in posture profile: default|strict|onboarding|gke-autopilot",
		},
		{
			Name:    "only",
			Type:    emit.FlagString,
			Default: "",
			Help:    "comma-separated check kind or kind/reason globs to run exclusively",
		},
		{
			Name:    "skip",
			Type:    emit.FlagString,
			Default: "",
			Help:    "comma-separated check kind or kind/reason globs to skip",
		},
		{
			Name:    "min-severity",
			Type:    emit.FlagString,
			Default: "",
			Help:    "minimum finding severity to emit: info|warning|critical",
		},
		{
			Name:    "set",
			Type:    emit.FlagString,
			Default: "",
			Help:    "comma-separated check parameter overrides (<kind>.<param>=<val>)",
		},
	}
}

// LoadPolicy constructs and validates the effective *policy.Policy from a
// command's FlagValues. Any validation error is returned via emit.UsageErrorf
// (exit 2).
func LoadPolicy(flags emit.FlagValues) (*policy.Policy, error) {
	policyPath := flags.String("policy")
	profileName := flags.String("profile")

	var (
		pol *policy.Policy
		err error
	)
	if policyPath != "" {
		pol, err = policy.Load(policyPath)
		if err != nil {
			return nil, emit.UsageErrorf("%v", err)
		}
		if profileName != "" {
			if err := pol.SetProfile(profileName); err != nil {
				return nil, emit.UsageErrorf("%v", err)
			}
		}
	} else {
		pol, err = policy.ForProfile(profileName)
		if err != nil {
			return nil, emit.UsageErrorf("%v", err)
		}
	}

	var only, skip, sets []string
	if v := flags.String("only"); v != "" {
		only = []string{v}
	}
	if v := flags.String("skip"); v != "" {
		skip = []string{v}
	}
	if v := flags.String("set"); v != "" {
		sets = []string{v}
	}
	if err := pol.ApplyFlags(only, skip, flags.String("min-severity"), sets); err != nil {
		return nil, emit.UsageErrorf("%v", err)
	}
	return pol, nil
}

// bindPolicy loads the effective posture policy from inv.Flags and attaches it
// to inv.Out when the user explicitly supplied a policy/profile/CLI override.
func bindPolicy(inv emit.Invocation) (*policy.Policy, error) {
	pol, err := LoadPolicy(inv.Flags)
	if err != nil {
		return nil, err
	}
	if pol.IsActive() {
		inv.Out.SetPolicy(pol)
	}
	return pol, nil
}

func skipSystemNamespacesFlag() emit.FlagSpec {
	return emit.FlagSpec{
		Name:    "skip-system-namespaces",
		Type:    emit.FlagBool,
		Default: "false",
		Help:    "skip system namespaces matching the active posture policy's system_namespaces patterns",
	}
}

func perWorkloadFlag() emit.FlagSpec {
	return emit.FlagSpec{
		Name:    "per-workload",
		Type:    emit.FlagBool,
		Default: "false",
		Help:    "emit one finding per affected workload instead of one summary finding per holder object",
	}
}

func skippedSystemNamespacesField() checks.OutputField {
	return checks.OutputField{
		Name: "skipped_system_namespaces",
		Doc:  "summary note: distinct system namespaces skipped under `--skip-system-namespaces`",
	}
}

// PolicyCommand returns the `lookout audit policy` command, which dumps the
// effective rule table as `audit.policy_rule` info records.
func PolicyCommand() checks.Command {
	return checks.Command{
		Name:        "audit policy",
		MCPName:     "k8s_audit_policy",
		MCPProfiles: []string{"audit"},
		Summary:     "Dump the effective posture rule table after merging detector defaults, --profile, --policy, and CLI overrides (--only, --skip, --min-severity, --set) so operators and agents can inspect what a scan will enforce before running it.",
		Flags:       PolicyFlags(),
		Kinds: []checks.KindField{
			checks.Kind(kindPolicyRule, "the resolved enablement, effective severity, provenance layer, and threshold parameters for one posture check rule", emit.SeverityInfo),
		},
		Output: []checks.OutputField{
			{Name: "target_kind", Doc: "the posture check kind (`<kind>`) or reason (`<kind>/<reason>`) this rule governs"},
			{Name: "enabled", Doc: "whether findings matching this rule will be emitted (`true` or `false`)"},
			{Name: "effective_severity", Doc: "the severity findings matching this rule will carry after profile and policy overrides"},
			{Name: "default_severity", Doc: "the detector's built-in severity before any profile or policy override"},
			{Name: "source", Doc: "which precedence layer determined this rule (`default`, `profile`, `file`, or `flag`)"},
			{Name: "params", Doc: "comma-separated `<key>=<val>` threshold parameters in effect for this check, when parameterized"},
		},
		Examples: []string{
			"lookout audit policy",
			"lookout audit policy --profile=strict",
			"lookout audit policy --policy=policy.yaml --min-severity=warning",
		},
		Run: runPolicy,
	}
}

func runPolicy(_ context.Context, inv emit.Invocation) (int, error) {
	if inv.Scope.Namespace != "" || inv.Scope.AllNamespaces || !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("audit policy inspects the posture rule table, not objects inside the cluster: --namespace/-A/--workload do not apply")
	}
	pol, err := LoadPolicy(inv.Flags)
	if err != nil {
		return 0, err
	}
	inv.Out.SetPolicy(pol)

	rules := pol.EffectiveRules(knownAuditRuleSpecs())
	for _, r := range rules {
		reason := "PolicyRuleEnabled"
		msg := fmt.Sprintf("%s enabled at %s severity (default %s, source=%s)", r.Target, r.EffectiveSeverity, r.DefaultSeverity, r.Source)
		if !r.Enabled {
			reason = "PolicyRuleDisabled"
			msg = fmt.Sprintf("%s disabled (default %s, source=%s)", r.Target, r.DefaultSeverity, r.Source)
		}
		details := []emit.Field{
			{Key: "target_kind", Value: r.Target},
			{Key: "enabled", Value: strconv.FormatBool(r.Enabled)},
			{Key: "effective_severity", Value: r.EffectiveSeverity},
			{Key: "default_severity", Value: r.DefaultSeverity},
			{Key: "source", Value: r.Source},
		}
		if formatted := formatParams(r.Params); formatted != "" {
			details = append(details, emit.Field{Key: "params", Value: formatted})
		}
		if err := inv.Out.Emit(emit.Finding{
			Kind:        kindPolicyRule,
			Severity:    emit.SeverityInfo,
			Name:        r.Target,
			Reason:      reason,
			Message:     msg,
			Fingerprint: engine.PostureFingerprint(kindPolicyRule, reason, ""),
			Details:     details,
		}); err != nil {
			return 0, err
		}
	}
	return len(rules), nil
}

var defaultAuditRuleSpecs = []policy.RuleSpec{
	{Target: "audit.cluster_admin_binding", DefaultSeverity: emit.SeverityCritical},
	{Target: "audit.crd_conversion_webhook", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.dangerous_capabilities", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.default_sa_automount", DefaultSeverity: emit.SeverityInfo},
	{Target: "audit.deprecated_api", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.exec_impersonate_grant", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.helm_drift", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.host_namespace", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.hostpath_mount", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.image_floating_tag", DefaultSeverity: emit.SeverityInfo, DefaultParams: map[string]string{"floating_tags": "latest"}},
	{Target: "audit.legacy_metadata", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.liveness_preempts_readiness", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.liveness_preempts_readiness/LivenessDeadlineTiesReadiness", DefaultSeverity: emit.SeverityInfo},
	{Target: "audit.liveness_preempts_readiness/LivenessFiresFirst", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.namespace_onboarding_gaps", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.no_limit_range", DefaultSeverity: emit.SeverityInfo},
	{Target: "audit.no_liveness_probe", DefaultSeverity: emit.SeverityInfo},
	{Target: "audit.no_network_policy", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.no_pdb", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.no_probes", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.no_resource_quota", DefaultSeverity: emit.SeverityInfo},
	{Target: "audit.no_resources", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.no_spread", DefaultSeverity: emit.SeverityInfo},
	{Target: "audit.nodepool_cannot_grow", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.pdb_blocks_drain", DefaultSeverity: emit.SeverityWarning, DefaultParams: map[string]string{"min_replicas": "1"}},
	{Target: "audit.permissive_network_policy", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.pod_create_grant", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.podsecurity_gaps", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.privileged_container", DefaultSeverity: emit.SeverityCritical},
	{Target: "audit.secret_access_grant", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.single_replica", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.single_zone_nodepool", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.suspended_cronjob", DefaultSeverity: emit.SeverityInfo, DefaultParams: map[string]string{"suspended_for": "168h"}},
	{Target: "audit.suspended_job", DefaultSeverity: emit.SeverityWarning, DefaultParams: map[string]string{"suspended_for": "24h"}},
	{Target: "audit.unselected_network_policy", DefaultSeverity: emit.SeverityInfo},
	{Target: "audit.version_skew", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.webhook_blast_radius", DefaultSeverity: emit.SeverityWarning},
	{Target: "audit.wildcard_grant", DefaultSeverity: emit.SeverityWarning},
}

func knownAuditRuleSpecs() []policy.RuleSpec {
	byTarget := make(map[string]policy.RuleSpec, len(defaultAuditRuleSpecs))
	for _, spec := range defaultAuditRuleSpecs {
		byTarget[spec.Target] = spec
	}
	unread := checks.UnreadKind().Name
	for _, cmd := range checks.Default().GroupCommands("audit") {
		for _, k := range cmd.Kinds {
			if k.Name == unread || k.Name == kindPolicyRule || !strings.HasPrefix(k.Name, "audit.") {
				continue
			}
			if _, exists := byTarget[k.Name]; !exists {
				defSev := emit.SeverityWarning
				if len(k.Severity) > 0 {
					defSev = k.Severity[0]
				}
				byTarget[k.Name] = policy.RuleSpec{
					Target:          k.Name,
					DefaultSeverity: defSev,
				}
			}
		}
	}
	out := make([]policy.RuleSpec, 0, len(byTarget))
	for _, spec := range byTarget {
		out = append(out, spec)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Target < out[j].Target
	})
	return out
}

func formatParams(params map[string]string) string {
	if len(params) == 0 {
		return ""
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+params[k])
	}
	return strings.Join(parts, ",")
}
