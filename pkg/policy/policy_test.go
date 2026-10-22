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

package policy_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/policy"
)

func TestBuiltinProfiles(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		p, err := policy.ForProfile(policy.ProfileDefault)
		if err != nil {
			t.Fatalf("ForProfile(default): %v", err)
		}
		if p.Name() != "default" {
			t.Fatalf("Name() = %q, want default", p.Name())
		}
		if ok, sev := p.Decide("audit.no_liveness_probe", "NoLivenessProbe", emit.SeverityInfo); !ok || sev != emit.SeverityInfo {
			t.Errorf("Decide(no_liveness_probe) = (%v, %q), want (true, info)", ok, sev)
		}
		if ok, sev := p.Decide("audit.privileged_container", "Privileged", emit.SeverityCritical); !ok || sev != emit.SeverityCritical {
			t.Errorf("Decide(privileged_container) = (%v, %q), want (true, critical)", ok, sev)
		}
	})

	t.Run("strict", func(t *testing.T) {
		p, err := policy.ForProfile(policy.ProfileStrict)
		if err != nil {
			t.Fatalf("ForProfile(strict): %v", err)
		}
		for _, kind := range []string{
			"audit.no_liveness_probe",
			"audit.no_spread",
			"audit.image_floating_tag",
		} {
			ok, sev := p.Decide(kind, "AnyReason", emit.SeverityInfo)
			if !ok || sev != emit.SeverityWarning {
				t.Errorf("strict Decide(%s) = (%v, %q), want (true, warning)", kind, ok, sev)
			}
		}
		// Specific reason promotion on audit.liveness_preempts_readiness.
		ok, sev := p.Decide("audit.liveness_preempts_readiness", "LivenessDeadlineTiesReadiness", emit.SeverityInfo)
		if !ok || sev != emit.SeverityWarning {
			t.Errorf("strict Decide(LivenessDeadlineTiesReadiness) = (%v, %q), want (true, warning)", ok, sev)
		}
		// Other reasons on the same kind keep their default severity.
		ok, sev = p.Decide("audit.liveness_preempts_readiness", "LivenessFasterThanReadiness", emit.SeverityWarning)
		if !ok || sev != emit.SeverityWarning {
			t.Errorf("strict Decide(LivenessFasterThanReadiness) = (%v, %q), want (true, warning)", ok, sev)
		}
	})

	t.Run("onboarding", func(t *testing.T) {
		p, err := policy.ForProfile(policy.ProfileOnboarding)
		if err != nil {
			t.Fatalf("ForProfile(onboarding): %v", err)
		}
		enabledKinds := []string{
			"audit.podsecurity_gaps",
			"audit.default_sa_automount",
			"audit.no_network_policy",
			"audit.no_resource_quota",
			"audit.no_limit_range",
			"audit.namespace_onboarding_gaps",
			"audit.cluster_admin_binding",
			"audit.wildcard_grant",
		}
		for _, k := range enabledKinds {
			if ok, _ := p.Decide(k, "Reason", emit.SeverityWarning); !ok {
				t.Errorf("onboarding should enable %s", k)
			}
		}
		disabledKinds := []string{
			"audit.no_probes",
			"audit.no_pdb",
			"audit.single_replica",
			"audit.helm_drift",
			"audit.deprecated_api",
		}
		for _, k := range disabledKinds {
			if ok, _ := p.Decide(k, "Reason", emit.SeverityWarning); ok {
				t.Errorf("onboarding should disable %s", k)
			}
		}
	})

	t.Run("gke-autopilot", func(t *testing.T) {
		p, err := policy.ForProfile(policy.ProfileGKEAutopilot)
		if err != nil {
			t.Fatalf("ForProfile(gke-autopilot): %v", err)
		}
		autopilotManaged := []string{
			"audit.nodepool_cannot_grow",
			"audit.legacy_metadata",
			"audit.single_zone_nodepool",
			"audit.privileged_container",
			"audit.host_namespace",
			"audit.hostpath_mount",
		}
		for _, k := range autopilotManaged {
			if ok, _ := p.Decide(k, "Reason", emit.SeverityWarning); ok {
				t.Errorf("gke-autopilot should disable %s", k)
			}
		}
		if ok, _ := p.Decide("audit.no_probes", "NoReadinessProbe", emit.SeverityWarning); !ok {
			t.Errorf("gke-autopilot should keep workload check audit.no_probes enabled")
		}
	})
}

func TestPrecedenceAndSpecificity(t *testing.T) {
	yamlDoc := `
name: prod-baseline
profile: strict
min_severity: warning
system_namespaces:
  - platform-*
checks:
  audit.no_liveness_probe:
    enabled: false
  audit.no_spread:
    severity: info
  audit.no_probes:
    severity: warning
  audit.no_probes/NoReadinessProbe:
    severity: critical
  audit.suspended_cronjob:
    severity: warning
    params:
      suspended_for: 168h
  audit.pdb_blocks_drain:
    params:
      min_replicas: 3
  audit.image_floating_tag:
    params:
      floating_tags:
        - latest
        - staging
`
	dir := t.TempDir()
	polPath := filepath.Join(dir, "policy.yaml")
	if err := os.WriteFile(polPath, []byte(yamlDoc), 0o600); err != nil {
		t.Fatalf("write policy.yaml: %v", err)
	}

	p, err := policy.Load(polPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if p.Name() != "prod-baseline" {
		t.Errorf("Name() = %q, want prod-baseline", p.Name())
	}

	// 1. Profile `strict` promoted `audit.no_liveness_probe` to warning, but file disabled it.
	if ok, _ := p.Decide("audit.no_liveness_probe", "NoLivenessProbe", emit.SeverityInfo); ok {
		t.Errorf("file should disable audit.no_liveness_probe over strict profile")
	}

	// 2. Profile `strict` promoted `audit.no_spread` to warning, file demoted it back to info,
	// and file `min_severity: warning` then suppresses it!
	if ok, sev := p.Decide("audit.no_spread", "NoTopologySpread", emit.SeverityInfo); ok || sev != emit.SeverityInfo {
		t.Errorf("Decide(no_spread) = (%v, %q), want (false, info) due to demotion + min_severity", ok, sev)
	}

	// 3. Specificity: `<kind>/<reason>` overrides `<kind>`.
	if ok, sev := p.Decide("audit.no_probes", "NoReadinessProbe", emit.SeverityWarning); !ok || sev != emit.SeverityCritical {
		t.Errorf("Decide(no_probes/NoReadinessProbe) = (%v, %q), want (true, critical)", ok, sev)
	}
	if ok, sev := p.Decide("audit.no_probes", "OtherProbeIssue", emit.SeverityInfo); !ok || sev != emit.SeverityWarning {
		t.Errorf("Decide(no_probes/OtherProbeIssue) = (%v, %q), want (true, warning)", ok, sev)
	}

	// 4. Layer 4 CLI flags override policy file and profile.
	err = p.ApplyFlags(
		[]string{"audit.no_liveness_probe", "audit.no_probes", "audit.suspended_cronjob"},
		[]string{"audit.no_probes/NoReadinessProbe"},
		emit.SeverityCritical,
		[]string{
			"audit.suspended_cronjob.suspended_for=336h",
			"audit.image_floating_tag.floating_tags=latest,dev,canary",
		},
	)
	if err != nil {
		t.Fatalf("ApplyFlags: %v", err)
	}

	// --skip suppresses NoReadinessProbe even though it matched --only audit.no_probes.
	if ok, _ := p.Decide("audit.no_probes", "NoReadinessProbe", emit.SeverityWarning); ok {
		t.Errorf("--skip should suppress audit.no_probes/NoReadinessProbe")
	}
	// --min-severity=critical suppresses warning-severity findings.
	if ok, _ := p.Decide("audit.suspended_cronjob", "SuspendedCronJob", emit.SeverityWarning); ok {
		t.Errorf("--min-severity=critical should suppress warning finding")
	}

	// Parameter precedence: --set > file > default.
	if got := p.DurationParam("audit.suspended_cronjob", "suspended_for", 24*time.Hour); got != 336*time.Hour {
		t.Errorf("DurationParam(suspended_for) = %v, want 336h", got)
	}
	if got := p.IntParam("audit.pdb_blocks_drain", "min_replicas", 1); got != 3 {
		t.Errorf("IntParam(min_replicas) = %d, want 3 (from file)", got)
	}
	wantTags := []string{"latest", "dev", "canary"}
	if got := p.StringSliceParam("audit.image_floating_tag", "floating_tags", []string{"latest"}); !reflect.DeepEqual(got, wantTags) {
		t.Errorf("StringSliceParam(floating_tags) = %v, want %v", got, wantTags)
	}
}

func TestParamFallbacksAndDayDuration(t *testing.T) {
	p, err := policy.LoadBytes([]byte(`
checks:
  audit.suspended_cronjob:
    params:
      suspended_for: 7d
      bad_duration: not-a-duration
      bad_int: nope
`))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if got := p.DurationParam("audit.suspended_cronjob", "suspended_for", time.Hour); got != 7*24*time.Hour {
		t.Errorf("DurationParam(7d) = %v, want 168h", got)
	}
	if got := p.DurationParam("audit.suspended_cronjob", "bad_duration", 12*time.Hour); got != 12*time.Hour {
		t.Errorf("DurationParam(bad_duration) = %v, want fallback 12h", got)
	}
	if got := p.IntParam("audit.suspended_cronjob", "bad_int", 5); got != 5 {
		t.Errorf("IntParam(bad_int) = %d, want fallback 5", got)
	}
	if got := p.StringSliceParam("audit.unknown", "tags", []string{"a", "b"}); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("StringSliceParam(unset) = %v, want [a b]", got)
	}
}

func TestSystemNamespaces(t *testing.T) {
	p, err := policy.LoadBytes([]byte(`
system_namespaces:
  - platform-*
  - internal-ops
`))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	for _, ns := range []string{
		"kube-system",
		"kube-public",
		"kube-node-lease",
		"gke-managed-cim",
		"gmp-system",
		"istio-system",
		"asm-system",
		"config-management-system",
		"platform-ingress",
		"internal-ops",
	} {
		if !p.IsSystemNamespace(ns) {
			t.Errorf("IsSystemNamespace(%q) = false, want true", ns)
		}
	}
	for _, ns := range []string{"default", "payments", "prod-api"} {
		if p.IsSystemNamespace(ns) {
			t.Errorf("IsSystemNamespace(%q) = true, want false", ns)
		}
	}
}

func TestUnavailableAndScanNeverSuppressed(t *testing.T) {
	p, err := policy.ForProfile(policy.ProfileOnboarding)
	if err != nil {
		t.Fatalf("ForProfile: %v", err)
	}
	if err := p.ApplyFlags([]string{"audit.no_probes"}, []string{"*"}, emit.SeverityCritical, nil); err != nil {
		t.Fatalf("ApplyFlags: %v", err)
	}
	if ok, sev := p.Decide("audit.unavailable", "CloudProviderRequired", emit.SeverityInfo); !ok || sev != emit.SeverityInfo {
		t.Errorf("Decide(audit.unavailable) = (%v, %q), want (true, info)", ok, sev)
	}
	if ok, sev := p.Decide("scan.summary", "DomainSummary", emit.SeverityInfo); !ok || sev != emit.SeverityInfo {
		t.Errorf("Decide(scan.summary) = (%v, %q), want (true, info)", ok, sev)
	}
}

func TestEffectiveRules(t *testing.T) {
	p, err := policy.LoadBytes([]byte(`
profile: strict
checks:
  audit.no_pdb:
    enabled: false
`))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if err := p.ApplyFlags(nil, []string{"audit.single_replica"}, "", []string{"audit.suspended_cronjob.suspended_for=72h"}); err != nil {
		t.Fatalf("ApplyFlags: %v", err)
	}

	known := []policy.RuleSpec{
		{Target: "audit.no_liveness_probe", DefaultSeverity: emit.SeverityInfo},
		{Target: "audit.no_pdb", DefaultSeverity: emit.SeverityWarning},
		{Target: "audit.single_replica", DefaultSeverity: emit.SeverityWarning},
		{Target: "audit.no_probes", DefaultSeverity: emit.SeverityWarning},
		{Target: "audit.suspended_cronjob", DefaultSeverity: emit.SeverityInfo, DefaultParams: map[string]string{"suspended_for": "168h"}},
	}

	rules := p.EffectiveRules(known)
	byTarget := map[string]policy.EffectiveRule{}
	for _, r := range rules {
		byTarget[r.Target] = r
	}

	if r := byTarget["audit.no_liveness_probe"]; !r.Enabled || r.EffectiveSeverity != emit.SeverityWarning || r.Source != policy.SourceProfile {
		t.Errorf("no_liveness_probe rule = %+v, want enabled=true eff=warning source=profile", r)
	}
	if r := byTarget["audit.no_pdb"]; r.Enabled || r.Source != policy.SourceFile {
		t.Errorf("no_pdb rule = %+v, want enabled=false source=file", r)
	}
	if r := byTarget["audit.single_replica"]; r.Enabled || r.Source != policy.SourceFlag {
		t.Errorf("single_replica rule = %+v, want enabled=false source=flag", r)
	}
	if r := byTarget["audit.no_probes"]; !r.Enabled || r.Source != policy.SourceDefault {
		t.Errorf("no_probes rule = %+v, want enabled=true source=default", r)
	}
	if r := byTarget["audit.suspended_cronjob"]; r.Params["suspended_for"] != "72h" || r.Source != policy.SourceFlag {
		t.Errorf("suspended_cronjob rule = %+v, want params[suspended_for]=72h source=flag", r)
	}
}

func TestValidationErrors(t *testing.T) {
	if _, err := policy.ForProfile("nonexistent"); err == nil {
		t.Errorf("expected error for unknown profile")
	}

	badYAMLs := []struct {
		name string
		doc  string
		want string
	}{
		{
			name: "unknown top-level field (UnmarshalStrict)",
			doc:  "unknown_field: true\n",
			want: "unknown_field",
		},
		{
			name: "unknown check rule field (UnmarshalStrict)",
			doc:  "checks:\n  audit.no_probes:\n    bogus: 1\n",
			want: "bogus",
		},
		{
			name: "invalid profile in YAML",
			doc:  "profile: bogus\n",
			want: "unknown profile",
		},
		{
			name: "invalid min_severity in YAML",
			doc:  "min_severity: fatal\n",
			want: "invalid severity",
		},
		{
			name: "invalid check severity in YAML",
			doc:  "checks:\n  audit.no_probes:\n    severity: high\n",
			want: "invalid severity",
		},
		{
			name: "invalid check key without domain dot",
			doc:  "checks:\n  noprobes:\n    enabled: false\n",
			want: "invalid check key",
		},
		{
			name: "invalid system_namespaces glob",
			doc:  "system_namespaces:\n  - \"[unclosed\"\n",
			want: "invalid system_namespaces glob",
		},
	}

	for _, tc := range badYAMLs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := policy.LoadBytes([]byte(tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("LoadBytes() err = %v, want substring %q", err, tc.want)
			}
		})
	}

	p, err := policy.ForProfile(policy.ProfileDefault)
	if err != nil {
		t.Fatalf("ForProfile: %v", err)
	}

	if err := p.ApplyFlags([]string{"[bad"}, nil, "", nil); err == nil {
		t.Errorf("expected error for invalid --only glob")
	}
	if err := p.ApplyFlags(nil, []string{"[bad"}, "", nil); err == nil {
		t.Errorf("expected error for invalid --skip glob")
	}
	if err := p.ApplyFlags(nil, nil, "blocker", nil); err == nil {
		t.Errorf("expected error for invalid --min-severity")
	}

	badSets := []string{
		"no_equals_sign",
		"audit.no_probes=",
		"=val",
		"notsufficientdots=val",
		"audit.no_probes=val", // missing .<param>
	}
	for _, s := range badSets {
		if err := p.ApplyFlags(nil, nil, "", []string{s}); err == nil {
			t.Errorf("ApplyFlags(--set %q) succeeded, want error", s)
		}
	}
}
