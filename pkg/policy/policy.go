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

// Package policy evaluates posture check enablement, severity promotion/demotion,
// and per-check threshold parameters from built-in profiles, YAML policy files,
// and CLI flags.
package policy

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// RuleSource identifies which layer determined a check's effective configuration.
const (
	SourceDefault = "default"
	SourceProfile = "profile"
	SourceFile    = "file"
	SourceFlag    = "flag"
)

// Policy configures posture check enablement, severity overrides, and check
// parameters across four precedence layers (lowest to highest):
//  1. Detector defaults
//  2. Built-in profile (--profile or profile: in YAML)
//  3. Policy file (--policy=<file>) checks: entries and min_severity
//  4. CLI flags (--only, --skip, --min-severity, --set)
type Policy struct {
	NameValue       string               `json:"name,omitempty" yaml:"name,omitempty"`
	Profile         string               `json:"profile,omitempty" yaml:"profile,omitempty"`
	MinSeverity     emit.Severity        `json:"min_severity,omitempty" yaml:"min_severity,omitempty"`
	SystemNS        []string             `json:"system_namespaces,omitempty" yaml:"system_namespaces,omitempty"`
	Checks          map[string]CheckRule `json:"checks,omitempty" yaml:"checks,omitempty"`
	flagOnly        []string
	flagSkip        []string
	flagMinSeverity emit.Severity
	flagParams      map[string]map[string]string
	loadedFromFile  bool
	explicitProfile bool
}

// CheckRule configures a single check kind (`<kind>`) or reason (`<kind>/<reason>`).
type CheckRule struct {
	Enabled  *bool             `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Severity emit.Severity     `json:"severity,omitempty" yaml:"severity,omitempty"`
	Params   map[string]string `json:"params,omitempty" yaml:"params,omitempty"`
}

// rawPolicy and rawCheckRule allow YAML numbers/booleans/lists in params: to
// unmarshal cleanly into strings while keeping yaml.UnmarshalStrict enabled.
type rawPolicy struct {
	Name        string                  `json:"name,omitempty" yaml:"name,omitempty"`
	Profile     string                  `json:"profile,omitempty" yaml:"profile,omitempty"`
	MinSeverity emit.Severity           `json:"min_severity,omitempty" yaml:"min_severity,omitempty"`
	SystemNS    []string                `json:"system_namespaces,omitempty" yaml:"system_namespaces,omitempty"`
	Checks      map[string]rawCheckRule `json:"checks,omitempty" yaml:"checks,omitempty"`
}

type rawCheckRule struct {
	Enabled  *bool                 `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Severity emit.Severity         `json:"severity,omitempty" yaml:"severity,omitempty"`
	Params   map[string]paramValue `json:"params,omitempty" yaml:"params,omitempty"`
}

type paramValue string

func (pv *paramValue) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*pv = paramValue(s)
		return nil
	}
	var num json.Number
	if err := json.Unmarshal(b, &num); err == nil {
		*pv = paramValue(num.String())
		return nil
	}
	var boolean bool
	if err := json.Unmarshal(b, &boolean); err == nil {
		*pv = paramValue(strconv.FormatBool(boolean))
		return nil
	}
	var slice []any
	if err := json.Unmarshal(b, &slice); err == nil {
		parts := make([]string, 0, len(slice))
		for _, elem := range slice {
			parts = append(parts, fmt.Sprint(elem))
		}
		*pv = paramValue(strings.Join(parts, ","))
		return nil
	}
	return fmt.Errorf("unsupported param value %s", string(b))
}

// ForProfile constructs a Policy seeded with the named built-in profile.
// An empty name selects "default".
func ForProfile(name string) (*Policy, error) {
	prof, err := lookupProfile(name)
	if err != nil {
		return nil, err
	}
	return &Policy{
		NameValue:       prof.name,
		Profile:         prof.name,
		Checks:          map[string]CheckRule{},
		flagParams:      map[string]map[string]string{},
		explicitProfile: name != "",
	}, nil
}

// Load parses a YAML policy file from disk with strict schema checking.
func Load(filePath string) (*Policy, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read policy file %q: %w", filePath, err)
	}
	p, err := LoadBytes(data)
	if err != nil {
		return nil, fmt.Errorf("policy file %q: %w", filePath, err)
	}
	return p, nil
}

// LoadBytes parses YAML policy bytes with strict schema checking.
func LoadBytes(data []byte) (*Policy, error) {
	var raw rawPolicy
	if err := yaml.UnmarshalStrict(data, &raw); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}
	profName := raw.Profile
	if profName == "" {
		profName = ProfileDefault
	}
	if _, err := lookupProfile(profName); err != nil {
		return nil, err
	}
	if raw.MinSeverity != "" {
		if err := validateSeverity(raw.MinSeverity); err != nil {
			return nil, fmt.Errorf("min_severity: %w", err)
		}
	}
	for pat := range raw.SystemNS {
		if _, err := path.Match(raw.SystemNS[pat], "ns"); err != nil {
			return nil, fmt.Errorf("invalid system_namespaces glob %q: %w", raw.SystemNS[pat], err)
		}
	}
	checks := make(map[string]CheckRule, len(raw.Checks))
	for key, rc := range raw.Checks {
		if err := validateCheckKey(key); err != nil {
			return nil, err
		}
		if rc.Severity != "" {
			if err := validateSeverity(rc.Severity); err != nil {
				return nil, fmt.Errorf("checks[%q].severity: %w", key, err)
			}
		}
		var params map[string]string
		if len(rc.Params) > 0 {
			params = make(map[string]string, len(rc.Params))
			for k, v := range rc.Params {
				params[k] = string(v)
			}
		}
		checks[key] = CheckRule{
			Enabled:  rc.Enabled,
			Severity: rc.Severity,
			Params:   params,
		}
	}
	name := raw.Name
	if name == "" {
		name = profName
	}
	return &Policy{
		NameValue:       name,
		Profile:         profName,
		MinSeverity:     raw.MinSeverity,
		SystemNS:        append([]string(nil), raw.SystemNS...),
		Checks:          checks,
		flagParams:      map[string]map[string]string{},
		loadedFromFile:  true,
		explicitProfile: raw.Profile != "",
	}, nil
}

// SetProfile overrides the base profile on an existing Policy (for example when
// both --policy and --profile are supplied).
func (p *Policy) SetProfile(name string) error {
	prof, err := lookupProfile(name)
	if err != nil {
		return err
	}
	p.Profile = prof.name
	p.explicitProfile = true
	if !p.loadedFromFile || p.NameValue == "" {
		p.NameValue = prof.name
	}
	return nil
}

// Name returns the policy name for summary emission (`policy=<name>`).
func (p *Policy) Name() string {
	if p == nil {
		return ProfileDefault
	}
	if p.NameValue != "" {
		return p.NameValue
	}
	if p.Profile != "" {
		return p.Profile
	}
	return ProfileDefault
}

// IsActive reports whether the policy was loaded from a file, selected via an
// explicit --profile flag, or customized with CLI filter/parameter flags.
func (p *Policy) IsActive() bool {
	if p == nil {
		return false
	}
	return p.loadedFromFile ||
		p.explicitProfile ||
		len(p.flagOnly) > 0 ||
		len(p.flagSkip) > 0 ||
		p.flagMinSeverity != "" ||
		len(p.flagParams) > 0
}

// ApplyFlags layers CLI overrides (--only, --skip, --min-severity, --set) on
// top of the policy.
func (p *Policy) ApplyFlags(only, skip []string, minSeverity emit.Severity, sets []string) error {
	if p == nil {
		return fmt.Errorf("cannot apply flags to nil Policy")
	}
	parsedOnly, err := expandGlobs("--only", only)
	if err != nil {
		return err
	}
	parsedSkip, err := expandGlobs("--skip", skip)
	if err != nil {
		return err
	}
	if minSeverity != "" {
		if err := validateSeverity(minSeverity); err != nil {
			return fmt.Errorf("--min-severity: %w", err)
		}
	}
	if p.flagParams == nil {
		p.flagParams = map[string]map[string]string{}
	}
	for _, rawSet := range sets {
		for _, token := range splitSetAssignments(rawSet) {
			kind, param, val, err := parseSetAssignment(token)
			if err != nil {
				return err
			}
			if p.flagParams[kind] == nil {
				p.flagParams[kind] = map[string]string{}
			}
			p.flagParams[kind][param] = val
		}
	}
	p.flagOnly = append(p.flagOnly, parsedOnly...)
	p.flagSkip = append(p.flagSkip, parsedSkip...)
	if minSeverity != "" {
		p.flagMinSeverity = minSeverity
	}
	return nil
}

// Decide evaluates whether a finding of (kind, reason) with defaultSev should
// be emitted, and returns its effective severity.
//
// Precedence (lowest to highest):
//  1. Detector defaults (enabled=true, sev=defaultSev)
//  2. Built-in profile (profile allowlist + checks: [<kind>, then <kind>/<reason>])
//  3. Policy file (checks: [<kind>, then <kind>/<reason>], then min_severity)
//  4. CLI flags (--only, --skip, --min-severity)
//
// Meta findings (`*.unavailable` and `scan.*`) are never suppressed.
func (p *Policy) Decide(kind, reason string, defaultSev emit.Severity) (enabled bool, sev emit.Severity) {
	if defaultSev == "" {
		defaultSev = emit.SeverityInfo
	}
	if p == nil || isUnsuppressibleKind(kind) {
		return true, defaultSev
	}
	enabled, sev, _ = p.decideDetailed(kind, reason, defaultSev)
	return enabled, sev
}

func (p *Policy) decideDetailed(kind, reason string, defaultSev emit.Severity) (enabled bool, sev emit.Severity, source string) {
	enabled = true
	sev = defaultSev
	source = SourceDefault

	prof, err := lookupProfile(p.Profile)
	if err == nil {
		if prof.onlyAllowlist != nil {
			if prof.onlyAllowlist[kind] {
				enabled = true
				source = SourceProfile
			} else {
				enabled = false
				source = SourceProfile
			}
		}
		if rule, ok := prof.checks[kind]; ok {
			enabled, sev = applyRule(enabled, sev, rule)
			source = SourceProfile
		}
		if reason != "" {
			if rule, ok := prof.checks[kind+"/"+reason]; ok {
				enabled, sev = applyRule(enabled, sev, rule)
				source = SourceProfile
			}
		}
	}

	if p.Checks != nil {
		if rule, ok := p.Checks[kind]; ok {
			enabled, sev = applyRule(enabled, sev, rule)
			source = SourceFile
		}
		if reason != "" {
			if rule, ok := p.Checks[kind+"/"+reason]; ok {
				enabled, sev = applyRule(enabled, sev, rule)
				source = SourceFile
			}
		}
	}

	if p.MinSeverity != "" && severityRank(sev) < severityRank(p.MinSeverity) {
		if enabled {
			source = SourceFile
		}
		enabled = false
	}

	if len(p.flagOnly) > 0 {
		if matchesAnyGlob(p.flagOnly, kind, reason) {
			// If a check was disabled by profile or file (e.g. onboarding),
			// explicit --only re-enables it unless --skip or --min-severity suppresses it.
			if !enabled {
				enabled = true
			}
			source = SourceFlag
		} else {
			enabled = false
			source = SourceFlag
		}
	}

	if len(p.flagSkip) > 0 && matchesAnyGlob(p.flagSkip, kind, reason) {
		enabled = false
		source = SourceFlag
	}

	if p.flagMinSeverity != "" && severityRank(sev) < severityRank(p.flagMinSeverity) {
		enabled = false
		source = SourceFlag
	}

	return enabled, sev, source
}

func applyRule(enabled bool, sev emit.Severity, rule CheckRule) (bool, emit.Severity) {
	if rule.Enabled != nil {
		enabled = *rule.Enabled
	}
	if rule.Severity != "" {
		sev = rule.Severity
	}
	return enabled, sev
}

// DurationParam resolves a duration parameter for kind (`time.ParseDuration`,
// plus `<N>d` day shorthand), falling back to def if unset or invalid.
func (p *Policy) DurationParam(kind, key string, def time.Duration) time.Duration {
	raw, ok := p.rawParam(kind, key)
	if !ok || raw == "" {
		return def
	}
	d, err := parseDuration(raw)
	if err != nil {
		return def
	}
	return d
}

// IntParam resolves an integer parameter for kind, falling back to def if unset
// or invalid.
func (p *Policy) IntParam(kind, key string, def int) int {
	raw, ok := p.rawParam(kind, key)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return def
	}
	return v
}

// StringSliceParam resolves a comma-separated string list parameter for kind,
// falling back to def if unset.
func (p *Policy) StringSliceParam(kind, key string, def []string) []string {
	raw, ok := p.rawParam(kind, key)
	if !ok {
		return def
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func (p *Policy) rawParam(kind, key string) (string, bool) {
	if p == nil {
		return "", false
	}
	if p.flagParams != nil {
		if m, ok := p.flagParams[kind]; ok {
			if v, ok := m[key]; ok {
				return v, true
			}
		}
	}
	if p.Checks != nil {
		if rule, ok := p.Checks[kind]; ok && rule.Params != nil {
			if v, ok := rule.Params[key]; ok {
				return v, true
			}
		}
	}
	if prof, err := lookupProfile(p.Profile); err == nil {
		if rule, ok := prof.checks[kind]; ok && rule.Params != nil {
			if v, ok := rule.Params[key]; ok {
				return v, true
			}
		}
	}
	return "", false
}

// EffectiveParams returns the merged parameter map for kind across profile,
// policy file, and --set flags, along with whether any parameter came from a
// higher-precedence layer.
func (p *Policy) EffectiveParams(kind string) (params map[string]string, paramSource string) {
	if p == nil {
		return nil, ""
	}
	merged := map[string]string{}
	if prof, err := lookupProfile(p.Profile); err == nil {
		if rule, ok := prof.checks[kind]; ok {
			for k, v := range rule.Params {
				merged[k] = v
				paramSource = SourceProfile
			}
		}
	}
	if p.Checks != nil {
		if rule, ok := p.Checks[kind]; ok {
			for k, v := range rule.Params {
				merged[k] = v
				paramSource = SourceFile
			}
		}
	}
	if p.flagParams != nil {
		if m, ok := p.flagParams[kind]; ok {
			for k, v := range m {
				merged[k] = v
				paramSource = SourceFlag
			}
		}
	}
	if len(merged) == 0 {
		return nil, ""
	}
	return merged, paramSource
}

// SystemNamespaces returns the deduplicated list of system namespace patterns
// (built-in defaults plus any `system_namespaces:` entries from the policy).
func (p *Policy) SystemNamespaces() []string {
	seen := make(map[string]bool, len(DefaultSystemNamespaces))
	out := make([]string, 0, len(DefaultSystemNamespaces))
	for _, ns := range DefaultSystemNamespaces {
		if !seen[ns] {
			seen[ns] = true
			out = append(out, ns)
		}
	}
	if p != nil {
		for _, ns := range p.SystemNS {
			trimmed := strings.TrimSpace(ns)
			if trimmed != "" && !seen[trimmed] {
				seen[trimmed] = true
				out = append(out, trimmed)
			}
		}
	}
	return out
}

// IsSystemNamespace reports whether ns matches any built-in or policy-configured
// system namespace pattern (exact name or `path.Match` glob such as `gke-*`).
func (p *Policy) IsSystemNamespace(ns string) bool {
	for _, pattern := range p.SystemNamespaces() {
		if pattern == ns {
			return true
		}
		if ok, err := path.Match(pattern, ns); err == nil && ok {
			return true
		}
	}
	return false
}

// RuleSpec describes a registered posture check kind (or kind/reason) for
// `EffectiveRules` introspection (`lookout audit policy`).
type RuleSpec struct {
	Target          string
	DefaultSeverity emit.Severity
	DefaultParams   map[string]string
}

// EffectiveRule is the resolved state of a single check rule under a Policy.
type EffectiveRule struct {
	Target            string
	Enabled           bool
	EffectiveSeverity emit.Severity
	DefaultSeverity   emit.Severity
	Source            string
	Params            map[string]string
}

// EffectiveRules resolves the effective enablement, severity, source, and
// parameters for every known RuleSpec, plus any additional `<kind>` or
// `<kind>/<reason>` rules referenced by the active profile, policy file, or
// `--set` flags. Results are sorted deterministically by Target.
func (p *Policy) EffectiveRules(known []RuleSpec) []EffectiveRule {
	byTarget := make(map[string]RuleSpec, len(known))
	for _, spec := range known {
		if spec.Target == "" {
			continue
		}
		sev := spec.DefaultSeverity
		if sev == "" {
			sev = emit.SeverityWarning
		}
		byTarget[spec.Target] = RuleSpec{
			Target:          spec.Target,
			DefaultSeverity: sev,
			DefaultParams:   spec.DefaultParams,
		}
	}

	addTarget := func(target string) {
		if target == "" {
			return
		}
		if _, exists := byTarget[target]; exists {
			return
		}
		baseKind, _ := splitKindReason(target)
		defSev := emit.SeverityWarning
		if baseSpec, ok := byTarget[baseKind]; ok && baseSpec.DefaultSeverity != "" {
			defSev = baseSpec.DefaultSeverity
		} else if strings.HasSuffix(target, "/LivenessDeadlineTiesReadiness") {
			defSev = emit.SeverityInfo
		}
		byTarget[target] = RuleSpec{
			Target:          target,
			DefaultSeverity: defSev,
		}
	}

	if p != nil {
		if prof, err := lookupProfile(p.Profile); err == nil {
			for k := range prof.checks {
				addTarget(k)
			}
			for k := range prof.onlyAllowlist {
				addTarget(k)
			}
		}
		for k := range p.Checks {
			addTarget(k)
		}
		for k := range p.flagParams {
			addTarget(k)
		}
	}

	targets := make([]string, 0, len(byTarget))
	for t := range byTarget {
		targets = append(targets, t)
	}
	sort.Strings(targets)

	out := make([]EffectiveRule, 0, len(targets))
	for _, target := range targets {
		spec := byTarget[target]
		kind, reason := splitKindReason(target)
		enabled, effSev, source := true, spec.DefaultSeverity, SourceDefault
		if p != nil {
			enabled, effSev, source = p.decideDetailed(kind, reason, spec.DefaultSeverity)
		}
		params := map[string]string{}
		for k, v := range spec.DefaultParams {
			params[k] = v
		}
		if p != nil {
			effParams, paramSource := p.EffectiveParams(target)
			for k, v := range effParams {
				params[k] = v
			}
			if paramSource != "" && sourceRank(paramSource) > sourceRank(source) {
				source = paramSource
			}
		}
		if len(params) == 0 {
			params = nil
		}
		out = append(out, EffectiveRule{
			Target:            target,
			Enabled:           enabled,
			EffectiveSeverity: effSev,
			DefaultSeverity:   spec.DefaultSeverity,
			Source:            source,
			Params:            params,
		})
	}
	return out
}

func sourceRank(s string) int {
	switch s {
	case SourceFlag:
		return 4
	case SourceFile:
		return 3
	case SourceProfile:
		return 2
	default:
		return 1
	}
}

func severityRank(s emit.Severity) int {
	switch s {
	case emit.SeverityCritical:
		return 3
	case emit.SeverityWarning:
		return 2
	case emit.SeverityInfo:
		return 1
	default:
		return 0
	}
}

func validateSeverity(s emit.Severity) error {
	switch s {
	case emit.SeverityCritical, emit.SeverityWarning, emit.SeverityInfo:
		return nil
	default:
		return fmt.Errorf("invalid severity %q (expected critical, warning, or info)", s)
	}
}

func validateCheckKey(key string) error {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return fmt.Errorf("empty check key in policy")
	}
	kind, reason := splitKindReason(trimmed)
	if !strings.Contains(kind, ".") {
		return fmt.Errorf("invalid check key %q (expected <kind> or <kind>/<reason>)", key)
	}
	if strings.Contains(trimmed, "/") && reason == "" {
		return fmt.Errorf("invalid check key %q (empty reason after '/')", key)
	}
	return nil
}

func splitKindReason(target string) (kind, reason string) {
	if idx := strings.IndexByte(target, '/'); idx >= 0 {
		return target[:idx], target[idx+1:]
	}
	return target, ""
}

func expandGlobs(flagName string, raw []string) ([]string, error) {
	var out []string
	for _, entry := range raw {
		for _, part := range strings.Split(entry, ",") {
			pat := strings.TrimSpace(part)
			if pat == "" {
				continue
			}
			if _, err := path.Match(pat, "audit.example"); err != nil {
				return nil, fmt.Errorf("%s: invalid glob %q: %w", flagName, pat, err)
			}
			out = append(out, pat)
		}
	}
	return out, nil
}

func matchesAnyGlob(patterns []string, kind, reason string) bool {
	full := kind
	if reason != "" {
		full = kind + "/" + reason
	}
	for _, pat := range patterns {
		if ok, err := path.Match(pat, kind); err == nil && ok {
			return true
		}
		if reason != "" {
			if ok, err := path.Match(pat, full); err == nil && ok {
				return true
			}
		}
	}
	return false
}

// splitSetAssignments splits a `--set` argument into individual `<kind>.<param>=<val>`
// tokens, preserving comma-separated list values (e.g. `floating_tags=latest,dev`)
// while supporting multiple assignments (`a.b.p=1,c.d.q=2`).
func splitSetAssignments(raw string) []string {
	parts := strings.Split(raw, ",")
	var out []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, "=") || len(out) == 0 {
			out = append(out, trimmed)
		} else {
			out[len(out)-1] = out[len(out)-1] + "," + trimmed
		}
	}
	return out
}

func parseSetAssignment(token string) (kind, param, val string, err error) {
	eq := strings.IndexByte(token, '=')
	if eq <= 0 || eq == len(token)-1 {
		return "", "", "", fmt.Errorf("--set %q: expected <kind>.<param>=<val>", token)
	}
	lhs := strings.TrimSpace(token[:eq])
	val = strings.TrimSpace(token[eq+1:])
	if val == "" {
		return "", "", "", fmt.Errorf("--set %q: empty value (expected <kind>.<param>=<val>)", token)
	}
	lastDot := strings.LastIndexByte(lhs, '.')
	if lastDot <= 0 || lastDot == len(lhs)-1 {
		return "", "", "", fmt.Errorf("--set %q: expected <kind>.<param>=<val>", token)
	}
	kind = lhs[:lastDot]
	param = lhs[lastDot+1:]
	if !strings.Contains(kind, ".") {
		return "", "", "", fmt.Errorf("--set %q: kind %q must be <domain>.<name> (expected <kind>.<param>=<val>)", token, kind)
	}
	return kind, param, val, nil
}

func parseDuration(raw string) (time.Duration, error) {
	s := strings.TrimSpace(raw)
	if strings.HasSuffix(s, "d") {
		daysStr := strings.TrimSuffix(s, "d")
		days, err := strconv.ParseFloat(daysStr, 64)
		if err == nil {
			return time.Duration(days * float64(24*time.Hour)), nil
		}
	}
	return time.ParseDuration(s)
}

func isUnsuppressibleKind(kind string) bool {
	return strings.HasSuffix(kind, ".unavailable") || strings.HasPrefix(kind, "scan.") || kind == "audit.policy_rule"
}
