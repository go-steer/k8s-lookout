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

package watch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// alertExcludeReloadInterval is how often the sentinel re-reads
// --alert-excludes without a pod restart (#836). SIGHUP triggers an
// immediate reload between ticks.
const alertExcludeReloadInterval = time.Minute

// alertExcludeFile is the strict on-disk YAML envelope for
// --alert-excludes (#836). A required top-level `excludes:` wrapper
// aligns with pkg/exempt (#234) so a truncated or empty file cannot
// silently decode as an empty rule set.
type alertExcludeFile struct {
	Excludes *[]AlertExcludeRule `json:"excludes"`
}

// AlertExcludeRule declares one alert-exclude rule that downgrades
// matching signals to SeverityInfo (RouteStore) without dropping the
// object or namespace from the watch (#836).
//
// Matching is by signal fields only — no label selectors — and every
// non-empty field is AND-ed. Involved-object Kind (distinct from
// --severity's signal kind) PLUS at least one of Namespace, Reason, or
// Name is required so a rule cannot act as a cluster-wide off switch.
type AlertExcludeRule struct {
	// Kind is the involved Kubernetes object kind (e.g. "Pod",
	// "Deployment"), matched case-insensitively. Required; wildcards
	// forbidden.
	Kind string `json:"kind"`
	// Namespace narrows to an exact namespace.
	Namespace string `json:"namespace,omitempty"`
	// Reason matches either the signal's wire Event.Reason or its
	// message-aware canonical reason (e.g. CrashLoopBackOff matches
	// kubelet's BackOff container-restart events), case-insensitively.
	// Autoscaler verdict reasons (TriggeredScaleUp, NotTriggerScaleUp,
	// FailedScaleUp) are refused at load time so a rule cannot blind
	// the FailedScheduling verdict memo.
	Reason string `json:"reason,omitempty"`
	// Name matches the involved object name either exactly or by a
	// trailing "*" prefix (e.g. "loadgen-*"). Bare "*" and mid-string
	// "*" are rejected at load time.
	Name string `json:"name,omitempty"`
	// Cluster narrows to an exact cluster name (multi-cluster mode).
	Cluster string `json:"cluster,omitempty"`
	// Project narrows to an exact cloud project/account (multi-cluster mode).
	Project string `json:"project,omitempty"`
	// Location narrows to an exact zone or region (matches Signal.Zone
	// or Signal.Region).
	Location string `json:"location,omitempty"`

	// namePrefix is populated at parse time when Name ends with "*".
	namePrefix string
}

// String renders the rule with every value quoted for safe, scannable
// startup/reload audit log lines (gosec G706 safe).
func (r AlertExcludeRule) String() string {
	parts := []string{fmt.Sprintf("kind=%q", r.Kind)}
	if r.Namespace != "" {
		parts = append(parts, fmt.Sprintf("namespace=%q", r.Namespace))
	}
	if r.Reason != "" {
		parts = append(parts, fmt.Sprintf("reason=%q", r.Reason))
	}
	if r.Name != "" {
		parts = append(parts, fmt.Sprintf("name=%q", r.Name))
	}
	if r.Cluster != "" {
		parts = append(parts, fmt.Sprintf("cluster=%q", r.Cluster))
	}
	if r.Project != "" {
		parts = append(parts, fmt.Sprintf("project=%q", r.Project))
	}
	if r.Location != "" {
		parts = append(parts, fmt.Sprintf("location=%q", r.Location))
	}
	return strings.Join(parts, " ")
}

// Match reports whether sig satisfies every non-empty field of r.
func (r AlertExcludeRule) Match(sig engine.Signal) bool {
	if !strings.EqualFold(r.Kind, sig.KindOfObject) {
		return false
	}
	if r.Namespace != "" && sig.Namespace != r.Namespace {
		return false
	}
	if r.Reason != "" {
		canonical := engine.CanonicalReasonForEvent(sig.Key.Reason, sig.Message)
		if !strings.EqualFold(r.Reason, sig.Key.Reason) && !strings.EqualFold(r.Reason, canonical) {
			return false
		}
	}
	if r.Name != "" {
		if r.namePrefix != "" {
			if !strings.HasPrefix(sig.Name, r.namePrefix) {
				return false
			}
		} else if sig.Name != r.Name {
			return false
		}
	}
	if r.Cluster != "" && sig.Cluster != r.Cluster {
		return false
	}
	if r.Project != "" && sig.Project != r.Project {
		return false
	}
	if r.Location != "" && sig.Zone != r.Location && sig.Region != r.Location {
		return false
	}
	return true
}

func isAutoscalerVerdictReason(reason string) bool {
	return strings.EqualFold(reason, "TriggeredScaleUp") ||
		strings.EqualFold(reason, "NotTriggerScaleUp") ||
		strings.EqualFold(reason, "FailedScaleUp")
}

// parseAlertExcludes decodes and strictly validates an --alert-excludes
// YAML payload. Any schema violation, unknown key, missing wrapper,
// kind-only rule, invalid wildcard, or autoscaler verdict reason fails
// the entire load rather than silently dropping a rule.
func parseAlertExcludes(raw []byte) ([]AlertExcludeRule, error) {
	var f alertExcludeFile
	if err := yaml.UnmarshalStrict(raw, &f); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}
	if f.Excludes == nil {
		return nil, errors.New("missing required top-level \"excludes:\" list (use \"excludes: []\" for an empty rule set)")
	}
	rules := make([]AlertExcludeRule, 0, len(*f.Excludes))
	for i, r := range *f.Excludes {
		r.Kind = strings.TrimSpace(r.Kind)
		r.Namespace = strings.TrimSpace(r.Namespace)
		r.Reason = strings.TrimSpace(r.Reason)
		r.Name = strings.TrimSpace(r.Name)
		r.Cluster = strings.TrimSpace(r.Cluster)
		r.Project = strings.TrimSpace(r.Project)
		r.Location = strings.TrimSpace(r.Location)

		if err := validateAlertExcludeRule(&r); err != nil {
			return nil, fmt.Errorf("rule %d: %w", i+1, err)
		}
		rules = append(rules, r)
	}
	return rules, nil
}

func validateAlertExcludeRule(r *AlertExcludeRule) error {
	if r.Kind == "" {
		return errors.New("kind is required (involved Kubernetes object kind, e.g. Pod)")
	}
	if strings.Contains(r.Kind, "*") {
		return fmt.Errorf("kind %q must not contain '*' wildcards", r.Kind)
	}
	if r.Namespace == "" && r.Reason == "" && r.Name == "" {
		return fmt.Errorf("kind %q requires at least one narrowing matcher (namespace, reason, or name); kind-only rules are rejected", r.Kind)
	}
	for _, pair := range []struct {
		field string
		val   string
	}{
		{"namespace", r.Namespace},
		{"reason", r.Reason},
		{"cluster", r.Cluster},
		{"project", r.Project},
		{"location", r.Location},
	} {
		if strings.Contains(pair.val, "*") {
			return fmt.Errorf("%s %q must not contain '*' (wildcards are only supported as a trailing '*' prefix on name)", pair.field, pair.val)
		}
	}
	if r.Reason != "" && isAutoscalerVerdictReason(r.Reason) {
		return fmt.Errorf("reason %q is an autoscaler verdict reason and cannot be excluded (required by the FailedScheduling verdict gate)", r.Reason)
	}
	if r.Name != "" {
		if r.Name == "*" {
			return errors.New("name \"*\" is forbidden; provide an exact name or a non-empty trailing '*' prefix (e.g. \"loadgen-*\")")
		}
		if idx := strings.IndexByte(r.Name, '*'); idx >= 0 {
			if idx != len(r.Name)-1 {
				return fmt.Errorf("name %q: '*' wildcard is only supported as a single trailing prefix (e.g. \"loadgen-*\")", r.Name)
			}
			r.namePrefix = r.Name[:len(r.Name)-1]
		}
	}
	return nil
}

func loadAlertExcludesFile(path string) ([]AlertExcludeRule, []byte, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: operator-configured --alert-excludes path
	if err != nil {
		return nil, nil, fmt.Errorf("read %q: %w", path, err)
	}
	rules, err := parseAlertExcludes(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("%q: %w", path, err)
	}
	return rules, raw, nil
}

// alertExcludeStore holds the live, hot-reloadable --alert-excludes
// rules for a dispatcher. Safe for concurrent Match and Reload calls.
type alertExcludeStore struct {
	path    string
	metrics *metrics
	logf    func(string, ...any)

	mu         sync.RWMutex
	rules      []AlertExcludeRule
	lastRaw    []byte
	hasLastRaw bool
	lastErr    string
}

func newAlertExcludeStore(path string, initial []AlertExcludeRule, raw []byte, m *metrics, logf func(string, ...any)) *alertExcludeStore {
	if logf == nil {
		logf = log.Printf
	}
	s := &alertExcludeStore{
		path:       path,
		metrics:    m,
		logf:       logf,
		rules:      append([]AlertExcludeRule(nil), initial...),
		lastRaw:    append([]byte(nil), raw...),
		hasLastRaw: raw != nil,
	}
	if m != nil {
		m.alertExcludeRules.Set(float64(len(s.rules)))
		m.alertExcludeRefused.Set(0)
	}
	s.logActiveLocked("loaded")
	return s
}

func (s *alertExcludeStore) logActiveLocked(verb string) {
	s.logf("alert-excludes: %s %d rule(s) from %q (matching signals downgrade to severity=info)", verb, len(s.rules), s.path)
	for i, r := range s.rules {
		s.logf("alert-excludes: rule [%d/%d] %s", i+1, len(s.rules), r.String())
	}
}

// Match returns the first matching rule and true if sig is excluded.
// Nil-safe so a dispatcher without --alert-excludes incurs no setup.
func (s *alertExcludeStore) Match(sig engine.Signal) (AlertExcludeRule, bool) {
	if s == nil {
		return AlertExcludeRule{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.rules {
		if r.Match(sig) {
			return r, true
		}
	}
	return AlertExcludeRule{}, false
}

// Len returns the number of active rules.
func (s *alertExcludeStore) Len() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rules)
}

// Reload re-reads and re-validates the configured --alert-excludes file.
// On read or parse failure, the previously loaded valid rules remain in
// force, lookout_watch_alert_exclude_refused is set to 1, and a loud
// diagnostic is logged. On success, rules are swapped atomically,
// refused resets to 0, and every active rule is logged on its own line
// whenever the file changed, recovered from an error, or forceAudit is
// true (e.g. explicit SIGHUP).
func (s *alertExcludeStore) Reload(forceAudit bool) error {
	next, raw, err := loadAlertExcludesFile(s.path)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		if s.metrics != nil {
			s.metrics.alertExcludeRefused.Set(1)
		}
		errText := err.Error()
		if forceAudit || errText != s.lastErr {
			s.lastErr = errText
			s.logf("alert-excludes: reload of %q REFUSED (%v) — keeping %d previously loaded rule(s) in force; lookout_watch_alert_exclude_refused=1",
				s.path, err, len(s.rules))
		}
		return err
	}

	changed := !s.hasLastRaw || !bytes.Equal(s.lastRaw, raw)
	recovered := s.lastErr != ""
	s.rules = next
	s.lastRaw = append([]byte(nil), raw...)
	s.hasLastRaw = true
	s.lastErr = ""
	if s.metrics != nil {
		s.metrics.alertExcludeRules.Set(float64(len(next)))
		s.metrics.alertExcludeRefused.Set(0)
	}
	if changed || recovered || forceAudit {
		s.logActiveLocked("reloaded")
	}
	return nil
}

// RunReloader polls s.path every interval and reloads immediately on any
// signal received from sighup. Returns when ctx is cancelled.
func (s *alertExcludeStore) RunReloader(ctx context.Context, interval time.Duration, sighup <-chan os.Signal) {
	if interval <= 0 {
		interval = alertExcludeReloadInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.Reload(false)
		case _, ok := <-sighup:
			if !ok {
				sighup = nil
				continue
			}
			_ = s.Reload(true)
		}
	}
}

func startAlertExcludeReloader(ctx context.Context, s *alertExcludeStore) {
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	go func() {
		defer signal.Stop(sighup)
		s.RunReloader(ctx, alertExcludeReloadInterval, sighup)
	}()
}
