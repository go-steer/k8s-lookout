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
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// grantReviewer is a fake SSAR reviewer driven by a predicate:
// allow(req) == true grants the requirement.
type grantReviewer struct {
	allow func(sources.Requirement) bool
}

func (r grantReviewer) Allowed(_ context.Context, req sources.Requirement) (bool, error) {
	return r.allow(req), nil
}

// erroringReviewer fails every probe — the "could not verify" case.
type erroringReviewer struct{}

func (erroringReviewer) Allowed(context.Context, sources.Requirement) (bool, error) {
	return false, errors.New("apiserver unreachable")
}

func allowAll() grantReviewer {
	return grantReviewer{allow: func(sources.Requirement) bool { return true }}
}

// namespaceTier mimics a Role-only deployment: events readable,
// everything cluster-scoped (nodes, replicasets, cluster-wide pods,
// metrics API, admission webhooks, the kube-system ConfigMap) denied.
func namespaceTier() grantReviewer {
	return grantReviewer{allow: func(req sources.Requirement) bool {
		return req.Group == "" && req.Resource == "events"
	}}
}

func metricsPresent() error { return nil }
func metricsAbsent() error  { return errors.New("the server could not find the requested resource") }

func autoFlags(t *testing.T, args ...string) *flags {
	t.Helper()
	f, err := parseFlags(append([]string{"--dry-run"}, args...))
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	return f
}

// TestResolveSourcesAuto_FullGrants: every grant present and the
// metrics API served → all seven portable sources enabled, quota and
// token-burn nowhere in sight.
func TestResolveSourcesAuto_FullGrants(t *testing.T) {
	t.Parallel()
	f := autoFlags(t)
	res, err := resolveSourcesAuto(context.Background(), f, fake.NewSimpleClientset(), allowAll(), metricsPresent)
	if err != nil {
		t.Fatalf("resolveSourcesAuto: %v", err)
	}
	if !slices.Equal(res.enabled, autoSourceNames) {
		t.Errorf("enabled = %v, want the full portable set %v", res.enabled, autoSourceNames)
	}
	for _, off := range []string{"quota", "token-burn"} {
		if slices.Contains(res.enabled, off) {
			t.Errorf("%s must never be auto-enabled", off)
		}
	}
}

// TestResolveSourcesAuto_SummaryBlockStable is the golden-ish pin on
// the startup summary: the block ALWAYS prints every candidate's line
// (enabled ones included — §11: decisions are read, not inferred from
// silence), in §7.2 order, in this exact shape. Operators grep these
// lines and the troubleshooting page quotes them; change them
// deliberately or not at all.
func TestResolveSourcesAuto_SummaryBlockStable(t *testing.T) {
	t.Parallel()
	f := autoFlags(t)
	res, err := resolveSourcesAuto(context.Background(), f, fake.NewSimpleClientset(), allowAll(), metricsPresent)
	if err != nil {
		t.Fatalf("resolveSourcesAuto: %v", err)
	}
	want := []string{
		"sources: auto — probing the portable set (RBAC per source; metrics.k8s.io for saturation); misses are skipped loudly — pin --sources explicitly to make a miss fatal (§11)",
		"source k8s-events: enabled (always on — a sentinel that cannot watch events is misdeployed)",
		"source object-state: enabled",
		"source rollout: enabled",
		"source saturation: enabled",
		"source degradation: enabled",
		"source expiry: enabled",
		"source capacity: enabled",
		"sources: auto resolved → k8s-events,object-state,rollout,saturation,degradation,expiry,capacity (quota and token-burn stay explicit-only: project tier and the core-agent cost stack)",
	}
	if !slices.Equal(res.lines, want) {
		t.Errorf("summary block drifted:\n got: %q\nwant: %q", res.lines, want)
	}
}

// TestResolveSourcesAuto_NamespaceTier: an events-only grant set
// resolves to k8s-events alone, with one explicit skip line per
// missing source naming a concrete grant and the manifest that
// carries it.
func TestResolveSourcesAuto_NamespaceTier(t *testing.T) {
	t.Parallel()
	f := autoFlags(t)
	res, err := resolveSourcesAuto(context.Background(), f, fake.NewSimpleClientset(), namespaceTier(), metricsPresent)
	if err != nil {
		t.Fatalf("resolveSourcesAuto: %v", err)
	}
	if !slices.Equal(res.enabled, []string{"k8s-events"}) {
		t.Fatalf("enabled = %v, want [k8s-events]", res.enabled)
	}
	for _, name := range autoSourceNames[1:] {
		found := false
		for _, l := range res.lines {
			if strings.HasPrefix(l, fmt.Sprintf("source %s: disabled (missing ", name)) &&
				strings.Contains(l, "deploy/1") {
				found = true
			}
		}
		if !found {
			t.Errorf("no skip line naming the missing grant for %s:\n%s", name, strings.Join(res.lines, "\n"))
		}
	}
	// The capacity skip must point at ITS manifests (the kube-system
	// Role pair), not the ClusterRole. Denied pods come first in its
	// declaration order only after events pass — with events granted,
	// the first miss is pods (ClusterRole)… so pin the hint routing on
	// a configmaps-only miss instead.
	cmOnly := grantReviewer{allow: func(req sources.Requirement) bool { return req.Resource != "configmaps" }}
	res, err = resolveSourcesAuto(context.Background(), f, fake.NewSimpleClientset(), cmOnly, metricsPresent)
	if err != nil {
		t.Fatalf("resolveSourcesAuto: %v", err)
	}
	wantLine := `source capacity: disabled (missing "get configmaps cluster-autoscaler-status in namespace kube-system" — apply deploy/14-role-watcher-capacity.yaml + deploy/15-rolebinding-watcher-capacity.yaml, or name it in --sources to make this fatal)`
	if !slices.Contains(res.lines, wantLine) {
		t.Errorf("capacity skip line drifted:\n got block: %q\nwant line: %s", res.lines, wantLine)
	}
}

// TestResolveSourcesAuto_MetricsAPIAbsent: RBAC all green but no
// metrics.k8s.io → saturation (and only saturation) skips, with the
// exact install-metrics-server line.
func TestResolveSourcesAuto_MetricsAPIAbsent(t *testing.T) {
	t.Parallel()
	f := autoFlags(t)
	res, err := resolveSourcesAuto(context.Background(), f, fake.NewSimpleClientset(), allowAll(), metricsAbsent)
	if err != nil {
		t.Fatalf("resolveSourcesAuto: %v", err)
	}
	want := []string{"k8s-events", "object-state", "rollout", "degradation", "expiry", "capacity"}
	if !slices.Equal(res.enabled, want) {
		t.Errorf("enabled = %v, want %v (saturation off)", res.enabled, want)
	}
	line := "source saturation: disabled (metrics.k8s.io unavailable — install metrics-server)"
	if !slices.Contains(res.lines, line) {
		t.Errorf("missing the exact saturation skip line %q in:\n%s", line, strings.Join(res.lines, "\n"))
	}
}

// TestResolveSourcesAuto_EventsDeniedFatal: k8s-events is the one
// candidate auto never downgrades — a sentinel that cannot watch
// events is misdeployed, and that is a startup error, not a skip.
func TestResolveSourcesAuto_EventsDeniedFatal(t *testing.T) {
	t.Parallel()
	f := autoFlags(t)
	deny := grantReviewer{allow: func(req sources.Requirement) bool { return req.Resource != "events" }}
	_, err := resolveSourcesAuto(context.Background(), f, fake.NewSimpleClientset(), deny, metricsPresent)
	if err == nil {
		t.Fatal("events denied under auto must be FATAL, never a skip")
	}
	if !strings.Contains(err.Error(), "k8s-events") || !strings.Contains(err.Error(), "misdeployed") {
		t.Errorf("error must name the source and the misdeployment: %v", err)
	}
}

// TestResolveSourcesAuto_ProbeErrorFatal: §11 — "could not verify"
// must not degrade into "assumed fine", auto included.
func TestResolveSourcesAuto_ProbeErrorFatal(t *testing.T) {
	t.Parallel()
	f := autoFlags(t)
	_, err := resolveSourcesAuto(context.Background(), f, fake.NewSimpleClientset(), erroringReviewer{}, metricsPresent)
	if err == nil {
		t.Fatal("a probe evaluation error must be fatal under auto")
	}
	if !strings.Contains(err.Error(), "capability probe") {
		t.Errorf("error should say the probe itself failed: %v", err)
	}
}

// TestResolveSourcesAuto_ExpiryNamespacesScopeProbed: auto probes
// exactly what an enabled expiry source would read — the scoped
// namespaces from --expiry-namespaces, not cluster-wide secrets.
func TestResolveSourcesAuto_ExpiryNamespacesScopeProbed(t *testing.T) {
	t.Parallel()
	f := autoFlags(t, "--expiry-namespaces=prod")
	scoped := grantReviewer{allow: func(req sources.Requirement) bool {
		if req.Resource == "secrets" || req.Resource == "serviceaccounts" {
			return req.Namespace == "prod"
		}
		return true
	}}
	res, err := resolveSourcesAuto(context.Background(), f, fake.NewSimpleClientset(), scoped, metricsPresent)
	if err != nil {
		t.Fatalf("resolveSourcesAuto: %v", err)
	}
	if !slices.Contains(res.enabled, "expiry") {
		t.Errorf("expiry must resolve on when the scoped grants match --expiry-namespaces: %v", res.enabled)
	}
}

// TestResolveStormAuto covers the storm side of auto: grants present
// → on; a miss → off with the line naming the grant and the fatal
// escape hatch; window=0 → off; probe error → fatal. The resolution
// is INDEPENDENT of object-state (the graph feed runs its own
// pods/nodes/replicasets informers; factory sharing is an
// optimization) — pinned here by resolving on with a reviewer that
// would deny object-state's poddisruptionbudgets.
func TestResolveStormAuto(t *testing.T) {
	t.Parallel()
	f := autoFlags(t)

	noPDB := grantReviewer{allow: func(req sources.Requirement) bool { return req.Resource != "poddisruptionbudgets" }}
	on, line, err := resolveStormAuto(context.Background(), f, noPDB)
	if err != nil || !on {
		t.Fatalf("resolveStormAuto(graph grants present) = on=%v err=%v, want on", on, err)
	}
	if !strings.Contains(line, "storm: auto — on") {
		t.Errorf("on line drifted: %q", line)
	}

	noRS := grantReviewer{allow: func(req sources.Requirement) bool { return req.Resource != "replicasets" }}
	on, line, err = resolveStormAuto(context.Background(), f, noRS)
	if err != nil || on {
		t.Fatalf("resolveStormAuto(no replicasets) = on=%v err=%v, want off", on, err)
	}
	if !strings.Contains(line, "replicasets") || !strings.Contains(line, "--storm=on") {
		t.Errorf("off line must name the grant and the fatal escape hatch: %q", line)
	}

	zero := autoFlags(t, "--storm-window=0")
	on, line, err = resolveStormAuto(context.Background(), zero, allowAll())
	if err != nil || on {
		t.Fatalf("resolveStormAuto(window=0) = on=%v err=%v, want off", on, err)
	}
	if !strings.Contains(line, "--storm-window=0") {
		t.Errorf("window=0 line should say why: %q", line)
	}

	if _, _, err := resolveStormAuto(context.Background(), f, erroringReviewer{}); err == nil {
		t.Fatal("a graph-probe evaluation error must be fatal under auto")
	}
}

// TestStormFlag_ParsingModes: the string modes and bool-era aliases,
// plus the two shapes of the removed bare-flag syntax — trailing
// (parse error from the flag package) and mid-args (the next token is
// swallowed as the value; validate names the fix).
func TestStormFlag_ParsingModes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ arg, want string }{
		{"--storm=auto", stormAuto},
		{"--storm=on", stormOn},
		{"--storm=off", stormOff},
		{"--storm=true", stormOn},
		{"--storm=false", stormOff},
	} {
		f, err := parseFlags([]string{"--dry-run", tc.arg})
		if err != nil {
			t.Fatalf("parseFlags(%s): %v", tc.arg, err)
		}
		if err := f.validate(); err != nil {
			t.Fatalf("validate(%s): %v", tc.arg, err)
		}
		if f.storm != tc.want {
			t.Errorf("%s → storm=%q, want %q", tc.arg, f.storm, tc.want)
		}
	}

	if _, err := parseFlags([]string{"--dry-run", "--storm"}); err == nil {
		t.Error("trailing bare --storm must be a parse error (string flag, no value)")
	} else if !strings.Contains(err.Error(), "storm") {
		t.Errorf("the parse error should name the flag: %v", err)
	}

	f, err := parseFlags([]string{"--storm", "--enrich=critical", "--dry-run"})
	if err != nil {
		t.Fatalf("parseFlags(mid-args bare --storm): %v (the flag package takes the next token as the value; the error belongs to validate)", err)
	}
	err = f.validate()
	if err == nil {
		t.Fatal("mid-args bare --storm must be a config error")
	}
	if !strings.Contains(err.Error(), "bare --storm") || !strings.Contains(err.Error(), "--storm=on") {
		t.Errorf("the error must explain the syntax change and the fix: %v", err)
	}

	bad, _ := parseFlags([]string{"--dry-run", "--storm=maybe"})
	if err := bad.validate(); err == nil {
		t.Error("--storm=maybe must be rejected")
	}
}

// TestResolveAutoDefaults_EndToEnd drives the realMain entry point
// against a fake clientset: an allow-all SSAR reactor with no metrics
// API in discovery resolves the defaults to six sources (saturation
// off) and storm on, rewriting the flags in place; adding the metrics
// group flips saturation on. This is the path a default `lookout
// watch` invocation takes on a fully-granted cluster.
func TestResolveAutoDefaults_EndToEnd(t *testing.T) {
	t.Parallel()
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		sar, ok := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
		if !ok {
			return false, nil, nil
		}
		sar.Status.Allowed = true
		return true, sar, nil
	})

	f := autoFlags(t)
	if !f.sourcesAuto() || f.storm != stormAuto {
		t.Fatalf("defaults must be auto/auto, got sources=%q storm=%q", f.sources, f.storm)
	}
	if err := resolveAutoDefaults(context.Background(), f, client); err != nil {
		t.Fatalf("resolveAutoDefaults: %v", err)
	}
	if want := "k8s-events,object-state,rollout,degradation,expiry,capacity"; f.sources != want {
		t.Errorf("resolved sources = %q, want %q (no metrics API in fake discovery)", f.sources, want)
	}
	if f.storm != stormOn {
		t.Errorf("resolved storm = %q, want on", f.storm)
	}

	client.Resources = []*metav1.APIResourceList{{GroupVersion: metricsAPIGroupVersion}}
	f2 := autoFlags(t)
	if err := resolveAutoDefaults(context.Background(), f2, client); err != nil {
		t.Fatalf("resolveAutoDefaults: %v", err)
	}
	if want := strings.Join(autoSourceNames, ","); f2.sources != want {
		t.Errorf("resolved sources = %q, want the full set %q with metrics served", f2.sources, want)
	}

	// Explicit values are untouched — auto resolution is a no-op.
	f3 := autoFlags(t, "--sources=k8s-events", "--storm=off")
	if err := resolveAutoDefaults(context.Background(), f3, client); err != nil {
		t.Fatalf("resolveAutoDefaults(explicit): %v", err)
	}
	if f3.sources != "k8s-events" || f3.storm != stormOff {
		t.Errorf("explicit flags must pass through untouched: sources=%q storm=%q", f3.sources, f3.storm)
	}
}
