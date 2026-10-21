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

package rules_test

import (
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/rules"
)

// A minimal subject: one Deployment, two replicas, one container
// behind one Service, no preStop.
func subject() rules.Subject {
	return rules.Subject{
		Kind: "Deployment", Namespace: "prod", Name: "api",
		Object: map[string]any{
			"kind":     "Deployment",
			"metadata": map[string]any{"namespace": "prod", "name": "api"},
			"spec": map[string]any{
				"replicas": int64(2),
				"strategy": map[string]any{},
				"template": map[string]any{
					"metadata": map[string]any{"labels": map[string]any{"app": "api"}},
					"spec": map[string]any{
						"containers": []any{map[string]any{
							"name": "api", "image": "ghcr.io/acme/api:latest",
							"ports": []any{map[string]any{"name": "http", "containerPort": int64(8080)}},
						}},
					},
				},
			},
		},
		Services: []any{map[string]any{
			"metadata": map[string]any{"namespace": "prod", "name": "api"},
			"spec": map[string]any{
				"selector": map[string]any{"app": "api"},
				"ports":    []any{map[string]any{"name": "http", "port": int64(80), "targetPort": "http"}},
			},
		}},
	}
}

func TestBuiltinLoadsCompilesAndFires(t *testing.T) {
	eng, err := rules.New(rules.Builtin())
	if err != nil {
		t.Fatal(err)
	}
	fs, err := eng.Evaluate(subject())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range fs {
		got = append(got, f.Kind+"/"+f.Reason)
	}
	want := "audit.rollout_drops_traffic/NoPreStopOnServingContainer audit.image_floating_tag/FloatingTag"
	if strings.Join(got, " ") != want {
		t.Fatalf("fired %v, want %s", got, want)
	}
	for _, f := range fs {
		if f.Namespace != "prod" || f.KindOfObject != "Deployment" || f.Name != "api" || !strings.HasPrefix(f.Fingerprint, "sha256:") {
			t.Errorf("subject fields not stamped: %+v", f)
		}
	}
}

const minimal = `
rules:
  - id: r
    kind: audit.r
    doc: d
    subjects: [Deployment]
    reason: R
    severity: warning
    match: %s
    message: '"m"'
`

func load(t *testing.T, match string) (*rules.Engine, error) {
	t.Helper()
	set, err := rules.Load([]byte(strings.ReplaceAll(minimal, "%s", match)))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return rules.New(set)
}

func TestLoadValidates(t *testing.T) {
	cases := map[string]string{
		"missing kind prefix":              "rules: [{id: a, kind: x, doc: d, subjects: [Deployment], reason: R, severity: warning, match: 'true', message: '\"m\"'}]",
		"duplicate id":                     "rules: [{id: a, kind: audit.x, doc: d, subjects: [Deployment], reason: R, severity: warning, match: 'true', message: '\"m\"'}, {id: a, kind: audit.y, doc: d, subjects: [Deployment], reason: R, severity: warning, match: 'true', message: '\"m\"'}]",
		"no severity":                      "rules: [{id: a, kind: audit.x, doc: d, subjects: [Deployment], reason: R, match: 'true', message: '\"m\"'}]",
		"severity_expr without severities": "rules: [{id: a, kind: audit.x, doc: d, subjects: [Deployment], reason: R, severity_expr: '\"warning\"', match: 'true', message: '\"m\"'}]",
		"detail without doc":               "rules: [{id: a, kind: audit.x, doc: d, subjects: [Deployment], reason: R, severity: warning, match: 'true', message: '\"m\"', details: [{name: n, expr: '\"v\"'}]}]",
	}
	for name, src := range cases {
		if _, err := rules.Load([]byte(src)); err == nil {
			t.Errorf("%s: loaded without error", name)
		}
	}
}

func TestCompileErrorNamesTheRuleAndField(t *testing.T) {
	_, err := load(t, "object.spec.replicas >")
	if err == nil || !strings.Contains(err.Error(), "r.match") {
		t.Fatalf("err=%v, want one naming r.match", err)
	}
}

func TestMatchMustBeBool(t *testing.T) {
	// Statically, when the type is known.
	if _, err := load(t, `'"yes"'`); err == nil || !strings.Contains(err.Error(), "want bool") {
		t.Fatalf("err=%v, want a compile-time type error", err)
	}
	// At evaluation, when it is not (every field of `object` is dyn).
	eng, err := load(t, "object.spec.replicas")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Evaluate(subject()); err == nil || !strings.Contains(err.Error(), "want bool") {
		t.Fatalf("err=%v, want an evaluation-time type error", err)
	}
}

// A missing field is an error the caller sees, not a silent non-match.
func TestEvaluationErrorIsReported(t *testing.T) {
	eng, err := load(t, "object.spec.nope > 1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Evaluate(subject()); err == nil || !strings.Contains(err.Error(), "r.match on Deployment/prod/api") {
		t.Fatalf("err=%v, want an evaluation error naming the rule and subject", err)
	}
}

func TestCostLimitIsEnforced(t *testing.T) {
	set := rules.Builtin()
	eng, err := rules.New(set, rules.WithCostLimit(5))
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Evaluate(subject())
	if err == nil || !strings.Contains(err.Error(), "cost") {
		t.Fatalf("err=%v, want a cost-limit error", err)
	}
}

func TestSeverityExprOutsideDeclaredSetIsAnError(t *testing.T) {
	src := `
rules:
  - id: r
    kind: audit.r
    doc: d
    subjects: [Deployment]
    reason: R
    severities: [warning]
    severity_expr: '"critical"'
    match: 'true'
    message: '"m"'
`
	set, err := rules.Load([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := rules.New(set)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Evaluate(subject()); err == nil || !strings.Contains(err.Error(), "not one of") {
		t.Fatalf("err=%v", err)
	}
}
