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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/rules"
)

// RulesCommand builds `lookout audit rules`: the CEL arm of the rules
// pilot. It evaluates the built-in rule file (pkg/rules/builtin/
// audit.yaml) over the same workload index `audit workloads` reads,
// through the same Snapshot, emitter, sanitizer and fingerprint path,
// and exists so the two arms can be run over one cluster and diffed.
//
// PILOT-ONLY. The command name, its kinds ledger (derived from the
// rule file) and its presence in the audit MCP profile are all
// provisional; before any merge to main this either folds into the
// commands that own the kinds or is removed with the branch.
func RulesCommand(deps Deps) checks.Command {
	set := rules.Builtin()
	return checks.Command{
		Name:        "audit rules",
		MCPName:     "k8s_audit_rules",
		MCPProfiles: []string{"audit"},
		Summary:     "PILOT: evaluates the built-in CEL posture rules (rollout safety and image-tag pinning) over Deployments, StatefulSets and DaemonSets. The same claims `audit workloads` and `audit hardening` make in Go, authored as data; output for the shared kinds is byte-identical by test. Scope with --namespace or -A; scanned counts workloads examined.",
		Kinds:       rulesKinds(set),
		Output:      rulesOutput(set),
		Examples: []string{
			"lookout audit rules -A",
			"lookout audit rules --namespace=prod",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runRules(ctx, deps, set, inv)
		},
	}
}

// rulesKinds derives the kinds ledger from the rule file: one entry per
// kind, its doc from the first rule declaring it, its severities the
// union over those rules. The ledger is the registry's view of what the
// command may emit, so a rule cannot add a kind without adding it here
// — which is the point of deriving it.
func rulesKinds(set *rules.Set) []checks.KindField {
	var out []checks.KindField
	index := map[string]int{}
	for _, r := range set.Rules {
		i, ok := index[r.Kind]
		if !ok {
			index[r.Kind] = len(out)
			out = append(out, checks.Kind(r.Kind, r.Doc))
			i = len(out) - 1
		}
		for _, s := range r.Severities {
			if !containsString(out[i].Severity, s) {
				out[i].Severity = append(out[i].Severity, s)
			}
		}
	}
	return append(out, checks.UnreadKind())
}

// rulesOutput derives the output glossary from the rules' details, in
// first-appearance order, plus the summary notes.
func rulesOutput(set *rules.Set) []checks.OutputField {
	var out []checks.OutputField
	seen := map[string]bool{}
	for _, r := range set.Rules {
		for _, d := range r.Details {
			if seen[d.Name] {
				continue
			}
			seen[d.Name] = true
			out = append(out, checks.OutputField{Name: d.Name, Doc: d.Doc})
		}
	}
	out = append(out,
		checks.OutputField{Name: "rules", Doc: "summary note: rules evaluated per workload"},
		checks.OutputField{Name: "workloads", Doc: "summary note: workloads examined, broken down as deployments/statefulsets/daemonsets"},
	)
	return append(out, checks.UnreadFields()...)
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func runRules(ctx context.Context, deps Deps, set *rules.Set, inv emit.Invocation) (int, error) {
	if inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces {
		return 0, emit.UsageErrorf("no scope: pass --namespace=<ns> or -A")
	}
	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}
	eng, err := rules.New(set)
	if err != nil {
		return 0, err
	}
	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ix, err := listTemplateOwners(ctx, client, listNS)
	if err != nil {
		return 0, err
	}
	// Services are converted once; every subject shares the slice. A
	// refused Service list leaves it empty, so the Service-backed rules
	// stay silent and the unread record below says why.
	services := make([]any, 0, len(ix.services))
	for _, svc := range ix.services {
		m, err := toUnstructured(svc)
		if err != nil {
			return 0, err
		}
		services = append(services, m)
	}

	var findings []emit.Finding
	var counts [3]int
	for _, w := range ix.workloads {
		switch w.kind {
		case "Deployment":
			counts[0]++
		case "StatefulSet":
			counts[1]++
		default:
			counts[2]++
		}
		subject, err := ruleSubject(w, services)
		if err != nil {
			return 0, err
		}
		fs, err := eng.Evaluate(subject)
		if err != nil {
			return 0, err
		}
		findings = append(findings, fs...)
	}
	sortFindings(findings)

	if ix.servicesRefused != nil {
		if err := inv.Out.Emit(checks.RefusedFinding(*ix.servicesRefused,
			"audit.rollout_drops_traffic and audit.recreate_strategy not judged: \"Service-backed\" is decided from the Service objects")); err != nil {
			return 0, err
		}
	}
	for _, kind := range workloadStepKinds[:3] {
		if r, ok := ix.kindsRefused[kind]; ok {
			if err := inv.Out.Emit(checks.RefusedFinding(r, kind+"s not audited")); err != nil {
				return 0, err
			}
		}
	}
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	if err := inv.Out.Note("rules", itoa(len(set.Rules))); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("workloads", fmt.Sprintf("%d/%d/%d", counts[0], counts[1], counts[2])); err != nil {
		return 0, err
	}
	return len(ix.workloads), nil
}

// ruleSubject renders one workload the way a rule reads it: kind,
// metadata, and a spec with the template as the API serves it,
// spec.replicas resolved (nil is 1; absent on a DaemonSet) and the
// Deployment strategy when there is one — the same flattening the Go
// detectors get from the workload struct, so neither arm sees a field
// the other does not.
func ruleSubject(w workload, services []any) (rules.Subject, error) {
	tmpl, err := toUnstructured(&w.template)
	if err != nil {
		return rules.Subject{}, fmt.Errorf("%s %s/%s: %w", w.kind, w.namespace, w.name, err)
	}
	spec := map[string]any{"template": tmpl}
	if w.kind != "DaemonSet" {
		spec["replicas"] = int64(w.wantReplicas())
	}
	if w.strategy != nil {
		s, err := toUnstructured(w.strategy)
		if err != nil {
			return rules.Subject{}, fmt.Errorf("%s %s/%s strategy: %w", w.kind, w.namespace, w.name, err)
		}
		spec["strategy"] = s
	}
	return rules.Subject{
		Kind:      w.kind,
		Namespace: w.namespace,
		Name:      w.name,
		Object: map[string]any{
			"kind":     w.kind,
			"metadata": map[string]any{"namespace": w.namespace, "name": w.name},
			"spec":     spec,
		},
		Services: services,
	}, nil
}

// toUnstructured renders a typed API object as the map form a rule
// reads: JSON field names, int64 numbers, omitempty honored.
func toUnstructured(obj any) (map[string]any, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, fmt.Errorf("converting %T for the rules: %w", obj, err)
	}
	return m, nil
}
