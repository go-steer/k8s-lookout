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

package rules

import (
	"fmt"
	"strconv"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types/ref"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

// DefaultCostLimit bounds one expression's evaluation. CEL's cost unit
// is roughly one operation; a rule that joins every container of a
// workload against every port of every Service in a namespace of a
// thousand Services stays under 10^6 by two orders of magnitude, and a
// runaway comprehension (nested over the Service list twice, say) hits
// the limit instead of the --timeout.
const DefaultCostLimit uint64 = 1_000_000

// Engine holds a rule set compiled once; Evaluate runs it per subject.
type Engine struct {
	set    *Set
	locals []cel.Program // the file-level locals, in order
	rules  []compiledRule
}

type compiledRule struct {
	rule     Rule
	subjects map[string]bool
	locals   []cel.Program
	match    cel.Program
	message  cel.Program
	severity cel.Program // nil when the rule has a fixed severity
	details  []cel.Program
}

// Option configures New.
type Option func(*options)

type options struct {
	costLimit uint64
}

// WithCostLimit overrides DefaultCostLimit.
func WithCostLimit(n uint64) Option { return func(o *options) { o.costLimit = n } }

// New compiles every expression in the set. A compile error names the
// rule and the field, so a typo in the YAML fails at init (for the
// built-in file) or at load (for a file on disk), never at a finding.
func New(set *Set, opts ...Option) (*Engine, error) {
	o := options{costLimit: DefaultCostLimit}
	for _, opt := range opts {
		opt(&o)
	}
	base, err := baseEnv()
	if err != nil {
		return nil, fmt.Errorf("rules: environment: %w", err)
	}
	e := &Engine{set: set}
	for _, l := range set.Locals {
		ast, iss := base.Compile(l.Expr)
		if iss != nil && iss.Err() != nil {
			return nil, fmt.Errorf("rules: locals.%s: %w", l.Name, iss.Err())
		}
		prg, err := base.Program(ast, cel.CostLimit(o.costLimit))
		if err != nil {
			return nil, fmt.Errorf("rules: locals.%s: %w", l.Name, err)
		}
		e.locals = append(e.locals, prg)
		if base, err = base.Extend(cel.Variable(l.Name, cel.DynType)); err != nil {
			return nil, fmt.Errorf("rules: locals.%s: %w", l.Name, err)
		}
	}
	for _, r := range set.Rules {
		cr, err := compileRule(base, r, o.costLimit)
		if err != nil {
			return nil, err
		}
		e.rules = append(e.rules, cr)
	}
	return e, nil
}

func compileRule(base *cel.Env, r Rule, costLimit uint64) (compiledRule, error) {
	cr := compiledRule{rule: r, subjects: map[string]bool{}}
	for _, s := range r.Subjects {
		cr.subjects[s] = true
	}
	env := base
	compile := func(field, src string, want *cel.Type) (cel.Program, error) {
		ast, iss := env.Compile(src)
		if iss != nil && iss.Err() != nil {
			return nil, fmt.Errorf("rules: %s.%s: %w", r.ID, field, iss.Err())
		}
		if want != nil && !ast.OutputType().IsAssignableType(want) && ast.OutputType() != cel.DynType {
			return nil, fmt.Errorf("rules: %s.%s: evaluates to %s, want %s", r.ID, field, ast.OutputType(), want)
		}
		prg, err := env.Program(ast, cel.CostLimit(costLimit))
		if err != nil {
			return nil, fmt.Errorf("rules: %s.%s: %w", r.ID, field, err)
		}
		return prg, nil
	}
	for _, l := range r.Locals {
		prg, err := compile("locals."+l.Name, l.Expr, nil)
		if err != nil {
			return cr, err
		}
		cr.locals = append(cr.locals, prg)
		// Later expressions see the local as an untyped variable; its
		// real type is whatever the expression produced at runtime.
		env, err = env.Extend(cel.Variable(l.Name, cel.DynType))
		if err != nil {
			return cr, fmt.Errorf("rules: %s.locals.%s: %w", r.ID, l.Name, err)
		}
	}
	var err error
	if cr.match, err = compile("match", r.Match, cel.BoolType); err != nil {
		return cr, err
	}
	if cr.message, err = compile("message", r.Message, cel.StringType); err != nil {
		return cr, err
	}
	if r.SeverityExpr != "" {
		if cr.severity, err = compile("severity_expr", r.SeverityExpr, cel.StringType); err != nil {
			return cr, err
		}
	}
	for _, d := range r.Details {
		prg, err := compile("details."+d.Name, d.Expr, nil)
		if err != nil {
			return cr, err
		}
		cr.details = append(cr.details, prg)
	}
	return cr, nil
}

// Subject is one object the rules are asked about. Object is the
// workload as an unstructured map (what a rule reads as `object`),
// Services the namespace's Services in the same form.
type Subject struct {
	Kind      string
	Namespace string
	Name      string
	Object    map[string]any
	Services  []any
}

// Evaluate runs every rule whose subjects include the subject's kind
// and returns the findings that matched, stamped with subject fields
// and the posture fingerprint. An evaluation error — a rule over its
// cost budget, a message that is not a string, a field access on a
// missing key — is returned as an error naming the rule, never
// swallowed: the caller decides whether that fails the command or is
// reported as an unread record, but it is never silence.
func (e *Engine) Evaluate(s Subject) ([]emit.Finding, error) {
	act := map[string]any{
		varObject:     s.Object,
		varKind:       s.Kind,
		varServices:   s.Services,
		varThresholds: e.set.Thresholds,
	}
	for i, l := range e.set.Locals {
		v, _, err := e.locals[i].Eval(act)
		if err != nil {
			return nil, fmt.Errorf("rules: locals.%s on %s/%s/%s: %w", l.Name, s.Kind, s.Namespace, s.Name, err)
		}
		act[l.Name] = v
	}
	var out []emit.Finding
	for i := range e.rules {
		cr := &e.rules[i]
		if !cr.subjects[s.Kind] {
			continue
		}
		f, matched, err := cr.eval(s, act)
		if err != nil {
			return out, err
		}
		if matched {
			out = append(out, f)
		}
	}
	return out, nil
}

// eval runs one rule over the subject's activation. The rule's own
// locals are added to a copy, so one rule's names never leak into the
// next.
func (cr *compiledRule) eval(s Subject, shared map[string]any) (emit.Finding, bool, error) {
	r := cr.rule
	act := shared
	if len(r.Locals) > 0 {
		act = make(map[string]any, len(shared)+len(r.Locals))
		for k, v := range shared {
			act[k] = v
		}
	}
	run := func(field string, prg cel.Program) (ref.Val, error) {
		v, _, err := prg.Eval(act)
		if err != nil {
			return nil, fmt.Errorf("rules: %s.%s on %s/%s/%s: %w", r.ID, field, s.Kind, s.Namespace, s.Name, err)
		}
		return v, nil
	}
	for i, l := range r.Locals {
		v, err := run("locals."+l.Name, cr.locals[i])
		if err != nil {
			return emit.Finding{}, false, err
		}
		act[l.Name] = v
	}
	v, err := run("match", cr.match)
	if err != nil {
		return emit.Finding{}, false, err
	}
	matched, ok := v.Value().(bool)
	if !ok {
		return emit.Finding{}, false, fmt.Errorf("rules: %s.match: evaluated to %T, want bool", r.ID, v.Value())
	}
	if !matched {
		return emit.Finding{}, false, nil
	}
	msg, err := run("message", cr.message)
	if err != nil {
		return emit.Finding{}, false, err
	}
	severity := r.Severity
	if cr.severity != nil {
		sv, err := run("severity_expr", cr.severity)
		if err != nil {
			return emit.Finding{}, false, err
		}
		severity = fmt.Sprint(sv.Value())
		if !contains(r.Severities, severity) {
			return emit.Finding{}, false, fmt.Errorf("rules: %s.severity_expr produced %q, not one of %v", r.ID, severity, r.Severities)
		}
	}
	f := emit.Finding{
		Kind:         r.Kind,
		Severity:     severity,
		Reason:       r.Reason,
		Message:      fmt.Sprint(msg.Value()),
		Namespace:    s.Namespace,
		KindOfObject: s.Kind,
		Name:         s.Name,
		Fingerprint:  engine.PostureFingerprint(r.Kind, r.Reason, s.Kind),
	}
	for i, d := range r.Details {
		dv, err := run("details."+d.Name, cr.details[i])
		if err != nil {
			return emit.Finding{}, false, err
		}
		f.Details = append(f.Details, emit.Field{Key: d.Name, Value: detailString(dv.Value())})
	}
	return f, true, nil
}

func detailString(v any) string {
	switch n := v.(type) {
	case string:
		return n
	case int64:
		return strconv.FormatInt(n, 10)
	}
	return fmt.Sprint(v)
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
