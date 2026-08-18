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

package all_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// kindName is the ledger's naming convention, mirrored from
// checks.kindNamePattern: <owner>.<slug>, lowercase. It is also the
// filter that keeps this sweep off the many other `Kind:` fields in
// the tree — a TypeMeta's Kind is "Deployment", never "pod.crashloop".
var kindName = regexp.MustCompile(`^[a-z][a-z0-9]*(?:_[a-z0-9]+)*\.[a-z][a-z0-9]*(?:_[a-z0-9]+)*$`)

// TestEveryEmittedKindIsDeclared is the static half of the #278
// ledger's enforcement, and the half that matters most.
//
// checktest.Verify already rejects an emitted kind that the command's
// Kinds ledger does not declare — but only for the kinds a test run
// actually produces. A branch no fixture reaches emits an undeclared
// kind to a real cluster and nothing complains, which is precisely the
// coverage lie §11 forbids: the generated glossary would claim a
// vocabulary the binary does not keep to.
//
// So this reads the source instead of the output. Every string the
// tree assigns to a Finding's Kind — literal, package constant, or a
// constant aliasing one in another package — must appear in some
// registered command's ledger. It is deliberately one-directional: a
// declared kind no code path emits is honest (a check that never fires
// still owes the reader its vocabulary), an emitted kind no
// declaration covers is not.
func TestEveryEmittedKindIsDeclared(t *testing.T) {
	src := parseCheckPackages(t)

	declared := map[string]bool{}
	for _, c := range checks.Default().All() {
		for _, k := range c.Kinds {
			declared[k.Name] = true
		}
	}
	if len(declared) == 0 {
		t.Fatal("no kinds declared in the default registry — the ledger is empty, not the tree")
	}

	var unresolved []string
	for _, e := range src.emissions() {
		values, ok := src.resolve(e.dir, e.expr, 0)
		if !ok {
			if e.strict {
				unresolved = append(unresolved, e.where)
			}
			continue
		}
		for _, value := range values {
			if !kindName.MatchString(value) {
				continue // a TypeMeta Kind, a graph node kind, a format string
			}
			if !declared[value] {
				t.Errorf("%s: emits kind %q, which no registered command declares\n"+
					"\tadd checks.Kind(%q, <one-line claim>, <severities…>) to the command's Kinds ledger (issue #278)",
					e.where, value, value)
			}
		}
	}
	// An unresolvable Kind is not a pass. It means this sweep cannot
	// see what that line emits, so the ledger's guarantee has a hole
	// exactly there — name the line rather than quietly skipping it.
	for _, where := range unresolved {
		t.Errorf("%s: the kind assigned here is not a literal or a package constant, so it cannot be checked against the ledger\n"+
			"\tspell it as a constant (issue #278)", where)
	}
}

// kindSource is the parsed pkg/checks tree: the constants each
// directory declares, plus a package-name index so a constant that
// aliases another package's can be followed.
type kindSource struct {
	consts  map[string]map[string]ast.Expr   // dir -> const name -> value
	fields  map[string]map[string][]ast.Expr // dir -> struct field name -> every value assigned to it
	returns map[string]map[string][]ast.Expr // dir -> func name -> every single-value return expression
	locals  map[string]map[string][]ast.Expr // dir -> local variable name -> every value assigned to it
	dirOf   map[string]string                // package name -> dir
	assigns []kindAssign
}

// kindAssign is one place the tree sets a Finding's Kind. strict is
// false for the one shape that legitimately cannot resolve on its own
// — `Kind: kind`, where the local was assigned or passed in elsewhere
// and the assignment/parameter rules below see the literal directly.
// Everything else must resolve or the sweep says so.
type kindAssign struct {
	dir    string
	expr   ast.Expr
	where  string
	strict bool
}

func (s *kindSource) emissions() []kindAssign { return s.assigns }

// resolve follows an expression to the string constants it can hold.
// It returns a set rather than one value because the table-driven
// emitters reach their kind through a struct field (`Kind:
// q.findingKind`) whose every possible value is a literal somewhere in
// the package's spec table — all of them have to be declared, so all
// of them are returned. depth bounds the alias chain.
func (s *kindSource) resolve(dir string, e ast.Expr, depth int) ([]string, bool) {
	if depth > 4 {
		return nil, false
	}
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return nil, false
		}
		out, err := strconv.Unquote(v.Value)
		if err != nil {
			return nil, false
		}
		return []string{out}, true
	case *ast.Ident:
		if next, ok := s.consts[dir][v.Name]; ok {
			return s.resolve(dir, next, depth+1)
		}
		// A local the emitter builds up before it returns — the
		// `class` accumulator in a classifier, say. Every value the
		// package assigns to that name is a candidate; the union has
		// to be declared, so a stray same-named local only makes this
		// stricter.
		return s.resolveAll(dir, s.locals[dir][v.Name], depth)
	case *ast.SelectorExpr:
		if pkg, ok := v.X.(*ast.Ident); ok {
			if other, ok := s.dirOf[pkg.Name]; ok {
				if next, ok := s.consts[other][v.Sel.Name]; ok {
					return s.resolve(other, next, depth+1)
				}
			}
		}
		// Not a package constant, so a struct field: every literal the
		// package assigns to a field of that name is a kind this site
		// can emit.
		return s.resolveAll(dir, s.fields[dir][v.Sel.Name], depth)
	case *ast.CallExpr:
		// A classifier: `Kind: classify(rec)`. Everything it can
		// return is a kind this site can emit.
		fn, ok := v.Fun.(*ast.Ident)
		if !ok {
			return nil, false
		}
		return s.resolveAll(dir, s.returns[dir][fn.Name], depth)
	}
	return nil, false
}

// resolveAll resolves a set of candidate expressions, failing whole if
// any one of them is opaque — a half-resolved set would let the
// unresolved branch through unchecked.
func (s *kindSource) resolveAll(dir string, exprs []ast.Expr, depth int) ([]string, bool) {
	var out []string
	for _, e := range exprs {
		got, ok := s.resolve(dir, e, depth+1)
		if !ok {
			return nil, false
		}
		out = append(out, got...)
	}
	return out, len(out) > 0
}

// parseCheckPackages walks pkg/checks for non-test Go files, indexes
// their string constants, and collects every assignment to a Finding's
// Kind — both the `Kind:` field of a composite literal and the
// `f.Kind = …` / `kind, severity = …` forms the switch-shaped emitters
// use.
func parseCheckPackages(t *testing.T) *kindSource {
	t.Helper()
	const root = ".." // pkg/checks
	src := &kindSource{
		consts:  map[string]map[string]ast.Expr{},
		fields:  map[string]map[string][]ast.Expr{},
		returns: map[string]map[string][]ast.Expr{},
		locals:  map[string]map[string][]ast.Expr{},
		dirOf:   map[string]string{},
	}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "checktest" {
			// The scaffold's own two commands register into whatever
			// registry a test builds, never the default one, so their
			// kinds are not in this glossary by design.
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(fset, p, body, 0)
		if err != nil {
			return err
		}
		dir := filepath.Dir(p)
		src.dirOf[f.Name.Name] = dir
		if src.consts[dir] == nil {
			src.consts[dir] = map[string]ast.Expr{}
			src.fields[dir] = map[string][]ast.Expr{}
			src.returns[dir] = map[string][]ast.Expr{}
			src.locals[dir] = map[string][]ast.Expr{}
		}
		collectConsts(f, src.consts[dir])
		collectFields(f, src.fields[dir])
		collectReturns(f, src.returns[dir])
		collectLocals(f, src.locals[dir])
		src.assigns = append(src.assigns, collectKindAssigns(fset, f, dir)...)
		return nil
	})
	if err != nil {
		t.Fatalf("walking pkg/checks: %v", err)
	}
	if len(src.assigns) == 0 {
		t.Fatal("found no Kind assignments under pkg/checks — the walk is broken, not the tree")
	}
	return src
}

// collectConsts records every `const name = <expr>` in the file, at
// any nesting depth.
func collectConsts(f *ast.File, into map[string]ast.Expr) {
	ast.Inspect(f, func(n ast.Node) bool {
		decl, ok := n.(*ast.GenDecl)
		if !ok || decl.Tok != token.CONST {
			return true
		}
		for _, spec := range decl.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if i < len(vs.Values) {
					into[name.Name] = vs.Values[i]
				}
			}
		}
		return true
	})
}

// collectFields records every value the file assigns to a named
// struct field in a composite literal, keyed by field name. It is how
// the table-driven emitters resolve: `Kind: q.findingKind` is checked
// against every literal the package's spec table puts in a
// findingKind.
func collectFields(f *ast.File, into map[string][]ast.Expr) {
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		for _, el := range lit.Elts {
			kv, ok := el.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := kv.Key.(*ast.Ident); ok {
				into[key.Name] = append(into[key.Name], kv.Value)
			}
		}
		return true
	})
}

// collectReturns records every single-value return expression of each
// top-level function, keyed by name. It is how a classifier resolves:
// `Kind: classify(rec)` is checked against everything classify can
// return.
func collectReturns(f *ast.File, into map[string][]ast.Expr) {
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			ret, ok := n.(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 1 {
				return true
			}
			into[fn.Name.Name] = append(into[fn.Name.Name], ret.Results[0])
			return true
		})
	}
}

// collectLocals records every value the file assigns to a plain
// variable name, so a kind held in a local until the emitter is
// reached can still be traced back to its literals.
func collectLocals(f *ast.File, into map[string][]ast.Expr) {
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.AssignStmt:
			for i, lhs := range v.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok || i >= len(v.Rhs) || id.Name == "_" {
					continue
				}
				into[id.Name] = append(into[id.Name], v.Rhs[i])
			}
		case *ast.ValueSpec:
			for i, name := range v.Names {
				if i < len(v.Values) {
					into[name.Name] = append(into[name.Name], v.Values[i])
				}
			}
		}
		return true
	})
}

// collectKindAssigns finds every expression the file assigns to a
// finding's Kind. Three shapes cover the tree:
//
//   - emit.Finding{Kind: …} — the common case. The composite's type is
//     checked, because `Kind` is also how emit.WorkloadRef spells a
//     Kubernetes object kind, and "Deployment" is not a finding kind;
//   - kind, severity, reason = "x", … — the switch-shaped emitters
//     that pick a kind before building the finding;
//   - f.Kind = … — the fixup the gateway and radius emitters do after
//     building a finding, restricted to locals of type emit.Finding
//     for the same reason the composite's type is checked.
func collectKindAssigns(fset *token.FileSet, f *ast.File, dir string) []kindAssign {
	var out []kindAssign
	at := func(pos token.Pos) string {
		p := fset.Position(pos)
		return filepath.ToSlash(p.Filename) + ":" + strconv.Itoa(p.Line)
	}
	findings := findingLocals(f)
	kindParam := kindFuncs(f)
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.CompositeLit:
			if !isFindingType(v.Type) {
				return true
			}
			for _, el := range v.Elts {
				kv, ok := el.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok || key.Name != "Kind" {
					continue
				}
				_, bare := kv.Value.(*ast.Ident)
				out = append(out, kindAssign{dir: dir, expr: kv.Value, where: at(kv.Pos()), strict: !bare})
			}
		case *ast.AssignStmt:
			if len(v.Lhs) != len(v.Rhs) {
				return true
			}
			for i, lhs := range v.Lhs {
				switch t := lhs.(type) {
				case *ast.Ident:
					// Only the tuple form, `kind, severity, reason =
					// "x", …`: a lone `kind := …` is far more often a
					// Kubernetes object kind being canonicalized.
					if t.Name != "kind" || len(v.Lhs) == 1 {
						continue
					}
				case *ast.SelectorExpr:
					recv, ok := t.X.(*ast.Ident)
					if !ok || t.Sel.Name != "Kind" || !findings[recv.Name] {
						continue
					}
				default:
					continue
				}
				out = append(out, kindAssign{dir: dir, expr: v.Rhs[i], where: at(v.Pos()), strict: true})
			}
		case *ast.CallExpr:
			// The fourth shape: a local helper that builds findings for
			// a family, `base := func(kind, reason, message string) …`.
			// The literal only ever appears at the call sites.
			fn, ok := v.Fun.(*ast.Ident)
			if !ok || !kindParam[fn.Name] || len(v.Args) == 0 {
				return true
			}
			out = append(out, kindAssign{dir: dir, expr: v.Args[0], where: at(v.Pos()), strict: true})
		}
		return true
	})
	return out
}

// kindFuncs names the file's finding-family helpers: a first
// parameter called `kind` AND an emit.Finding result. Both halves are
// needed — plenty of helpers take a Kubernetes object kind first
// (refKey, matchKey, renderSpec), and passing "Deployment" to one of
// those is not an emission.
func kindFuncs(f *ast.File) map[string]bool {
	out := map[string]bool{}
	builds := func(ft *ast.FuncType) bool {
		if ft.Params == nil || len(ft.Params.List) == 0 || len(ft.Params.List[0].Names) == 0 {
			return false
		}
		if ft.Params.List[0].Names[0].Name != "kind" || ft.Results == nil {
			return false
		}
		for _, r := range ft.Results.List {
			if isFindingType(r.Type) {
				return true
			}
		}
		return false
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.FuncDecl:
			if builds(v.Type) {
				out[v.Name.Name] = true
			}
		case *ast.AssignStmt:
			for i, lhs := range v.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok || i >= len(v.Rhs) {
					continue
				}
				if lit, ok := v.Rhs[i].(*ast.FuncLit); ok && builds(lit.Type) {
					out[id.Name] = true
				}
			}
		}
		return true
	})
	return out
}

// isFindingType reports whether a composite literal's type is
// emit.Finding.
func isFindingType(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "emit" && sel.Sel.Name == "Finding"
}

// findingLocals names the file's variables bound to an emit.Finding,
// so `f.Kind = …` can be told apart from `wl.Kind = …`. File scope is
// coarse — two functions using the same variable name are pooled — but
// it errs toward checking more, which is the safe direction.
func findingLocals(f *ast.File) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.AssignStmt:
			for i, lhs := range v.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok || i >= len(v.Rhs) {
					continue
				}
				if lit, ok := v.Rhs[i].(*ast.CompositeLit); ok && isFindingType(lit.Type) {
					out[id.Name] = true
				}
			}
		case *ast.ValueSpec:
			if !isFindingType(v.Type) {
				return true
			}
			for _, name := range v.Names {
				out[name.Name] = true
			}
		}
		return true
	})
	return out
}

// TestDeclaredKindsAreUnique holds the ledger to one owner per kind
// where it can: two commands may legitimately share a kind (scan and
// health both re-emit their stages'), but they must describe it the
// same way, or the generated glossary has to pick a winner and the
// reader gets whichever one sorted first.
func TestDeclaredKindsAreUnique(t *testing.T) {
	type entry struct{ command, doc string }
	seen := map[string]entry{}
	for _, c := range checks.Default().All() {
		for _, k := range c.Kinds {
			prev, ok := seen[k.Name]
			if !ok {
				seen[k.Name] = entry{c.Name, k.Doc}
				continue
			}
			if prev.doc != k.Doc {
				t.Errorf("kind %q is described two different ways:\n\t`%s`: %s\n\t`%s`: %s\n"+
					"compose the ledger from the owning command's declaration instead of restating it",
					k.Name, prev.command, prev.doc, c.Name, k.Doc)
			}
		}
	}
}

// TestKindGlossaryIsSorted is a readability guard on the generated
// glossary's input: nothing depends on ledger order at runtime, but a
// stable, sorted union is what makes the generated page reviewable.
func TestKindGlossaryIsSorted(t *testing.T) {
	entries := checks.Default().KindGlossary()
	if len(entries) == 0 {
		t.Fatal("the kind glossary is empty")
	}
	sorted := make([]string, len(entries))
	for i, k := range entries {
		sorted[i] = k.Name
	}
	if !sort.StringsAreSorted(sorted) {
		t.Errorf("KindGlossary is not sorted by name: %v", sorted)
	}
}
