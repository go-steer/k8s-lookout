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

// Command newcheck scaffolds a read-path check: the command file, its
// test suite, and a first golden. Run it through dev/tools/new-check.
//
// The point is not to save typing. A check has eight touchpoints, two
// of them conditional, and the failure mode of missing one is quiet —
// a command that exists for the binary but not the docs, or a kind
// that no glossary lists. docs/adding-a-check.md describes the
// touchpoints; this generates the ones a generator can, and prints the
// ones only a human can decide.
//
// What comes out compiles and its tests pass, emitting nothing. That
// is a legitimate check — §4.2 zero nominal state — so the scaffold is
// a working command from the first commit rather than a broken tree
// you have to finish before you can run anything.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "new-check:", err)
		os.Exit(1)
	}
}

var (
	group   = flag.String("group", "", "existing command group package under pkg/checks (state, audit, delta, …)")
	check   = flag.String("check", "", "the new check's name within the group, lowercase (webhooks, storage, …)")
	summary = flag.String("summary", "", "the when-to-use line: when a reader should run this, then what it reports")
	mcpName = flag.String("mcp-name", "", "MCP tool name (default k8s_<group>_<check>)")
	root    = flag.String("root", ".", "repository root")
)

func run() error {
	flag.Parse()
	switch {
	case *group == "":
		return fmt.Errorf("--group is required")
	case *check == "":
		return fmt.Errorf("--check is required")
	case *summary == "":
		return fmt.Errorf("--summary is required — the when-to-use line is the whole reason an agent picks this command; it is not boilerplate to fill in later")
	}
	if !ident.MatchString(*group) {
		return fmt.Errorf("--group %q must be lowercase letters and digits", *group)
	}
	if !ident.MatchString(*check) {
		return fmt.Errorf("--check %q must be lowercase letters and digits", *check)
	}

	dir := filepath.Join(*root, "pkg", "checks", *group)
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("no such group package %s — a new GROUP is a bigger decision "+
			"than a new check: it needs a package doc stating what claim the group makes, "+
			"a Deps struct, a blank import in pkg/checks/all/all.go, and a place in "+
			"`lookout --help`; read docs/adding-a-check.md ('Adding a whole group') "+
			"and do it by hand", dir)
	}

	pkg, err := inspect(dir)
	if err != nil {
		return err
	}

	name := *group + " " + *check
	mcp := *mcpName
	if mcp == "" {
		mcp = "k8s_" + *group + "_" + *check
	}
	d := data{
		Group:    *group,
		Check:    *check,
		Exported: strings.ToUpper((*check)[:1]) + (*check)[1:],
		Name:     name,
		MCPName:  mcp,
		Summary:  strings.ReplaceAll(*summary, `"`, `\"`),
		Kind:     *group + "." + *check + "_todo",
		Deps:     pkg.hasDeps,
		Client:   pkg.hasClient,
		Now:      pkg.hasNow,
	}

	files := map[string]string{}
	for path, tmpl := range map[string]string{
		filepath.Join(dir, *check+".go"):      commandTemplate,
		filepath.Join(dir, *check+"_test.go"): testTemplate,
	} {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s already exists — pick another name or delete it", path)
		}
		src, err := format.Source([]byte(render(tmpl, d)))
		if err != nil {
			return fmt.Errorf("the generated %s does not parse (a template bug, not yours): %w", filepath.Base(path), err)
		}
		files[path] = string(src)
	}
	if err := os.MkdirAll(filepath.Join(dir, "testdata"), 0o755); err != nil {
		return err
	}
	for path, body := range files {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			return err
		}
		fmt.Println("wrote", path)
	}

	// Write the first golden by running the generated golden test the
	// same way a contributor will every time the output changes. This
	// also proves the scaffold compiles before it is handed over.
	pkgPath := "./pkg/checks/" + *group
	// The two interpolated arguments are the group and check names, both
	// already matched against `ident` above, so neither can carry a shell
	// metacharacter — and exec.Command does not use a shell regardless.
	cmd := exec.Command("go", "test", "-run", "Test"+d.Exported+"Golden", pkgPath) //nolint:gosec // arguments validated above
	cmd.Dir = *root
	cmd.Env = append(os.Environ(), "UPDATE_GOLDEN=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("the scaffold does not build or its golden test fails:\n%s", out)
	}
	fmt.Println("wrote", filepath.Join(dir, "testdata", *check+".golden"))

	fmt.Print(render(checklistTemplate, d))
	return nil
}

var ident = regexp.MustCompile(`^[a-z][a-z0-9]*$`)

// pkgFacts are the things about the target group that change what the
// scaffold can say: the Deps struct's shape decides whether the
// generated command takes dependencies and whether the test can inject
// a fake clientset.
type pkgFacts struct {
	hasDeps   bool
	hasClient bool
	hasNow    bool
}

func inspect(dir string) (pkgFacts, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return pkgFacts{}, err
	}
	fset := token.NewFileSet()
	var f pkgFacts
	var clientField, clientAccessor, nowField bool
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, n), nil, 0)
		if err != nil {
			return pkgFacts{}, err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.TypeSpec:
				st, ok := v.Type.(*ast.StructType)
				if !ok || v.Name.Name != "Deps" {
					return true
				}
				f.hasDeps = true
				for _, field := range st.Fields.List {
					for _, fn := range field.Names {
						switch fn.Name {
						case "Client":
							clientField = true
						case "Now":
							nowField = true
						}
					}
				}
			case *ast.FuncDecl:
				// The generated Run calls deps.client(ctx), so the
				// accessor has to exist as well as the field.
				if v.Name.Name == "client" && receiverIs(v, "Deps") {
					clientAccessor = true
				}
			}
			return true
		})
	}
	f.hasClient = clientField && clientAccessor
	f.hasNow = nowField
	return f, nil
}

// receiverIs reports whether fn is a method on the named type, by
// value or by pointer.
func receiverIs(fn *ast.FuncDecl, name string) bool {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return false
	}
	t := fn.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	id, ok := t.(*ast.Ident)
	return ok && id.Name == name
}

type data struct {
	Group    string
	Check    string
	Exported string
	Name     string
	MCPName  string
	Summary  string
	Kind     string
	Deps     bool
	Client   bool
	Now      bool
}

// render is a deliberately small template evaluator: {{Field}} for a
// value and {{#Field}}…{{/Field}} for a boolean section. text/template
// would work, but its own delimiters collide with Go source often
// enough in templates that ARE Go source that the escaping becomes the
// hard part to read.
//
// A section marker alone on a line takes that whole line with it, so
// the templates stay readable without leaving a blank line behind
// wherever a section was switched off.
func render(tmpl string, d data) string {
	fields := map[string]string{
		"Group": d.Group, "Check": d.Check, "Exported": d.Exported,
		"Name": d.Name, "MCPName": d.MCPName, "Summary": d.Summary,
		"Kind": d.Kind,
	}
	sections := map[string]bool{"Deps": d.Deps, "Client": d.Client, "Now": d.Now}

	out := tmpl
	for name, on := range sections {
		for _, re := range []*regexp.Regexp{
			regexp.MustCompile(`(?s)[ \t]*\{\{#` + name + `\}\}\n(.*?)[ \t]*\{\{/` + name + `\}\}\n`),
			regexp.MustCompile(`(?s)\{\{#` + name + `\}\}(.*?)\{\{/` + name + `\}\}`),
		} {
			out = re.ReplaceAllStringFunc(out, func(m string) string {
				if !on {
					return ""
				}
				return re.FindStringSubmatch(m)[1]
			})
		}
	}
	for name, val := range fields {
		out = strings.ReplaceAll(out, "{{"+name+"}}", val)
	}
	return out
}

const license = `// Copyright 2026 Google LLC
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

`

const commandTemplate = license + `package {{Group}}

// ` + "`{{Name}}`" + ` (DESIGN.md §5): TODO — one paragraph on WHY this
// claim is worth making. What breaks when it goes unnoticed, why it is
// invisible from the object's own status, and which look-alike shapes
// must stay silent. The convention here is that the comment carries
// the reasoning and the code carries the mechanism; a comment that
// restates the code below is worse than none.
//
// Finding kinds and severities:
//
//	{{Kind}}  warning  TODO: the one-line claim
//
// Healthy objects emit nothing (§4.2 zero nominal state).

import (
	"context"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func init() {
	// Some groups (audit) register every command from one init in the
	// group file instead. If yours does, move this line there and
	// delete this block.
	checks.Register({{Exported}}Command({{#Deps}}Deps{}{{/Deps}}))
}

// {{Exported}}Command builds the ` + "`lookout {{Name}}`" + ` command.
func {{Exported}}Command({{#Deps}}deps Deps{{/Deps}}) checks.Command {
	return checks.Command{
		Name:    "{{Name}}",
		MCPName: "{{MCPName}}",
		Summary: "{{Summary}}",
		Flags: []emit.FlagSpec{
			// TODO: every flag is declared here and nowhere else — the
			// declaration generates --help, the MCP input schema, and
			// the reference pages. Delete this field if there are none.
		},
		Kinds: []checks.KindField{
			// The ledger: every kind this command can emit, and every
			// severity it carries each at. Emitting one that is not
			// here fails the contract test and the source sweep.
			checks.Kind("{{Kind}}", "TODO: what is true of the subject when this fires, in the present tense, written for someone deciding whether to act", emit.SeverityWarning),
		},
		Output: []checks.OutputField{
			// TODO: every detail field beyond the shared envelope.
			// checktest.VerifyContract fails on an undeclared one.
		},
		Examples: []string{
			"lookout {{Name}}",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return run{{Exported}}(ctx, {{#Deps}}deps, {{/Deps}}inv)
		},
	}
}

func run{{Exported}}(ctx context.Context, {{#Deps}}deps Deps, {{/Deps}}inv emit.Invocation) (int, error) {
	// Reject a malformed invocation with emit.UsageErrorf, which exits
	// 2. A caller has to be able to tell "the cluster is unreachable"
	// (retry) from "you typed it wrong" (do not retry).
{{#Client}}
	if _, err := deps.client(ctx); err != nil {
		return 0, err
	}

{{/Client}}
	// TODO: list what the claim needs, judge each object, and emit one
	// finding per defect through inv.Out.Emit.
	//
	// Return the number of objects EXAMINED, not the number of
	// findings: scanned= is a coverage claim (§11). Reporting a count
	// you did not actually look at is the one thing this envelope
	// cannot forgive.
	return 0, nil
}
`

const testTemplate = license + `package {{Group}}_test

// ` + "`{{Name}}`" + ` tests, §13 conventions:{{#Client}} fake.Clientset fixtures,{{/Client}}
// an exact assertion per defect, a healthy fixture proving zero nominal
// state, one golden over a mixed cluster, and the checktest contract
// round-trip in both formats.
//
// The silent cases carry as much weight as the loud ones. Every rule
// worth writing has a legitimate look-alike, and a check that fires on
// one is worse than no check at all.

import (
{{#Client}}
	"context"
{{/Client}}
	"strings"
	"testing"
{{#Client}}

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
{{/Client}}

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/{{Group}}"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

func {{Check}}TestCommand({{#Client}}objs ...runtime.Object{{/Client}}) checks.Command {
{{#Client}}
	cs := fake.NewClientset(objs...)
{{/Client}}
	return {{Group}}.{{Exported}}Command({{#Deps}}{{Group}}.Deps{
{{#Client}}
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
{{/Client}}
	}{{/Deps}})
}

// A cluster with nothing wrong produces nothing: no "0 problems found"
// line, no per-object all-clear (§4.2). findings=0 in the summary is
// the whole report.
func Test{{Exported}}QuietWhenHealthy(t *testing.T) {
	res := checktest.Run(t, {{Check}}TestCommand())
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "findings=0") {
		t.Errorf("a healthy cluster must produce nothing:\n%s", res.Stdout)
	}
}

// TODO: one test per finding kind, asserting the exact fields — the
// kind, the severity, the reason, and every detail the reader acts on.
// A test that only counts findings does not catch the field that
// stopped being emitted.

// Test{{Exported}}Contract round-trips the output against the declared
// metadata in both formats: an emitted field the Output glossary does
// not declare, or a kind the ledger does not, fails here.
func Test{{Exported}}Contract(t *testing.T) {
	checktest.VerifyContract(t, {{Check}}TestCommand())
}

// Test{{Exported}}Golden pins the whole payload over one mixed cluster:
// ordering, formatting, and the summary line. Rerun with
// UPDATE_GOLDEN=1 to accept a change, after reading the diff.
func Test{{Exported}}Golden(t *testing.T) {
	res := checktest.Run(t, {{Check}}TestCommand( /* TODO: the mixed fixture */ ))
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/{{Check}}.golden", res.Stdout)
}
`

const checklistTemplate = `
` + "`{{Name}}`" + ` is registered, tested, and green. What is left is
what a generator cannot decide:

  1. Write the claim. The rationale comment, the Kinds ledger entry,
     the Output glossary, and run{{Exported}}. Start with the ledger —
     naming the kinds first is what keeps the finding set from growing
     into whatever the code happened to emit.

  2. Decide whether a bare ` + "`lookout scan`" + ` should run it.
     pkg/checks/scan: add "{{Name}}" to Stage1, or to Excluded with a
     reason. The coverage test fails until you do — that is deliberate.

  3. RBAC, if the check lists a kind nothing else does. Add it to
     state.LoadClusterListRequirements and to
     deploy/12-clusterrole-watcher.yaml; state/rbac_test.go parses the
     manifest against the requirements and will tell you.

  4. Skills, if a workflow should reach for it. Add "{{Name}}" to the
     relevant entry of skilldoc.SkillCommands, and mention it in that
     skill's SKILL.md decision tree.

  5. Regenerate the docs, in this order:
       ./dev/tools/gen-skill-refs
       ./dev/tools/gen-site-docs

  6. ./dev/tools/ci

docs/adding-a-check.md walks a real check end to end.
`
