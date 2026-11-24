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

package sitedoc_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/internal/watch"
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

var (
	mermaidBlockRe    = regexp.MustCompile("(?s)```mermaid[ \\t]*\\r?\\n(.*?)```")
	mermaidFlagRe     = regexp.MustCompile(`(^|[^a-zA-Z0-9_.-])--([a-z][a-z0-9-]*)`)
	mermaidSinkValRe  = regexp.MustCompile(`--sink=([a-z0-9-]+)`)
	mermaidSrcValRe   = regexp.MustCompile(`--sources=([a-z0-9.,-]+)`)
	mermaidCylinderRe = regexp.MustCompile(`\[\("([a-z0-9_]+)(?:<br/>|["\s])`)
	mermaidMCPToolRe  = regexp.MustCompile(`\bk8s_[a-z0-9_]+\b`)
	mermaidAuditCmdRe = regexp.MustCompile(`\baudit\s+([a-z][a-z0-9-]*)\b`)
	mermaidDeployRe   = regexp.MustCompile(`\bdeploy/[a-z0-9/*_-]+`)
	mcpFlagDeclRe     = regexp.MustCompile(`\bfs\.(?:String|Bool|Int|Duration)\("([a-z0-9-]+)"`)
	hyphenIdentRe     = regexp.MustCompile(`\b[a-z][a-z0-9]*(?:-[a-z0-9]+)+\b`)
)

// retiredMermaidTokens are source and SQLite table identifiers retired in #842
// that must never reappear in sentinel or store diagrams.
var retiredMermaidTokens = []string{
	"topology-drift",
	"compute-class",
	"leeway_alert_state",
	"leeway_baseline",
}

type mermaidVocab struct {
	flags       map[string]bool
	sources     map[string]bool
	autoSources []string
	explicitSrc []string
	sinks       map[string]bool
	tables      map[string]bool
	mcpTools    map[string]bool
	auditCmds   map[string]bool
	deployDirs  map[string]bool
	deployComps []string
}

func buildMermaidVocab(t *testing.T) mermaidVocab {
	t.Helper()

	flags := map[string]bool{}
	for _, fd := range watch.FlagInventory() {
		flags[fd.Name] = true
	}
	for _, fs := range emit.CommonFlags() {
		flags[fs.Name] = true
	}
	for _, fs := range emit.GraphHistoryFlags() {
		flags[fs.Name] = true
	}
	for _, c := range checks.All() {
		for _, fs := range c.Flags {
			flags[fs.Name] = true
		}
	}
	mcpMatches, err := filepath.Glob(filepath.Join(repoRoot, "cmd", "lookout", "mcp*.go"))
	if err != nil || len(mcpMatches) == 0 {
		t.Fatalf("glob cmd/lookout/mcp*.go: %v", err)
	}
	for _, p := range mcpMatches {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		b, rerr := os.ReadFile(p)
		if rerr != nil {
			t.Fatalf("read %s: %v", p, rerr)
		}
		for _, m := range mcpFlagDeclRe.FindAllStringSubmatch(string(b), -1) {
			flags[m[1]] = true
		}
	}

	knownSrc := watch.KnownSources()
	autoSrc := watch.AutoSources()
	sources := make(map[string]bool, len(knownSrc))
	for _, s := range knownSrc {
		sources[s] = true
	}
	autoSet := make(map[string]bool, len(autoSrc))
	for _, s := range autoSrc {
		autoSet[s] = true
	}
	var explicitSrc []string
	for _, s := range knownSrc {
		if !autoSet[s] {
			explicitSrc = append(explicitSrc, s)
		}
	}

	sinks := map[string]bool{}
	for _, s := range watch.KnownSinks() {
		sinks[s] = true
	}

	// Derive active SQLite tables directly from a migrated store.Open database,
	// excluding schema_version metadata and the retired #842 leeway tables.
	dbPath := filepath.Join(t.TempDir(), "diagram-vocab.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	_ = st.Close()

	rawDB, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = rawDB.Close() }()

	rows, err := rawDB.Query(`SELECT name FROM sqlite_master WHERE type = 'table'`)
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	defer func() { _ = rows.Close() }()

	retiredTables := map[string]bool{
		"schema_version":     true,
		"leeway_alert_state": true,
		"leeway_baseline":    true,
	}
	tables := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		if strings.HasPrefix(name, "sqlite_") || retiredTables[name] {
			continue
		}
		tables[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sqlite_master: %v", err)
	}

	mcpTools := map[string]bool{
		"k8s_list_clusters": true, // multi-cluster meta-tool served by internal/mcpserver
	}
	for _, c := range checks.All() {
		if c.MCPName != "" {
			mcpTools[c.MCPName] = true
		}
	}

	auditCmds := map[string]bool{}
	for _, c := range checks.Default().GroupCommands("audit") {
		if sub, ok := strings.CutPrefix(c.Name, "audit "); ok {
			auditCmds[sub] = true
		}
	}

	deployDirs := map[string]bool{}
	var deployComps []string
	compRoot := filepath.Join(repoRoot, "deploy", "components")
	compEntries, err := os.ReadDir(compRoot)
	if err != nil {
		t.Fatalf("read deploy/components: %v", err)
	}
	for _, e := range compEntries {
		if e.IsDir() {
			deployComps = append(deployComps, e.Name())
		}
	}
	slices.Sort(deployComps)

	err = filepath.WalkDir(filepath.Join(repoRoot, "deploy"), func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || !d.IsDir() {
			return walkErr
		}
		rel, rerr := filepath.Rel(repoRoot, path)
		if rerr != nil {
			return rerr
		}
		deployDirs[filepath.ToSlash(rel)] = true
		deployDirs[filepath.ToSlash(rel)+"/"] = true
		return nil
	})
	if err != nil {
		t.Fatalf("walk deploy/: %v", err)
	}

	return mermaidVocab{
		flags:       flags,
		sources:     sources,
		autoSources: autoSrc,
		explicitSrc: explicitSrc,
		sinks:       sinks,
		tables:      tables,
		mcpTools:    mcpTools,
		auditCmds:   auditCmds,
		deployDirs:  deployDirs,
		deployComps: deployComps,
	}
}

func validateMermaidBlock(src string, v mermaidVocab) error {
	trimmed := strings.TrimSpace(src)
	if trimmed == "" {
		return fmt.Errorf("empty mermaid block")
	}
	firstLine := strings.TrimSpace(strings.SplitN(trimmed, "\n", 2)[0])
	if !strings.HasPrefix(firstLine, "flowchart ") && !strings.HasPrefix(firstLine, "graph ") && !strings.HasPrefix(firstLine, "sequenceDiagram") {
		return fmt.Errorf("unsupported mermaid diagram header %q", firstLine)
	}

	var subgraphs, ends int
	for _, line := range strings.Split(trimmed, "\n") {
		l := strings.TrimSpace(line)
		if strings.HasPrefix(l, "subgraph ") {
			subgraphs++
		} else if l == "end" {
			ends++
		}
	}
	if subgraphs != ends {
		return fmt.Errorf("unbalanced subgraph/end count (%d subgraph vs %d end)", subgraphs, ends)
	}

	for _, retired := range retiredMermaidTokens {
		if strings.Contains(trimmed, retired) {
			return fmt.Errorf("diagram contains retired #842 token %q", retired)
		}
	}

	for _, m := range mermaidFlagRe.FindAllStringSubmatch(trimmed, -1) {
		name := m[2]
		if !v.flags[name] {
			return fmt.Errorf("unknown CLI flag --%s in diagram", name)
		}
	}

	for _, m := range mermaidSinkValRe.FindAllStringSubmatch(trimmed, -1) {
		sink := m[1]
		if !v.sinks[sink] {
			return fmt.Errorf("unknown --sink=%s in diagram", sink)
		}
	}

	for _, m := range mermaidSrcValRe.FindAllStringSubmatch(trimmed, -1) {
		for _, part := range strings.Split(m[1], ",") {
			if part == "" || part == "auto" || part == "..." {
				continue
			}
			if !v.sources[part] {
				return fmt.Errorf("unknown --sources entry %q in diagram", part)
			}
		}
	}

	// Inside `subgraph Srcs[...]`, every hyphenated identifier other than flag
	// tokens and descriptive prose compounds (`auto-probed`, `explicit-only`,
	// `cost-stack`) must be a known source name so a typo'd or retired source
	// in the Sentinel source node fails immediately.
	inSrcs := false
	srcsAllow := map[string]bool{
		"sources":       true,
		"auto-probed":   true,
		"explicit-only": true,
	}
	for _, line := range strings.Split(trimmed, "\n") {
		l := strings.TrimSpace(line)
		if strings.HasPrefix(l, "subgraph Srcs[") {
			inSrcs = true
			continue
		}
		if inSrcs && l == "end" {
			inSrcs = false
			continue
		}
		if !inSrcs {
			continue
		}
		withoutFlags := mermaidFlagRe.ReplaceAllString(l, "$1")
		for _, tok := range hyphenIdentRe.FindAllString(strings.ToLower(withoutFlags), -1) {
			if srcsAllow[tok] {
				continue
			}
			if !v.sources[tok] {
				return fmt.Errorf("unknown signal source %q in Srcs subgraph", tok)
			}
		}
	}

	for _, m := range mermaidCylinderRe.FindAllStringSubmatch(trimmed, -1) {
		tbl := m[1]
		// Multi-table summary node `DB[("SQLite Store ...")]` starts with
		// uppercase `SQLite`, so lowercase-only `mermaidCylinderRe` matches
		// individual table cylinders such as `Tocc[("occurrences<br/>...")]`.
		if !v.tables[tbl] {
			return fmt.Errorf("unknown or retired SQLite table cylinder %q in diagram", tbl)
		}
	}

	for _, m := range mermaidMCPToolRe.FindAllString(trimmed, -1) {
		if !v.mcpTools[m] {
			return fmt.Errorf("unknown MCP tool %q in diagram", m)
		}
	}

	for _, m := range mermaidAuditCmdRe.FindAllStringSubmatch(trimmed, -1) {
		sub := m[1]
		// Skip English prose words if any appear after `audit` (`audit policy`
		// IS a real command, so only non-command nouns would need care; none
		// appear in our diagrams, so keep this strict!).
		if !v.auditCmds[sub] {
			return fmt.Errorf("unknown `audit %s` subcommand in diagram", sub)
		}
	}

	for _, m := range mermaidDeployRe.FindAllString(trimmed, -1) {
		clean := strings.TrimSuffix(m, "/*")
		if !v.deployDirs[clean] {
			return fmt.Errorf("unknown deployment path %q in diagram", m)
		}
	}

	return nil
}

func TestDocsSiteMermaidDiagramsMatchLiveCode(t *testing.T) {
	t.Parallel()

	siteDir := filepath.Join(repoRoot, "docs", "site")
	docsContentDir := filepath.Join(siteDir, "src", "content", "docs")

	// 1. Verify hermetic Vite-bundled Mermaid integration (no external CDN URLs).
	themeProvider, err := os.ReadFile(filepath.Join(siteDir, "src", "components", "ThemeProvider.astro"))
	if err != nil {
		t.Fatalf("read ThemeProvider.astro: %v", err)
	}
	if !strings.Contains(string(themeProvider), "../scripts/mermaid-render.js") {
		t.Errorf("ThemeProvider.astro does not load ../scripts/mermaid-render.js")
	}

	renderJS, err := os.ReadFile(filepath.Join(siteDir, "src", "scripts", "mermaid-render.js"))
	if err != nil {
		t.Fatalf("read src/scripts/mermaid-render.js: %v", err)
	}
	for lineno, line := range strings.Split(string(renderJS), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "*") {
			continue
		}
		if strings.Contains(trimmed, "https://") || strings.Contains(trimmed, "http://") {
			t.Errorf("src/scripts/mermaid-render.js:%d contains an external HTTP(S) URL; keep Mermaid hermetically bundled via npm", lineno+1)
		}
	}

	pkgJSON, err := os.ReadFile(filepath.Join(siteDir, "package.json"))
	if err != nil {
		t.Fatalf("read docs/site/package.json: %v", err)
	}
	if !strings.Contains(string(pkgJSON), `"mermaid"`) {
		t.Errorf("docs/site/package.json does not declare mermaid dependency")
	}
	lockBytes, err := os.ReadFile(filepath.Join(siteDir, "package-lock.json"))
	if err != nil {
		t.Fatalf("read docs/site/package-lock.json: %v", err)
	}
	var lockFile struct {
		Packages map[string]struct {
			Resolved string `json:"resolved"`
		} `json:"packages"`
	}
	if err := json.Unmarshal(lockBytes, &lockFile); err != nil {
		t.Fatalf("parse docs/site/package-lock.json: %v", err)
	}
	for pkgKey, meta := range lockFile.Packages {
		if pkgKey == "" || meta.Resolved == "" {
			continue
		}
		if !strings.HasPrefix(meta.Resolved, "https://registry.npmjs.org/") {
			t.Errorf("docs/site/package-lock.json %s has non-public resolved URL %q; normalize to https://registry.npmjs.org/ so GitHub Actions npm ci succeeds", pkgKey, meta.Resolved)
		}
	}

	// 2. Verify docs/repo-map.md links to the site diagrams instead of keeping ASCII sketches.
	repoMapBytes, err := os.ReadFile(filepath.Join(repoRoot, "docs", "repo-map.md"))
	if err != nil {
		t.Fatalf("read docs/repo-map.md: %v", err)
	}
	repoMap := string(repoMapBytes)
	if start := strings.Index(repoMap, "## The two data paths"); start >= 0 {
		rest := repoMap[start+len("## The two data paths"):]
		if end := strings.Index(rest, "\n## "); end >= 0 {
			rest = rest[:end]
		}
		if strings.Contains(rest, "```") {
			t.Errorf("docs/repo-map.md ## The two data paths still contains a fenced ASCII block; link to the site diagrams instead")
		}
		for _, wantURL := range []string{
			"https://go-steer.github.io/k8s-lookout/concepts/architecture/#the-read-path",
			"https://go-steer.github.io/k8s-lookout/concepts/architecture/#the-watch-path",
			"https://go-steer.github.io/k8s-lookout/getting-started/mcp/",
			"https://go-steer.github.io/k8s-lookout/guides/posture-audit/",
			"https://go-steer.github.io/k8s-lookout/operations/store/",
			"https://go-steer.github.io/k8s-lookout/getting-started/deploy/",
		} {
			if !strings.Contains(rest, wantURL) {
				t.Errorf("docs/repo-map.md ## The two data paths missing link %q", wantURL)
			}
		}
	} else {
		t.Errorf("docs/repo-map.md missing ## The two data paths section")
	}

	// 3. Walk every docs-site Markdown file and validate every Mermaid block.
	vocab := buildMermaidVocab(t)
	blocksByPage := map[string][]string{}

	err = filepath.WalkDir(docsContentDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		if ext := filepath.Ext(path); ext != ".md" && ext != ".mdx" {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(docsContentDir, path)
		rel = filepath.ToSlash(rel)
		matches := mermaidBlockRe.FindAllStringSubmatch(string(raw), -1)
		for i, m := range matches {
			blocksByPage[rel] = append(blocksByPage[rel], m[1])
			if verr := validateMermaidBlock(m[1], vocab); verr != nil {
				t.Errorf("%s mermaid block #%d: %v", rel, i+1, verr)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", docsContentDir, err)
	}

	wantMinBlocks := map[string]int{
		"concepts/architecture.md":  2, // read-path + watch-path (sentinel)
		"getting-started/deploy.md": 1,
		"getting-started/mcp.md":    1,
		"guides/posture-audit.md":   1,
		"operations/store.md":       1,
	}
	for page, minCount := range wantMinBlocks {
		if got := len(blocksByPage[page]); got < minCount {
			t.Errorf("docs/site/src/content/docs/%s has %d ```mermaid block(s), want >= %d", page, got, minCount)
		}
	}

	// 4. Completeness checks: adding a new source, sink, SQLite table, audit
	// detector, or deploy component must fail CI until its diagram includes it.
	archJoined := strings.Join(blocksByPage["concepts/architecture.md"], "\n")
	for _, s := range vocab.autoSources {
		if !strings.Contains(archJoined, s) {
			t.Errorf("concepts/architecture.md sentinel diagram missing auto source %q", s)
		}
	}
	for _, s := range vocab.explicitSrc {
		if !strings.Contains(archJoined, s) {
			t.Errorf("concepts/architecture.md sentinel diagram missing explicit-only source %q", s)
		}
	}
	for sink := range vocab.sinks {
		if !strings.Contains(archJoined, "--sink="+sink) {
			t.Errorf("concepts/architecture.md sentinel diagram missing --sink=%s", sink)
		}
	}

	storeJoined := strings.Join(blocksByPage["operations/store.md"], "\n")
	seenTables := map[string]bool{}
	for _, m := range mermaidCylinderRe.FindAllStringSubmatch(storeJoined, -1) {
		seenTables[m[1]] = true
	}
	for tbl := range vocab.tables {
		if !seenTables[tbl] {
			t.Errorf("operations/store.md store diagram missing active SQLite table %q", tbl)
		}
	}

	postureJoined := strings.Join(blocksByPage["guides/posture-audit.md"], "\n")
	for sub := range vocab.auditCmds {
		if !strings.Contains(postureJoined, "audit "+sub) {
			t.Errorf("guides/posture-audit.md posture diagram missing `audit %s`", sub)
		}
	}

	deployJoined := strings.Join(blocksByPage["getting-started/deploy.md"], "\n")
	for _, comp := range vocab.deployComps {
		if !strings.Contains(deployJoined, comp) {
			t.Errorf("getting-started/deploy.md deployment diagram missing deploy/components/%s", comp)
		}
	}
}

func TestValidateMermaidBlock_RefusesStaleTokens(t *testing.T) {
	t.Parallel()

	vocab := buildMermaidVocab(t)

	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "retired topology-drift source",
			src:  "flowchart TB\n  A[\"topology-drift\"] --> B[\"ok\"]",
			want: "retired #842 token \"topology-drift\"",
		},
		{
			name: "retired compute-class source",
			src:  "flowchart TB\n  A[\"compute-class\"] --> B[\"ok\"]",
			want: "retired #842 token \"compute-class\"",
		},
		{
			name: "retired leeway_alert_state table",
			src:  "flowchart LR\n  T[(\"leeway_alert_state<br/>rows\")] --> R[\"reader\"]",
			want: "retired #842 token \"leeway_alert_state\"",
		},
		{
			name: "unknown source in Srcs subgraph",
			src:  "flowchart TB\n  subgraph Srcs[\"Sources\"]\n    A[\"k8s-events, bogus-source\"]\n  end",
			want: "unknown signal source \"bogus-source\"",
		},
		{
			name: "unknown --sources entry",
			src:  "flowchart TB\n  A[\"--sources=k8s-events,ghost\"] --> B[\"ok\"]",
			want: "unknown --sources entry \"ghost\"",
		},
		{
			name: "unknown --sink value",
			src:  "flowchart TB\n  A[\"--sink=pagerduty\"] --> B[\"ok\"]",
			want: "unknown --sink=pagerduty",
		},
		{
			name: "unknown SQLite table cylinder",
			src:  "flowchart LR\n  T[(\"nonexistent_table<br/>desc\")] --> R[\"reader\"]",
			want: "unknown or retired SQLite table cylinder \"nonexistent_table\"",
		},
		{
			name: "unknown CLI flag",
			src:  "flowchart TB\n  A[\"--made-up-flag\"] --> B[\"ok\"]",
			want: "unknown CLI flag --made-up-flag",
		},
		{
			name: "unknown MCP tool",
			src:  "flowchart TB\n  A[\"k8s_nonexistent_tool\"] --> B[\"ok\"]",
			want: "unknown MCP tool \"k8s_nonexistent_tool\"",
		},
		{
			name: "unknown audit command",
			src:  "flowchart TB\n  A[\"audit nonexistent\"] --> B[\"ok\"]",
			want: "unknown `audit nonexistent` subcommand",
		},
		{
			name: "unknown deploy directory",
			src:  "flowchart TB\n  A[\"deploy/components/ghost\"] --> B[\"ok\"]",
			want: "unknown deployment path \"deploy/components/ghost\"",
		},
		{
			name: "unbalanced subgraph",
			src:  "flowchart TB\n  subgraph S[\"One\"]\n    A[\"node\"]",
			want: "unbalanced subgraph/end count",
		},
		{
			name: "empty mermaid block",
			src:  "",
			want: "empty mermaid block",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMermaidBlock(tc.src, vocab)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateMermaidBlock() error = %v, want substring %q", err, tc.want)
			}
		})
	}
}
