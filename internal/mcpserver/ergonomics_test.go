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

package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// Item 1 (#867): MCP usage errors omit the CLI `--help` hint and name MCP
// JSON property names rather than CLI flags, while preserving server-scoped
// flags and masking secrets.
func TestMCPUsageErrorOmitsHelpAndUsesPropertyNames(t *testing.T) {
	reg := checks.NewRegistry()
	reg.Register(checks.Command{
		Name:    "audit fake",
		MCPName: "k8s_audit_fake",
		Summary: "Audit fake resources. Scope with --namespace, -A, or --workload; see --min-severity and --exemptions.",
		Flags: []emit.FlagSpec{
			{Name: "min-severity", Type: emit.FlagString, Help: "minimum severity (pairs with --namespace and -A)"},
		},
		Kinds: []checks.KindField{checks.Kind("fake.audit", "synthetic", emit.SeverityInfo)},
		Run: func(_ context.Context, inv emit.Invocation) (int, error) {
			if inv.Flags.String("min-severity") == "secret" {
				return 0, emit.UsageErrorf("bad --min-severity with token Bearer ghp_123456789012345678901234567890123456; pass --exemptions=<path>")
			}
			if inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces && inv.Scope.Workload.IsZero() {
				return 0, emit.UsageErrorf("no scope: pass --namespace=<ns>, -A, or --workload=<Kind>/<ns>/<name>")
			}
			return 1, nil
		},
	})
	cs := connect(t, New(reg, "test"))

	// 1. Check-returned UsageErrorf: rewrites --namespace, -A, --workload and omits --help.
	_, err := callTool(t, cs, "k8s_audit_fake", map[string]any{})
	wantInvalidParams(t, err, "no scope: pass namespace=<ns>, all_namespaces, or workload=<Kind>/<ns>/<name>")
	if strings.Contains(err.Error(), "--help") || strings.Contains(err.Error(), "--namespace") || strings.Contains(err.Error(), "-A") || strings.Contains(err.Error(), "--workload") {
		t.Errorf("CLI wording leaked into MCP usage error: %q", err.Error())
	}

	// 2. Go flag parse error: rewrites `for flag -timeout` and omits --help.
	_, err = callTool(t, cs, "k8s_audit_fake", map[string]any{"timeout": "notaduration"})
	wantInvalidParams(t, err, `for argument "timeout"`)
	if strings.Contains(err.Error(), "--help") || strings.Contains(err.Error(), "for flag -timeout") {
		t.Errorf("CLI flag parse wording leaked into MCP error: %q", err.Error())
	}

	// 3. Server-scoped flags (--exemptions) stay CLI-worded and secrets stay masked.
	_, err = callTool(t, cs, "k8s_audit_fake", map[string]any{"min_severity": "secret"})
	wantInvalidParams(t, err, "bad min_severity")
	if !strings.Contains(err.Error(), "--exemptions=<path>") {
		t.Errorf("server-scoped flag should stay --exemptions: %q", err.Error())
	}
	if strings.Contains(err.Error(), "ghp_1234567890") {
		t.Errorf("secret leaked into MCP usage error: %q", err.Error())
	}

	// 4. Tool & property descriptions rewrite MCP flags and keep server-scoped flags.
	tools, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	if len(tools.Tools) != 1 {
		t.Fatalf("got %d tools, want 1", len(tools.Tools))
	}
	desc := tools.Tools[0].Description
	if !strings.Contains(desc, "Scope with namespace, all_namespaces, or workload; see min_severity and --exemptions.") {
		t.Errorf("tool description did not translate flags cleanly:\n%s", desc)
	}
	var schema jsonschema.Schema
	raw, _ := json.Marshal(tools.Tools[0].InputSchema)
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if got := schema.Properties["min_severity"].Description; got != "minimum severity (pairs with namespace and all_namespaces)" {
		t.Errorf("property description = %q", got)
	}
}

// Item 2 (#867): commands declaring MCPDefaultAllNamespaces default to
// all_namespaces=true on MCP when no scope property is supplied, while
// explicit namespace, workload, target alias, or all_namespaces=false
// override the default cleanly.
func TestMCPDefaultAllNamespaces(t *testing.T) {
	reg := checks.NewRegistry()
	reg.Register(checks.Command{
		Name:                    "audit sample",
		MCPName:                 "k8s_audit_sample",
		Summary:                 "Sample namespace-scoped audit. Scope with --namespace, -A, or --workload.",
		MCPDefaultAllNamespaces: true,
		Kinds:                   []checks.KindField{checks.Kind("sample.scope", "observed scope", emit.SeverityInfo)},
		Output: []checks.OutputField{
			{Name: "ns", Doc: "observed namespace"},
			{Name: "all_ns", Doc: "observed all_namespaces"},
			{Name: "wl", Doc: "observed workload"},
		},
		Run: func(_ context.Context, inv emit.Invocation) (int, error) {
			if inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces && inv.Scope.Workload.IsZero() {
				return 0, emit.UsageErrorf("no scope: pass --namespace=<ns>, -A, or --workload=<Kind>/<ns>/<name>")
			}
			return 1, inv.Out.Emit(emit.Finding{
				Kind:     "sample.scope",
				Severity: emit.SeverityInfo,
				Details: []emit.Field{
					{Key: "ns", Value: inv.Scope.Namespace},
					{Key: "all_ns", Value: strconv.FormatBool(inv.Scope.AllNamespaces)},
					{Key: "wl", Value: inv.Scope.Workload.String()},
				},
			})
		},
	})
	cs := connect(t, New(reg, "test"))

	// Schema declares default: true on all_namespaces and notes the default in description.
	list, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	tool := list.Tools[0]
	if !strings.Contains(tool.Description, "Default scope on MCP: all_namespaces=true") {
		t.Errorf("description missing MCP default note:\n%s", tool.Description)
	}
	var schema jsonschema.Schema
	raw, _ := json.Marshal(tool.InputSchema)
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if string(schema.Properties["all_namespaces"].Default) != "true" {
		t.Errorf("all_namespaces default = %s, want true", string(schema.Properties["all_namespaces"].Default))
	}

	// Empty args, blank namespace, and JSON nulls on optional scope
	// properties all default to all_namespaces=true.
	for _, args := range []map[string]any{
		{},
		{"namespace": ""},
		{"namespace": nil},
		{"all_namespaces": nil},
		{"workload": nil, "all_namespaces": nil},
	} {
		res, err := callTool(t, cs, "k8s_audit_sample", args)
		if err != nil || res.IsError {
			t.Fatalf("args %v failed: %v %+v", args, err, res)
		}
		if out := text(t, res); !strings.Contains(out, "all_ns=true") {
			t.Errorf("args %v did not default to all_namespaces=true:\n%s", args, out)
		}
	}
	// Unknown property with null is still rejected as an unknown argument.
	_, err = callTool(t, cs, "k8s_audit_sample", map[string]any{"bogus": nil})
	wantInvalidParams(t, err, `unknown argument "bogus"`)

	// Explicit namespace overrides without injecting all_namespaces.
	res, err := callTool(t, cs, "k8s_audit_sample", map[string]any{"namespace": "prod"})
	if err != nil || res.IsError {
		t.Fatalf("namespace override failed: %v %+v", err, res)
	}
	if out := text(t, res); !strings.Contains(out, "ns=prod") || !strings.Contains(out, "all_ns=false") {
		t.Errorf("namespace override unexpected:\n%s", out)
	}

	// Explicit workload and target alias override without injecting all_namespaces.
	for _, key := range []string{"workload", "target"} {
		res, err := callTool(t, cs, "k8s_audit_sample", map[string]any{key: "Deployment/prod/api"})
		if err != nil || res.IsError {
			t.Fatalf("%s override failed: %v %+v", key, err, res)
		}
		if out := text(t, res); !strings.Contains(out, "wl=Deployment/prod/api") || !strings.Contains(out, "all_ns=false") {
			t.Errorf("%s override unexpected:\n%s", key, out)
		}
	}

	// Explicit all_namespaces=false disables the default and hits the MCP-worded usage error.
	_, err = callTool(t, cs, "k8s_audit_sample", map[string]any{"all_namespaces": false})
	wantInvalidParams(t, err, "no scope: pass namespace=<ns>, all_namespaces, or workload=<Kind>/<ns>/<name>")
}

// Item 3 (#867): commands declaring InapplicableCommonFlags omit those
// properties from inputSchema, reject them in argv (including the target
// alias when workload is inapplicable), and never suggest them in
// response_hint narrowing text.
func TestStripInapplicableCommonFlagsAndNarrowing(t *testing.T) {
	clusterOnly := checks.Command{
		Name:                    "audit clusteronly",
		MCPName:                 "k8s_audit_clusteronly",
		Summary:                 "Cluster-only check; takes no --namespace/-A/--workload.",
		InapplicableCommonFlags: []string{"namespace", "A", "workload"},
		Kinds:                   []checks.KindField{checks.Kind("fake.cluster", "synthetic", emit.SeverityInfo)},
		Run:                     func(_ context.Context, _ emit.Invocation) (int, error) { return 1, nil },
	}
	nsOnly := checks.Command{
		Name:                    "audit nsonly",
		MCPName:                 "k8s_audit_nsonly",
		Summary:                 "Namespace-scoped check; no workload.",
		InapplicableCommonFlags: []string{"workload"},
		MCPDefaultAllNamespaces: true,
		Kinds:                   []checks.KindField{checks.Kind("fake.ns", "synthetic", emit.SeverityInfo)},
		Run:                     func(_ context.Context, _ emit.Invocation) (int, error) { return 1, nil },
	}
	reg := checks.NewRegistry()
	reg.Register(clusterOnly)
	reg.Register(nsOnly)
	cs := connect(t, New(reg, "test"))

	list, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]jsonschema.Schema{}
	for _, tool := range list.Tools {
		var s jsonschema.Schema
		raw, _ := json.Marshal(tool.InputSchema)
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatal(err)
		}
		byName[tool.Name] = s
	}

	// clusterOnly omits namespace, all_namespaces, workload.
	for _, omitted := range []string{"namespace", "all_namespaces", "workload"} {
		if byName["k8s_audit_clusteronly"].Properties[omitted] != nil {
			t.Errorf("k8s_audit_clusteronly schema must omit %q", omitted)
		}
	}
	// nsOnly keeps namespace + all_namespaces and omits workload.
	if byName["k8s_audit_nsonly"].Properties["workload"] != nil {
		t.Error("k8s_audit_nsonly schema must omit workload")
	}
	if byName["k8s_audit_nsonly"].Properties["namespace"] == nil || byName["k8s_audit_nsonly"].Properties["all_namespaces"] == nil {
		t.Error("k8s_audit_nsonly schema must retain namespace and all_namespaces")
	}

	// Sending a stripped property (or target alias when workload is stripped) is rejected.
	for _, prop := range []string{"namespace", "all_namespaces", "workload", "target"} {
		_, err := callTool(t, cs, "k8s_audit_clusteronly", map[string]any{prop: "x"})
		wantInvalidParams(t, err, `unknown argument "`+prop+`"`)
		if strings.Contains(err.Error(), "workload") && prop != "workload" {
			t.Errorf("error accepts list must not advertise stripped property workload: %q", err.Error())
		}
	}
	_, err = callTool(t, cs, "k8s_audit_nsonly", map[string]any{"target": "Deployment/prod/api"})
	wantInvalidParams(t, err, `unknown argument "target"`)

	// Narrowing hints only suggest accepted properties.
	if got := narrowing(clusterOnly, nil); got != "" {
		t.Errorf("narrowing(clusterOnly) = %q, want empty", got)
	}
	if got := responseHint(clusterOnly, nil, 5, 20, 1000); !strings.Contains(got, "this tool has no narrowing arguments") {
		t.Errorf("responseHint(clusterOnly) = %q", got)
	}
	if got := narrowing(nsOnly, nil); got != "namespace" {
		t.Errorf("narrowing(nsOnly) = %q, want namespace", got)
	}
	if got := narrowing(nsOnly, []string{"--namespace=prod"}); got != "" {
		t.Errorf("narrowing(nsOnly with namespace) = %q, want empty", got)
	}
}

// Item 4 (#867): tools declaring MCPRequiresServerFlag are hidden when
// their required server flag is unset, exposed once set, allowed in
// `-<tool>` removals, and rejected with an actionable error if explicitly
// requested without their flag.
func TestUnconfiguredServerFlagToolsOmitted(t *testing.T) {
	reg := checks.NewRegistry()
	reg.Register(checks.Command{
		Name: "triage ok", MCPName: "k8s_triage_ok", MCPProfiles: []string{"triage"},
		Summary: "Always available; test scaffolding only.",
		Kinds:   []checks.KindField{checks.Kind("test.ok", "synthetic", emit.SeverityInfo)},
		Run:     func(_ context.Context, _ emit.Invocation) (int, error) { return 0, nil },
	})
	reg.Register(checks.Command{
		Name: "audit ex", MCPName: "k8s_audit_ex", MCPProfiles: []string{"audit"},
		MCPRequiresServerFlag: "exemptions",
		Summary:               "Needs --exemptions; test scaffolding only.",
		Kinds:                 []checks.KindField{checks.Kind("test.ex", "synthetic", emit.SeverityInfo)},
		Run:                   func(_ context.Context, _ emit.Invocation) (int, error) { return 0, nil },
	})
	reg.Register(checks.Command{
		Name: "audit base", MCPName: "k8s_audit_base", MCPProfiles: []string{"audit"},
		Summary: "Audit base; test scaffolding only.",
		Kinds:   []checks.KindField{checks.Kind("test.base", "synthetic", emit.SeverityInfo)},
		Run:     func(_ context.Context, _ emit.Invocation) (int, error) { return 0, nil },
	})
	reg.Register(checks.Command{
		Name: "findings d", MCPName: "k8s_findings_d", MCPProfiles: []string{"triage"},
		MCPRequiresServerFlag: "store",
		Summary:               "Needs --store; test scaffolding only.",
		Kinds:                 []checks.KindField{checks.Kind("test.diff", "synthetic", emit.SeverityInfo)},
		Run:                   func(_ context.Context, _ emit.Invocation) (int, error) { return 0, nil },
	})

	// Unconfigured New hides both server-flag tools.
	cs := connect(t, New(reg, "test"))
	list, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Tools) != 2 {
		t.Fatalf("unconfigured New served %d tools, want 2", len(list.Tools))
	}

	// Configured New serves all 4 tools.
	csCfg := connect(t, New(reg, "test", WithExemptions("/tmp/e.yaml"), WithStore("/tmp/s.db")))
	listCfg, err := csCfg.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(listCfg.Tools) != 4 {
		t.Fatalf("configured New served %d tools, want 4", len(listCfg.Tools))
	}

	// ResolveToolsFor omits unconfigured tools from profiles/all, accepts
	// `-<tool>` removals, and rejects explicit positive selection.
	sel, err := ResolveToolsFor(reg, "audit", "-k8s_findings_d", ServerFlags{})
	if err != nil {
		t.Fatalf("ResolveToolsFor(audit, -k8s_findings_d): %v", err)
	}
	if sel["k8s_audit_ex"] || !sel["k8s_audit_base"] {
		t.Errorf("unexpected audit selection without --exemptions: %v", sel)
	}
	if _, err := ResolveToolsFor(reg, "", "k8s_audit_ex", ServerFlags{}); err == nil || !strings.Contains(err.Error(), "--exemptions") {
		t.Errorf("explicit k8s_audit_ex without --exemptions: err = %v", err)
	}
	if _, err := ResolveToolsFor(reg, "", "k8s_findings_d", ServerFlags{}); err == nil || !strings.Contains(err.Error(), "--store") {
		t.Errorf("explicit k8s_findings_d without --store: err = %v", err)
	}
}

// Item 5 (#867) unit test: callerLimit.waitAcquireUpTo queues, isolates keys,
// respects context cancellation without leaking slots, and handles double release.
func TestCallerLimitWaitAcquireUpTo(t *testing.T) {
	l := newCallerLimit(1)
	if _, err := l.waitAcquireUpTo(t.Context(), "a", 0); err == nil {
		t.Fatal("max=0 must return an error, not block forever")
	}

	rel1, err := l.waitAcquireUpTo(t.Context(), "cluster-a", 1)
	if err != nil {
		t.Fatal(err)
	}

	// Another key acquires immediately.
	relB, err := l.waitAcquireUpTo(t.Context(), "cluster-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	relB()

	// Waiter canceled via ctx returns promptly without leaking a slot.
	cancelCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := l.waitAcquireUpTo(cancelCtx, "cluster-a", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait = %v, want context.Canceled", err)
	}

	// Second waiter blocks until rel1 runs, then acquires cleanly.
	acquired := make(chan func(), 1)
	go func() {
		rel, err := l.waitAcquireUpTo(t.Context(), "cluster-a", 1)
		if err == nil {
			acquired <- rel
		}
	}()
	// Double-calling rel1 must be idempotent and wake the waiter once.
	rel1()
	rel1()
	select {
	case rel2 := <-acquired:
		// Panic while holding a slot still releases via defer without leaking.
		func() {
			defer func() { _ = recover() }()
			defer rel2()
			panic("simulated tool panic")
		}()
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not wake after release")
	}
	l.mu.Lock()
	inflight, notify := len(l.inflight), len(l.notify)
	l.mu.Unlock()
	if inflight != 0 || notify != 0 {
		t.Errorf("leaked state after all releases: inflight=%d notify=%d", inflight, notify)
	}
}

// resultText extracts text content without calling t.Fatalf, making it safe
// for use inside spawned goroutines.
func resultText(res *mcp.CallToolResult) (string, error) {
	if res == nil || len(res.Content) == 0 {
		return "", errors.New("empty tool result")
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		return "", errors.New("non-text tool result")
	}
	return tc.Text, nil
}

// Item 5 (#867) end-to-end stdio test: over the real stdio transport,
// calls past a cluster's per-cluster concurrency cap queue until a slot
// opens (without failing fast), do not block other clusters, and honor
// cancellation without leaking slots.
func TestStdioPerClusterQueueing(t *testing.T) {
	_, dir, reg := multiClusterServer(t, nil) // 3 clusters -> per-cluster cap 2
	started := make(chan string, 16)
	release := make(chan struct{})
	release2 := make(chan struct{})
	r := reg()
	r.Register(checks.Command{
		Name: "triage slow", MCPName: "k8s_slow", Summary: "Blocks until released; test scaffolding only.",
		Flags: []emit.FlagSpec{
			{Name: "tag", Type: emit.FlagString, Help: "call tag"},
		},
		Kinds: []checks.KindField{checks.Kind("fake.slow", "synthetic", emit.SeverityInfo)},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			tag := inv.Flags.String("tag")
			started <- tag
			switch tag {
			case "hold":
				select {
				case <-release:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			case "wave2":
				select {
				case <-release2:
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			return 1, inv.Out.Emit(emit.Finding{Kind: "fake.slow", Severity: emit.SeverityInfo, Name: tag})
		},
	})

	srv := New(r, "test", WithClusters(dir))
	c2sR, c2sW := io.Pipe()
	s2cR, s2cW := io.Pipe()
	serveCtx, cancelServe := context.WithCancel(t.Context())
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- Serve(serveCtx, srv, ServeOptions{Stdin: c2sR, Stdout: s2cW})
	}()
	t.Cleanup(func() {
		cancelServe()
		_ = c2sW.Close()
		_ = s2cR.Close()
		<-serveDone
	})

	client := mcp.NewClient(&mcp.Implementation{Name: "stdio-client", Version: "0"}, nil)
	cs, err := client.Connect(t.Context(), &mcp.IOTransport{Reader: s2cR, Writer: c2sW}, nil)
	if err != nil {
		t.Fatalf("stdio connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	// Fill dev's 2 per-cluster slots.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
				Name:      "k8s_slow",
				Arguments: map[string]any{"cluster": "dev", "tag": "hold"},
			})
			if err != nil {
				errs <- err
			} else if res.IsError {
				msg, _ := resultText(res)
				errs <- errors.New("hold call failed: " + msg)
			}
		}()
	}
	<-started
	<-started

	// A 3rd call to dev must QUEUE rather than fail fast.
	queuedDone := make(chan string, 1)
	go func() {
		res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
			Name:      "k8s_slow",
			Arguments: map[string]any{"cluster": "dev", "tag": "queued"},
		})
		if err != nil {
			errs <- err
			return
		}
		out, err := resultText(res)
		if err != nil {
			errs <- err
			return
		}
		if res.IsError {
			errs <- errors.New("queued call returned tool error: " + out)
			return
		}
		queuedDone <- out
	}()

	// A 4th call to dev with a short timeout must unblock on cancellation
	// without leaking a slot.
	cancelCtx, cancelWait := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancelWait()
	_, cancelErr := cs.CallTool(cancelCtx, &mcp.CallToolParams{
		Name:      "k8s_slow",
		Arguments: map[string]any{"cluster": "dev", "tag": "canceled"},
	})
	if !errors.Is(cancelErr, context.DeadlineExceeded) && !errors.Is(cancelErr, context.Canceled) {
		t.Errorf("canceled queued call err = %v, want context cancellation/deadline", cancelErr)
	}

	// Meanwhile, another cluster is NOT blocked by dev's full slots.
	otherRes, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "k8s_slow",
		Arguments: map[string]any{"cluster": "prod@proj-b", "tag": "other"},
	})
	if err != nil || otherRes.IsError || !strings.Contains(text(t, otherRes), "name=other") {
		t.Fatalf("other cluster blocked or failed: %v %+v", err, otherRes)
	}
	if tag := <-started; tag != "other" {
		t.Fatalf("unexpected call started while dev slots were full: %q", tag)
	}
	// Verify queued has neither failed nor bypassed dev's 2-slot cap.
	select {
	case out := <-queuedDone:
		t.Fatalf("queued call completed before dev slots freed: %s", out)
	case err := <-errs:
		t.Fatalf("queued call failed instead of queueing: %v", err)
	default:
	}

	// Release the two holding calls; the queued 3rd call must now run and succeed.
	close(release)
	select {
	case out := <-queuedDone:
		if !strings.Contains(out, "name=queued") {
			t.Errorf("queued call output unexpected: %s", out)
		}
	case err := <-errs:
		t.Fatalf("call error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("queued stdio call did not complete after slots freed")
	}
	if tag := <-started; tag != "queued" {
		t.Fatalf("expected queued call to start after release, got %q", tag)
	}
	wg.Wait()

	// Prove zero slot leaks after the canceled waiter: both of dev's 2
	// per-cluster slots must be simultaneously acquirable in wave 2.
	var wave2 sync.WaitGroup
	for range 2 {
		wave2.Add(1)
		go func() {
			defer wave2.Done()
			res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
				Name:      "k8s_slow",
				Arguments: map[string]any{"cluster": "dev", "tag": "wave2"},
			})
			if err != nil {
				errs <- err
			} else if res.IsError {
				msg, _ := resultText(res)
				errs <- errors.New("wave2 failed: " + msg)
			}
		}()
	}
	for range 2 {
		select {
		case tag := <-started:
			if tag != "wave2" {
				t.Fatalf("unexpected tag in wave2: %q", tag)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("slot leak detected: could not concurrently acquire both dev slots in wave 2")
		}
	}
	close(release2)
	wave2.Wait()

	close(errs)
	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Item 5 (#867) end-to-end HTTP test: multi-tenant HTTP MUST keep fail-fast
// per-cluster limiting so one slow cluster cannot wedge shared slots, even
// if WithQueuePerCluster(true) was passed to New.
func TestHTTPPerClusterStillFailsFast(t *testing.T) {
	_, dir, reg := multiClusterServer(t, nil) // 3 clusters -> per-cluster cap 2
	started, release := make(chan struct{}, 8), make(chan struct{})
	r := reg()
	r.Register(checks.Command{
		Name: "triage block", MCPName: "k8s_block", Summary: "Blocks until released; test scaffolding only.",
		Kinds: []checks.KindField{checks.Kind("fake.block", "synthetic", emit.SeverityInfo)},
		Run: func(ctx context.Context, _ emit.Invocation) (int, error) {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return 0, nil
		},
	})

	ctx, cancel := context.WithCancel(t.Context())
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- Serve(ctx, New(r, "test", WithClusters(dir), WithQueuePerCluster(true)), ServeOptions{
			Listen:    "127.0.0.1:0",
			Ready:     ready,
			Stateless: true,
		})
	}()
	addr := <-ready
	t.Cleanup(func() {
		cancel()
		<-done
	})

	client := mcp.NewClient(&mcp.Implementation{Name: "http-client", Version: "0"}, nil)
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: "http://" + addr}, nil)
	if err != nil {
		t.Fatalf("http connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cs.CallTool(ctx, &mcp.CallToolParams{Name: "k8s_block", Arguments: map[string]any{"cluster": "dev"}})
		}()
	}
	<-started
	<-started

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "k8s_block", Arguments: map[string]any{"cluster": "dev"}})
	if err != nil || !res.IsError || !strings.Contains(text(t, res), "calls in flight") {
		t.Errorf("HTTP 3rd call must fail fast with 'calls in flight', got err=%v res=%+v", err, res)
	}
	close(release)
	wg.Wait()
}

// Item 6 (#867): fitResponse stably sorts findings worst-first by severity
// before cutting at --max-response-bytes so critical/warning findings in
// later namespaces survive truncation while alphabetical-first info findings
// are dropped; NewestLast timelines keep chronological order.
func TestFitResponse_SortsBySeverityBeforeTruncating(t *testing.T) {
	mkCmd := func(newestLast bool) checks.Command {
		return checks.Command{
			Name:       "audit mixed",
			MCPName:    "k8s_audit_mixed",
			Summary:    "Mixed-severity alphabetical stream; test scaffolding only.",
			NewestLast: newestLast,
			Kinds: []checks.KindField{
				checks.Kind("mixed.info", "synthetic", emit.SeverityInfo),
				checks.Kind("mixed.warning", "synthetic", emit.SeverityWarning),
				checks.Kind("mixed.critical", "synthetic", emit.SeverityCritical),
			},
			Output: []checks.OutputField{{Name: "seq", Doc: "original emission index"}},
			Run: func(_ context.Context, inv emit.Invocation) (int, error) {
				// Emit 60 alphabetical "alpha" info findings first, then
				// 3 "mid" warnings, then 2 "zeta" criticals at the very end.
				seq := 0
				for i := range 60 {
					if err := inv.Out.Emit(emit.Finding{
						Kind: "mixed.info", Severity: emit.SeverityInfo, Namespace: "alpha",
						KindOfObject: "Pod", Name: "info-" + strconv.Itoa(i), Reason: "Low",
						Message: "alphabetically first low-severity finding padding text",
						Details: []emit.Field{{Key: "seq", Value: strconv.Itoa(seq)}},
					}); err != nil {
						return 0, err
					}
					seq++
				}
				for i := range 3 {
					if err := inv.Out.Emit(emit.Finding{
						Kind: "mixed.warning", Severity: emit.SeverityWarning, Namespace: "mid",
						KindOfObject: "Pod", Name: "warn-" + strconv.Itoa(i), Reason: "Med",
						Message: "mid-alphabet warning finding that must survive truncation",
						Details: []emit.Field{{Key: "seq", Value: strconv.Itoa(seq)}},
					}); err != nil {
						return 0, err
					}
					seq++
				}
				for i := range 2 {
					if err := inv.Out.Emit(emit.Finding{
						Kind: "mixed.critical", Severity: emit.SeverityCritical, Namespace: "zeta",
						KindOfObject: "Pod", Name: "crit-" + strconv.Itoa(i), Reason: "High",
						Message: "last-alphabet critical finding that must survive truncation",
						Details: []emit.Field{{Key: "seq", Value: strconv.Itoa(seq)}},
					}); err != nil {
						return 0, err
					}
					seq++
				}
				return seq, nil
			},
		}
	}

	c := mkCmd(false)
	for _, format := range formats() {
		t.Run(string(format), func(t *testing.T) {
			full := stream(t, c, "--format="+string(format))
			// Budget big enough for the 2 criticals + 3 warnings + a few infos,
			// far smaller than the 60 info findings alone.
			limit := len(full) / 4
			got, dropped := fitResponse(c, nil, full, limit)
			if dropped == 0 || len(got) > limit {
				t.Fatalf("dropped=%d len=%d limit=%d", dropped, len(got), limit)
			}
			lines, summary := splitStream(got)
			if len(lines) < 5 {
				t.Fatalf("kept only %d lines, want at least the 5 critical+warning lines", len(lines))
			}
			// First 2 lines must be crit-0, crit-1 (deterministic tie-break);
			// next 3 lines must be warn-0, warn-1, warn-2.
			wantPrefix := []string{"crit-0", "crit-1", "warn-0", "warn-1", "warn-2"}
			for i, want := range wantPrefix {
				if !strings.Contains(lines[i], want) {
					t.Errorf("line %d = %q, want %q (worst-first with stable tie-breaking)", i, lines[i], want)
				}
			}
			f := summaryFields(t, summary)
			wantSev := "critical:0,warning:0,info:" + strconv.Itoa(dropped)
			if f["response_truncated_severity"] != wantSev {
				t.Errorf("response_truncated_severity = %q, want %q", f["response_truncated_severity"], wantSev)
			}
			if err := checktest.Verify(c, got, format); err != nil {
				t.Errorf("cut answer breaks output contract: %v", err)
			}
		})
	}

	// NewestLast timelines keep chronological order rather than sorting by severity.
	tl := mkCmd(true)
	full := stream(t, tl)
	got, dropped := fitResponse(tl, nil, full, len(full)/4)
	if dropped == 0 {
		t.Fatal("timeline was not truncated")
	}
	lines, _ := splitStream(got)
	if !strings.Contains(lines[len(lines)-1], "crit-1") || !strings.Contains(lines[len(lines)-2], "crit-0") {
		t.Errorf("NewestLast timeline reordered tail:\n%s", got)
	}
}

// Item 6 (#867): emitting cluster elicitation records a structured access-log
// entry (`elicit=cluster`) immediately before waiting on the client, without
// leaking arguments or candidate lists.
func TestElicitationWritesAccessLog(t *testing.T) {
	_, _, reg := multiClusterServer(t, nil)
	dir := choiceClusters(t, "", "")
	log, buf := testLog(t, 5*time.Millisecond)
	server := New(reg(), "test", WithClusters(dir), WithAccessLog(log))

	sawElicitLogged := false
	elicit := func(_ context.Context, _ *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		// The access-log entry must already be written BEFORE the client
		// responds, so a hung or dropped elicitation is visible in the log.
		if strings.Contains(buf.String(), "elicit=cluster") {
			sawElicitLogged = true
		}
		return &mcp.ElicitResult{Action: "accept", Content: map[string]any{"cluster": "payments"}}, nil
	}
	cs := connectWith(t, server, &mcp.ClientOptions{ElicitationHandler: elicit})
	res, err := callTool(t, cs, "k8s_echo_target", map[string]any{})
	if err != nil || res.IsError {
		t.Fatalf("accepted elicitation failed: %v %+v", err, res)
	}
	if !sawElicitLogged {
		t.Error("elicit=cluster was not written to --access-log before the client answered")
	}

	got := records(t, buf)
	if len(got) != 2 {
		t.Fatalf("expected 2 access-log lines (elicit + execution), got %d:\n%s", len(got), buf.String())
	}
	if got[0]["tool"] != "k8s_echo_target" || got[0]["elicit"] != "cluster" || got[0]["exit"] != "0" || got[0]["bytes"] != "0" {
		t.Errorf("elicit log line = %v", got[0])
	}
	if got[1]["tool"] != "k8s_echo_target" || got[1]["cluster"] != "proj-a/us-east1/payments" || got[1]["elicit"] != "" || got[1]["exit"] != "0" {
		t.Errorf("execution log line = %v", got[1])
	}
	if strings.Contains(buf.String(), "checkout@proj-a") {
		t.Errorf("candidate cluster list leaked into access log:\n%s", buf.String())
	}

	// Declined elicitation logs elicit=cluster followed by exit=2.
	buf.Reset()
	decline := func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		return &mcp.ElicitResult{Action: "decline"}, nil
	}
	csDecline := connectWith(t, server, &mcp.ClientOptions{ElicitationHandler: decline})
	if _, err := callTool(t, csDecline, "k8s_echo_target", map[string]any{}); err == nil {
		t.Fatal("expected declined elicitation to fail")
	}
	declined := records(t, buf)
	if len(declined) != 2 || declined[0]["elicit"] != "cluster" || declined[1]["exit"] != "2" {
		t.Errorf("declined access-log lines = %v\nraw:\n%s", declined, buf.String())
	}
}
