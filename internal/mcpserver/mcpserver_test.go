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
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// fakeSpecCommand is a synthetic command with a positional argument
// and every FlagSpec type, so the schema golden and the target
// round-trip cover the whole derivation surface without depending on
// real commands (which would churn the golden as they merge).
func fakeSpecCommand() checks.Command {
	return checks.Command{
		Name:    "fake",
		MCPName: "k8s_fake_spec",
		Summary: "Synthetic positional command covering every flag type; test scaffolding only.",
		Positional: &checks.Positional{
			Meta: "<Kind>/[<namespace>/]<name>",
			Doc:  "the one resource to read; namespace defaults to --namespace",
		},
		Flags: []emit.FlagSpec{
			{Name: "container", Type: emit.FlagString, Default: "", Help: "limit to one container"},
			{Name: "diff", Type: emit.FlagBool, Default: "false", Help: "diff against the previous revision"},
			{Name: "tail", Type: emit.FlagInt, Default: "400", Help: "lines per container"},
			{Name: "grace", Type: emit.FlagDuration, Default: "30s", Help: "grace window"},
		},
		Kinds: []checks.KindField{
			checks.Kind("fake.spec", "synthetic; test scaffolding only", emit.SeverityInfo),
		},
		Output: []checks.OutputField{
			{Name: "target", Doc: "the positional argument as received"},
			{Name: "grace", Doc: "the parsed --grace value"},
		},
		Examples: []string{"lookout fake Pod/prod/api --tail=100"},
		Run: func(_ context.Context, inv emit.Invocation) (int, error) {
			target := ""
			if len(inv.Args) > 0 {
				target = inv.Args[0]
			}
			f := emit.Finding{
				Kind:         "fake.echo",
				Severity:     emit.SeverityInfo,
				KindOfObject: "Pod",
				Name:         "echo",
				Reason:       "Echo",
				Message:      "echoes its invocation for round-trip tests",
				Details: []emit.Field{
					{Key: "target", Value: target},
					{Key: "grace", Value: inv.Flags.Duration("grace").String()},
				},
			}
			if err := inv.Out.Emit(f); err != nil {
				return 0, err
			}
			return 1, nil
		},
	}
}

// testRegistry builds the test-local registry: the demo command
// un-hidden (so it is served), the positional fake, and one hidden
// command that must NOT appear as a tool.
func testRegistry(t *testing.T) *checks.Registry {
	t.Helper()
	reg := checks.NewRegistry()

	demo := checktest.DemoCommand()
	demo.Hidden = false
	reg.Register(demo)

	reg.Register(fakeSpecCommand())

	hidden := checktest.DemoCommand() // Hidden: true as shipped
	hidden.Name = "triage hidden"
	hidden.MCPName = "k8s_triage_hidden"
	reg.Register(hidden)

	return reg
}

// connect wires a client to the server over the SDK's in-memory
// transport pair (the same newline-delimited JSON-RPC framing as
// stdio) and returns the client session.
func connect(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	ctx := t.Context()
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := server.Connect(ctx, serverT, nil); err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	cs, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func listTools(t *testing.T, cs *mcp.ClientSession) []*mcp.Tool {
	t.Helper()
	res, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	return res.Tools
}

// TestToolListGolden pins the full tools/list JSON for the test-local
// registry: names, micro-skill descriptions, and the mechanically
// derived schemas. Real commands are deliberately absent so late
// merges into the default registry never churn this golden.
func TestToolListGolden(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	got, err := json.MarshalIndent(listTools(t, cs), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')

	checktest.GoldenBytes(t, "testdata/tools.golden.json", got)
}

func TestHiddenCommandsAreNotServed(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	for _, tool := range listTools(t, cs) {
		if tool.Name == "k8s_triage_hidden" {
			t.Fatal("hidden command was served as an MCP tool")
		}
	}
}

func callTool(t *testing.T, cs *mcp.ClientSession, name string, args map[string]any) (*mcp.CallToolResult, error) {
	t.Helper()
	return cs.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
}

// text concatenates the text content of a tool result.
func text(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	var b strings.Builder
	for _, c := range res.Content {
		tc, ok := c.(*mcp.TextContent)
		if !ok {
			t.Fatalf("unexpected content type %T", c)
		}
		b.WriteString(tc.Text)
	}
	return b.String()
}

func TestCallTool_SuccessPayloadWithSummary(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	res, err := callTool(t, cs, "k8s_triage_demo", map[string]any{
		"count":     1,
		"namespace": "prod",
	})
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected tool error: %s", text(t, res))
	}
	got := text(t, res)
	if !strings.Contains(got, "kind=demo.finding") || !strings.Contains(got, "namespace=prod") {
		t.Errorf("payload missing logfmt finding:\n%s", got)
	}
	// The §4.2 contract holds on the MCP surface too: the payload is
	// terminated by the mandatory summary line.
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	last := lines[len(lines)-1]
	if !strings.HasPrefix(last, "scanned=5 findings=1 elapsed=") {
		t.Errorf("payload not terminated by summary line, got %q", last)
	}
}

func TestCallTool_FormatJSONHonored(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	res, err := callTool(t, cs, "k8s_triage_demo", map[string]any{
		"count":  1,
		"format": "json",
	})
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	got := text(t, res)
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	for _, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("line is not a JSON object with --format=json: %q: %v", line, err)
		}
	}
	if want := `"findings":1`; !strings.Contains(lines[len(lines)-1], want) {
		t.Errorf("JSON summary line missing %s: %q", want, lines[len(lines)-1])
	}
}

func TestCallTool_TargetPositionalAndTypedFlags(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	res, err := callTool(t, cs, "k8s_fake_spec", map[string]any{
		"target": "Pod/prod/api",
		"grace":  "90s",
		"tail":   7,
		"diff":   false,
	})
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected tool error: %s", text(t, res))
	}
	got := text(t, res)
	if !strings.Contains(got, "target=Pod/prod/api") {
		t.Errorf("target positional did not reach the command:\n%s", got)
	}
	if !strings.Contains(got, "grace=1m30s") {
		t.Errorf("duration argument did not round-trip through the flag parser:\n%s", got)
	}
}

// TestCallTool_RuntimeErrorIsToolError checks the exit-1 mapping: a
// tool error (IsError, stderr text as content) — visible to the
// model, not a protocol failure.
func TestCallTool_RuntimeErrorIsToolError(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	res, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"fail": true})
	if err != nil {
		t.Fatalf("runtime error must not be a protocol error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected IsError for a runtime failure")
	}
	if got := text(t, res); !strings.Contains(got, "demo failure requested via --fail") {
		t.Errorf("tool error does not carry stderr diagnostics: %q", got)
	}
}

// wantInvalidParams asserts a tools/call failed as the JSON-RPC
// invalid-params protocol error (-32602), the exit-2 mapping.
func wantInvalidParams(t *testing.T, err error, msgFragment string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an invalid-params error, got success")
	}
	var wire *jsonrpc.Error
	if !errors.As(err, &wire) {
		t.Fatalf("expected *jsonrpc.Error, got %T: %v", err, err)
	}
	if wire.Code != jsonrpc.CodeInvalidParams {
		t.Errorf("error code = %d, want %d (invalid params)", wire.Code, jsonrpc.CodeInvalidParams)
	}
	if !strings.Contains(wire.Message, msgFragment) {
		t.Errorf("error message %q does not contain %q", wire.Message, msgFragment)
	}
}

func TestCallTool_UsageErrorIsInvalidParams(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	// --since must not be negative: rejected by the shared §4.2
	// parser inside emit.Run (exit 2), not by the schema layer.
	_, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"since": "-5s"})
	wantInvalidParams(t, err, "--since must not be negative")
}

func TestCallTool_UnknownArgumentIsInvalidParams(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	_, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"bogus": 1})
	wantInvalidParams(t, err, `unknown argument "bogus"`)
}

// An MCP client that guesses a parameter name must be able to recover
// from the error itself, in one round trip. Before #232 the rejection
// named only the wrong parameter, and the measured behaviour was a
// retry ladder of four guesses on one call.
func TestCallTool_UnknownArgumentNamesTheRightOne(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	for _, tc := range []struct{ arg, want string }{
		{"namespaces", `did you mean "namespace"?`},
		{"form", `did you mean "format"?`},
		// Nothing is close to "request": no suggestion, but the list
		// of accepted names still answers the question.
		{"request", "(accepts: "},
	} {
		_, err := callTool(t, cs, "k8s_triage_demo", map[string]any{tc.arg: "x"})
		wantInvalidParams(t, err, tc.want)
		wantInvalidParams(t, err, "workload")
	}
}

// `target` is the property name on the three tools that take a
// positional, and clients apply it to the other twenty. Accept it as
// `workload` rather than spend a round trip on the rejection (#232).
func TestCallTool_TargetIsAcceptedAsWorkload(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	res, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"target": "Deployment/prod/api"})
	if err != nil {
		t.Fatalf("target was rejected: %v", err)
	}
	if res.IsError {
		t.Fatalf("target produced a tool error: %s", text(t, res))
	}
	// Saying it twice is a mistake worth naming, not a silent
	// last-one-wins.
	_, err = callTool(t, cs, "k8s_triage_demo", map[string]any{
		"target": "Deployment/prod/api", "workload": "Deployment/prod/other",
	})
	wantInvalidParams(t, err, "name the same parameter")
}

func TestCallTool_WrongTypeIsInvalidParams(t *testing.T) {
	cs := connect(t, New(testRegistry(t), "test"))
	_, err := callTool(t, cs, "k8s_triage_demo", map[string]any{"count": "three"})
	wantInvalidParams(t, err, `argument "count" must be an integer`)
}

// TestCallTool_SanitizerHoldsOnMCPSurface plants a credential-shaped
// value in a finding message and asserts the §6.5 sanitizer masked it
// in the tool result — the MCP surface goes through the same
// sanitizing Writer as the CLI, by construction.
func TestCallTool_SanitizerHoldsOnMCPSurface(t *testing.T) {
	reg := checks.NewRegistry()
	reg.Register(checks.Command{
		Name:    "leaky",
		MCPName: "k8s_leaky",
		Summary: "Emits a credential-shaped message; test scaffolding only.",
		Kinds:   []checks.KindField{checks.Kind("fake.leak", "synthetic; test scaffolding only", emit.SeverityInfo)},
		Run: func(_ context.Context, inv emit.Invocation) (int, error) {
			return 1, inv.Out.Emit(emit.Finding{
				Kind:         "fake.leak",
				Severity:     emit.SeverityInfo,
				KindOfObject: "Pod",
				Name:         "leaky",
				Reason:       "Leak",
				Message:      "connect with --password=hunter2-marker-value",
			})
		},
	})
	cs := connect(t, New(reg, "test"))
	res, err := callTool(t, cs, "k8s_leaky", nil)
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}
	got := text(t, res)
	if strings.Contains(got, "hunter2-marker-value") {
		t.Fatalf("credential value reached the MCP surface:\n%s", got)
	}
	if !strings.Contains(got, "REDACTED") {
		t.Errorf("expected a redaction marker in:\n%s", got)
	}
}
