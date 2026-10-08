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
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/delta"
	"github.com/go-steer/k8s-lookout/pkg/checks/stab"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// TestCallTool_CustomRoleRefusalMatchesCLI is #584 on the MCP surface:
// under a custom role that refuses Pods — a read the built-in `view`
// role grants — a command that degrades around them answers with the
// same records the CLI prints, and one that cannot answer without them
// is a tool error carrying exactly the CLI's diagnostic, in the shared
// refusal wording rather than the API server's bare "is forbidden".
func TestCallTool_CustomRoleRefusalMatchesCLI(t *testing.T) {
	cs := fake.NewClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-0"}, Spec: corev1.PodSpec{NodeName: "n1"}},
	)
	checktest.RefuseRead(&cs.Fake, schema.GroupResource{Resource: "pods"})
	client := func(context.Context) (kubernetes.Interface, error) { return cs, nil }
	now := func() time.Time { return time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC) }
	deltaCmd := delta.New(client)
	drainCmd := stab.DrainCommand(stab.Deps{Client: client, Now: now,
		Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil }})

	reg := checks.NewRegistry()
	reg.Register(deltaCmd)
	reg.Register(drainCmd)
	session := connect(t, New(reg, "test"))

	refusal := "forbidden: list pods — namespaced, this identity lacks it; grant list on pods (core) via a ClusterRole or Role, as lookout's shipped ClusterRole does"

	t.Run("degrades", func(t *testing.T) {
		cli := checktest.Run(t, deltaCmd)
		if cli.Code != 0 || !strings.Contains(cli.Stdout, refusal) {
			t.Fatalf("CLI: exit %d, want 0 with the refusal record\n%s%s", cli.Code, cli.Stdout, cli.Stderr)
		}
		res, err := callTool(t, session, "k8s_triage_delta", nil)
		if err != nil {
			t.Fatalf("tools/call: %v", err)
		}
		got := text(t, res)
		if res.IsError {
			t.Fatalf("a degraded answer became a tool error:\n%s", got)
		}
		// Everything but the summary line, whose elapsed= is the
		// clock's, is byte-identical.
		if a, b := withoutSummary(cli.Stdout), withoutSummary(got); a != b {
			t.Errorf("MCP payload differs from the CLI's\n CLI: %s\n MCP: %s", a, b)
		}
	})
	t.Run("fails worded", func(t *testing.T) {
		cli := checktest.Run(t, drainCmd, "-A")
		// The caller's context ("listing pods") stays ahead of the
		// shared wording.
		if cli.Code != 1 || !strings.Contains(cli.Stderr, "lookout stab drain: listing pods: "+refusal+" — the command cannot answer without it") {
			t.Fatalf("CLI: exit %d, want 1 in the shared wording\n%s", cli.Code, cli.Stderr)
		}
		res, err := callTool(t, session, "k8s_drain_blockers", map[string]any{"all_namespaces": true})
		if err != nil {
			t.Fatalf("tools/call: %v", err)
		}
		if !res.IsError {
			t.Fatalf("want a tool error, got an answer:\n%s", text(t, res))
		}
		if got, want := text(t, res), strings.TrimSpace(cli.Stderr); got != want {
			t.Errorf("MCP diagnostic differs from the CLI's\n CLI: %s\n MCP: %s", want, got)
		}
	})
}

// withoutSummary drops the final summary line.
func withoutSummary(out string) string {
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	return strings.Join(lines[:len(lines)-1], "\n")
}
