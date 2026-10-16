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
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/client-go/rest"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

// accessByHost answers the access check from the cluster's host.
func accessByHost(byHost map[string]string) accessFunc {
	return func(_ context.Context, cfg *rest.Config) string {
		if a, ok := byHost[cfg.Host]; ok {
			return a
		}
		return accessLimited
	}
}

func host(r cloud.ClusterRef) string { return "https://" + r.Endpoint }

// choiceClusters is a set of five: two readable in proj-a, one readable
// elsewhere, one the caller can see but not read, one unreachable.
func choiceClusters(t *testing.T, defCluster, defProject string) *Clusters {
	t.Helper()
	refs := []cloud.ClusterRef{
		gref("proj-a", "us-central1", "checkout"),
		gref("proj-a", "us-east1", "payments"),
		gref("proj-b", "europe-west1", "checkout"),
		gref("proj-c", "us-central1", "ap"),
		gref("proj-d", "us-central1", "gone"),
	}
	c, _ := newTestClusters(&fakeFleet{sequence: [][]cloud.ClusterRef{refs}}, false)
	c.access = accessByHost(map[string]string{
		host(refs[0]): accessRead, host(refs[1]): accessRead, host(refs[2]): accessRead,
		host(refs[4]): accessUnreachable,
	})
	c.SetDefaults(defCluster, defProject)
	if _, _, err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestAccessOrdersTheSet: readable clusters first (the default project's
// leading), then the ones the caller can only see, then the unreachable.
func TestAccessOrdersTheSet(t *testing.T) {
	c := choiceClusters(t, "", "proj-b")
	entries, _, err := c.List(t.Context(), "", "")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.name+" "+e.access)
	}
	want := []string{"checkout@proj-b read", "checkout@proj-a read", "payments read", "ap limited", "gone unreachable"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order:\n%v\nwant:\n%v", got, want)
	}
}

func TestChooseWithoutAName(t *testing.T) {
	for _, tc := range []struct {
		name, defCluster, defProject, want, wantErr string
	}{
		{name: "default cluster", defCluster: "payments", want: "proj-a/us-east1/payments"},
		{name: "default cluster by canonical name", defCluster: "proj-b/europe-west1/checkout", want: "proj-b/europe-west1/checkout"},
		// A namespace-scoped caller may still use the operator's default.
		{name: "limited default is used", defCluster: "ap", want: "proj-c/us-central1/ap"},
		{name: "unreachable default is not used", defCluster: "gone", wantErr: "names none"},
		{name: "only readable cluster in the default project", defProject: "proj-b", want: "proj-b/europe-west1/checkout"},
		{name: "two readable in the default project", defProject: "proj-a", wantErr: "pass cluster, e.g. checkout@proj-a, payments, checkout@proj-b, ap (3 you can read cluster-wide, 1 where you may have access to some namespaces"},
		{name: "no defaults", wantErr: "serves 5 clusters and this call names none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := choiceClusters(t, tc.defCluster, tc.defProject)
			e, err := c.Choose(t.Context(), "", "")
			if tc.wantErr != "" {
				var ce *choiceError
				if !errors.As(err, &ce) || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want a choiceError containing %q", err, tc.wantErr)
				}
				if len(ce.candidates) == 0 || strings.Contains(strings.Join(ce.candidates, ","), "gone") {
					t.Errorf("candidates %v: want reachable clusters only", ce.candidates)
				}
				return
			}
			if err != nil || e.canonical != tc.want {
				t.Errorf("chose %q (%v), want %q", e.canonical, err, tc.want)
			}
		})
	}
}

// TestChooseOnlyReadableCluster: one readable cluster among many is the
// caller's cluster, whatever else they can see.
func TestChooseOnlyReadableCluster(t *testing.T) {
	refs := []cloud.ClusterRef{gref("p", "l", "mine"), gref("q", "l", "theirs"), gref("r", "l", "others")}
	c, _ := newTestClusters(&fakeFleet{sequence: [][]cloud.ClusterRef{refs}}, false)
	c.access = accessByHost(map[string]string{host(refs[0]): accessRead})
	if _, _, err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if e, err := c.Choose(t.Context(), "", ""); err != nil || e.ref.Name != "mine" {
		t.Errorf("chose %+v (%v), want mine", e, err)
	}
}

// TestChooseNamespaceScopedCaller: a caller whose RoleBindings cover
// only some namespaces cannot list pods cluster-wide anywhere. Their
// clusters are limited, not unreadable: the only one is theirs, and
// several are offered (with the advice to pass a namespace) rather than
// refused.
func TestChooseNamespaceScopedCaller(t *testing.T) {
	refs := []cloud.ClusterRef{gref("p", "l", "team-a"), gref("q", "l", "gone")}
	c, _ := newTestClusters(&fakeFleet{sequence: [][]cloud.ClusterRef{refs}}, false)
	c.access = accessByHost(map[string]string{host(refs[1]): accessUnreachable}) // team-a: limited
	if _, _, err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if e, err := c.Choose(t.Context(), "", ""); err != nil || e.ref.Name != "team-a" {
		t.Errorf("chose %+v (%v), want their only reachable cluster, team-a", e, err)
	}

	refs = []cloud.ClusterRef{gref("p", "l", "a"), gref("q", "l", "b")}
	c, _ = newTestClusters(&fakeFleet{sequence: [][]cloud.ClusterRef{refs}}, false)
	c.access = accessByHost(nil) // both limited
	if _, _, err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, err := c.Choose(t.Context(), "", "")
	if err == nil || !strings.Contains(err.Error(), "pass cluster, e.g. a, b (0 you can read cluster-wide, 2 where you may have access to some namespaces: pass namespace there)") {
		t.Errorf("err = %v", err)
	}
}

func TestChooseNothingReachable(t *testing.T) {
	refs := []cloud.ClusterRef{gref("p", "l", "a"), gref("q", "l", "b")}
	c, _ := newTestClusters(&fakeFleet{sequence: [][]cloud.ClusterRef{refs}}, false)
	c.access = func(context.Context, *rest.Config) string { return accessUnreachable }
	if _, _, err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Choose(t.Context(), "", ""); err == nil || !strings.Contains(err.Error(), "lookout could not reach any of them") {
		t.Errorf("err = %v", err)
	}
}

func TestChooseNamed(t *testing.T) {
	c := choiceClusters(t, "", "proj-a")
	for _, tc := range []struct{ name, want, wantErr string }{
		{name: "payments", want: "proj-a/us-east1/payments"},
		// A bare name repeated across projects: the default project's.
		{name: "checkout", want: "proj-a/us-central1/checkout"},
		// A near miss names its neighbours.
		{name: "paymnts", wantErr: `unknown cluster "paymnts"; did you mean payments?`},
		// Nothing close: no guesses, just the list tool.
		{name: "zzzzzzzzzz", wantErr: `unknown cluster "zzzzzzzzzz" k8s_list_clusters lists all 5`},
		// A cluster the caller cannot read is still theirs to name.
		{name: "ap", want: "proj-c/us-central1/ap"},
	} {
		e, err := c.Choose(t.Context(), "", tc.name)
		if tc.wantErr != "" {
			if !errors.Is(err, errUnknownCluster) || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("%s: err = %v, want an unknown cluster containing %q", tc.name, err, tc.wantErr)
			}
			continue
		}
		if err != nil || e.canonical != tc.want {
			t.Errorf("%s: chose %q (%v), want %q", tc.name, e.canonical, err, tc.want)
		}
	}
	// Without a default project the repeated name is not guessed.
	c = choiceClusters(t, "", "")
	if _, err := c.Choose(t.Context(), "", "checkout"); err == nil || !strings.Contains(err.Error(), "did you mean checkout@proj-a, checkout@proj-b?") {
		t.Errorf("ambiguous name: %v", err)
	}
}

// TestElicitationPicksTheCluster: a stateful client that supports
// elicitation is asked which cluster to read, offered readable ones.
func TestElicitationPicksTheCluster(t *testing.T) {
	_, _, reg := multiClusterServer(t, nil)
	dir := choiceClusters(t, "", "")
	server := New(reg(), "test", WithClusters(dir))

	var offered []string
	elicit := func(_ context.Context, req *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		props := req.Params.RequestedSchema.(map[string]any)["properties"].(map[string]any)
		for _, v := range props["cluster"].(map[string]any)["enum"].([]any) {
			offered = append(offered, v.(string))
		}
		return &mcp.ElicitResult{Action: "accept", Content: map[string]any{"cluster": "payments"}}, nil
	}
	cs := connectWith(t, server, &mcp.ClientOptions{ElicitationHandler: elicit})
	res, err := callTool(t, cs, "k8s_echo_target", map[string]any{})
	if err != nil || res.IsError {
		t.Fatalf("%v %+v", err, res)
	}
	if out := text(t, res); !strings.Contains(out, "target_name=payments") {
		t.Errorf("the elicited cluster was not read:\n%s", out)
	}
	if want := []string{"checkout@proj-a", "checkout@proj-b", "payments", "ap"}; !reflect.DeepEqual(offered, want) {
		t.Errorf("offered %v, want %v: readable cluster-wide first, then limited, never unreachable", offered, want)
	}

	decline := func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		return &mcp.ElicitResult{Action: "decline"}, nil
	}
	cs = connectWith(t, server, &mcp.ClientOptions{ElicitationHandler: decline})
	if _, err := callTool(t, cs, "k8s_echo_target", map[string]any{}); err == nil || !strings.Contains(err.Error(), "names none") {
		t.Errorf("declined: %v", err)
	}
}

func connectWith(t *testing.T, server *mcp.Server, opts *mcp.ClientOptions) *mcp.ClientSession {
	t.Helper()
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := server.Connect(t.Context(), serverT, nil); err != nil {
		t.Fatal(err)
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, opts).Connect(t.Context(), clientT, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}
