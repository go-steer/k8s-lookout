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
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// targetEchoCommand reports the cluster its invocation reads, and its
// own cluster flag when it declares one (as findings diff does).
func targetEchoCommand(name, mcpName string, withClusterFlag bool) checks.Command {
	c := checks.Command{
		Name:    name,
		MCPName: mcpName,
		Summary: "Echoes the target cluster of its invocation; test scaffolding only.",
		Kinds:   []checks.KindField{checks.Kind("fake.target", "synthetic; test scaffolding only", emit.SeverityInfo)},
		Output: []checks.OutputField{
			{Name: "target_name", Doc: "the target cluster's name"},
			{Name: "host", Doc: "the target config's host"},
			{Name: "cluster_flag", Doc: "the command's own --cluster value"},
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			t := kube.SelectionFrom(ctx).Target
			if t == nil {
				t = &kube.Target{}
			}
			host := ""
			if t.Config != nil {
				host = t.Config.Host
			}
			details := []emit.Field{
				{Key: "target_name", Value: t.Name},
				{Key: "project", Value: t.Project},
				{Key: "location", Value: t.Location},
				{Key: "host", Value: host},
			}
			if withClusterFlag {
				details = append(details, emit.Field{Key: "cluster_flag", Value: inv.Flags.String("cluster")})
			}
			return 1, inv.Out.Emit(emit.Finding{
				Kind: "fake.target", Severity: emit.SeverityInfo, KindOfObject: "Cluster", Name: t.Name,
				Reason: "Echo", Message: "echoes its target", Details: details,
			})
		},
	}
	if withClusterFlag {
		c.Flags = []emit.FlagSpec{
			{Name: "cluster", Type: emit.FlagString, Default: "", Help: "cluster label"},
			emit.StoreClusterFlag(),
		}
	}
	return c
}

func multiClusterServer(t *testing.T, log *AccessLog) (*fakeFleet, *Clusters, func() *checks.Registry) {
	t.Helper()
	src := &fakeFleet{sequence: [][]cloud.ClusterRef{{
		gref("proj-a", "us-central1", "prod"),
		gref("proj-b", "us-east1", "prod"),
		gref("proj-a", "us-central1", "dev"),
	}}}
	dir, _ := newTestClusters(src, false)
	if _, _, err := dir.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	reg := func() *checks.Registry {
		r := testRegistry(t)
		r.Register(targetEchoCommand("triage echotarget", "k8s_echo_target", false))
		r.Register(targetEchoCommand("triage echolabelled", "k8s_echo_labelled", true))
		return r
	}
	return src, dir, reg
}

// TestMultiClusterToolList: every tool requires cluster, the server
// keeps store_cluster and a command's own cluster flag, and
// k8s_list_clusters is served.
func TestMultiClusterToolList(t *testing.T) {
	_, dir, reg := multiClusterServer(t, nil)
	cs := connect(t, New(reg(), "test", WithClusters(dir)))
	var names []string
	for _, tool := range listTools(t, cs) {
		names = append(names, tool.Name)
		if tool.Name == listClustersName {
			continue
		}
		raw, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		schema := string(raw)
		if !strings.Contains(schema, `"cluster":{`) || strings.Contains(schema, `"required":["cluster"]`) {
			t.Errorf("%s: cluster should be an optional property: %s", tool.Name, schema)
		}
		if tool.Name == "k8s_echo_labelled" && (strings.Contains(schema, "store_cluster") || strings.Contains(schema, `"cluster label"`)) {
			t.Errorf("%s: the server's store_cluster or cluster label leaked into the schema: %s", tool.Name, schema)
		}
	}
	if !slices.Contains(names, listClustersName) {
		t.Errorf("tools %v: no %s", names, listClustersName)
	}
}

// TestSingleClusterSchemaUnchanged: without WithClusters a schema is
// exactly what it was (the byte-identical promise; the tools golden
// pins the whole list).
func TestSingleClusterSchemaUnchanged(t *testing.T) {
	c := targetEchoCommand("triage echolabelled", "k8s_echo_labelled", true)
	if !reflect.DeepEqual(toolSchema(c, false), inputSchema(c)) {
		t.Error("toolSchema(c, false) differs from inputSchema(c)")
	}
}

// TestMultiClusterCallReadsTheNamedCluster: the named cluster's
// identity and config reach the command; a short name and the canonical
// one are the same cluster; a command that labels by cluster gets the
// canonical name.
func TestMultiClusterCallReadsTheNamedCluster(t *testing.T) {
	var logBuf bytes.Buffer
	log := NewAccessLog(&logBuf)
	_, dir, reg := multiClusterServer(t, log)
	cs := connect(t, New(reg(), "test", WithClusters(dir), WithAccessLog(log)))

	for _, name := range []string{"prod@proj-b", "proj-b/us-east1/prod"} {
		res, err := callTool(t, cs, "k8s_echo_target", map[string]any{"cluster": name})
		if err != nil || res.IsError {
			t.Fatalf("%s: %v %s", name, err, text(t, res))
		}
		out := text(t, res)
		for _, want := range []string{"target_name=prod", "project=proj-b", "location=us-east1", "host=https://prod.us-east1.gke.goog"} {
			if !strings.Contains(out, want) {
				t.Errorf("%s: output missing %q:\n%s", name, want, out)
			}
		}
	}
	res, err := callTool(t, cs, "k8s_echo_labelled", map[string]any{"cluster": "dev"})
	if err != nil || res.IsError {
		t.Fatalf("labelled: %v", err)
	}
	if out := text(t, res); !strings.Contains(out, "cluster_flag=proj-a/us-central1/dev") {
		t.Errorf("the command's cluster label is not the canonical name:\n%s", out)
	}
	if !strings.Contains(logBuf.String(), "tool=k8s_echo_target exit=0") || !strings.Contains(logBuf.String(), "cluster=proj-b/us-east1/prod") {
		t.Errorf("access log does not name the canonical cluster:\n%s", logBuf.String())
	}
}

func TestMultiClusterCallErrors(t *testing.T) {
	_, dir, reg := multiClusterServer(t, nil)
	cs := connect(t, New(reg(), "test", WithClusters(dir)))
	for _, tc := range []struct {
		name string
		args map[string]any
		want []string
	}{
		{"missing", map[string]any{}, []string{"serves 3 clusters and this call names none", "pass cluster, e.g. dev, prod@proj-a, prod@proj-b", "k8s_list_clusters"}},
		{"ambiguous short name", map[string]any{"cluster": "prod"}, []string{`unknown cluster "prod"; did you mean prod@proj-a, prod@proj-b?`, "k8s_list_clusters lists all 3"}},
		{"unknown", map[string]any{"cluster": "nope"}, []string{`unknown cluster "nope"`}},
		{"not a string", map[string]any{"cluster": 7}, []string{"cluster"}},
		{"unknown argument lists cluster", map[string]any{"cluster": "dev", "bogus": "x"}, []string{"(accepts: cluster, "}},
		{"store_cluster", map[string]any{"cluster": "dev", "store_cluster": "x"}, []string{"store_cluster is not accepted"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tool := "k8s_echo_target"
			if tc.name == "store_cluster" {
				tool = "k8s_echo_labelled"
			}
			_, err := callTool(t, cs, tool, tc.args)
			if err == nil {
				t.Fatal("no error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
		})
	}
}

func TestListClustersTool(t *testing.T) {
	_, dir, reg := multiClusterServer(t, nil)
	cs := connect(t, New(reg(), "test", WithClusters(dir)))
	res, err := callTool(t, cs, listClustersName, map[string]any{})
	if err != nil || res.IsError {
		t.Fatalf("%v", err)
	}
	out := text(t, res)
	for _, want := range []string{
		"cluster=dev project=proj-a location=us-central1 canonical=proj-a/us-central1/dev dns_endpoint=true\n",
		"cluster=prod@proj-a project=proj-a location=us-central1 canonical=proj-a/us-central1/prod dns_endpoint=true\n",
		"cluster=prod@proj-b ",
		"scope=p via=container clusters=3\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if last := lines[len(lines)-1]; !strings.HasPrefix(last, "scanned=3 findings=0 elapsed=") || !strings.HasSuffix(last, " clusters=3") {
		t.Errorf("summary line %q", last)
	}

	res, err = callTool(t, cs, listClustersName, map[string]any{"filter": "east"})
	if err != nil {
		t.Fatal(err)
	}
	if out := text(t, res); !strings.Contains(out, "cluster=prod@proj-b") || strings.Contains(out, "cluster=dev") || !strings.Contains(out, "scanned=3 ") || !strings.Contains(out, " clusters=1") {
		t.Errorf("filtered output:\n%s", out)
	}
	if _, err := callTool(t, cs, listClustersName, map[string]any{"cluster": "dev"}); err == nil || !strings.Contains(err.Error(), "accepts: filter") {
		t.Errorf("unknown argument: %v", err)
	}
}

// TestGoogleModeMultiClusterDiscoversAsTheCaller: in Google-token mode
// each caller's clusters are discovered with their own token, so each
// lists, and can name, only what they can see.
func TestGoogleModeMultiClusterDiscoversAsTheCaller(t *testing.T) {
	base := fakeGoogle(t)
	g, err := NewGoogleAuth(GoogleOptions{
		ClientIDs: []string{googleClientID}, Allow: []string{"@cymbal.example"},
		Resource: "http://127.0.0.1/mcp", TokenInfoURL: base + "/tokeninfo", UserInfoURL: base + "/userinfo",
	})
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeFleet{byToken: map[string][]cloud.ClusterRef{
		"alice-token": {gref("proj-a", "us-central1", "alpha"), gref("proj-s", "us-central1", "shared")},
		"bob-token":   {gref("proj-s", "us-central1", "shared")},
	}}
	reg := checks.NewRegistry()
	reg.Register(targetEchoCommand("triage echotarget", "k8s_echo_target", false))
	dir := NewClusters(src, true, 0)
	dir.access = nil
	server := New(reg, "test", WithGoogleAuth(g), WithClusters(dir))
	ctx, cancel := context.WithCancel(context.Background())
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() { done <- Serve(ctx, server, ServeOptions{Listen: "127.0.0.1:0", Google: g, Ready: ready}) }()
	t.Cleanup(func() { cancel(); <-done })
	s := googleServer{url: "http://" + <-ready}

	list := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"k8s_list_clusters","arguments":{}}}`
	if _, body := s.post(t, "/mcp", "alice-token", list); !strings.Contains(body, "cluster=alpha") || !strings.Contains(body, "cluster=shared") {
		t.Errorf("alice's list: %s", body)
	}
	if _, body := s.post(t, "/mcp", "bob-token", list); strings.Contains(body, "alpha") || !strings.Contains(body, "cluster=shared") {
		t.Errorf("bob's list shows alice's cluster, or not his own: %s", body)
	}
	echo := func(cluster string) string {
		return `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"k8s_echo_target","arguments":{"cluster":"` + cluster + `"}}}`
	}
	if _, body := s.post(t, "/mcp", "bob-token", echo("alpha")); !strings.Contains(body, `unknown cluster \"alpha\"`) {
		t.Errorf("bob named alice's cluster: %s", body)
	}
	if _, body := s.post(t, "/mcp", "alice-token", echo("alpha")); !strings.Contains(body, "target_name=alpha") {
		t.Errorf("alice could not read her cluster: %s", body)
	}
}

// TestMultiClusterListingCountsTheSurface: --list-tools in multi-cluster
// mode counts k8s_list_clusters and each schema's cluster argument.
func TestMultiClusterListingCountsTheSurface(t *testing.T) {
	reg := testRegistry(t)
	single, multi := ToolListing(reg, nil), MultiClusterToolListing(reg, nil)
	if strings.Contains(single, listClustersName) || !strings.Contains(multi, listClustersName) {
		t.Errorf("single:\n%s\nmulti:\n%s", single, multi)
	}
	if !strings.Contains(multi, "3 tools") {
		t.Errorf("multi-cluster listing should count k8s_list_clusters:\n%s", multi)
	}
}
