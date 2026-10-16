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

package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestMCPMultiClusterFlags: the multi-cluster flags of lookout mcp
// (#700) refuse what cannot work with them, before anything starts.
func TestMCPMultiClusterFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"both sources", []string{"--clusters=a=x.gke.goog", "--clusters-from=p"}, "mutually exclusive"},
		{"pinned kubeconfig", []string{"--clusters-from=p", "--kubeconfig=/k"}, "--kubeconfig and --context pin one cluster"},
		{"pinned context", []string{"--clusters=a=x.gke.goog", "--context=c"}, "--kubeconfig and --context pin one cluster"},
		{"store", []string{"--clusters-from=p", "--store=/s.db"}, "not yet supported on a multi-cluster endpoint"},
		{"report", []string{"--clusters-from=p", "--report=/r"}, "not yet supported on a multi-cluster endpoint"},
		{"bad scope", []string{"--clusters-from=folders/abc"}, "numeric ID"},
		{"bad pairs", []string{"--clusters=a="}, "no endpoint"},
		{"rediscover alone", []string{"--rediscover=5m"}, "--rediscover only applies"},
		{"rediscover zero", []string{"--clusters-from=p", "--rediscover=0s"}, "must be positive"},
		{"google kubeconfig fleet", append(googleFlags(), "--clusters-from=kubeconfig:/k"), "only GKE clusters"},
		{"google non-GKE endpoint", append(googleFlags(), "--clusters=eks=eks.example.com"), "only GKE clusters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := mcpMain(t.Context(), tc.args, &stdout, &stderr); code != 2 {
				t.Fatalf("exit %d, want 2; stderr: %s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Errorf("stderr %q does not contain %q", stderr.String(), tc.want)
			}
		})
	}
}

func googleFlags() []string {
	return []string{"--listen=127.0.0.1:0", "--google-client-id=c.apps.googleusercontent.com", "--google-allow=@example.com", "--resource-url=https://h.example.com/mcp"}
}

// TestMCPMultiClusterListTools: --list-tools needs no discovery, and
// counts k8s_list_clusters and each schema's cluster argument.
func TestMCPMultiClusterListTools(t *testing.T) {
	var single, multi, stderr bytes.Buffer
	if code := mcpMain(t.Context(), []string{"--profile=triage", "--list-tools"}, &single, &stderr); code != 0 {
		t.Fatalf("single: exit %d: %s", code, stderr.String())
	}
	if code := mcpMain(t.Context(), []string{"--profile=triage", "--list-tools", "--clusters-from=folders/1"}, &multi, &stderr); code != 0 {
		t.Fatalf("multi: exit %d: %s", code, stderr.String())
	}
	if strings.Contains(single.String(), "k8s_list_clusters") || !strings.Contains(multi.String(), "k8s_list_clusters") {
		t.Errorf("single:\n%s\nmulti:\n%s", single.String(), multi.String())
	}
}
