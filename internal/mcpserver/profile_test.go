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
	"sort"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// profileRegistry is three visible commands in two profiles plus a
// hidden one, so every branch of the selection grammar has something
// to resolve against without depending on the real command set.
func profileRegistry(t *testing.T) *checks.Registry {
	t.Helper()
	reg := checks.NewRegistry()
	add := func(name, mcp string, profiles []string, hidden bool) {
		reg.Register(checks.Command{
			Name:        name,
			MCPName:     mcp,
			Summary:     "test scaffolding only",
			MCPProfiles: profiles,
			Hidden:      hidden,
			Kinds:       []checks.KindField{checks.Kind("test.finding", "a synthetic finding", emit.SeverityInfo)},
			Run:         func(ctx context.Context, inv emit.Invocation) (int, error) { return 0, nil },
		})
	}
	add("triage alpha", "k8s_alpha", []string{"triage"}, false)
	add("triage beta", "k8s_beta", []string{"triage"}, false)
	add("audit gamma", "k8s_gamma", []string{"audit"}, false)
	add("triage delta", "k8s_delta", nil, false)
	add("triage hidden", "k8s_hidden", nil, true)
	return reg
}

func names(t *testing.T, set map[string]bool) []string {
	t.Helper()
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func TestResolveTools(t *testing.T) {
	reg := profileRegistry(t)
	cases := []struct {
		name    string
		profile string
		tools   string
		want    []string // nil means "everything"
	}{
		{"nothing selected is everything", "", "", nil},
		{"a profile", "triage", "", []string{"k8s_alpha", "k8s_beta"}},
		{"a profile through --tools", "", "triage", []string{"k8s_alpha", "k8s_beta"}},
		{"profile minus a tool", "triage", "-k8s_beta", []string{"k8s_alpha"}},
		{"two profiles", "triage", "audit", []string{"k8s_alpha", "k8s_beta", "k8s_gamma"}},
		{"explicit tools", "", "k8s_delta,k8s_gamma", []string{"k8s_delta", "k8s_gamma"}},
		{"all minus one", "", "all,-k8s_delta", []string{"k8s_alpha", "k8s_beta", "k8s_gamma"}},
		{"full is a synonym for all", "", "full,-k8s_delta", []string{"k8s_alpha", "k8s_beta", "k8s_gamma"}},
		{"left to right: removed then re-added", "", "triage,-k8s_alpha,k8s_alpha", []string{"k8s_alpha", "k8s_beta"}},
		{"whitespace and empty tokens", "  triage ", ", -k8s_beta ,", []string{"k8s_alpha"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveTools(reg, tc.profile, tc.tools)
			if err != nil {
				t.Fatalf("ResolveTools(%q, %q): %v", tc.profile, tc.tools, err)
			}
			if tc.want == nil {
				if got != nil {
					t.Fatalf("selection = %v, want nil (the full surface)", names(t, got))
				}
				return
			}
			if diff := names(t, got); !equal(diff, tc.want) {
				t.Errorf("selection = %v, want %v", diff, tc.want)
			}
		})
	}
}

// A hidden command is not advertised, so it is not selectable either
// — naming one is the same mistake as naming a tool that does not
// exist, and gets the same error.
func TestResolveToolsRejectsUnservedNames(t *testing.T) {
	reg := profileRegistry(t)
	for _, spec := range []string{"k8s_hidden", "k8s_nope", "trage", "-k8s_hidden"} {
		if _, err := ResolveTools(reg, "", spec); err == nil {
			t.Errorf("--tools=%q was accepted", spec)
		}
	}
}

// An empty tool list is indistinguishable to a client from lookout
// not being installed, so it is refused rather than served.
func TestResolveToolsRefusesAnEmptySurface(t *testing.T) {
	reg := profileRegistry(t)
	_, err := ResolveTools(reg, "triage", "-k8s_alpha,-k8s_beta")
	if err == nil {
		t.Fatal("a selection resolving to zero tools was accepted")
	}
	if !strings.Contains(err.Error(), "no tools") {
		t.Errorf("error %q does not say the surface came out empty", err)
	}
}

func TestToolListingTotalsTheSelection(t *testing.T) {
	reg := profileRegistry(t)
	sel, err := ResolveTools(reg, "triage", "")
	if err != nil {
		t.Fatal(err)
	}
	out := ToolListing(reg, sel)
	if !strings.Contains(out, "k8s_alpha") || !strings.Contains(out, "k8s_beta") {
		t.Errorf("listing omits a selected tool:\n%s", out)
	}
	if strings.Contains(out, "k8s_gamma") || strings.Contains(out, "k8s_hidden") {
		t.Errorf("listing includes an unselected tool:\n%s", out)
	}
	if !strings.Contains(out, "2 tools") {
		t.Errorf("listing does not total the selection:\n%s", out)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
