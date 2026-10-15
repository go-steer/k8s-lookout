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

//go:build gke || allproviders

package gke

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/oauth2"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

const callerTok = "ya29.caller-token"

func callerCtx() context.Context {
	return kube.WithCallerToken(context.Background(), oauth2.StaticTokenSource(&oauth2.Token{AccessToken: callerTok}))
}

// googlePackage reports whether an import path is a Google API client
// library, whose constructors fall back to Application Default Credentials.
func googlePackage(path string) bool {
	return strings.HasPrefix(path, "google.golang.org/api/") || strings.HasPrefix(path, "cloud.google.com/go/")
}

// importName is the identifier a file uses for an import: its alias, or
// the package name, which for versioned paths (compute/v1,
// cloudquotas/apiv1) is the element before the version.
func importName(spec *ast.ImportSpec) string {
	if spec.Name != nil {
		return spec.Name.Name
	}
	parts := strings.Split(strings.Trim(spec.Path.Value, `"`), "/")
	name := parts[len(parts)-1]
	if len(parts) > 1 && regexp.MustCompile(`^(api)?v\d+(beta\d*|alpha\d*)?$`).MatchString(name) {
		name = parts[len(parts)-2]
	}
	return name
}

// TestEveryClientUsesClientOptions is the guard behind per-caller identity
// on the cloud side: every Google API client this package builds must take
// clientOptions(ctx), or it would read as lookout's own identity on a
// caller's behalf. It parses the package source, so a new construction
// site that forgets fails here rather than in production.
//
// A construction is any call X.New…(ctx, …) where X is a Google client
// library import (under any alias): NewService, NewClient, NewMetricClient,
// NewInstancesRESTClient. Its last argument must be clientOptions(ctx)...
// And no file but fleet.go may reach for google.Default* credentials.
func TestEveryClientUsesClientOptions(t *testing.T) {
	t.Parallel()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	sites := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		google := map[string]bool{}
		oauthGoogle := ""
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if googlePackage(p) {
				google[importName(imp)] = true
			}
			if p == "golang.org/x/oauth2/google" {
				oauthGoogle = importName(imp)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if ok && oauthGoogle != "" && name != "fleet.go" {
				if x, ok := sel.X.(*ast.Ident); ok && x.Name == oauthGoogle && strings.HasPrefix(sel.Sel.Name, "Default") {
					t.Errorf("%s: %s.%s uses lookout's own credentials; use clientOptions(ctx)", fset.Position(sel.Pos()), x.Name, sel.Sel.Name)
				}
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !strings.HasPrefix(fn.Sel.Name, "New") {
				return true
			}
			x, ok := fn.X.(*ast.Ident)
			if !ok || !google[x.Name] || len(call.Args) == 0 {
				return true
			}
			if first, ok := call.Args[0].(*ast.Ident); !ok || first.Name != "ctx" {
				return true // a sub-service constructor, not a client
			}
			sites++
			last, ok := call.Args[len(call.Args)-1].(*ast.CallExpr)
			fnLast, _ := func() (*ast.Ident, bool) {
				if !ok {
					return nil, false
				}
				id, ok := last.Fun.(*ast.Ident)
				return id, ok
			}()
			if !call.Ellipsis.IsValid() || fnLast == nil || fnLast.Name != "clientOptions" {
				t.Errorf("%s: %s.%s builds a Google client without clientOptions(ctx)...", fset.Position(call.Pos()), x.Name, fn.Sel.Name)
			}
			return true
		})
	}
	if sites < 14 {
		t.Errorf("found %d client construction sites, want at least the 14 that exist: has the check stopped matching?", sites)
	}
}

func TestClientOptions(t *testing.T) {
	t.Parallel()
	if opts := clientOptions(context.Background()); opts != nil {
		t.Errorf("without a caller token: %d options, want none (ADC)", len(opts))
	}
	if opts := clientOptions(callerCtx()); len(opts) != 1 {
		t.Errorf("with a caller token: %d options, want 1", len(opts))
	}
}

// TestComputeClientSendsCallerToken builds a real Compute client the way
// the provider does and checks the caller's token reaches the API.
func TestComputeClientSendsCallerToken(t *testing.T) {
	t.Parallel()
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer srv.Close()
	ctx := callerCtx()
	svc, err := compute.NewService(ctx, append(clientOptions(ctx), option.WithEndpoint(srv.URL))...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Zones.List("p").Context(ctx).Do(); err != nil {
		t.Fatal(err)
	}
	if h, _ := got.Load().(string); h != "Bearer "+callerTok {
		t.Errorf("Compute saw Authorization %q, want the caller's token", h)
	}
}

// TestFleetRESTConfigUsesCallerToken: the multi-cluster path mints its
// cluster credential from the caller's token, not ADC, when one is set.
func TestFleetRESTConfigUsesCallerToken(t *testing.T) {
	t.Parallel()
	var got atomic.Value
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"major":"1","minor":"35"}`))
	}))
	defer srv.Close()
	ctx := callerCtx()
	cfg, err := (&Provider{}).RESTConfig(ctx, cloud.ClusterRef{Name: "c", Endpoint: strings.TrimPrefix(srv.URL, "https://")})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Insecure = true // the test server's self-signed certificate
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Discovery().ServerVersion(); err != nil {
		t.Fatal(err)
	}
	if h, _ := got.Load().(string); h != "Bearer "+callerTok {
		t.Errorf("API server saw Authorization %q, want the caller's token", h)
	}
}
