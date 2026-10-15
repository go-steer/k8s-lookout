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

// Command ge-token-spike is a throwaway MCP server for issue #698: does the
// Google access token a Gemini Enterprise app sends for each person work
// for what lookout needs? It serves one tool, spike_report, which runs every
// check with the caller's own token and returns a pass/fail table. The token
// is never logged or returned.
//
// It is built on steward v0.1.0 (googletoken, allow, httpauth, mcpauth), so
// it is also the first run of steward against a real Gemini Enterprise.
//
// Configuration, all required except PORT:
//
//	SPIKE_CLIENT_ID   the OAuth client ID the data store is configured with
//	SPIKE_MEMBERS     comma-separated emails or @domain entries to admit
//	SPIKE_RESOURCE    this server's MCP URL, e.g. https://ge-token-spike-123.us-central1.run.app/mcp
//	SPIKE_CLUSTER     projects/P/locations/L/clusters/C, the GKE cluster to read
//	PORT              listen port (Cloud Run sets it; default 8080)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/go-steer/steward/allow"
	"github.com/go-steer/steward/googletoken"
	"github.com/go-steer/steward/httpauth"
	"github.com/go-steer/steward/mcpauth"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("ge-token-spike", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	h, metaPath, metaHandler, err := newHandler(cfg, log, nil)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle(metaPath, metaHandler)
	mux.Handle(resourcePath(cfg.Resource), h)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Info("ge-token-spike listening", "addr", srv.Addr, "resource", cfg.Resource, "metadata", metaPath)
	return srv.ListenAndServe()
}

type config struct {
	ClientID string
	Members  []string
	Resource string
	Target   Target
	Port     string
}

func configFromEnv() (config, error) {
	c := config{
		ClientID: os.Getenv("SPIKE_CLIENT_ID"),
		Resource: os.Getenv("SPIKE_RESOURCE"),
		Port:     os.Getenv("PORT"),
	}
	for _, m := range strings.Split(os.Getenv("SPIKE_MEMBERS"), ",") {
		if m = strings.TrimSpace(m); m != "" {
			c.Members = append(c.Members, m)
		}
	}
	if c.Port == "" {
		c.Port = "8080"
	}
	var errs []error
	if c.ClientID == "" {
		errs = append(errs, errors.New("SPIKE_CLIENT_ID is required"))
	}
	if len(c.Members) == 0 {
		errs = append(errs, errors.New("SPIKE_MEMBERS is required"))
	}
	if c.Resource == "" {
		errs = append(errs, errors.New("SPIKE_RESOURCE is required"))
	}
	t, err := parseCluster(os.Getenv("SPIKE_CLUSTER"))
	if err != nil {
		errs = append(errs, err)
	}
	c.Target = t
	return c, errors.Join(errs...)
}

// parseCluster parses projects/P/locations/L/clusters/C.
func parseCluster(s string) (Target, error) {
	p := strings.Split(strings.Trim(s, "/"), "/")
	if len(p) != 6 || p[0] != "projects" || p[2] != "locations" || p[4] != "clusters" || p[1] == "" || p[3] == "" || p[5] == "" {
		return Target{}, fmt.Errorf("SPIKE_CLUSTER %q: want projects/P/locations/L/clusters/C", s)
	}
	return Target{Project: p[1], Location: p[3], Cluster: p[5]}, nil
}

func resourcePath(resource string) string {
	u, err := url.Parse(resource)
	if err != nil || u.Path == "" {
		return "/"
	}
	return u.Path
}

// newHandler builds the protected MCP handler. spike, when non-nil,
// replaces the real Google endpoints (tests); verifierCfg likewise.
func newHandler(cfg config, log *slog.Logger, override *testOverrides) (http.Handler, string, http.Handler, error) {
	vc := googletoken.Config{
		ClientIDs:      []string{cfg.ClientID},
		RequiredScopes: []string{cloudScope},
		Logger:         log,
	}
	spike := &Spike{Target: cfg.Target, HTTP: &http.Client{Timeout: 15 * time.Second}}
	if override != nil {
		vc.TokenInfoURL, vc.UserInfoURL = override.TokenInfoURL, override.UserInfoURL
		spike = override.Spike
	}
	v, err := googletoken.New(vc)
	if err != nil {
		return nil, "", nil, err
	}
	admit, err := allow.Parse(cfg.Members)
	if err != nil {
		return nil, "", nil, err
	}
	a, err := googletoken.NewAuthenticator(v, admit)
	if err != nil {
		return nil, "", nil, err
	}
	p, err := httpauth.New(httpauth.Options{
		Authenticator: a,
		Resource:      cfg.Resource,
		Scopes:        []string{"openid", "email", cloudScope},
		ResourceName:  "lookout Gemini Enterprise token spike (#698)",
		Logger:        log,
	})
	if err != nil {
		return nil, "", nil, err
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "ge-token-spike", Version: "0.1.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name: "spike_report",
		Description: "Runs the lookout issue #698 checks with the caller's own Google token: who Google says " +
			"the caller is, whether the token carries cloud-platform, whether a GKE cluster authenticates it as " +
			"the caller (DNS and IP endpoints), and whether Compute, Monitoring and Logging accept it. " +
			"Show the returned table to the user verbatim.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, _ any) (*mcp.CallToolResult, any, error) {
		id, ok := mcpauth.Identity(req)
		ts, ok2 := mcpauth.TokenSource(req)
		if !ok || !ok2 {
			return nil, nil, errors.New("no caller token on this request")
		}
		tok, err := ts.Token()
		if err != nil {
			return nil, nil, fmt.Errorf("caller token: %w", err)
		}
		results := spike.Run(ctx, id, tok.AccessToken)
		log.Info("spike_report", "caller", id.Email, "results", results)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: Format(results)}}}, nil, nil
	})
	sdk := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	return mcpauth.Middleware(p)(sdk), p.MetadataPath(), p.MetadataHandler(), nil
}

// testOverrides points the server at fake Google endpoints.
type testOverrides struct {
	TokenInfoURL, UserInfoURL string
	Spike                     *Spike
}
