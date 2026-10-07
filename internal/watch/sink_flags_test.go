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

package watch

import (
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/sources/tokenburn"
)

// TestSinkFlags_Defaults pins the ADDITIVE agent-sink flag surface:
// --sink defaults to core-agent (existing deployments keep the exact
// pre-Sink behavior with zero config change), --sink-url and
// --sink-token-env default empty.
func TestSinkFlags_Defaults(t *testing.T) {
	t.Parallel()
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	if f.sink != sinkCoreAgent {
		t.Errorf("default --sink = %q, want core-agent", f.sink)
	}
	if f.sinkURL != "" {
		t.Errorf("default --sink-url = %q, want empty", f.sinkURL)
	}
	if f.sinkTokenEnv != "" {
		t.Errorf("default --sink-token-env = %q, want empty", f.sinkTokenEnv)
	}
}

// TestSinkFlags_ValidationMatrix is the --sink combination table:
// webhook requires --sink-url and rejects the core-agent session
// concepts (--mode/--target-session/--owner); core-agent rejects the
// webhook-only flags; typos fail in every mode.
func TestSinkFlags_ValidationMatrix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		args    []string
		wantErr string // substring; "" = must validate clean
	}{
		{
			name: "webhook minimal config is valid without daemon flags",
			args: []string{"--sink=webhook", "--sink-url=https://hooks.example:8443"},
		},
		{
			name: "webhook with bearer env is valid",
			args: []string{"--sink=webhook", "--sink-url=https://hooks.example:8443", "--sink-token-env=HOOK_TOKEN"},
		},
		{
			name:    "unknown sink rejected",
			args:    []string{"--sink=kafka", "--dry-run"},
			wantErr: "--sink must be core-agent, webhook or ax",
		},
		{
			name: "ax minimal config is valid without daemon flags",
			args: []string{"--sink=ax", "--ax-server=ax-server.ax-system.svc:8080", "--ax-task-template=/etc/lookout/task.yaml", "--token-env=TOK"},
		},
		{
			name: "ax with owner and a custom router is valid",
			args: []string{"--sink=ax", "--ax-server=ax:8080", "--ax-task-template=t.yaml", "--token-env=TOK", "--owner=lookout", "--ax-router-url=http://router.local"},
		},
		{
			name:    "ax requires ax-server",
			args:    []string{"--sink=ax", "--ax-task-template=t.yaml", "--token-env=TOK"},
			wantErr: "--ax-server is required",
		},
		{
			name:    "ax requires a task template",
			args:    []string{"--sink=ax", "--ax-server=ax:8080", "--token-env=TOK"},
			wantErr: "--ax-task-template is required",
		},
		{
			name:    "ax requires token-env",
			args:    []string{"--sink=ax", "--ax-server=ax:8080", "--ax-task-template=t.yaml"},
			wantErr: "--token-env is required with --sink=ax",
		},
		{
			name: "ax without its flags allowed in dry-run",
			args: []string{"--sink=ax", "--dry-run"},
		},
		{
			name:    "ax rejects shared mode",
			args:    []string{"--sink=ax", "--ax-server=ax:8080", "--ax-task-template=t.yaml", "--token-env=TOK", "--mode=shared", "--target-session=s"},
			wantErr: "--sink=ax always runs one task per incident",
		},
		{
			name:    "ax rejects daemon-url",
			args:    []string{"--sink=ax", "--ax-server=ax:8080", "--ax-task-template=t.yaml", "--token-env=TOK", "--daemon-url=http://daemon.local"},
			wantErr: "--daemon-url is not valid with --sink=ax",
		},
		{
			name:    "ax router trailing slash rejected",
			args:    []string{"--sink=ax", "--dry-run", "--ax-router-url=http://router.local/"},
			wantErr: "--ax-router-url must not end with '/'",
		},
		{
			name:    "core-agent rejects ax flags",
			args:    []string{"--ax-server=ax:8080", "--dry-run"},
			wantErr: "only valid with --sink=ax",
		},
		{
			name:    "webhook rejects ax flags",
			args:    []string{"--sink=webhook", "--sink-url=https://hooks.example", "--ax-task-template=t.yaml"},
			wantErr: "only valid with --sink=ax",
		},
		{
			name:    "webhook requires sink-url",
			args:    []string{"--sink=webhook"},
			wantErr: "--sink-url is required",
		},
		{
			name: "webhook without sink-url allowed in dry-run",
			args: []string{"--sink=webhook", "--dry-run"},
		},
		{
			name:    "sink-url trailing slash rejected in every mode",
			args:    []string{"--sink=webhook", "--sink-url=https://hooks.example/", "--dry-run"},
			wantErr: "--sink-url must not end with '/'",
		},
		{
			name:    "webhook rejects --mode",
			args:    []string{"--sink=webhook", "--sink-url=https://hooks.example", "--mode=shared", "--target-session=sess-1"},
			wantErr: "--mode is a core-agent session concept",
		},
		{
			name:    "webhook rejects --target-session",
			args:    []string{"--sink=webhook", "--sink-url=https://hooks.example", "--target-session=sess-1"},
			wantErr: "--target-session is a core-agent session concept",
		},
		{
			name:    "webhook rejects --owner",
			args:    []string{"--sink=webhook", "--sink-url=https://hooks.example", "--owner=sre@example.com"},
			wantErr: "--owner is a core-agent session concept",
		},
		{
			name:    "core-agent rejects --sink-url",
			args:    []string{"--sink-url=https://hooks.example", "--dry-run"},
			wantErr: "--sink-url is only valid with --sink=webhook",
		},
		{
			name:    "core-agent rejects --sink-token-env",
			args:    []string{"--sink-token-env=HOOK_TOKEN", "--dry-run"},
			wantErr: "--sink-token-env is only valid with --sink=webhook",
		},
		{
			name: "core-agent surface unchanged",
			args: []string{"--daemon-url=http://daemon.local:8420", "--token-env=TOK", "--owner=sre@example.com"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f, err := parseFlags(c.args)
			if err != nil {
				t.Fatalf("parseFlags(%v): %v", c.args, err)
			}
			err = f.validate()
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("validate(%v) = %v, want nil", c.args, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validate(%v) accepted an invalid combination, want error containing %q", c.args, c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("validate(%v) error = %q, want it to contain %q", c.args, err, c.wantErr)
			}
		})
	}
}

// TestSinkFlags_WebhookIdlesTokenBurn: the token-burn source requires
// the core-agent sink (its §12 cost stack IS the daemon's attach
// API). With --sink=webhook the config validates — no daemon flags
// required — and buildSources idles the source with the loud startup
// message instead of constructing it.
func TestSinkFlags_WebhookIdlesTokenBurn(t *testing.T) {
	// NOT t.Parallel — captureLogOutput swaps the global log writer.
	logBuf, restoreLog := captureLogOutput(t)
	defer restoreLog()

	f, err := parseFlags([]string{"--sink=webhook", "--sink-url=https://hooks.example", "--sources=k8s-events,token-burn"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if err := f.validate(); err != nil {
		t.Fatalf("validate: %v (token-burn with --sink=webhook must not demand --daemon-url)", err)
	}
	bs, err := buildSources(f, "", fake.NewSimpleClientset(), nil, nil, nil)
	if err != nil {
		t.Fatalf("buildSources: %v", err)
	}
	if bs.tokenBurn != nil {
		t.Error("token-burn must NOT be constructed with --sink=webhook")
	}
	if _, ok := bs.registry.Lookup(tokenburn.Name); ok {
		t.Error("token-burn must NOT be registered with --sink=webhook")
	}
	if _, ok := bs.registry.Lookup("k8s-events"); !ok {
		t.Error("the other requested sources must still register")
	}
	logs := logBuf.String()
	for _, s := range []string{"token-burn: disabled", "--sink=webhook", "requires --sink=core-agent"} {
		if !strings.Contains(logs, s) {
			t.Errorf("loud idle message missing %q; got: %q", s, logs)
		}
	}
}
