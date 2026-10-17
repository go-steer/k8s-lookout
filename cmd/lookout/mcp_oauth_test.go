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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
)

const (
	oauthTestClientSecret = "GOCSPX-planted-in-the-test"
	oauthTestKey          = "c2VjcmV0LWtleS1tYXRlcmlhbC0zMi1ieXRlcy0hISE="
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// oauthArgs is a complete, loopback Path B configuration.
func oauthArgs(t *testing.T) []string {
	return []string{
		"--listen=127.0.0.1:0",
		"--google-client-id=123-abc.apps.googleusercontent.com",
		"--google-allow=@example.com",
		"--resource-url=http://127.0.0.1/mcp",
		"--oauth-issuer=http://127.0.0.1",
		"--google-client-secret-file=" + writeFile(t, "client.json",
			`{"web":{"client_id":"123-abc.apps.googleusercontent.com","client_secret":"`+oauthTestClientSecret+`"}}`),
		"--oauth-custody=sealed",
		"--oauth-keys-file=" + writeFile(t, "keys", "k1 "+oauthTestKey+"\n"),
		"--oauth-open-registration",
	}
}

func assertNoOAuthSecrets(t *testing.T, out string) {
	t.Helper()
	for _, s := range []string{oauthTestClientSecret, oauthTestKey, "cc-planted-secret"} {
		if strings.Contains(out, s) {
			t.Errorf("output carries a secret: %s", out)
		}
	}
}

// TestMCPOAuthServes: the real wiring mounts the authorization server on
// --listen and tells the operator which redirect URI to register.
func TestMCPOAuthServes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	var stdout, stderr bytes.Buffer
	if code := mcpMain(ctx, oauthArgs(t), &stdout, &stderr); code != 0 || strings.Contains(stderr.String(), "internal") {
		t.Fatalf("exit %d, stderr: %s", code, stderr.String())
	}
	for _, want := range []string{"authorization server for generic MCP clients at http://127.0.0.1", "http://127.0.0.1/callback"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("banner lacks %q: %s", want, stderr.String())
		}
	}
	if stdout.Len() != 0 {
		t.Errorf("serving wrote to stdout: %q", stdout.String())
	}
	assertNoOAuthSecrets(t, stderr.String())
}

// TestMCPOAuthClientCredentialsUseTheWorkloadRules: client-credentials
// clients are workloads, governed by --workload-rules, which then stands
// without --mtls-listen; without rules they are refused at startup.
func TestMCPOAuthClientCredentialsUseTheWorkloadRules(t *testing.T) {
	clients := "--oauth-clients-file=" + writeFile(t, "clients.json",
		`[{"client_id":"robot","client_secret":"cc-planted-secret","client_credentials":true}]`)

	var stdout, stderr bytes.Buffer
	if code := mcpMain(t.Context(), append(oauthArgs(t), clients), &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "client:<client_id>") {
		t.Errorf("client-credentials clients without rules: exit %d, stderr: %s", code, stderr.String())
	}
	assertNoOAuthSecrets(t, stderr.String())

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	stdout.Reset()
	stderr.Reset()
	args := append(oauthArgs(t), clients,
		"--workload-rules="+writeRules(t, "rules:\n- name: robot\n  identities: [client:robot]\n  tools: [triage]\n"))
	if code := mcpMain(ctx, args, &stdout, &stderr); code != 0 || strings.Contains(stderr.String(), "internal") {
		t.Fatalf("exit %d, stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "1 rule(s) in --workload-rules decide what each client-credentials client may call") {
		t.Errorf("banner lacks the workload rules: %s", stderr.String())
	}
	assertNoOAuthSecrets(t, stderr.String())

	// A service account on the client is refused: it belongs on the rule.
	stderr.Reset()
	sa := "--oauth-clients-file=" + writeFile(t, "clients.json",
		`[{"client_id":"robot","client_secret":"cc-planted-secret","service_account":"r@p.iam.gserviceaccount.com"}]`)
	args = append(oauthArgs(t), sa,
		"--workload-rules="+writeRules(t, "rules:\n- name: robot\n  identities: [client:robot]\n  tools: [triage]\n"))
	if code := mcpMain(t.Context(), args, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "--workload-rules rule") {
		t.Errorf("a client naming a service account: exit %d, stderr: %s", code, stderr.String())
	}
}

func TestMCPOAuthUsage(t *testing.T) {
	without := func(args []string, prefix string) []string {
		var out []string
		for _, a := range args {
			if !strings.HasPrefix(a, prefix) {
				out = append(out, a)
			}
		}
		return out
	}
	base := oauthArgs(t)
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"oauth flag without issuer":      {[]string{"--oauth-open-registration"}, "pass --oauth-issuer"},
		"secret file without issuer":     {[]string{"--google-client-secret-file=x"}, "pass --oauth-issuer"},
		"issuer without Google mode":     {[]string{"--listen=127.0.0.1:0", "--oauth-issuer=http://127.0.0.1"}, "needs Google-token mode"},
		"no secret file":                 {without(base, "--google-client-secret-file"), "--google-client-secret-file"},
		"no custody":                     {without(base, "--oauth-custody"), "--oauth-custody"},
		"bad custody":                    {append(without(base, "--oauth-custody"), "--oauth-custody=disk"), "want vault or sealed"},
		"vault without auth provider":    {append(without(base, "--oauth-custody"), "--oauth-custody=vault"), "--oauth-vault-auth-provider"},
		"auth provider with sealed":      {append(base, "--oauth-vault-auth-provider=projects/p/locations/global/authProviders/a"), "only applies to --oauth-custody=vault"},
		"no keys":                        {without(base, "--oauth-keys-file"), "exactly one of --oauth-keys-file and --oauth-keys-secret"},
		"two key sources":                {append(base, "--oauth-keys-secret=projects/p/secrets/s/versions/latest"), "exactly one"},
		"no client could sign in":        {without(base, "--oauth-open-registration"), "no client could sign in"},
		"rules without a way in":         {[]string{"--workload-rules=x"}, "pass --mtls-listen"},
		"unreadable keys":                {append(without(base, "--oauth-keys-file"), "--oauth-keys-file="+filepath.Join(t.TempDir(), "absent")), "--oauth-keys-file="},
		"malformed keys":                 {append(without(base, "--oauth-keys-file"), "--oauth-keys-file="+writeFile(t, "bad", "k1 "+oauthTestKey[:10]+"\n")), "--oauth-keys-file="},
		"unreadable client secret file":  {append(without(base, "--google-client-secret-file"), "--google-client-secret-file="+filepath.Join(t.TempDir(), "absent")), "--google-client-secret-file="},
		"client secret file not console": {append(without(base, "--google-client-secret-file"), "--google-client-secret-file="+writeFile(t, "c.json", `{"other":{}}`)), "--google-client-secret-file="},
	} {
		var stdout, stderr bytes.Buffer
		if code := mcpMain(t.Context(), tc.args, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), tc.want) {
			t.Errorf("%s: exit %d, want 2 naming %q; stderr: %s", name, code, tc.want, stderr.String())
		}
		assertNoOAuthSecrets(t, stderr.String())
	}
}

// TestMCPOAuthGoogleSideNeedsTheGKEBuild: the vault and Secret Manager are
// called as lookout's own identity, which only the GKE build can do; a
// build without it refuses at startup.
func TestMCPOAuthGoogleSideNeedsTheGKEBuild(t *testing.T) {
	if cloud.CanServiceClient() {
		t.Skip("this build can call Google APIs as lookout")
	}
	without := func(args []string, prefix string) []string {
		var out []string
		for _, a := range args {
			if !strings.HasPrefix(a, prefix) {
				out = append(out, a)
			}
		}
		return out
	}
	for name, args := range map[string][]string{
		"vault": append(without(oauthArgs(t), "--oauth-custody"), "--oauth-custody=vault",
			"--oauth-vault-auth-provider=projects/p/locations/global/authProviders/a"),
		"secret manager": append(without(oauthArgs(t), "--oauth-keys-file"), "--oauth-keys-secret=projects/p/secrets/s/versions/latest"),
	} {
		var stdout, stderr bytes.Buffer
		if code := mcpMain(t.Context(), args, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "-tags gke") {
			t.Errorf("%s: exit %d, stderr: %s", name, code, stderr.String())
		}
	}
}
