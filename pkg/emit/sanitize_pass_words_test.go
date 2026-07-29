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

package emit

// Regression drills for issue #106: credentialWords carries
// password/passwd/pwd but NOT the equally common 'pass' /
// 'passphrase'. keyWords splits DB_PASS → {db,pass}, so
// credentialKey("DB_PASS") is false; a human password like "hunter2"
// is well below the secretShaped gate (needs 20+ chars, 3 char
// classes, entropy ≥ 3.0), so it is masked by NEITHER anchor and leaks
// verbatim through SanitizeUnstructured / triage spec / MCP output.
// The same omission sits in reCredentialFlag for --db-pass= /
// --ssl-passphrase= flags.
//
// The precision half of the invariant (§1 principle 8 / AGENTS.md hard
// rule): whole-word / boundary-anchored matching must keep benign
// names like BYPASS and --bypass= from being treated as credentials —
// so the fix cannot be a naive substring add.

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// TestCredentialKey_PassAndPassphrase pins the key-name half of issue
// #106: 'pass' and 'passphrase' are credential words, matched per-word
// so BYPASS/COMPASS/passenger (where "pass" is only a substring) are
// NOT credentials.
func TestCredentialKey_PassAndPassphrase(t *testing.T) {
	for key, want := range map[string]bool{
		"DB_PASS":        true, // keyWords → {db,pass} (issue #106)
		"PG_PASS":        true,
		"REDIS_PASS":     true,
		"SSL_PASSPHRASE": true, // {ssl,passphrase}
		"passphrase":     true,
		// Precision guards: "pass" must match only as a whole word.
		"BYPASS":    false,
		"COMPASS":   false,
		"passenger": false,
	} {
		if got := credentialKey(key); got != want {
			t.Errorf("credentialKey(%q) = %v, want %v (issue #106)", key, got, want)
		}
	}
}

// TestSanitizeObject_PassEnvVars is the leak repro through the real
// spec path (mirrors TestSanitizeObject_Typed): a human password under
// a *_PASS / *_PASSPHRASE env name must be masked — it is not
// secret-shaped, so only the env NAME anchor can catch it.
func TestSanitizeObject_PassEnvVars(t *testing.T) {
	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "prod"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "api",
				Image: "example.com/api:v1",
				Env: []corev1.EnvVar{
					{Name: "DB_PASS", Value: "hunter2"},
					{Name: "PG_PASS", Value: "trustno1"},
					{Name: "SSL_PASSPHRASE", Value: "swordfish"},
					{Name: "LOG_LEVEL", Value: "debug"},
				},
			}},
		},
	}
	got, err := SanitizeObject(pod)
	if err != nil {
		t.Fatal(err)
	}
	out, err := yaml.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"hunter2", "trustno1", "swordfish"} {
		if strings.Contains(string(out), leak) {
			t.Errorf("issue #106: password %q under a *_PASS/PASSPHRASE env name leaked verbatim:\n%s", leak, out)
		}
	}
	// Precision: env NAMES are references that must survive, and a
	// benign var must not be redacted.
	for _, want := range []string{"name: DB_PASS", "name: SSL_PASSPHRASE", "value: debug"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("over-redaction: %q missing from sanitized pod:\n%s", want, out)
		}
	}
}

// TestMaskEnvEntries_PassWords hits maskEnvEntries directly — the
// unstructured env slice as it appears in a spec — so the failure is
// pinned to the name-anchor gap without the surrounding walk.
func TestMaskEnvEntries_PassWords(t *testing.T) {
	env := []any{
		map[string]any{"name": "DB_PASS", "value": "hunter2"},
		map[string]any{"name": "SSL_PASSPHRASE", "value": "swordfish"},
	}
	maskEnvEntries(env)
	for _, e := range env {
		entry := e.(map[string]any)
		if got := entry["value"].(string); got != Redacted {
			t.Errorf("issue #106: maskEnvEntries left %s value = %q, want %q",
				entry["name"], got, Redacted)
		}
	}
}

// TestMaskString_PassFlags pins the flag half of issue #106 and its
// precision guard: --db-pass= / --ssl-passphrase= / --pass values are
// masked, but --bypass= (where "pass" is only a substring) survives.
func TestMaskString_PassFlags(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"db-pass equals", "exec --db-pass=hunter2 --listen=:80", "exec --db-pass=[REDACTED] --listen=:80"},
		{"ssl-passphrase equals", "run --ssl-passphrase=swordfish", "run --ssl-passphrase=[REDACTED]"},
		{"pass space form", "run --pass hunter2", "run --pass [REDACTED]"},
		// Precision guard: not a credential flag — value must survive.
		{"bypass flag survives", "run --bypass=true", "run --bypass=true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MaskString(tt.in); got != tt.want {
				t.Errorf("MaskString(%q) = %q, want %q (issue #106)", tt.in, got, tt.want)
			}
		})
	}
}
