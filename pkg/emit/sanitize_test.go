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

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// specFixtures are the §13 golden fixtures: specs with a credential
// planted in every position we know of.
var specFixtures = []string{"pod.yaml", "secret.yaml", "deployment.yaml", "configmap.yaml"}

// fixtureMarkers maps every planted credential position to the
// unique needle that must never appear in any sanitized output or
// committed golden file. Secret.data needles are the base64 forms
// actually present in the fixture (the raw marker never appears
// un-encoded there).
var fixtureMarkers = map[string]string{
	"pod env literal value, credential-named, main container": "SUPERSECRETVALUE_ENV",
	"pod env literal value, init container":                   "SUPERSECRETVALUE_INIT",
	"pod env literal value, ephemeral container":              "SUPERSECRETVALUE_EPHEMERAL",
	"pod env JWT-shaped value under benign name":              "SUPERSECRETVALUE_JWT",
	"pod env URL userinfo password":                           "SUPERSECRETVALUE_URL",
	"pod container args --db-password=":                       "SUPERSECRETVALUE_ARGS",
	"pod init container args two-element --vault-token":       "SUPERSECRETVALUE_PAIRARG",
	"pod annotation with credential-named key":                "SUPERSECRETVALUE_ANNOTATION",
	"pod annotation AWS key ID by shape":                      "AKIASUPERSECRETVALUE",
	"pod last-applied-configuration annotation":               "SUPERSECRETVALUE_LASTAPPLIED",
	"Secret.data password (base64 needle)":                    "U1VQRVJTRUNSRVRWQUxVRV9EQVRB",
	"Secret.data tls.key (base64 needle)":                     "U0VDUkVUVkFMVUVfVExTS0VZ",
	"Secret.stringData api-token":                             "SUPERSECRETVALUE_STRINGDATA",
	"deployment template env, credential-named":               "SUPERSECRETVALUE_TPL",
	"deployment template annotation, credential-named key":    "SUPERSECRETVALUE_TPL_ANNOTATION",
	"deployment PEM private key under benign env name":        "SUPERSECRETVALUE_PEM",
	"configmap base64 value under credential-named key":       "x9QzSUPERSECRETVALUEb64K2pq",
	"configmap hex value under credential-named key":          "5ec4e7abad1dea5ec4e7abad1dea5ec4e7",
	"configmap bearer token by shape":                         "SUPERSECRETVALUE_BEARER",
	"configmap GCP service-account private_key":               "SUPERSECRETVALUE_GCPSA",
}

// findingMarkers plant credentials in finding positions; the
// findings below carry them through the DefaultSanitizer.
var findingMarkers = map[string]string{
	"finding message JWT":                              "SUPERSECRETVALUE_FMSG",
	"finding message Basic auth (base64 needle)":       "U1VQRVJTRUNSRVRWQUxVRV9CQVNJQw",
	"finding detail base64 under credential-named key": "SUPERSECRETVALUEfind",
	"finding detail URL userinfo password":             "SUPERSECRETVALUE_FURL",
}

// markerFindings is the finding-side credential fixture. The
// secret=db-credentials detail is the precision control: a resource
// *name* under a credential-ish key must survive.
var markerFindings = []Finding{
	{
		Kind:     "pod.crashloop",
		Severity: SeverityCritical,
		Message:  `container exited after auth with "Bearer eyJhbGciOiJSUzI1NiJ9.SUPERSECRETVALUE_FMSGx.c2lnbmF0dXJl", retrying`,
		Details: []Field{
			{Key: "api_token", Value: "x9QzSUPERSECRETVALUEfindK2pq"},
			{Key: "secret", Value: "db-credentials"},
			{Key: "registry_url", Value: "https://ci:SUPERSECRETVALUE_FURL@registry.example.com/v2"},
		},
	},
	{
		Kind:    "pod.pull_error",
		Reason:  "ErrImagePull",
		Message: "401 fetching manifest with header Basic U1VQRVJTRUNSRVRWQUxVRV9CQVNJQw==",
	},
}

func loadFixture(t *testing.T, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "sanitize", name))
	if err != nil {
		t.Fatal(err)
	}
	var u map[string]any
	if err := yaml.Unmarshal(raw, &u); err != nil {
		t.Fatalf("parsing %s: %v", name, err)
	}
	return u
}

func sanitizeFixture(t *testing.T, name string) []byte {
	t.Helper()
	out, err := yaml.Marshal(SanitizeUnstructured(loadFixture(t, name)))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestSanitizeObject_Golden pins the full sanitized rendering of
// every fixture. Review the goldens by eye on every change: they are
// the §13 contract for what survives and what is masked.
func TestSanitizeObject_Golden(t *testing.T) {
	for _, name := range specFixtures {
		t.Run(name, func(t *testing.T) {
			checkGolden(t, filepath.Join("sanitize", name+".golden"), sanitizeFixture(t, name))
		})
	}
}

// TestNoFixtureCredentialLeaks is the CI tripwire (§13): every
// fixture is sanitized fresh AND every committed golden file is
// scanned; any credential marker surviving anywhere fails the test
// naming its position. It also asserts each marker IS present in the
// raw fixtures, so a reworded fixture cannot silently make the
// tripwire vacuous.
func TestNoFixtureCredentialLeaks(t *testing.T) {
	var rawAll, sanitizedAll bytes.Buffer
	for _, name := range specFixtures {
		raw, err := os.ReadFile(filepath.Join("testdata", "sanitize", name))
		if err != nil {
			t.Fatal(err)
		}
		rawAll.Write(raw)
		sanitizedAll.Write(sanitizeFixture(t, name))

		golden, err := os.ReadFile(filepath.Join("testdata", "sanitize", name+".golden"))
		if err != nil {
			t.Fatalf("missing golden (run go test ./pkg/emit -update): %v", err)
		}
		sanitizedAll.Write(golden)
	}

	for position, marker := range fixtureMarkers {
		if !bytes.Contains(rawAll.Bytes(), []byte(marker)) {
			t.Errorf("tripwire is vacuous: marker %q (%s) not present in any raw fixture", marker, position)
		}
		if bytes.Contains(sanitizedAll.Bytes(), []byte(marker)) {
			t.Errorf("SECRET LEAK at position %q: marker %q survived sanitization", position, marker)
		}
	}
	// Catch-all: no variant of the shared marker prefix survives in
	// any spec output, planted or future.
	if bytes.Contains(sanitizedAll.Bytes(), []byte("SUPERSECRETVALUE")) {
		t.Error("SECRET LEAK: a SUPERSECRETVALUE marker survived spec sanitization")
	}

	// Finding surface: encode marker findings through a production
	// Writer (DefaultSanitizer) in both formats.
	var rawFindings, encoded bytes.Buffer
	for _, f := range markerFindings {
		rawFindings.WriteString(f.Reason + "\n" + f.Message + "\n")
		for _, d := range f.Details {
			rawFindings.WriteString(d.Key + "=" + d.Value + "\n")
		}
	}
	for _, format := range []Format{FormatLogfmt, FormatJSON} {
		w, err := NewWriter(&encoded, format)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range markerFindings {
			if err := w.Emit(f); err != nil {
				t.Fatal(err)
			}
		}
	}
	for position, marker := range findingMarkers {
		if !bytes.Contains(rawFindings.Bytes(), []byte(marker)) {
			t.Errorf("tripwire is vacuous: marker %q (%s) not present in any marker finding", marker, position)
		}
		if bytes.Contains(encoded.Bytes(), []byte(marker)) {
			t.Errorf("SECRET LEAK at position %q: marker %q survived the finding sanitizer", position, marker)
		}
	}
	// The precision control must still be present.
	if !bytes.Contains(encoded.Bytes(), []byte("secret=db-credentials")) {
		t.Error("over-redaction: detail secret=db-credentials (a resource name) did not survive")
	}
}

// TestSanitizeSurvivors pins the precision side: references, names,
// and digests that MUST survive sanitization.
func TestSanitizeSurvivors(t *testing.T) {
	pod := string(sanitizeFixture(t, "pod.yaml"))
	for _, want := range []string{
		"db-credentials",     // secretKeyRef name
		"payments-env",       // envFrom secretRef name
		"payments-tls",       // secret volume secretName
		"payments-ca",        // projected secret source name
		"csi-creds",          // CSI nodePublishSecretRef name
		"payments-kv",        // CSI secretProviderClass value
		"emptyDir",           // load-bearing empty object survives eliding
		"prometheus.io/path", // benign annotation
		"sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", // image digest
		"postgres://payments:[REDACTED]@db.prod.svc:5432/payments",                // DSN minus password
		"--db-password=[REDACTED]", // flag masked, flag name survives
		"payments-api-7d9c4b",      // ownerReference name survives (uid does not)
	} {
		if !strings.Contains(pod, want) {
			t.Errorf("over-redaction: %q missing from sanitized pod", want)
		}
	}
	for _, gone := range []string{
		"managedFields", "resourceVersion", "selfLink", "creationTimestamp",
		"uid", "imageID", "containerID", "observedGeneration",
		"last-applied-configuration",
	} {
		if strings.Contains(pod, gone) {
			t.Errorf("system metadata %q survived sanitization", gone)
		}
	}

	secret := string(sanitizeFixture(t, "secret.yaml"))
	for _, want := range []string{
		"password: '[REDACTED:21B]'",  // decoded content length, never a prefix
		"tls.key: '[REDACTED:84B]'",   // PEM decoded length
		"empty: '[REDACTED:0B]'",      // empty vs non-empty stays answerable
		"api-token: '[REDACTED:27B]'", // stringData masked with raw length
	} {
		if !strings.Contains(secret, want) {
			t.Errorf("secret masking: %q missing from sanitized Secret:\n%s", want, secret)
		}
	}

	cm := string(sanitizeFixture(t, "configmap.yaml"))
	for _, want := range []string{
		"endpoint: https://api.example.com/v1",                                       // benign URL survives
		"checksum: 2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", // hex under benign key survives
	} {
		if !strings.Contains(cm, want) {
			t.Errorf("over-redaction: %q missing from sanitized ConfigMap", want)
		}
	}
}

// TestSanitizeObject_Typed proves the typed client-go path: the same
// rules apply after JSON normalization, and the input is not mutated.
func TestSanitizeObject_Typed(t *testing.T) {
	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "prod", ResourceVersion: "99", UID: "abc-123"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "api",
				Image: "example.com/api:v1",
				Env: []corev1.EnvVar{
					{Name: "DB_PASSWORD", Value: "hunter2-typed-secret"},
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
	if strings.Contains(string(out), "hunter2-typed-secret") {
		t.Errorf("typed env credential survived:\n%s", out)
	}
	for _, want := range []string{"name: DB_PASSWORD", "value: '[REDACTED]'", "value: debug"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("sanitized typed pod missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(string(out), "resourceVersion") {
		t.Errorf("resourceVersion survived typed sanitization:\n%s", out)
	}
	if pod.Spec.Containers[0].Env[0].Value != "hunter2-typed-secret" {
		t.Error("SanitizeObject mutated its input")
	}
}

func TestMaskString(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{"pem private key surgical", "before\n-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY-----\nafter", "before\n[REDACTED]\nafter"},
		{"pem in json escaped", `{"private_key":"-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n"}`, `{"private_key":"[REDACTED]"}`},
		{"certificate not masked", "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----", "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----"},
		{"jwt", "token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r expired", "token [REDACTED] expired"},
		{"bearer header", "sent Authorization: Bearer c2VjcmV0dG9rZW52YWx1ZQ then failed", "sent Authorization: Bearer [REDACTED] then failed"},
		{"basic header", "Authorization: Basic dXNlcjpodW50ZXIy00", "Authorization: Basic [REDACTED]"},
		{"basic prose not masked", "Basic configuration missing", "Basic configuration missing"},
		{"aws key id", "found AKIAIOSFODNN7EXAMPLE in env", "found [REDACTED] in env"},
		{"url password", "dsn is postgres://app:hunter2@db:5432/x", "dsn is postgres://app:[REDACTED]@db:5432/x"},
		{"url no userinfo untouched", "https://db.prod.svc:5432/path", "https://db.prod.svc:5432/path"},
		{"flag equals", "exec --db-password=hunter2 --listen=:80", "exec --db-password=[REDACTED] --listen=:80"},
		{"flag space", "run --token abc123def", "run --token [REDACTED]"},
		{"flag ref suffix survives", "--secret-name=db-credentials", "--secret-name=db-credentials"},
		{"image digest untouched", "image@sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08", "image@sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"},
		{"uid untouched", "uid 550e8400-e29b-41d4-a716-446655440000", "uid 550e8400-e29b-41d4-a716-446655440000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MaskString(tt.in); got != tt.want {
				t.Errorf("MaskString(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestCredentialKey(t *testing.T) {
	for key, want := range map[string]bool{
		"DB_PASSWORD":           true,
		"MIGRATE_DB_TOKEN":      true,
		"SIGNING_KEY":           true,
		"APIKEY":                true,
		"DEBUG_AUTH":            true,
		"example.com/api-token": true,
		"secretProviderClass":   true, // key-anchored rules still need a shaped value
		"topologyKey":           true, // ditto — shape gate protects these
		"DATABASE_URL":          false,
		"BOOTSTRAP_ASSERTION":   false,
		"checksum":              false,
		"author":                false, // "auth" must not match inside a word
		"LOG_LEVEL":             false,
	} {
		if got := credentialKey(key); got != want {
			t.Errorf("credentialKey(%q) = %v, want %v", key, got, want)
		}
	}
}

func TestSecretShaped(t *testing.T) {
	for value, want := range map[string]bool{
		"x9QzSUPERSECRETVALUEb64K2pq":          true,  // base64, 3 classes, high entropy
		"5ec4e7abad1dea5ec4e7abad1dea5ec4e7":   true,  // hex ≥ 32
		"db-credentials":                       false, // resource name: short, single class
		"my-deployment-name-here-longer":       false, // single class fails the gate
		"550e8400-e29b-41d4-a716-446655440000": false, // uid: two classes only
		"kubernetes.io/service-account-token":  false, // not base64 charset
		"payments-kv":                          false,
	} {
		if got := secretShaped(value); got != want {
			t.Errorf("secretShaped(%q) = %v, want %v", value, got, want)
		}
	}
}

func TestSanitizeFinding_PrecisionControls(t *testing.T) {
	f := SanitizeFinding(Finding{
		Kind:    "config.drift",
		Reason:  "SecretChanged",
		Message: "secret db-credentials changed (hash 2c26b46b68ffc68f)",
		Details: []Field{{Key: "secret", Value: "db-credentials"}},
	})
	if f.Reason != "SecretChanged" || f.Details[0].Value != "db-credentials" {
		t.Errorf("over-redaction of names: %+v", f)
	}
	if !strings.Contains(f.Message, "db-credentials") {
		t.Errorf("over-redaction of message: %q", f.Message)
	}
}
