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

package triage

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/oauth2"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/memory"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

// runStatusCtx runs `triage status` with ctx, the way lookout mcp does in
// Google-token mode.
func runStatusCtx(ctx context.Context, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := emit.Run(ctx, StatusCommand().RunConfig(&stdout, &stderr), args)
	return code, stdout.String(), stderr.String()
}

// TestStatusRecordsCaller: a write made on a caller's behalf stores and
// prints recorded_by.
func TestStatusRecordsCaller(t *testing.T) {
	path := newStatusStore(t)
	ctx := kube.WithCallerName(context.Background(), "dana@cymbal.example")
	code, out, errOut := runStatusCtx(ctx, "--store="+path, "--fingerprint="+testFP, "--resource="+testKey, "--status=triaged")
	if code != emit.ExitData {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if !strings.Contains(out, "recorded_by=dana@cymbal.example") {
		t.Errorf("recorded_by not printed:\n%s", out)
	}
	st, err := store.OpenRead(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	recs, err := st.TriageStatuses(context.Background(), memory.TriageQuery{Fingerprint: testFP})
	if err != nil || len(recs) != 1 || recs[0].RecordedBy != "dana@cymbal.example" {
		t.Errorf("stored record: %+v, %v", recs, err)
	}
}

// TestStatusRefusesWriteOutsideCallersNamespaces: running as a caller who
// cannot list pods in the record's namespace, the write is refused and
// nothing is stored.
func TestStatusRefusesWriteOutsideCallersNamespaces(t *testing.T) {
	path := newStatusStore(t)
	kc := denyingAPIServer(t)
	ctx := kube.WithSelection(context.Background(), kube.Selection{Kubeconfig: kc})
	ctx = kube.WithCallerToken(ctx, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "caller-token"}))
	code, _, errOut := runStatusCtx(ctx, "--store="+path, "--fingerprint="+testFP, "--resource="+testKey, "--status=triaged")
	if code == emit.ExitData || !strings.Contains(errOut, "cannot list pods in its namespace") {
		t.Fatalf("exit %d, stderr %q: want a refusal", code, errOut)
	}
	st, err := store.OpenRead(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if recs, _ := st.TriageStatuses(context.Background(), memory.TriageQuery{Fingerprint: testFP}); len(recs) != 0 {
		t.Errorf("a refused write was stored: %+v", recs)
	}
}

// denyingAPIServer answers every access review "not allowed", over
// verified TLS, and returns a kubeconfig for it.
func denyingAPIServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
			"status": map[string]any{"allowed": false},
		})
	}))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	kc := filepath.Join(dir, "kubeconfig")
	cfg := "apiVersion: v1\nkind: Config\ncurrent-context: c\nclusters:\n- name: c\n  cluster:\n    server: " + srv.URL +
		"\n    certificate-authority: " + ca + "\ncontexts:\n- name: c\n  context:\n    cluster: c\n    user: u\n" +
		"users:\n- name: u\n  user:\n    token: lookout-own\n"
	if err := os.WriteFile(kc, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return kc
}
