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

package findings

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
	"time"

	"golang.org/x/oauth2"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	findingstate "github.com/go-steer/k8s-lookout/pkg/findings"
	"github.com/go-steer/k8s-lookout/pkg/kube"
	"github.com/go-steer/k8s-lookout/pkg/store"
)

const ackKey = "prod-east/prod/Pod/payment-backend/CrashLoopBackOff"

// callerCtx is a context running as a caller against a fake API server
// that answers access reviews with allowed.
func callerCtx(t *testing.T, allowed bool) context.Context {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
			"status": map[string]any{"allowed": allowed},
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
	ctx := kube.WithSelection(context.Background(), kube.Selection{Kubeconfig: kc})
	ctx = kube.WithCallerToken(ctx, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "caller-token"}))
	return kube.WithCallerName(ctx, "dana@cymbal.example")
}

// seededStore has one open finding-state row for ackKey.
func seededStore(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lookout.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	if err := st.ReplaceFindingStates(context.Background(), "prod-east", []findingstate.State{{
		SubjectKey: ackKey, Cluster: "prod-east", Namespace: "prod", KindOfObject: "Pod", Name: "payment-backend",
		Reason: "CrashLoopBackOff", Severity: "critical", FirstSeen: now, LastSeen: now,
	}}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	return path
}

func ackState(t *testing.T, path string) findingstate.State {
	t.Helper()
	st, err := store.OpenRead(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	rows, err := st.FindingStates(context.Background(), "prod-east")
	if err != nil || len(rows) != 1 {
		t.Fatalf("finding states: %+v, %v", rows, err)
	}
	return rows[0]
}

// TestAckAsCallerOutsideTheirNamespaces: the ack is refused before the
// store is touched.
func TestAckAsCallerOutsideTheirNamespaces(t *testing.T) {
	path := seededStore(t)
	var stdout, stderr bytes.Buffer
	code := emit.Run(callerCtx(t, false), AckCommand(Deps{}).RunConfig(&stdout, &stderr),
		[]string{ackKey, "--store=" + path, "--for=1h", "--by=mallory"})
	if code == emit.ExitData || !strings.Contains(stderr.String(), "cannot list pods in its namespace") {
		t.Fatalf("exit %d, stderr %q: want a refusal", code, stderr.String())
	}
	if s := ackState(t, path); !s.AckUntil.IsZero() || s.AckBy != "" {
		t.Errorf("a refused ack was stored: %+v", s)
	}
}

// TestAckAsCallerIsRecordedAsTheCaller: --by cannot claim someone else.
func TestAckAsCallerIsRecordedAsTheCaller(t *testing.T) {
	path := seededStore(t)
	var stdout, stderr bytes.Buffer
	code := emit.Run(callerCtx(t, true), AckCommand(Deps{}).RunConfig(&stdout, &stderr),
		[]string{ackKey, "--store=" + path, "--for=1h", "--by=somebody-else"})
	if code != emit.ExitData {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if s := ackState(t, path); s.AckBy != "dana@cymbal.example" || s.AckUntil.IsZero() {
		t.Errorf("ack stored as %q until %v, want the caller", s.AckBy, s.AckUntil)
	}
}

// TestDiffAsCallerNeverPersists: a caller's diff reports, but leaves the
// cluster's stored finding state as it was.
func TestDiffAsCallerNeverPersists(t *testing.T) {
	path := seededStore(t)
	report := filepath.Join(t.TempDir(), "scan.logfmt")
	if err := os.WriteFile(report, []byte("scanned=0 findings=0 elapsed=1s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := emit.Run(callerCtx(t, true), DiffCommand(Deps{}).RunConfig(&stdout, &stderr),
		[]string{"--report=" + report, "--store=" + path, "--cluster=prod-east"})
	if code != emit.ExitData {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "resolved") {
		t.Errorf("the empty report did not report the stored subject resolved:\n%s", stdout.String())
	}
	ackState(t, path) // still exactly the one row: nothing was persisted
}
