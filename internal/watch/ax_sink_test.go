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
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-steer/k8s-lookout/internal/axapi"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

func writeTemplate(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "task.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAXTaskTemplate(t *testing.T) {
	p := writeTemplate(t, `
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  atespace: triage
spec:
  image: us-docker.pkg.dev/p/r/agent@sha256:abc
  command: [mast, --workload=/opt/mast/workloads/gke-triage]
  resources:
    limits:
      cpu: 500m
      memory: 1Gi
  workspace:
    name: triage
  egress:
    - hosts: [aiplatform.googleapis.com]
      credentials:
        - header: Authorization
          prefix: "Bearer "
          credentialUri: ate-secret://k8s.io/default/creds/google-token/token
  http:
    port: 8484
  idle:
    suspendAfter: 10m
    busyPath: /busy
  onCompletion: Suspend
`)
	tmpl, err := loadAXTaskTemplate(p)
	if err != nil {
		t.Fatalf("loadAXTaskTemplate: %v", err)
	}
	if tmpl.GetMetadata().GetAtespace() != "triage" || tmpl.GetSpec().GetHttp().GetPort() != 8484 {
		t.Errorf("template not decoded: %v", tmpl)
	}
	if got := tmpl.GetSpec().GetEgress()[0].GetCredentials()[0].GetCredentialUri(); !strings.HasPrefix(got, "ate-secret://") {
		t.Errorf("credential uri = %q", got)
	}
	if tmpl.GetSpec().GetWorkspace().GetName() != "triage" {
		t.Errorf("workspace not decoded: %v", tmpl.GetSpec().GetWorkspace())
	}
	if lim := tmpl.GetSpec().GetResources().GetLimits(); lim.GetCpu() != "500m" || lim.GetMemory() != "1Gi" {
		t.Errorf("resource limits not decoded: %v", tmpl.GetSpec().GetResources())
	}
	// AX suspends idle or finished tasks itself when the template asks it to;
	// the sink only has to pass the fields through.
	if idle := tmpl.GetSpec().GetIdle(); idle.GetSuspendAfter() != "10m" || idle.GetBusyPath() != "/busy" || tmpl.GetSpec().GetOnCompletion() != "Suspend" {
		t.Errorf("idle policy not decoded: %v", tmpl.GetSpec())
	}
}

// A template written for the old AX API's workspaces list fails with a
// pointer to the singular field, not only protojson's "unknown field".
func TestLoadAXTaskTemplate_OldWorkspacesListSaysWhatChanged(t *testing.T) {
	_, err := loadAXTaskTemplate(writeTemplate(t, "kind: Task\nspec:\n  image: x\n  workspaces:\n    - name: triage\n"))
	if err == nil || !strings.Contains(err.Error(), "spec.workspace") {
		t.Fatalf("old workspaces list: err=%v, want a pointer to spec.workspace", err)
	}
}

func TestLoadAXTaskTemplate_Rejects(t *testing.T) {
	cases := map[string]string{
		"unknown field": "kind: Task\nspec:\n  image: x\n  imgae: typo\n",
		"wrong kind":    "kind: Workspace\nspec:\n  image: x\n",
		"no spec":       "kind: Task\nmetadata:\n  atespace: a\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := loadAXTaskTemplate(writeTemplate(t, body)); err == nil {
				t.Errorf("expected %s to be rejected", name)
			}
		})
	}
}

type watchTestAXServer struct {
	axapi.UnimplementedAXServer
	mu      sync.Mutex
	created map[string]*axapi.Task
}

func (s *watchTestAXServer) CreateTask(_ context.Context, req *axapi.CreateTaskRequest) (*axapi.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := req.GetTask().GetMetadata().GetName()
	if _, ok := s.created[name]; ok {
		return nil, status.Errorf(codes.FailedPrecondition, "task %s already exists", name)
	}
	s.created[name] = req.GetTask()
	return req.GetTask(), nil
}

func (s *watchTestAXServer) GetTask(_ context.Context, req *axapi.GetTaskRequest) (*axapi.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.created[req.GetName()]; ok {
		return t, nil
	}
	return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetName())
}

func (s *watchTestAXServer) ResumeTask(context.Context, *axapi.ResumeTaskRequest) (*axapi.Task, error) {
	return &axapi.Task{}, nil
}

func startWatchTestAXEnv(t *testing.T) (axAddr, routerURL string, axSrv *watchTestAXServer) {
	t.Helper()
	axSrv = &watchTestAXServer{created: map[string]*axapi.Task{}}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	axapi.RegisterAXServer(gs, axSrv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	var sessions int
	var mu sync.Mutex
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/readyz":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/sessions":
			mu.Lock()
			sessions++
			sid := sessions
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"sessionID":"attach-%d"}`, sid)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/inject"):
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(hs.Close)
	return lis.Addr().String(), hs.URL, axSrv
}

// #847: newAXSink falls back to --ax-task-template when
// --ax-watchboard-task-template is unset, and stamps watchboard tasks from
// --ax-watchboard-task-template when configured while keeping critical
// incident tasks on --ax-task-template.
func TestNewAXSink_WatchboardTemplateFallbackAndDistinct(t *testing.T) {
	incPath := filepath.Join(t.TempDir(), "incident-task.yaml")
	if err := os.WriteFile(incPath, []byte("kind: Task\nmetadata:\n  atespace: triage\nspec:\n  image: incident-agent@sha256:111\n  http:\n    port: 8484\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wbPath := filepath.Join(t.TempDir(), "watchboard-task.yaml")
	if err := os.WriteFile(wbPath, []byte("kind: Task\nmetadata:\n  atespace: watchboard\nspec:\n  image: watchboard-agent@sha256:222\n  http:\n    port: 8484\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	incKey := inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyIncident, ID: "u1/CrashLoopBackOff"}
	wbKey := inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyWatchboard}

	t.Run("default fallback to ax-task-template", func(t *testing.T) {
		axAddr, routerURL, axSrv := startWatchTestAXEnv(t)
		f := &flags{
			axServer:       axAddr,
			axTaskTemplate: incPath,
			axRouterURL:    routerURL,
			axTaskScope:    axScopeIncident,
		}
		sink, closeAX, err := newAXSink(f, "tok")
		if err != nil {
			t.Fatalf("newAXSink: %v", err)
		}
		defer closeAX()

		if _, err := sink.OpenIncidentKeyed(ctx, incKey, inject.Payload{UID: "u1", Reason: "CrashLoopBackOff"}); err != nil {
			t.Fatalf("OpenIncidentKeyed: %v", err)
		}
		wbID, err := sink.CreateSessionKeyed(ctx, wbKey)
		if err != nil {
			t.Fatalf("CreateSessionKeyed: %v", err)
		}
		wbTask, _, _ := strings.Cut(wbID, "/")
		axSrv.mu.Lock()
		defer axSrv.mu.Unlock()
		if got := axSrv.created[wbTask]; got == nil || got.GetSpec().GetImage() != "incident-agent@sha256:111" {
			t.Errorf("watchboard task = %v, want fallback to incident-agent@sha256:111", got)
		}
	})

	t.Run("distinct ax-watchboard-task-template", func(t *testing.T) {
		axAddr, routerURL, axSrv := startWatchTestAXEnv(t)
		buf, restoreLog := captureLogOutput(t)
		defer restoreLog()

		f := &flags{
			axServer:                 axAddr,
			axTaskTemplate:           incPath,
			axWatchboardTaskTemplate: wbPath,
			axRouterURL:              routerURL,
			axTaskScope:              axScopeIncident,
		}
		sink, closeAX, err := newAXSink(f, "tok")
		if err != nil {
			t.Fatalf("newAXSink: %v", err)
		}
		defer closeAX()

		incID, err := sink.OpenIncidentKeyed(ctx, incKey, inject.Payload{UID: "u1", Reason: "CrashLoopBackOff"})
		if err != nil {
			t.Fatalf("OpenIncidentKeyed: %v", err)
		}
		wbID, err := sink.CreateSessionKeyed(ctx, wbKey)
		if err != nil {
			t.Fatalf("CreateSessionKeyed: %v", err)
		}
		incTask, _, _ := strings.Cut(incID, "/")
		wbTask, _, _ := strings.Cut(wbID, "/")

		axSrv.mu.Lock()
		defer axSrv.mu.Unlock()
		if got := axSrv.created[incTask]; got == nil || got.GetSpec().GetImage() != "incident-agent@sha256:111" {
			t.Errorf("incident task = %v, want incident-agent@sha256:111", got)
		}
		if got := axSrv.created[wbTask]; got == nil || got.GetSpec().GetImage() != "watchboard-agent@sha256:222" || got.GetMetadata().GetAtespace() != "watchboard" {
			t.Errorf("watchboard task = %v, want watchboard-agent@sha256:222 in atespace watchboard", got)
		}
		if !strings.Contains(buf.String(), wbPath) {
			t.Errorf("startup log %q did not record watchboard template path %q", buf.String(), wbPath)
		}
	})
}

func TestNewAXSink_WatchboardTemplateRejectsInvalidManifest(t *testing.T) {
	incPath := writeTemplate(t, "kind: Task\nspec:\n  image: incident-agent@sha256:111\n")
	cases := map[string]string{
		"missing file":  filepath.Join(t.TempDir(), "does-not-exist.yaml"),
		"unknown field": writeTemplate(t, "kind: Task\nspec:\n  image: x\n  bogus: 1\n"),
		"wrong kind":    writeTemplate(t, "kind: Workspace\nspec:\n  image: x\n"),
		"no spec":       writeTemplate(t, "kind: Task\nmetadata:\n  atespace: wb\n"),
	}
	for name, wbPath := range cases {
		t.Run(name, func(t *testing.T) {
			f := &flags{
				axServer:                 "127.0.0.1:1",
				axTaskTemplate:           incPath,
				axWatchboardTaskTemplate: wbPath,
				axRouterURL:              "http://127.0.0.1:1",
				axTaskScope:              axScopeIncident,
			}
			_, _, err := newAXSink(f, "tok")
			if err == nil || !strings.Contains(err.Error(), "--ax-watchboard-task-template") {
				t.Errorf("%s: newAXSink err = %v, want error naming --ax-watchboard-task-template", name, err)
			}
		})
	}
}
