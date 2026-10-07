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

package axsink

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/go-steer/k8s-lookout/internal/axapi"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// fakeAX records task creation and resumes.
type fakeAX struct {
	axapi.UnimplementedAXServer
	mu      sync.Mutex
	created map[string]*axapi.Task
	resumed []string
}

func (f *fakeAX) CreateTask(_ context.Context, req *axapi.CreateTaskRequest) (*axapi.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := req.GetTask().GetMetadata().GetName()
	if _, ok := f.created[name]; ok {
		// What AX actually returns for an existing name.
		return nil, status.Errorf(codes.FailedPrecondition, "task %s already exists and is immutable", name)
	}
	f.created[name] = req.GetTask()
	return req.GetTask(), nil
}

func (f *fakeAX) GetTask(_ context.Context, req *axapi.GetTaskRequest) (*axapi.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.created[req.GetName()]; ok {
		return t, nil
	}
	return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetName())
}

func (f *fakeAX) ResumeTask(_ context.Context, req *axapi.ResumeTaskRequest) (*axapi.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumed = append(f.resumed, req.GetAtespace()+"/"+req.GetName())
	return &axapi.Task{}, nil
}

// fakeRouter stands in for Substrate's router plus the agent's session API.
type fakeRouter struct {
	mu        sync.Mutex
	injects   map[string][]string // actor -> messages
	readyHits int
	notReady  int // answer /readyz with 503 this many times first
}

func (r *fakeRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	actor := req.Header.Get("ate-target-actor")
	if actor == "" {
		http.Error(w, "no ate-target-actor", http.StatusNotFound)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case req.URL.Path == "/readyz":
		r.readyHits++
		if r.notReady > 0 {
			r.notReady--
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	case req.Method == http.MethodPost && req.URL.Path == "/sessions":
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sessionID":"attach-1"}`))
	case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/inject"):
		var body struct {
			Message string `json:"message"`
		}
		b, _ := io.ReadAll(req.Body)
		_ = json.Unmarshal(b, &body)
		if r.injects == nil {
			r.injects = map[string][]string{}
		}
		r.injects[actor+" "+req.URL.Path] = append(r.injects[actor+" "+req.URL.Path], body.Message)
		w.WriteHeader(http.StatusAccepted)
	default:
		http.NotFound(w, req)
	}
}

func newTestAXSink(t *testing.T, router *fakeRouter) (*Sink, *fakeAX) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	ax := &fakeAX{created: map[string]*axapi.Task{}}
	axapi.RegisterAXServer(srv, ax)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	hs := httptest.NewServer(router)
	t.Cleanup(hs.Close)

	sink, err := New(Config{
		Client: axapi.NewAXClient(conn),
		Template: &axapi.Task{
			Metadata: &axapi.ObjectMeta{Atespace: "triage"},
			Spec:     &axapi.TaskSpec{Image: "agent@sha256:abc", Http: &axapi.TaskHTTP{Port: 8484}},
		},
		RouterURL:    hs.URL,
		BearerToken:  "tok",
		StartTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sink, ax
}

func TestAXSink_OpenAndAppend(t *testing.T) {
	router := &fakeRouter{notReady: 2}
	sink, ax := newTestAXSink(t, router)
	ctx := context.Background()

	payload := inject.Payload{Kind: "Pod", Reason: "CrashLoopBackOff", Namespace: "demo", Name: "web-1", UID: "u1", Fingerprint: "fp-1"}
	id, err := sink.OpenIncident(ctx, payload)
	if err != nil {
		t.Fatalf("OpenIncident: %v", err)
	}
	task := taskNameFor(payload)
	if id != task+"/attach-1" {
		t.Errorf("id = %q, want %q", id, task+"/attach-1")
	}
	created := ax.created[task]
	if created == nil {
		t.Fatalf("task %s not created; got %v", task, ax.created)
	}
	if created.GetMetadata().GetAtespace() != "triage" || created.GetSpec().GetImage() != "agent@sha256:abc" {
		t.Errorf("task not built from the template: %v", created)
	}
	if len(ax.resumed) != 1 || ax.resumed[0] != "triage/"+task {
		t.Errorf("resumed = %v", ax.resumed)
	}
	if router.readyHits < 3 {
		t.Errorf("expected the sink to wait for /readyz, got %d checks", router.readyHits)
	}

	if err := sink.Append(ctx, id, inject.ResolvedPayload{Kind: "resolved"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	msgs := router.injects["triage/"+task+" /sessions/attach-1/inject"]
	if len(msgs) != 2 {
		t.Fatalf("expected open + append routed to the task's session, got %v", router.injects)
	}
	if !strings.Contains(msgs[0], `"reason":"CrashLoopBackOff"`) || !strings.Contains(msgs[1], `"kind":"resolved"`) {
		t.Errorf("unexpected messages %q", msgs)
	}
}

func TestAXSink_ReopenReusesTask(t *testing.T) {
	sink, ax := newTestAXSink(t, &fakeRouter{})
	ctx := context.Background()
	payload := inject.Payload{Kind: "Pod", Reason: "OOMKilled", UID: "u2", Fingerprint: "fp-2"}
	id1, err := sink.OpenIncident(ctx, payload)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := sink.OpenIncident(ctx, payload)
	if err != nil {
		t.Fatalf("second open of the same incident: %v", err)
	}
	task1, _, _ := strings.Cut(id1, "/")
	task2, _, _ := strings.Cut(id2, "/")
	if task1 != task2 || len(ax.created) != 1 {
		t.Errorf("same incident should reuse one task: %s vs %s, created %d", task1, task2, len(ax.created))
	}
}

func TestAXSink_AppendRejectsBadID(t *testing.T) {
	sink, _ := newTestAXSink(t, &fakeRouter{})
	if err := sink.Append(context.Background(), "no-slash", inject.Payload{}); err == nil {
		t.Error("expected an error for an id without <task>/<session>")
	}
}

func TestTaskNameFor(t *testing.T) {
	a := taskNameFor(inject.Payload{Fingerprint: "x", UID: "1"})
	b := taskNameFor(inject.Payload{Fingerprint: "x", UID: "2"})
	c := taskNameFor(inject.Payload{UID: "1", Reason: "r"})
	if a != b {
		t.Errorf("same fingerprint should give the same task: %s vs %s", a, b)
	}
	if a == c {
		t.Errorf("different incidents should not share a task")
	}
	if len(a) > 63 || !strings.HasPrefix(a, "lookout-") {
		t.Errorf("task name %q is not a valid short name", a)
	}
}
