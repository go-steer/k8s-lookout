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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
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
	"github.com/go-steer/k8s-lookout/pkg/store"
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
// Sessions are per task (actor): attach-1, attach-2, ... An inject into a
// session the task doesn't have answers 404, as core-agent does.
type fakeRouter struct {
	mu        sync.Mutex
	injects   map[string][]string // "<actor> <path>" -> messages
	sessions  map[string]int      // actor -> sessions created
	readyHits int
	notReady  int // answer /readyz with 503 this many times first
	failWith  int // answer injects with this status, when set
}

func (r *fakeRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	actor := req.Header.Get("ate-target-actor")
	if actor == "" {
		http.Error(w, "no ate-target-actor", http.StatusNotFound)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions == nil {
		r.sessions = map[string]int{}
		r.injects = map[string][]string{}
	}
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
		r.sessions[actor]++
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"sessionID":"attach-%d"}`, r.sessions[actor])
	case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/inject"):
		if r.failWith != 0 {
			http.Error(w, "nope", r.failWith)
			return
		}
		var n int
		if _, err := fmt.Sscanf(strings.TrimPrefix(req.URL.Path, "/sessions/"), "attach-%d/inject", &n); err != nil || n < 1 || n > r.sessions[actor] {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}
		var body struct {
			Message string `json:"message"`
		}
		b, _ := io.ReadAll(req.Body)
		_ = json.Unmarshal(b, &body)
		r.injects[actor+" "+req.URL.Path] = append(r.injects[actor+" "+req.URL.Path], body.Message)
		w.WriteHeader(http.StatusAccepted)
	default:
		http.NotFound(w, req)
	}
}

// forget drops every session the agent in actor has, as if its state
// had been lost.
func (r *fakeRouter) forget(actor string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[actor] = 0
}

func newTestAXSink(t *testing.T, router *fakeRouter) (*Sink, *fakeAX) {
	t.Helper()
	return newTestAXSinkScoped(t, router, ScopeIncident)
}

func newTestAXSinkScoped(t *testing.T, router *fakeRouter, scope Scope) (*Sink, *fakeAX) {
	t.Helper()
	client, ax := newFakeAXClient(t)
	hs := httptest.NewServer(router)
	t.Cleanup(hs.Close)

	sink, err := New(Config{
		Client:       client,
		Template:     testTemplate(),
		RouterURL:    hs.URL,
		BearerToken:  "tok",
		Scope:        scope,
		StartTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sink, ax
}

func testTemplate() *axapi.Task {
	return &axapi.Task{
		Metadata: &axapi.ObjectMeta{Atespace: "triage"},
		Spec:     &axapi.TaskSpec{Image: "agent@sha256:abc", Http: &axapi.TaskHTTP{Port: 8484}},
	}
}

// newFakeAXClient serves a fakeAX over bufconn and returns a client for it.
func newFakeAXClient(t *testing.T) (axapi.AXClient, *fakeAX) {
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
	return axapi.NewAXClient(conn), ax
}

var (
	crashKey = inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyIncident, ID: "u1/CrashLoopBackOff"}
	oomKey   = inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyIncident, ID: "u2/OOMKilled"}
	boardKey = inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyWatchboard}
)

func TestAXSink_OpenAndAppend(t *testing.T) {
	router := &fakeRouter{notReady: 2}
	sink, ax := newTestAXSink(t, router)
	ctx := context.Background()

	payload := inject.Payload{Kind: "Pod", Reason: "CrashLoopBackOff", Namespace: "demo", Name: "web-1", UID: "u1", Fingerprint: "fp-1"}
	id, err := sink.OpenIncidentKeyed(ctx, crashKey, payload)
	if err != nil {
		t.Fatalf("OpenIncidentKeyed: %v", err)
	}
	task := sink.taskFor(crashKey)
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

// An https router works end to end (#580): readiness, session open, inject
// and append all go over TLS, with the bearer token and ate-target-actor.
func TestAXSink_HTTPSRouter(t *testing.T) {
	router := &fakeRouter{}
	var mu sync.Mutex
	var plain, unauthenticated int
	hs := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.TLS == nil {
			plain++
		}
		// The readiness probe is unauthenticated; the session calls are not.
		if strings.HasPrefix(r.URL.Path, "/sessions") && r.Header.Get("Authorization") != "Bearer tok" {
			unauthenticated++
		}
		mu.Unlock()
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(hs.Close)

	// The shared sink client (timeout, trace propagation) with the test
	// server's CA trusted, as SSL_CERT_FILE would make it in production.
	httpClient := inject.NewSinkHTTPClient()
	httpClient.Transport = hs.Client().Transport

	client, _ := newFakeAXClient(t)
	sink, err := New(Config{
		Client:       client,
		Template:     testTemplate(),
		RouterURL:    hs.URL,
		BearerToken:  "tok",
		HTTPClient:   httpClient,
		StartTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hs.URL, "https://") {
		t.Fatalf("test server URL %q is not https", hs.URL)
	}

	ctx := context.Background()
	id, err := sink.OpenIncidentKeyed(ctx, crashKey, inject.Payload{Kind: "Pod", Reason: "CrashLoopBackOff", UID: "u1"})
	if err != nil {
		t.Fatalf("OpenIncidentKeyed over https: %v", err)
	}
	if err := sink.Append(ctx, id, inject.ResolvedPayload{Kind: "resolved"}); err != nil {
		t.Fatalf("Append over https: %v", err)
	}
	task := sink.taskFor(crashKey)
	if got := router.injects["triage/"+task+" /sessions/attach-1/inject"]; len(got) != 2 {
		t.Fatalf("expected open + append delivered over https, got %v", router.injects)
	}
	mu.Lock()
	defer mu.Unlock()
	if plain != 0 || unauthenticated != 0 {
		t.Errorf("plain-http requests = %d, session calls without the bearer token = %d; want 0 and 0", plain, unauthenticated)
	}
}

// Reopening an incident goes back into the same task AND the same
// session, so the agent keeps its conversation.
func TestAXSink_ReopenReusesTaskAndSession(t *testing.T) {
	router := &fakeRouter{}
	sink, ax := newTestAXSink(t, router)
	ctx := context.Background()
	payload := inject.Payload{Kind: "Pod", Reason: "OOMKilled", UID: "u2"}
	id1, err := sink.OpenIncidentKeyed(ctx, oomKey, payload)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := sink.OpenIncidentKeyed(ctx, oomKey, payload)
	if err != nil {
		t.Fatalf("second open of the same incident: %v", err)
	}
	if id1 != id2 || len(ax.created) != 1 {
		t.Errorf("same incident should reuse its task and session: %s vs %s, created %d", id1, id2, len(ax.created))
	}
	task := sink.taskFor(oomKey)
	if n := router.sessions["triage/"+task]; n != 1 {
		t.Errorf("POST /sessions called %d times, want 1", n)
	}
	if msgs := router.injects["triage/"+task+" /sessions/attach-1/inject"]; len(msgs) != 2 {
		t.Errorf("both opens should land in attach-1, got %v", router.injects)
	}
}

// When the agent no longer has the remembered session (404), the reopen
// starts a new one and remembers that instead.
func TestAXSink_ReopenAfterSessionLostOpensNewSession(t *testing.T) {
	router := &fakeRouter{}
	sink, _ := newTestAXSink(t, router)
	ctx := context.Background()
	payload := inject.Payload{Kind: "Pod", Reason: "OOMKilled", UID: "u2"}
	if _, err := sink.OpenIncidentKeyed(ctx, oomKey, payload); err != nil {
		t.Fatal(err)
	}
	task := sink.taskFor(oomKey)
	router.forget("triage/" + task)

	id, err := sink.OpenIncidentKeyed(ctx, oomKey, payload)
	if err != nil {
		t.Fatalf("reopen after the session was lost: %v", err)
	}
	if id != task+"/attach-1" || router.sessions["triage/"+task] != 1 {
		t.Errorf("want a fresh session in the same task, got %s (sessions %d)", id, router.sessions["triage/"+task])
	}
	if got, _ := sink.recall(ctx, oomKey); got != id {
		t.Errorf("remembered %q, want the new session %q", got, id)
	}
}

// Any other failure keeps the incident bound to its remembered session
// and reports the delivery error, like a partial open.
func TestAXSink_ReopenDeliveryFailureStaysBound(t *testing.T) {
	router := &fakeRouter{}
	sink, _ := newTestAXSink(t, router)
	ctx := context.Background()
	id1, err := sink.OpenIncidentKeyed(ctx, oomKey, inject.Payload{UID: "u2"})
	if err != nil {
		t.Fatal(err)
	}
	router.failWith = http.StatusServiceUnavailable
	id2, err := sink.OpenIncidentKeyed(ctx, oomKey, inject.Payload{UID: "u2"})
	if err == nil || id2 != id1 {
		t.Errorf("got (%q, %v), want (%q, an error)", id2, err, id1)
	}
}

// With a store, the incident -> session mapping survives a restart: a
// brand new sink (empty memory) reopens into the old session.
func TestAXSink_ReopenAcrossRestartUsesTheStore(t *testing.T) {
	router := &fakeRouter{}
	st, err := store.Open(filepath.Join(t.TempDir(), "lookout.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	payload := inject.Payload{UID: "u1", Reason: "CrashLoopBackOff"}

	before, _ := newTestAXSink(t, router)
	before.UseSessionStore("prod", st)
	id1, err := before.OpenIncidentKeyed(ctx, crashKey, payload)
	if err != nil {
		t.Fatal(err)
	}

	after, _ := newTestAXSink(t, router) // the restarted sentinel
	after.UseSessionStore("prod", st)
	id2, err := after.OpenIncidentKeyed(ctx, crashKey, payload)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Errorf("after a restart the incident should reopen into %s, got %s", id1, id2)
	}

	// Without the store, the restarted sink can't know and opens anew.
	cold, _ := newTestAXSink(t, router)
	id3, err := cold.OpenIncidentKeyed(ctx, crashKey, payload)
	if err != nil {
		t.Fatal(err)
	}
	if id3 == id1 {
		t.Errorf("a sink without the store should open a new session, got the old one %s", id3)
	}
}

// Cluster scope: one task for the cluster, a session per incident, and
// the watchboard in the same task.
func TestAXSink_ClusterScopeSharesOneTask(t *testing.T) {
	router := &fakeRouter{}
	sink, ax := newTestAXSinkScoped(t, router, ScopeCluster)
	ctx := context.Background()
	a, err := sink.OpenIncidentKeyed(ctx, crashKey, inject.Payload{UID: "u1"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := sink.OpenIncidentKeyed(ctx, oomKey, inject.Payload{UID: "u2"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := sink.CreateSessionKeyed(ctx, boardKey)
	if err != nil {
		t.Fatal(err)
	}
	taskA, sidA, _ := splitID(a)
	taskB, sidB, _ := splitID(b)
	taskW, sidW, _ := splitID(w)
	if taskA != taskB || taskA != taskW || len(ax.created) != 1 {
		t.Errorf("cluster scope should use one task, got %s %s %s (created %d)", taskA, taskB, taskW, len(ax.created))
	}
	if sidA == sidB || sidA == sidW || sidB == sidW {
		t.Errorf("each incident should get its own session: %s %s %s", sidA, sidB, sidW)
	}
	if !strings.HasPrefix(taskA, "lookout-prod-") {
		t.Errorf("cluster task %q should carry the cluster name", taskA)
	}
}

// The watchboard has one stable task per cluster; each rotation is a new
// session in it, not a new task.
func TestAXSink_WatchboardStaysInOneTask(t *testing.T) {
	router := &fakeRouter{}
	sink, ax := newTestAXSink(t, router)
	ctx := context.Background()
	w1, err := sink.CreateSessionKeyed(ctx, boardKey)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := sink.CreateSessionKeyed(ctx, boardKey)
	if err != nil {
		t.Fatal(err)
	}
	t1, s1, _ := splitID(w1)
	t2, s2, _ := splitID(w2)
	if t1 != t2 || s1 == s2 || len(ax.created) != 1 {
		t.Errorf("rotation should open a new session in the same task: %s, %s (created %d)", w1, w2, len(ax.created))
	}
	if !strings.HasPrefix(t1, "lookout-wb-prod-") {
		t.Errorf("watchboard task %q should be lookout-wb-<cluster>-...", t1)
	}
}

func TestAXSink_AppendRejectsBadID(t *testing.T) {
	sink, _ := newTestAXSink(t, &fakeRouter{})
	for _, id := range []string{"no-slash", "/sid", "task/"} {
		if err := sink.Append(context.Background(), id, inject.Payload{}); err == nil {
			t.Errorf("expected an error for id %q", id)
		}
	}
}

var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func TestTaskFor(t *testing.T) {
	incident, _ := New(Config{Client: axapi.NewAXClient(nil), Template: &axapi.Task{Spec: &axapi.TaskSpec{}}, RouterURL: "http://r", BearerToken: "t"})
	cluster, _ := New(Config{Client: axapi.NewAXClient(nil), Template: &axapi.Task{Spec: &axapi.TaskSpec{}}, RouterURL: "http://r", BearerToken: "t", Scope: ScopeCluster})

	stormA := inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyStorm, ID: "Node//gke-pool-1-abcd"}
	stormB := inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyStorm, ID: "Node//gke-pool-2-wxyz"}
	otherCluster := crashKey
	otherCluster.Cluster = "staging"

	if incident.taskFor(stormA) == incident.taskFor(stormB) {
		t.Error("storms on different ancestors must not share a task")
	}
	if incident.taskFor(crashKey) == incident.taskFor(otherCluster) {
		t.Error("the same object key in two clusters must not share a task")
	}
	if incident.taskFor(crashKey) == incident.taskFor(oomKey) {
		t.Error("different incidents must not share a task")
	}
	if again, _ := New(Config{Client: axapi.NewAXClient(nil), Template: &axapi.Task{Spec: &axapi.TaskSpec{}}, RouterURL: "http://r", BearerToken: "t"}); again.taskFor(crashKey) != incident.taskFor(crashKey) {
		t.Error("task names must be stable across sinks (and restarts)")
	}
	if cluster.taskFor(crashKey) != cluster.taskFor(stormA) || cluster.taskFor(crashKey) == cluster.taskFor(otherCluster) {
		t.Error("cluster scope: one task per cluster")
	}

	long := inject.IncidentKey{Cluster: "projects/my-project/locations/us-central1/clusters/" + strings.Repeat("Very_Long.", 10), Kind: inject.IncidentKeyWatchboard}
	for _, name := range []string{
		incident.taskFor(crashKey), incident.taskFor(stormA), incident.taskFor(boardKey), incident.taskFor(long),
		incident.taskFor(inject.IncidentKey{Kind: inject.IncidentKeyWatchboard}),
		cluster.taskFor(crashKey), cluster.taskFor(long), cluster.taskFor(inject.IncidentKey{Cluster: "---"}),
	} {
		if len(name) > 63 || !dnsLabel.MatchString(name) || !strings.HasPrefix(name, "lookout-") {
			t.Errorf("task name %q is not a valid AX task name", name)
		}
	}
}

// Without a key, the sink rebuilds one from the payload — and never from
// the fingerprint, which is the incident class.
func TestKeyFromPayload(t *testing.T) {
	a := keyFromPayload(inject.Payload{Cluster: "prod", Fingerprint: "x", UID: "1", Reason: "BackOff", ReasonClass: "ImagePullBackOff"})
	b := keyFromPayload(inject.Payload{Cluster: "prod", Fingerprint: "x", UID: "2", Reason: "BackOff", ReasonClass: "ImagePullBackOff"})
	if a == b {
		t.Errorf("same fingerprint, different objects must give different keys: %v", a)
	}
	if want := (inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyIncident, ID: "1/ImagePullBackOff"}); a != want {
		t.Errorf("key = %+v, want %+v", a, want)
	}
	storm := keyFromPayload(inject.StormPayload{Cluster: "prod", Fingerprint: "x", AncestorKind: "Node", AncestorName: "n1"})
	if want := (inject.IncidentKey{Cluster: "prod", Kind: inject.IncidentKeyStorm, ID: "Node//n1"}); storm != want {
		t.Errorf("storm key = %+v, want %+v", storm, want)
	}
	board := keyFromPayload(inject.WatchboardDigestPayload{Cluster: "prod"})
	if board != boardKey {
		t.Errorf("digest key = %+v, want %+v", board, boardKey)
	}
}
