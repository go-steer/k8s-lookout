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

// Package axsink is the ax implementation of lookout's agent Sink
// (docs/ax-sink-design.md): each incident runs in its own Agent Executor
// (AX) task on Agent Substrate. It lives under internal/ because it carries
// the AX gRPC stubs, which the embeddable pkg/ half must not depend on.
package axsink

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/go-steer/k8s-lookout/internal/axapi"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// Config configures the ax sink (docs/ax-sink-design.md): each incident
// runs in its own Agent Executor (AX) task on Agent Substrate, and its
// payloads reach the agent through Substrate's router.
type Config struct {
	// Client is the AX API client. Required.
	Client axapi.AXClient
	// Template is the Task every incident runs as: image, command, egress,
	// credentials, http.port. The sink sets metadata.name per incident and
	// never changes anything else. metadata.atespace defaults to "default".
	Template *axapi.Task
	// RouterURL is Agent Substrate's router, e.g.
	// "http://atenet-router.ate-system.svc.cluster.local", without a
	// trailing slash. Requests carry ate-target-actor: <atespace>/<task>.
	RouterURL string
	// BearerToken and AssertedCaller authenticate to the agent's session
	// API inside the task, exactly as for the core-agent sink.
	BearerToken    string
	AssertedCaller string
	// HTTPClient lets tests swap the transport; nil uses the shared sink
	// client (10s timeout, otelhttp).
	HTTPClient *http.Client
	// StartTimeout bounds creating and resuming a task and waiting for its
	// server to come up. Zero means two minutes.
	StartTimeout time.Duration
}

// Sink is the ax implementation of inject.Sink. The incident id it returns is
// "<task>/<session>": the task carries the incident, the session is the
// agent's conversation for it inside the task.
type Sink struct {
	cfg       Config
	atespace  string
	base      *http.Client
	injectors sync.Map // task name -> *inject.Injector
}

var (
	_ inject.Sink          = (*Sink)(nil)
	_ inject.SessionOpener = (*Sink)(nil)
)

// New validates cfg and returns the sink.
func New(cfg Config) (*Sink, error) {
	if cfg.Client == nil {
		return nil, errors.New("ax sink: AX client is required")
	}
	if cfg.Template == nil || cfg.Template.GetSpec() == nil {
		return nil, errors.New("ax sink: a task template with a spec is required")
	}
	if cfg.RouterURL == "" || strings.HasSuffix(cfg.RouterURL, "/") {
		return nil, fmt.Errorf("ax sink: router URL is required and must not end with '/' (got %q)", cfg.RouterURL)
	}
	if cfg.BearerToken == "" {
		return nil, errors.New("ax sink: bearer token for the agent's session API is required")
	}
	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = 2 * time.Minute
	}
	base := cfg.HTTPClient
	if base == nil {
		base = inject.NewSinkHTTPClient()
	}
	atespace := cfg.Template.GetMetadata().GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	return &Sink{cfg: cfg, atespace: atespace, base: base}, nil
}

// OpenIncident creates the incident's task (or finds it, when the same
// incident is opened again), resumes it, then opens a session in it and
// delivers payload — the same POST /sessions + inject pair as the
// core-agent sink, sent through the router.
func (s *Sink) OpenIncident(ctx context.Context, payload any) (string, error) {
	task := taskNameFor(payload)
	if err := s.startTask(ctx, task); err != nil {
		return "", err
	}
	sid, err := s.injectorFor(task).OpenIncident(ctx, payload)
	if sid == "" {
		return "", err
	}
	return task + "/" + sid, err
}

// CreateSession opens an empty incident in a fresh task, for watchboard
// rotation (SessionOpener).
func (s *Sink) CreateSession(ctx context.Context) (string, error) {
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("ax sink: generating task name: %w", err)
	}
	task := "lookout-" + hex.EncodeToString(suffix)
	if err := s.startTask(ctx, task); err != nil {
		return "", err
	}
	sid, err := s.injectorFor(task).CreateSession(ctx)
	if err != nil {
		return "", err
	}
	return task + "/" + sid, nil
}

// Append delivers payload to the incident's session. If the task is
// suspended, Substrate's router resumes it before forwarding.
func (s *Sink) Append(ctx context.Context, id string, payload any) error {
	task, sid, ok := strings.Cut(id, "/")
	if !ok || task == "" || sid == "" {
		return fmt.Errorf("ax sink: incident id %q is not <task>/<session>", id)
	}
	return s.injectorFor(task).Append(ctx, sid, payload)
}

// startTask creates task from the template (an existing task with that name
// is reused), resumes it, and waits until its server answers through the
// router. AX reports a task resumed once its workspace is ready, which can
// be a moment before the agent's own server is listening.
func (s *Sink) startTask(ctx context.Context, task string) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.StartTimeout)
	defer cancel()

	t := proto.Clone(s.cfg.Template).(*axapi.Task)
	if t.Metadata == nil {
		t.Metadata = &axapi.ObjectMeta{}
	}
	t.Metadata.Name = task
	t.Metadata.Atespace = s.atespace
	if t.ApiVersion == "" {
		t.ApiVersion = "ax.io/v1alpha1"
	}
	if t.Kind == "" {
		t.Kind = "Task"
	}
	if _, err := s.cfg.Client.CreateTask(ctx, &axapi.CreateTaskRequest{Task: t}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("ax sink: creating task %s/%s: %w", s.atespace, task, err)
	}
	if _, err := s.cfg.Client.ResumeTask(ctx, &axapi.ResumeTaskRequest{Atespace: s.atespace, Name: task}); err != nil {
		return fmt.Errorf("ax sink: resuming task %s/%s: %w", s.atespace, task, err)
	}
	return s.waitReady(ctx, task)
}

// waitReady polls the task runner's /readyz through the router. With a
// pass-through (spec.http.port) the runner reports ready only once the
// agent's server accepts connections.
func (s *Sink) waitReady(ctx context.Context, task string) error {
	client := s.routedClient(task)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.RouterURL+"/readyz", nil)
		if err != nil {
			return err
		}
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ax sink: task %s/%s not ready: %w", s.atespace, task, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

// injectorFor returns a core-agent Injector that reaches task through the
// router, so the session wire stays identical to the core-agent sink.
func (s *Sink) injectorFor(task string) *inject.Injector {
	if v, ok := s.injectors.Load(task); ok {
		return v.(*inject.Injector)
	}
	inj, _ := inject.NewInjector(inject.Config{ // fields validated in New
		DaemonURL:      s.cfg.RouterURL,
		BearerToken:    s.cfg.BearerToken,
		AssertedCaller: s.cfg.AssertedCaller,
		HTTPClient:     s.routedClient(task),
	})
	v, _ := s.injectors.LoadOrStore(task, inj)
	return v.(*inject.Injector)
}

// routedClient is the base client with ate-target-actor set for task.
func (s *Sink) routedClient(task string) *http.Client {
	base := s.base.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	c := *s.base
	c.Transport = actorTransport{base: base, actor: s.atespace + "/" + task}
	return &c
}

type actorTransport struct {
	base  http.RoundTripper
	actor string
}

func (t actorTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("ate-target-actor", t.actor)
	return t.base.RoundTrip(r)
}

// taskNameFor names the task that carries an incident. It hashes the
// payload's fingerprint, so opening the same incident again (a retry, or a
// re-fire after the dedup cooldown) reaches the same task. Payloads without
// a fingerprint fall back to uid+reason, then to the whole payload.
func taskNameFor(payload any) string {
	b, _ := json.Marshal(payload)
	var fields struct {
		Fingerprint string `json:"fingerprint"`
		UID         string `json:"uid"`
		Reason      string `json:"reason"`
	}
	_ = json.Unmarshal(b, &fields)
	key := fields.Fingerprint
	if key == "" && fields.UID != "" {
		key = fields.UID + "/" + fields.Reason
	}
	if key == "" {
		key = string(b)
	}
	sum := sha256.Sum256([]byte(key))
	return "lookout-" + hex.EncodeToString(sum[:8])
}
