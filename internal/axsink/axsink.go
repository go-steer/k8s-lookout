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
// (docs/ax-sink-design.md): incidents run in Agent Executor (AX) tasks on
// Agent Substrate, one task per incident or one per cluster, named from
// lookout's own incident identity. It lives under internal/ because it carries
// the AX gRPC stubs, which the embeddable pkg/ half must not depend on.
package axsink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/go-steer/k8s-lookout/internal/axapi"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// BearerCredentials returns gRPC PerRPCCredentials that attach
// "authorization: Bearer <token>" metadata to every RPC over a
// transport-secured (TLS) connection. RequireTransportSecurity is
// always true so bearer tokens never cross plaintext gRPC links (#580).
func BearerCredentials(token string) credentials.PerRPCCredentials {
	return bearerPerRPCCreds{token: token}
}

type bearerPerRPCCreds struct {
	token string
}

func (b bearerPerRPCCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (bearerPerRPCCreds) RequireTransportSecurity() bool {
	return true
}

// Scope is how incidents map onto AX tasks (--ax-task-scope).
type Scope string

const (
	// ScopeIncident runs each incident (and each storm) in its own task,
	// plus one stable watchboard task per cluster. The default.
	ScopeIncident Scope = "incident"
	// ScopeCluster runs one long-lived task per cluster; every incident,
	// storm and watchboard digest gets its own session inside it.
	ScopeCluster Scope = "cluster"
)

// SessionStore persists which incident id ("<task>/<session>") each
// incident was last opened into, so a reopen after a restart reaches the
// same conversation. *store.Store implements it (--store).
type SessionStore interface {
	SinkSession(ctx context.Context, cluster, incidentKey string) (string, bool, error)
	PutSinkSession(ctx context.Context, cluster, incidentKey, id string) error
}

// Config configures the ax sink (docs/ax-sink-design.md): each incident
// runs in an Agent Executor (AX) task on Agent Substrate, and its
// payloads reach the agent through Substrate's router.
type Config struct {
	// Client is the AX API client. Required.
	Client axapi.AXClient
	// Template is the Task every incident runs as: image, command, egress,
	// credentials, http.port. The sink sets metadata.name per task and
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
	// Scope maps incidents onto tasks. Empty means ScopeIncident.
	Scope Scope
	// HTTPClient lets tests swap the transport; nil uses the shared sink
	// client (10s timeout, otelhttp).
	HTTPClient *http.Client
	// StartTimeout bounds creating and resuming a task and waiting for its
	// server to come up. Zero means two minutes.
	StartTimeout time.Duration
	// Now is the clock for holding reminders (hold.go); nil is time.Now.
	Now func() time.Time
}

// maxRemembered bounds the in-memory incident -> session map. Past it, an
// arbitrary entry is forgotten; that incident's next reopen consults the
// store (if any) or opens a new session.
const maxRemembered = 4096

// maxInjectors bounds the per-task *inject.Injector cache so high-churn
// per-incident tasks cannot grow Sink memory without bound. When full, the
// entry with the oldest lastUsed timestamp is evicted.
const maxInjectors = 4096

type injectorEntry struct {
	inj      *inject.Injector
	lastUsed time.Time
}

// Sink is the ax implementation of inject.Sink. The incident id it returns is
// "<task>/<session>": the task carries the incident, the session is the
// agent's conversation for it inside the task.
type Sink struct {
	cfg      Config
	atespace string
	base     *http.Client
	stores   sync.Map // cluster -> SessionStore

	injMu     sync.Mutex
	injectors map[string]*injectorEntry // task name -> cached injector + lastUsed

	mu         sync.Mutex
	remembered map[string]string // IncidentKey.String() -> "<task>/<session>"

	hold      *holder       // held reminders and each task's last real inject (hold.go)
	idleAfter time.Duration // a task with no real inject for this long is idle
}

func (s *Sink) now() time.Time {
	if s.cfg.Now != nil {
		return s.cfg.Now()
	}
	return time.Now()
}

var (
	_ inject.Sink          = (*Sink)(nil)
	_ inject.SessionOpener = (*Sink)(nil)
	_ inject.KeyedOpener   = (*Sink)(nil)
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
		return nil, fmt.Errorf("ax sink: router URL is required and must not end with '/' (got %q)", inject.RedactedURL(cfg.RouterURL))
	}
	if cfg.BearerToken == "" {
		return nil, errors.New("ax sink: bearer token for the agent's session API is required")
	}
	switch cfg.Scope {
	case "":
		cfg.Scope = ScopeIncident
	case ScopeIncident, ScopeCluster:
	default:
		return nil, fmt.Errorf("ax sink: task scope must be %q or %q (got %q)", ScopeIncident, ScopeCluster, cfg.Scope)
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
	return &Sink{
		cfg:        cfg,
		atespace:   atespace,
		base:       base,
		injectors:  map[string]*injectorEntry{},
		remembered: map[string]string{},
		hold:       newHolder(),
		idleAfter:  idleAfterFor(cfg.Template),
	}, nil
}

// UseSessionStore makes the sink remember cluster's incident sessions in
// st as well as in memory, so a reopen after a restart reaches the same
// session. The sink is process-wide and each cluster runner has its own
// store, hence the cluster. A later call for the same cluster replaces
// the earlier store (a restarted runner reopens its store).
func (s *Sink) UseSessionStore(cluster string, st SessionStore) {
	if st == nil {
		s.stores.Delete(cluster)
		return
	}
	s.stores.Store(cluster, st)
}

func (s *Sink) isTaskKnownAwake(task string) bool {
	s.hold.mu.Lock()
	last, ok := s.hold.lastReal[task]
	s.hold.mu.Unlock()
	return ok && s.now().Sub(last) < s.idleAfter
}

func (s *Sink) clearAwake(task string) {
	s.hold.mu.Lock()
	delete(s.hold.lastReal, task)
	s.hold.mu.Unlock()
}

// OpenIncidentKeyed opens the incident lookout identifies as key
// (inject.KeyedOpener). The task comes from the key and the scope; if
// this incident was opened before and its session is still known, the
// payload goes into that session so the agent keeps the conversation.
// Otherwise it opens a new session — the same POST /sessions + inject
// pair as the core-agent sink, sent through the router.
//
// Fast path: when task received a real inject within s.idleAfter, skip
// startTask (avoiding redundant CreateTask/GetTask/ResumeTask + /readyz
// round-trips) and attempt direct session delivery first; on 404/410 or
// session creation failure, clear wake state and fall back to startTask.
func (s *Sink) OpenIncidentKeyed(ctx context.Context, key inject.IncidentKey, payload any) (string, error) {
	task := s.taskFor(key)
	inj := s.injectorFor(task)

	recalledID, hasRecalled := s.recall(ctx, key)
	recalledGone := false

	if s.isTaskKnownAwake(task) {
		s.flushHeld(ctx, task)
		if hasRecalled {
			if t, sid, ok := splitID(recalledID); ok && t == task {
				err := inj.Append(ctx, sid, payload)
				var se *inject.StatusError
				switch {
				case err == nil:
					s.remember(ctx, key, recalledID)
					s.paid(recalledID)
					s.noteReal(task)
					return recalledID, nil
				case !errors.As(err, &se) || (se.Code != http.StatusNotFound && se.Code != http.StatusGone):
					s.owe(recalledID)
					return recalledID, err
				default:
					// 404 or 410: task or session gone — fall through to startTask + new session.
					recalledGone = true
					s.clearAwake(task)
				}
			}
		} else {
			// Task is known awake (e.g. ScopeCluster with another incident open):
			// attempt opening the new session directly without startTask RPCs.
			if sid, err := inj.OpenIncident(ctx, payload); sid != "" {
				id := task + "/" + sid
				s.remember(ctx, key, id)
				if err != nil {
					s.owe(id)
					return id, err
				}
				s.paid(id)
				s.noteReal(task)
				return id, nil
			}
			s.clearAwake(task)
		}
	}

	if err := s.startTask(ctx, task); err != nil {
		return "", err
	}
	// The task is awake now: reminders held for it go first.
	s.flushHeld(ctx, task)
	if hasRecalled && !recalledGone {
		if t, sid, ok := splitID(recalledID); ok && t == task {
			err := inj.Append(ctx, sid, payload)
			var se *inject.StatusError
			switch {
			case err == nil:
				s.remember(ctx, key, recalledID)
				s.paid(recalledID)
				s.noteReal(task)
				return recalledID, nil
			case !errors.As(err, &se) || (se.Code != http.StatusNotFound && se.Code != http.StatusGone):
				// The session may well still be there (a timeout, a
				// 5xx): stay bound to it and let the caller count the
				// failed delivery, as for a partial open.
				s.owe(recalledID)
				return recalledID, err
			}
			// 404 or 410: the agent no longer has the session (task
			// recreated, agent state lost). Start a new one.
		}
	}
	sid, err := inj.OpenIncident(ctx, payload)
	if sid == "" {
		return "", err
	}
	id := task + "/" + sid
	s.remember(ctx, key, id)
	if err != nil {
		// The session exists but the payload never arrived.
		s.owe(id)
		return id, err
	}
	s.paid(id)
	s.noteReal(task)
	return id, nil
}

// CreateSessionKeyed opens an empty session in key's task
// (inject.KeyedOpener). The watchboard uses it for its first session and
// for each rotation, so it always opens a new session, never a
// remembered one.
func (s *Sink) CreateSessionKeyed(ctx context.Context, key inject.IncidentKey) (string, error) {
	task := s.taskFor(key)
	inj := s.injectorFor(task)
	if s.isTaskKnownAwake(task) {
		s.flushHeld(ctx, task)
		if sid, err := inj.CreateSession(ctx); err == nil && sid != "" {
			s.noteReal(task)
			return task + "/" + sid, nil
		}
		s.clearAwake(task)
	}
	if err := s.startTask(ctx, task); err != nil {
		return "", err
	}
	s.flushHeld(ctx, task)
	sid, err := inj.CreateSession(ctx)
	if err != nil {
		return "", err
	}
	s.noteReal(task)
	return task + "/" + sid, nil
}

// OpenIncident is the plain Sink verb, for callers that don't pass
// lookout's incident identity (lookout's dispatcher always does, through
// OpenIncidentKeyed). The key is rebuilt from the payload as well as it
// can be: uid + reason class for an incident, the ancestor for a storm,
// otherwise the cluster's watchboard task.
func (s *Sink) OpenIncident(ctx context.Context, payload any) (string, error) {
	return s.OpenIncidentKeyed(ctx, keyFromPayload(payload), payload)
}

// CreateSession is the SessionOpener verb for callers without a key: an
// empty session in the (cluster-less) watchboard task.
func (s *Sink) CreateSession(ctx context.Context) (string, error) {
	return s.CreateSessionKeyed(ctx, inject.IncidentKey{Kind: inject.IncidentKeyWatchboard})
}

// Append delivers payload to the incident's session. If the task is
// suspended, Substrate's router resumes it before forwarding.
//
// A pure reminder (repeat_count > 0) is the exception (issue #599): if the
// task is idle it is held, not sent, and Append returns nil; the latest
// held reminder per session goes ahead of the next real inject to the
// task. Anything else goes straight through, held reminders first.
func (s *Sink) Append(ctx context.Context, id string, payload any) error {
	task, sid, ok := splitID(id)
	if !ok {
		return fmt.Errorf("ax sink: incident id %q is not <task>/<session>", id)
	}
	if cluster, isReminder := reminder(payload); isReminder {
		// A reminder after a failed real inject may be the agent's first
		// word on the incident: never hold it.
		if !s.owes(id) {
			switch s.stateOf(ctx, task) {
			case taskGone:
				s.dropHeldIn(task)
				return s.goneError(task)
			case taskIdle:
				s.holdReminder(id, cluster, payload)
				log.Printf("ax sink: holding the reminder for %s: the task is idle, so it waits for the next real inject", id)
				return nil
			}
		}
		s.forgetHeld(id)
		err := s.injectorFor(task).Append(ctx, sid, payload)
		if err == nil {
			s.paid(id) // it carries the incident's payload
			s.countReminder(cluster, ReminderDirect)
		}
		return err
	}
	s.flushHeld(ctx, task)
	err := s.injectorFor(task).Append(ctx, sid, payload)
	if err != nil {
		s.owe(id)
		return err
	}
	s.paid(id)
	s.noteReal(task)
	return nil
}

func splitID(id string) (task, sid string, ok bool) {
	task, sid, ok = strings.Cut(id, "/")
	return task, sid, ok && task != "" && sid != ""
}

// recall returns the incident id key was last opened into: from memory,
// else from the cluster's store.
func (s *Sink) recall(ctx context.Context, key inject.IncidentKey) (string, bool) {
	s.mu.Lock()
	id, ok := s.remembered[key.String()]
	s.mu.Unlock()
	if ok {
		return id, true
	}
	st, ok := s.stores.Load(key.Cluster)
	if !ok {
		return "", false
	}
	id, ok, err := st.(SessionStore).SinkSession(ctx, key.Cluster, key.String())
	if err != nil {
		log.Printf("ax sink: looking up the session for %s: %v (opening a new one)", key, err)
		return "", false
	}
	return id, ok
}

// remember records that key now lives in id, in memory and in the
// cluster's store. A store failure only costs session reuse after a
// restart, so it is logged, not returned.
func (s *Sink) remember(ctx context.Context, key inject.IncidentKey, id string) {
	k := key.String()
	s.mu.Lock()
	if _, ok := s.remembered[k]; !ok && len(s.remembered) >= maxRemembered {
		for old := range s.remembered {
			delete(s.remembered, old)
			break
		}
	}
	s.remembered[k] = id
	s.mu.Unlock()
	if st, ok := s.stores.Load(key.Cluster); ok {
		if err := st.(SessionStore).PutSinkSession(ctx, key.Cluster, k, id); err != nil {
			log.Printf("ax sink: remembering the session for %s: %v (a reopen after a restart will start a new session)", key, err)
		}
	}
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
	if _, err := s.cfg.Client.CreateTask(ctx, &axapi.CreateTaskRequest{Task: t}); err != nil {
		// AX refuses a second task with the same name (FailedPrecondition:
		// tasks are immutable). That's the reopen case: use the task. Ask
		// rather than match the error, so any other failure still surfaces.
		if _, gerr := s.cfg.Client.GetTask(ctx, &axapi.GetTaskRequest{Atespace: s.atespace, Name: task}); gerr != nil {
			return fmt.Errorf("ax sink: creating task %s/%s: %w", s.atespace, task, err)
		}
	}
	if _, err := s.cfg.Client.ResumeTask(ctx, &axapi.ResumeTaskRequest{Atespace: s.atespace, Name: task}); err != nil {
		return fmt.Errorf("ax sink: resuming task %s/%s: %w", s.atespace, task, err)
	}
	return s.waitReady(ctx, task)
}

// waitReady polls the task runner's /readyz through the router. With a
// pass-through (spec.http.port) the runner reports ready only once the
// agent's server accepts connections.
//
// On timeout the error carries the last attempt's outcome. A task still
// starting looks the same as a router that will never answer (an
// untrusted certificate, a name mismatch, a SPIFFE ID missing from the
// router's allowlist) until something says which, and the first incident
// is usually where an operator finds out.
func (s *Sink) waitReady(ctx context.Context, task string) error {
	client := s.routedClient(task)
	last := "no attempt completed"
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.RouterURL+"/readyz", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		switch {
		case err != nil:
			// The attempt the deadline cut short says nothing new.
			// net/http strips any URL password from its errors.
			if ctx.Err() == nil {
				last = err.Error()
			}
		case resp.StatusCode == http.StatusOK:
			_ = resp.Body.Close()
			return nil
		default:
			_ = resp.Body.Close()
			last = "router answered " + resp.Status
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ax sink: task %s/%s not ready: %w (last attempt: %s)", s.atespace, task, ctx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

// injectorFor returns a core-agent Injector that reaches task through the
// router, so the session wire stays identical to the core-agent sink.
// Evicts the oldest idle entry when the cache exceeds maxInjectors.
func (s *Sink) injectorFor(task string) *inject.Injector {
	now := s.now()
	s.injMu.Lock()
	defer s.injMu.Unlock()
	if entry, ok := s.injectors[task]; ok {
		entry.lastUsed = now
		return entry.inj
	}
	if len(s.injectors) >= maxInjectors {
		var oldestTask string
		var oldestAt time.Time
		for k, e := range s.injectors {
			if oldestTask == "" || e.lastUsed.Before(oldestAt) {
				oldestTask = k
				oldestAt = e.lastUsed
			}
		}
		if oldestTask != "" {
			delete(s.injectors, oldestTask)
		}
	}
	inj, _ := inject.NewInjector(inject.Config{ // fields validated in New
		DaemonURL:      s.cfg.RouterURL,
		BearerToken:    s.cfg.BearerToken,
		AssertedCaller: s.cfg.AssertedCaller,
		HTTPClient:     s.routedClient(task),
	})
	s.injectors[task] = &injectorEntry{inj: inj, lastUsed: now}
	return inj
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

// taskFor names the task that carries key under the sink's scope.
//
// AX task names are DNS labels: at most 63 characters of [a-z0-9-]. Names
// derived from a cluster keep a readable slug of it, followed by a hash of
// the exact name so two clusters that slug alike still differ.
//
//   - cluster scope, any kind:     lookout-<cluster>-<hash8>
//   - incident scope, watchboard:  lookout-wb-<cluster>-<hash8>
//   - incident scope, incident or storm: lookout-<hash16 of cluster, kind, id>
func (s *Sink) taskFor(key inject.IncidentKey) string {
	if s.cfg.Scope == ScopeCluster {
		return clusterTaskName("lookout-", key.Cluster)
	}
	if key.Kind == inject.IncidentKeyWatchboard {
		return clusterTaskName("lookout-wb-", key.Cluster)
	}
	sum := sha256.Sum256([]byte(key.Cluster + "\x00" + string(key.Kind) + "\x00" + key.ID))
	return "lookout-" + hex.EncodeToString(sum[:8])
}

func clusterTaskName(prefix, cluster string) string {
	sum := sha256.Sum256([]byte(cluster))
	suffix := "-" + hex.EncodeToString(sum[:4])
	var b strings.Builder
	for _, r := range strings.ToLower(cluster) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	slug := strings.Trim(b.String(), "-")
	if limit := 63 - len(prefix) - len(suffix); len(slug) > limit {
		slug = strings.TrimRight(slug[:limit], "-")
	}
	if slug == "" {
		slug = "cluster"
	}
	return prefix + slug + suffix
}

// keyFromPayload rebuilds an incident key for an OpenIncident without
// one. Never the payload's fingerprint: that is the incident class.
func keyFromPayload(payload any) inject.IncidentKey {
	b, _ := json.Marshal(payload)
	var f struct {
		Cluster           string `json:"cluster"`
		UID               string `json:"uid"`
		Reason            string `json:"reason"`
		ReasonClass       string `json:"reason_class"`
		AncestorKind      string `json:"ancestor_kind"`
		AncestorNamespace string `json:"ancestor_namespace"`
		AncestorName      string `json:"ancestor_name"`
	}
	_ = json.Unmarshal(b, &f)
	switch {
	case f.UID != "":
		reason := f.ReasonClass
		if reason == "" {
			reason = f.Reason
		}
		return inject.IncidentKey{Cluster: f.Cluster, Kind: inject.IncidentKeyIncident, ID: f.UID + "/" + reason}
	case f.AncestorKind != "":
		return inject.IncidentKey{Cluster: f.Cluster, Kind: inject.IncidentKeyStorm, ID: f.AncestorKind + "/" + f.AncestorNamespace + "/" + f.AncestorName}
	default:
		return inject.IncidentKey{Cluster: f.Cluster, Kind: inject.IncidentKeyWatchboard}
	}
}
