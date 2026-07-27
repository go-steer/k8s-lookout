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

package inject

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Sink is the agent-sink abstraction every sentinel inject routes
// through (docs/agent-sink-design.md): two verbs, nothing else. A
// payload is one of the frozen signal-schema v1 wire structs in this
// package (Payload, ResolvedPayload, StormPayload, StormMemberPayload,
// StormUpdatePayload, TriageRegressedPayload, WatchboardDigestPayload,
// WatchboardRotatedPayload) — the sink serializes it; it never
// composes or mutates it.
//
// Implementations:
//
//   - *Injector (the default, --sink=core-agent): POST /sessions opens
//     a per-incident session, POST /sessions/<sid>/inject delivers each
//     payload inside the daemon's {"message": "<payload JSON>"}
//     envelope — byte-identical to the pre-Sink client.
//   - *WebhookSink (--sink=webhook): POST <url>/incidents opens an
//     incident with the payload JSON as the request body (no
//     envelope), POST <url>/incidents/<id>/events appends follow-ups.
type Sink interface {
	// OpenIncident opens a new incident container at the receiver and
	// delivers payload as its initial record, returning the receiver's
	// opaque incident id. Implementations whose open is a separate wire
	// call from the initial delivery (the core-agent sink) MAY return a
	// non-empty id together with a non-nil error when the container was
	// opened but the initial delivery failed — callers should bind to
	// the id when it is non-empty and count the error separately.
	OpenIncident(ctx context.Context, payload any) (id string, err error)
	// Append delivers one payload into the existing incident id (a
	// followup, outcome record, digest, or lineage pointer).
	Append(ctx context.Context, id string, payload any) error
}

// SessionOpener is the optional capability the core-agent sink carries
// beyond Sink: opening an EMPTY incident container ahead of its first
// payload (POST /sessions exists independently of the first inject).
// The watchboard's size-based rotation depends on it to keep the
// frozen §15 Q2 wire order — successor id known first, the
// kind=watchboard.rotated lineage pointer on the wire BEFORE the
// successor's opening digest. Stateless receivers (the webhook sink)
// deliberately do not implement it: there an incident exists only by
// receiving its first payload, so rotation appends the lineage pointer
// after the successor's opening digest instead.
type SessionOpener interface {
	CreateSession(ctx context.Context) (string, error)
}

// newSinkHTTPClient is the shared production transport for every
// sink: a modest timeout — every sink POST is cheap; if the receiver
// takes >10s to accept one, something's wrong — and an
// otelhttp-wrapped transport so outbound POSTs carry the current
// span's traceparent across the process boundary (see #217; the
// core-agent daemon wraps its attach mux with otelhttp.NewHandler and
// continues the trace). When telemetry is off (otel.exporter=none,
// the default), the propagator is a no-op and the wrapper adds
// negligible overhead. Retries are deliberately absent for every
// sink: one attempt, errors surface to the caller whose metrics and
// logs count them.
func newSinkHTTPClient() *http.Client {
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
}
