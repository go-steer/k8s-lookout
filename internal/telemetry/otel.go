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

// Package telemetry initializes OpenTelemetry for the sentinel.
//
// Tracing is off by default — no exporter is configured — so a fresh
// invocation makes zero outbound network calls. Operators opt in with
// `--otel-exporter`:
//
//   - "console" — writes spans to stdout; useful for local debug
//   - "otlp"    — honors the standard OTEL env vars
//     (OTEL_EXPORTER_OTLP_ENDPOINT, etc.) to ship to a collector
//   - "none"    — the default; no spans leave
//
// The OpenTelemetry-standard env var `OTEL_TRACES_EXPORTER` overrides
// the flag when set (matches the OTel spec's env-var-wins convention).
// This is the load-bearing knob for fleet deployments where the base
// ConfigMap/Deployment is shared but each Pod's exporter target
// differs — operators wire it via a per-Deployment env-var patch
// instead of duplicating the manifest.
//
// This package was ported out of core-agent's pkg/telemetry (which
// wrapped ADK's telemetry.New) so the sentinel owns its own OTel
// bootstrap: it is ~100 lines of standard OTel SDK wiring, and going
// direct dropped both the core-agent module and google.golang.org/adk
// — an agent framework none of whose agent, model, session, or tool
// machinery this binary uses — from the dependency graph.
package telemetry

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/go-logr/stdr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"

	"github.com/go-steer/k8s-lookout/internal/version"
)

// Mode names recognized by Setup — the `--otel-exporter` values.
const (
	ModeNone    = "none"    // default; no spans exported
	ModeConsole = "console" // stdout exporter; for local dev
	ModeOTLP    = "otlp"    // honors OTEL_EXPORTER_OTLP_ENDPOINT etc.
)

// TracesExporterEnvVar names the OTel-standard env var that overrides
// the flag-supplied exporter mode. Same shape as the flag: "none",
// "console", or "otlp". Unknown values fall through to the mode
// switch and produce the same error an unknown flag value does, so an
// operator typo in a manifest is never silently ignored.
const TracesExporterEnvVar = "OTEL_TRACES_EXPORTER"

// serviceName is the service.name resource attribute stamped on every
// span, unless the operator named the service themselves via
// OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES.
const serviceName = "lookout"

// unknownServicePrefix is what the OTel SDK's default resource uses
// for service.name when nobody set one ("unknown_service:<argv0>").
// Seeing it is how we know the operator did not name the service and
// we may stamp our own.
const unknownServicePrefix = "unknown_service"

// Setup configures OpenTelemetry tracing. Returns a shutdown function
// the caller MUST call (typically deferred) so buffered spans are
// flushed on the way out.
//
// When mode is "" or "none", no provider is constructed and shutdown
// is a no-op — call sites stay clean either way. The W3C propagator is
// registered regardless of mode (see below).
func Setup(ctx context.Context, mode string) (shutdown func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }

	// Env-var override wins over the flag. Empty leaves the flag value
	// intact (env-unset ≠ "select none"), matching the OTel SDK spec
	// convention where env vars override in-process defaults.
	if envMode := strings.TrimSpace(os.Getenv(TracesExporterEnvVar)); envMode != "" {
		mode = envMode
	}

	// Route OTel SDK internal diag messages + span-export errors to
	// stderr so exporter failures (unreachable collector, TLS mismatch,
	// wrong port, wrong protocol) surface loudly instead of silently
	// dropping spans. The SDK's default handlers are noop; without
	// these two hooks "no spans in the backend" is indistinguishable
	// from "backend rejecting them silently". Verbosity gates via
	// OTEL_LOG_LEVEL — 0=fatal, 1=error (default), higher = more.
	logLevel := 1
	if lvl := os.Getenv("OTEL_LOG_LEVEL"); lvl == "debug" {
		logLevel = 8
	}
	otel.SetLogger(stdr.New(log.New(os.Stderr, "otel-diag ", log.LstdFlags)).V(logLevel))
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		fmt.Fprintf(os.Stderr, "lookout: otel-export: %v\n", err)
	}))

	// Register the W3C TextMapPropagator globally REGARDLESS of the
	// exporter mode. Even with no exporter, code that starts spans
	// against the noop tracer still produces contexts; the
	// otelhttp-wrapped sink needs a propagator to inject traceparent
	// headers so distributed-trace continuity with the daemon works the
	// moment an operator flips the exporter to otlp. A composite of
	// TraceContext (traceparent) + Baggage is the W3C shape every
	// OTel-instrumented downstream expects. See #217.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	switch mode {
	case "", ModeNone:
		return noop, nil
	case ModeConsole, ModeOTLP:
		// fall through
	default:
		return noop, fmt.Errorf("telemetry: unknown mode %q (want console/otlp/none)", mode)
	}

	res, err := newResource()
	if err != nil {
		return noop, err
	}

	var exporter sdktrace.SpanExporter
	switch mode {
	case ModeConsole:
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return noop, fmt.Errorf("telemetry: console exporter: %w", err)
		}
	case ModeOTLP:
		// OTLP over HTTP. The exporter reads the standard env vars
		// itself (OTEL_EXPORTER_OTLP_TRACES_ENDPOINT >
		// OTEL_EXPORTER_OTLP_ENDPOINT > the spec default
		// http://localhost:4318, plus headers/timeout/compression). Log
		// the resolved target so operators can grep boot logs to confirm
		// where spans are going — an unreachable collector then shows up
		// as otel-export lines from the error handler above, not silence.
		exporter, err = otlptracehttp.New(ctx)
		if err != nil {
			return noop, fmt.Errorf("telemetry: otlp exporter: %w", err)
		}
		fmt.Fprintf(os.Stderr, "lookout: telemetry: OTLP HTTP exporter → %s\n", otlpEndpoint())
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// newResource builds the resource stamped on every span: the SDK
// default (which honors OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES)
// plus this binary's identity and, when the environment names one, the
// GCP project.
func newResource() (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceVersionKey.String(version.Semver()),
	}

	// gcp.project_id is required by Cloud Trace's OTLP ingress — it
	// rejects whole batches that lack it. GOOGLE_CLOUD_PROJECT is the
	// env var Google client libraries already read, so an operator who
	// set it once gets the attribute for free. Unset → not stamped:
	// an empty attribute would satisfy no backend and only add noise.
	if project := strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_PROJECT")); project != "" {
		attrs = append(attrs, attribute.String("gcp.project_id", project))
	}

	// Our attributes are merged OVER resource.Default(), so guard
	// service.name: stamp "lookout" only when the operator has not
	// named the service themselves (via OTEL_SERVICE_NAME or
	// OTEL_RESOURCE_ATTRIBUTES) — detectable because the SDK default
	// is the "unknown_service:<argv0>" placeholder.
	base := resource.Default()
	if isUnnamedService(base) {
		attrs = append(attrs, semconv.ServiceNameKey.String(serviceName))
	}

	// NewSchemaless, not resource.New: an explicit schema URL would
	// conflict with whatever the SDK default carries and Merge would
	// fail. Attribute keys are the same either way.
	res, err := resource.Merge(base, resource.NewSchemaless(attrs...))
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}
	return res, nil
}

// isUnnamedService reports whether r's service.name is still the SDK's
// "unknown_service:<argv0>" placeholder — i.e. nobody named it.
func isUnnamedService(r *resource.Resource) bool {
	for _, kv := range r.Attributes() {
		if kv.Key == semconv.ServiceNameKey {
			return strings.HasPrefix(kv.Value.AsString(), unknownServicePrefix)
		}
	}
	return true
}

// otlpEndpoint renders the OTLP target for the startup log, resolved
// the way the exporter itself resolves it.
func otlpEndpoint() string {
	if v := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); v != "" {
		return v
	}
	return "http://localhost:4318 (spec default)"
}
