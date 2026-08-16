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

package telemetry

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

// The Setup tests are deliberately NOT parallel: they mutate process
// env and the OTel global providers.

func TestSetup_None(t *testing.T) {
	shutdown, err := Setup(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("noop shutdown errored: %v", err)
	}
}

func TestSetup_UnknownMode(t *testing.T) {
	_, err := Setup(context.Background(), "smoke-signals")
	if err == nil || !strings.Contains(err.Error(), "unknown mode") {
		t.Fatalf("expected unknown-mode error, got %v", err)
	}
}

func TestSetup_Console(t *testing.T) {
	shutdown, err := Setup(context.Background(), ModeConsole)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })
}

// TestSetup_OTLP pins that `--otel-exporter=otlp` always builds a
// provider, endpoint env var or not: the OTel spec's localhost:4318
// default applies, and an unreachable collector surfaces through the
// error handler. The ADK-backed implementation this replaced silently
// built NO provider unless OTEL_EXPORTER_OTLP_ENDPOINT was set, which
// made "otlp" a no-op that still logged an endpoint.
func TestSetup_OTLP(t *testing.T) {
	before := otel.GetTracerProvider()
	shutdown, err := Setup(context.Background(), ModeOTLP)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	// A fresh SDK TracerProvider is installed globally, so spans
	// started anywhere in the process are recorded and batched.
	if otel.GetTracerProvider() == before {
		t.Errorf("otlp mode installed no TracerProvider (still %T)", before)
	}
}

// TestSetup_PropagatorRegisteredInNoneMode pins #217: the sentinel's
// otelhttp-wrapped sink must inject traceparent headers even with no
// exporter, so trace continuity works the moment an operator flips the
// exporter on.
func TestSetup_PropagatorRegisteredInNoneMode(t *testing.T) {
	if _, err := Setup(context.Background(), ModeNone); err != nil {
		t.Fatal(err)
	}
	// A synthetic span context, not a live tracer: none mode installs
	// no provider, and the assertion is about the propagator alone.
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		TraceFlags: trace.FlagsSampled,
	})
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(trace.ContextWithSpanContext(context.Background(), sc), carrier)
	if _, ok := carrier["traceparent"]; !ok {
		t.Errorf("no traceparent injected in none mode; carrier=%v", carrier)
	}
}

// TestSetup_EnvVarOverridesMode pins the OTel-standard override
// convention: when OTEL_TRACES_EXPORTER is set it wins over the
// --otel-exporter value. Load-bearing for fleet deployments where a
// shared manifest can't carry per-Pod exporter targets.
func TestSetup_EnvVarOverridesMode(t *testing.T) {
	// Flag says "none"; env says "console". Env wins — provable
	// because ModeNone would have short-circuited before installing a
	// TracerProvider.
	t.Setenv(TracesExporterEnvVar, ModeConsole)
	before := otel.GetTracerProvider()
	shutdown, err := Setup(context.Background(), ModeNone)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	if otel.GetTracerProvider() == before {
		t.Errorf("env override did not take effect: no TracerProvider installed")
	}
}

// TestSetup_EmptyEnvVarLeavesFlagMode pins that an unset (or
// explicitly empty) env var doesn't override a non-none flag value.
// Env-unset ≠ "select none".
func TestSetup_EmptyEnvVarLeavesFlagMode(t *testing.T) {
	t.Setenv(TracesExporterEnvVar, "") // explicit empty
	shutdown, err := Setup(context.Background(), ModeConsole)
	if err != nil {
		t.Fatalf("Setup: %v (flag mode should have applied when env is empty)", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })
}

// TestSetup_EnvVarInvalidValueSurfacesSameError pins the error
// surface: an invalid env-var value produces the same clear
// "unknown mode" error as an invalid flag value — no silent
// fallthrough that could mask an operator typo in a k8s manifest.
func TestSetup_EnvVarInvalidValueSurfacesSameError(t *testing.T) {
	t.Setenv(TracesExporterEnvVar, "smoke-signals")
	_, err := Setup(context.Background(), ModeNone)
	if err == nil || !strings.Contains(err.Error(), "unknown mode") {
		t.Fatalf("expected unknown-mode error for invalid env value, got %v", err)
	}
}

// TestNewResource_ServiceIdentity pins the span identity an operator
// reads in the backend: named "lookout", carrying this build's semver.
// Deliberately does NOT set OTEL_SERVICE_NAME — resource.Default() is
// computed once per process, so an env-var test here would be
// order-dependent; the override branch is covered by
// TestIsUnnamedService.
func TestNewResource_ServiceIdentity(t *testing.T) {
	res, err := newResource()
	if err != nil {
		t.Fatal(err)
	}
	attrs := attrMap(res)
	if got := attrs[string(semconv.ServiceNameKey)]; got != serviceName {
		t.Errorf("service.name = %q, want %q", got, serviceName)
	}
	if attrs[string(semconv.ServiceVersionKey)] == "" {
		t.Errorf("service.version is empty; attrs=%v", attrs)
	}
}

// TestNewResource_GCPProject pins the Cloud Trace requirement:
// gcp.project_id is stamped from GOOGLE_CLOUD_PROJECT when set (Cloud
// Trace's OTLP ingress rejects whole batches without it) and omitted
// entirely when unset — an empty value satisfies no backend.
func TestNewResource_GCPProject(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	res, err := newResource()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := attrMap(res)["gcp.project_id"]; ok {
		t.Errorf("gcp.project_id stamped with no GOOGLE_CLOUD_PROJECT set")
	}

	t.Setenv("GOOGLE_CLOUD_PROJECT", "my-proj")
	res, err = newResource()
	if err != nil {
		t.Fatal(err)
	}
	if got := attrMap(res)["gcp.project_id"]; got != "my-proj" {
		t.Errorf("gcp.project_id = %q, want %q", got, "my-proj")
	}
}

func TestIsUnnamedService(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		res  *resource.Resource
		want bool
	}{
		{"sdk placeholder", resource.NewSchemaless(semconv.ServiceNameKey.String("unknown_service:lookout")), true},
		{"operator named", resource.NewSchemaless(semconv.ServiceNameKey.String("sentinel-prod")), false},
		{"no service.name at all", resource.NewSchemaless(attribute.String("host.name", "n1")), true},
	} {
		if got := isUnnamedService(tc.res); got != tc.want {
			t.Errorf("%s: isUnnamedService = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestOTLPEndpoint pins the startup log's resolution order — it must
// match what the exporter itself resolves, or the line lies about
// where spans went.
func TestOTLPEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	if got := otlpEndpoint(); !strings.Contains(got, "localhost:4318") {
		t.Errorf("unset endpoints: got %q, want the spec default", got)
	}

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
	if got := otlpEndpoint(); got != "http://collector:4318" {
		t.Errorf("generic endpoint: got %q", got)
	}

	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4318/v1/traces")
	if got := otlpEndpoint(); got != "http://traces:4318/v1/traces" {
		t.Errorf("traces endpoint must win over the generic one: got %q", got)
	}
}

// attrMap flattens a resource's attributes for assertions.
func attrMap(r *resource.Resource) map[string]string {
	m := map[string]string{}
	for _, kv := range r.Attributes() {
		m[string(kv.Key)] = kv.Value.AsString()
	}
	return m
}
