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

package cloud

import (
	"context"
	"strings"
	"testing"
)

func TestNoProviderCapabilitiesAllUnavailable(t *testing.T) {
	if got := NoProvider.Name(); got != NoProviderName {
		t.Errorf("NoProvider.Name() = %q, want %q", got, NoProviderName)
	}
	statuses := NoProvider.Capabilities()
	if len(statuses) != len(AllCapabilities()) {
		t.Fatalf("NoProvider reports %d capabilities, want %d", len(statuses), len(AllCapabilities()))
	}
	for _, status := range statuses {
		if status.Available {
			t.Errorf("NoProvider capability %s reported available", status.Capability)
		}
		if status.Reason != NoProviderReason {
			t.Errorf("NoProvider capability %s reason = %q, want %q", status.Capability, status.Reason, NoProviderReason)
		}
	}
}

func TestNoProviderGettersUnavailable(t *testing.T) {
	p := NoProvider
	checks := []struct {
		capability Capability
		ok         bool
	}{
		{CapabilityMetrics, second(p.Metrics())},
		{CapabilityCapacity, second(p.Capacity())},
		{CapabilityQuota, second(p.Quota())},
		{CapabilityOrphans, second(p.Orphans())},
		{CapabilityIPSpace, second(p.IPSpace())},
		{CapabilityStockout, second(p.Stockouts())},
		{CapabilityWorkloadIdentity, second(p.WorkloadIdentity())},
	}
	for _, c := range checks {
		if c.ok {
			t.Errorf("NoProvider getter for %s returned available=true", c.capability)
		}
	}
}

// second collapses a (impl, ok) getter result to ok, so the table
// above stays one line per capability regardless of the impl type.
func second[T any](_ T, ok bool) bool { return ok }

func TestUnavailableMarker(t *testing.T) {
	u := Unavailable(NoProvider, CapabilityQuota)
	if u.Reason != NoProviderReason {
		t.Errorf("Unavailable reason = %q, want %q", u.Reason, NoProviderReason)
	}
	if u.Provider != NoProviderName || u.Capability != CapabilityQuota {
		t.Errorf("Unavailable identity = %q/%q, want %q/%q", u.Provider, u.Capability, NoProviderName, CapabilityQuota)
	}
	// §2 prescribes this exact marker shape for summary lines.
	want := `unavailable reason="no cloud provider configured"`
	if got := u.Marker(); got != want {
		t.Errorf("Marker() = %s, want %s", got, want)
	}
}

func TestNewExplicitUnknownProviderErrors(t *testing.T) {
	_, err := New(context.Background(), Config{Provider: "atlantis"})
	if err == nil {
		t.Fatal("New with unknown provider name: want error, got nil")
	}
	if !strings.Contains(err.Error(), "atlantis") {
		t.Errorf("error %q does not name the unknown provider", err)
	}
}

func TestNewExplicitNoneForcesSentinel(t *testing.T) {
	p, err := New(context.Background(), Config{Provider: NoProviderName})
	if err != nil {
		t.Fatalf("New(none) error: %v", err)
	}
	if p != NoProvider {
		t.Errorf("New(none) = %v, want the NoProvider sentinel", p)
	}
}

func TestNewSelectionFromEnv(t *testing.T) {
	t.Setenv(ProviderEnv, NoProviderName)
	p, err := New(context.Background(), Config{})
	if err != nil {
		t.Fatalf("New with %s=%s error: %v", ProviderEnv, NoProviderName, err)
	}
	if p.Name() != NoProviderName {
		t.Errorf("provider = %q, want %q", p.Name(), NoProviderName)
	}
}

func TestRegisterRejectsReservedAndDuplicate(t *testing.T) {
	mustPanic(t, "reserved name", func() { Register(NoProviderName, nil) })
	mustPanic(t, "empty name", func() { Register("", nil) })
	mustPanic(t, "nil factory", func() { Register("niltest", nil) })

	Register("duptest", func(context.Context, Config) (Provider, error) { return NoProvider, nil })
	t.Cleanup(func() {
		registryMu.Lock()
		delete(registry, "duptest")
		registryMu.Unlock()
	})
	mustPanic(t, "duplicate name", func() {
		Register("duptest", func(context.Context, Config) (Provider, error) { return NoProvider, nil })
	})
}

func mustPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: Register did not panic", name)
		}
	}()
	f()
}
