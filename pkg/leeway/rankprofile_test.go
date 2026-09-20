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

package leeway

import (
	"strings"
	"testing"
)

// gkeExtractor is the shipped table, which is what almost every case here
// wants — the point of the table is that the defaults are the measured ones.
func gkeExtractor(t *testing.T) *ProfileExtractor {
	t.Helper()
	e, err := NewProfileExtractor(DefaultNodeProfileConfig())
	if err != nil {
		t.Fatalf("the default table does not compile: %v", err)
	}
	return e
}

// liveNodeLabels is the label set read off a real GKE node in §7.7.2, plus the
// reservation and accelerator keys spike S3 measured on a second one.
func liveNodeLabels() map[string]string {
	return map[string]string{
		"node.kubernetes.io/instance-type":      "n4-highmem-2",
		"cloud.google.com/machine-family":       "n4",
		"cloud.google.com/gke-provisioning":     "standard",
		"cloud.google.com/compute-class":        "n4-preferred",
		"topology.kubernetes.io/zone":           "us-central1-f",
		"cloud.google.com/reservation-name":     "res-a",
		"cloud.google.com/reservation-project":  "proj-b",
		"cloud.google.com/reservation-affinity": "SPECIFIC",
		"cloud.google.com/gke-accelerator":      "nvidia-tesla-t4",
	}
}

func TestProfileExtractor_ALiveGKENode(t *testing.T) {
	p := gkeExtractor(t).Extract(liveNodeLabels(), 2, 16)

	if p.InstanceType != "n4-highmem-2" || p.MachineFamily != "n4" {
		t.Errorf("instance type %q family %q, want n4-highmem-2 / n4", p.InstanceType, p.MachineFamily)
	}
	if p.Spot == nil || *p.Spot {
		t.Errorf("Spot = %v, want a definite false — gke-provisioning is standard", p.Spot)
	}
	want := ReservationRef{Name: "res-a", Project: "proj-b", Affinity: "specific"}
	if p.Reservation != want {
		t.Errorf("Reservation = %+v, want %+v — affinity is lowercased", p.Reservation, want)
	}
	if p.Accelerator != "nvidia-tesla-t4" {
		t.Errorf("Accelerator = %q", p.Accelerator)
	}
	if p.Cores != 2 || p.MemoryGB != 16 {
		t.Errorf("cores/memory = %d/%v, want 2/16 — these come from capacity, not a label", p.Cores, p.MemoryGB)
	}
	if p.Labels["topology.kubernetes.io/zone"] != "us-central1-f" {
		t.Error("raw labels were not carried through for user-defined matchers")
	}
}

func TestProfileExtractor_MachineFamilyFallsBackToTheInstanceType(t *testing.T) {
	// The family label is GKE's; a node pool created outside NAP may not
	// carry it, and the instance type always encodes the family.
	labels := map[string]string{"node.kubernetes.io/instance-type": "n2-standard-8"}
	if got := gkeExtractor(t).Extract(labels, 8, 32).MachineFamily; got != "n2" {
		t.Errorf("MachineFamily = %q, want n2 derived from the instance type", got)
	}
}

func TestProfileExtractor_TheFamilyLabelWinsOverThePattern(t *testing.T) {
	// The label is what the provider says; the pattern is our inference from
	// a naming convention, so it is the fallback and not the other way round.
	labels := map[string]string{
		"node.kubernetes.io/instance-type": "n2-standard-8",
		"cloud.google.com/machine-family":  "custom",
	}
	if got := gkeExtractor(t).Extract(labels, 8, 32).MachineFamily; got != "custom" {
		t.Errorf("MachineFamily = %q, want the label's value", got)
	}
}

func TestProfileExtractor_AnUnparseableInstanceTypeLeavesTheFamilyEmpty(t *testing.T) {
	labels := map[string]string{"node.kubernetes.io/instance-type": "WeirdBox"}
	if got := gkeExtractor(t).Extract(labels, 1, 1).MachineFamily; got != "" {
		t.Errorf("MachineFamily = %q, want empty — an unmatched pattern must not invent one", got)
	}
}

// TestProfileExtractor_SpotIsTriState is the reason NodeProfile.Spot is a
// pointer. A node we cannot classify must not be classified as on-demand, or
// a rule saying spot: false matches it and attributes it to the wrong rule.
func TestProfileExtractor_SpotIsTriState(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   *bool
	}{
		{"no provisioning labels at all", map[string]string{}, nil},
		{"explicitly standard", map[string]string{"cloud.google.com/gke-provisioning": "standard"}, boolPtr(false)},
		{"provisioning says spot", map[string]string{"cloud.google.com/gke-provisioning": "spot"}, boolPtr(true)},
		{"the older spot label", map[string]string{"cloud.google.com/gke-spot": "true"}, boolPtr(true)},
		{"the older label says false", map[string]string{"cloud.google.com/gke-spot": "false"}, boolPtr(false)},
		{"either rule is enough", map[string]string{
			"cloud.google.com/gke-provisioning": "standard",
			"cloud.google.com/gke-spot":         "true",
		}, boolPtr(true)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := gkeExtractor(t).Extract(tc.labels, 1, 1).Spot
			switch {
			case tc.want == nil && got != nil:
				t.Errorf("Spot = %v, want unknown", *got)
			case tc.want != nil && got == nil:
				t.Errorf("Spot = unknown, want %v", *tc.want)
			case tc.want != nil && *got != *tc.want:
				t.Errorf("Spot = %v, want %v", *got, *tc.want)
			}
		})
	}
}

func TestProfileExtractor_NoReservationIsEmptyNotPartial(t *testing.T) {
	p := gkeExtractor(t).Extract(map[string]string{}, 1, 1)
	if !p.Reservation.Empty() {
		t.Errorf("Reservation = %+v, want empty", p.Reservation)
	}
	// And a node with only an affinity — which GKE stamps without a name
	// when nothing was consumed — still counts as having consumed nothing.
	p = gkeExtractor(t).Extract(map[string]string{"cloud.google.com/reservation-affinity": "any"}, 1, 1)
	if !p.Reservation.Empty() {
		t.Errorf("Reservation = %+v, want empty — affinity alone is not a reservation", p.Reservation)
	}
}

func TestProfileExtractor_AnUnconfiguredPodFamilyDoesNotReadTheEmptyKey(t *testing.T) {
	// No node label carrying podFamily has been observed, so the default
	// table leaves it unset. A bare labels[""] lookup would then return
	// whatever a node happened to have under the empty key.
	labels := map[string]string{"": "surprise"}
	if got := gkeExtractor(t).Extract(labels, 1, 1).PodFamily; got != "" {
		t.Errorf("PodFamily = %q, want empty when no key is configured", got)
	}

	cfg := DefaultNodeProfileConfig()
	cfg.PodFamilyLabel = "example.com/pod-family"
	e, err := NewProfileExtractor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := e.Extract(map[string]string{"example.com/pod-family": "general-purpose"}, 1, 1).PodFamily
	if got != "general-purpose" {
		t.Errorf("PodFamily = %q, want the configured label's value", got)
	}
}

func TestNewProfileExtractor_RejectsABadPattern(t *testing.T) {
	cfg := DefaultNodeProfileConfig()
	cfg.MachineFamilyPattern = `^([a-z`
	if _, err := NewProfileExtractor(cfg); err == nil {
		t.Fatal("an uncompilable pattern was accepted")
	} else if !strings.Contains(err.Error(), "machine family pattern") {
		t.Errorf("error = %v, want it to name the setting", err)
	}
}

func TestNewProfileExtractor_RejectsAPatternWithNothingToCapture(t *testing.T) {
	// It would compile, match, and then have no group 1 to read — a panic
	// on the first node rather than a config error at startup.
	cfg := DefaultNodeProfileConfig()
	cfg.MachineFamilyPattern = `^[a-z0-9]+-`
	if _, err := NewProfileExtractor(cfg); err == nil {
		t.Fatal("a pattern with no capture group was accepted")
	}
}

func TestNewProfileExtractor_NoPatternIsFine(t *testing.T) {
	cfg := DefaultNodeProfileConfig()
	cfg.MachineFamilyPattern = ""
	e, err := NewProfileExtractor(cfg)
	if err != nil {
		t.Fatalf("an empty pattern was rejected: %v", err)
	}
	labels := map[string]string{"node.kubernetes.io/instance-type": "n2-standard-8"}
	if got := e.Extract(labels, 8, 32).MachineFamily; got != "" {
		t.Errorf("MachineFamily = %q, want empty with derivation switched off", got)
	}
}
