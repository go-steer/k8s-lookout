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
	"fmt"
	"regexp"
	"strings"
)

// NodeProfile is the normalised attribute set preference rules are matched
// against — one node's placement-relevant facts, with the provider's label
// spellings already resolved away.
//
// The pointer fields are the ones where absence is not a value. A node with no
// provisioning label is not thereby a standard node; it is a node we cannot
// say. That distinction is what keeps the matcher from failing open, so it is
// in the type rather than in a convention.
type NodeProfile struct {
	InstanceType  string // "n2-standard-8"
	MachineFamily string // "n2"
	PodFamily     string // GKE's Autopilot classes constrain on this
	Spot          *bool  // nil when no provisioning label was found
	Reservation   ReservationRef
	Accelerator   string // GPU type verbatim, e.g. "nvidia-tesla-t4"
	Cores         int64
	MemoryGB      float64
	Labels        map[string]string // raw, for user-defined matchers
}

// ReservationRef identifies a consumed GCE reservation.
//
// Three fields, not one. A reservation name is unique only within its project
// and GKE can consume a reservation shared from another project (spike S3
// measured the project label on a live node), so identity is the (project,
// name) pair and the name alone is not an identity.
type ReservationRef struct {
	Name     string
	Project  string
	Affinity string // "specific" | "any" | "" — carried, never matched on
}

// Empty reports whether this node consumed no reservation at all.
func (r ReservationRef) Empty() bool { return r.Name == "" && r.Project == "" }

// SpotRule is one provider-specific way of saying "this node is preemptible".
//
// Exactly one of Equals and NotEquals is set. NotEquals requires the label to
// be present and to differ, which is not the same as the label being absent —
// GKE's spelling is gke-provisioning != standard, and a node missing the label
// entirely tells us nothing rather than telling us "spot".
type SpotRule struct {
	Label     string
	Equals    string
	NotEquals string
}

// NodeProfileConfig is §7.7.2's extractor table: which label carries which
// fact. It is configuration rather than code so that a provider changing a
// label key, or a second provider appearing, does not need a release.
type NodeProfileConfig struct {
	InstanceTypeLabel  string
	MachineFamilyLabel string
	// MachineFamilyPattern derives the family from the instance type when the
	// family label is absent. The first capture group is the family.
	MachineFamilyPattern     string
	PodFamilyLabel           string
	SpotRules                []SpotRule
	ReservationNameLabel     string
	ReservationProjectLabel  string
	ReservationAffinityLabel string
	AcceleratorLabel         string
}

// DefaultNodeProfileConfig is the GKE table from §7.7.2. Every key in it was
// read off a live node on std-simian-test rather than off a documentation
// page; the spikes that did so are named in the design.
func DefaultNodeProfileConfig() NodeProfileConfig {
	return NodeProfileConfig{
		InstanceTypeLabel:    "node.kubernetes.io/instance-type",
		MachineFamilyLabel:   "cloud.google.com/machine-family",
		MachineFamilyPattern: `^([a-z0-9]+)-`,
		PodFamilyLabel:       "",
		SpotRules: []SpotRule{
			{Label: "cloud.google.com/gke-provisioning", NotEquals: "standard"},
			{Label: "cloud.google.com/gke-spot", Equals: "true"},
		},
		ReservationNameLabel:     "cloud.google.com/reservation-name",
		ReservationProjectLabel:  "cloud.google.com/reservation-project",
		ReservationAffinityLabel: "cloud.google.com/reservation-affinity",
		AcceleratorLabel:         "cloud.google.com/gke-accelerator",
	}
}

// PodFamilyLabel is deliberately empty in the default table: no node label
// carrying it has been observed, and guessing one would make every rule that
// constrains podFamily silently unmatchable instead of visibly unsupported.
// A deployment that knows the key can set it.

// ProfileExtractor turns a node's labels and capacity into a NodeProfile.
//
// It is built once per configuration rather than per node because the family
// pattern has to be compiled, and a regexp compiled per node on a large
// cluster is the kind of cost that only shows up in a flame graph.
type ProfileExtractor struct {
	cfg    NodeProfileConfig
	family *regexp.Regexp
}

// NewProfileExtractor compiles an extractor table, and reports a bad family
// pattern rather than silently declining to derive families from it.
func NewProfileExtractor(cfg NodeProfileConfig) (*ProfileExtractor, error) {
	e := &ProfileExtractor{cfg: cfg}
	if cfg.MachineFamilyPattern != "" {
		re, err := regexp.Compile(cfg.MachineFamilyPattern)
		if err != nil {
			return nil, fmt.Errorf("machine family pattern %q: %w", cfg.MachineFamilyPattern, err)
		}
		if re.NumSubexp() < 1 {
			return nil, fmt.Errorf("machine family pattern %q has no capture group, so there is nothing to extract", cfg.MachineFamilyPattern)
		}
		e.family = re
	}
	return e, nil
}

// Extract reads one node's profile. Cores and memory come from the node's
// capacity rather than from a label, because minCores and minMemoryGb are
// rules about the machine and not about how it was labelled.
func (e *ProfileExtractor) Extract(labels map[string]string, cores int64, memoryGB float64) NodeProfile {
	p := NodeProfile{
		InstanceType:  labels[e.cfg.InstanceTypeLabel],
		MachineFamily: labels[e.cfg.MachineFamilyLabel],
		PodFamily:     valueIfKeySet(labels, e.cfg.PodFamilyLabel),
		Accelerator:   labels[e.cfg.AcceleratorLabel],
		Cores:         cores,
		MemoryGB:      memoryGB,
		Labels:        labels,
		Reservation: ReservationRef{
			Name:     labels[e.cfg.ReservationNameLabel],
			Project:  labels[e.cfg.ReservationProjectLabel],
			Affinity: strings.ToLower(labels[e.cfg.ReservationAffinityLabel]),
		},
	}
	if p.MachineFamily == "" && e.family != nil {
		if m := e.family.FindStringSubmatch(p.InstanceType); m != nil {
			p.MachineFamily = m[1]
		}
	}
	p.Spot = e.spot(labels)
	return p
}

// valueIfKeySet avoids the empty-key lookup, which would return whatever a
// node happened to have under "" — nothing today, but a silent dependency on
// that staying true.
func valueIfKeySet(labels map[string]string, key string) string {
	if key == "" {
		return ""
	}
	return labels[key]
}

// spot evaluates the anyOf list, and returns nil when not one of its labels is
// present on the node.
//
// Nil rather than false is the whole reason this returns a pointer. A node we
// cannot classify must not be classified as on-demand, because a rule saying
// spot: false would then match it and attribute the node to the wrong rule.
func (e *ProfileExtractor) spot(labels map[string]string) *bool {
	var known bool
	for _, r := range e.cfg.SpotRules {
		v, ok := labels[r.Label]
		if !ok {
			continue
		}
		known = true
		switch {
		case r.Equals != "" && v == r.Equals:
			return boolPtr(true)
		case r.NotEquals != "" && v != r.NotEquals:
			return boolPtr(true)
		}
	}
	if !known {
		return nil
	}
	return boolPtr(false)
}

func boolPtr(b bool) *bool { return &b }
