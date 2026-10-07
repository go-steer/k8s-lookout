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

// Package certmanager judges a cert-manager.io/v1 Certificate's
// issuance state from its status.
//
// One judgement, shared by every surface that reads Certificates: the
// `expiry` sentinel source (expiry.warning) and `lookout health`'s
// certs category. It is pure — an unstructured object in, facts out,
// no client and no clock of its own — so the watch path and the read
// path cannot drift on what "never issued", "renewal failed" or
// "still inside the first-issuance grace" mean. What each surface DOES
// with the facts (the sentinel's pager latch and 72h critical window,
// health's scorecard grading) stays with the surface.
//
// The object is read unstructured: a build-time dependency on the
// cert-manager module to read a handful of status fields would cost
// more than it saves (the dependency policy).
package certmanager

import (
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GV is the Certificate API's group-version.
	GV = schema.GroupVersion{Group: "cert-manager.io", Version: "v1"}
	// CertificateGVR locates the Certificate CRs.
	CertificateGVR = GV.WithResource("certificates")
)

// DefaultFirstIssuanceGrace is how long a never-issued Certificate may
// read Ready=False with an in-progress reason before that counts as a
// problem: every new Certificate reads Ready=False / DoesNotExist
// until its first issuance completes — seconds for a CA issuer,
// minutes for ACME. It is the expiry source's --expiry-acme-grace
// default, and the fixed grace `lookout health` applies.
const DefaultFirstIssuanceGrace = 15 * time.Minute

// firstIssuanceReasons are the Ready=False reasons cert-manager sets on
// a Certificate whose first issuance is still in progress — the only
// ones the first-issuance grace covers. Any other reason on a
// never-issued Certificate is judged at once.
var firstIssuanceReasons = map[string]bool{
	"DoesNotExist": true,
	"Issuing":      true,
}

// Status is one Certificate's issuance facts.
type Status struct {
	Namespace string
	Name      string
	UID       string
	// Created is the creationTimestamp (zero when unknown).
	Created time.Time
	// SecretName is spec.secretName: the TLS Secret this Certificate
	// writes, which a surface judging Secrets attributes to it.
	SecretName string
	// NotAfter is status.notAfter; zero means never issued.
	NotAfter time.Time
	// Ready is the Ready condition's status ("" when absent).
	Ready string
	// ReadyDetail is the Ready condition's "reason message".
	ReadyDetail string
	// LastFailure is status.lastFailureTime as written ("" = none).
	LastFailure string

	// RenewalFailed: the last issuance failed — a recorded
	// lastFailureTime, or Ready=False.
	RenewalFailed bool
	// NeverIssued: no status.notAfter, so no countdown exists.
	NeverIssued bool
	// FirstIssuing: never issued, no failure recorded, and Ready=False
	// with an in-progress reason — judged only past the first-issuance
	// grace.
	FirstIssuing bool
}

// Read extracts one Certificate CR's issuance facts.
func Read(u *unstructured.Unstructured) Status {
	s := Status{
		Namespace: u.GetNamespace(),
		Name:      u.GetName(),
		UID:       string(u.GetUID()),
		Created:   u.GetCreationTimestamp().Time,
	}
	s.SecretName, _, _ = unstructured.NestedString(u.Object, "spec", "secretName")

	notAfterStr, _, _ := unstructured.NestedString(u.Object, "status", "notAfter")
	if t, err := time.Parse(time.RFC3339, notAfterStr); err == nil {
		s.NotAfter = t
	}

	readyReason := ""
	issuingFailed := false
	if conds, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); found {
		for _, c := range conds {
			m, isMap := c.(map[string]any)
			if !isMap {
				continue
			}
			status, _ := m["status"].(string)
			reason, _ := m["reason"].(string)
			switch t, _ := m["type"].(string); t {
			case "Ready":
				s.Ready, readyReason = status, reason
				msg, _ := m["message"].(string)
				s.ReadyDetail = strings.TrimSpace(reason + " " + msg)
			case "Issuing":
				issuingFailed = status == "False" && reason == "Failed"
			}
		}
	}
	s.LastFailure, _, _ = unstructured.NestedString(u.Object, "status", "lastFailureTime")
	s.RenewalFailed = s.LastFailure != "" || s.Ready == "False"
	s.NeverIssued = s.NotAfter.IsZero()
	s.FirstIssuing = s.NeverIssued && s.LastFailure == "" && !issuingFailed &&
		s.Ready == "False" && firstIssuanceReasons[readyReason]
	return s
}

// Reportable reports whether there is anything to judge: false for a
// Certificate that has never issued and has not failed (Ready not
// False) — nothing to count down from, nothing to report.
func (s Status) Reportable() bool {
	return !s.NeverIssued || s.RenewalFailed
}

// InFirstIssuanceGrace reports whether a never-issued Certificate is
// still inside its first-issuance grace (grace from its
// creationTimestamp). stalled is the caller's proof the first
// issuance demonstrably failed — an ACME stall naming the Certificate
// — which ends the grace early, as a recorded failure does. An unknown
// creationTimestamp (zero) is never inside it: what cannot be timed
// is not suppressed.
func (s Status) InFirstIssuanceGrace(now time.Time, grace time.Duration, stalled bool) bool {
	return s.FirstIssuing && !stalled && !s.Created.IsZero() && now.Sub(s.Created) < grace
}

// NeverIssuedMessage is the shared wording for a Certificate that has
// never issued: "certificate never issued: <Ready reason message>".
func (s Status) NeverIssuedMessage() string {
	detail := s.ReadyDetail
	if detail == "" {
		detail = "no status.notAfter"
	}
	return "certificate never issued: " + detail
}
