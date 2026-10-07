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

package certmanager

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var now = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

func cert(created time.Time, status map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "prod", "name": "api", "uid": "u1"},
		"spec":     map[string]any{"secretName": "api-tls"},
		"status":   status,
	}}
	u.SetCreationTimestamp(metav1.Time{Time: created})
	return u
}

func ready(status, reason, message string) map[string]any {
	return map[string]any{"type": "Ready", "status": status, "reason": reason, "message": message}
}

func TestRead(t *testing.T) {
	young, old := now.Add(-5*time.Minute), now.Add(-time.Hour)
	for _, tc := range []struct {
		name                                  string
		u                                     *unstructured.Unstructured
		reportable, neverIssued, renewal, grc bool
		message                               string
	}{
		{"first issuance inside grace", cert(young, map[string]any{"conditions": []any{ready("False", "DoesNotExist", "no secret")}}),
			true, true, true, true, "certificate never issued: DoesNotExist no secret"},
		{"first issuance past grace", cert(old, map[string]any{"conditions": []any{ready("False", "Issuing", "")}}),
			true, true, true, false, "certificate never issued: Issuing"},
		{"recorded failure ends grace", cert(young, map[string]any{"lastFailureTime": "2026-06-30T23:59:00Z", "conditions": []any{ready("False", "DoesNotExist", "")}}),
			true, true, true, false, "certificate never issued: DoesNotExist"},
		{"Issuing=False/Failed ends grace", cert(young, map[string]any{"conditions": []any{ready("False", "DoesNotExist", ""),
			map[string]any{"type": "Issuing", "status": "False", "reason": "Failed"}}}),
			true, true, true, false, "certificate never issued: DoesNotExist"},
		{"other reason judged at once", cert(young, map[string]any{"conditions": []any{ready("False", "InvalidKeyPair", "")}}),
			true, true, true, false, "certificate never issued: InvalidKeyPair"},
		{"unknown creation is never in grace", cert(time.Time{}, map[string]any{"conditions": []any{ready("False", "DoesNotExist", "")}}),
			true, true, true, false, "certificate never issued: DoesNotExist"},
		{"no Ready yet is not reportable", cert(young, map[string]any{}),
			false, true, false, false, "certificate never issued: no status.notAfter"},
		{"issued and ready", cert(old, map[string]any{"notAfter": "2026-09-01T00:00:00Z", "conditions": []any{ready("True", "Ready", "")}}),
			true, false, false, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := Read(tc.u)
			if s.Reportable() != tc.reportable || s.NeverIssued != tc.neverIssued || s.RenewalFailed != tc.renewal {
				t.Errorf("reportable=%t never=%t renewal=%t; want %t %t %t", s.Reportable(), s.NeverIssued, s.RenewalFailed, tc.reportable, tc.neverIssued, tc.renewal)
			}
			if got := s.InFirstIssuanceGrace(now, DefaultFirstIssuanceGrace, false); got != tc.grc {
				t.Errorf("in grace = %t; want %t", got, tc.grc)
			}
			if tc.grc && s.InFirstIssuanceGrace(now, DefaultFirstIssuanceGrace, true) {
				t.Error("a stalled Certificate is still in grace")
			}
			if tc.message != "" && s.NeverIssuedMessage() != tc.message {
				t.Errorf("message = %q; want %q", s.NeverIssuedMessage(), tc.message)
			}
			if s.SecretName != "api-tls" {
				t.Errorf("secret = %q", s.SecretName)
			}
		})
	}
}
