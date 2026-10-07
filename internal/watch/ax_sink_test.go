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

package watch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTemplate(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "task.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAXTaskTemplate(t *testing.T) {
	p := writeTemplate(t, `
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  atespace: triage
spec:
  image: us-docker.pkg.dev/p/r/agent@sha256:abc
  command: [mast, --workload=/opt/mast/workloads/gke-triage]
  egress:
    - hosts: [aiplatform.googleapis.com]
      credentials:
        - header: Authorization
          prefix: "Bearer "
          credentialUri: ate-secret://k8s.io/default/creds/google-token/token
  http:
    port: 8484
`)
	tmpl, err := loadAXTaskTemplate(p)
	if err != nil {
		t.Fatalf("loadAXTaskTemplate: %v", err)
	}
	if tmpl.GetMetadata().GetAtespace() != "triage" || tmpl.GetSpec().GetHttp().GetPort() != 8484 {
		t.Errorf("template not decoded: %v", tmpl)
	}
	if got := tmpl.GetSpec().GetEgress()[0].GetCredentials()[0].GetCredentialUri(); !strings.HasPrefix(got, "ate-secret://") {
		t.Errorf("credential uri = %q", got)
	}
}

func TestLoadAXTaskTemplate_Rejects(t *testing.T) {
	cases := map[string]string{
		"unknown field": "kind: Task\nspec:\n  image: x\n  imgae: typo\n",
		"wrong kind":    "kind: Workspace\nspec:\n  image: x\n",
		"no spec":       "kind: Task\nmetadata:\n  atespace: a\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := loadAXTaskTemplate(writeTemplate(t, body)); err == nil {
				t.Errorf("expected %s to be rejected", name)
			}
		})
	}
}
