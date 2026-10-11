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

//go:build gke || allproviders

package gke_test

// End-to-end #614 regression: the real GKE provider, the real REST
// clients and credential stack, a metadata server refusing the token
// the way GKE does for a missing KSA→GSA binding, and the real
// commands. Every one must exit 0 with the CredentialsUnavailable
// record instead of the raw multi-line auth error (the UAT T2/T3
// failure, #179/#180).

import (
	"context"
	"net/http"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/checks/perf"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/cloud/gke"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const refusal = "Unable to generate access token; IAM returned 403 Forbidden: Permission 'iam.serviceAccounts.getAccessToken' denied on resource (or it may not exist).\n" +
	"This error could be caused by a missing IAM policy binding on the target IAM service account.\n"

func TestCommands_TokenRefusalDegrades(t *testing.T) {
	gke.RefusingMetadataServer(t, http.StatusForbidden, refusal)
	p := gke.PinnedProvider(t)
	provider := func(context.Context) (cloud.Provider, error) { return p, nil }

	cs := fake.NewClientset(
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
			Namespace: "lookout-uat", Name: "app",
			Annotations: map[string]string{"iam.gke.io/gcp-service-account": "app@my-project.iam.gserviceaccount.com"},
		}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "lookout-uat", Name: "app-1"},
			Spec:       corev1.PodSpec{ServiceAccountName: "app", Containers: []corev1.Container{{Name: "app"}}},
		},
	)

	tests := []struct {
		name       string
		cmd        checks.Command
		args       []string
		capability cloud.Capability
		what       string
	}{
		{
			name: "state wi",
			cmd: state.WICommand(state.WIDeps{
				Client:   func(context.Context) (kubernetes.Interface, error) { return cs, nil },
				Provider: provider,
			}),
			capability: cloud.CapabilityWorkloadIdentity,
			what:       "state wi",
		},
		{
			name:       "cloud quota",
			cmd:        cloudcheck.QuotaCommand(cloudcheck.Deps{Provider: provider}),
			capability: cloud.CapabilityQuota,
			what:       "cloud quota",
		},
		{
			name:       "perf probe",
			cmd:        perf.Command(perf.Deps{Provider: provider}),
			args:       []string{"--pack=apiserver"},
			capability: cloud.CapabilityMetrics,
			what:       "perf probe --pack=apiserver",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := checktest.Run(t, tt.cmd, tt.args...)
			t.Logf("stdout:\n%s", res.Stdout)
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, want 0 (stderr: %s)", res.Code, res.Stderr)
			}
			lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
			if len(lines) != 2 {
				t.Fatalf("want exactly the CredentialsUnavailable record + summary, got:\n%s", res.Stdout)
			}
			for _, want := range []string{
				"kind=cloud.unavailable",
				"reason=CredentialsUnavailable",
				"capability=" + string(tt.capability),
				"provider=gke",
				tt.what + " could not run: lookout's own cloud identity could not obtain an access token",
				"roles/iam.workloadIdentityUser",
			} {
				if !strings.Contains(lines[0], want) {
					t.Errorf("record %q missing %q", lines[0], want)
				}
			}
			if !strings.Contains(lines[1], `unavailable="`+cloud.CredentialsUnavailableNote+`"`) {
				t.Errorf("summary %q lacks the unavailable= note", lines[1])
			}
			// Nothing of the raw error reaches stdout: no URL, no
			// server prose, no token-endpoint detail.
			for _, raw := range []string{"https://", "googleapis.com", "Unable to generate", "cannot fetch token", "computeMetadata", "wi."} {
				if strings.Contains(res.Stdout, raw) {
					t.Errorf("stdout carries raw error text %q:\n%s", raw, res.Stdout)
				}
			}
		})
	}
}
