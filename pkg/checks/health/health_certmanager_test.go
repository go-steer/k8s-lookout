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

package health_test

// The certs category's cert-manager half: Certificates read through
// the dynamic fake, discovery-gated on the fake clientset's discovery.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/certmanager"
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/health"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// cmFixture is one cert-manager cluster: whether discovery serves the
// Certificate CRD, the Certificates, which reads are refused, and the
// built-in objects.
type cmFixture struct {
	served       bool
	certs        []*unstructured.Unstructured
	denyCerts    bool
	denySecrets  bool
	core         []runtime.Object
	dynamicCalls *int
}

func (fx cmFixture) command(t *testing.T) checks.Command {
	t.Helper()
	cs := fake.NewClientset(fx.core...)
	if fx.served {
		cs.Resources = []*metav1.APIResourceList{{
			GroupVersion: certmanager.GV.String(),
			APIResources: []metav1.APIResource{{Name: "certificates", Namespaced: true, Kind: "Certificate"}},
		}}
	}
	if fx.denySecrets {
		cs.PrependReactor("list", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, forbidden(schema.GroupResource{Resource: "secrets"})
		})
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{certmanager.CertificateGVR: "CertificateList"})
	for _, c := range fx.certs {
		if err := dyn.Tracker().Create(certmanager.CertificateGVR, c, c.GetNamespace()); err != nil {
			t.Fatalf("seed Certificate %s: %v", c.GetName(), err)
		}
	}
	if fx.denyCerts {
		dyn.PrependReactor("list", "certificates", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(certmanager.CertificateGVR.GroupResource(), "", errors.New("denied by test"))
		})
	}
	return health.New(health.Deps{
		Client:   func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Provider: func(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil },
		Now:      func() time.Time { return fixedNow },
		Dynamic: func(context.Context) (dynamic.Interface, error) {
			if fx.dynamicCalls != nil {
				*fx.dynamicCalls++
			}
			return dyn, nil
		},
	})
}

// cmCert builds one Certificate. notAfter zero = never issued; ready
// "" = no Ready condition.
func cmCert(ns, name, secret string, created, notAfter time.Time, ready, reason, message, lastFailure string) *unstructured.Unstructured {
	status := map[string]any{}
	if !notAfter.IsZero() {
		status["notAfter"] = notAfter.UTC().Format(time.RFC3339)
	}
	if ready != "" {
		status["conditions"] = []any{map[string]any{
			"type": "Ready", "status": ready, "reason": reason, "message": message,
		}}
	}
	if lastFailure != "" {
		status["lastFailureTime"] = lastFailure
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata": map[string]any{
			"namespace": ns, "name": name, "uid": ns + "-" + name,
		},
		"spec":   map[string]any{"secretName": secret},
		"status": status,
	}}
	u.SetCreationTimestamp(metav1.Time{Time: created})
	return u
}

// certsOutcome is the certs scorecard line and its detail records.
func certsOutcome(t *testing.T, cmd checks.Command) (line map[string]string, details []map[string]string) {
	t.Helper()
	res := checktest.Run(t, cmd)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	for _, l := range lines[:len(lines)-1] {
		rec := parseLine(t, l)
		if rec["category"] != "certs" {
			continue
		}
		if rec["kind"] == "health.category" {
			line = rec
		} else {
			details = append(details, rec)
		}
	}
	if line == nil {
		t.Fatalf("no certs scorecard line:\n%s", res.Stdout)
	}
	return line, details
}

const doesNotExist = "Issuing certificate as Secret does not exist"

func TestCertManagerNeverIssuedPastGraceIsCritical(t *testing.T) {
	fx := cmFixture{served: true, certs: []*unstructured.Unstructured{
		cmCert("prod", "api", "api-tls", fixedNow.Add(-time.Hour), time.Time{}, "False", "DoesNotExist", doesNotExist, ""),
	}}
	line, details := certsOutcome(t, fx.command(t))
	if line["status"] != "degraded" || line["severity"] != "critical" {
		t.Errorf("certs line = %v; want degraded critical", line)
	}
	if len(details) != 1 {
		t.Fatalf("want one detail, got %v", details)
	}
	d := details[0]
	if d["kind"] != "cert.never_issued" || d["kind_of_object"] != "Certificate" {
		t.Errorf("detail = %v; want cert.never_issued on the Certificate", d)
	}
	if want := "certificate never issued: DoesNotExist " + doesNotExist; d["message"] != want {
		t.Errorf("message = %q; want %q (the expiry source's wording)", d["message"], want)
	}
	if d["reason"] != "CertificateNeverIssued" || d["secret"] != "api-tls" {
		t.Errorf("detail = %v; want reason=CertificateNeverIssued secret=api-tls", d)
	}
}

func TestCertManagerInsideFirstIssuanceGraceIsHealthy(t *testing.T) {
	fx := cmFixture{served: true, certs: []*unstructured.Unstructured{
		cmCert("prod", "api", "api-tls", fixedNow.Add(-5*time.Minute), time.Time{}, "False", "DoesNotExist", doesNotExist, ""),
	}}
	line, details := certsOutcome(t, fx.command(t))
	if line["status"] != "healthy" || len(details) != 0 {
		t.Errorf("certs = %v %v; a Certificate 5m into its first issuance is not a problem", line, details)
	}
}

// A recorded failure ends the grace early, as it does in the expiry
// source.
func TestCertManagerRecordedFailureSkipsGrace(t *testing.T) {
	fx := cmFixture{served: true, certs: []*unstructured.Unstructured{
		cmCert("prod", "api", "api-tls", fixedNow.Add(-5*time.Minute), time.Time{}, "False", "DoesNotExist", doesNotExist,
			fixedNow.Add(-time.Minute).Format(time.RFC3339)),
	}}
	_, details := certsOutcome(t, fx.command(t))
	if len(details) != 1 || details[0]["kind"] != "cert.never_issued" {
		t.Errorf("details = %v; want cert.never_issued despite the young Certificate", details)
	}
}

func TestCertManagerIssuedAndValidIsHealthy(t *testing.T) {
	fx := cmFixture{served: true,
		certs: []*unstructured.Unstructured{
			cmCert("prod", "api", "api-tls", fixedNow.Add(-30*24*time.Hour), fixedNow.Add(60*24*time.Hour), "True", "Ready", "Certificate is up to date and has not expired", ""),
		},
		core: []runtime.Object{tlsSecret(t, "prod", "api-tls", "api.example.com", fixedNow.Add(60*24*time.Hour))},
	}
	line, details := certsOutcome(t, fx.command(t))
	if line["status"] != "healthy" || len(details) != 0 {
		t.Errorf("certs = %v %v; want healthy", line, details)
	}
}

func TestCertManagerRenewalFailedIsWarning(t *testing.T) {
	fx := cmFixture{served: true, certs: []*unstructured.Unstructured{
		cmCert("prod", "api", "api-tls", fixedNow.Add(-90*24*time.Hour), fixedNow.Add(60*24*time.Hour), "False", "Failed", "issuer unreachable",
			fixedNow.Add(-time.Hour).Format(time.RFC3339)),
	}}
	line, details := certsOutcome(t, fx.command(t))
	if line["status"] != "degraded" || line["severity"] != "warning" {
		t.Errorf("certs line = %v; want degraded warning", line)
	}
	if len(details) != 1 || details[0]["kind"] != "cert.renewal_failed" {
		t.Fatalf("details = %v; want one cert.renewal_failed", details)
	}
	if want := "certificate renewal failed: Failed issuer unreachable; the current certificate expires in 60d"; details[0]["message"] != want {
		t.Errorf("message = %q; want %q", details[0]["message"], want)
	}
}

// One problem, one reason: the Secret a Certificate writes is judged as
// the Certificate, not again as a Secret. An unmanaged Secret beside it
// is still judged.
func TestCertManagerOwnedSecretIsNotJudgedTwice(t *testing.T) {
	soon := fixedNow.Add(10 * 24 * time.Hour)
	fx := cmFixture{served: true,
		certs: []*unstructured.Unstructured{
			cmCert("prod", "api", "api-tls", fixedNow.Add(-80*24*time.Hour), soon, "False", "Failed", "issuer unreachable", ""),
		},
		core: []runtime.Object{
			tlsSecret(t, "prod", "api-tls", "api.example.com", soon),
			tlsSecret(t, "prod", "legacy-tls", "legacy.example.com", soon),
		},
	}
	_, details := certsOutcome(t, fx.command(t))
	var got []string
	for _, d := range details {
		got = append(got, d["kind"]+" "+d["name"])
	}
	if want := "cert.renewal_failed api,cert.expiring legacy-tls"; strings.Join(got, ",") != want {
		t.Errorf("certs details = %v; want %s", got, want)
	}
}

// On a cluster without the CRD nothing changes: the certs line and
// details are byte-identical to the Secrets-only check, and the
// dynamic client is never built.
func TestCertManagerAbsentChangesNothing(t *testing.T) {
	calls := 0
	objs := brokenObjects(t)
	fx := cmFixture{
		certs: []*unstructured.Unstructured{
			cmCert("prod", "api", "api-tls", fixedNow.Add(-time.Hour), time.Time{}, "False", "DoesNotExist", doesNotExist, ""),
		},
		core:         objs,
		dynamicCalls: &calls,
	}
	got := checktest.Run(t, fx.command(t))
	want := checktest.Run(t, testCommand(objs...))
	if got.Stdout != want.Stdout {
		t.Errorf("output changed without the CRD:\n got: %s\nwant: %s", got.Stdout, want.Stdout)
	}
	if calls != 0 {
		t.Errorf("dynamic client built %d times on a cluster without cert-manager", calls)
	}
}

func TestCertManagerForbiddenCertificatesNotedSecretsStillJudged(t *testing.T) {
	fx := cmFixture{served: true, denyCerts: true,
		certs: []*unstructured.Unstructured{
			cmCert("prod", "api", "api-tls", fixedNow.Add(-time.Hour), time.Time{}, "False", "DoesNotExist", doesNotExist, ""),
		},
		core: []runtime.Object{tlsSecret(t, "prod", "api-tls", "api.example.com", fixedNow.Add(-24*time.Hour))},
	}
	line, details := certsOutcome(t, fx.command(t))
	if line["status"] != "degraded" {
		t.Errorf("certs line = %v; want degraded on the Secret alone", line)
	}
	if want := "cert-manager Certificate issuance and renewal state (" + checks.Refused("list", "cert-manager.io", "certificates").String() + ")"; line["unverified"] != want {
		t.Errorf("unverified = %q; want %q", line["unverified"], want)
	}
	if len(details) != 1 || details[0]["kind"] != "cert.expired" || details[0]["name"] != "api-tls" {
		t.Errorf("details = %v; want the Secret's cert.expired only", details)
	}
}

func TestCertManagerForbiddenSecretsNotedCertificatesStillJudged(t *testing.T) {
	fx := cmFixture{served: true, denySecrets: true, certs: []*unstructured.Unstructured{
		cmCert("prod", "api", "api-tls", fixedNow.Add(-time.Hour), time.Time{}, "False", "DoesNotExist", doesNotExist, ""),
	}}
	line, details := certsOutcome(t, fx.command(t))
	if line["status"] != "degraded" {
		t.Errorf("certs line = %v; want degraded on the Certificate", line)
	}
	if want := "TLS Secrets no cert-manager Certificate manages (" + checks.Refused("list", "", "secrets").String() + ")"; line["unverified"] != want {
		t.Errorf("unverified = %q; want %q", line["unverified"], want)
	}
	if len(details) != 1 || details[0]["kind"] != "cert.never_issued" {
		t.Errorf("details = %v; want cert.never_issued", details)
	}
}

func TestCertManagerBothForbiddenIsUnavailable(t *testing.T) {
	fx := cmFixture{served: true, denySecrets: true, denyCerts: true}
	line, details := certsOutcome(t, fx.command(t))
	if line["status"] != "unavailable" || len(details) != 0 {
		t.Errorf("certs = %v %v; want unavailable with no details", line, details)
	}
	for _, want := range []string{checks.Refused("list", "", "secrets").String(), checks.Refused("list", "cert-manager.io", "certificates").String()} {
		if !strings.Contains(line["message"], want) {
			t.Errorf("message %q does not name %q", line["message"], want)
		}
	}
}

func TestCertManagerVerifyContract(t *testing.T) {
	fx := cmFixture{served: true, certs: []*unstructured.Unstructured{
		cmCert("prod", "never", "never-tls", fixedNow.Add(-time.Hour), time.Time{}, "False", "DoesNotExist", doesNotExist, ""),
		cmCert("prod", "failing", "failing-tls", fixedNow.Add(-time.Hour), fixedNow.Add(40*24*time.Hour), "False", "Failed", "x", ""),
		cmCert("prod", "expired", "expired-tls", fixedNow.Add(-time.Hour), fixedNow.Add(-time.Hour), "True", "Ready", "", ""),
		cmCert("prod", "soon", "soon-tls", fixedNow.Add(-time.Hour), fixedNow.Add(5*24*time.Hour), "True", "Ready", "", ""),
	}}
	checktest.VerifyContract(t, fx.command(t))
}
