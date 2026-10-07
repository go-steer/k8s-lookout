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

package health

// The certs category's cert-manager half: Certificates
// (cert-manager.io/v1), discovery-gated and read through the dynamic
// client like every other optional-CRD read on the read path
// (pkg/checks/crd). On a cluster without the CRD nothing here runs and
// the category is exactly the TLS-Secret check it always was.
//
// The judgement — never issued, renewal failed, inside the
// first-issuance grace — is pkg/certmanager's, the same code the
// expiry sentinel source pages on, so the scan and the sentinel cannot
// disagree about what a Certificate's status means. The GRADING is
// health's own and follows how the category already grades a TLS
// Secret: broken now is critical (expired, or never issued — nothing
// to serve), at risk is warning (expiring inside --cert-warn, or a
// failed renewal while the current certificate is still valid).
//
// One problem, one reason: the TLS Secret a Certificate writes
// (spec.secretName) is attributed to the Certificate — which carries
// the renewal state the Secret cannot — and is not judged again as a
// Secret, the expiry source's attribution rule.

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/certmanager"
	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/crd"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// certManagerGroup is the optional API group the certs category reads.
var certManagerGroup = crd.Group{
	Name:      "cert-manager",
	GV:        certmanager.GV,
	Resources: []string{certmanager.CertificateGVR.Resource},
	Install:   "install cert-manager to have Certificates judged",
}

// certificatesResult is one Certificate pass: the findings, the TLS
// Secrets the Certificates own ("ns/name"), and how many were read.
type certificatesResult struct {
	findings []emit.Finding
	managed  map[string]bool
	scanned  int
}

// checkCertificates lists the Certificates in scope and grades each
// through the shared judgement. Nothing reaches the scorecard unless
// the whole List succeeded: a partial category must not score as if
// it were whole.
func checkCertificates(ctx context.Context, dyn dynamic.Interface, ns string, now time.Time, warn time.Duration) (certificatesResult, error) {
	res := certificatesResult{managed: map[string]bool{}}
	items, err := crd.ListAll(ctx, dyn, ns, certmanager.CertificateGVR)
	if err != nil {
		return res, err
	}
	now = now.UTC()
	for _, u := range items {
		c := certmanager.Read(u)
		res.scanned++
		if c.SecretName != "" {
			res.managed[c.Namespace+"/"+c.SecretName] = true
		}
		if f, ok := gradeCertificate(c, now, warn); ok {
			res.findings = append(res.findings, f)
		}
	}
	return res, nil
}

// gradeCertificate maps one Certificate's facts onto the category's
// severities; ok=false is a Certificate that does not count against
// health (issued and valid, not reportable yet, or still inside the
// first-issuance grace).
func gradeCertificate(c certmanager.Status, now time.Time, warn time.Duration) (emit.Finding, bool) {
	if !c.Reportable() || c.InFirstIssuanceGrace(now, certmanager.DefaultFirstIssuanceGrace, false) {
		return emit.Finding{}, false
	}
	f := emit.Finding{
		Namespace:    c.Namespace,
		KindOfObject: "Certificate",
		Name:         c.Name,
	}
	if c.SecretName != "" {
		f.Details = append(f.Details, emit.Field{Key: "secret", Value: c.SecretName})
	}
	if c.NeverIssued {
		// Reportable and never issued means Ready=False past the
		// grace (or with a recorded failure): nothing to serve.
		f.Kind = "cert.never_issued"
		f.Severity = emit.SeverityCritical
		f.Reason = "CertificateNeverIssued"
		f.Message = c.NeverIssuedMessage()
		return f, true
	}
	days := int(math.Floor(c.NotAfter.Sub(now).Hours() / 24))
	f.Details = append(f.Details,
		emit.Field{Key: "not_after", Value: c.NotAfter.UTC().Format(time.RFC3339)},
		emit.Field{Key: "days_left", Value: strconv.Itoa(days)},
	)
	switch {
	case c.NotAfter.Before(now):
		f.Kind = "cert.expired"
		f.Severity = emit.SeverityCritical
		f.Reason = "CertificateExpired"
		f.Message = fmt.Sprintf("certificate expired %dd ago", -days)
		if c.RenewalFailed {
			f.Message += "; renewal failed: " + renewalEvidence(c)
		}
	case c.RenewalFailed:
		f.Kind = "cert.renewal_failed"
		f.Severity = emit.SeverityWarning
		f.Reason = "CertificateRenewalFailed"
		f.Message = fmt.Sprintf("certificate renewal failed: %s; the current certificate expires in %dd", renewalEvidence(c), days)
	case c.NotAfter.Sub(now) <= warn:
		f.Kind = "cert.expiring"
		f.Severity = emit.SeverityWarning
		f.Reason = "CertificateExpiringSoon"
		f.Message = fmt.Sprintf("certificate expires in %dd", days)
	default:
		return emit.Finding{}, false
	}
	return f, true
}

// renewalEvidence is what says a renewal failed: the Ready condition's
// reason and message, else the recorded failure time.
func renewalEvidence(c certmanager.Status) string {
	switch {
	case c.ReadyDetail != "":
		return c.ReadyDetail
	case c.LastFailure != "":
		return "last failure " + c.LastFailure
	}
	return "Ready=False"
}

// secretsNeeds explains the certs category's TLS-Secret read: the
// unavailable reason's tail when that read is refused and nothing
// else scored the category.
const secretsNeeds = "certificate expiry is read from the tls.crt of each kubernetes.io/tls Secret"

// scoreCerts scores the certs category: cert-manager Certificates
// first when discovery says the CRD is served, then the TLS Secrets no
// Certificate owns. Refusals degrade per #546 — the category answers
// unavailable only when nothing it reads could be read; a refusal of
// one half while the other scored is named on the scorecard line as
// unverified=. refused is run's Forbidden classifier.
func scoreCerts(ctx context.Context, deps Deps, client kubernetes.Interface, ns string, now time.Time, warn time.Duration, card *scorecard, refused func(cat, needs string, err error) error) (int, error) {
	scanned := 0
	served := crd.NewResolver(client.Discovery()).Resolve(certManagerGroup).Serves(certmanager.CertificateGVR.Resource)
	var managed map[string]bool
	certRefused := ""
	if served {
		dyn, err := deps.dynamic(ctx)
		if err != nil {
			return 0, err
		}
		res, err := checkCertificates(ctx, dyn, ns, now, warn)
		refusal, forbidden := checks.ForbiddenRefusal(err)
		switch {
		case err == nil:
			scanned += res.scanned
			managed = res.managed
			for _, f := range res.findings {
				card.add("certs", f)
			}
		case forbidden:
			certRefused = refusal.String()
		default:
			return 0, err
		}
	}

	secrets, n, err := checkCerts(ctx, client, ns, now, warn, managed)
	if err != nil {
		refusal, forbidden := checks.ForbiddenRefusal(err)
		switch {
		case !forbidden:
			return 0, err
		case !served:
			// No cert-manager: the TLS Secrets were the whole category.
			return scanned, refused("certs", secretsNeeds, err)
		case certRefused != "":
			// Nothing the category reads could be read.
			delete(card.findings, "certs")
			card.unavailable["certs"] = refusal.String() + "; " + certRefused +
				" — certificate expiry is read from cert-manager Certificates' status and the tls.crt of each kubernetes.io/tls Secret"
			return scanned, nil
		default:
			// The Certificates scored; the Secrets nothing manages are
			// the blind spot.
			card.unverified["certs"] = "TLS Secrets no cert-manager Certificate manages (" + refusal.String() + ")"
			return scanned, nil
		}
	}
	scanned += n
	for _, f := range secrets {
		card.add("certs", f)
	}
	if certRefused != "" {
		card.unverified["certs"] = "cert-manager Certificate issuance and renewal state (" + certRefused + ")"
	}
	return scanned, nil
}
