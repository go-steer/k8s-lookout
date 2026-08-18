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

package state

// `state wi` (DESIGN.md §5): Workload Identity KSA↔GSA binding
// verification — the wi-scout mission. The cluster side (which pods
// run as which ServiceAccount, which SAs claim a cloud identity via
// the iam.gke.io/gcp-service-account annotation) is read with
// client-go; the cloud side (does the claimed GSA exist, does the
// KSA hold roles/iam.workloadIdentityUser on it) goes through the
// pkg/cloud WorkloadIdentityAPI capability (§2: this package never
// imports cloud SDKs). Vanilla clusters degrade to the standard
// explicit `cloud.unavailable` record, mirroring the `cloud` group.
//
// Missing ServiceAccount objects are deliberately NOT reported here:
// a pod referencing a nonexistent SA is `state edges` territory
// (edge.missing_ref); wi verifies the cloud half of the chain.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// wiAnnotation is where GKE reads the claimed cloud identity: the
// iam.gke.io/gcp-service-account annotation on the ServiceAccount
// object (not the pod).
const wiAnnotation = "iam.gke.io/gcp-service-account"

// wiCredEnv is the env var the unannotated-use heuristic looks for:
// a pod pointing at a mounted credential file instead of Workload
// Identity.
const wiCredEnv = "GOOGLE_APPLICATION_CREDENTIALS" //nolint:gosec // the env var's NAME, not a credential value (G101 false positive)

// WIDeps are the injectable dependencies of `state wi`. The zero
// value gives production behavior; tests inject a fake clientset and
// a fake provider.
type WIDeps struct {
	// Client builds the Kubernetes client. Nil means kube.BuildClient
	// with default resolution.
	Client func(ctx context.Context) (kubernetes.Interface, error)
	// Provider yields the cloud provider. Nil means cloud.New
	// default detection (the NoProvider sentinel on vanilla builds —
	// the command then reports unavailable, never silence, §2).
	Provider func(ctx context.Context) (cloud.Provider, error)
	// Now is the clock. Nil means time.Now. Reserved seam: wi has no
	// time math today, kept for parity with the other state deps.
	Now func() time.Time
}

func (d WIDeps) client(ctx context.Context) (kubernetes.Interface, error) {
	if d.Client != nil {
		return d.Client(ctx)
	}
	return kube.BuildClient(kube.Options{})
}

func (d WIDeps) provider(ctx context.Context) (cloud.Provider, error) {
	if d.Provider != nil {
		return d.Provider(ctx)
	}
	return cloud.New(ctx, cloud.Config{})
}

func init() {
	checks.Register(WICommand(WIDeps{}))
}

// WICommand builds the `lookout state wi` command (§5 tool matrix
// row: wi-scout — GKE Workload Identity KSA↔GSA binding verification
// via the IAM API, behind the §2 provider boundary).
func WICommand(deps WIDeps) checks.Command {
	return checks.Command{
		Name:    "state wi",
		MCPName: "k8s_workload_identity",
		Summary: "When a GKE pod gets 403s or metadata-server errors calling GCP APIs, verify the Workload Identity chain — KSA annotation (iam.gke.io/gcp-service-account) → roles/iam.workloadIdentityUser binding on the GSA — reporting only the broken links; vanilla clusters report an explicit unavailable.",
		Output: []checks.OutputField{
			{Name: "gsa", Doc: "the cloud identity (GSA email) the ServiceAccount's annotation claims"},
			{Name: "pods", Doc: "how many in-scope pods run as the affected ServiceAccount"},
			{Name: "problem", Doc: "machine-matchable problem code from the provider (e.g. no-workload-identity-binding)"},
			{Name: "container", Doc: "container carrying the GOOGLE_APPLICATION_CREDENTIALS env var"},
			{Name: "env", Doc: "the credential-file env var found (" + wiCredEnv + ")"},
			{Name: "capability", Doc: "cloud.unavailable: the provider capability this command needed (" + string(cloud.CapabilityWorkloadIdentity) + ")"},
			{Name: "provider", Doc: "cloud.unavailable: the provider that was asked"},
			{Name: "unavailable", Doc: "summary-line note (§2 marker): why the cloud read could not be served"},
		},
		Examples: []string{
			"lookout state wi",
			"lookout state wi --namespace=prod",
			"lookout state wi --workload=Deployment/prod/api --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runWI(ctx, deps, inv)
		},
	}
}

// runWI drives one scan. Scoping: --namespace limits to one
// namespace; the default (like -A) is all namespaces; --workload
// limits to that workload's pods (resolved via the Cluster seam).
// scanned counts the pods considered plus the ServiceAccounts
// listed.
func runWI(ctx context.Context, deps WIDeps, inv emit.Invocation) (int, error) {
	wl := inv.Scope.Workload
	if !wl.IsZero() {
		if _, ok := workloadKinds[wl.Kind]; !ok {
			return 0, emit.UsageErrorf("unsupported workload kind %q (want %s)", wl.Kind, workloadKindNames())
		}
		if inv.Scope.Namespace != "" && inv.Scope.Namespace != wl.Namespace {
			return 0, emit.UsageErrorf("--namespace=%s contradicts --workload namespace %s", inv.Scope.Namespace, wl.Namespace)
		}
	}

	// Capability first (§2): on a vanilla build nothing is listed —
	// scanned=0 is honest, the degradation record explains why.
	provider, err := deps.provider(ctx)
	if err != nil {
		return 0, err
	}
	api, ok := provider.WorkloadIdentity()
	if !ok {
		return wiEmitUnavailable(inv, provider)
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}

	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}
	if !wl.IsZero() {
		listNS = wl.Namespace
	}

	var pods []*corev1.Pod
	if wl.IsZero() {
		err = listPages("pods", func(o metav1.ListOptions) ([]corev1.Pod, string, error) {
			l, err := client.CoreV1().Pods(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(p *corev1.Pod) { pods = append(pods, p) })
		if err != nil {
			return 0, err
		}
	} else {
		// The workload path resolves the pod set through the same
		// Cluster seam `state edges` uses (owner-chain traversal, not
		// name-prefix guessing).
		cluster, err := LoadCluster(ctx, client, wl.Namespace)
		if err != nil {
			return 0, err
		}
		pods, err = cluster.WorkloadPods(wl)
		if err != nil {
			return 0, err
		}
	}

	serviceAccounts := map[string]*corev1.ServiceAccount{} // ns/name
	saListed := 0
	err = listPages("serviceaccounts", func(o metav1.ListOptions) ([]corev1.ServiceAccount, string, error) {
		l, err := client.CoreV1().ServiceAccounts(listNS).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, func(s *corev1.ServiceAccount) {
		serviceAccounts[key(s.Namespace, s.Name)] = s
		saListed++
	})
	if err != nil {
		return 0, err
	}

	findings, err := wiScan(ctx, api, pods, serviceAccounts)
	if err != nil {
		return 0, err
	}
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	return len(pods) + saListed, nil
}

// wiScan groups pods by (namespace, serviceAccountName), verifies
// every annotated SA's claim through the provider, and applies the
// unannotated-use heuristic to the rest. Healthy (annotated + bound)
// identities emit nothing.
func wiScan(ctx context.Context, api cloud.WorkloadIdentityAPI, pods []*corev1.Pod, serviceAccounts map[string]*corev1.ServiceAccount) ([]emit.Finding, error) {
	bySA := map[string][]*corev1.Pod{} // ns/saName
	for _, p := range pods {
		sa := p.Spec.ServiceAccountName
		if sa == "" {
			sa = "default"
		}
		k := key(p.Namespace, sa)
		bySA[k] = append(bySA[k], p)
	}
	saKeys := make([]string, 0, len(bySA))
	for k := range bySA {
		saKeys = append(saKeys, k)
	}
	sort.Strings(saKeys)

	var findings []emit.Finding
	for _, k := range saKeys {
		group := bySA[k]
		ns := group[0].Namespace
		sa, exists := serviceAccounts[k]
		if exists && sa.Annotations[wiAnnotation] != "" {
			gsa := sa.Annotations[wiAnnotation]
			b, err := api.VerifyBinding(ctx, ns, sa.Name, gsa)
			if err != nil {
				return nil, fmt.Errorf("verifying workload identity of serviceaccount %s (gsa %s): %w", k, gsa, err)
			}
			if !b.Bound {
				findings = append(findings, wiBrokenFinding(ns, sa.Name, gsa, len(group), b))
			}
			continue
		}
		// No annotated SA (missing SA objects are `state edges`
		// territory — skipped silently here): flag pods that carry a
		// credential-file env var instead of Workload Identity.
		for _, p := range group {
			if f, ok := wiUnannotatedUse(p); ok {
				findings = append(findings, f)
			}
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		a, b := findings[i], findings[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.Kind < b.Kind
	})
	return findings, nil
}

// wiBrokenFinding maps one !Bound verification result onto a
// finding, branching on the machine-matchable WIProblem* code (never
// on prose).
func wiBrokenFinding(ns, ksa, gsa string, podCount int, b cloud.WIBinding) emit.Finding {
	code, detail := wiSplitProblem(b.Problems)
	f := emit.Finding{
		Severity:     emit.SeverityCritical,
		Namespace:    ns,
		KindOfObject: "ServiceAccount",
		Name:         ksa,
		Details: []emit.Field{
			{Key: "gsa", Value: gsa},
			{Key: "pods", Value: strconv.Itoa(podCount)},
		},
	}
	if code == cloud.WIProblemIdentityMissing {
		f.Kind = "wi.gsa_missing"
		f.Reason = "GSAMissing"
		f.Message = fmt.Sprintf("annotated GSA %s does not exist — every GCP call from these pods fails", gsa)
		return f
	}
	f.Kind = "wi.unbound"
	f.Reason = "BindingMissing"
	if code != "" {
		f.Details = append(f.Details, emit.Field{Key: "problem", Value: code})
	}
	msg := fmt.Sprintf("annotation claims %s but the workload identity binding is missing", gsa)
	if detail != "" {
		msg += ": " + detail
	}
	f.Message = msg
	return f
}

// wiSplitProblem returns the leading WIProblem* code and detail of
// Problems[0] ("<code>: <detail>"; detail optional).
func wiSplitProblem(problems []string) (code, detail string) {
	if len(problems) == 0 {
		return "", ""
	}
	code, detail, _ = strings.Cut(problems[0], ":")
	return strings.TrimSpace(code), strings.TrimSpace(detail)
}

// wiUnannotatedUse is the minimal heuristic for pods outside
// Workload Identity: a container env var pointing at a mounted
// credential file. Informational only — it works, but static keys
// carry rotation/leakage risk.
func wiUnannotatedUse(p *corev1.Pod) (emit.Finding, bool) {
	for _, c := range p.Spec.Containers {
		for _, e := range c.Env {
			if e.Name != wiCredEnv {
				continue
			}
			return emit.Finding{
				Kind:         "wi.unannotated_use",
				Severity:     emit.SeverityInfo,
				Namespace:    p.Namespace,
				KindOfObject: "Pod",
				Name:         p.Name,
				Reason:       "CredentialFileWithoutWI",
				Message:      "pod points at a mounted credential file instead of Workload Identity — works, but key rotation/leakage risk",
				Details: []emit.Field{
					{Key: "container", Value: c.Name},
					{Key: "env", Value: wiCredEnv},
				},
			}, true
		}
	}
	return emit.Finding{}, false
}

// wiEmitUnavailable is the §2-mandated degradation path (same record
// as the `cloud` group's): one explicit cloud.unavailable finding,
// the summary marker, exit 0 with scanned=0 — nothing was listed,
// and that is reported, not implied.
func wiEmitUnavailable(inv emit.Invocation, p cloud.Provider) (int, error) {
	u := cloud.Unavailable(p, cloud.CapabilityWorkloadIdentity)
	if err := inv.Out.Emit(emit.Finding{
		Kind:     "cloud.unavailable",
		Severity: emit.SeverityInfo,
		Reason:   "CapabilityUnavailable",
		Message:  fmt.Sprintf("state wi needs the provider %s capability: %s", cloud.CapabilityWorkloadIdentity, u.Reason),
		Details: []emit.Field{
			{Key: "capability", Value: string(u.Capability)},
			{Key: "provider", Value: u.Provider},
		},
	}); err != nil {
		return 0, err
	}
	if err := inv.Out.Note("unavailable", u.Reason); err != nil {
		return 0, err
	}
	return 0, nil
}
