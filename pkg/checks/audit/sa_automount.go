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

package audit

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

const (
	kindDefaultSAMount       = "audit.default_sa_automount"
	reasonDefaultSAAutomount = "DefaultServiceAccountAutomount"
)

// defaultServiceAccount is the ServiceAccount every namespace has and
// every pod naming none is given.
const defaultServiceAccount = "default"

// mountsDefaultToken reports whether this template would actually
// receive the default ServiceAccount's token: it runs as the default
// SA, and it does not turn the mount off at the pod level. The
// SA-level field is checked separately — pod-level wins where both are
// set, which is why the two halves cannot be collapsed.
func mountsDefaultToken(spec corev1.PodSpec) bool {
	if n := spec.ServiceAccountName; n != "" && n != defaultServiceAccount {
		return false
	}
	return spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken
}

// defaultSAFinding judges the default ServiceAccount's token mount.
//
// It fires only when the token is BOTH offered and taken. Every
// namespace in every cluster has a default ServiceAccount that
// automounts, so the offer alone is not a finding — it is the resting
// state of Kubernetes, and a check that reported it would fire once
// per namespace forever.
func (ix *hardeningIndex) defaultSAFinding(namespace string) (emit.Finding, bool) {
	sa, ok := ix.defaultSAs[namespace]
	if !ok {
		return emit.Finding{}, false
	}
	if sa.AutomountServiceAccountToken != nil && !*sa.AutomountServiceAccountToken {
		return emit.Finding{}, false
	}
	users := ix.defaultSAUsers[namespace]
	if len(users) == 0 {
		return emit.Finding{}, false
	}
	sort.Strings(users)
	f := emit.Finding{
		Kind:     kindDefaultSAMount,
		Severity: emit.SeverityWarning,
		Reason:   reasonDefaultSAAutomount,
		Message: fmt.Sprintf("%d %s run as the default ServiceAccount and receive its token: an API credential is sitting in a container that never asked for one, and it is the same credential for everything in the namespace",
			len(users), plural(len(users), "workload")),
		Details: []emit.Field{
			{Key: "service_account", Value: defaultServiceAccount},
			{Key: "mounting_workloads", Value: itoa(len(users))},
			{Key: "mounting_workload_names", Value: cappedList(users)},
		},
		Namespace:    namespace,
		KindOfObject: "ServiceAccount",
		Name:         defaultServiceAccount,
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, f.KindOfObject)
	return f, true
}

func (ix *hardeningIndex) defaultSAFindings(namespace string) []emit.Finding {
	f, ok := ix.defaultSAFinding(namespace)
	if !ok {
		return nil
	}
	if !ix.perWorkload {
		return []emit.Finding{f}
	}
	users := append([]string(nil), ix.defaultSAUsers[namespace]...)
	sort.Strings(users)
	out := make([]emit.Finding, 0, len(users))
	for _, u := range users {
		wlKind, wlName, ok := strings.Cut(u, "/")
		if !ok {
			continue
		}
		wf := f
		wf.Namespace = namespace
		wf.KindOfObject = wlKind
		wf.Name = wlName
		wf.Fingerprint = engine.PostureFingerprint(wf.Kind, wf.Reason, wf.KindOfObject)
		out = append(out, wf)
	}
	return out
}
