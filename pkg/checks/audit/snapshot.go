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
	"context"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// Snapshot is the audit group's shared, lazy, per-invocation read of
// the cluster. Each getter lists its kind at most once for the life of
// the snapshot, so detectors that run together (`audit workloads` and a
// rules evaluation in the same process, or a future `audit all`) read
// Services once instead of once each. A detector that never asks never
// pays.
//
// The snapshot rides the context (WithSnapshot / snapshotFor) rather
// than Deps, because Deps is per command and the snapshot is per
// invocation: the same Deps value serves every call, the snapshot
// serves one scan of one scope.
//
// Only Services live here today. Moving the workload lists over is the
// next step (the roadmap's `audit.Snapshot`); the pilot keeps the
// change to what the new detectors need.
type Snapshot struct {
	client kubernetes.Interface
	ns     string

	servicesOnce    sync.Once
	services        []*corev1.Service
	servicesErr     error
	servicesRefused *checks.Refusal
}

type snapshotKey struct{}

// WithSnapshot attaches a snapshot to the context.
func WithSnapshot(ctx context.Context, s *Snapshot) context.Context {
	return context.WithValue(ctx, snapshotKey{}, s)
}

// snapshotFor returns the snapshot on the context when it was built
// for the same client and namespace scope, else a fresh one.
func snapshotFor(ctx context.Context, client kubernetes.Interface, ns string) *Snapshot {
	if s, ok := ctx.Value(snapshotKey{}).(*Snapshot); ok && s != nil && s.client == client && s.ns == ns {
		return s
	}
	return &Snapshot{client: client, ns: ns}
}

// Services lists the Services in scope, once. An RBAC refusal is not an
// error: it is returned as a Refusal so the caller can skip the claims
// that need Services and say so, rather than fail the whole command.
func (s *Snapshot) Services(ctx context.Context) ([]*corev1.Service, *checks.Refusal, error) {
	s.servicesOnce.Do(func() {
		err := listPages("services", func(o metav1.ListOptions) ([]corev1.Service, string, error) {
			l, err := s.client.CoreV1().Services(s.ns).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(svc *corev1.Service) {
			s.services = append(s.services, svc)
		})
		if err != nil {
			if r, ok := checks.ForbiddenRefusal(err); ok {
				s.servicesRefused = &r
				return
			}
			s.servicesErr = err
			return
		}
		sort.Slice(s.services, func(i, j int) bool {
			a, b := s.services[i], s.services[j]
			if a.Namespace != b.Namespace {
				return a.Namespace < b.Namespace
			}
			return a.Name < b.Name
		})
	})
	return s.services, s.servicesRefused, s.servicesErr
}

// selectingServices returns the Services in a namespace whose selector
// matches a pod template's labels. ExternalName Services and Services
// with no selector route to nothing in the cluster and are skipped —
// the same rule collect.py's `_selecting_services` applies, so the two
// tools agree on what "Service-backed" means.
func selectingServices(services []*corev1.Service, namespace string, podLabels map[string]string) []*corev1.Service {
	var out []*corev1.Service
	for _, svc := range services {
		if svc.Namespace != namespace || svc.Spec.Type == corev1.ServiceTypeExternalName || len(svc.Spec.Selector) == 0 {
			continue
		}
		if labels.SelectorFromSet(svc.Spec.Selector).Matches(labels.Set(podLabels)) {
			out = append(out, svc)
		}
	}
	return out
}
