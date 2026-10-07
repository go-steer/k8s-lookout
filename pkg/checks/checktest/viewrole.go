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

package checktest

import (
	"errors"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
)

// ViewRoleReads is every (group, resource) the built-in `view`
// ClusterRole lets a subject list — checks.ViewRoleReads, the same
// table the production refusal formatter consults to decide whether a
// refusal is "not granted by the built-in view role". It is an ALLOW
// list on purpose: anything not named there — nodes, secrets, every
// rbac.authorization.k8s.io kind, PersistentVolumes, StorageClasses,
// IngressClasses, admission webhook configurations, CRDs — is
// refused, so a read-path command that grows a new read is tested
// against `view` without anyone updating a deny list (#546).
var ViewRoleReads = checks.ViewRoleReads

// ViewRole makes cs behave as a credential bound to exactly the
// built-in `view` ClusterRole: every get/list/watch of a resource
// outside ViewRoleReads answers 403 the way the API server does. It
// returns a func reporting the refused resources in the order they
// were first asked for, so a test can pin what a command still tries.
func ViewRole(cs *fake.Clientset) (refused func() []schema.GroupResource) {
	return ViewRoleFake(&cs.Fake)
}

// ViewRoleFake is ViewRole for any client built on k8stesting.Fake —
// the dynamic fake client and the metrics fake client included — so a
// command's CRD and metrics reads face the same `view` role as its
// typed ones.
func ViewRoleFake(f *k8stesting.Fake) (refused func() []schema.GroupResource) {
	var (
		mu   sync.Mutex
		seen = map[schema.GroupResource]bool{}
		out  []schema.GroupResource
	)
	deny := func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		gr := action.GetResource().GroupResource()
		if ViewRoleReads[gr] {
			return false, nil, nil
		}
		mu.Lock()
		if !seen[gr] {
			seen[gr] = true
			out = append(out, gr)
		}
		mu.Unlock()
		return true, nil, forbidden(action.GetVerb(), gr)
	}
	for _, verb := range []string{"get", "list"} {
		f.PrependReactor(verb, "*", deny)
	}
	f.PrependWatchReactor("*", func(action k8stesting.Action) (bool, watch.Interface, error) {
		gr := action.GetResource().GroupResource()
		if ViewRoleReads[gr] {
			return false, nil, nil
		}
		return true, nil, forbidden("watch", gr)
	})
	return func() []schema.GroupResource {
		mu.Lock()
		defer mu.Unlock()
		return append([]schema.GroupResource(nil), out...)
	}
}

// forbidden is the 403 the API server answers, worded as it words it.
func forbidden(verb string, gr schema.GroupResource) error {
	return apierrors.NewForbidden(gr, "", errors.New(`User "system:serviceaccount:lookout:agent" cannot `+verb+` resource "`+gr.Resource+`" in API group "`+gr.Group+`"`))
}
