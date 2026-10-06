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
)

// ViewRoleReads is every (group, resource) the built-in `view`
// ClusterRole lets a subject list, transcribed from the aggregated
// rules of system:aggregate-to-view in the upstream bootstrap policy
// (kubernetes/kubernetes plugin/pkg/auth/authorizer/rbac/
// bootstrappolicy/testdata/cluster-roles.yaml). It is an ALLOW list on
// purpose: anything not named here — nodes, secrets, every
// rbac.authorization.k8s.io kind, PersistentVolumes, StorageClasses,
// IngressClasses, admission webhook configurations, CRDs — is
// refused, so a read-path command that grows a new read is tested
// against `view` without anyone updating a deny list (#546).
//
// metrics.k8s.io is absent: metrics-server's own aggregate-to-view
// role adds it on clusters that run metrics-server, but `view` itself
// does not grant it.
var ViewRoleReads = map[schema.GroupResource]bool{
	{Resource: "configmaps"}:                                       true,
	{Resource: "endpoints"}:                                        true,
	{Resource: "persistentvolumeclaims"}:                           true,
	{Resource: "pods"}:                                             true,
	{Resource: "replicationcontrollers"}:                           true,
	{Resource: "serviceaccounts"}:                                  true,
	{Resource: "services"}:                                         true,
	{Resource: "bindings"}:                                         true,
	{Resource: "limitranges"}:                                      true,
	{Resource: "resourcequotas"}:                                   true,
	{Resource: "namespaces"}:                                       true,
	{Resource: "events"}:                                           true,
	{Group: "events.k8s.io", Resource: "events"}:                   true,
	{Group: "discovery.k8s.io", Resource: "endpointslices"}:        true,
	{Group: "apps", Resource: "controllerrevisions"}:               true,
	{Group: "apps", Resource: "daemonsets"}:                        true,
	{Group: "apps", Resource: "deployments"}:                       true,
	{Group: "apps", Resource: "replicasets"}:                       true,
	{Group: "apps", Resource: "statefulsets"}:                      true,
	{Group: "autoscaling", Resource: "horizontalpodautoscalers"}:   true,
	{Group: "batch", Resource: "cronjobs"}:                         true,
	{Group: "batch", Resource: "jobs"}:                             true,
	{Group: "extensions", Resource: "daemonsets"}:                  true,
	{Group: "extensions", Resource: "deployments"}:                 true,
	{Group: "extensions", Resource: "ingresses"}:                   true,
	{Group: "extensions", Resource: "networkpolicies"}:             true,
	{Group: "extensions", Resource: "replicasets"}:                 true,
	{Group: "policy", Resource: "poddisruptionbudgets"}:            true,
	{Group: "networking.k8s.io", Resource: "ingresses"}:            true,
	{Group: "networking.k8s.io", Resource: "networkpolicies"}:      true,
	{Group: "resource.k8s.io", Resource: "resourceclaims"}:         true,
	{Group: "resource.k8s.io", Resource: "resourceclaimtemplates"}: true,
}

// ViewRole makes cs behave as a credential bound to exactly the
// built-in `view` ClusterRole: every get/list/watch of a resource
// outside ViewRoleReads answers 403 the way the API server does. It
// returns a func reporting the refused resources in the order they
// were first asked for, so a test can pin what a command still tries.
func ViewRole(cs *fake.Clientset) (refused func() []schema.GroupResource) {
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
		return true, nil, apierrors.NewForbidden(gr, "", errors.New(`User "system:serviceaccount:lookout:agent" cannot `+action.GetVerb()+` resource "`+gr.Resource+`" in API group "`+gr.Group+`"`))
	}
	for _, verb := range []string{"get", "list"} {
		cs.PrependReactor(verb, "*", deny)
	}
	cs.PrependWatchReactor("*", func(action k8stesting.Action) (bool, watch.Interface, error) {
		gr := action.GetResource().GroupResource()
		if ViewRoleReads[gr] {
			return false, nil, nil
		}
		return true, nil, apierrors.NewForbidden(gr, "", errors.New("cannot watch"))
	})
	return func() []schema.GroupResource {
		mu.Lock()
		defer mu.Unlock()
		return append([]schema.GroupResource(nil), out...)
	}
}
