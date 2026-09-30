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

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// The four RBAC Lists, exported so that `audit rbac` (#184) reads the
// objects through the same paged calls `state edges` indexes them with
// (docs/fleet-audit-detectors-design.md decision 4: share the loader,
// not the group). Each drives one paged List to exhaustion and hands
// every item to each; ns is NamespaceAll for a cluster-wide read and is
// ignored by the two cluster-scoped kinds.
//
// They stay four calls rather than one "load RBAC" because listCluster
// accounts for skips per (group, resource) — a caller allowed to list
// RoleBindings but not ClusterRoleBindings gets a partial index with
// the gap named, which a single combined loader could not express.

// ListRoleBindings pages through the RoleBindings in ns.
func ListRoleBindings(ctx context.Context, client kubernetes.Interface, ns string, each func(*rbacv1.RoleBinding)) error {
	return listPages("rolebindings", func(o metav1.ListOptions) ([]rbacv1.RoleBinding, string, error) {
		l, err := client.RbacV1().RoleBindings(ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, each)
}

// ListRoles pages through the Roles in ns.
func ListRoles(ctx context.Context, client kubernetes.Interface, ns string, each func(*rbacv1.Role)) error {
	return listPages("roles", func(o metav1.ListOptions) ([]rbacv1.Role, string, error) {
		l, err := client.RbacV1().Roles(ns).List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, each)
}

// ListClusterRoleBindings pages through every ClusterRoleBinding.
func ListClusterRoleBindings(ctx context.Context, client kubernetes.Interface, each func(*rbacv1.ClusterRoleBinding)) error {
	return listPages("clusterrolebindings", func(o metav1.ListOptions) ([]rbacv1.ClusterRoleBinding, string, error) {
		l, err := client.RbacV1().ClusterRoleBindings().List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, each)
}

// ListClusterRoles pages through every ClusterRole.
func ListClusterRoles(ctx context.Context, client kubernetes.Interface, each func(*rbacv1.ClusterRole)) error {
	return listPages("clusterroles", func(o metav1.ListOptions) ([]rbacv1.ClusterRole, string, error) {
		l, err := client.RbacV1().ClusterRoles().List(ctx, o)
		if err != nil {
			return nil, "", err
		}
		return l.Items, l.Continue, nil
	}, each)
}
