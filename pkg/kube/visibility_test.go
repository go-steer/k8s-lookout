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

package kube

import (
	"context"
	"errors"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// reviewing returns a CallerVisibility over a fake clientset that answers
// pod-list access reviews from allowed (namespace → allowed; "" is
// cluster-wide), counting the reviews asked.
func reviewing(allowed map[string]bool, fail bool) (*CallerVisibility, *int) {
	cs := fake.NewClientset()
	calls := 0
	cs.PrependReactor("create", "selfsubjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if fail {
			return true, nil, errors.New("apiserver unavailable")
		}
		r := a.(k8stesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
		attrs := r.Spec.ResourceAttributes
		if attrs.Verb != "list" || attrs.Resource != "pods" {
			return true, nil, errors.New("unexpected review")
		}
		r.Status.Allowed = allowed[attrs.Namespace]
		return true, r, nil
	})
	v := &CallerVisibility{
		ctx:    context.Background(),
		client: func() (kubernetes.Interface, error) { return cs, nil },
		byName: map[string]bool{},
	}
	return v, &calls
}

func TestCallerVisibilityClusterWideReadSeesEverything(t *testing.T) {
	t.Parallel()
	v, calls := reviewing(map[string]bool{"": true}, false)
	for _, ns := range []string{"", "team-a", "team-b", "kube-system"} {
		if !v.Visible(ns) {
			t.Errorf("cluster-wide read: %q hidden", ns)
		}
	}
	if *calls != 1 {
		t.Errorf("%d reviews, want 1: cluster-wide read settles everything", *calls)
	}
}

func TestCallerVisibilityPerNamespace(t *testing.T) {
	t.Parallel()
	v, calls := reviewing(map[string]bool{"team-a": true}, false)
	if !v.Visible("team-a") || v.Visible("team-b") {
		t.Error("per-namespace access not applied")
	}
	if v.Visible("") {
		t.Error("a cluster-scoped row was shown without cluster-wide read")
	}
	// Cached: asking again costs nothing.
	before := *calls
	_ = v.Visible("team-a")
	_ = v.Visible("team-b")
	if *calls != before {
		t.Errorf("answers were not cached: %d more reviews", *calls-before)
	}
}

func TestCallerVisibilityFailsClosed(t *testing.T) {
	t.Parallel()
	v, _ := reviewing(nil, true)
	if v.Visible("team-a") || v.Visible("") {
		t.Error("a failed review showed the row")
	}
	broken := &CallerVisibility{ctx: context.Background(), byName: map[string]bool{},
		client: func() (kubernetes.Interface, error) { return nil, errors.New("no client") }}
	if broken.Visible("team-a") {
		t.Error("no client, yet the row was shown")
	}
}

func TestCallerName(t *testing.T) {
	t.Parallel()
	if CallerNameFrom(context.Background()) != "" {
		t.Error("a bare context named a caller")
	}
	if ctx := WithCallerName(context.Background(), ""); ctx != context.Background() {
		t.Error("WithCallerName(\"\") changed the context")
	}
	if got := CallerNameFrom(WithCallerName(context.Background(), "dana@example.com")); got != "dana@example.com" {
		t.Errorf("CallerNameFrom = %q", got)
	}
}
