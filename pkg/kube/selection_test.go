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
	"testing"
)

func TestSelectionRoundTrip(t *testing.T) {
	base := context.Background()

	if got := SelectionFrom(base); !got.IsZero() {
		t.Errorf("bare context carries %+v, want the zero selection", got)
	}
	if got := OptionsFrom(base); got != (Options{}) {
		t.Errorf("OptionsFrom(bare) = %+v, want zero Options", got)
	}

	want := Selection{Kubeconfig: "/tmp/kc", Context: "prod"}
	ctx := WithSelection(base, want)
	if got := SelectionFrom(ctx); got != want {
		t.Errorf("SelectionFrom = %+v, want %+v", got, want)
	}
	if got := (OptionsFrom(ctx)); got != (Options{Kubeconfig: "/tmp/kc", Context: "prod"}) {
		t.Errorf("OptionsFrom = %+v, want the selection's two fields", got)
	}
}

// Two invocations against two clusters must not see each other's
// choice — that race is the reason the selection rides the context
// instead of a package-level default.
func TestSelectionsDoNotLeakBetweenContexts(t *testing.T) {
	a := WithSelection(context.Background(), Selection{Context: "prod"})
	b := WithSelection(context.Background(), Selection{Context: "staging"})

	if got := SelectionFrom(a).Context; got != "prod" {
		t.Errorf("first context = %q, want prod", got)
	}
	if got := SelectionFrom(b).Context; got != "staging" {
		t.Errorf("second context = %q, want staging", got)
	}
}

// A zero selection adds nothing to the context, so the common case
// (no flags) does not allocate a value the accessors then have to
// distinguish from a deliberate empty choice.
func TestWithSelectionZeroIsANoOp(t *testing.T) {
	base := context.Background()
	if got := WithSelection(base, Selection{}); got != base {
		t.Error("WithSelection(zero) wrapped the context")
	}
}
