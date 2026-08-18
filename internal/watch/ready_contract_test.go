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

package watch

import (
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/autoscaling"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/degradation"
	"github.com/go-steer/k8s-lookout/pkg/sources/expiry"
	"github.com/go-steer/k8s-lookout/pkg/sources/gateway"
	"github.com/go-steer/k8s-lookout/pkg/sources/ingress"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
	"github.com/go-steer/k8s-lookout/pkg/sources/notifications"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
	"github.com/go-steer/k8s-lookout/pkg/sources/quota"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
	"github.com/go-steer/k8s-lookout/pkg/sources/tokenburn"
	"github.com/go-steer/k8s-lookout/pkg/sources/workload"
)

// TestEverySourceDeclaresItsBarrier holds /readyz to the source set.
//
// The failure this exists to catch is silent and one-directional: a new
// informer-backed source that forgets HasSynced does not break
// anything visibly — it just means the sentinel reports ready while
// that source is still listing, which is precisely the bug #285 fixed
// and precisely the bug nobody notices until an incident is missed
// during a rollout.
//
// The poll-driven half is asserted too, so "no barrier" stays a
// decision rather than an omission: if one of them grows an informer,
// this test fails until someone decides what its readiness means.
func TestEverySourceDeclaresItsBarrier(t *testing.T) {
	t.Parallel()

	// Informer-backed: Run does an initial LIST and arms afterwards, so
	// there is a window in which the source is up and blind.
	informerBacked := map[string]any{
		autoscaling.Name: (*autoscaling.Source)(nil),
		capacity.Name:    (*capacity.Source)(nil),
		degradation.Name: (*degradation.Source)(nil),
		gateway.Name:     (*gateway.Source)(nil),
		ingress.Name:     (*ingress.Source)(nil),
		k8sevents.Name:   (*k8sevents.Source)(nil),
		objectstate.Name: (*objectstate.Source)(nil),
		rollout.Name:     (*rollout.Source)(nil),
		workload.Name:    (*workload.Source)(nil),
	}
	// Poll-driven: no cache to fill, so ready the moment Run is
	// entered. sources.AllSynced treats an absent barrier as ready.
	pollDriven := map[string]any{
		expiry.Name:        (*expiry.Source)(nil),
		notifications.Name: (*notifications.Source)(nil),
		quota.Name:         (*quota.Source)(nil),
		saturation.Name:    (*saturation.Source)(nil),
		tokenburn.Name:     (*tokenburn.Source)(nil),
	}

	for name, src := range informerBacked {
		if _, ok := src.(sources.SyncReporter); !ok {
			t.Errorf("source %q is informer-backed but does not implement sources.SyncReporter: /readyz would report ready while its initial LIST is still running", name)
		}
	}
	for name, src := range pollDriven {
		if _, ok := src.(sources.SyncReporter); ok {
			t.Errorf("source %q implements sources.SyncReporter but is listed here as poll-driven — move it to informerBacked", name)
		}
	}

	// Held to knownSources rather than a hand-kept count, so a source
	// added to the sentinel and not classified here fails immediately
	// instead of quietly opting out of the readiness contract.
	classified := make(map[string]bool, len(informerBacked)+len(pollDriven))
	for name := range informerBacked {
		classified[name] = true
	}
	for name := range pollDriven {
		if classified[name] {
			t.Errorf("source %q is in both lists", name)
		}
		classified[name] = true
	}
	for _, name := range knownSources {
		if !classified[name] {
			t.Errorf("source %q is registrable (knownSources) but unclassified here — decide whether it has an initial-LIST barrier", name)
		}
		delete(classified, name)
	}
	for name := range classified {
		t.Errorf("source %q is classified here but is not in knownSources", name)
	}
}
