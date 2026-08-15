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

// Package audit implements the `lookout audit` command group
// (docs/fleet-audit-detectors-design.md, epic #182): deterministic
// best-practice POSTURE checks, as distinct from the incident groups
// that report what is currently broken.
//
// # Why posture is its own group and not its own binary
//
// A posture finding makes a different claim from every other group in
// this tree. `triage`, `state`, and `stab` say "something is wrong
// right now"; `audit` says "nothing is wrong right now, and there is no
// safety net for when it is". Those answer different questions, carry
// different urgency, and are read by different consumers, so they are
// separated — but by GROUP, not by binary.
//
// A sibling binary was the alternative and costs a second release
// artifact, a second image, a second RBAC surface, a second deploy, and
// a forked doc generator, permanently. The isolation ladder is
// group → build tag → separate binary, and every rung above the first
// is still walkable later if posture turns out to need it. Starting at
// the top rung is not.
//
// # The detectors do not share a runtime
//
// Unlike `triage`, the audit commands have no common cluster client or
// shared scan pass: a posture detector reads whatever API objects its
// own claim needs. What they DO share is the §4.2 envelope, the
// exemption seam (pkg/exempt, wired into emit.Writer for every command
// in the tree, not just this group), and the posture fingerprint recipe
// (engine.PostureFingerprint). Those three are the group's actual
// contract; see docs/audit-ingestion-contract.md for how a consumer
// ingests what comes out.
package audit

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// Deps are the injectable dependencies of the audit commands. The
// zero value gives production behavior; tests inject a fake clientset
// and a fake provider (§13). Not every command needs either —
// `audit exemptions` reads a file, `audit cluster` reads the cloud and
// no cluster objects at all — which is the "no shared runtime" point
// above made concrete.
type Deps struct {
	// Client builds the Kubernetes client. Nil means kube.BuildClient
	// with default resolution (in-cluster autodetect, then
	// $KUBECONFIG / ~/.kube/config).
	Client func(ctx context.Context) (kubernetes.Interface, error)
	// Provider yields the cloud provider. Nil means cloud.New default
	// detection (the NoProvider sentinel on vanilla builds — the
	// cloud-backed commands then report unavailable, never silence,
	// §2).
	Provider func(ctx context.Context) (cloud.Provider, error)
	// Now is the clock. Nil means time.Now. Only the claims that are
	// about a moment need it: `audit upgrades` asks which maintenance
	// exclusions are in force right now.
	Now func() time.Time
}

func (d Deps) client(ctx context.Context) (kubernetes.Interface, error) {
	if d.Client != nil {
		return d.Client(ctx)
	}
	return kube.BuildClient(kube.Options{})
}

func (d Deps) provider(ctx context.Context) (cloud.Provider, error) {
	if d.Provider != nil {
		return d.Provider(ctx)
	}
	return cloud.New(ctx, cloud.Config{})
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func init() {
	checks.Register(ClusterCommand(Deps{}))
	checks.Register(ExemptionsCommand())
	checks.Register(HardeningCommand(Deps{}))
	checks.Register(NetpolCommand(Deps{}))
	checks.Register(UpgradesCommand(Deps{}))
	checks.Register(WorkloadsCommand(Deps{}))
}

// maxListItems caps rendered name lists inside a single detail value;
// the count beside it always carries the full total.
const maxListItems = 8

// cappedList joins items with commas, truncating past maxListItems
// with an explicit ",+N more" tail so the value stays one readable
// token rather than an unbounded blob.
func cappedList(items []string) string {
	if len(items) <= maxListItems {
		return strings.Join(items, ",")
	}
	return strings.Join(items[:maxListItems], ",") +
		fmt.Sprintf(",+%d more", len(items)-maxListItems)
}

func itoa(n int) string { return strconv.Itoa(n) }

func plural(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}
