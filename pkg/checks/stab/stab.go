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

// Package stab implements the `lookout stab` command group
// (DESIGN.md §5): stability reads. `stab drift` detects out-of-band
// edits vs the GitOps manager via managedFields — manager strings by
// default; `--identity` resolves them to audited principals through
// the §2 provider boundary (the §5 identity query pack, issue #128).
// `stab drain` reports everything that will block, or be destroyed
// by, a node drain — a gridlocked PDB IS a drain blocker.
package stab

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// Deps are the injectable dependencies of the stab commands. The
// zero value gives production behavior; tests inject a fake clientset
// and a fixed clock (§13).
type Deps struct {
	// Client builds the Kubernetes client. Nil means kube.BuildClient
	// with default resolution (in-cluster autodetect, then
	// $KUBECONFIG / ~/.kube/config).
	Client func(ctx context.Context) (kubernetes.Interface, error)
	// Now anchors age math (managedFields entry times). Nil means
	// time.Now.
	Now func() time.Time
	// Provider yields the cloud provider for the optional identity
	// enrichment (`stab drift --identity`). Nil means cloud.New
	// default detection — the NoProvider sentinel on vanilla builds,
	// where the flag reports an explicit unavailable, never silence
	// (§2).
	Provider func(ctx context.Context) (cloud.Provider, error)
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
	checks.Register(DriftCommand(Deps{}))
	checks.Register(DrainCommand(Deps{}))
}

// maxListItems caps rendered name/path lists inside a single detail
// value; the numeric count next to it always carries the full total.
const maxListItems = 8

// cappedList joins items with commas, truncating past maxListItems
// with an explicit ",+N more" tail so the value stays one readable
// token, never an unbounded blob.
func cappedList(items []string) string {
	if len(items) <= maxListItems {
		return strings.Join(items, ",")
	}
	return strings.Join(items[:maxListItems], ",") +
		fmt.Sprintf(",+%d more", len(items)-maxListItems)
}

// compactDuration renders d like kubectl ages: "3h20m", "45s", "2h".
// Findings data must be deterministic under a pinned clock, so checks
// format durations themselves (§4.2).
func compactDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	s := d.Truncate(time.Second).String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}

func itoa(n int) string     { return strconv.Itoa(n) }
func itoa32(n int32) string { return strconv.FormatInt(int64(n), 10) }

func plural(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}

// severityRank orders findings critical-first (same convention as
// `triage delta`); unknown severities sink to the bottom.
func severityRank(sev string) int {
	switch sev {
	case emit.SeverityCritical:
		return 0
	case emit.SeverityWarning:
		return 1
	case emit.SeverityInfo:
		return 2
	}
	return 3
}

// sortFindings orders by severity rank, then namespace/name, then
// kind and details for a fully deterministic stream.
func sortFindings(fs []emit.Finding) {
	key := func(f emit.Finding) string {
		var b strings.Builder
		fmt.Fprintf(&b, "%d\x00%s\x00%s\x00%s\x00%s", severityRank(f.Severity), f.Namespace, f.Name, f.Kind, f.KindOfObject)
		for _, d := range f.Details {
			b.WriteString("\x00" + d.Key + "=" + d.Value)
		}
		return b.String()
	}
	sort.Slice(fs, func(i, j int) bool { return key(fs[i]) < key(fs[j]) })
}

// pageLimit is the paged-List page size (§6.3; a one-shot CLI call
// keeps pages small to bound peak memory).
const pageLimit = 500

// listPages drives one paged List to exhaustion: list returns a
// page's items plus the continue token; each is called per item.
func listPages[T any](what string, list func(metav1.ListOptions) ([]T, string, error), each func(*T)) error {
	opts := metav1.ListOptions{Limit: pageLimit}
	for {
		items, cont, err := list(opts)
		if err != nil {
			return fmt.Errorf("listing %s: %w", what, err)
		}
		for i := range items {
			each(&items[i])
		}
		if cont == "" {
			return nil
		}
		opts.Continue = cont
	}
}
