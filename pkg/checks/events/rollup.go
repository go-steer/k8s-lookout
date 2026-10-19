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

package events

// Rolling up a namespace or cluster-wide timeline (#747).
//
// The timeline collapses repeats per object, so a busy cluster's hour
// is one entry per pod per reason: measured on an Autopilot cluster
// cycling nodes, 3,350 entries and 1 MB, 87% of it the same five
// lifecycle reasons for each system DaemonSet pod on each new node.
// No model can use that, and MCP clients cut it off unseen.
//
// Namespace and -A timelines therefore roll up entries that share an
// owner and a reason family: pods by the owner their generated name
// encodes (a DaemonSet's <name>-xxxxx, a Deployment's
// <name>-<hash>-xxxxx, a StatefulSet's <name>-<ordinal>; the pod may be
// gone, so its name is all there is), Nodes and anything else by kind.
// A group rolls up only when it is big enough to matter — three objects
// for Normal events, five for Warning, whose names matter more and which keep five names, so a group at the threshold still names every object — so a
// small timeline reads exactly as before. A rolled-up entry keeps the
// kind, the summed count and the first/last seen, and adds objects= and
// a few example names. Workload timelines are never rolled up (their
// per-pod detail is the point), and --per-object turns rollup off.

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

const (
	rollupNormalMin  = 3 // objects before Normal entries roll up
	rollupWarningMin = 5 // objects before Warning entries roll up
	examplesNormal   = 3 // names kept on a rolled-up Normal entry
	examplesWarning  = 5 // names kept on a rolled-up Warning entry
)

// Kubernetes' generated-name alphabet (k8s.io/apimachinery/pkg/util/rand):
// no vowels, no 0, 1 or 3, so a suffix is unlikely to be a real word.
const genAlphabet = "[bcdfghjklmnpqrstvwxz2456789]"

var (
	// <owner>-xxxxx: a DaemonSet's, Job's or ReplicaSet's pod.
	podSuffix = regexp.MustCompile(`-` + genAlphabet + `{5}$`)
	// <deployment>-<pod-template-hash> left after the pod suffix.
	templateHash = regexp.MustCompile(`-` + genAlphabet + `{6,10}$`)
	// <statefulset>-<ordinal>, or a CronJob's <cronjob>-<minutes>.
	ordinal = regexp.MustCompile(`-[0-9]+$`)
)

// podOwner is the owner a pod's generated name encodes, or "" when the
// name encodes none.
func podOwner(name string) string {
	switch {
	case podSuffix.MatchString(name):
		owner := podSuffix.ReplaceAllString(name, "")
		owner = templateHash.ReplaceAllString(owner, "")
		return ordinal.ReplaceAllString(owner, "")
	case ordinal.MatchString(name):
		return ordinal.ReplaceAllString(name, "")
	}
	return ""
}

// groupKey is a rollup group: same namespace, kind, owner, reason and
// event type.
type groupKey struct {
	namespace, kind, owner, reason string
	warning                        bool
}

func keyOf(en *entry) (groupKey, bool) {
	k := groupKey{namespace: en.namespace, kind: en.kind, reason: en.reason, warning: en.warning}
	switch en.kind {
	case "Pod":
		k.owner = podOwner(en.name)
		return k, k.owner != ""
	case "Node":
		return k, true
	}
	return k, false
}

// rollup turns the chronological entries into timeline findings, merging
// big enough groups into one entry each, still ordered by newest
// activity.
func rollup(entries []*entry) []emit.Finding {
	groups := map[groupKey][]*entry{}
	for _, en := range entries {
		if k, ok := keyOf(en); ok {
			groups[k] = append(groups[k], en)
		}
	}
	type row struct {
		last time.Time
		f    emit.Finding
	}
	rows := make([]row, 0, len(entries))
	done := map[groupKey]bool{}
	for _, en := range entries {
		k, ok := keyOf(en)
		members := groups[k]
		minimum := rollupNormalMin
		if en.warning {
			minimum = rollupWarningMin
		}
		if !ok || len(members) < minimum {
			rows = append(rows, row{en.last, en.finding()})
			continue
		}
		if done[k] {
			continue
		}
		done[k] = true
		f, last := merged(k, members)
		rows = append(rows, row{last, f})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].last.Before(rows[j].last) })
	out := make([]emit.Finding, len(rows))
	for i := range rows {
		out[i] = rows[i].f
	}
	return out
}

// merged is one group's entry: the newest member's message and source,
// counts summed, the widest first/last, the variants combined, and the
// newest members named.
func merged(k groupKey, members []*entry) (emit.Finding, time.Time) {
	sum := &entry{
		namespace: k.namespace, kind: k.kind, reason: k.reason, warning: k.warning,
		variants: map[string]struct{}{}, first: members[0].first, last: members[0].last,
	}
	for _, m := range members {
		sum.count += m.count
		for v := range m.variants {
			sum.variants[v] = struct{}{}
		}
		if m.first.Before(sum.first) {
			sum.first = m.first
		}
		if !m.last.Before(sum.last) {
			sum.last, sum.message, sum.source = m.last, m.message, m.source
		}
	}
	sum.name = k.owner + "-*"
	if k.kind == "Node" {
		sum.name = "*"
	}
	f := sum.finding()

	byNewest := append([]*entry(nil), members...)
	sort.SliceStable(byNewest, func(i, j int) bool { return byNewest[i].last.After(byNewest[j].last) })
	keep := examplesNormal
	if k.warning {
		keep = examplesWarning
	}
	names := make([]string, 0, keep)
	for i := 0; i < len(byNewest) && i < keep; i++ {
		names = append(names, byNewest[i].name)
	}
	f.Details = append(f.Details,
		emit.Field{Key: "objects", Value: strconv.Itoa(len(members))},
		emit.Field{Key: "examples", Value: strings.Join(names, ",")},
	)
	f.Message = fmt.Sprintf("%d %ss: %s", len(members), strings.ToLower(k.kind), f.Message)
	return f, sum.last
}
