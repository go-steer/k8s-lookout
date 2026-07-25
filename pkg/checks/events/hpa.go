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

// HPA thrash detection (§5, absorbing v2's hpa-loop-catcher). The
// HPA object keeps NO replica history — status carries only the
// current/desired counts and a lastScaleTime — so an oscillation can
// only be recovered from the controller's SuccessfulRescale events,
// whose messages carry the one datum that survives: "New size: N".
//
// Reconstruction caveat, honestly stated: k8s aggregates repeated
// identical events into one Event object with a bumped count, so a
// loop that alternates between exactly two messages yields two Event
// objects, not 2N. Each object contributes its first AND last
// activity timestamps as sequence points (when they differ), which
// recovers the oscillation envelope; sequences whose every rescale
// carries a distinct size reconstruct exactly.

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// rescale is one SuccessfulRescale event, reduced to what the
// analysis needs.
type rescale struct {
	namespace, name string // the HPA
	uid             string
	message         string
	first, last     time.Time
	count           int
}

func newRescale(ev *corev1.Event, first, last time.Time) rescale {
	return rescale{
		namespace: ev.InvolvedObject.Namespace,
		name:      ev.InvolvedObject.Name,
		uid:       string(ev.InvolvedObject.UID),
		message:   ev.Message,
		first:     first,
		last:      last,
		count:     int(ev.Count),
	}
}

// newSizeRe extracts the replica count from the HPA controller's
// message format ("New size: 3; reason: cpu resource utilization …"),
// unchanged since autoscaling/v1.
var newSizeRe = regexp.MustCompile(`New size: (\d+)`)

// sizePoint is one (time, replicas) sample of the recovered
// sequence.
type sizePoint struct {
	t    time.Time
	size int
}

// thrashFindings analyzes the scope's SuccessfulRescale events per
// HPA and reports event.hpa_thrash where the replica count changed
// direction (up→down or down→up) at least minFlips times inside one
// window. A monotonic ramp — however fast — never fires: it has zero
// direction changes.
func thrashFindings(rescales []rescale, hpaTargets map[string]string, window time.Duration, minFlips int) []emit.Finding {
	byHPA := map[string][]rescale{}
	for _, r := range rescales {
		key := r.uid
		if key == "" {
			key = r.namespace + "/" + r.name
		}
		byHPA[key] = append(byHPA[key], r)
	}
	keys := make([]string, 0, len(byHPA))
	for k := range byHPA {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out []emit.Finding
	for _, k := range keys {
		if f, ok := analyzeHPA(byHPA[k], hpaTargets, window, minFlips); ok {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func analyzeHPA(rs []rescale, hpaTargets map[string]string, window time.Duration, minFlips int) (emit.Finding, bool) {
	var points []sizePoint
	for _, r := range rs {
		m := newSizeRe.FindStringSubmatch(r.message)
		if m == nil {
			continue
		}
		size, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		points = append(points, sizePoint{t: r.last, size: size})
		if r.count > 1 && r.first.Before(r.last) {
			// The aggregated event's earliest occurrence is a
			// sequence point too (see the package-top caveat).
			points = append(points, sizePoint{t: r.first, size: size})
		}
	}
	if len(points) < 3 {
		// Fewer than three sizes cannot contain a direction change
		// bracketed on both sides (up→down→up needs 4, up→down
		// needs 3 for even one flip).
		return emit.Finding{}, false
	}
	sort.Slice(points, func(i, j int) bool {
		if !points[i].t.Equal(points[j].t) {
			return points[i].t.Before(points[j].t)
		}
		return points[i].size < points[j].size
	})

	// Collapse consecutive equal sizes, then walk directions: a flip
	// is a sign change between consecutive scale moves, timestamped
	// at the move that reversed course.
	sequence := []int{points[0].size}
	var flipTimes []time.Time
	prevDir := 0
	for i := 1; i < len(points); i++ {
		d := points[i].size - points[i-1].size
		if d == 0 {
			continue
		}
		sequence = append(sequence, points[i].size)
		dir := 1
		if d < 0 {
			dir = -1
		}
		if prevDir != 0 && dir != prevDir {
			flipTimes = append(flipTimes, points[i].t)
		}
		prevDir = dir
	}

	// Most flips inside any single --hpa-window span (two pointers
	// over the chronologically ordered flip times).
	best := 0
	lo := 0
	for hi := range flipTimes {
		for flipTimes[hi].Sub(flipTimes[lo]) > window {
			lo++
		}
		if n := hi - lo + 1; n > best {
			best = n
		}
	}
	if best < minFlips {
		return emit.Finding{}, false
	}

	seq := make([]string, len(sequence))
	for i, s := range sequence {
		seq[i] = strconv.Itoa(s)
	}
	r0 := rs[0]
	f := emit.Finding{
		Kind:         "event.hpa_thrash",
		Severity:     emit.SeverityWarning,
		Namespace:    r0.namespace,
		KindOfObject: hpaKind,
		Name:         r0.name,
		Reason:       "HPAThrash",
		Message: fmt.Sprintf("replica count changed direction %d times within %s — the HPA is oscillating, not converging",
			best, window),
		Details: []emit.Field{
			{Key: "replicas", Value: strings.Join(seq, "->")},
			{Key: "flips", Value: strconv.Itoa(best)},
			{Key: "window", Value: window.String()},
		},
	}
	if target := hpaTargets[r0.namespace+"/"+r0.name]; target != "" {
		f.Details = append(f.Details, emit.Field{Key: "target", Value: target})
	}
	return f, true
}
