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

package logs

// The distillation engine: one shared cluster space fed by
// per-container streams. Streams keep their own stack-trace detector
// state (traces never interleave inside one container stream);
// everything downstream — probe stripping, Drain clustering, stack
// clustering — merges across pods, so a template shared by N pods is
// one cluster with pods=N, and the varying pod-specific tokens decay
// into wildcards.

import (
	"sort"
	"strings"
	"time"
)

type engine struct {
	stripProbes bool
	tree        *drainTree
	stacks      map[string]*stackCluster
	stackOrder  []*stackCluster
	lines       int // every raw line read (the summary's scanned count)
	probes      int // probe-noise lines stripped
}

func newEngine(stripProbes bool) *engine {
	return &engine{
		stripProbes: stripProbes,
		tree:        newDrainTree(),
		stacks:      map[string]*stackCluster{},
	}
}

// stream returns a line sink for one container stream of one pod.
// Call close when the stream ends to flush detector state.
type stream struct {
	e   *engine
	pod podKey
	det stackDetector
}

func (e *engine) stream(namespace, pod string) *stream {
	return &stream{e: e, pod: podKey{namespace: namespace, pod: pod}}
}

func (s *stream) add(raw string) {
	s.e.lines++
	ts, text := splitTimestamp(raw)
	release, done := s.det.feed(entry{ts: ts, text: text})
	s.dispatch(release, done)
}

func (s *stream) close() {
	release, done := s.det.flush()
	s.dispatch(release, done)
}

func (s *stream) dispatch(release []entry, done *trace) {
	for _, en := range release {
		s.e.addLine(s.pod, en)
	}
	if done != nil {
		s.e.addTrace(s.pod, done)
	}
}

func (e *engine) addLine(pod podKey, en entry) {
	if strings.TrimSpace(en.text) == "" {
		return
	}
	if e.stripProbes && isProbeNoise(en.text) {
		e.probes++
		return
	}
	e.tree.add(pod, en)
}

func (e *engine) addTrace(pod podKey, tr *trace) {
	key := tr.lang + "\x00" + strings.Join(tr.frames, "\x00")
	c, ok := e.stacks[key]
	if !ok {
		c = &stackCluster{
			lang:     tr.lang,
			frames:   tr.frames,
			template: strings.Join(tokenizeMask(tr.head), " "),
			pods:     map[podKey]struct{}{},
			sample:   truncate(tr.head, maxSample),
		}
		e.stacks[key] = c
		e.stackOrder = append(e.stackOrder, c)
	}
	c.count++
	c.pods[pod] = struct{}{}
	if !tr.first.IsZero() && (c.first.IsZero() || tr.first.Before(c.first)) {
		c.first = tr.first
	}
	if !tr.last.IsZero() && (c.last.IsZero() || tr.last.After(c.last)) {
		c.last = tr.last
	}
}

// result is one cluster normalized for emission.
type result struct {
	stack    bool
	lang     string
	frames   []string
	template string
	count    int
	pods     map[podKey]struct{}
	first    time.Time
	last     time.Time
	sample   string
	level    int
}

// errorish drives the primary output ordering: error-ish clusters
// first (§5), then count.
func (r result) errorish() bool { return r.stack || r.level >= levelError }

// results returns every cluster, ordered: error-ish first, then by
// count descending, then by template for determinism.
func (e *engine) results() []result {
	out := make([]result, 0, len(e.tree.all)+len(e.stackOrder))
	for _, c := range e.tree.all {
		out = append(out, result{
			template: strings.Join(c.template, " "),
			count:    c.count,
			pods:     c.pods,
			first:    c.first,
			last:     c.last,
			sample:   c.sample,
			level:    c.level,
		})
	}
	for _, c := range e.stackOrder {
		lvl := levelError
		if c.lang == "go" {
			// A Go panic/fatal error always kills the process.
			lvl = levelFatal
		}
		out = append(out, result{
			stack:    true,
			lang:     c.lang,
			frames:   c.frames,
			template: c.template,
			count:    c.count,
			pods:     c.pods,
			first:    c.first,
			last:     c.last,
			sample:   c.sample,
			level:    lvl,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].errorish() != out[j].errorish() {
			return out[i].errorish()
		}
		if out[i].count != out[j].count {
			return out[i].count > out[j].count
		}
		return out[i].template < out[j].template
	})
	return out
}
