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

package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-steer/k8s-lookout/internal/version"
	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// emitter is this binary's replacement for the sentinel's inject path.
// A source emits a Signal; here that becomes a counter increment and a
// line on stderr, and nothing else happens to it.
//
// The counter rather than a gauge is deliberate. A leeway finding is
// already a state — the source's own gauges say which subjects are in
// breach right now — so a second gauge saying the same thing would be
// two answers to one question. What is NOT otherwise recoverable is how
// often the subsystem decided something was worth saying, which is a
// rate, and a rate wants a counter.
type emitter struct {
	signals *prometheus.CounterVec
	log     io.Writer
}

// The metric names are spelled out rather than built from a shared
// prefix constant: they are a contract with whatever scrapes this, and
// grep for the literal name is how somebody finds them.
func newEmitter(reg prometheus.Registerer, log io.Writer) (*emitter, error) {
	e := &emitter{
		log: log,
		signals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lookout_leeway_standalone_signals_total",
			Help: "Total findings the leeway sources raised in this standalone process, by source, kind and severity. This is a rate, not a state: the sources' own gauges say which subjects are in breach now, and this says how often the subsystem decided one was worth saying. It has no sentinel counterpart — under lookout watch the same findings go to the inject path and are counted there.",
		}, []string{"source", "kind", "severity"}),
	}
	info := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lookout_leeway_standalone_info",
		Help: "Always 1, labelled with the version of the standalone leeway binary exporting this endpoint. Its presence is the signal: a scrape target carrying it is the metrics-only deployment and not lookout watch, which matters because the two must not run against one cluster and a dashboard cannot otherwise tell them apart.",
	}, []string{"version"})
	if err := reg.Register(e.signals); err != nil {
		return nil, fmt.Errorf("registering the signals counter: %w", err)
	}
	if err := reg.Register(info); err != nil {
		return nil, fmt.Errorf("registering the info gauge: %w", err)
	}
	info.WithLabelValues(version.Semver()).Set(1)
	return e, nil
}

// emit is the sources' emit callback. It is called from source
// goroutines, so everything it touches has to be safe for concurrent
// use: the counter is, and the stderr write is one Fprintf, which is
// one Write on an *os.File.
func (e *emitter) emit(sig sources.Signal) {
	e.signals.WithLabelValues(sig.Source, sig.Kind, string(sig.Severity)).Inc()
	fmt.Fprintf(e.log, "leeway: %s %s %s: %s\n",
		sig.Severity, sig.Kind, subjectOf(sig), oneLine(sig.Message))
}

// subjectOf names what the finding is about, in the shape an operator
// would type it back into kubectl. Leeway subjects are frequently
// cluster-scoped — a node pool, or a topology domain — so the
// namespace is omitted rather than rendered as an empty segment.
func subjectOf(sig sources.Signal) string {
	kind := sig.KindOfObject
	if kind == "" {
		kind = "object"
	}
	if sig.Namespace == "" {
		return fmt.Sprintf("%s/%s", strings.ToLower(kind), sig.Name)
	}
	return fmt.Sprintf("%s/%s/%s", sig.Namespace, strings.ToLower(kind), sig.Name)
}

// oneLine keeps a finding to a single stderr line. A message that
// wrapped would break every `grep leeway:` an operator writes against
// this output, which is the only consumer it has.
func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}
