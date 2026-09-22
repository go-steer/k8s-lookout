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
	"regexp"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-steer/k8s-lookout/internal/telemetry"
	"github.com/go-steer/k8s-lookout/pkg/sources/computeclass"
	"github.com/go-steer/k8s-lookout/pkg/sources/topologydrift"
)

// MetricDoc documents one sentinel Prometheus metric for generated
// reference surfaces (the docs site's metrics page).
type MetricDoc struct {
	Name   string   // fully-qualified metric name
	Type   string   // counter | gauge | histogram
	Labels []string // variable label names, nil for unlabeled
	Help   string   // the registered help string, verbatim
	// Optional marks a series that is absent from a default scrape
	// because a flag has to turn it on. Rendered on the generated page
	// so a reader who cannot find it does not conclude it is broken.
	Optional bool
}

// MetricsInventory returns every metric the sentinel serves on
// --metrics-addr, in registration order.
//
// Documented choice (docs-site PR): names and help strings are
// DERIVED from the live collectors (newMetrics + Describe), so they
// cannot drift from metrics.go; the type and label columns are
// stamped here per collector because the Prometheus client does not
// expose them pre-observation. The enumeration below is kept
// complete by TestMetricsInventoryComplete, which fails when a field
// is added to either metrics struct without a row here.
//
// The per-runner bundle comes first, then the process-level fleet
// metrics — every per-runner series additionally carries a const
// cluster label, which the fleet ones cannot (see fleetMetrics) — and
// last the leeway block, which is the one part of this page that cannot
// be derived: those instruments are declared on the OpenTelemetry API
// and reach the same registry through a bridge, so there is no
// Collector to Describe. They are owned, and pinned against the real
// exporter, by pkg/sources/topologydrift.MetricDocs and
// pkg/sources/computeclass.MetricDocs.
func MetricsInventory() []MetricDoc {
	m := newMetrics()
	fm := newFleetMetrics(prometheus.NewRegistry())
	rows := []struct {
		c      prometheus.Collector
		typ    string
		labels []string
	}{
		{m.eventsSeen, "counter", []string{"reason", "namespace"}},
		{m.eventsInjected, "counter", []string{"reason", "namespace"}},
		{m.eventsDedupSuppress, "counter", []string{"reason", "namespace"}},
		{m.eventsFiltered, "counter", []string{"gate"}},
		{m.injectErrors, "counter", []string{"reason", "http_code"}},
		{m.injectShrinks, "counter", []string{"shed"}},
		{m.sessionCreates, "counter", []string{"outcome"}},
		{m.activeIncidents, "gauge", nil},
		{m.recoveriesObserved, "counter", []string{"resolution"}},
		{m.recoveriesReverted, "counter", nil},
		{m.recoveryTracking, "gauge", nil},
		{m.recoveryDrops, "counter", []string{"cause"}},
		{m.stormsFormed, "counter", nil},
		{m.stormsResolved, "counter", nil},
		{m.stormsActive, "gauge", nil},
		{m.stormMembers, "counter", []string{"kind"}},
		{m.stormUpdates, "counter", nil},
		{m.watchboardEntries, "counter", []string{"kind"}},
		{m.watchboardDigests, "counter", nil},
		{m.watchboardRotations, "counter", nil},
		{m.watchboardBuffered, "gauge", nil},
		{m.watchboardReattached, "counter", []string{"kind"}},
		{m.infoDropped, "counter", []string{"kind"}},
		{m.findings, "counter", []string{"kind", "severity"}},
		{m.storeRecords, "counter", []string{"route"}},
		{m.storeDrops, "counter", []string{"cause"}},
		{m.storePruned, "counter", []string{"cause"}},
		{m.enrichments, "counter", []string{"outcome"}},
		{m.enrichmentBytes, "histogram", nil},
		{m.enrichmentTruncated, "counter", nil},
		{m.enrichmentFailures, "counter", []string{"stage"}},
		{m.memoryFacts, "counter", []string{"class"}},
		{m.distillErrors, "counter", nil},
		{m.triageOverrides, "counter", []string{"action"}},
		{m.triageFlips, "counter", nil},
		{m.triageRegressed, "counter", nil},
		{m.crossSourceFollowups, "counter", []string{"source"}},
		{m.sinkInfo, "gauge", []string{"sink"}},
		{m.runnerUp, "gauge", nil},
		{m.runnerRestarts, "counter", nil},
		{m.runnerTerminal, "gauge", []string{"reason"}},
		{m.sourceDenied, "gauge", []string{"source", "resource", "required"}},
		{fm.clusterResolveErrors, "counter", []string{"cluster", "cause"}},
	}
	out := make([]MetricDoc, 0, len(rows)+len(topologydrift.MetricDocs())+len(computeclass.MetricDocs()))
	for _, r := range rows {
		name, help := describeCollector(r.c)
		out = append(out, MetricDoc{Name: name, Type: r.typ, Labels: r.labels, Help: help})
	}
	for _, d := range topologydrift.MetricDocs() {
		out = append(out, MetricDoc{
			Name:     d.Name,
			Type:     d.Type,
			Labels:   d.Labels,
			Help:     d.Help,
			Optional: d.Optional,
		})
	}
	// §7.7's preference block, bridged the same way. It carries no
	// Optional rows: none of these is behind a flag — the whole source is
	// behind a CRD instead, and a page that said "opt-in" would send the
	// reader looking for a flag that does not exist.
	for _, d := range computeclass.MetricDocs() {
		out = append(out, MetricDoc{Name: d.Name, Type: d.Type, Labels: d.Labels, Help: d.Help})
	}
	// §8.4's OTLP export SLIs. Plain collectors like the sentinel's own
	// — so the name and help are derived, not written twice — but owned
	// by internal/telemetry, because that is where the reader they
	// describe is built. All optional: unlike the leeway series, which
	// read zero to prove they are looking, these describe a push
	// pipeline that does not exist until --otel-exporter=otlp builds
	// one, and a zeroed export counter on a process with no exporter
	// would claim a path that was never wired.
	for _, d := range telemetry.ExportSLIDocs() {
		name, help := describeCollector(d.Collector)
		out = append(out, MetricDoc{Name: name, Type: d.Type, Labels: d.Labels, Help: help, Optional: true})
	}
	return out
}

// descRe extracts fqName and help from prometheus.Desc's String()
// form, which is client_golang's only way out: Desc's fields are
// unexported and it exposes no accessors.
//
//	Desc{fqName: "...", help: "...", unit: "", constLabels: {...}, ...}
//
// The `unit` field is optional here because client_golang added it
// between v1.19 and v1.24; matching it optionally means the parse
// survives both. Each field is matched as a run of non-quote
// characters rather than greedily to the last delimiter — a greedy
// help match swallows the intervening `", unit: "` and appends it to
// every rendered help string (which is exactly what happened when
// the unit field appeared). Help strings registered in metrics.go
// contain no double quotes, an invariant TestDescRegexpParsesHelp
// enforces so this stays exact.
var descRe = regexp.MustCompile(`^Desc\{fqName: "([^"]*)", help: "([^"]*)"(?:, unit: "[^"]*")?, constLabels: `)

// describeCollector returns the single Desc a sentinel collector
// declares (every metric here is one Desc — vectors included).
func describeCollector(c prometheus.Collector) (name, help string) {
	ch := make(chan *prometheus.Desc, 2)
	c.Describe(ch)
	close(ch)
	d := <-ch
	if d == nil {
		return "", ""
	}
	m := descRe.FindStringSubmatch(d.String())
	if m == nil {
		return d.String(), ""
	}
	return m[1], m[2]
}
