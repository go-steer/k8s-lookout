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
)

// MetricDoc documents one sentinel Prometheus metric for generated
// reference surfaces (the docs site's metrics page).
type MetricDoc struct {
	Name   string   // fully-qualified metric name
	Type   string   // counter | gauge | histogram
	Labels []string // variable label names, nil for unlabeled
	Help   string   // the registered help string, verbatim
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
// is added to the metrics struct without a row here.
func MetricsInventory() []MetricDoc {
	m := newMetrics()
	rows := []struct {
		c      prometheus.Collector
		typ    string
		labels []string
	}{
		{m.eventsSeen, "counter", []string{"reason", "namespace"}},
		{m.eventsInjected, "counter", []string{"reason", "namespace"}},
		{m.eventsDedupSuppress, "counter", []string{"reason", "namespace"}},
		{m.injectErrors, "counter", []string{"reason", "http_code"}},
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
		{m.infoDropped, "counter", []string{"kind"}},
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
	}
	out := make([]MetricDoc, 0, len(rows))
	for _, r := range rows {
		name, help := describeCollector(r.c)
		out = append(out, MetricDoc{Name: name, Type: r.typ, Labels: r.labels, Help: help})
	}
	return out
}

// descRe extracts fqName and help from prometheus.Desc's String()
// form: Desc{fqName: "...", help: "...", constLabels: {...}, ...}.
// Help strings registered in metrics.go contain no double quotes;
// the anchored ", constLabels:" delimiter keeps the match exact.
var descRe = regexp.MustCompile(`^Desc\{fqName: "([^"]+)", help: "(.*)", constLabels: `)

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
