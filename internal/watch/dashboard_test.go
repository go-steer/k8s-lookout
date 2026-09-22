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
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// The shipped Grafana dashboard is the one artefact in this repo that
// names metrics and labels in free text nothing compiles. A rename
// anywhere in the leeway instrument set would leave it rendering empty
// panels and saying nothing about why, which is the exact failure the
// dashboard's own first row exists to prevent it committing.
const dashboardPath = "../../deploy/dashboards/leeway.json"

// Labels every leeway series carries but no MetricDoc row lists: the
// const cluster label the runner stamps on, and the histogram bucket
// boundary.
var dashboardImplicitLabels = []string{"cluster", "le"}

type dashboard struct {
	Panels []struct {
		ID      int    `json:"id"`
		Title   string `json:"title"`
		Type    string `json:"type"`
		GridPos struct {
			H, W, X, Y int
		} `json:"gridPos"`
		Targets []struct {
			Expr    string `json:"expr"`
			Instant bool   `json:"instant"`
		} `json:"targets"`
	} `json:"panels"`
	Templating struct {
		List []struct {
			Name       string `json:"name"`
			Definition string `json:"definition"`
		} `json:"list"`
	} `json:"templating"`
}

func loadDashboard(t *testing.T) (dashboard, string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(dashboardPath))
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	var d dashboard
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("dashboard is not valid JSON: %v", err)
	}
	return d, string(raw)
}

// metricRefs matches a metric name and, when present, the selector
// immediately after it.
var metricRefs = regexp.MustCompile(`(lookout_[a-z0-9_]+)(?:\{([^}]*)\})?`)

// selectorLabels matches the label name in one matcher of a selector.
var selectorLabels = regexp.MustCompile(`([a-z_][a-z0-9_]*)\s*(?:=~|!~|!=|=)`)

func TestDashboardNamesOnlyMetricsWeServe(t *testing.T) {
	_, raw := loadDashboard(t)

	labels := map[string][]string{}
	for _, m := range MetricsInventory() {
		labels[m.Name] = m.Labels
	}

	seen := map[string]bool{}
	for _, m := range metricRefs.FindAllStringSubmatch(raw, -1) {
		name := strings.TrimSuffix(m[1], "_bucket")
		known, ok := labels[name]
		if !ok {
			t.Errorf("dashboard queries %q, which the sentinel does not serve", m[1])
			continue
		}
		seen[name] = true
		for _, l := range selectorLabels.FindAllStringSubmatch(m[2], -1) {
			if slices.Contains(known, l[1]) || slices.Contains(dashboardImplicitLabels, l[1]) {
				continue
			}
			t.Errorf("dashboard selects %s on label %q, which it does not carry (has %v)",
				name, l[1], known)
		}
	}

	if len(seen) == 0 {
		t.Fatal("matched no metrics at all — the extraction is broken, not the dashboard")
	}
}

// The dashboard's mean-achieved-rank panel divides rank-weighted
// pod-time by pod-time over tier ranks only, and it does that by
// excluding the sentinels by name in a regex. Renaming one would not
// break the query — it would quietly put an unsatisfiable bucket back
// into the denominator of a mean, which is worse.
func TestDashboardExcludesExactlyTheNonTierRanks(t *testing.T) {
	_, raw := loadDashboard(t)

	excl := regexp.MustCompile(`rank!~\\"([^\\"]*)\\"`).FindStringSubmatch(raw)
	if excl == nil {
		t.Fatal("no rank exclusion found; the mean-achieved-rank panel must not average over non-tier buckets")
	}
	got := strings.Split(excl[1], "|")
	sort.Strings(got)

	var want []string
	for _, r := range []leeway.Rank{leeway.RankUnknown, leeway.RankUnsatisfiable, leeway.RankOffAxis} {
		if r.IsTier() {
			t.Fatalf("%v is a tier rank and should not be in this list", r)
		}
		want = append(want, r.String())
	}
	sort.Strings(want)

	if !slices.Equal(got, want) {
		t.Errorf("rank exclusion is %v, want %v", got, want)
	}
}

// Both episode gauges carry only subjects with an open episode, so a
// range query over them graphs disappearances as gaps and says nothing
// about what is open now. They have to be instant.
func TestDashboardReadsEpisodeStateInstantly(t *testing.T) {
	d, _ := loadDashboard(t)

	found := 0
	for _, p := range d.Panels {
		for _, tg := range p.Targets {
			if !strings.Contains(tg.Expr, "alert_state") {
				continue
			}
			found++
			if !tg.Instant {
				t.Errorf("panel %q queries alert_state over a range; it must be instant", p.Title)
			}
		}
	}
	if found != 2 {
		t.Errorf("found %d alert_state panels, want 2 (topology drift and compute-class)", found)
	}
}

func TestDashboardLayoutIsWellFormed(t *testing.T) {
	d, _ := loadDashboard(t)

	ids := map[int]string{}
	for _, p := range d.Panels {
		if prev, dup := ids[p.ID]; dup {
			t.Errorf("panel id %d used by both %q and %q; Grafana keys panels by id", p.ID, prev, p.Title)
		}
		ids[p.ID] = p.Title
		if p.GridPos.X+p.GridPos.W > 24 {
			t.Errorf("panel %q overflows the 24-column grid (x=%d w=%d)", p.Title, p.GridPos.X, p.GridPos.W)
		}
		if p.Type != "row" && p.GridPos.H == 0 {
			t.Errorf("panel %q has no height", p.Title)
		}
	}

	// The first row is the SLIs on purpose. A dashboard that leads with
	// drift is one somebody will read without ever checking whether the
	// drift is measured correctly.
	if len(d.Panels) == 0 || d.Panels[0].Type != "row" {
		t.Fatal("dashboard does not open with a row")
	}
	if got := d.Panels[0].Title; got != "Is the measurement sound?" {
		t.Errorf("first row is %q; the trust SLIs come before drift", got)
	}
}
