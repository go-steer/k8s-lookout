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

package topologydrift

import (
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

var logT0 = time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

func logKey(name string) baselineKey {
	return baselineKey{
		Subject: leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: name},
		Key:     zoneKey,
	}
}

func logZones() []leeway.Domain {
	return []leeway.Domain{"us-central1-a", "us-central1-b", "us-central1-c"}
}

// quickBaselines matures in ten samples over ten minutes, so a test can watch
// a baseline become usable without waiting six hours.
func quickBaselines() leeway.BaselineConfig {
	cfg := leeway.DefaultBaselineConfig()
	cfg.MinSamples = 10
	cfg.MinAge = 10 * time.Minute
	cfg.HalfLife = 20 * time.Minute
	return cfg.Normalized()
}

// feed drives one key to maturity on a steady placement, returning the time of
// the last sample.
func feed(l *baselineLog, k baselineKey, counts []int64, cfg leeway.BaselineConfig) time.Time {
	now := logT0
	for range cfg.MinSamples + 1 {
		now = now.Add(time.Minute)
		l.Observe(k, logZones(), counts, now, cfg)
	}
	return now
}

func TestBaselineLog_LearnsAndRendersAnIntent(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	l := newBaselineLog()
	k := logKey("web")

	if got := l.Intent(k, logT0, cfg); got != nil {
		t.Errorf("an unknown subject rendered an intent: %+v", got)
	}

	now := feed(l, k, []int64{6, 3, 1}, cfg)
	in := l.Intent(k, now, cfg)
	if in == nil {
		t.Fatal("a matured baseline rendered no intent")
	}
	if in.Source != leeway.SourceLearnedBaseline || len(in.Bands) != 3 {
		t.Errorf("intent = %q with %d bands, want a learned one with three", in.Source, len(in.Bands))
	}
	if got := in.ExplicitShares[logZones()[0]]; got < 0.55 || got > 0.65 {
		t.Errorf("learned share for zone a = %v, want about 0.6", got)
	}
}

// A sample with nothing in it is not a sample. A subject scaled to zero has no
// shares, and recording zeros would teach the baseline that empty is normal and
// then read the scale-up back as drift.
func TestBaselineLog_AnEmptySampleIsNotLearnedFrom(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	l := newBaselineLog()
	k := logKey("web")

	if got := l.Observe(k, logZones(), []int64{0, 0, 0}, logT0, cfg); got != leeway.ObserveEmpty {
		t.Errorf("an all-zero sample = %v, want ObserveEmpty", got)
	}
	if recs, _ := l.Flush("prod"); len(recs) != 0 {
		t.Errorf("an empty sample marked the set dirty: %+v", recs)
	}
	// Tracked, though: the set exists so the next real sample seeds it rather
	// than paying a second first-sight.
	if l.Len() != 1 {
		t.Errorf("Len = %d, want the set to exist", l.Len())
	}
}

// Freezing is what stops a baseline converging on the very drift it is being
// used to judge — and it has to stop the CLOCK, not just the arithmetic, or a
// long freeze thaws and absorbs the drift in one step.
func TestBaselineLog_AFrozenSetLearnsNothing(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	l := newBaselineLog()
	k := logKey("web")
	now := feed(l, k, []int64{6, 3, 1}, cfg)
	before := l.Intent(k, now, cfg)

	if !l.SetFrozen(k, true) {
		t.Fatal("freezing an unfrozen set reported no change")
	}
	if l.SetFrozen(k, true) {
		t.Error("freezing an already-frozen set reported a change")
	}
	for range 30 {
		now = now.Add(time.Minute)
		if got := l.Observe(k, logZones(), []int64{0, 0, 10}, now, cfg); got != leeway.ObserveHeld {
			t.Fatalf("a frozen set returned %v, want ObserveHeld", got)
		}
	}
	after := l.Intent(k, now, cfg)
	if after.ExplicitShares[logZones()[0]] != before.ExplicitShares[logZones()[0]] {
		t.Errorf("a frozen set learned: zone a went %v → %v",
			before.ExplicitShares[logZones()[0]], after.ExplicitShares[logZones()[0]])
	}

	// And thawing does not catch up. One sample after a half-hour freeze must
	// move the estimate by one sample's worth, not by half an hour's.
	l.SetFrozen(k, false)
	now = now.Add(time.Minute)
	l.Observe(k, logZones(), []int64{0, 0, 10}, now, cfg)
	thawed := l.Intent(k, now, cfg)
	if moved := before.ExplicitShares[logZones()[0]] - thawed.ExplicitShares[logZones()[0]]; moved > 0.1 {
		t.Errorf("one sample after a 30m freeze moved zone a by %v, want a single sample's worth", moved)
	}
}

// Only what moved is written, because a flush is a batch: writing every set
// every thirty seconds would be twenty thousand UPSERTs a minute to record
// that a quiet estate is still quiet.
func TestBaselineLog_FlushDrainsOnlyWhatMoved(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	l := newBaselineLog()
	a, b := logKey("api"), logKey("web")

	feed(l, a, []int64{6, 3, 1}, cfg)
	feed(l, b, []int64{3, 3, 3}, cfg)
	recs, gone := l.Flush("prod")
	if len(recs) != 2 || len(gone) != 0 {
		t.Fatalf("first flush = %d records, %d deletions; want 2, 0", len(recs), len(gone))
	}
	// Sorted, so a batch in one transaction is reproducible.
	if recs[0].SubjectKey > recs[1].SubjectKey {
		t.Errorf("flush is unsorted: %q before %q", recs[0].SubjectKey, recs[1].SubjectKey)
	}
	if recs[0].Cluster != "prod" {
		t.Errorf("Cluster = %q, want prod", recs[0].Cluster)
	}

	if recs, gone := l.Flush("prod"); len(recs) != 0 || len(gone) != 0 {
		t.Errorf("a second flush with nothing between them wrote %d records and %d deletions", len(recs), len(gone))
	}

	// A forget is a deletion and not an empty write: leaving the row behind
	// would hand the next workload to take that name a predecessor's history.
	l.Forget(b)
	l.Forget(a)
	recs, gone = l.Flush("prod")
	if len(recs) != 0 || len(gone) != 2 {
		t.Fatalf("after two forgets: %d records, %v deletions; want 0, 2", len(recs), gone)
	}
	if gone[0] != a || gone[1] != b {
		t.Errorf("deletions = %v, want api before web: a batch has to be reproducible", gone)
	}
	// Forgetting what is already gone queues nothing.
	l.Forget(a)
	if _, gone := l.Flush("prod"); len(gone) != 0 {
		t.Errorf("forgetting an unknown key queued %v", gone)
	}
}

// A Deployment deleted and re-applied inside one flush interval must not have
// the row it just wrote deleted behind it.
func TestBaselineLog_ARecreatedSubjectCancelsItsPendingDelete(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	l := newBaselineLog()
	k := logKey("web")

	feed(l, k, []int64{6, 3, 1}, cfg)
	l.Flush("prod")
	l.Forget(k)
	l.Observe(k, logZones(), []int64{3, 3, 3}, logT0.Add(time.Hour), cfg)

	recs, gone := l.Flush("prod")
	if len(gone) != 0 {
		t.Errorf("a re-created subject still had a pending delete: %v", gone)
	}
	if len(recs) != 1 {
		t.Fatalf("wrote %d records, want the re-created one", len(recs))
	}
}

func TestBaselineLog_ForgetSubjectDropsEveryAxis(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	l := newBaselineLog()
	sub := leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "web"}
	zone := baselineKey{Subject: sub, Key: zoneKey}
	host := baselineKey{Subject: sub, Key: "kubernetes.io/hostname"}
	other := logKey("api")

	feed(l, zone, []int64{6, 3, 1}, cfg)
	feed(l, host, []int64{6, 3, 1}, cfg)
	feed(l, other, []int64{3, 3, 3}, cfg)
	l.Flush("prod")

	l.ForgetSubject(sub)
	if l.Len() != 1 {
		t.Errorf("Len = %d after forgetting one subject's two axes, want 1", l.Len())
	}
	if _, gone := l.Flush("prod"); len(gone) != 2 {
		t.Errorf("queued %d deletions, want both axes", len(gone))
	}
}

// §9.3 steps 1 and 7: what comes back, and what widening or staleness it comes
// back with.
func TestBaselineLog_LoadAppliesTheDowntimeRules(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	src := newBaselineLog()
	now := feed(src, logKey("web"), []int64{6, 3, 1}, cfg)
	recs, _ := src.Flush("prod")
	if len(recs) != 1 {
		t.Fatalf("seeded %d records, want 1", len(recs))
	}

	for name, tc := range map[string]struct {
		gap     time.Duration
		verdict leeway.DowntimeVerdict
		mature  bool
	}{
		"a restart":   {time.Minute, leeway.DowntimeResumed, true},
		"a short gap": {6 * time.Hour, leeway.DowntimeWidened, true},
		"a long gap":  {48 * time.Hour, leeway.DowntimeStale, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			l := newBaselineLog()
			at := now.Add(tc.gap)
			n, verdicts := l.Load(recs, at, cfg)
			if n != 1 || verdicts[tc.verdict] != 1 {
				t.Fatalf("loaded %d with %v, want 1 %v", n, verdicts, tc.verdict)
			}
			if got := l.Intent(logKey("web"), at, cfg) != nil; got != tc.mature {
				t.Errorf("mature after %s = %v, want %v", tc.gap, got, tc.mature)
			}
			// A resumed set is byte-for-byte what was read, so writing it back
			// would make every restart a full-table rewrite. The other two
			// changed something the row does not say yet.
			written, _ := l.Flush("prod")
			if wantDirty := tc.verdict != leeway.DowntimeResumed; (len(written) > 0) != wantDirty {
				t.Errorf("%s flushed %d records, want dirty=%v", name, len(written), wantDirty)
			}
		})
	}
}

// Everything Load will not use is dropped without taking the pass down: a row
// that cannot rebuild, and one whose identity does not parse.
func TestBaselineLog_LoadSkipsWhatItCannotUse(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	l := newBaselineLog()

	good := leeway.BaselineRecord{
		Cluster: "prod", SubjectKey: logKey("web").Subject.String(), TopologyKey: string(zoneKey),
		Domains:   []leeway.BaselineDomain{{Domain: "us-central1-a", Share: 1}},
		DevWeight: 0.5, Samples: 50, FirstSeen: logT0, UpdatedAt: logT0.Add(time.Hour),
	}
	damaged := good
	damaged.SubjectKey = logKey("broken").Subject.String()
	damaged.Samples = 0
	unparseable := good
	unparseable.SubjectKey = "this is not a subject ref"
	noAxis := good
	noAxis.SubjectKey = logKey("axisless").Subject.String()
	noAxis.TopologyKey = ""

	n, _ := l.Load([]leeway.BaselineRecord{good, damaged, unparseable, noAxis}, logT0.Add(2*time.Hour), cfg)
	if n != 1 || l.Len() != 1 {
		t.Errorf("loaded %d (Len %d), want only the good row", n, l.Len())
	}
}

// A set already in memory wins: it was built from this cluster as it is now,
// which is strictly better evidence than a row from before the restart.
func TestBaselineLog_LoadDoesNotClobberALiveSet(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	src := newBaselineLog()
	now := feed(src, logKey("web"), []int64{10, 0, 0}, cfg)
	recs, _ := src.Flush("prod")

	l := newBaselineLog()
	live := feed(l, logKey("web"), []int64{0, 0, 10}, cfg)
	if n, _ := l.Load(recs, now, cfg); n != 0 {
		t.Errorf("Load installed %d rows over a live set, want 0", n)
	}
	if got := l.Intent(logKey("web"), live, cfg).ExplicitShares["us-central1-c"]; got < 0.9 {
		t.Errorf("zone c share = %v, want the live set's ~1.0", got)
	}
}

// Flush skips a dirty key with no set behind it. Nothing reaches that state
// today — Forget clears the dirty mark as it drops the set — but the guard is
// what keeps a future third mutator from turning a bookkeeping slip into a nil
// dereference inside a batch, so it is exercised directly.
func TestBaselineLog_FlushSkipsADirtyKeyWithNoSet(t *testing.T) {
	t.Parallel()
	l := newBaselineLog()
	l.dirty[logKey("ghost")] = true
	if recs, gone := l.Flush("prod"); len(recs) != 0 || len(gone) != 0 {
		t.Errorf("flush = %+v / %v, want nothing for a set that is not there", recs, gone)
	}
}

func TestBaselineLog_StatsCountWhatTheGaugeReports(t *testing.T) {
	t.Parallel()
	cfg := quickBaselines()
	l := newBaselineLog()
	ripe, green := logKey("web"), logKey("api")

	now := feed(l, ripe, []int64{6, 3, 1}, cfg)
	l.Observe(green, logZones(), []int64{3, 3, 3}, now, cfg)
	l.Observe(green, logZones(), []int64{0, 0, 0}, now, cfg)
	l.SetFrozen(ripe, true)

	st := l.Stats(now, cfg)
	if st.Tracked != 2 || st.Mature != 1 || st.Frozen != 1 {
		t.Errorf("stats = %+v, want 2 tracked / 1 mature / 1 frozen", st)
	}
	if st.Outcomes[leeway.ObserveSeeded] != 2 || st.Outcomes[leeway.ObserveEmpty] != 1 {
		t.Errorf("outcomes = %v, want two seeds and one empty", st.Outcomes)
	}
	if st.Pending == 0 {
		t.Error("nothing pending after learning two subjects")
	}
	// Freezing an unknown key is a no-op rather than a panic — a subject can
	// go away between the walk that decided to freeze it and the freeze.
	if l.SetFrozen(logKey("ghost"), true) {
		t.Error("freezing an unknown key reported a change")
	}
}
