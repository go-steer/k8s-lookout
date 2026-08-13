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

package engine

import (
	"strings"
	"testing"
	"time"
)

// minedCorrelator builds a correlator with mining on and a topology
// that knows nothing — the hardest case for mining, and the one where
// it earns its keep, since every declared key is unavailable.
func minedCorrelator(t *testing.T, min int) (*StormCorrelator, *time.Time) {
	t.Helper()
	c, now := newTestCorrelator(t, DefaultStormWindow, DefaultStormMin, &fakeResolver{byObject: map[ObjectRef][]Ancestor{}})
	if err := c.EnableMining(DefaultMinedDimensions, min); err != nil {
		t.Fatalf("EnableMining: %v", err)
	}
	return c, now
}

// TestMining_OffByDefault: mining groups on a coincidence, so it must
// not turn itself on. A correlator nobody configured behaves exactly
// as it did before #225.
func TestMining_OffByDefault(t *testing.T) {
	t.Parallel()
	c, now := newTestCorrelator(t, DefaultStormWindow, DefaultStormMin, &fakeResolver{byObject: map[ObjectRef][]Ancestor{}})
	for i := 0; i < DefaultMinedMin+3; i++ {
		*now = now.Add(time.Second)
		sig := podSignal(i, "shop")
		sig.Node = "gke-node-a"
		if v := c.Observe(sig); v.Kind != StormNone {
			t.Fatalf("incident %d: verdict = %v, want StormNone with mining off", i, v.Kind)
		}
	}
	if c.ActiveStorms() != 0 {
		t.Errorf("ActiveStorms = %d, want 0", c.ActiveStorms())
	}
}

// TestMining_GroupsOnAnUnmodelledSharedAttribute is the tier's reason
// for existing: one broken image rolled out across unrelated
// workloads. No topology ancestor connects them (different namespaces,
// different owners, different nodes) and no ExternalAncestor extractor
// covers it — the registry-host key does not fire because these are
// terminal failures, deliberately.
func TestMining_GroupsOnAnUnmodelledSharedAttribute(t *testing.T) {
	t.Parallel()
	c, now := minedCorrelator(t, DefaultMinedMin)

	const image = "gcr.io/proj/sidecar:v2.3"
	namespaces := []string{"shop", "billing", "search", "auth", "web"}
	var storm StormInfo
	var formed int
	for i, ns := range namespaces {
		*now = now.Add(time.Second)
		sig := podSignal(i, ns)
		sig.Node = "gke-node-" + ns // deliberately all different
		sig.Message = `Failed to pull image "` + image + `": manifest unknown`
		if v := c.Observe(sig); v.Kind == StormFormed {
			formed++
			storm = v.Storm
		}
	}
	if formed != 1 {
		t.Fatalf("storms formed = %d, want 1", formed)
	}
	if want := (Ancestor{Kind: "Image", Name: image}); storm.Ancestor != want {
		t.Errorf("ancestor = %+v, want %+v", storm.Ancestor, want)
	}
	if storm.KeySource != MinedKeySource("image") {
		t.Errorf("KeySource = %q, want %q", storm.KeySource, MinedKeySource("image"))
	}
	if storm.AffectedCount != len(namespaces) {
		t.Errorf("AffectedCount = %d, want %d", storm.AffectedCount, len(namespaces))
	}
	if storm.NamespaceCount != len(namespaces) {
		t.Errorf("NamespaceCount = %d, want %d", storm.NamespaceCount, len(namespaces))
	}
}

// TestMining_ExplainsItself is the explainability gate observed from
// the outside: whatever a mined storm groups on, it can render the
// reason in words. This is the property that makes the tier safe to
// page on — "these five are one incident" is only actionable with the
// "because".
func TestMining_ExplainsItself(t *testing.T) {
	t.Parallel()
	c, now := minedCorrelator(t, DefaultMinedMin)

	var storm StormInfo
	for i := 0; i < DefaultMinedMin; i++ {
		*now = now.Add(time.Second)
		sig := podSignal(i, "shop")
		sig.Node = "gke-node-7"
		if v := c.Observe(sig); v.Kind == StormFormed {
			storm = v.Storm
		}
	}
	display := storm.Ancestor.Display()
	if display == "" {
		t.Fatal("mined storm rendered no explanation")
	}
	if !strings.Contains(display, "gke-node-7") {
		t.Errorf("explanation %q does not name what was shared", display)
	}
	if !strings.HasPrefix(storm.KeySource, "mined:") {
		t.Errorf("KeySource = %q, want a mined: prefix so downstream can tell "+
			"a discovered key from a modelled one", storm.KeySource)
	}
}

// TestMining_RequiresMoreEvidenceThanADeclaredKey: a discovered key is
// circumstantial, so DefaultStormMin incidents are NOT enough for one.
func TestMining_RequiresMoreEvidenceThanADeclaredKey(t *testing.T) {
	t.Parallel()
	if DefaultMinedMin <= DefaultStormMin {
		t.Fatalf("DefaultMinedMin (%d) must exceed DefaultStormMin (%d)", DefaultMinedMin, DefaultStormMin)
	}
	c, now := minedCorrelator(t, DefaultMinedMin)

	// One short of the mined threshold.
	for i := 0; i < DefaultMinedMin-1; i++ {
		*now = now.Add(time.Second)
		sig := podSignal(i, "shop")
		sig.Node = "gke-node-a"
		if v := c.Observe(sig); v.Kind != StormNone {
			t.Fatalf("incident %d: verdict = %v, want StormNone below the mined threshold", i, v.Kind)
		}
	}
	// The one that reaches it.
	*now = now.Add(time.Second)
	sig := podSignal(DefaultMinedMin, "shop")
	sig.Node = "gke-node-a"
	if v := c.Observe(sig); v.Kind != StormFormed {
		t.Errorf("verdict at the mined threshold = %v, want StormFormed", v.Kind)
	}
}

// TestMining_DeclaredKeysWinWhenBothApply: mining is a fallback, not a
// competitor. Where a modelled ancestor already explains the burst,
// the storm must be keyed on it — the modelled key is the better
// explanation and the one operators have dashboards for.
func TestMining_DeclaredKeysWinWhenBothApply(t *testing.T) {
	t.Parallel()

	res := &fakeResolver{byObject: map[ObjectRef][]Ancestor{}}
	var sigs []Signal
	for i := 0; i < DefaultMinedMin; i++ {
		s := podSignal(i, "shop")
		s.Node = "gke-node-a"
		s.Message = `Failed to pull image "gcr.io/proj/app:v1": manifest unknown`
		res.byObject[s.ref()] = []Ancestor{ancRS} // a modelled owner
		sigs = append(sigs, s)
	}
	c, now := newTestCorrelator(t, DefaultStormWindow, DefaultStormMin, res)
	if err := c.EnableMining(DefaultMinedDimensions, DefaultMinedMin); err != nil {
		t.Fatalf("EnableMining: %v", err)
	}

	var storm StormInfo
	for _, s := range sigs {
		*now = now.Add(time.Second)
		if v := c.Observe(s); v.Kind == StormFormed {
			storm = v.Storm
		}
	}
	if storm.Ancestor != ancRS {
		t.Errorf("ancestor = %+v, want the declared %+v", storm.Ancestor, ancRS)
	}
	if storm.KeySource != KeySourceTopology {
		t.Errorf("KeySource = %q, want %q", storm.KeySource, KeySourceTopology)
	}
}

// TestMining_AbsentAttributeNeverCorrelates: signals missing an
// attribute must not group with each other on its emptiness. Without
// this, every incident with no node recorded would be "the same
// incident".
func TestMining_AbsentAttributeNeverCorrelates(t *testing.T) {
	t.Parallel()
	c, now := minedCorrelator(t, DefaultMinedMin)

	for i := 0; i < DefaultMinedMin+4; i++ {
		*now = now.Add(time.Second)
		// No Node, no Container, no image in the message.
		sig := podSignal(i, "shop")
		sig.Message = "something went wrong"
		if v := c.Observe(sig); v.Kind != StormNone {
			t.Fatalf("incident %d: verdict = %v, want StormNone — "+
				"missing attributes are not a shared attribute", i, v.Kind)
		}
	}
}

// TestMining_LateArrivalsAttachToAMinedStorm: a mined storm has to
// behave like any other once open, or the sixth pod with the broken
// image opens a second session and the tier has achieved nothing.
func TestMining_LateArrivalsAttachToAMinedStorm(t *testing.T) {
	t.Parallel()
	c, now := minedCorrelator(t, DefaultMinedMin)

	const image = "gcr.io/proj/sidecar:v2.3"
	msg := `Failed to pull image "` + image + `": manifest unknown`
	for i := 0; i < DefaultMinedMin; i++ {
		*now = now.Add(time.Second)
		sig := podSignal(i, "shop")
		sig.Message = msg
		c.Observe(sig)
	}
	*now = now.Add(time.Second)
	late := podSignal(99, "billing")
	late.Message = msg
	v := c.Observe(late)
	if v.Kind != StormAttached {
		t.Fatalf("late arrival verdict = %v, want StormAttached", v.Kind)
	}
	if v.Storm.AffectedCount != DefaultMinedMin+1 {
		t.Errorf("AffectedCount = %d, want %d", v.Storm.AffectedCount, DefaultMinedMin+1)
	}
	if c.ActiveStorms() != 1 {
		t.Errorf("ActiveStorms = %d, want 1 — the late arrival must not open a second", c.ActiveStorms())
	}
}

// TestValidateMinedDimensions is the explainability gate from the
// inside: a dimension that cannot name itself is rejected at
// configuration time, not silently tolerated at correlation time.
func TestValidateMinedDimensions(t *testing.T) {
	t.Parallel()
	value := func(Signal) string { return "x" }
	cases := []struct {
		name string
		dims []MinedDimension
		ok   bool
	}{
		{"shipped defaults", DefaultMinedDimensions, true},
		{"no name", []MinedDimension{{Kind: "Image", Value: value}}, false},
		{"no kind", []MinedDimension{{Name: "image", Value: value}}, false},
		{"no value", []MinedDimension{{Name: "image", Kind: "Image"}}, false},
		{"duplicate name", []MinedDimension{
			{Name: "image", Kind: "Image", Value: value},
			{Name: "image", Kind: "Other", Value: value},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateMinedDimensions(tc.dims)
			if tc.ok && err != nil {
				t.Errorf("ValidateMinedDimensions: unexpected error %v", err)
			}
			if !tc.ok && err == nil {
				t.Error("ValidateMinedDimensions: want an error, got nil")
			}
		})
	}
}

// TestEnableMining_RejectsAnEasierThresholdThanDeclaredKeys: a mined
// key must never be cheaper to form than a modelled one.
func TestEnableMining_RejectsAnEasierThresholdThanDeclaredKeys(t *testing.T) {
	t.Parallel()
	c, _ := newTestCorrelator(t, DefaultStormWindow, 4, &fakeResolver{byObject: map[ObjectRef][]Ancestor{}})
	if err := c.EnableMining(DefaultMinedDimensions, 3); err == nil {
		t.Error("EnableMining with min below --storm-min: want an error, got nil")
	}
	if err := c.EnableMining(nil, 5); err == nil {
		t.Error("EnableMining with no dimensions: want an error, got nil")
	}
	if err := c.EnableMining(DefaultMinedDimensions, 4); err != nil {
		t.Errorf("EnableMining at exactly --storm-min: unexpected error %v", err)
	}
}
