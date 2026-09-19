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
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// The shipped CRD and this package's decoder are two statements of the same
// schema, written in different languages and validated in different processes.
// These tests are what stops them drifting: a spelling the API server accepts
// and DecodePolicy rejects is a policy that applies cleanly and then does
// nothing, which is the failure an operator has no way to see.

func loadCRDs(t *testing.T) map[string]map[string]any {
	t.Helper()
	path := filepath.Join("..", "..", "..", "deploy", "crds", "leewaypolicies.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	out := map[string]map[string]any{}
	for _, doc := range bytes.Split(data, []byte("\n---\n")) {
		if len(bytes.TrimSpace(doc)) == 0 {
			continue
		}
		var obj map[string]any
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		if obj["kind"] != "CustomResourceDefinition" {
			continue
		}
		u := unstructured.Unstructured{Object: obj}
		kind, _, _ := unstructured.NestedString(u.Object, "spec", "names", "kind")
		out[kind] = obj
	}
	return out
}

func crdSchema(t *testing.T, crd map[string]any) map[string]any {
	t.Helper()
	versions, ok, _ := unstructured.NestedSlice(crd, "spec", "versions")
	if !ok || len(versions) != 1 {
		t.Fatalf("want exactly one served version, got %v", versions)
	}
	v, _ := versions[0].(map[string]any)
	schema, ok, _ := unstructured.NestedMap(v, "schema", "openAPIV3Schema")
	if !ok {
		t.Fatal("no openAPIV3Schema")
	}
	return schema
}

// crdEnum reads an enum out of the spec schema by property path.
func crdEnum(t *testing.T, schema map[string]any, path ...string) []string {
	t.Helper()
	raw, ok, err := unstructured.NestedStringSlice(schema, append(path, "enum")...)
	if err != nil || !ok {
		t.Fatalf("no enum at %v: %v", path, err)
	}
	return raw
}

func TestCRD_ShipsBothKinds(t *testing.T) {
	crds := loadCRDs(t)
	for _, kind := range []string{PolicyKind, ClusterPolicyKind} {
		crd, ok := crds[kind]
		if !ok {
			t.Fatalf("deploy/crds/leewaypolicies.yaml does not define %s", kind)
		}
		group, _, _ := unstructured.NestedString(crd, "spec", "group")
		if group != PolicyGroup {
			t.Errorf("%s group = %q, want %q", kind, group, PolicyGroup)
		}
		versions, _, _ := unstructured.NestedSlice(crd, "spec", "versions")
		v, _ := versions[0].(map[string]any)
		if v["name"] != PolicyVersion {
			t.Errorf("%s version = %v, want %q", kind, v["name"], PolicyVersion)
		}
	}
	// The plural names are what the dynamic informer asks for. A typo here is
	// a watch that 404s at startup on a cluster that does have the CRD.
	for kind, wantPlural := range map[string]string{
		PolicyKind:        policyGVR.Resource,
		ClusterPolicyKind: clusterPolicyGVR.Resource,
	} {
		plural, _, _ := unstructured.NestedString(crds[kind], "spec", "names", "plural")
		if plural != wantPlural {
			t.Errorf("%s plural = %q, want %q (the GVR the informer watches)", kind, plural, wantPlural)
		}
	}
	// Scope decides which informer delivers the object, and DecodePolicy is
	// told which one it was.
	for kind, wantScope := range map[string]string{
		PolicyKind:        "Namespaced",
		ClusterPolicyKind: "Cluster",
	} {
		scope, _, _ := unstructured.NestedString(crds[kind], "spec", "scope")
		if scope != wantScope {
			t.Errorf("%s scope = %q, want %q", kind, scope, wantScope)
		}
	}
}

func TestCRD_BothKindsShareOneSchema(t *testing.T) {
	// §10.1 says the two kinds share a schema, and YAML anchors do not cross
	// document boundaries, so the file states it twice. This is the guard that
	// the two copies stay the same one.
	crds := loadCRDs(t)
	ns := crdSchema(t, crds[PolicyKind])
	cluster := crdSchema(t, crds[ClusterPolicyKind])
	if !reflect.DeepEqual(ns, cluster) {
		t.Error("the two CRDs' schemas have drifted apart")
	}
}

func TestCRD_EnumsMatchTheDecoder(t *testing.T) {
	schema := crdSchema(t, loadCRDs(t)[PolicyKind])
	keyProps := []string{"properties", "spec", "properties", "topologyKeys", "items", "properties"}

	t.Run("mode", func(t *testing.T) {
		got := crdEnum(t, schema, append(slices.Clone(keyProps), "mode")...)
		want := []string{leeway.ModeSpread.String(), leeway.ModeColocate.String(), leeway.ModeIgnore.String()}
		if !slices.Equal(got, want) {
			t.Errorf("CRD mode enum = %v, want %v", got, want)
		}
		for _, s := range got {
			if _, ok := leeway.ParseIntentMode(s); !ok {
				t.Errorf("the CRD accepts mode %q and the decoder rejects it", s)
			}
		}
	})

	t.Run("weighting", func(t *testing.T) {
		got := crdEnum(t, schema, append(slices.Clone(keyProps), "weighting")...)
		want := []string{
			leeway.WeightEqual.String(), leeway.WeightNodeCount.String(),
			leeway.WeightAllocatableCPU.String(), leeway.WeightAllocatableMemory.String(),
		}
		if !slices.Equal(got, want) {
			t.Errorf("CRD weighting enum = %v, want %v", got, want)
		}
		for _, s := range got {
			if _, ok := leeway.ParseWeighting(s); !ok {
				t.Errorf("the CRD accepts weighting %q and the decoder rejects it", s)
			}
		}
	})

	t.Run("inference sources", func(t *testing.T) {
		got := crdEnum(t, schema,
			"properties", "spec", "properties", "inference", "properties", "sources", "items")
		// Every source except the policy's own: a policy cannot meaningfully
		// allow or deny itself, and filterBySources never applies the
		// allowlist to SourcePolicyCRD.
		var want []string
		for _, s := range leeway.IntentSources() {
			if s == leeway.SourcePolicyCRD {
				continue
			}
			want = append(want, s.APIName())
		}
		if !slices.Equal(got, want) {
			t.Errorf("CRD sources enum = %v,\nwant %v\n(a new IntentSource needs an entry, in precedence order)", got, want)
		}
		for _, s := range got {
			if _, ok := leeway.ParseIntentSource(s); !ok {
				t.Errorf("the CRD accepts source %q and the decoder rejects it", s)
			}
		}
	})
}

func TestCRD_DefaultsMatchTheDecoder(t *testing.T) {
	// The API server applies these defaults before the object reaches us, and
	// DecodePolicy applies the same ones to a document that bypassed
	// validation. Disagreement means a policy behaves differently depending on
	// whether it was read from the API or a fixture.
	schema := crdSchema(t, loadCRDs(t)[PolicyKind])
	decoded := decode(t, `
metadata: { name: minimal }
spec:
  topologyKeys: [{ key: zone }]
`, true)

	modeDefault, _, _ := unstructured.NestedString(schema,
		"properties", "spec", "properties", "topologyKeys", "items", "properties", "mode", "default")
	if modeDefault != decoded.Keys["zone"].Mode.String() {
		t.Errorf("CRD mode default = %q, decoder default = %q", modeDefault, decoded.Keys["zone"].Mode)
	}

	weightDefault, _, _ := unstructured.NestedString(schema,
		"properties", "spec", "properties", "topologyKeys", "items", "properties", "weighting", "default")
	if weightDefault != decoded.Keys["zone"].Weighting.String() {
		t.Errorf("CRD weighting default = %q, decoder default = %q", weightDefault, decoded.Keys["zone"].Weighting)
	}

	enabledDefault, _, _ := unstructured.NestedBool(schema,
		"properties", "spec", "properties", "inference", "properties", "enabled", "default")
	if enabledDefault != decoded.Inference.Enabled {
		t.Errorf("CRD inference.enabled default = %v, decoder default = %v", enabledDefault, decoded.Inference.Enabled)
	}
	if !enabledDefault {
		t.Error("inference defaults off in the CRD — the one default that must not be")
	}
}

func TestCRD_OmitsTheFieldsNoReleaseHonoursYet(t *testing.T) {
	// §10.1 also sketches thresholds, baseline and exclusions. They belong to
	// later phases, and a structural schema prunes what it does not declare —
	// so leaving them out means an operator who writes one sees it vanish,
	// rather than believing a threshold is in force that nothing reads.
	// Adding them when they work is an additive change, which is the safe
	// direction; this test is here so that happens deliberately.
	schema := crdSchema(t, loadCRDs(t)[PolicyKind])
	props, _, _ := unstructured.NestedMap(schema, "properties", "spec", "properties")
	for _, field := range []string{"thresholds", "baseline", "exclusions"} {
		if _, ok := props[field]; ok {
			t.Errorf("spec.%s is in the schema: wire it into DecodePolicy in the same change, or drop it", field)
		}
	}
	// And the ones that are declared are exactly the ones DecodePolicy reads.
	var got []string
	for k := range props {
		got = append(got, k)
	}
	slices.Sort(got)
	want := []string{"inference", "selector", "subjectKinds", "topologyKeys"}
	if !slices.Equal(got, want) {
		t.Errorf("spec properties = %v, want %v", got, want)
	}
}

func TestCRD_ChartShipsTheSameFile(t *testing.T) {
	// The chart serves the CRD from files/ rather than restating it, so there
	// are two copies on disk and exactly one of them is written by hand. Byte
	// equality is the cheap guarantee that `helm install --set
	// leewayPolicyCRD.install=true` and `kubectl apply -f deploy/crds/` install
	// the same schema — a divergence would mean the decoder's drift guard above
	// is validating a document half the users never receive.
	root := filepath.Join("..", "..", "..")
	canonical, err := os.ReadFile(filepath.Join(root, "deploy", "crds", "leewaypolicies.yaml"))
	if err != nil {
		t.Fatalf("reading the canonical CRD: %v", err)
	}
	chart, err := os.ReadFile(filepath.Join(root, "deploy", "chart", "files", "leewaypolicies.yaml"))
	if err != nil {
		t.Fatalf("reading the chart's copy: %v", err)
	}
	if !bytes.Equal(canonical, chart) {
		t.Error("deploy/chart/files/leewaypolicies.yaml has drifted from deploy/crds/leewaypolicies.yaml — copy the canonical file over it")
	}
}

func TestCRD_ExampleFromTheDesignDocDecodes(t *testing.T) {
	// The §10.1 worked example, which is also what the docs page shows. If it
	// stops decoding, the documentation is wrong.
	if p := decode(t, fullPolicy, false); len(p.Keys) != 1 {
		t.Errorf("the design doc's own example decoded to %d keys", len(p.Keys))
	}
}
