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

package emit

import (
	"flag"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func TestPerClusterPath(t *testing.T) {
	for _, tc := range []struct {
		name    string
		base    string
		cluster string
		want    string
	}{
		{"suffix before the extension", "/var/lib/lookout/lookout.db", "prod-us", "/var/lib/lookout/lookout-prod-us.db"},
		{"no extension", "/data/lookout", "prod-eu", "/data/lookout-prod-eu"},
		{"dedup snapshot shares the rule", "/data/dedup.json", "prod-us", "/data/dedup-prod-us.json"},
		{"relative stem", "lookout.db", "kind", "lookout-kind.db"},
		{"dotted cluster name flattens", "/data/l.db", "prod.us.example", "/data/l-prod-us-example.db"},
		{"underscores survive", "/data/l.db", "prod_us", "/data/l-prod_us.db"},
		// The disabled states: --store unset, or a ref with no name.
		// Both must return the base untouched so the single-cluster
		// default is byte-identical.
		{"empty base", "", "prod-us", ""},
		{"empty cluster", "/data/l.db", "", "/data/l.db"},
		{"unusable cluster name", "/data/l.db", "///", "/data/l.db"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := PerClusterPath(tc.base, tc.cluster); got != tc.want {
				t.Errorf("PerClusterPath(%q, %q) = %q, want %q", tc.base, tc.cluster, got, tc.want)
			}
		})
	}
}

// TestPerClusterPathCannotEscapeTheStem is the security property: the
// cluster name reaching this function is operator-supplied (a --clusters
// pair, a kubeconfig context), and it must not be able to steer the file
// out of the stem's directory or onto a path of its choosing.
func TestPerClusterPathCannotEscapeTheStem(t *testing.T) {
	dir := "/var/lib/lookout"
	for _, name := range []string{
		"../../etc/passwd",
		"/etc/shadow",
		"a/b/c",
		"..",
		"prod\x00us",
		"prod\nus",
	} {
		got := PerClusterPath(filepath.Join(dir, "lookout.db"), name)
		if filepath.Dir(got) != dir {
			t.Errorf("cluster %q produced %q, which leaves %s", name, got, dir)
		}
		if strings.Count(strings.TrimPrefix(got, dir+"/"), "/") != 0 {
			t.Errorf("cluster %q produced %q, which is more than one path component", name, got)
		}
	}
}

// Two clusters must never derive one path — the whole point. The names
// here are distinct, so the slugs must be too.
func TestPerClusterPathIsInjectiveOnDistinctNames(t *testing.T) {
	seen := map[string]string{}
	for _, name := range []string{"prod-us", "prod-eu", "prod_us", "staging", "dev-1", "dev-2"} {
		got := PerClusterPath("/data/lookout.db", name)
		if prev, dup := seen[got]; dup {
			t.Errorf("clusters %q and %q both derive %q", prev, name, got)
		}
		seen[got] = name
	}
}

// StorePath is the one seam every --store consumer goes through, so the
// two flags cannot drift apart: --store alone is the literal path a
// single-cluster sentinel writes, and adding --store-cluster says the
// value is a fleet stem (#410).
func TestStorePathResolvesAgainstStoreCluster(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"literal without the selector", []string{"--store=/var/lib/lookout/lookout.db"}, "/var/lib/lookout/lookout.db"},
		{"stem plus selector", []string{"--store=/var/lib/lookout/lookout.db", "--store-cluster=prod-us"}, "/var/lib/lookout/lookout-prod-us.db"},
		{"no store at all", []string{"--store-cluster=prod-us"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			specs := []FlagSpec{
				{Name: "store", Type: FlagString, Default: "", Help: "h"},
				StoreClusterFlag(),
			}
			if err := registerSpecs(fs, specs); err != nil {
				t.Fatal(err)
			}
			if err := fs.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			if got := StorePath(FlagValues{fs: fs}); got != tc.want {
				t.Errorf("StorePath(%v) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}

// The selector is a separate flag from any cluster LABEL on purpose: a
// single-cluster sentinel has a cluster name and still writes the literal
// --store path, so deriving from cluster identity alone would send a
// triage record to a file nothing reads. Guard the name and the default,
// which are the contract.
func TestStoreClusterFlagShape(t *testing.T) {
	f := StoreClusterFlag()
	if f.Name != "store-cluster" || f.Type != FlagString || f.Default != "" {
		t.Errorf("StoreClusterFlag() = %+v, want a string --store-cluster defaulting to empty", f)
	}
	if !strings.Contains(f.Help, "--store") {
		t.Errorf("help does not say what it selects: %q", f.Help)
	}
}
