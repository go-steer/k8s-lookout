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
	"slices"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
	"github.com/go-steer/k8s-lookout/pkg/sources/topologydrift"
)

// TestTopologyKeysDefault_IsTheSourcesOwn keeps the flag's default and
// the source's DefaultTopologyKeys from drifting into two different
// answers to "the standard labels". The flag renders the source's list;
// this is what fails if someone hardcodes the strings instead.
func TestTopologyKeysDefault_IsTheSourcesOwn(t *testing.T) {
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	want := topologydrift.DefaultTopologyKeys
	if got := topologyKeysFrom(f.topologyKeys); !slices.Equal(got, want) {
		t.Errorf("--topology-keys default parses to %q, want %q", got, want)
	}
	if f.topologyPerDomain {
		t.Errorf("--topology-per-domain-series defaults on; the 480k-series posture must be opt-in")
	}
}

func TestTopologyKeysFrom_ParsesAndTrims(t *testing.T) {
	got := topologyKeysFrom(" rack , cell,,  ")
	want := []leeway.TopologyKey{"rack", "cell"}
	if !slices.Equal(got, want) {
		t.Errorf("topologyKeysFrom = %q, want %q", got, want)
	}
}

// TestTopologyKeysEmpty_IsRejected pins the deliberate choice not to
// default an emptied flag: the source's normalize would substitute
// zone/region, handing an operator who cleared the flag the exact
// opposite of what they asked for, silently.
func TestTopologyKeysEmpty_IsRejected(t *testing.T) {
	for _, arg := range []string{"--topology-keys=", "--topology-keys=  , ,"} {
		f, err := parseFlags([]string{arg, "--dry-run"})
		if err != nil {
			t.Fatalf("parseFlags(%s): %v", arg, err)
		}
		err = f.validate()
		if err == nil {
			t.Errorf("validate with %s: want an error", arg)
			continue
		}
		if !strings.Contains(err.Error(), "--topology-keys") {
			t.Errorf("error %q does not name the flag", err)
		}
	}
}
