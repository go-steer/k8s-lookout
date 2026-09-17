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
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/go-steer/k8s-lookout/pkg/leeway"
)

// sameBacking reports whether two strings share a backing array. Equality of
// value is not what interning promises — sharing is — so the tests assert on
// the pointer, which is the only thing that distinguishes an interner from an
// identity function.
func sameBacking(a, b string) bool {
	return unsafe.StringData(a) == unsafe.StringData(b)
}

func TestInterner_NameSharesBacking(t *testing.T) {
	i := newInterner()

	// Built separately so the compiler cannot hand both callers one constant,
	// which is the situation at runtime: each name arrives from its own JSON
	// decode.
	first := strings.Join([]string{"gke", "node", "abc123"}, "-")
	second := strings.Join([]string{"gke", "node", "abc123"}, "-")
	if sameBacking(first, second) {
		t.Fatal("the two inputs already share backing; the test proves nothing")
	}

	a := i.name(first)
	b := i.name(second)
	if a != b {
		t.Fatalf("interned names differ: %q, %q", a, b)
	}
	if !sameBacking(a, b) {
		t.Error("interned names do not share a backing array")
	}
	if names, _, _ := i.size(); names != 1 {
		t.Errorf("names table holds %d entries, want 1", names)
	}
}

func TestInterner_EmptyIsNotStored(t *testing.T) {
	i := newInterner()
	if got := i.name(""); got != "" {
		t.Errorf("name(\"\") = %q", got)
	}
	if got := i.domain(""); got != "" {
		t.Errorf("domain(\"\") = %q", got)
	}
	names, domains, tuples := i.size()
	if names != 0 || domains != 0 || tuples != 0 {
		t.Errorf("empty values were stored: %d/%d/%d", names, domains, tuples)
	}
}

func TestInterner_TupleSharesOneSlice(t *testing.T) {
	i := newInterner()

	first := []leeway.Domain{"us-central1-a", "pool-1"}
	second := []leeway.Domain{"us-central1-a", "pool-1"}

	a := i.tuple(first)
	b := i.tuple(second)
	if &a[0] != &b[0] {
		t.Error("equal tuples did not collapse onto one backing array")
	}
	// The canonical slice is the interner's own, not the caller's: a caller
	// that reuses its scratch slice must not be able to rewrite what every
	// Placement in the index points at.
	if &a[0] == &first[0] {
		t.Error("tuple returned the caller's slice rather than a private copy")
	}
	if _, _, tuples := i.size(); tuples != 1 {
		t.Errorf("tuples table holds %d entries, want 1", tuples)
	}

	different := i.tuple([]leeway.Domain{"us-central1-b", "pool-1"})
	if &different[0] == &a[0] {
		t.Error("different tuples collapsed onto one backing array")
	}
}

func TestInterner_TupleDistinguishesAxisBoundaries(t *testing.T) {
	// The key is a join, so the separator is load-bearing: ("ab", "c") and
	// ("a", "bc") must not collide.
	i := newInterner()
	a := i.tuple([]leeway.Domain{"ab", "c"})
	b := i.tuple([]leeway.Domain{"a", "bc"})
	if &a[0] == &b[0] {
		t.Fatal("tuples with the same concatenation collided")
	}
	if a[0] != "ab" || b[0] != "a" {
		t.Errorf("tuple contents corrupted: %v, %v", a, b)
	}
}

func TestInterner_TupleInternsItsDomains(t *testing.T) {
	i := newInterner()
	i.tuple([]leeway.Domain{"zone-a", "pool-1"})
	i.tuple([]leeway.Domain{"zone-a", "pool-2"})

	// Two tuples, three distinct domain values between them.
	_, domains, tuples := i.size()
	if domains != 3 {
		t.Errorf("domains table holds %d entries, want 3", domains)
	}
	if tuples != 2 {
		t.Errorf("tuples table holds %d entries, want 2", tuples)
	}
}

func TestInterner_TupleEmpty(t *testing.T) {
	i := newInterner()
	if got := i.tuple(nil); got != nil {
		t.Errorf("tuple(nil) = %v, want nil", got)
	}
	if got := i.tuple([]leeway.Domain{}); got != nil {
		t.Errorf("tuple(empty) = %v, want nil", got)
	}
	if _, _, tuples := i.size(); tuples != 0 {
		t.Error("the empty tuple was stored")
	}
}

func TestInterner_Forget(t *testing.T) {
	i := newInterner()
	i.name("node-1")
	i.domain("zone-a")

	i.forget("node-1")
	names, domains, _ := i.size()
	if names != 0 {
		t.Errorf("names table holds %d entries after forget, want 0", names)
	}
	// Domains survive on purpose: a zone that briefly empties is the case
	// leeway exists to notice, and there are only ever a handful of them.
	if domains != 1 {
		t.Errorf("forget dropped a domain: table holds %d entries, want 1", domains)
	}

	// Forgetting something absent is a no-op, not a panic: Remove calls it for
	// every node it drops, including ones whose name never reached the table.
	i.forget("never-seen")
}

func TestInterner_ForgetBoundsTheTable(t *testing.T) {
	// The leak this guards against: a cluster cycling through preemptible
	// nodes would otherwise accumulate one dead string per node forever.
	i := newInterner()
	for n := range 10_000 {
		name := fmt.Sprintf("preemptible-node-%d", n)
		i.name(name)
		i.forget(name)
	}
	if names, _, _ := i.size(); names != 0 {
		t.Errorf("names table holds %d entries after churning 10k nodes, want 0", names)
	}
}
