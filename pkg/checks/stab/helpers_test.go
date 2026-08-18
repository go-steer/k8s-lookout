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

package stab_test

// §13 conventions: fake.Clientset fixtures, a pinned clock so ages
// are golden-testable, VerifyContract in both formats, and one golden
// per command. The logfmt/golden helpers mirror the other command
// test suites (cloudcheck).

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks/stab"
)

// fixedNow is the pinned scan clock; fixture timestamps are relative
// to it so ages in findings are byte-stable.
var fixedNow = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// testDeps binds the stab commands to a fake clientset seeded with
// objs and to the pinned clock.
func testDeps(objs ...runtime.Object) stab.Deps {
	client := fake.NewClientset(objs...)
	return stab.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return client, nil },
		Now:    func() time.Time { return fixedNow },
	}
}

func ago(d time.Duration) metav1.Time { return metav1.Time{Time: fixedNow.Add(-d)} }

func ptr[T any](v T) *T { return &v }

// Shared logfmt parsing helpers (same shapes as the other command
// test suites).

func parseLine(t *testing.T, line string) map[string]string {
	t.Helper()
	out := map[string]string{}
	rest := line
	for rest != "" {
		eq := strings.IndexByte(rest, '=')
		if eq <= 0 {
			t.Fatalf("bad logfmt line %q", line)
		}
		key := rest[:eq]
		rest = rest[eq+1:]
		var val string
		if strings.HasPrefix(rest, `"`) {
			end := len(rest)
			for i := 1; i < len(rest); i++ {
				if rest[i] == '"' && rest[i-1] != '\\' {
					end = i + 1
					break
				}
			}
			val = strings.ReplaceAll(rest[1:end-1], `\"`, `"`)
			rest = rest[end:]
		} else if sp := strings.IndexByte(rest, ' '); sp >= 0 {
			val, rest = rest[:sp], rest[sp:]
		} else {
			val, rest = rest, ""
		}
		out[key] = val
		rest = strings.TrimPrefix(rest, " ")
	}
	return out
}

func findingLines(t *testing.T, stdout string) []map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	var out []map[string]string
	for _, l := range lines[:len(lines)-1] {
		out = append(out, parseLine(t, l))
	}
	return out
}

func summaryLine(t *testing.T, stdout string) map[string]string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	return parseLine(t, lines[len(lines)-1])
}
