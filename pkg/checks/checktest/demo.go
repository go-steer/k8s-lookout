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

package checktest

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// DemoCommand returns the hidden synthetic check used by envelope,
// registry, and dispatcher tests. It is never registered in the
// default registry; tests register it into their own. Its knobs
// exist to exercise the runner's exit paths: --count findings on
// success, --fail for a runtime error after emitting, --hang to
// block until --timeout fires.
func DemoCommand() checks.Command {
	return checks.Command{
		Name:    "triage demo",
		MCPName: "k8s_triage_demo",
		Summary: "Emit synthetic findings to exercise the §4.2 envelope; test scaffolding only.",
		Hidden:  true,
		Flags: []emit.FlagSpec{
			{Name: "count", Type: emit.FlagInt, Default: "2", Help: "number of synthetic findings to emit"},
			{Name: "fail", Type: emit.FlagBool, Default: "false", Help: "return a runtime error after emitting"},
			{Name: "hang", Type: emit.FlagBool, Default: "false", Help: "block until the --timeout context fires"},
		},
		Kinds: []checks.KindField{
			checks.Kind("demo.finding", "a synthetic finding; carries no claim about any cluster", emit.SeverityWarning),
		},
		Output: []checks.OutputField{
			{Name: "index", Doc: "1-based ordinal of the synthetic finding"},
		},
		Examples: []string{
			"lookout triage demo --count=1 --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			if inv.Flags.Bool("hang") {
				<-ctx.Done()
				return 0, ctx.Err()
			}
			ns := inv.Scope.Namespace
			if ns == "" {
				ns = "default"
			}
			for i := 1; i <= inv.Flags.Int("count"); i++ {
				f := emit.Finding{
					Kind:         "demo.finding",
					Severity:     emit.SeverityWarning,
					Namespace:    ns,
					KindOfObject: "Pod",
					Name:         fmt.Sprintf("demo-%d", i),
					Reason:       "DemoReason",
					Message:      "synthetic finding for envelope tests",
					Details:      []emit.Field{{Key: "index", Value: strconv.Itoa(i)}},
				}
				if err := inv.Out.Emit(f); err != nil {
					return 0, err
				}
			}
			if inv.Flags.Bool("fail") {
				return 0, errors.New("demo failure requested via --fail")
			}
			return 5, nil
		},
	}
}
