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

package checks_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// TestDemoCommandContract is the §13 scaffold in action — the same
// three lines every future command's test suite includes.
func TestDemoCommandContract(t *testing.T) {
	checktest.VerifyContract(t, checktest.DemoCommand())
	checktest.VerifyContract(t, checktest.DemoCommand(), "--count=0")
	checktest.VerifyContract(t, checktest.DemoCommand(), "--count=7", "--namespace=prod")
}

// TestVerifyCatchesUndeclaredField proves the contract check has
// teeth: a command emitting a field missing from its glossary fails.
func TestVerifyCatchesUndeclaredField(t *testing.T) {
	c := checktest.DemoCommand()
	c.Run = func(ctx context.Context, inv emit.Invocation) (int, error) {
		f := emit.Finding{
			Kind:    "demo.finding",
			Details: []emit.Field{{Key: "surprise", Value: "undeclared"}},
		}
		return 1, inv.Out.Emit(f)
	}
	for _, format := range []emit.Format{emit.FormatLogfmt, emit.FormatJSON} {
		res := checktest.Run(t, c, "--format="+string(format))
		if res.Code != 0 {
			t.Fatalf("exit = %d", res.Code)
		}
		err := checktest.Verify(c, res.Stdout, format)
		if err == nil || !strings.Contains(err.Error(), "surprise") {
			t.Errorf("Verify (%s) should reject the undeclared field, got %v", format, err)
		}
	}
}

// TestVerifyCatchesMissingSummary proves a payload without the
// mandatory terminating summary line is rejected.
func TestVerifyCatchesMissingSummary(t *testing.T) {
	c := checktest.DemoCommand()
	if err := checktest.Verify(c, "kind=demo.finding index=1\n", emit.FormatLogfmt); err == nil {
		t.Error("Verify should reject output without a summary line")
	}
	if err := checktest.Verify(c, "", emit.FormatLogfmt); err == nil {
		t.Error("Verify should reject empty output")
	}
}

// TestVerifyCatchesCountDrift proves the findings count in the
// summary must match the emitted lines.
func TestVerifyCatchesCountDrift(t *testing.T) {
	c := checktest.DemoCommand()
	out := "kind=demo.finding index=1\nscanned=5 findings=2 elapsed=100ms\n"
	if err := checktest.Verify(c, out, emit.FormatLogfmt); err == nil {
		t.Error("Verify should reject a summary whose findings count drifts from the payload")
	}
}

// TestRegisteredCommandsHonorContract sweeps whatever is in the
// default registry (nothing yet — the first real check inherits this
// test for free) plus the demo command.
func TestRegisteredCommandsHonorContract(t *testing.T) {
	for _, c := range checks.All() {
		if err := c.Validate(); err != nil {
			t.Errorf("registered command %q is invalid: %v", c.Name, err)
		}
	}
}
