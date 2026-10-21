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

package rules

import (
	"fmt"
	"reflect"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/ext"
)

// The variables every rule expression sees.
const (
	varObject     = "object"     // the workload, as an unstructured map
	varKind       = "kind"       // its kind: Deployment, StatefulSet, DaemonSet
	varServices   = "services"   // every Service in scope, as unstructured maps
	varThresholds = "thresholds" // the rule file's knobs
)

// Kubelet defaults for the probe fields that decide a deadline; the
// same constants rollout.go uses.
const (
	probeDefaultPeriodSeconds    = 10
	probeDefaultFailureThreshold = 3
)

// maxListItems caps rendered name lists, as audit.cappedList does.
const maxListItems = 8

// baseEnv is the environment every rule is compiled in: the four
// variables, the strings extension (split, lastIndexOf, substring,
// join — the tag arithmetic needs them) and three helper functions.
//
// The helpers are deliberately few and deliberately generic. The
// pilot's question is whether the CHECK can be data; a check whose
// logic lives in a bespoke Go built-in called from a one-line CEL
// expression is a Go check with extra steps. So the joins — which
// Services select this template, which containers sit behind them —
// are written in CEL in the rule file, and the helpers are the three
// things CEL cannot express at all or cannot express once:
//
//   - handler(probe): the probe's handler kind ("httpGet", "tcpSocket",
//     "exec", "grpc", or "") — a four-way has() chain CEL has no way
//     to name and reuse inside a comprehension.
//   - deadline(probe): initialDelaySeconds + failureThreshold × periodSeconds
//     with kubelet defaults — the same, three has() guards per call site.
//   - capped(list): the ",+N more" rendering every name list uses.
func baseEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable(varObject, cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable(varKind, cel.StringType),
		cel.Variable(varServices, cel.ListType(cel.DynType)),
		cel.Variable(varThresholds, cel.MapType(cel.StringType, cel.DynType)),
		ext.Strings(),
		ext.Lists(),
		cel.Function("handler",
			cel.Overload("handler_probe", []*cel.Type{cel.DynType}, cel.StringType,
				cel.UnaryBinding(handlerFn))),
		cel.Function("deadline",
			cel.Overload("deadline_probe", []*cel.Type{cel.DynType}, cel.IntType,
				cel.UnaryBinding(deadlineFn))),
		cel.Function("capped",
			cel.Overload("capped_list", []*cel.Type{cel.ListType(cel.DynType)}, cel.StringType,
				cel.UnaryBinding(cappedFn))),
	)
}

// probeHandlerKinds in the order rollout.go's probeHandler tests them.
var probeHandlerKinds = []string{"httpGet", "tcpSocket", "exec", "grpc"}

func handlerFn(v ref.Val) ref.Val {
	m, ok := v.Value().(map[string]any)
	if !ok {
		return types.NewErr("handler: want a probe map, got %T", v.Value())
	}
	for _, k := range probeHandlerKinds {
		if _, ok := m[k]; ok {
			return types.String(k)
		}
	}
	return types.String("")
}

func deadlineFn(v ref.Val) ref.Val {
	m, ok := v.Value().(map[string]any)
	if !ok {
		return types.NewErr("deadline: want a probe map, got %T", v.Value())
	}
	period, threshold := intField(m, "periodSeconds"), intField(m, "failureThreshold")
	if period == 0 {
		period = probeDefaultPeriodSeconds
	}
	if threshold == 0 {
		threshold = probeDefaultFailureThreshold
	}
	return types.Int(intField(m, "initialDelaySeconds") + threshold*period)
}

// intField reads a numeric field however the unstructured converter
// left it (int64 from a typed object, float64 from JSON).
func intField(m map[string]any, key string) int64 {
	switch n := m[key].(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

func cappedFn(v ref.Val) ref.Val {
	native, err := v.ConvertToNative(reflect.TypeOf([]string{}))
	if err != nil {
		return types.NewErr("capped: %v", err)
	}
	items := native.([]string)
	if len(items) <= maxListItems {
		return types.String(strings.Join(items, ","))
	}
	return types.String(strings.Join(items[:maxListItems], ",") +
		fmt.Sprintf(",+%d more", len(items)-maxListItems))
}
