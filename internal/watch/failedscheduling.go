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
	"context"
	"log"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/engine"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
)

type failedSchedulingSource interface {
	engine.PodStateLookup
	engine.ScaleUpVerdictLookup
	WithRejectedVerdictHook(fn func())
}

func configureFailedSchedulingGate(
	ctx context.Context,
	f *flags,
	capSrc failedSchedulingSource,
	provider cloud.Provider,
	filter *engine.Filter,
	m *metrics,
	logf func(format string, args ...any),
) {
	if logf == nil {
		logf = log.Printf
	}
	allOff := f.failedSchedulingMinCount <= 1 &&
		f.failedSchedulingMinSpan <= 0 &&
		f.scaleUpHold <= 0 &&
		!f.autopilotScaleToZeroHold
	if allOff {
		filter.SetFailedScheduling(engine.FailedSchedulingConfig{})
		logf("failedscheduling gate: disabled by flags (--failedscheduling-min-count=1 --failedscheduling-min-span=0 --scaleup-hold=0 --autopilot-scale-to-zero-hold=false) — FailedScheduling events pass on event #1")
		return
	}
	if capSrc == nil {
		filter.SetFailedScheduling(engine.FailedSchedulingConfig{})
		logf("failedscheduling gate: DISABLED — capacity source is not enabled (without capacity.pending-aged and its synced podLister/verdict memo, FailedScheduling events pass on event #1)")
		return
	}

	if m != nil {
		capSrc.WithRejectedVerdictHook(func() {
			m.scaleUpVerdictsRejected.Inc()
		})
	}

	autopilot := false
	if f.autopilotScaleToZeroHold {
		autopilot = detectAutopilotCluster(ctx, provider, logf)
	}

	filter.SetFailedScheduling(engine.FailedSchedulingConfig{
		Enabled:                  true,
		MinCount:                 f.failedSchedulingMinCount,
		MinSpan:                  f.failedSchedulingMinSpan,
		ScaleUpHold:              f.scaleUpHold,
		AutopilotScaleToZeroHold: f.autopilotScaleToZeroHold,
		Autopilot:                autopilot,
		PodState:                 capSrc,
		Verdicts:                 capSrc,
	})
	muteBound := max(capacity.CriticalPendingAge, f.pendingAge)
	logf("failedscheduling gate: enabled (min-count=%d, min-span=%s, scaleup-hold=%s, autopilot-scale-to-zero=%t [autopilot=%t]; reporter=%q; forged-event mute bounded at %s by capacity.pending-aged)",
		f.failedSchedulingMinCount, f.failedSchedulingMinSpan, f.scaleUpHold,
		f.autopilotScaleToZeroHold, autopilot, capacity.ScaleUpReporter, muteBound)
}

func detectAutopilotCluster(ctx context.Context, provider cloud.Provider, logf func(string, ...any)) bool {
	if provider == nil {
		provider = cloud.NoProvider
	}
	api, ok := provider.ClusterConfig()
	if !ok {
		logf("failedscheduling gate: Autopilot scale-to-zero hold inactive — %s (fail open)",
			cloud.Unavailable(provider, cloud.CapabilityClusterConfig).Marker())
		return false
	}
	ccCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cfg, err := api.Config(ccCtx)
	if err != nil {
		logf("failedscheduling gate: Autopilot scale-to-zero hold inactive — ClusterConfig unavailable: %v (fail open)", err)
		return false
	}
	if cfg.Autopilot {
		logf("failedscheduling gate: Autopilot cluster detected via %s provider — scale-to-zero hold active for %q with no autoscaler verdict",
			provider.Name(), engine.NoNodesAvailableMessage)
		return true
	}
	logf("failedscheduling gate: Standard (non-Autopilot) cluster detected via %s provider — Autopilot scale-to-zero hold inactive",
		provider.Name())
	return false
}
