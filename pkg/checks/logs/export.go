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

package logs

// Exported seams for the §5 composition commands: `bundle` distills
// the target workload's logs through the same engine `triage logs`
// runs, over pods the caller already resolved.

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// Target is one (pod, container) log stream to distill.
type Target struct {
	Namespace, Pod, Container string
}

// PodTargets expands one pod into its container streams, in spec
// order (init, regular, ephemeral) — the same expansion `triage
// logs` applies.
func PodTargets(pod *corev1.Pod) []Target {
	var out []Target
	for _, c := range podContainers(pod, "") {
		out = append(out, Target{Namespace: pod.Namespace, Pod: pod.Name, Container: c})
	}
	return out
}

// ClientGetter returns the production PodLogGetter backed by the
// clientset's GetLogs subresource.
func ClientGetter(cs kubernetes.Interface) PodLogGetter {
	return clientLogGetter{cs: cs}
}

// DistillOptions tune one Distill run. Zero fields mean the `triage
// logs` flag defaults (Since 1h, Tail 5000, MaxTemplates 40; probe
// lines stripped).
type DistillOptions struct {
	Since        time.Duration
	Previous     bool
	Tail         int
	MaxTemplates int
	KeepProbes   bool
}

// Distill fetches and Drain-clusters the targets' logs, returning
// the distilled findings (log.template / log.stacktrace /
// log.overflow / log.probe_noise, fetch failures as log.fetch_error
// findings) plus the number of raw lines processed. Unlike `triage
// logs` it never aborts on all-streams-failed: a composition caller
// wants its other sections regardless, and the fetch_error findings
// carry the story.
func Distill(ctx context.Context, getter PodLogGetter, targets []Target, opts DistillOptions) (int, []emit.Finding, error) {
	if opts.Since <= 0 {
		opts.Since = defaultSince
	}
	if opts.Tail == 0 {
		opts.Tail = 5000
	}
	if opts.MaxTemplates <= 0 {
		opts.MaxTemplates = 40
	}
	eng := newEngine(!opts.KeepProbes)
	var failures []fetchFailure
	for _, t := range targets {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		tt := target{namespace: t.Namespace, pod: t.Pod, container: t.Container}
		if err := fetchOne(ctx, getter, eng, tt, opts.Since, opts.Previous, opts.Tail); err != nil {
			failures = append(failures, fetchFailure{target: tt, err: err})
		}
	}
	return eng.lines, collectFindings(eng, failures, opts.MaxTemplates), nil
}
