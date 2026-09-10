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
	"errors"

	"github.com/go-steer/k8s-lookout/pkg/sources"
)

// Runner-exit classification (issue #383).
//
// The supervisor's default posture — restart anything that stops — is
// right for the failures it was written for: an apiserver rolling, a
// network partition, a cluster mid-upgrade. All of those resolve on
// their own, and the runner that comes back finds a working cluster.
//
// A settled authorization refusal is a different animal. The runner
// exits during its startup probe, the supervisor restarts it, it
// re-probes, it is refused again — and it will keep being refused
// until a human edits a ClusterRoleBinding. Retrying that costs an
// SSAR round trip and a client-go dial per attempt, forever, per
// denied cluster; at fleet scale (the motivating report was 26
// clusters) it is a memory and API-server load problem, and the log
// line that would tell an operator what is wrong scrolls past every
// backoff period instead of standing still.
//
// So exits split two ways. TRANSIENT keeps the old behaviour, with
// backoff that now grows. TERMINAL stops the runner, marks the
// cluster degraded in /readyz, and raises a metric an alert can sit
// on — the process keeps watching every cluster that still works.

// terminalReason names the class of a terminal exit, for the
// lookout_runner_terminal label. Deliberately a small closed set: it
// is a metric label, and the operator-facing detail lives in the log
// line and the /readyz?verbose body, which are not cardinality-bound.
type terminalReason string

// reasonAccessDenied is, today, the only terminal class: the
// authorizer refused a required permission.
//
// The set is deliberately narrow. "Terminal" is a promise that no
// amount of waiting fixes this, and a wrong terminal verdict is worse
// than a wrong transient one — a transient misclassification wastes
// retries, a terminal misclassification permanently stops watching a
// cluster that would have recovered. A denial is the one failure the
// sentinel can be sure about, because it asked the authorizer
// directly and got a decision, not a transport error. Everything
// else, including a 404 on a cluster that may have been deleted,
// stays transient until it earns its own case.
const reasonAccessDenied terminalReason = "access_denied"

// classifyExit returns the terminal class of a runner's exit error,
// or "" when the exit is transient (and so retryable).
func classifyExit(err error) terminalReason {
	if errors.Is(err, sources.ErrAccessDenied) {
		return reasonAccessDenied
	}
	return ""
}
