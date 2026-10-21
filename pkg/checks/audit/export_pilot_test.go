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

package audit

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/rules"
)

// PilotJudgers is a test-only hook for the rules pilot's runtime
// comparison. It lists the scope once, through the same index both
// arms use, and returns two closures that each judge every workload
// for the four pilot kinds — the Go detectors directly, and the CEL
// engine over the same objects — returning the finding count. Listing
// stays outside the benchmark timer; conversion to the unstructured
// form the rules read is inside the CEL closure, because it is part
// of what that approach costs.
//
// convertOnly does the CEL arm's conversion and no evaluation, so the
// two costs can be told apart.
func PilotJudgers(ctx context.Context, client kubernetes.Interface, ns string) (goArm, celArm, convertOnly func() (int, error), err error) {
	ix, err := listTemplateOwners(ctx, client, ns)
	if err != nil {
		return nil, nil, nil, err
	}
	goArm = func() (int, error) {
		n := 0
		for _, w := range ix.workloads {
			n += len(ix.rollout(w))
			n += len(imageTagFindings(w.template.Spec))
		}
		return n, nil
	}
	set := rules.Builtin()
	eng, err := rules.New(set)
	if err != nil {
		return nil, nil, nil, err
	}
	convertOnly = func() (int, error) {
		n := 0
		for _, svc := range ix.services {
			if _, err := toUnstructured(svc); err != nil {
				return 0, err
			}
			n++
		}
		for _, w := range ix.workloads {
			if _, err := ruleSubject(w, nil); err != nil {
				return 0, err
			}
			n++
		}
		return n, nil
	}
	celArm = func() (int, error) {
		services := make([]any, 0, len(ix.services))
		for _, svc := range ix.services {
			m, err := toUnstructured(svc)
			if err != nil {
				return 0, err
			}
			services = append(services, m)
		}
		n := 0
		for _, w := range ix.workloads {
			s, err := ruleSubject(w, services)
			if err != nil {
				return 0, err
			}
			fs, err := eng.Evaluate(s)
			if err != nil {
				return 0, fmt.Errorf("%s/%s: %w", w.namespace, w.name, err)
			}
			n += len(fs)
		}
		return n, nil
	}
	return goArm, celArm, convertOnly, nil
}
