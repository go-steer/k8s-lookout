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
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const kindNoPDB = "audit.no_pdb"

// covered reports whether any PDB in the workload's namespace selects
// its pod template. Matching the TEMPLATE labels rather than live
// pods is deliberate: the claim is about the spec, so it holds while
// the workload is scaled down, mid-rollout, or momentarily podless,
// and it costs no pod List.
func (ix *workloadIndex) covered(w workload) bool {
	return hasMatchingPDB(ix.pdbs, w.namespace, w.template.Labels)
}

// hasMatchingPDB reports whether any PDB in namespace selects podLabels.
func hasMatchingPDB(pdbs []*policyv1.PodDisruptionBudget, namespace string, podLabels map[string]string) bool {
	tpl := labels.Set(podLabels)
	for _, pdb := range pdbs {
		if pdb.Namespace != namespace {
			continue
		}
		// LabelSelectorAsSelector encodes the policy/v1 distinction
		// this check would otherwise get wrong: a nil selector matches
		// NOTHING, an empty one matches everything in the namespace.
		sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		if sel.Matches(tpl) {
			return true
		}
	}
	return false
}
