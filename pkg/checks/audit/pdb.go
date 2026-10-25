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
	"fmt"
	"strings"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/engine"
)

const (
	kindNoPDB      = "audit.no_pdb"
	kindPDBPosture = "audit.pdb_posture"
)

const (
	reasonOverlappingPDB        = "OverlappingPDBSelectors"
	reasonZeroDisruptionsBySpec = "ZeroDisruptionsAllowedBySpec"
)

const pdbObjectClass = "PodDisruptionBudget"

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
	for _, pdb := range pdbs {
		if pdbMatchesLabels(pdb, namespace, podLabels) {
			return true
		}
	}
	return false
}

func pdbMatchesLabels(pdb *policyv1.PodDisruptionBudget, namespace string, podLabels map[string]string) bool {
	if pdb.Namespace != namespace {
		return false
	}
	// LabelSelectorAsSelector encodes the policy/v1 distinction
	// this check would otherwise get wrong: a nil selector matches
	// NOTHING, an empty one matches everything in the namespace.
	sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		return false
	}
	return sel.Matches(labels.Set(podLabels))
}

// pdbFindings evaluates L-20 (audit.pdb_posture): overlapping PDB selectors
// and zero-disruption PDB specifications.
func (ix *workloadIndex) pdbFindings(wl emit.WorkloadRef) []emit.Finding {
	if ix.pdbsRefused != nil {
		return nil
	}
	var out []emit.Finding
	for _, p := range ix.pdbs {
		var matched []workload
		var matchedNames []string
		matchesScopeWorkload := wl.IsZero()
		for _, w := range ix.workloads {
			if pdbMatchesLabels(p, w.namespace, w.template.Labels) {
				matched = append(matched, w)
				matchedNames = append(matchedNames, w.kind+"/"+w.name)
				if !wl.IsZero() && w.namespace == wl.Namespace && w.kind == wl.Kind && w.name == wl.Name {
					matchesScopeWorkload = true
				}
			}
		}
		if !matchesScopeWorkload {
			continue
		}

		// 1. OverlappingPDBSelectors
		var overlappingPDBs []string
		seenPDB := map[string]bool{}
		var overlappingWorkloads []string
		for _, w := range matched {
			overlappedThisWorkload := false
			for _, other := range ix.pdbs {
				if other.Namespace != p.Namespace || other.Name <= p.Name {
					continue
				}
				if pdbMatchesLabels(other, w.namespace, w.template.Labels) {
					overlappedThisWorkload = true
					if !seenPDB[other.Name] {
						seenPDB[other.Name] = true
						overlappingPDBs = append(overlappingPDBs, other.Name)
					}
				}
			}
			if overlappedThisWorkload {
				overlappingWorkloads = append(overlappingWorkloads, w.kind+"/"+w.name)
			}
		}
		if len(overlappingPDBs) > 0 {
			f := emit.Finding{
				Kind:         kindPDBPosture,
				Severity:     emit.SeverityWarning,
				Namespace:    p.Namespace,
				KindOfObject: pdbObjectClass,
				Name:         p.Name,
				Reason:       reasonOverlappingPDB,
				Message: fmt.Sprintf("selector overlaps PodDisruptionBudget %s on %s: the eviction controller refuses to evict pods matched by more than one PodDisruptionBudget, blocking voluntary node drains",
					cappedList(overlappingPDBs), cappedList(overlappingWorkloads)),
				Details: []emit.Field{
					{Key: "overlapping_pdb", Value: cappedList(overlappingPDBs)},
					{Key: "workload", Value: cappedList(overlappingWorkloads)},
				},
			}
			f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, pdbObjectClass)
			out = append(out, f)
		}

		// 2. ZeroDisruptionsAllowedBySpec
		if f, ok := ix.zeroDisruptionsFinding(p, matched, matchedNames); ok {
			out = append(out, f)
		}
	}
	return out
}

func (ix *workloadIndex) zeroDisruptionsFinding(p *policyv1.PodDisruptionBudget, matched []workload, matchedNames []string) (emit.Finding, bool) {
	var specReason string
	var details []emit.Field

	if mu := p.Spec.MaxUnavailable; mu != nil {
		if (mu.Type == intstr.Int && mu.IntVal <= 0) || (mu.Type == intstr.String && strings.TrimSpace(mu.StrVal) == "0%") {
			specReason = fmt.Sprintf("spec.maxUnavailable=%s permits zero voluntary disruptions", mu.String())
		}
	}
	if specReason == "" && p.Spec.MinAvailable != nil {
		ma := p.Spec.MinAvailable
		switch ma.Type {
		case intstr.String:
			if strings.TrimSpace(ma.StrVal) == "100%" {
				specReason = "spec.minAvailable=100% requires every pod to stay available, permitting zero voluntary disruptions"
			}
		case intstr.Int:
			if ma.IntVal > 0 {
				for _, w := range matched {
					if w.kind == "DaemonSet" {
						continue
					}
					reps := w.wantReplicas()
					if ix.hpasRefused == nil {
						if floor, _ := ix.replicaFloor(w); floor < reps {
							reps = floor
						}
					}
					if reps > 0 && ma.IntVal >= reps {
						specReason = fmt.Sprintf("spec.minAvailable=%d is at or above %s/%s replicas=%d, permitting zero voluntary disruptions",
							ma.IntVal, w.kind, w.name, reps)
						details = append(details,
							emit.Field{Key: "workload", Value: w.kind + "/" + w.name},
							emit.Field{Key: "replicas", Value: itoa(int(reps))},
						)
						break
					}
				}
			}
		}
	}
	if specReason == "" {
		return emit.Finding{}, false
	}
	if len(details) == 0 && len(matchedNames) > 0 {
		details = append(details, emit.Field{Key: "workload", Value: cappedList(matchedNames)})
	}
	f := emit.Finding{
		Kind:         kindPDBPosture,
		Severity:     emit.SeverityWarning,
		Namespace:    p.Namespace,
		KindOfObject: pdbObjectClass,
		Name:         p.Name,
		Reason:       reasonZeroDisruptionsBySpec,
		Message:      specReason + ": voluntary node drains and upgrades will be permanently blocked",
		Details:      details,
	}
	f.Fingerprint = engine.PostureFingerprint(f.Kind, f.Reason, pdbObjectClass)
	return f, true
}
