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

package delta

// The pods class: broken workloads. Pod-level container pathologies
// (crashloop, image pull, OOM, restart churn, stuck Pending,
// not-ready), failed Jobs, and workload-level rollout mismatch with
// stalled-progress detection.

import (
	"github.com/go-steer/k8s-lookout/pkg/emit"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// imagePullReasons are the waiting reasons that mean "the kubelet
// cannot get the image" — the pod will never start without action.
var imagePullReasons = map[string]bool{
	"ImagePullBackOff":  true,
	"ErrImagePull":      true,
	"InvalidImageName":  true,
	"ErrImageNeverPull": true,
	"ImageInspectError": true,
}

// errorWaitingReasons are the remaining waiting reasons that are
// error states rather than normal startup phases. ContainerCreating
// and PodInitializing are deliberately absent: they are nominal
// unless they persist, which surfaces via pod.pending/pod.notready.
var errorWaitingReasons = map[string]bool{
	"CreateContainerConfigError": true,
	"CreateContainerError":       true,
	"RunContainerError":          true,
	"StartError":                 true,
	"PreCreateHookError":         true,
	"PostStartHookError":         true,
}

// checkPods derives the pod.* findings.
func (s *scanner) checkPods(pods []corev1.Pod) {
	for i := range pods {
		s.checkPod(&pods[i])
	}
}

func (s *scanner) checkPod(pod *corev1.Pod) {
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		return // completed work is nominal
	case corev1.PodFailed:
		// Failed pods of a Job are reported once, at the Job
		// (job.failed), not per retry pod.
		if ownedBy(pod.OwnerReferences, "Job") {
			return
		}
		reason := pod.Status.Reason
		if reason == "" {
			reason = "PodFailed"
		}
		s.add(emit.Finding{
			Kind:         "pod.failed",
			Severity:     emit.SeverityWarning,
			Namespace:    pod.Namespace,
			KindOfObject: "Pod",
			Name:         pod.Name,
			Reason:       reason,
			Message:      pod.Status.Message,
		})
		return
	}

	// Pending, Running, and Unknown pods: judge per container
	// first — a Pending pod in ImagePullBackOff or with a
	// crashlooping init container has a concrete diagnosis, and
	// the generic aged-pending finding is only the fallback when
	// no container tells a better story. Init container statuses
	// are included: a crashlooping init container wedges the pod
	// exactly like a main one.
	found := false
	for i := range pod.Status.InitContainerStatuses {
		found = s.checkContainer(pod, pod.Status.InitContainerStatuses[i], "init:") || found
	}
	for i := range pod.Status.ContainerStatuses {
		found = s.checkContainer(pod, pod.Status.ContainerStatuses[i], "") || found
	}
	if pod.Status.Phase == corev1.PodPending && !found {
		s.checkPending(pod)
	}
}

// checkPending flags pods stuck in Pending past the threshold, with
// the scheduler's verdict when it has one.
func (s *scanner) checkPending(pod *corev1.Pod) {
	pendingFor := s.now.Sub(pod.CreationTimestamp.Time)
	if pendingFor < s.th.pendingAge {
		return // young Pending is nominal scheduling latency
	}
	f := emit.Finding{
		Kind:         "pod.pending",
		Severity:     emit.SeverityWarning,
		Namespace:    pod.Namespace,
		KindOfObject: "Pod",
		Name:         pod.Name,
		Reason:       "Pending",
		Details:      []emit.Field{{Key: "age", Value: s.age(pod.CreationTimestamp.Time)}},
	}
	if c := podCondition(pod, corev1.PodScheduled); c != nil && c.Status == corev1.ConditionFalse {
		if c.Reason != "" {
			f.Reason = c.Reason
		}
		f.Message = c.Message
		// An explicitly unschedulable pod is a capacity/constraint
		// problem, not latency — that is the critical case.
		if c.Reason == corev1.PodReasonUnschedulable {
			f.Severity = emit.SeverityCritical
		}
	}
	s.add(f)
}

// checkContainer emits at most one finding per container, worst
// state first: crashloop > image pull > other waiting error >
// OOM history > restart churn > not ready. It reports whether it
// emitted, so checkPod can suppress the generic aged-pending
// fallback when a container already carries the diagnosis.
func (s *scanner) checkContainer(pod *corev1.Pod, cs corev1.ContainerStatus, prefix string) bool {
	name := prefix + cs.Name
	base := emit.Finding{
		Namespace:    pod.Namespace,
		KindOfObject: "Pod",
		Name:         pod.Name,
	}
	details := func(extra ...emit.Field) []emit.Field {
		return append([]emit.Field{{Key: "container", Value: name}}, extra...)
	}
	restarts := emit.Field{Key: "restarts", Value: itoa32(cs.RestartCount)}

	if w := cs.State.Waiting; w != nil {
		switch {
		case w.Reason == "CrashLoopBackOff":
			f := base
			f.Kind = "pod.crashloop"
			f.Severity = emit.SeverityCritical
			f.Reason = w.Reason
			f.Details = details(restarts)
			if t := cs.LastTerminationState.Terminated; t != nil {
				if t.Reason != "" {
					f.Details = append(f.Details, emit.Field{Key: "last_state", Value: t.Reason})
				}
				f.Details = append(f.Details, emit.Field{Key: "exit_code", Value: itoa32(t.ExitCode)})
			}
			s.add(f)
			return true
		case imagePullReasons[w.Reason]:
			f := base
			f.Kind = "pod.imagepull"
			f.Severity = emit.SeverityCritical
			f.Reason = w.Reason
			f.Message = w.Message
			f.Details = details(emit.Field{Key: "image", Value: cs.Image})
			s.add(f)
			return true
		case errorWaitingReasons[w.Reason]:
			f := base
			f.Kind = "pod.waiting"
			f.Severity = emit.SeverityWarning
			f.Reason = w.Reason
			f.Message = w.Message
			f.Details = details()
			s.add(f)
			return true
		}
	}

	if t := cs.LastTerminationState.Terminated; t != nil && t.Reason == "OOMKilled" {
		f := base
		f.Kind = "pod.oomkilled"
		f.Severity = emit.SeverityWarning
		f.Reason = "OOMKilled"
		f.Details = details(restarts, emit.Field{Key: "exit_code", Value: itoa32(t.ExitCode)})
		s.add(f)
		return true
	}

	if int(cs.RestartCount) >= s.th.restarts {
		f := base
		f.Kind = "pod.restarts"
		f.Severity = emit.SeverityWarning
		f.Reason = "ExcessiveRestarts"
		f.Details = details(restarts)
		s.add(f)
		return true
	}

	// Not-ready in a pod that claims Running: flagged only past
	// the grace window and never for terminating pods (their
	// containers go not-ready by design on the way down).
	if !cs.Ready && prefix == "" && pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
		started := pod.CreationTimestamp.Time
		if pod.Status.StartTime != nil {
			started = pod.Status.StartTime.Time
		}
		if s.now.Sub(started) >= s.th.pendingAge {
			f := base
			f.Kind = "pod.notready"
			f.Severity = emit.SeverityWarning
			f.Reason = "ContainersNotReady"
			f.Details = details(restarts, emit.Field{Key: "age", Value: s.age(started)})
			s.add(f)
			return true
		}
	}
	return false
}

// checkWorkloads derives workload.rollout / workload.stalled for
// Deployments, StatefulSets, and DaemonSets whose status has not
// converged on spec. skipAddons excludes kube-system objects the
// system class reports with richer context (addon.degraded), so one
// degraded add-on does not produce two findings.
func (s *scanner) checkWorkloads(deps []appsv1.Deployment, stss []appsv1.StatefulSet, dss []appsv1.DaemonSet, skipAddons bool) {
	for i := range deps {
		d := &deps[i]
		if skipAddons && d.Namespace == metav1.NamespaceSystem && addonRole(d.Name, d.Labels) != "" {
			continue
		}
		s.checkDeployment(d)
	}
	for i := range stss {
		s.checkStatefulSet(&stss[i])
	}
	for i := range dss {
		d := &dss[i]
		if skipAddons && d.Namespace == metav1.NamespaceSystem && addonRole(d.Name, d.Labels) != "" {
			continue
		}
		s.checkDaemonSet(d)
	}
}

func (s *scanner) checkDeployment(d *appsv1.Deployment) {
	if d.Spec.Paused {
		return // an intentionally paused rollout is not a delta
	}
	desired := int32(1)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	if desired == 0 {
		return // scaled to zero on purpose
	}
	rollout := rolloutDetails(desired, d.Status.ReadyReplicas, d.Status.UpdatedReplicas, d.Status.AvailableReplicas)

	// Stalled beats lagging: the controller itself has given up.
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse {
			s.add(emit.Finding{
				Kind:         "workload.stalled",
				Severity:     emit.SeverityCritical,
				Namespace:    d.Namespace,
				KindOfObject: "Deployment",
				Name:         d.Name,
				Reason:       c.Reason,
				Message:      c.Message,
				Details:      rollout,
			})
			return
		}
	}
	if d.Status.ReadyReplicas < desired || d.Status.UpdatedReplicas < desired {
		sev := emit.SeverityWarning
		if d.Status.AvailableReplicas == 0 {
			sev = emit.SeverityCritical // nothing serving at all
		}
		s.add(emit.Finding{
			Kind:         "workload.rollout",
			Severity:     sev,
			Namespace:    d.Namespace,
			KindOfObject: "Deployment",
			Name:         d.Name,
			Reason:       "RolloutIncomplete",
			Details:      rollout,
		})
	}
}

func (s *scanner) checkStatefulSet(st *appsv1.StatefulSet) {
	desired := int32(1)
	if st.Spec.Replicas != nil {
		desired = *st.Spec.Replicas
	}
	if desired == 0 {
		return
	}
	if st.Status.ReadyReplicas < desired || st.Status.UpdatedReplicas < desired {
		sev := emit.SeverityWarning
		if st.Status.ReadyReplicas == 0 {
			sev = emit.SeverityCritical
		}
		s.add(emit.Finding{
			Kind:         "workload.rollout",
			Severity:     sev,
			Namespace:    st.Namespace,
			KindOfObject: "StatefulSet",
			Name:         st.Name,
			Reason:       "RolloutIncomplete",
			Details:      rolloutDetails(desired, st.Status.ReadyReplicas, st.Status.UpdatedReplicas, st.Status.AvailableReplicas),
		})
	}
}

func (s *scanner) checkDaemonSet(d *appsv1.DaemonSet) {
	desired := d.Status.DesiredNumberScheduled
	if desired == 0 {
		return // no nodes want this DaemonSet
	}
	if d.Status.NumberReady < desired || d.Status.UpdatedNumberScheduled < desired {
		sev := emit.SeverityWarning
		if d.Status.NumberReady == 0 {
			sev = emit.SeverityCritical
		}
		s.add(emit.Finding{
			Kind:         "workload.rollout",
			Severity:     sev,
			Namespace:    d.Namespace,
			KindOfObject: "DaemonSet",
			Name:         d.Name,
			Reason:       "RolloutIncomplete",
			Details:      rolloutDetails(desired, d.Status.NumberReady, d.Status.UpdatedNumberScheduled, d.Status.NumberAvailable),
		})
	}
}

// rolloutDetails is the shared desired/ready/updated/available block.
func rolloutDetails(desired, ready, updated, available int32) []emit.Field {
	return []emit.Field{
		{Key: "desired", Value: itoa32(desired)},
		{Key: "ready", Value: itoa32(ready)},
		{Key: "updated", Value: itoa32(updated)},
		{Key: "available", Value: itoa32(available)},
	}
}

// checkJobs flags Jobs the controller has marked Failed. Active and
// suspended Jobs are nominal.
func (s *scanner) checkJobs(jobs []batchv1.Job) {
	for i := range jobs {
		j := &jobs[i]
		for _, c := range j.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
				reason := c.Reason
				if reason == "" {
					reason = "JobFailed"
				}
				s.add(emit.Finding{
					Kind:         "job.failed",
					Severity:     emit.SeverityWarning,
					Namespace:    j.Namespace,
					KindOfObject: "Job",
					Name:         j.Name,
					Reason:       reason,
					Message:      c.Message,
					Details:      []emit.Field{{Key: "failed", Value: itoa32(j.Status.Failed)}},
				})
				break
			}
		}
	}
}

func podCondition(pod *corev1.Pod, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == t {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

func ownedBy(refs []metav1.OwnerReference, kind string) bool {
	for _, r := range refs {
		if r.Kind == kind {
			return true
		}
	}
	return false
}
