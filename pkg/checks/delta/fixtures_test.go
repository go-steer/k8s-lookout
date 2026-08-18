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

// §13 fixtures: minimal but status-complete objects for the
// fake.Clientset. Every builder is either explicitly healthy (must
// emit nothing) or exhibits exactly one pathology.

import (
	"context"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// testNow is the pinned scan clock; fixture timestamps are relative
// to it so ages in findings are golden-testable.
var testNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// testCommand returns a triage-delta command bound to a fake
// clientset seeded with objs and to the pinned clock.
func testCommand(objs ...runtime.Object) checks.Command {
	client := fake.NewClientset(objs...)
	source := func(context.Context) (kubernetes.Interface, error) { return client, nil }
	return newCommand(source, func() time.Time { return testNow })
}

func ago(d time.Duration) metav1.Time { return metav1.Time{Time: testNow.Add(-d)} }

func ptr[T any](v T) *T { return &v }

// --- pods -----------------------------------------------------------------

func basePod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, CreationTimestamp: ago(time.Hour)},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: ptr(ago(time.Hour)),
		},
	}
}

func healthyPod(ns, name string) *corev1.Pod {
	p := basePod(ns, name)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "app", Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(time.Hour)}},
	}}
	return p
}

func crashloopPod(ns, name string) *corev1.Pod {
	p := basePod(ns, name)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "app", Ready: false, RestartCount: 12,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason: "CrashLoopBackOff", Message: "back-off 5m0s restarting failed container",
		}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: "Error", ExitCode: 1,
		}},
	}}
	return p
}

func imagePullPod(ns, name string) *corev1.Pod {
	p := basePod(ns, name)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "app", Ready: false, Image: "ghcr.io/acme/api:v9",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
	}}
	return p
}

func oomPod(ns, name string) *corev1.Pod {
	p := basePod(ns, name)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "worker", Ready: true, RestartCount: 3,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(10 * time.Minute)}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: "OOMKilled", ExitCode: 137,
		}},
	}}
	return p
}

func restartsPod(ns, name string, count int32) *corev1.Pod {
	p := basePod(ns, name)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "app", Ready: true, RestartCount: count,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(time.Minute)}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: "Error", ExitCode: 2,
		}},
	}}
	return p
}

func pendingPod(ns, name string, age time.Duration, unschedulable bool) *corev1.Pod {
	p := basePod(ns, name)
	p.CreationTimestamp = ago(age)
	p.Status = corev1.PodStatus{Phase: corev1.PodPending}
	if unschedulable {
		p.Status.Conditions = []corev1.PodCondition{{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
			Reason: corev1.PodReasonUnschedulable, Message: "0/3 nodes are available: 3 Insufficient cpu.",
		}}
	}
	return p
}

func notReadyPod(ns, name string) *corev1.Pod {
	p := basePod(ns, name)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "app", Ready: false, RestartCount: 1,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ago(30 * time.Minute)}},
	}}
	return p
}

func evictedPod(ns, name string) *corev1.Pod {
	p := basePod(ns, name)
	p.Status = corev1.PodStatus{
		Phase: corev1.PodFailed, Reason: "Evicted",
		Message: "The node was low on resource: ephemeral-storage.",
	}
	return p
}

// --- workloads ------------------------------------------------------------

func healthyDeployment(ns, name string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr(replicas)},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: replicas, UpdatedReplicas: replicas, AvailableReplicas: replicas,
		},
	}
}

func rolloutDeployment(ns, name string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr(int32(3))},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1,
			Conditions: []appsv1.DeploymentCondition{{
				Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "ReplicaSetUpdated",
			}},
		},
	}
}

func stalledDeployment(ns, name string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr(int32(4))},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: 2, UpdatedReplicas: 2, AvailableReplicas: 2,
			Conditions: []appsv1.DeploymentCondition{{
				Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
				Reason: "ProgressDeadlineExceeded", Message: "ReplicaSet \"api-7d4b9\" has timed out progressing.",
			}},
		},
	}
}

// replicaFailureDeployment is the quota-denial shape: the controller
// created NO pods, so status is all zeros and the only evidence in the
// cluster is the condition the deployment controller lifted off its
// ReplicaSet.
func replicaFailureDeployment(ns, name string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr(int32(3))},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: 0, UpdatedReplicas: 0, AvailableReplicas: 0,
			Conditions: []appsv1.DeploymentCondition{{
				Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue,
				Reason:  "FailedCreate",
				Message: `pods "etl-5c9f8-" is forbidden: exceeded quota: compute, requested: pods=1, used: pods=10, limited: pods=10`,
			}},
		},
	}
}

// replicaFailureAndStalledDeployment carries both conditions, which is
// what a real quota denial looks like after the progress deadline
// passes. Only the more specific one should be reported.
func replicaFailureAndStalledDeployment(ns, name string) *appsv1.Deployment {
	d := replicaFailureDeployment(ns, name)
	d.Status.Conditions = append(d.Status.Conditions, appsv1.DeploymentCondition{
		Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
		Reason: "ProgressDeadlineExceeded", Message: "ReplicaSet \"etl-5c9f8\" has timed out progressing.",
	})
	return d
}

// clearedReplicaFailureDeployment has the condition present but False —
// the controller records the recovery rather than deleting it.
func clearedReplicaFailureDeployment(ns, name string, replicas int32) *appsv1.Deployment {
	d := healthyDeployment(ns, name, replicas)
	d.Status.Conditions = []appsv1.DeploymentCondition{{
		Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionFalse, Reason: "FailedCreate",
	}}
	return d
}

func rolloutStatefulSet(ns, name string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr(int32(3))},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 0, UpdatedReplicas: 3},
	}
}

func rolloutDaemonSet(ns, name string, desired, ready int32) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status: appsv1.DaemonSetStatus{
			DesiredNumberScheduled: desired, NumberReady: ready,
			UpdatedNumberScheduled: desired, NumberAvailable: ready,
		},
	}
}

func failedJob(ns, name string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status: batchv1.JobStatus{
			Failed: 4,
			Conditions: []batchv1.JobCondition{{
				Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
				Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit",
			}},
		},
	}
}

// --- nodes ----------------------------------------------------------------

func healthyNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
			{Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse},
			{Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse},
		}},
	}
}

func notReadyNode(name string) *corev1.Node {
	n := healthyNode(name)
	n.Status.Conditions[0] = corev1.NodeCondition{
		Type: corev1.NodeReady, Status: corev1.ConditionUnknown,
		Reason: "NodeStatusUnknown", Message: "Kubelet stopped posting node status.",
		LastTransitionTime: ago(15 * time.Minute),
	}
	return n
}

func pressureNode(name string) *corev1.Node {
	n := healthyNode(name)
	n.Status.Conditions[2] = corev1.NodeCondition{
		Type: corev1.NodeDiskPressure, Status: corev1.ConditionTrue,
		Reason: "KubeletHasDiskPressure", Message: "kubelet has disk pressure",
	}
	return n
}

// npdNode carries one node-problem-detector-style condition set True.
func npdNode(name string, condType corev1.NodeConditionType, reason string) *corev1.Node {
	n := healthyNode(name)
	n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
		Type: condType, Status: corev1.ConditionTrue, Reason: reason,
	})
	return n
}

func cordonedNode(name string) *corev1.Node {
	n := healthyNode(name)
	n.Spec.Unschedulable = true
	return n
}

func preemptNode(name, taint string) *corev1.Node {
	n := healthyNode(name)
	n.Spec.Taints = []corev1.Taint{{Key: taint, Effect: corev1.TaintEffectNoSchedule}}
	return n
}

// podOnNode pins a healthy pod to a node for occupancy counts.
func podOnNode(ns, name, node string) *corev1.Pod {
	p := healthyPod(ns, name)
	p.Spec.NodeName = node
	return p
}

// --- pdb / system / quota --------------------------------------------------

func pdb(ns, name string, allowed, healthy, desired, expected int32) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status: policyv1.PodDisruptionBudgetStatus{
			DisruptionsAllowed: allowed, CurrentHealthy: healthy,
			DesiredHealthy: desired, ExpectedPods: expected,
		},
	}
}

func systemDeployment(name string, labels map[string]string, desired, ready int32) *appsv1.Deployment {
	d := healthyDeployment(metav1.NamespaceSystem, name, desired)
	d.Labels = labels
	d.Status.ReadyReplicas = ready
	d.Status.AvailableReplicas = ready
	d.Status.UpdatedReplicas = desired
	return d
}

func systemDaemonSet(name string, labels map[string]string, desired, ready int32) *appsv1.DaemonSet {
	d := rolloutDaemonSet(metav1.NamespaceSystem, name, desired, ready)
	d.Labels = labels
	return d
}

func quota(ns, name string, resources map[string][2]string) *corev1.ResourceQuota {
	hard, used := corev1.ResourceList{}, corev1.ResourceList{}
	for res, hu := range resources {
		hard[corev1.ResourceName(res)] = resource.MustParse(hu[0])
		used[corev1.ResourceName(res)] = resource.MustParse(hu[1])
	}
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status:     corev1.ResourceQuotaStatus{Hard: hard, Used: used},
	}
}
