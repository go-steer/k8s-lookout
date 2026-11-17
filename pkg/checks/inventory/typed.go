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

package inventory

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// listTypedKind lists any of the 22 built-in GVRs through the Protobuf-backed
// typed kubernetes.Interface, projecting each page directly into compact object
// rows via kube.ListAllPagesMap. Non-builtin resources (CRDs and aggregated
// APIs) return handled=false so the caller falls back to dynamic.Interface.
func listTypedKind(
	ctx context.Context,
	client kubernetes.Interface,
	k kindSpec,
	ns string,
	now time.Time,
) (items []object, handled bool, err error) {
	listNS := ns
	if !k.namespaced {
		listNS = ""
	}
	for _, fn := range []func(context.Context, kubernetes.Interface, kindSpec, string, time.Time) ([]object, bool, error){
		listWorkloadTypedKind,
		listCorePodNetTypedKind,
		listCoreConfigTypedKind,
		listPolicyClusterTypedKind,
	} {
		if items, ok, err := fn(ctx, client, k, listNS, now); ok {
			return items, true, err
		}
	}
	return nil, false, nil
}

func listWorkloadTypedKind(
	ctx context.Context,
	client kubernetes.Interface,
	k kindSpec,
	listNS string,
	now time.Time,
) (items []object, handled bool, err error) {
	switch k.gvr {
	case gvr("apps", "v1", "deployments"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
			l, err := client.AppsV1().Deployments(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(d *appsv1.Deployment) object {
			return newTypedObject("Deployment", d.Namespace, d.Name, d.CreationTimestamp, now, func(kv *kv) {
				desired := int32(1)
				if d.Spec.Replicas != nil {
					desired = *d.Spec.Replicas
				}
				kv.addf("ready", "%d/%d", d.Status.ReadyReplicas, desired)
				kv.addf("up_to_date", "%d", d.Status.UpdatedReplicas)
				kv.addf("available", "%d", d.Status.AvailableReplicas)
			})
		})
		return items, true, err

	case gvr("apps", "v1", "statefulsets"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
			l, err := client.AppsV1().StatefulSets(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(s *appsv1.StatefulSet) object {
			return newTypedObject("StatefulSet", s.Namespace, s.Name, s.CreationTimestamp, now, func(kv *kv) {
				desired := int32(1)
				if s.Spec.Replicas != nil {
					desired = *s.Spec.Replicas
				}
				kv.addf("ready", "%d/%d", s.Status.ReadyReplicas, desired)
			})
		})
		return items, true, err

	case gvr("apps", "v1", "daemonsets"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]appsv1.DaemonSet, string, error) {
			l, err := client.AppsV1().DaemonSets(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(d *appsv1.DaemonSet) object {
			return newTypedObject("DaemonSet", d.Namespace, d.Name, d.CreationTimestamp, now, func(kv *kv) {
				kv.addf("ready", "%d/%d", d.Status.NumberReady, d.Status.DesiredNumberScheduled)
				kv.addf("up_to_date", "%d", d.Status.UpdatedNumberScheduled)
				kv.addf("available", "%d", d.Status.NumberAvailable)
			})
		})
		return items, true, err

	case gvr("apps", "v1", "replicasets"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]appsv1.ReplicaSet, string, error) {
			l, err := client.AppsV1().ReplicaSets(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(r *appsv1.ReplicaSet) object {
			return newTypedObject("ReplicaSet", r.Namespace, r.Name, r.CreationTimestamp, now, func(kv *kv) {
				desired := int32(1)
				if r.Spec.Replicas != nil {
					desired = *r.Spec.Replicas
				}
				kv.addf("ready", "%d/%d", r.Status.ReadyReplicas, desired)
			})
		})
		return items, true, err

	case gvr("batch", "v1", "cronjobs"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]batchv1.CronJob, string, error) {
			l, err := client.BatchV1().CronJobs(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(cj *batchv1.CronJob) object {
			return newTypedObject("CronJob", cj.Namespace, cj.Name, cj.CreationTimestamp, now, func(kv *kv) {
				kv.add("schedule", cj.Spec.Schedule)
				if cj.Spec.TimeZone != nil {
					kv.add("timezone", *cj.Spec.TimeZone)
				}
				if cj.Spec.Suspend != nil && *cj.Spec.Suspend {
					kv.add("suspend", "true")
				}
				kv.addf("active", "%d", len(cj.Status.Active))
				if cj.Status.LastScheduleTime != nil && !cj.Status.LastScheduleTime.IsZero() {
					kv.add("last_schedule", kv.since(cj.Status.LastScheduleTime.UTC().Format(time.RFC3339)))
				}
			})
		})
		return items, true, err

	case gvr("batch", "v1", "jobs"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]batchv1.Job, string, error) {
			l, err := client.BatchV1().Jobs(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(j *batchv1.Job) object {
			return newTypedObject("Job", j.Namespace, j.Name, j.CreationTimestamp, now, func(kv *kv) {
				kv.add("status", typedJobStatus(j))
				want := int32(1)
				if j.Spec.Completions != nil {
					want = *j.Spec.Completions
				}
				kv.addf("completions", "%d/%d", j.Status.Succeeded, want)
			})
		})
		return items, true, err
	}
	return nil, false, nil
}

func listCorePodNetTypedKind(
	ctx context.Context,
	client kubernetes.Interface,
	k kindSpec,
	listNS string,
	now time.Time,
) (items []object, handled bool, err error) {
	switch k.gvr {
	case gvr("", "v1", "pods"):
		items, err = kube.ListClusterPodsMap(ctx, client, listNS, func(p *corev1.Pod) (object, bool) {
			return newTypedObject("Pod", p.Namespace, p.Name, p.CreationTimestamp, now, func(kv *kv) {
				ready, restarts := 0, 0
				for i := range p.Status.ContainerStatuses {
					if p.Status.ContainerStatuses[i].Ready {
						ready++
					}
					restarts += int(p.Status.ContainerStatuses[i].RestartCount)
				}
				for i := range p.Status.InitContainerStatuses {
					restarts += int(p.Status.InitContainerStatuses[i].RestartCount)
				}
				kv.addf("ready", "%d/%d", ready, max(len(p.Spec.Containers), len(p.Status.ContainerStatuses)))
				kv.add("status", typedPodStatus(p))
				kv.addf("restarts", "%d", restarts)
			}), true
		})
		if err == nil {
			sort.Slice(items, func(i, j int) bool {
				if items[i].namespace != items[j].namespace {
					return items[i].namespace < items[j].namespace
				}
				return items[i].name < items[j].name
			})
		}
		return items, true, err

	case gvr("", "v1", "services"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.Service, string, error) {
			l, err := client.CoreV1().Services(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(s *corev1.Service) object {
			return newTypedObject("Service", s.Namespace, s.Name, s.CreationTimestamp, now, func(kv *kv) {
				kv.add("type", string(s.Spec.Type))
				kv.add("cluster_ip", s.Spec.ClusterIP)
				kv.add("external_ip", typedServiceExternalIPs(s))
				ports := make([]string, 0, len(s.Spec.Ports))
				for i := range s.Spec.Ports {
					p := &s.Spec.Ports[i]
					proto := string(p.Protocol)
					if proto == "" {
						proto = "TCP"
					}
					port := strconv.Itoa(int(p.Port))
					if p.NodePort != 0 {
						port += ":" + strconv.Itoa(int(p.NodePort))
					}
					ports = append(ports, port+"/"+proto)
				}
				kv.add("ports", strings.Join(ports, ","))
			})
		})
		return items, true, err

	case gvr("", "v1", "endpoints"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.Endpoints, string, error) { //nolint:staticcheck // SA1019: kubectl-equivalent Endpoints inventory row
			l, err := client.CoreV1().Endpoints(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(ep *corev1.Endpoints) object { //nolint:staticcheck // SA1019: kubectl-equivalent Endpoints inventory row
			return newTypedObject("Endpoints", ep.Namespace, ep.Name, ep.CreationTimestamp, now, func(kv *kv) {
				n := 0
				for i := range ep.Subsets {
					n += len(ep.Subsets[i].Addresses)
				}
				kv.addf("addresses", "%d", n)
			})
		})
		return items, true, err

	case gvr("networking.k8s.io", "v1", "ingresses"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]networkingv1.Ingress, string, error) {
			l, err := client.NetworkingV1().Ingresses(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(ing *networkingv1.Ingress) object {
			return newTypedObject("Ingress", ing.Namespace, ing.Name, ing.CreationTimestamp, now, func(kv *kv) {
				if ing.Spec.IngressClassName != nil {
					kv.add("class", *ing.Spec.IngressClassName)
				}
				var hosts []string
				for i := range ing.Spec.Rules {
					if h := ing.Spec.Rules[i].Host; h != "" {
						hosts = append(hosts, h)
					}
				}
				kv.add("hosts", strings.Join(hosts, ","))
				var addr string
				for i := range ing.Status.LoadBalancer.Ingress {
					a := ing.Status.LoadBalancer.Ingress[i].IP
					if a == "" {
						a = ing.Status.LoadBalancer.Ingress[i].Hostname
					}
					addr = joinNonEmpty(addr, a)
				}
				kv.add("address", addr)
			})
		})
		return items, true, err
	}
	return nil, false, nil
}

func listCoreConfigTypedKind(
	ctx context.Context,
	client kubernetes.Interface,
	k kindSpec,
	listNS string,
	now time.Time,
) (items []object, handled bool, err error) {
	switch k.gvr {
	case gvr("", "v1", "configmaps"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.ConfigMap, string, error) {
			l, err := client.CoreV1().ConfigMaps(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(cm *corev1.ConfigMap) object {
			return newTypedObject("ConfigMap", cm.Namespace, cm.Name, cm.CreationTimestamp, now, func(kv *kv) {
				kv.addf("keys", "%d", len(cm.Data)+len(cm.BinaryData))
			})
		})
		return items, true, err

	case gvr("", "v1", "secrets"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.Secret, string, error) {
			l, err := client.CoreV1().Secrets(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(sec *corev1.Secret) object {
			return newTypedObject("Secret", sec.Namespace, sec.Name, sec.CreationTimestamp, now, func(kv *kv) {
				kv.add("type", string(sec.Type))
				kv.addf("keys", "%d", len(sec.Data)+len(sec.StringData))
			})
		})
		return items, true, err

	case gvr("", "v1", "persistentvolumeclaims"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.PersistentVolumeClaim, string, error) {
			l, err := client.CoreV1().PersistentVolumeClaims(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(pvc *corev1.PersistentVolumeClaim) object {
			return newTypedObject("PersistentVolumeClaim", pvc.Namespace, pvc.Name, pvc.CreationTimestamp, now, func(kv *kv) {
				kv.add("phase", string(pvc.Status.Phase))
				kv.add("volume", pvc.Spec.VolumeName)
				if q, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
					kv.add("capacity", q.String())
				}
				kv.add("access_modes", typedAccessModes(pvc.Spec.AccessModes))
				if pvc.Spec.StorageClassName != nil {
					kv.add("class", *pvc.Spec.StorageClassName)
				}
			})
		})
		return items, true, err

	case gvr("", "v1", "serviceaccounts"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.ServiceAccount, string, error) {
			l, err := client.CoreV1().ServiceAccounts(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(sa *corev1.ServiceAccount) object {
			return newTypedObject("ServiceAccount", sa.Namespace, sa.Name, sa.CreationTimestamp, now, nil)
		})
		return items, true, err

	case gvr("", "v1", "resourcequotas"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.ResourceQuota, string, error) {
			l, err := client.CoreV1().ResourceQuotas(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(rq *corev1.ResourceQuota) object {
			return newTypedObject("ResourceQuota", rq.Namespace, rq.Name, rq.CreationTimestamp, now, nil)
		})
		return items, true, err

	case gvr("", "v1", "limitranges"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.LimitRange, string, error) {
			l, err := client.CoreV1().LimitRanges(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(lr *corev1.LimitRange) object {
			return newTypedObject("LimitRange", lr.Namespace, lr.Name, lr.CreationTimestamp, now, nil)
		})
		return items, true, err
	}
	return nil, false, nil
}

func listPolicyClusterTypedKind(
	ctx context.Context,
	client kubernetes.Interface,
	k kindSpec,
	listNS string,
	now time.Time,
) (items []object, handled bool, err error) {
	switch k.gvr {
	case gvr("autoscaling", "v2", "horizontalpodautoscalers"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]autoscalingv2.HorizontalPodAutoscaler, string, error) {
			l, err := client.AutoscalingV2().HorizontalPodAutoscalers(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(hpa *autoscalingv2.HorizontalPodAutoscaler) object {
			return newTypedObject("HorizontalPodAutoscaler", hpa.Namespace, hpa.Name, hpa.CreationTimestamp, now, func(kv *kv) {
				ref := hpa.Spec.ScaleTargetRef
				if ref.Kind != "" || ref.Name != "" {
					kv.add("scale_target", ref.Kind+"/"+ref.Name)
				}
				minReplicas := int32(1)
				if hpa.Spec.MinReplicas != nil {
					minReplicas = *hpa.Spec.MinReplicas
				}
				kv.addf("min", "%d", minReplicas)
				kv.addf("max", "%d", hpa.Spec.MaxReplicas)
				kv.addf("replicas", "%d", hpa.Status.CurrentReplicas)
			})
		})
		return items, true, err

	case gvr("policy", "v1", "poddisruptionbudgets"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]policyv1.PodDisruptionBudget, string, error) {
			l, err := client.PolicyV1().PodDisruptionBudgets(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(pdb *policyv1.PodDisruptionBudget) object {
			return newTypedObject("PodDisruptionBudget", pdb.Namespace, pdb.Name, pdb.CreationTimestamp, now, func(kv *kv) {
				if pdb.Spec.MinAvailable != nil {
					kv.add("min_available", pdb.Spec.MinAvailable.String())
				}
				if pdb.Spec.MaxUnavailable != nil {
					kv.add("max_unavailable", pdb.Spec.MaxUnavailable.String())
				}
				kv.addf("allowed_disruptions", "%d", pdb.Status.DisruptionsAllowed)
			})
		})
		return items, true, err

	case gvr("networking.k8s.io", "v1", "networkpolicies"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]networkingv1.NetworkPolicy, string, error) {
			l, err := client.NetworkingV1().NetworkPolicies(listNS).List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(np *networkingv1.NetworkPolicy) object {
			return newTypedObject("NetworkPolicy", np.Namespace, np.Name, np.CreationTimestamp, now, func(kv *kv) {
				kv.add("pod_selector", typedPodSelector(np.Spec.PodSelector))
			})
		})
		return items, true, err

	case gvr("", "v1", "nodes"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.Node, string, error) {
			l, err := client.CoreV1().Nodes().List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(n *corev1.Node) object {
			return newTypedObject("Node", "", n.Name, n.CreationTimestamp, now, func(kv *kv) {
				kv.add("status", typedNodeStatus(n))
				kv.add("roles", typedNodeRoles(n.Labels))
				kv.add("version", n.Status.NodeInfo.KubeletVersion)
			})
		})
		return items, true, err

	case gvr("", "v1", "namespaces"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.Namespace, string, error) {
			l, err := client.CoreV1().Namespaces().List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(nsObj *corev1.Namespace) object {
			return newTypedObject("Namespace", "", nsObj.Name, nsObj.CreationTimestamp, now, func(kv *kv) {
				kv.add("phase", string(nsObj.Status.Phase))
			})
		})
		return items, true, err

	case gvr("", "v1", "persistentvolumes"):
		items, err = listAndSortTyped(ctx, func(ctx context.Context, o metav1.ListOptions) ([]corev1.PersistentVolume, string, error) {
			l, err := client.CoreV1().PersistentVolumes().List(ctx, o)
			if err != nil {
				return nil, "", err
			}
			return l.Items, l.Continue, nil
		}, func(pv *corev1.PersistentVolume) object {
			return newTypedObject("PersistentVolume", "", pv.Name, pv.CreationTimestamp, now, func(kv *kv) {
				kv.add("phase", string(pv.Status.Phase))
				if q, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok {
					kv.add("capacity", q.String())
				}
				kv.add("access_modes", typedAccessModes(pv.Spec.AccessModes))
				if pv.Spec.ClaimRef != nil {
					kv.add("claim", pv.Spec.ClaimRef.Namespace+"/"+pv.Spec.ClaimRef.Name)
				}
				kv.add("class", pv.Spec.StorageClassName)
			})
		})
		return items, true, err
	}

	return nil, false, nil
}

func listAndSortTyped[T any](
	ctx context.Context,
	list func(context.Context, metav1.ListOptions) ([]T, string, error),
	project func(*T) object,
) ([]object, error) {
	out, err := kube.ListAllPagesMap(ctx, list, func(item *T) (object, bool) {
		return project(item), true
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].namespace != out[j].namespace {
			return out[i].namespace < out[j].namespace
		}
		return out[i].name < out[j].name
	})
	return out, nil
}

func newTypedObject(kind, ns, name string, created metav1.Time, now time.Time, format func(*kv)) object {
	o := object{
		kind:      kind,
		namespace: ns,
		name:      name,
	}
	k := &kv{now: now}
	k.add("target", o.target())
	if format != nil {
		format(k)
	}
	if !created.IsZero() {
		k.add("age", compactAge(now.Sub(created.Time)))
	}
	o.details = k.fields
	return o
}

func typedPodStatus(p *corev1.Pod) string {
	if p.DeletionTimestamp != nil {
		return "Terminating"
	}
	if p.Status.Reason != "" {
		return p.Status.Reason
	}
	for i := range p.Status.InitContainerStatuses {
		c := &p.Status.InitContainerStatuses[i]
		if c.State.Waiting != nil && c.State.Waiting.Reason != "" && c.State.Waiting.Reason != "PodInitializing" {
			return "Init:" + c.State.Waiting.Reason
		}
		if c.State.Terminated != nil && c.State.Terminated.ExitCode != 0 && c.State.Terminated.Reason != "" {
			return "Init:" + c.State.Terminated.Reason
		}
	}
	for i := range p.Status.ContainerStatuses {
		c := &p.Status.ContainerStatuses[i]
		if c.State.Waiting != nil && c.State.Waiting.Reason != "" {
			return c.State.Waiting.Reason
		}
		if c.State.Terminated != nil && c.State.Terminated.Reason != "" && c.State.Terminated.Reason != "Completed" {
			return c.State.Terminated.Reason
		}
	}
	return string(p.Status.Phase)
}

func typedJobStatus(j *batchv1.Job) string {
	for i := range j.Status.Conditions {
		c := &j.Status.Conditions[i]
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return "Complete"
		case batchv1.JobFailed:
			return "Failed"
		case batchv1.JobSuspended:
			return "Suspended"
		}
	}
	return "Running"
}

func typedServiceExternalIPs(s *corev1.Service) string {
	var addrs string
	for i := range s.Status.LoadBalancer.Ingress {
		a := s.Status.LoadBalancer.Ingress[i].IP
		if a == "" {
			a = s.Status.LoadBalancer.Ingress[i].Hostname
		}
		addrs = joinNonEmpty(addrs, a)
	}
	if addrs == "" {
		for _, ip := range s.Spec.ExternalIPs {
			addrs = joinNonEmpty(addrs, ip)
		}
	}
	if addrs == "" && s.Spec.Type == corev1.ServiceTypeLoadBalancer {
		return "pending"
	}
	return addrs
}

func typedAccessModes(modes []corev1.PersistentVolumeAccessMode) string {
	var out []string
	for _, m := range modes {
		var s string
		switch m {
		case corev1.ReadWriteOnce:
			s = "RWO"
		case corev1.ReadOnlyMany:
			s = "ROX"
		case corev1.ReadWriteMany:
			s = "RWX"
		case corev1.ReadWriteOncePod:
			s = "RWOP"
		default:
			s = string(m)
		}
		if s != "" {
			out = append(out, s)
		}
	}
	return strings.Join(out, ",")
}

func typedPodSelector(sel metav1.LabelSelector) string {
	keys := make([]string, 0, len(sel.MatchLabels))
	for k := range sel.MatchLabels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys)+1)
	for _, k := range keys {
		parts = append(parts, k+"="+sel.MatchLabels[k])
	}
	if n := len(sel.MatchExpressions); n > 0 {
		parts = append(parts, fmt.Sprintf("+%d expressions", n))
	}
	if len(parts) == 0 {
		return "all"
	}
	return strings.Join(parts, ",")
}

func typedNodeStatus(n *corev1.Node) string {
	status := "Unknown"
	for i := range n.Status.Conditions {
		c := &n.Status.Conditions[i]
		if c.Type != corev1.NodeReady {
			continue
		}
		if c.Status == corev1.ConditionTrue {
			status = "Ready"
		} else {
			status = "NotReady"
		}
	}
	if n.Spec.Unschedulable {
		status += ",SchedulingDisabled"
	}
	return status
}

func typedNodeRoles(labels map[string]string) string {
	const prefix = "node-role.kubernetes.io/"
	var roles []string
	for k := range labels {
		if r, ok := strings.CutPrefix(k, prefix); ok && r != "" {
			roles = append(roles, r)
		}
	}
	if len(roles) == 0 {
		return "none"
	}
	sort.Strings(roles)
	return strings.Join(roles, ",")
}
