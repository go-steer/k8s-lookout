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
	"strings"
	"testing"
	"time"
)

// render runs one kind's formatter and returns the line it would
// contribute, so each case reads as the kubectl row it mirrors.
func render(kind string, o map[string]any) string {
	k := &kv{now: testNow}
	if f := formatters[kind]; f != nil {
		f(k, o)
	}
	parts := make([]string, 0, len(k.fields))
	for _, f := range k.fields {
		parts = append(parts, f.Key+"="+f.Value)
	}
	return strings.Join(parts, " ")
}

// TestFormatters pins each kind's line against the columns `kubectl
// get <kind>` prints. A case here is the whole argument for a field
// being present: if it is not a default-table column, it does not
// belong (see the rule at the top of render.go).
func TestFormatters(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
		obj  map[string]any
		want string
	}{{
		name: "pod with a crashing container reports the reason kubectl reports",
		kind: "Pod",
		obj: map[string]any{
			"spec": map[string]any{"containers": []any{map[string]any{"name": "api"}}},
			"status": map[string]any{
				"phase": "Running",
				"containerStatuses": []any{map[string]any{
					"ready": false, "restartCount": int64(7),
					"state": map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
				}},
			},
		},
		want: "ready=0/1 status=CrashLoopBackOff restarts=7",
	}, {
		name: "a deleting pod is Terminating whatever its phase says",
		kind: "Pod",
		obj: map[string]any{
			"metadata": map[string]any{"deletionTimestamp": ago(time.Minute)},
			"spec":     map[string]any{"containers": []any{map[string]any{"name": "api"}}},
			"status": map[string]any{
				"phase":             "Running",
				"containerStatuses": []any{map[string]any{"ready": true}},
			},
		},
		want: "ready=1/1 status=Terminating restarts=0",
	}, {
		name: "an evicted pod reports status.reason",
		kind: "Pod",
		obj: map[string]any{
			"spec":   map[string]any{"containers": []any{map[string]any{"name": "api"}}},
			"status": map[string]any{"phase": "Failed", "reason": "Evicted"},
		},
		want: "ready=0/1 status=Evicted restarts=0",
	}, {
		name: "a failing init container is prefixed as kubectl prefixes it",
		kind: "Pod",
		obj: map[string]any{
			"spec": map[string]any{"containers": []any{map[string]any{"name": "api"}}},
			"status": map[string]any{
				"phase": "Pending",
				"initContainerStatuses": []any{map[string]any{
					"restartCount": int64(3),
					"state":        map[string]any{"waiting": map[string]any{"reason": "ImagePullBackOff"}},
				}},
			},
		},
		want: "ready=0/1 status=Init:ImagePullBackOff restarts=3",
	}, {
		name: "PodInitializing is not a fault and does not mask the phase",
		kind: "Pod",
		obj: map[string]any{
			"spec": map[string]any{"containers": []any{map[string]any{"name": "api"}}},
			"status": map[string]any{
				"phase": "Pending",
				"initContainerStatuses": []any{map[string]any{
					"state": map[string]any{"waiting": map[string]any{"reason": "PodInitializing"}},
				}},
			},
		},
		want: "ready=0/1 status=Pending restarts=0",
	}, {
		name: "a workload that never set replicas still reads against the API default",
		kind: "Deployment",
		obj:  map[string]any{"status": map[string]any{}},
		want: "ready=0/1 up_to_date=0 available=0",
	}, {
		name: "replicasets carry the same ready pair, opt-in only",
		kind: "ReplicaSet",
		obj: map[string]any{
			"spec":   map[string]any{"replicas": int64(3)},
			"status": map[string]any{"readyReplicas": int64(3)},
		},
		want: "ready=3/3",
	}, {
		name: "a failed job is Failed, not Running",
		kind: "Job",
		obj: map[string]any{
			"spec": map[string]any{"completions": int64(5)},
			"status": map[string]any{
				"succeeded":  int64(2),
				"conditions": []any{map[string]any{"type": "Failed", "status": "True"}},
			},
		},
		want: "status=Failed completions=2/5",
	}, {
		name: "a suspended job says so",
		kind: "Job",
		obj: map[string]any{"status": map[string]any{
			"conditions": []any{map[string]any{"type": "Suspended", "status": "True"}},
		}},
		want: "status=Suspended completions=0/1",
	}, {
		name: "a condition that is not True is not the status",
		kind: "Job",
		obj: map[string]any{"status": map[string]any{
			"conditions": []any{map[string]any{"type": "Complete", "status": "False"}},
		}},
		want: "status=Running completions=0/1",
	}, {
		name: "an unsuspended cronjob omits suspend rather than saying false",
		kind: "CronJob",
		obj:  map[string]any{"spec": map[string]any{"schedule": "*/5 * * * *"}},
		want: `schedule=*/5 * * * * active=0`,
	}, {
		name: "a headless service keeps kubectl's None",
		kind: "Service",
		obj: map[string]any{"spec": map[string]any{
			"type": "ClusterIP", "clusterIP": "None",
			"ports": []any{map[string]any{"port": int64(9042)}},
		}},
		want: "type=ClusterIP cluster_ip=None ports=9042/TCP",
	}, {
		name: "a load balancer with an address reports it, not pending",
		kind: "Service",
		obj: map[string]any{
			"spec":   map[string]any{"type": "LoadBalancer", "clusterIP": "10.0.0.5"},
			"status": map[string]any{"loadBalancer": map[string]any{"ingress": []any{map[string]any{"hostname": "lb.example.com"}}}},
		},
		want: "type=LoadBalancer cluster_ip=10.0.0.5 external_ip=lb.example.com",
	}, {
		name: "spec.externalIPs count as an external address",
		kind: "Service",
		obj: map[string]any{"spec": map[string]any{
			"type": "ClusterIP", "clusterIP": "10.0.0.6", "externalIPs": []any{"203.0.113.7"},
		}},
		want: "type=ClusterIP cluster_ip=10.0.0.6 external_ip=203.0.113.7",
	}, {
		name: "an endpoints object sums addresses across subsets",
		kind: "Endpoints",
		obj: map[string]any{"subsets": []any{
			map[string]any{"addresses": []any{map[string]any{"ip": "10.8.0.1"}}},
			map[string]any{"addresses": []any{map[string]any{"ip": "10.8.0.2"}, map[string]any{"ip": "10.8.0.3"}}},
		}},
		want: "addresses=3",
	}, {
		name: "a pending PVC has no volume and no capacity to report",
		kind: "PersistentVolumeClaim",
		obj: map[string]any{
			"spec":   map[string]any{"accessModes": []any{"ReadWriteMany"}, "storageClassName": "nfs"},
			"status": map[string]any{"phase": "Pending"},
		},
		want: "phase=Pending access_modes=RWX class=nfs",
	}, {
		name: "a bound PV names its claim",
		kind: "PersistentVolume",
		obj: map[string]any{
			"spec": map[string]any{
				"capacity":         map[string]any{"storage": "100Gi"},
				"accessModes":      []any{"ReadWriteOnce"},
				"claimRef":         map[string]any{"namespace": "prod", "name": "data-0"},
				"storageClassName": "standard-rwo",
			},
			"status": map[string]any{"phase": "Bound"},
		},
		want: "phase=Bound capacity=100Gi access_modes=RWO claim=prod/data-0 class=standard-rwo",
	}, {
		name: "an unbound PV omits the claim rather than emitting a bare slash",
		kind: "PersistentVolume",
		obj: map[string]any{
			"spec":   map[string]any{"capacity": map[string]any{"storage": "1Gi"}, "claimRef": map[string]any{}},
			"status": map[string]any{"phase": "Available"},
		},
		want: "phase=Available capacity=1Gi",
	}, {
		name: "an HPA that never set minReplicas reads against the API default",
		kind: "HorizontalPodAutoscaler",
		obj: map[string]any{
			"spec":   map[string]any{"scaleTargetRef": map[string]any{"kind": "Deployment", "name": "api"}, "maxReplicas": int64(20)},
			"status": map[string]any{"currentReplicas": int64(4)},
		},
		want: "scale_target=Deployment/api min=1 max=20 replicas=4",
	}, {
		name: "a PDB expressed as maxUnavailable is not silently blank",
		kind: "PodDisruptionBudget",
		obj: map[string]any{
			"spec":   map[string]any{"maxUnavailable": int64(1)},
			"status": map[string]any{"disruptionsAllowed": int64(0)},
		},
		want: "max_unavailable=1 allowed_disruptions=0",
	}, {
		name: "a netpol selector renders its labels sorted",
		kind: "NetworkPolicy",
		obj: map[string]any{"spec": map[string]any{"podSelector": map[string]any{
			"matchLabels": map[string]any{"tier": "web", "app": "api"},
		}}},
		want: "pod_selector=app=api,tier=web",
	}, {
		name: "match expressions are counted, not expanded",
		kind: "NetworkPolicy",
		obj: map[string]any{"spec": map[string]any{"podSelector": map[string]any{
			"matchLabels":      map[string]any{"app": "api"},
			"matchExpressions": []any{map[string]any{"key": "tier"}, map[string]any{"key": "zone"}},
		}}},
		want: "pod_selector=app=api,+2 expressions",
	}, {
		name: "a cordoned node carries the marker that changes what a drain does",
		kind: "Node",
		obj: map[string]any{
			"spec": map[string]any{"unschedulable": true},
			"status": map[string]any{
				"conditions": []any{map[string]any{"type": "Ready", "status": "False"}},
				"nodeInfo":   map[string]any{"kubeletVersion": "v1.31.4"},
			},
		},
		want: "status=NotReady,SchedulingDisabled roles=none version=v1.31.4",
	}, {
		name: "a node with no Ready condition at all is Unknown, not Ready",
		kind: "Node",
		obj:  map[string]any{"status": map[string]any{"conditions": []any{}}},
		want: "status=Unknown roles=none",
	}, {
		name: "a terminating namespace is the whole point of listing namespaces",
		kind: "Namespace",
		obj:  map[string]any{"status": map[string]any{"phase": "Terminating"}},
		want: "phase=Terminating",
	}, {
		name: "a configmap counts binary data alongside data",
		kind: "ConfigMap",
		obj: map[string]any{
			"data":       map[string]any{"a": "1", "b": "2"},
			"binaryData": map[string]any{"c": "AA=="},
		},
		want: "keys=3",
	}, {
		name: "an ingress with no class or address says only what it knows",
		kind: "Ingress",
		obj: map[string]any{"spec": map[string]any{"rules": []any{
			map[string]any{"host": "a.example.com"}, map[string]any{"host": "b.example.com"},
		}}},
		want: "hosts=a.example.com,b.example.com",
	}, {
		name: "a kind with no formatter contributes nothing but its target and age",
		kind: "LimitRange",
		obj:  map[string]any{"spec": map[string]any{"limits": []any{map[string]any{}}}},
		want: "",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := render(tc.kind, tc.obj); got != tc.want {
				t.Errorf("render(%s):\n got: %s\nwant: %s", tc.kind, got, tc.want)
			}
		})
	}
}

// TestJSONRoundTrippedNumbersRead: an object that has been through
// JSON decodes its numbers as float64, not the int64 a live
// unstructured object carries. Reading only int64 would silently
// render every count as zero.
func TestJSONRoundTrippedNumbersRead(t *testing.T) {
	got := render("Deployment", map[string]any{
		"spec":   map[string]any{"replicas": float64(4)},
		"status": map[string]any{"readyReplicas": float64(4), "updatedReplicas": float64(4), "availableReplicas": float64(4)},
	})
	if got != "ready=4/4 up_to_date=4 available=4" {
		t.Errorf("render = %q", got)
	}
}
