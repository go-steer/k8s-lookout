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

// This file is the per-kind headline status: what an object's line
// says beyond its target.
//
// THE RULE, and it is deliberately mechanical: a kind's fields are
// the columns `kubectl get <kind>` prints in its own DEFAULT table,
// and nothing else. Not `-o wide`, not a field someone thought would
// be handy. That is what keeps this command an inventory rather than
// a check, and it settles the awkward cases by rule instead of by
// taste:
//
//   - an Endpoints object's address count is in, because `kubectl get
//     endpoints` prints its endpoints;
//   - a NetworkPolicy's pod selector is in, because `kubectl get
//     netpol` prints POD-SELECTOR;
//   - a SERVICE's selector is OUT, because `kubectl get svc` does not
//     print it — and, decisively, because "this Service's selector
//     matches no pods" is a judgement `state edges` already makes and
//     makes better. If enumeration diagnoses, the caller stops taking
//     the target to the tool that owns the diagnosis.
//     TestItDoesNotDiagnose pins this.
//
// Three documented departures, each of which keeps the rule's intent:
//
//  1. AGE is emitted for every kind. Every kubectl table has it, so
//     this is the rule applied uniformly rather than an exception.
//  2. Columns that are pure legacy are dropped: a ServiceAccount's
//     SECRETS count has been 0 on every cluster since 1.24.
//  3. Columns that would restate another command's judgement are
//     dropped: a ResourceQuota's REQUEST/LIMIT columns are quota
//     pressure, which `triage delta --only=quota` owns.
//
// A kind with no formatter prints its target and age alone, which is
// the right answer for a LimitRange or a CRD: existence IS the fact.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// formatters give each kind its headline status, keyed by canonical
// Kind. They read the unstructured object the dynamic client
// returned: eighteen unrelated schemas, two or three fields each, is
// not worth eighteen typed clients.
var formatters = map[string]func(*kv, map[string]any){
	// kubectl get pods: NAME READY STATUS RESTARTS AGE
	"Pod": func(k *kv, o map[string]any) {
		cs := sliceAt(o, "status", "containerStatuses")
		ready, restarts := 0, 0
		for _, c := range cs {
			m, _ := c.(map[string]any)
			if b, _ := m["ready"].(bool); b {
				ready++
			}
			restarts += intAt(m, "restartCount")
		}
		for _, c := range sliceAt(o, "status", "initContainerStatuses") {
			m, _ := c.(map[string]any)
			restarts += intAt(m, "restartCount")
		}
		k.addf("ready", "%d/%d", ready, max(len(sliceAt(o, "spec", "containers")), len(cs)))
		k.add("status", podStatus(o))
		k.addf("restarts", "%d", restarts)
	},
	// kubectl get deployments: NAME READY UP-TO-DATE AVAILABLE AGE
	"Deployment": func(k *kv, o map[string]any) {
		k.addf("ready", "%d/%d", intAt(o, "status", "readyReplicas"), desiredReplicas(o))
		k.addf("up_to_date", "%d", intAt(o, "status", "updatedReplicas"))
		k.addf("available", "%d", intAt(o, "status", "availableReplicas"))
	},
	// kubectl get statefulsets: NAME READY AGE
	"StatefulSet": func(k *kv, o map[string]any) {
		k.addf("ready", "%d/%d", intAt(o, "status", "readyReplicas"), desiredReplicas(o))
	},
	// kubectl get replicasets: NAME DESIRED CURRENT READY AGE
	"ReplicaSet": func(k *kv, o map[string]any) {
		k.addf("ready", "%d/%d", intAt(o, "status", "readyReplicas"), desiredReplicas(o))
	},
	// kubectl get daemonsets: NAME DESIRED CURRENT READY UP-TO-DATE
	// AVAILABLE NODE-SELECTOR AGE. The node selector is the one
	// column dropped as noise on the overwhelming majority of
	// DaemonSets, which select every node.
	"DaemonSet": func(k *kv, o map[string]any) {
		k.addf("ready", "%d/%d", intAt(o, "status", "numberReady"), intAt(o, "status", "desiredNumberScheduled"))
		k.addf("up_to_date", "%d", intAt(o, "status", "updatedNumberScheduled"))
		k.addf("available", "%d", intAt(o, "status", "numberAvailable"))
	},
	// kubectl get jobs: NAME STATUS COMPLETIONS DURATION AGE
	"Job": func(k *kv, o map[string]any) {
		k.add("status", jobStatus(o))
		want := 1
		if n, ok := intAtOK(o, "spec", "completions"); ok {
			want = n
		}
		k.addf("completions", "%d/%d", intAt(o, "status", "succeeded"), want)
	},
	// kubectl get cronjobs: NAME SCHEDULE TIMEZONE SUSPEND ACTIVE
	// LAST-SCHEDULE AGE
	"CronJob": func(k *kv, o map[string]any) {
		k.add("schedule", strAt(o, "spec", "schedule"))
		k.add("timezone", strAt(o, "spec", "timeZone"))
		if b, _ := mapAt(o, "spec")["suspend"].(bool); b {
			k.add("suspend", "true")
		}
		k.addf("active", "%d", len(sliceAt(o, "status", "active")))
		k.add("last_schedule", k.since(strAt(o, "status", "lastScheduleTime")))
	},
	// kubectl get services: NAME TYPE CLUSTER-IP EXTERNAL-IP PORT(S) AGE
	"Service": func(k *kv, o map[string]any) {
		k.add("type", strAt(o, "spec", "type"))
		k.add("cluster_ip", strAt(o, "spec", "clusterIP"))
		k.add("external_ip", externalIPs(o))
		var ports []string
		for _, p := range sliceAt(o, "spec", "ports") {
			m, _ := p.(map[string]any)
			proto := strAt(m, "protocol")
			if proto == "" {
				proto = "TCP"
			}
			port := strconv.Itoa(intAt(m, "port"))
			if np := intAt(m, "nodePort"); np != 0 {
				port += ":" + strconv.Itoa(np)
			}
			ports = append(ports, port+"/"+proto)
		}
		k.add("ports", strings.Join(ports, ","))
	},
	// kubectl get endpoints: NAME ENDPOINTS AGE. The count stands in
	// for kubectl's address list — the fact an agent needs from this
	// line is "is anything behind the Service", and a hundred
	// ip:port pairs would be the single largest line in the output.
	"Endpoints": func(k *kv, o map[string]any) {
		n := 0
		for _, s := range sliceAt(o, "subsets") {
			m, _ := s.(map[string]any)
			n += len(sliceAt(m, "addresses"))
		}
		k.addf("addresses", "%d", n)
	},
	// kubectl get ingresses: NAME CLASS HOSTS ADDRESS PORTS AGE
	"Ingress": func(k *kv, o map[string]any) {
		k.add("class", strAt(o, "spec", "ingressClassName"))
		var hosts []string
		for _, r := range sliceAt(o, "spec", "rules") {
			m, _ := r.(map[string]any)
			if h := strAt(m, "host"); h != "" {
				hosts = append(hosts, h)
			}
		}
		k.add("hosts", strings.Join(hosts, ","))
		k.add("address", loadBalancerAddresses(o))
	},
	// kubectl get configmaps: NAME DATA AGE
	"ConfigMap": func(k *kv, o map[string]any) {
		k.addf("keys", "%d", len(mapAt(o, "data"))+len(mapAt(o, "binaryData")))
	},
	// kubectl get secrets: NAME TYPE DATA AGE.
	//
	// Secrets are COUNTED, never read. lookout's read path is
	// secret-safe by design (§6.5) and an enumeration tool that
	// leaked one value would undo that in a single line; the key
	// count is exactly what kubectl prints and is enough to tell an
	// empty Secret from a populated one.
	"Secret": func(k *kv, o map[string]any) {
		k.add("type", strAt(o, "type"))
		k.addf("keys", "%d", len(mapAt(o, "data"))+len(mapAt(o, "stringData")))
	},
	// kubectl get pvc: NAME STATUS VOLUME CAPACITY ACCESS-MODES
	// STORAGECLASS AGE
	"PersistentVolumeClaim": func(k *kv, o map[string]any) {
		k.add("phase", strAt(o, "status", "phase"))
		k.add("volume", strAt(o, "spec", "volumeName"))
		k.add("capacity", strAt(o, "status", "capacity", "storage"))
		k.add("access_modes", accessModes(o))
		k.add("class", strAt(o, "spec", "storageClassName"))
	},
	// kubectl get pv: NAME CAPACITY ACCESS-MODES RECLAIM-POLICY
	// STATUS CLAIM STORAGECLASS REASON AGE
	"PersistentVolume": func(k *kv, o map[string]any) {
		k.add("phase", strAt(o, "status", "phase"))
		k.add("capacity", strAt(o, "spec", "capacity", "storage"))
		k.add("access_modes", accessModes(o))
		claim := mapAt(o, "spec", "claimRef")
		if claim != nil {
			k.add("claim", strAt(claim, "namespace")+"/"+strAt(claim, "name"))
		}
		k.add("class", strAt(o, "spec", "storageClassName"))
	},
	// kubectl get hpa: NAME REFERENCE TARGETS MINPODS MAXPODS
	// REPLICAS AGE. TARGETS is dropped: it is a rendering of
	// status.currentMetrics against spec.metrics, several fields
	// wide, and it is the input to a judgement (`audit workloads`
	// reports an HPA that cannot scale).
	"HorizontalPodAutoscaler": func(k *kv, o map[string]any) {
		ref := mapAt(o, "spec", "scaleTargetRef")
		if ref != nil {
			k.add("scale_target", strAt(ref, "kind")+"/"+strAt(ref, "name"))
		}
		minReplicas, ok := intAtOK(o, "spec", "minReplicas")
		if !ok {
			minReplicas = 1 // the API default, which kubectl prints
		}
		k.addf("min", "%d", minReplicas)
		k.addf("max", "%d", intAt(o, "spec", "maxReplicas"))
		k.addf("replicas", "%d", intAt(o, "status", "currentReplicas"))
	},
	// kubectl get pdb: NAME MIN-AVAILABLE MAX-UNAVAILABLE
	// ALLOWED-DISRUPTIONS AGE
	"PodDisruptionBudget": func(k *kv, o map[string]any) {
		k.add("min_available", scalarAt(o, "spec", "minAvailable"))
		k.add("max_unavailable", scalarAt(o, "spec", "maxUnavailable"))
		k.addf("allowed_disruptions", "%d", intAt(o, "status", "disruptionsAllowed"))
	},
	// kubectl get netpol: NAME POD-SELECTOR AGE
	"NetworkPolicy": func(k *kv, o map[string]any) {
		k.add("pod_selector", podSelector(o))
	},
	// kubectl get nodes: NAME STATUS ROLES AGE VERSION
	"Node": func(k *kv, o map[string]any) {
		k.add("status", nodeStatus(o))
		k.add("roles", nodeRoles(o))
		k.add("version", strAt(o, "status", "nodeInfo", "kubeletVersion"))
	},
	// kubectl get namespaces: NAME STATUS AGE
	"Namespace": func(k *kv, o map[string]any) {
		k.add("phase", strAt(o, "status", "phase"))
	},
}

// podStatus reproduces kubectl's STATUS column closely enough to be
// useful without reimplementing its full printer: a deleting pod is
// Terminating, an evicted pod carries status.reason, a container
// waiting or terminated for a reason shows that reason (init
// containers prefixed, as kubectl prefixes them), and everything else
// is the phase.
func podStatus(o map[string]any) string {
	if strAt(o, "metadata", "deletionTimestamp") != "" {
		return "Terminating"
	}
	if r := strAt(o, "status", "reason"); r != "" {
		return r // Evicted, NodeAffinity, Shutdown, …
	}
	for _, c := range sliceAt(o, "status", "initContainerStatuses") {
		m, _ := c.(map[string]any)
		if r := strAt(m, "state", "waiting", "reason"); r != "" && r != "PodInitializing" {
			return "Init:" + r
		}
		if t := mapAt(m, "state", "terminated"); t != nil && intAt(t, "exitCode") != 0 {
			if r := strAt(t, "reason"); r != "" {
				return "Init:" + r
			}
		}
	}
	for _, c := range sliceAt(o, "status", "containerStatuses") {
		m, _ := c.(map[string]any)
		if r := strAt(m, "state", "waiting", "reason"); r != "" {
			return r
		}
		if r := strAt(m, "state", "terminated", "reason"); r != "" && r != "Completed" {
			return r
		}
	}
	return strAt(o, "status", "phase")
}

// jobStatus is kubectl's STATUS column for a Job, read off the
// conditions the Job controller sets.
func jobStatus(o map[string]any) string {
	for _, c := range sliceAt(o, "status", "conditions") {
		m, _ := c.(map[string]any)
		if strAt(m, "status") != "True" {
			continue
		}
		switch strAt(m, "type") {
		case "Complete":
			return "Complete"
		case "Failed":
			return "Failed"
		case "Suspended":
			return "Suspended"
		}
	}
	return "Running"
}

// nodeStatus is kubectl's STATUS column: the Ready condition, plus
// the cordon marker that changes what a drain will do.
func nodeStatus(o map[string]any) string {
	status := "Unknown"
	for _, c := range sliceAt(o, "status", "conditions") {
		m, _ := c.(map[string]any)
		if strAt(m, "type") != "Ready" {
			continue
		}
		if strAt(m, "status") == "True" {
			status = "Ready"
		} else {
			status = "NotReady"
		}
	}
	if b, _ := mapAt(o, "spec")["unschedulable"].(bool); b {
		status += ",SchedulingDisabled"
	}
	return status
}

// nodeRoles is kubectl's ROLES column: the node-role.kubernetes.io/*
// labels, or "<none>" rendered as "none".
func nodeRoles(o map[string]any) string {
	const prefix = "node-role.kubernetes.io/"
	var roles []string
	for k := range mapAt(o, "metadata", "labels") {
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

// podSelector renders a NetworkPolicy's spec.podSelector. An EMPTY
// selector is not "no pods" — it selects every pod in the namespace —
// so it renders as "all" rather than being omitted, which is the one
// place a blank would actively mislead.
func podSelector(o map[string]any) string {
	sel := mapAt(o, "spec", "podSelector")
	labels := mapAt(sel, "matchLabels")
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys)+1)
	for _, k := range keys {
		v, _ := labels[k].(string)
		parts = append(parts, k+"="+v)
	}
	if n := len(sliceAt(sel, "matchExpressions")); n > 0 {
		parts = append(parts, fmt.Sprintf("+%d expressions", n))
	}
	if len(parts) == 0 {
		return "all"
	}
	return strings.Join(parts, ",")
}

// externalIPs is kubectl's EXTERNAL-IP column: the provisioned
// load-balancer addresses, or the literal "pending" for a
// LoadBalancer that has none yet — the state that means the Service
// is not reachable from outside and the one an omitted field would
// hide.
func externalIPs(o map[string]any) string {
	addrs := loadBalancerAddresses(o)
	if addrs == "" {
		for _, ip := range sliceAt(o, "spec", "externalIPs") {
			if s, ok := ip.(string); ok {
				addrs = joinNonEmpty(addrs, s)
			}
		}
	}
	if addrs == "" && strAt(o, "spec", "type") == "LoadBalancer" {
		return "pending"
	}
	return addrs
}

// loadBalancerAddresses reads status.loadBalancer.ingress[*], shared
// by Services and Ingresses (identical shape, identical meaning).
func loadBalancerAddresses(o map[string]any) string {
	var out string
	for _, in := range sliceAt(o, "status", "loadBalancer", "ingress") {
		m, _ := in.(map[string]any)
		addr := strAt(m, "ip")
		if addr == "" {
			addr = strAt(m, "hostname")
		}
		out = joinNonEmpty(out, addr)
	}
	return out
}

func joinNonEmpty(acc, s string) string {
	switch {
	case s == "":
		return acc
	case acc == "":
		return s
	}
	return acc + "," + s
}

func accessModes(o map[string]any) string {
	// kubectl abbreviates: ReadWriteOnce → RWO, and so on. The
	// abbreviations are what an operator reads in every `kubectl get
	// pvc`, and they are four times denser.
	short := map[string]string{
		"ReadWriteOnce":    "RWO",
		"ReadOnlyMany":     "ROX",
		"ReadWriteMany":    "RWX",
		"ReadWriteOncePod": "RWOP",
	}
	var out []string
	for _, m := range sliceAt(o, "spec", "accessModes") {
		s, _ := m.(string)
		if a, ok := short[s]; ok {
			s = a
		}
		if s != "" {
			out = append(out, s)
		}
	}
	return strings.Join(out, ",")
}

// desiredReplicas is spec.replicas with the API default (1) applied,
// so `ready=0/1` reads correctly on a workload that never set it.
func desiredReplicas(o map[string]any) int {
	if n, ok := intAtOK(o, "spec", "replicas"); ok {
		return n
	}
	return 1
}

// ---- the line builder ------------------------------------------------------

// kv accumulates a line's detail fields in declaration order,
// skipping the empty ones.
//
// Skipping matters for density: a Pending pod has no node and no
// waiting reason, and `class= address=` on every object is noise the
// caller pays for by the token. It also means the zero-nominal-state
// rule (§4.2) applies inside a line, not only between lines.
type kv struct {
	fields []emit.Field
	now    time.Time
}

func (k *kv) add(key, value string) {
	if value == "" || value == "/" {
		return
	}
	k.fields = append(k.fields, emit.Field{Key: key, Value: value})
}

func (k *kv) addf(key, format string, args ...any) { k.add(key, fmt.Sprintf(format, args...)) }

// since renders an RFC3339 timestamp as an age, for the timestamp
// columns that are not metadata.creationTimestamp (a CronJob's last
// schedule).
func (k *kv) since(ts string) string {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return ""
	}
	return compactAge(k.now.Sub(t))
}

// compactAge renders a duration the way kubectl renders an AGE
// column: seconds, then minutes/hours, then whole days past two.
// Findings must be deterministic under a pinned clock, so checks
// format durations themselves (§4.2).
func compactAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours())/24)
	case d >= time.Hour:
		if m := int(d.Minutes()) % 60; m != 0 {
			return fmt.Sprintf("%dh%dm", int(d.Hours()), m)
		}
		return fmt.Sprintf("%dh", int(d.Hours()))
	case d >= time.Minute:
		if s := int(d.Seconds()) % 60; s != 0 {
			return fmt.Sprintf("%dm%ds", int(d.Minutes()), s)
		}
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%ds", int(d.Seconds()))
}

// ---- accessors over the unstructured object --------------------------------
//
// These walk the decoded map and return a zero value for anything
// absent, which is the right behavior here: a Deployment mid-rollout
// genuinely has no status.readyReplicas, and 0 is what that means.

func mapAt(m map[string]any, path ...string) map[string]any {
	for _, p := range path {
		next, ok := m[p].(map[string]any)
		if !ok {
			return nil
		}
		m = next
	}
	return m
}

func strAt(m map[string]any, path ...string) string {
	s, _ := valueAt(m, path...).(string)
	return s
}

// scalarAt renders a field that may be a number or a string — an
// IntOrString, as PodDisruptionBudget's minAvailable is ("2" or
// "40%").
func scalarAt(m map[string]any, path ...string) string {
	switch v := valueAt(m, path...).(type) {
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatInt(int64(v), 10)
	}
	return ""
}

func intAt(m map[string]any, path ...string) int {
	n, _ := intAtOK(m, path...)
	return n
}

// intAtOK distinguishes an absent field from a present zero, which is
// what lets spec.replicas default to 1 only when it is really unset.
func intAtOK(m map[string]any, path ...string) (int, bool) {
	switch v := valueAt(m, path...).(type) {
	case int64: // unstructured objects from the API decode to int64
		return int(v), true
	case float64: // JSON round-tripped fixtures decode to float64
		return int(v), true
	}
	return 0, false
}

func sliceAt(m map[string]any, path ...string) []any {
	s, _ := valueAt(m, path...).([]any)
	return s
}

func valueAt(m map[string]any, path ...string) any {
	if len(path) == 0 {
		return nil
	}
	parent := mapAt(m, path[:len(path)-1]...)
	if parent == nil {
		return nil
	}
	return parent[path[len(path)-1]]
}
