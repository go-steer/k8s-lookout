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

package graph

// The §6.6 topology delta log: one ChangeRecord per applied delta,
// emitted through Options.OnChange. The record serves two consumers
// with one mechanism (§6.6):
//
//   - `triage changes` reads the identity + FieldChanges summary:
//     "what changed in the N minutes before onset". From/To values
//     are NAMES, HASHES, and COUNTS only — never config or secret
//     payloads (§6.5: the graph stores names, keys, and content
//     hashes; this file is where the content-hash half of that
//     sentence is implemented).
//   - `--at` point-in-time resolution replays the opaque Effect blob
//     (replay.go) on top of the nearest stored snapshot.
//
// FieldChanges are derived HERE, in the graph ingest, because this is
// the only place the typed objects are visible (the store sees only
// the finished record). What is tracked per kind:
//
//	Pod                        container images; ConfigMap/Secret/PVC
//	                           mount references (names)
//	Deployment/ReplicaSet/     spec.replicas
//	StatefulSet
//	Node                       spec.unschedulable
//	ConfigMap / Secret         content hash (sha256 over sorted
//	                           keys+values, truncated) — values are
//	                           read into the hash and dropped; the
//	                           hash is all that is ever retained or
//	                           emitted. DORMANT in the shipped
//	                           sentinel until the informer set grows
//	                           to watch ConfigMaps/Secrets (the M3
//	                           graph feed watches pods/nodes/
//	                           replicasets only); it lights up
//	                           automatically for any caller that
//	                           routes those objects through
//	                           Writer.Apply.
//	every kind                 labels — CHANGED KEYS only, values
//	                           reported as short hashes
//
// Everything else about an object is invisible to the graph and
// therefore to this log.

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"sort"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// String returns the persisted spelling of an Op ("add", "update",
// "delete"). These are wire values in the sentinel store — stable.
func (op Op) String() string {
	switch op {
	case OpAdd:
		return "add"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	default:
		return "invalid"
	}
}

// FieldChange is one changed field in an update's summary. From/To
// carry names, hashes, or counts only — never values. Paths are a
// stable mini-vocabulary, not JSONPath:
//
//	container/<name>/image   image references
//	replicas                 decimal counts
//	label/<key>              8-hex value hashes ("" = key absent)
//	mount/<Kind>/<name>      "" ↔ "mounted" (reference added/removed)
//	unschedulable            "true"/"false"
//	data                     16-hex content hashes (ConfigMap/Secret)
type FieldChange struct {
	Path string `json:"path"`
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

// ChangeRecord is one entry of the §6.6 delta log: the object
// identity, what happened, the changed-field summary (updates only),
// and the opaque replay Effect. Generation is the swap counter of
// the first published Snapshot that contains this change — the
// store's replay cursor (a change belongs to a snapshot iff its
// generation is <= the snapshot's).
type ChangeRecord struct {
	At           time.Time
	Generation   uint64
	Op           Op
	Kind         NodeKind
	Namespace    string
	Name         string
	UID          string
	FieldChanges []FieldChange
	// Effect is the compact re-applyable form of this delta's graph
	// mutations (replay.go). Opaque to every consumer except
	// Replayer.Apply.
	Effect []byte
}

// trackedState is the writer-private per-object fingerprint the
// FieldChanges diff runs against. It holds names, hashes, and counts
// only, mirroring what the records may carry.
type trackedState struct {
	labels        map[string]string // key → short value hash
	images        map[string]string // container name → image
	mounts        map[string]bool   // "Kind/name" reference set
	replicas      int32
	hasReplicas   bool
	unschedulable bool
	hasSched      bool
	contentHash   string
}

// trackChange updates the writer's tracked state for one delta and
// returns the ChangeRecord shell (Generation and Effect are filled
// in by flushLocked). Caller holds mu; only runs when OnChange is
// configured.
func (w *Writer) trackChange(d Delta) ChangeRecord {
	kind, namespace, name, uid := objectIdentity(d.Object)
	rec := ChangeRecord{
		At:        w.now(),
		Op:        d.Op,
		Kind:      kind,
		Namespace: namespace,
		Name:      name,
		UID:       uid,
	}
	id := w.intern.intern(nodeKey(kind, namespace, name))
	if d.Op == OpDelete {
		delete(w.tracked, id)
		return rec
	}
	st := newTrackedState(d.Object)
	// Diff whenever previous state exists: informer resyncs arrive as
	// adds, and Apply treats Add/Update as the same upsert.
	if old := w.tracked[id]; old != nil {
		rec.FieldChanges = diffTracked(old, st)
	}
	w.tracked[id] = st
	return rec
}

// trackSeed seeds tracked state during initial sync (FromObjects)
// without emitting a record: the first stored snapshot covers that
// state; the log starts at the first live delta.
func (w *Writer) trackSeed(obj any) {
	kind, namespace, name, _ := objectIdentity(obj)
	id := w.intern.intern(nodeKey(kind, namespace, name))
	w.tracked[id] = newTrackedState(obj)
}

// objectIdentity maps a validated typed object to its graph node
// identity + UID. The type switch is exhaustive over validateObject's
// accepted set.
func objectIdentity(obj any) (kind NodeKind, namespace, name, uid string) {
	m := obj.(metav1.Object)
	switch obj.(type) {
	case *corev1.Pod:
		kind = KindPod
	case *corev1.Service:
		kind = KindService
	case *corev1.Node:
		kind = KindNode
	case *corev1.Namespace:
		kind = KindNamespace
	case *corev1.ConfigMap:
		kind = KindConfigMap
	case *corev1.Secret:
		kind = KindSecret
	case *corev1.PersistentVolumeClaim:
		kind = KindPersistentVolumeClaim
	case *appsv1.Deployment:
		kind = KindDeployment
	case *appsv1.ReplicaSet:
		kind = KindReplicaSet
	case *appsv1.StatefulSet:
		kind = KindStatefulSet
	case *appsv1.DaemonSet:
		kind = KindDaemonSet
	case *batchv1.Job:
		kind = KindJob
	case *batchv1.CronJob:
		kind = KindCronJob
	case *discoveryv1.EndpointSlice:
		kind = KindEndpointSlice
	case *netv1.Ingress:
		kind = KindIngress
	case *netv1.NetworkPolicy:
		kind = KindNetworkPolicy
	default:
		kind = KindUnknown
	}
	return kind, m.GetNamespace(), m.GetName(), string(m.GetUID())
}

// newTrackedState extracts the tracked fingerprint from a typed
// object.
func newTrackedState(obj any) *trackedState {
	st := &trackedState{labels: hashLabels(obj.(metav1.Object).GetLabels())}
	switch o := obj.(type) {
	case *corev1.Pod:
		st.images = map[string]string{}
		for _, c := range o.Spec.InitContainers {
			st.images[c.Name] = c.Image
		}
		for _, c := range o.Spec.Containers {
			st.images[c.Name] = c.Image
		}
		st.mounts = podMountRefs(o)
	case *appsv1.Deployment:
		st.replicas, st.hasReplicas = replicasOrDefault(o.Spec.Replicas), true
	case *appsv1.ReplicaSet:
		st.replicas, st.hasReplicas = replicasOrDefault(o.Spec.Replicas), true
	case *appsv1.StatefulSet:
		st.replicas, st.hasReplicas = replicasOrDefault(o.Spec.Replicas), true
	case *corev1.Node:
		st.unschedulable, st.hasSched = o.Spec.Unschedulable, true
	case *corev1.ConfigMap:
		st.contentHash = hashConfigMap(o)
	case *corev1.Secret:
		st.contentHash = hashSecret(o)
	}
	return st
}

func replicasOrDefault(r *int32) int32 {
	if r == nil {
		return 1 // the API default for apps/v1 spec.replicas
	}
	return *r
}

// podMountRefs collects the pod's ConfigMap/Secret/PVC references as
// "Kind/name" (namespace implied — a pod can only reference its
// own). Same sources as the Mounts edge derivation (derive.go): env,
// envFrom, volumes, projected volumes.
func podMountRefs(pod *corev1.Pod) map[string]bool {
	refs := map[string]bool{}
	add := func(kind NodeKind, name string) {
		if name != "" {
			refs[kind.String()+"/"+name] = true
		}
	}
	containers := append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
	for i := range containers {
		c := &containers[i]
		for j := range c.Env {
			if vf := c.Env[j].ValueFrom; vf != nil {
				if vf.ConfigMapKeyRef != nil {
					add(KindConfigMap, vf.ConfigMapKeyRef.Name)
				}
				if vf.SecretKeyRef != nil {
					add(KindSecret, vf.SecretKeyRef.Name)
				}
			}
		}
		for j := range c.EnvFrom {
			if ref := c.EnvFrom[j].ConfigMapRef; ref != nil {
				add(KindConfigMap, ref.Name)
			}
			if ref := c.EnvFrom[j].SecretRef; ref != nil {
				add(KindSecret, ref.Name)
			}
		}
	}
	for i := range pod.Spec.Volumes {
		switch v := &pod.Spec.Volumes[i]; {
		case v.ConfigMap != nil:
			add(KindConfigMap, v.ConfigMap.Name)
		case v.Secret != nil:
			add(KindSecret, v.Secret.SecretName)
		case v.PersistentVolumeClaim != nil:
			add(KindPersistentVolumeClaim, v.PersistentVolumeClaim.ClaimName)
		case v.Projected != nil:
			for j := range v.Projected.Sources {
				s := &v.Projected.Sources[j]
				if s.ConfigMap != nil {
					add(KindConfigMap, s.ConfigMap.Name)
				}
				if s.Secret != nil {
					add(KindSecret, s.Secret.Name)
				}
			}
		}
	}
	return refs
}

// hashLabels maps every label to a short value hash: enough to detect
// a changed value and name the changed KEY without repeating the
// value in the log.
func hashLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = shortHash(v)
	}
	return out
}

// shortHash is the 8-hex label-value hash (fnv32a — a change
// detector, not a security boundary; label values are not secret,
// they are just noise the log does not need).
func shortHash(v string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(v))
	return hex.EncodeToString(h.Sum(nil))
}

// hashConfigMap returns the 16-hex content hash over the ConfigMap's
// data + binaryData, keys sorted.
func hashConfigMap(cm *corev1.ConfigMap) string {
	h := sha256.New()
	keys := make([]string, 0, len(cm.Data)+len(cm.BinaryData))
	for k := range cm.Data {
		keys = append(keys, "d:"+k)
	}
	for k := range cm.BinaryData {
		keys = append(keys, "b:"+k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte{0})
		if k[0] == 'd' {
			_, _ = h.Write([]byte(cm.Data[k[2:]]))
		} else {
			_, _ = h.Write(cm.BinaryData[k[2:]])
		}
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// hashSecret returns the 16-hex content hash over the Secret's data +
// stringData, keys sorted. The values pass through the hash and are
// DROPPED — this function is the entire extent of the graph's contact
// with secret material (§6.5: names, keys, and content hashes; a
// record can say "secret db-credentials changed" without ever holding
// the payload).
func hashSecret(sec *corev1.Secret) string {
	h := sha256.New()
	keys := make([]string, 0, len(sec.Data)+len(sec.StringData))
	for k := range sec.Data {
		keys = append(keys, "d:"+k)
	}
	for k := range sec.StringData {
		keys = append(keys, "s:"+k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte{0})
		if k[0] == 'd' {
			_, _ = h.Write(sec.Data[k[2:]])
		} else {
			_, _ = h.Write([]byte(sec.StringData[k[2:]]))
		}
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// diffTracked produces the deterministic (path-sorted) FieldChanges
// between two tracked states.
func diffTracked(old, cur *trackedState) []FieldChange {
	var out []FieldChange

	for _, k := range sortedKeyUnion(old.labels, cur.labels) {
		if old.labels[k] != cur.labels[k] {
			out = append(out, FieldChange{Path: "label/" + k, From: old.labels[k], To: cur.labels[k]})
		}
	}
	for _, name := range sortedKeyUnion(old.images, cur.images) {
		if old.images[name] != cur.images[name] {
			out = append(out, FieldChange{Path: "container/" + name + "/image", From: old.images[name], To: cur.images[name]})
		}
	}
	for _, ref := range sortedKeyUnion(old.mounts, cur.mounts) {
		switch {
		case old.mounts[ref] && !cur.mounts[ref]:
			out = append(out, FieldChange{Path: "mount/" + ref, From: "mounted", To: ""})
		case !old.mounts[ref] && cur.mounts[ref]:
			out = append(out, FieldChange{Path: "mount/" + ref, From: "", To: "mounted"})
		}
	}
	if old.hasReplicas && cur.hasReplicas && old.replicas != cur.replicas {
		out = append(out, FieldChange{
			Path: "replicas",
			From: strconv.FormatInt(int64(old.replicas), 10),
			To:   strconv.FormatInt(int64(cur.replicas), 10),
		})
	}
	if old.hasSched && cur.hasSched && old.unschedulable != cur.unschedulable {
		out = append(out, FieldChange{
			Path: "unschedulable",
			From: strconv.FormatBool(old.unschedulable),
			To:   strconv.FormatBool(cur.unschedulable),
		})
	}
	if old.contentHash != cur.contentHash {
		out = append(out, FieldChange{Path: "data", From: old.contentHash, To: cur.contentHash})
	}
	return out
}

// sortedKeyUnion returns the sorted union of two maps' keys.
func sortedKeyUnion[V any](a, b map[string]V) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for k := range a {
		seen[k] = true
		out = append(out, k)
	}
	for k := range b {
		if !seen[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
