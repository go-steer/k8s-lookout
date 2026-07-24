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

package checks

// Renderers for `triage spec`: flatten a SANITIZED unstructured
// object into the finding model. Everything here reads the sanitizer
// output, never the raw fetch (defense in depth — and the Writer
// sanitizes each finding once more on emit). Field order within each
// finding is fixed by construction; map-derived values are sorted, so
// output is byte-stable for golden tests.

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// renderSpec dispatches on the canonical kind. Kinds without a
// dedicated renderer get the generic treatment: head finding with the
// sanitized spec flattened to path=value pairs, plus abnormal
// conditions.
func renderSpec(out *emit.Writer, kind string, t specTarget, u map[string]any) error {
	head := emit.Finding{
		Kind:         "spec.resource",
		Severity:     emit.SeverityInfo,
		Namespace:    t.namespace,
		KindOfObject: kind,
		Name:         t.name,
		Details:      metaDetails(u),
	}
	var containers []emit.Finding
	switch kind {
	case "Pod":
		head.Details = append(head.Details, podDetails(u)...)
		containers = containerFindings(head, subMap(u, "spec"))
	case "Deployment", "ReplicaSet", "StatefulSet", "DaemonSet":
		head.Details = append(head.Details, workloadDetails(u)...)
		containers = containerFindings(head, subMap(u, "spec", "template", "spec"))
	case "Service":
		head.Details = append(head.Details, serviceDetails(u)...)
	case "ConfigMap":
		head.Details = append(head.Details, configMapDetails(u)...)
	case "Secret":
		head.Details = append(head.Details, secretDetails(u)...)
	default:
		if flat := flattenSpec(u); flat != "" {
			head.Details = append(head.Details, emit.Field{Key: "spec", Value: flat})
		}
	}
	appendPhase(&head, u, t.typed)

	if err := out.Emit(head); err != nil {
		return err
	}
	for _, f := range containers {
		if err := out.Emit(f); err != nil {
			return err
		}
	}
	return emitAbnormalConditions(out, head, u)
}

// appendPhase adds status.phase only when it is not one of the
// kind's nominal phases — a Running pod says nothing, a Pending one
// is the story.
func appendPhase(head *emit.Finding, u map[string]any, k *specKind) {
	phase := str(subMap(u, "status"), "phase")
	if phase == "" {
		return
	}
	if k != nil {
		for _, nominal := range k.nominalPhases {
			if phase == nominal {
				return
			}
		}
	}
	head.Details = append(head.Details, emit.Field{Key: "phase", Value: phase})
}

// metaDetails renders the metadata essentials: labels and the
// controlling owner. System metadata is already stripped by the
// sanitizer; the rest (annotations et al.) is deliberately elided —
// token density beats completeness here.
func metaDetails(u map[string]any) []emit.Field {
	meta := subMap(u, "metadata")
	var out []emit.Field
	if labels := sortedKV(subMap(meta, "labels")); labels != "" {
		out = append(out, emit.Field{Key: "labels", Value: labels})
	}
	for _, o := range subSlice(meta, "ownerReferences") {
		om, ok := o.(map[string]any)
		if !ok {
			continue
		}
		if ctrl, _ := om["controller"].(bool); !ctrl {
			continue
		}
		out = append(out, emit.Field{Key: "owner", Value: str(om, "kind") + "/" + str(om, "name")})
		break
	}
	return out
}

// --- Pod ---------------------------------------------------------------

func podDetails(u map[string]any) []emit.Field {
	spec := subMap(u, "spec")
	var out []emit.Field
	add := func(key, val string) {
		if val != "" {
			out = append(out, emit.Field{Key: key, Value: val})
		}
	}
	add("node", str(spec, "nodeName"))
	add("service_account", str(spec, "serviceAccountName"))
	var vols []string
	for _, v := range subSlice(spec, "volumes") {
		if vm, ok := v.(map[string]any); ok {
			vols = append(vols, str(vm, "name")+":"+volumeSource(vm))
		}
	}
	add("volumes", strings.Join(vols, ","))
	return out
}

// volumeSource names a volume's source and referent — the name of a
// ConfigMap/Secret/PVC is triage signal; its payload never appears.
func volumeSource(vm map[string]any) string {
	switch {
	case subMap(vm, "configMap") != nil:
		return "configMap/" + str(subMap(vm, "configMap"), "name")
	case subMap(vm, "secret") != nil:
		return "secret/" + str(subMap(vm, "secret"), "secretName")
	case subMap(vm, "persistentVolumeClaim") != nil:
		return "pvc/" + str(subMap(vm, "persistentVolumeClaim"), "claimName")
	case vm["emptyDir"] != nil:
		return "emptyDir"
	case subMap(vm, "hostPath") != nil:
		return "hostPath:" + str(subMap(vm, "hostPath"), "path")
	}
	// Unknown source: name its type (the single non-name key).
	keys := make([]string, 0, len(vm))
	for k := range vm {
		if k != "name" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return strings.Join(keys, "+")
}

// containerFindings emits one spec.container finding per container,
// init containers first, in spec order.
func containerFindings(head emit.Finding, podSpec map[string]any) []emit.Finding {
	var out []emit.Finding
	emitList := func(key string, init bool) {
		for _, c := range subSlice(podSpec, key) {
			cm, ok := c.(map[string]any)
			if !ok {
				continue
			}
			f := emit.Finding{
				Kind:         "spec.container",
				Severity:     emit.SeverityInfo,
				Namespace:    head.Namespace,
				KindOfObject: head.KindOfObject,
				Name:         head.Name,
				Details:      containerDetails(cm, init),
			}
			out = append(out, f)
		}
	}
	emitList("initContainers", true)
	emitList("containers", false)
	return out
}

func containerDetails(c map[string]any, init bool) []emit.Field {
	var out []emit.Field
	add := func(key, val string) {
		if val != "" {
			out = append(out, emit.Field{Key: key, Value: val})
		}
	}
	add("container", str(c, "name"))
	if init {
		add("init", "true")
	}
	add("image", str(c, "image"))
	res := subMap(c, "resources")
	add("requests", sortedKV(subMap(res, "requests")))
	add("limits", sortedKV(subMap(res, "limits")))
	var ports []string
	for _, p := range subSlice(c, "ports") {
		if pm, ok := p.(map[string]any); ok {
			ports = append(ports, containerPort(pm))
		}
	}
	add("ports", strings.Join(ports, ","))
	add("liveness", probeSummary(subMap(c, "livenessProbe")))
	add("readiness", probeSummary(subMap(c, "readinessProbe")))
	add("env", envSummary(subSlice(c, "env")))
	add("env_from", envFromSummary(subSlice(c, "envFrom")))
	return out
}

func containerPort(pm map[string]any) string {
	s := num(pm, "containerPort")
	if name := str(pm, "name"); name != "" {
		s = name + ":" + s
	}
	if proto := str(pm, "protocol"); proto != "" {
		s += "/" + proto
	}
	return s
}

// probeSummary is one probe as one token run: mechanism + target,
// then only the timings the spec actually set (defaults elided by
// the JSON round trip's omitempty).
func probeSummary(p map[string]any) string {
	if p == nil {
		return ""
	}
	var b []string
	switch {
	case subMap(p, "httpGet") != nil:
		hg := subMap(p, "httpGet")
		scheme := "http-get"
		if strings.EqualFold(str(hg, "scheme"), "HTTPS") {
			scheme = "https-get"
		}
		b = append(b, scheme+" :"+num(hg, "port")+str(hg, "path"))
	case subMap(p, "tcpSocket") != nil:
		b = append(b, "tcp :"+num(subMap(p, "tcpSocket"), "port"))
	case subMap(p, "grpc") != nil:
		b = append(b, "grpc :"+num(subMap(p, "grpc"), "port"))
	case subMap(p, "exec") != nil:
		var cmd []string
		for _, e := range subSlice(subMap(p, "exec"), "command") {
			if s, ok := e.(string); ok {
				cmd = append(cmd, s)
			}
		}
		b = append(b, truncate("exec "+strings.Join(cmd, " "), 60))
	}
	for _, t := range []struct{ key, label string }{
		{"initialDelaySeconds", "delay"},
		{"timeoutSeconds", "timeout"},
		{"periodSeconds", "period"},
		{"failureThreshold", "failure"},
	} {
		if v := num(p, t.key); v != "" {
			b = append(b, t.label+"="+v+secondsSuffix(t.key))
		}
	}
	return strings.Join(b, " ")
}

func secondsSuffix(key string) string {
	if strings.HasSuffix(key, "Seconds") {
		return "s"
	}
	return ""
}

// envSummary renders env vars in spec order. Literal values arrive
// already masked by the sanitizer; valueFrom entries render as the
// named reference they are (a secret's NAME is signal, §6.5).
func envSummary(env []any) string {
	var out []string
	for _, e := range env {
		em, ok := e.(map[string]any)
		if !ok {
			continue
		}
		name := str(em, "name")
		switch {
		case em["value"] != nil:
			out = append(out, name+"="+str(em, "value"))
		case subMap(em, "valueFrom") != nil:
			vf := subMap(em, "valueFrom")
			switch {
			case subMap(vf, "secretKeyRef") != nil:
				r := subMap(vf, "secretKeyRef")
				out = append(out, name+"=secretKeyRef:"+str(r, "name")+"."+str(r, "key"))
			case subMap(vf, "configMapKeyRef") != nil:
				r := subMap(vf, "configMapKeyRef")
				out = append(out, name+"=configMapKeyRef:"+str(r, "name")+"."+str(r, "key"))
			case subMap(vf, "fieldRef") != nil:
				out = append(out, name+"=fieldRef:"+str(subMap(vf, "fieldRef"), "fieldPath"))
			case subMap(vf, "resourceFieldRef") != nil:
				out = append(out, name+"=resourceFieldRef:"+str(subMap(vf, "resourceFieldRef"), "resource"))
			default:
				out = append(out, name)
			}
		default:
			out = append(out, name)
		}
	}
	return strings.Join(out, ",")
}

func envFromSummary(envFrom []any) string {
	var out []string
	for _, e := range envFrom {
		em, ok := e.(map[string]any)
		if !ok {
			continue
		}
		var s string
		switch {
		case subMap(em, "secretRef") != nil:
			s = "secretRef:" + str(subMap(em, "secretRef"), "name")
		case subMap(em, "configMapRef") != nil:
			s = "configMapRef:" + str(subMap(em, "configMapRef"), "name")
		default:
			continue
		}
		if prefix := str(em, "prefix"); prefix != "" {
			s += "(prefix=" + prefix + ")"
		}
		out = append(out, s)
	}
	return strings.Join(out, ",")
}

// --- Workload controllers ----------------------------------------------

func workloadDetails(u map[string]any) []emit.Field {
	spec := subMap(u, "spec")
	var out []emit.Field
	add := func(key, val string) {
		if val != "" {
			out = append(out, emit.Field{Key: key, Value: val})
		}
	}
	add("replicas", num(spec, "replicas"))
	add("strategy", strategySummary(spec))
	add("selector", selectorSummary(subMap(spec, "selector")))
	return out
}

// strategySummary handles both spellings: Deployment `strategy`,
// StatefulSet/DaemonSet `updateStrategy`.
func strategySummary(spec map[string]any) string {
	s := subMap(spec, "strategy")
	if s == nil {
		s = subMap(spec, "updateStrategy")
	}
	if s == nil {
		return ""
	}
	out := str(s, "type")
	ru := subMap(s, "rollingUpdate")
	if ru == nil {
		return out
	}
	var knobs []string
	for _, k := range []string{"maxSurge", "maxUnavailable", "partition"} {
		if v := num(ru, k); v != "" {
			knobs = append(knobs, k+"="+v)
		}
	}
	if len(knobs) > 0 {
		out += " " + strings.Join(knobs, " ")
	}
	return out
}

func selectorSummary(sel map[string]any) string {
	if sel == nil {
		return ""
	}
	out := sortedKV(subMap(sel, "matchLabels"))
	if len(subSlice(sel, "matchExpressions")) > 0 {
		if out != "" {
			out += ","
		}
		out += "+matchExpressions"
	}
	// Services carry the selector map directly (no matchLabels).
	if out == "" {
		out = sortedKV(sel)
	}
	return out
}

// --- Service ------------------------------------------------------------

func serviceDetails(u map[string]any) []emit.Field {
	spec := subMap(u, "spec")
	var out []emit.Field
	add := func(key, val string) {
		if val != "" {
			out = append(out, emit.Field{Key: key, Value: val})
		}
	}
	if t := str(spec, "type"); t != "" && t != "ClusterIP" { // default elided
		add("type", t)
	}
	add("external_name", str(spec, "externalName"))
	add("selector", sortedKV(subMap(spec, "selector")))
	var ports []string
	for _, p := range subSlice(spec, "ports") {
		if pm, ok := p.(map[string]any); ok {
			ports = append(ports, servicePort(pm))
		}
	}
	add("ports", strings.Join(ports, ","))
	if sa := str(spec, "sessionAffinity"); sa != "" && sa != "None" { // default elided
		add("session_affinity", sa)
	}
	return out
}

func servicePort(pm map[string]any) string {
	s := num(pm, "port")
	if name := str(pm, "name"); name != "" {
		s = name + ":" + s
	}
	if tp := num(pm, "targetPort"); tp != "" && tp != num(pm, "port") {
		s += "->" + tp
	}
	if proto := str(pm, "protocol"); proto != "" {
		s += "/" + proto
	}
	if np := num(pm, "nodePort"); np != "" {
		s += "@" + np
	}
	return s
}

// --- ConfigMap / Secret: data KEYS only ----------------------------------

func configMapDetails(u map[string]any) []emit.Field {
	var keys []string
	for k, v := range subMap(u, "data") {
		if s, ok := v.(string); ok {
			keys = append(keys, fmt.Sprintf("%s(%dB)", k, len(s)))
		} else {
			keys = append(keys, k)
		}
	}
	for k, v := range subMap(u, "binaryData") {
		size := ""
		if s, ok := v.(string); ok {
			if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
				size = fmt.Sprintf("(%dB)", len(decoded))
			}
		}
		keys = append(keys, k+size)
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	return []emit.Field{{Key: "keys", Value: strings.Join(keys, ",")}}
}

// reRedactedSize parses the sanitizer's length-only redaction
// ("[REDACTED:12B]") so the key list can report sizes without this
// code ever seeing a secret value.
var reRedactedSize = regexp.MustCompile(`^\[REDACTED:(\d+B)\]$`)

func secretDetails(u map[string]any) []emit.Field {
	var out []emit.Field
	if t := str(u, "type"); t != "" && t != "Opaque" { // default elided
		out = append(out, emit.Field{Key: "type", Value: t})
	}
	var keys []string
	for _, field := range []string{"data", "stringData"} {
		for k, v := range subMap(u, field) {
			// Values here are sanitizer redaction markers, never
			// payloads; only the parsed size survives.
			if s, ok := v.(string); ok {
				if m := reRedactedSize.FindStringSubmatch(s); m != nil {
					keys = append(keys, k+"("+m[1]+")")
					continue
				}
			}
			keys = append(keys, k)
		}
	}
	if len(keys) > 0 {
		sort.Strings(keys)
		out = append(out, emit.Field{Key: "keys", Value: strings.Join(keys, ",")})
	}
	return out
}

// --- Conditions ----------------------------------------------------------

// emitAbnormalConditions renders status.conditions whose state is
// not the healthy one, as warnings. Healthy conditions emit nothing
// (zero nominal state).
func emitAbnormalConditions(out *emit.Writer, head emit.Finding, u map[string]any) error {
	for _, c := range subSlice(subMap(u, "status"), "conditions") {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		condType, status := str(cm, "type"), str(cm, "status")
		if condType == "" || !conditionAbnormal(condType, status) {
			continue
		}
		f := emit.Finding{
			Kind:         "spec.condition",
			Severity:     emit.SeverityWarning,
			Namespace:    head.Namespace,
			KindOfObject: head.KindOfObject,
			Name:         head.Name,
			Reason:       str(cm, "reason"),
			Message:      str(cm, "message"),
			Details:      []emit.Field{{Key: "condition", Value: condType + "=" + status}},
		}
		if since := str(cm, "lastTransitionTime"); since != "" {
			f.Details = append(f.Details, emit.Field{Key: "since", Value: since})
		}
		if err := out.Emit(f); err != nil {
			return err
		}
	}
	return nil
}

// conditionAbnormal encodes condition polarity: most types are
// healthy at True (Ready, Available, Progressing, PodScheduled, …);
// types naming a problem (node pressure conditions, ReplicaFailure,
// NetworkUnavailable) are healthy at False. Unknown status is
// abnormal for both polarities.
func conditionAbnormal(condType, status string) bool {
	for _, bad := range []string{"Pressure", "Unavailable", "Problem", "Failure", "Failed"} {
		if strings.Contains(condType, bad) {
			return !strings.EqualFold(status, "False")
		}
	}
	return !strings.EqualFold(status, "True")
}

// --- Generic fallback -----------------------------------------------------

// specFlattenMax caps the generic spec detail; past it the value ends
// with an explicit truncation marker rather than silently stopping.
const specFlattenMax = 4096

// flattenSpec renders every non-system top-level field (everything
// but kind/apiVersion/metadata/status) as sorted path=value pairs —
// covering both spec'd kinds and spec-less shapes like EndpointSlice.
func flattenSpec(u map[string]any) string {
	var pairs []string
	roots := make([]string, 0, len(u))
	for k := range u {
		switch k {
		case "kind", "apiVersion", "metadata", "status":
			continue
		}
		roots = append(roots, k)
	}
	sort.Strings(roots)
	for _, k := range roots {
		flattenValue(k, u[k], &pairs)
	}
	out := strings.Join(pairs, " ")
	if len(out) > specFlattenMax {
		cut := strings.LastIndexByte(out[:specFlattenMax], ' ')
		if cut < 0 {
			cut = specFlattenMax
		}
		out = out[:cut] + " …(truncated)"
	}
	return out
}

func flattenValue(path string, v any, pairs *[]string) {
	switch val := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			flattenValue(path+"."+k, val[k], pairs)
		}
	case []any:
		for i, e := range val {
			flattenValue(fmt.Sprintf("%s[%d]", path, i), e, pairs)
		}
	default:
		*pairs = append(*pairs, path+"="+scalarString(v))
	}
}

// --- Small unstructured accessors ----------------------------------------

// subMap walks nested maps; nil at the first miss.
func subMap(u map[string]any, keys ...string) map[string]any {
	cur := u
	for _, k := range keys {
		if cur == nil {
			return nil
		}
		next, _ := cur[k].(map[string]any)
		cur = next
	}
	return cur
}

func subSlice(u map[string]any, key string) []any {
	if u == nil {
		return nil
	}
	s, _ := u[key].([]any)
	return s
}

func str(u map[string]any, key string) string {
	if u == nil {
		return ""
	}
	s, _ := u[key].(string)
	return s
}

// num renders a JSON scalar under key as its compact string form —
// integers without the float64 ".0", IntOrString values as-is.
func num(u map[string]any, key string) string {
	if u == nil || u[key] == nil {
		return ""
	}
	return scalarString(u[key])
}

func scalarString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	case nil:
		return ""
	default:
		return fmt.Sprint(val)
	}
}

// sortedKV renders a string map as sorted k=v pairs.
func sortedKV(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+scalarString(m[k]))
	}
	return strings.Join(parts, ",")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
