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
	"regexp"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// The rollout-safety claims. Both are about what happens to in-flight
// requests when a pod of a Service-backed workload goes away on purpose
// — a rollout, a drain, a scale-down — which the readiness and
// availability claims in workloads.go do not cover: a workload can have
// probes, replicas and a PDB and still drop every request on every
// deploy. The third claim of the family, audit.liveness_preempts_readiness,
// is a probe claim and lives in probes.go.
const (
	kindRolloutDrops = "audit.rollout_drops_traffic"
	kindRecreate     = "audit.recreate_strategy"
)

const (
	reasonNoPreStop             = "NoPreStopOnServingContainer"
	reasonPreStopExceedsGrace   = "PreStopExceedsGracePeriod"
	reasonRecreateServiceBacked = "RecreateServiceBacked"
	reasonMaxUnavailableIsAll   = "MaxUnavailableIsAll"
)

var preStopSleepRe = regexp.MustCompile(`\bsleep\s+([0-9]+)s?\b`)

// metricsOnlyPortNames mark a Service port as a scrape endpoint. A
// workload whose only Services expose such ports is not "serving" for
// the rollout claims: a reset there costs a gap in a graph, not a
// user's request. Prometheus retries. (A policy knob of the same name
// once pkg/policy exists.)
var metricsOnlyPortNames = map[string]bool{
	"metrics":       true,
	"http-metrics":  true,
	"https-metrics": true,
}

// defaultTerminationGrace is the API default for terminationGracePeriodSeconds.
const defaultTerminationGrace int64 = 30

// rollout judges the two claims for one workload and returns findings
// without subject fields; judge() stamps those. Nothing fires when the
// Service list was refused: "Service-backed" cannot be decided, and a
// claim about rollouts on a workload that may serve nothing would be
// noise. The refusal is reported through the existing unread record.
func (ix *workloadIndex) rollout(w workload) []emit.Finding {
	var out []emit.Finding
	if ix.servicesRefused != nil {
		return out
	}
	out = append(out, preStopExceedsGracePeriod(w.template.Spec)...)
	serving := servingServices(ix.services, w.namespace, w.template.Labels)
	if len(serving) == 0 {
		return out
	}
	if f, ok := rolloutDropsTraffic(w, serving); ok {
		out = append(out, f)
	}
	if f, ok := recreateStrategy(w, serving); ok {
		out = append(out, f)
	}
	return out
}

func preStopExceedsGracePeriod(spec corev1.PodSpec) []emit.Finding {
	grace := defaultTerminationGrace
	if spec.TerminationGracePeriodSeconds != nil {
		grace = *spec.TerminationGracePeriodSeconds
	}
	var out []emit.Finding
	for _, c := range spec.Containers {
		if selfHealthSidecars[c.Name] || c.Lifecycle == nil || c.Lifecycle.PreStop == nil {
			continue
		}
		sleepSec, ok := parsePreStopSleepSeconds(c.Lifecycle.PreStop)
		if !ok || sleepSec < grace {
			continue
		}
		out = append(out, emit.Finding{
			Kind:     kindRolloutDrops,
			Severity: emit.SeverityWarning,
			Reason:   reasonPreStopExceedsGrace,
			Message: fmt.Sprintf("container %s has a preStop sleep of %ds while terminationGracePeriodSeconds is %ds: the grace period runs from the start of preStop, so SIGKILL arrives before or right as preStop finishes, leaving zero drain time for the process to close connections",
				c.Name, sleepSec, grace),
			Details: []emit.Field{
				{Key: "container", Value: c.Name},
				{Key: "prestop_sleep_seconds", Value: itoa(int(sleepSec))},
				{Key: "termination_grace_period_seconds", Value: itoa(int(grace))},
			},
		})
	}
	return out
}

func parsePreStopSleepSeconds(h *corev1.LifecycleHandler) (int64, bool) {
	if h == nil {
		return 0, false
	}
	if h.Sleep != nil && h.Sleep.Seconds > 0 {
		return h.Sleep.Seconds, true
	}
	if h.Exec == nil || len(h.Exec.Command) == 0 {
		return 0, false
	}
	joined := strings.Join(h.Exec.Command, " ")
	m := preStopSleepRe.FindStringSubmatch(joined)
	if len(m) < 2 {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// servingServices are the Services selecting the template whose ports
// are not all scrape endpoints.
func servingServices(services []*corev1.Service, namespace string, podLabels map[string]string) []*corev1.Service {
	var out []*corev1.Service
	for _, svc := range selectingServices(services, namespace, podLabels) {
		for _, p := range svc.Spec.Ports {
			if !metricsOnlyPortNames[p.Name] {
				out = append(out, svc)
				break
			}
		}
	}
	return out
}

// containersBehind returns the template's containers that own a port
// one of the Services routes to (by targetPort name or number; a
// Service port with no targetPort routes to the same number). A
// container no Service reaches — a log shipper, a config reloader — is
// not in the request path and cannot reset anything.
func containersBehind(spec corev1.PodSpec, services []*corev1.Service) []corev1.Container {
	var out []corev1.Container
	for _, c := range spec.Containers {
		if containerBehind(c, services) {
			out = append(out, c)
		}
	}
	return out
}

func containerBehind(c corev1.Container, services []*corev1.Service) bool {
	for _, svc := range services {
		for _, sp := range svc.Spec.Ports {
			if metricsOnlyPortNames[sp.Name] {
				continue
			}
			target := sp.TargetPort
			if target.Type == intstr.Int && target.IntVal == 0 {
				target = intstr.FromInt32(sp.Port)
			}
			for _, cp := range c.Ports {
				if (target.Type == intstr.String && cp.Name == target.StrVal) || (target.Type == intstr.Int && cp.ContainerPort == target.IntVal) {
					return true
				}
			}
		}
	}
	return false
}

// rolloutDropsTraffic: a Service-backed workload whose serving
// container has no preStop hook. Endpoint removal and SIGTERM are two
// independent reactions to a pod deletion; the kubelet starts
// terminating while the endpoints controller is still propagating the
// removal to every node and every proxy holding a connection, so
// requests keep arriving at a process that is shutting down and are
// answered with resets. A preStop hook is the only thing in the pod
// lifecycle that delays SIGTERM.
//
// Narrowed three ways, because unnarrowed this fires on almost every
// workload: Service-backed and serving (not a scrape-only Service);
// more than one replica for Deployments and StatefulSets (a single
// replica has a hard outage on every rollout whatever its hooks do, and
// single_replica already says so; DaemonSets are in scope with no
// replica test — the other nodes keep serving, so a rolling update of a
// Service-backed DaemonSet drops exactly this traffic); and only the
// containers behind the Service's targetPort.
func rolloutDropsTraffic(w workload, serving []*corev1.Service) (emit.Finding, bool) {
	if w.kind != "DaemonSet" && w.wantReplicas() <= 1 {
		return emit.Finding{}, false
	}
	var missing []string
	behind := containersBehind(w.template.Spec, serving)
	for _, c := range behind {
		if selfHealthSidecars[c.Name] {
			continue
		}
		if c.Lifecycle == nil || c.Lifecycle.PreStop == nil {
			missing = append(missing, c.Name)
		}
	}
	if len(missing) == 0 {
		return emit.Finding{}, false
	}
	grace := defaultTerminationGrace
	if w.template.Spec.TerminationGracePeriodSeconds != nil {
		grace = *w.template.Spec.TerminationGracePeriodSeconds
	}
	return emit.Finding{
		Kind:     kindRolloutDrops,
		Severity: emit.SeverityWarning,
		Reason:   reasonNoPreStop,
		Message: fmt.Sprintf("%d Service%s %s to this workload and %d of its %d serving container%s %s no preStop hook: on every rollout, drain or scale-down the kubelet sends SIGTERM while endpoint removal is still propagating, and in-flight requests are answered with connection resets — a preStop sleep of a few seconds is the only thing in the pod lifecycle that delays SIGTERM",
			len(serving), plural(len(serving), ""), verb(len(serving), "routes", "route"), len(missing), len(behind), plural(len(behind), ""), verb(len(missing), "has", "have")),
		Details: []emit.Field{
			{Key: "containers", Value: itoa(len(missing))},
			{Key: "container_names", Value: cappedList(missing)},
			{Key: "total_containers", Value: itoa(len(w.template.Spec.Containers))},
			{Key: "services", Value: itoa(len(serving))},
			{Key: "service_names", Value: cappedList(serviceNames(serving))},
			{Key: "termination_grace", Value: fmt.Sprintf("%ds", grace)},
		},
	}, true
}

// recreateStrategy: a multi-replica, Service-backed Deployment whose
// update strategy permits zero surviving pods — `Recreate`, or
// `RollingUpdate` with maxUnavailable at or above the replica count,
// which is the same outage written the long way. Not a finding on one
// replica (single_replica owns that) and not on maxUnavailable merely
// being large: 2 of 3 is a deliberate speed-over-capacity choice that
// still leaves a pod serving.
func recreateStrategy(w workload, serving []*corev1.Service) (emit.Finding, bool) {
	if w.kind != "Deployment" || w.wantReplicas() <= 1 || w.strategy == nil {
		return emit.Finding{}, false
	}
	replicas := int(w.wantReplicas())
	var reason, detail string
	switch {
	case w.strategy.Type == appsv1.RecreateDeploymentStrategyType:
		reason, detail = reasonRecreateServiceBacked, "strategy.type=Recreate"
	case w.strategy.RollingUpdate != nil && w.strategy.RollingUpdate.MaxUnavailable != nil:
		mu := w.strategy.RollingUpdate.MaxUnavailable
		n, err := intstr.GetScaledValueFromIntOrPercent(mu, replicas, false)
		if err != nil || n < replicas {
			return emit.Finding{}, false
		}
		reason, detail = reasonMaxUnavailableIsAll, "rollingUpdate.maxUnavailable="+mu.String()
	default:
		return emit.Finding{}, false
	}
	return emit.Finding{
		Kind:     kindRecreate,
		Severity: emit.SeverityWarning,
		Reason:   reason,
		Message: fmt.Sprintf("%s with replicas=%d and %d Service%s routing to it: the controller may take every pod down before the first replacement is Ready, so each rollout is an outage lasting an image pull plus a cold start — invisible afterwards, because the Deployment reports Available between rollouts",
			detail, replicas, len(serving), plural(len(serving), "")),
		Details: []emit.Field{
			{Key: "replicas", Value: itoa(replicas)},
			{Key: "strategy", Value: detail},
			{Key: "services", Value: itoa(len(serving))},
			{Key: "service_names", Value: cappedList(serviceNames(serving))},
		},
	}, true
}

// verb picks the singular or plural form for a count.
func verb(n int, singular, pluralForm string) string {
	if n == 1 {
		return singular
	}
	return pluralForm
}

func serviceNames(svcs []*corev1.Service) []string {
	out := make([]string, 0, len(svcs))
	for _, s := range svcs {
		out = append(out, s.Name)
	}
	return out
}
