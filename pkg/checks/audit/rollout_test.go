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

package audit_test

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// The rollout-safety fixture: Service-backed workloads in every shape
// the three claims distinguish, plus the look-alikes each must stay
// silent on.

func httpProbe(path string, delay, period, threshold int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString("http")}},
		InitialDelaySeconds: delay, PeriodSeconds: period, FailureThreshold: threshold,
	}
}

func serving(name, image string, port int32, live, ready *corev1.Probe, preStop bool) corev1.Container {
	c := corev1.Container{
		Name: name, Image: image,
		Ports:          []corev1.ContainerPort{{Name: "http", ContainerPort: port}},
		LivenessProbe:  live,
		ReadinessProbe: ready,
	}
	if preStop {
		c.Lifecycle = &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 5}}}
	}
	return c
}

func svc(ns, name string, selector map[string]string, ports ...corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.ServiceSpec{Selector: selector, Ports: ports},
	}
}

func port(name string, port int32, target string) corev1.ServicePort {
	p := corev1.ServicePort{Name: name, Port: port}
	if target != "" {
		p.TargetPort = intstr.FromString(target)
	}
	return p
}

func withStrategy(d *appsv1.Deployment, s appsv1.DeploymentStrategy) *appsv1.Deployment {
	d.Spec.Strategy = s
	return d
}

func rolloutCluster() []runtime.Object {
	tie := httpProbe("/healthz", 0, 10, 3)       // 30s
	tieToo := httpProbe("/healthz", 0, 10, 3)    // same handler, same deadline
	fast := httpProbe("/healthz", 0, 5, 1)       // 5s: liveness strictly first
	slowReady := httpProbe("/healthz", 0, 10, 3) // 30s
	readyOnly := httpProbe("/ready", 0, 5, 3)    // different handler

	return []runtime.Object{
		// api: Service-backed, 3 replicas, probes tie on one handler, no preStop.
		//   -> liveness_preempts_readiness (info), rollout_drops_traffic
		deploy("prod", "api", 3, spread(template(lbl("api"), serving("api", "ghcr.io/acme/api:v9", 8080, tie, tieToo, false)))),
		svc("prod", "api", lbl("api"), port("http", 80, "http")),

		// worker: no Service; liveness strictly earlier than readiness, same handler.
		//   -> liveness_preempts_readiness (warning) only; no rollout claims
		deploy("prod", "worker", 3, spread(template(lbl("worker"), serving("worker", "ghcr.io/acme/worker:v9", 8080, fast, slowReady, false)))),

		// batch: Service-backed, Recreate, containers have preStop.
		//   -> recreate_strategy (RecreateServiceBacked) only
		withStrategy(deploy("prod", "batch", 3, spread(template(lbl("batch"), serving("batch", "ghcr.io/acme/batch:v9", 8080, fast, readyOnly, true)))),
			appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}),
		svc("prod", "batch", lbl("batch"), port("http", 80, "http")),

		// web: Service-backed, RollingUpdate with maxUnavailable=100%, preStop present.
		//   -> recreate_strategy (MaxUnavailableIsAll) only
		withStrategy(deploy("prod", "web", 4, spread(template(lbl("web"), serving("web", "ghcr.io/acme/web:v9", 8080, nil, readyOnly, true)))),
			appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType, RollingUpdate: &appsv1.RollingUpdateDeployment{
				MaxUnavailable: &intstr.IntOrString{Type: intstr.String, StrVal: "100%"}}}),
		svc("prod", "web", lbl("web"), port("http", 80, "http")),

		// scrape: the only Service port is a metrics scrape -> not serving; silent.
		deploy("prod", "scrape", 3, spread(template(lbl("scrape"), serving("scrape", "ghcr.io/acme/scrape:v9", 9090, nil, readyOnly, false)))),
		svc("prod", "scrape", lbl("scrape"), port("metrics", 9090, "")),

		// single: Service-backed, one replica, no preStop -> single_replica owns it; no rollout claim.
		deploy("prod", "single", 1, template(lbl("single"), serving("single", "ghcr.io/acme/single:v9", 8080, nil, readyOnly, false))),
		svc("prod", "single", lbl("single"), port("http", 80, "http")),

		// mesh: Service-backed, the only container without preStop is istio-proxy -> silent.
		deploy("prod", "mesh", 3, spread(template(lbl("mesh"),
			serving("mesh", "ghcr.io/acme/mesh:v9", 8080, nil, readyOnly, true),
			serving("istio-proxy", "docker.io/istio/proxyv2:1.22.0", 15090, nil, nil, false)))),
		svc("prod", "mesh", lbl("mesh"), port("http", 80, "http")),

		// logger: Service-backed, but the hookless container owns no served port -> silent.
		deploy("prod", "logger", 3, spread(template(lbl("logger"),
			serving("logger", "ghcr.io/acme/logger:v9", 8080, nil, readyOnly, true),
			corev1.Container{Name: "fluent-bit", Image: "docker.io/fluent/fluent-bit:3.1.0"}))),
		svc("prod", "logger", lbl("logger"), port("http", 80, "http")),

		// node-agent: a Service-backed DaemonSet with no preStop -> rollout_drops_traffic (no replica test).
		daemon("prod", "node-agent", template(lbl("node-agent"), serving("agent", "ghcr.io/acme/agent:v9", 8080, nil, readyOnly, false))),
		svc("prod", "node-agent", lbl("node-agent"), port("http", 80, "http")),

		// PDBs for the multi-replica ones keep no_pdb out of the picture.
		pdb("prod", "api", sel("api")), pdb("prod", "worker", sel("worker")), pdb("prod", "batch", sel("batch")),
		pdb("prod", "web", sel("web")), pdb("prod", "scrape", sel("scrape")), pdb("prod", "mesh", sel("mesh")), pdb("prod", "logger", sel("logger")),
	}
}

func TestRolloutGolden(t *testing.T) {
	res := checktest.Run(t, audit.WorkloadsCommand(testDeps(rolloutCluster()...)), "--namespace=prod")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/rollout.golden", res.Stdout)
}

func TestRolloutContract(t *testing.T) {
	checktest.VerifyContract(t, audit.WorkloadsCommand(testDeps(rolloutCluster()...)), "--namespace=prod")
}

func TestRolloutClaims(t *testing.T) {
	out := checktest.AssertEnvelopeContract(t, audit.WorkloadsCommand(testDeps(rolloutCluster()...)), "--namespace=prod")

	// api: the tie is info; the missing hook is a warning naming the Service.
	f := checktest.AssertHasFinding(t, out, checktest.FindingMatch{Kind: "audit.liveness_preempts_readiness", Name: "api", Severity: "info", Reason: "LivenessFiresFirst"})
	if f.Fields["container_names"] != "api" {
		t.Errorf("api: container_names=%q", f.Fields["container_names"])
	}
	f = checktest.AssertHasFinding(t, out, checktest.FindingMatch{Kind: "audit.rollout_drops_traffic", Name: "api", Severity: "warning", Reason: "NoPreStopOnServingContainer"})
	if f.Fields["service_names"] != "api" || f.Fields["termination_grace"] != "30s" {
		t.Errorf("api: fields=%v", f.Fields)
	}
	// worker: strictly earlier -> warning; no Service -> no rollout claim.
	checktest.AssertHasFinding(t, out, checktest.FindingMatch{Kind: "audit.liveness_preempts_readiness", Name: "worker", Severity: "warning"})
	checktest.AssertNoFinding(t, out, checktest.FindingMatch{Kind: "audit.rollout_drops_traffic", Name: "worker"})
	// batch / web: the two spellings of "take them all down".
	f = checktest.AssertHasFinding(t, out, checktest.FindingMatch{Kind: "audit.recreate_strategy", Name: "batch", Reason: "RecreateServiceBacked"})
	if f.Fields["replicas"] != "3" {
		t.Errorf("batch: replicas=%q", f.Fields["replicas"])
	}
	f = checktest.AssertHasFinding(t, out, checktest.FindingMatch{Kind: "audit.recreate_strategy", Name: "web", Reason: "MaxUnavailableIsAll"})
	if f.Fields["strategy"] != "rollingUpdate.maxUnavailable=100%" {
		t.Errorf("web: strategy=%q", f.Fields["strategy"])
	}
	checktest.AssertNoFinding(t, out, checktest.FindingMatch{Kind: "audit.rollout_drops_traffic", Name: "batch"})
	// DaemonSet is in scope with no replica test.
	checktest.AssertHasFinding(t, out, checktest.FindingMatch{Kind: "audit.rollout_drops_traffic", KindOfObject: "DaemonSet", Name: "node-agent"})
	// The look-alikes.
	for _, name := range []string{"scrape", "single", "mesh", "logger"} {
		checktest.AssertNoFinding(t, out, checktest.FindingMatch{Kind: "audit.rollout_drops_traffic", Name: name})
		checktest.AssertNoFinding(t, out, checktest.FindingMatch{Kind: "audit.recreate_strategy", Name: name})
	}
	// A different handler is never compared.
	checktest.AssertNoFinding(t, out, checktest.FindingMatch{Kind: "audit.liveness_preempts_readiness", Name: "batch"})
}

// A well-formed cluster is silent on all three claims.
func TestRolloutHealthyIsSilent(t *testing.T) {
	ready := httpProbe("/ready", 0, 5, 3)
	live := httpProbe("/healthz", 10, 10, 3)
	out := checktest.AssertEnvelopeContract(t, audit.WorkloadsCommand(testDeps(
		deploy("prod", "api", 3, spread(template(lbl("api"), serving("api", "ghcr.io/acme/api:v9", 8080, live, ready, true)))),
		svc("prod", "api", lbl("api"), port("http", 80, "http")),
		pdb("prod", "api", sel("api")),
	)), "--namespace=prod")
	for _, kind := range []string{"audit.liveness_preempts_readiness", "audit.rollout_drops_traffic", "audit.recreate_strategy"} {
		checktest.AssertNoFinding(t, out, checktest.FindingMatch{Kind: kind})
	}
}
