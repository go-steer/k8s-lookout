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
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// --- fixture builders ---

func nsObj(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func serviceAccount(namespace, name string, automount *bool) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta:                   metav1.ObjectMeta{Namespace: namespace, Name: name},
		AutomountServiceAccountToken: automount,
	}
}

func cronJob(namespace, name string, t corev1.PodTemplateSpec) *batchv1.CronJob {
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: t}}},
	}
}

func job(namespace, name string, t corev1.PodTemplateSpec) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       batchv1.JobSpec{Template: t},
	}
}

func barePod(namespace, name string, t corev1.PodTemplateSpec) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: t.Labels},
		Spec:       t.Spec,
	}
}

// owned stamps an ownerReference, the marker that says "this object is
// a copy of a template judged at its owner".
func owned[T metav1.Object](obj T, kind, name string) T {
	obj.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "batch/v1", Kind: kind, Name: name}})
	return obj
}

// --- pod-template mutators ---

func privileged(t corev1.PodTemplateSpec, names ...string) corev1.PodTemplateSpec {
	want := map[string]bool{}
	for _, n := range names {
		want[n] = true
	}
	for i, c := range t.Spec.Containers {
		if want[c.Name] {
			t.Spec.Containers[i].SecurityContext = &corev1.SecurityContext{Privileged: ptr(true)}
		}
	}
	return t
}

func addCaps(t corev1.PodTemplateSpec, name string, caps ...string) corev1.PodTemplateSpec {
	added := make([]corev1.Capability, 0, len(caps))
	for _, c := range caps {
		added = append(added, corev1.Capability(c))
	}
	for i, c := range t.Spec.Containers {
		if c.Name == name {
			t.Spec.Containers[i].SecurityContext = &corev1.SecurityContext{
				Capabilities: &corev1.Capabilities{Add: added},
			}
		}
	}
	return t
}

func hostNamespaces(t corev1.PodTemplateSpec, network, pid, ipc bool) corev1.PodTemplateSpec {
	t.Spec.HostNetwork, t.Spec.HostPID, t.Spec.HostIPC = network, pid, ipc
	return t
}

// hostPath declares a hostPath volume and mounts it in the named
// container. Passing mountIn="" declares the volume and mounts it
// nowhere, the shape that grants no access.
func hostPath(t corev1.PodTemplateSpec, volume, path, mountIn string, readOnly bool) corev1.PodTemplateSpec {
	t.Spec.Volumes = append(t.Spec.Volumes, corev1.Volume{
		Name:         volume,
		VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: path}},
	})
	for i, c := range t.Spec.Containers {
		if c.Name == mountIn {
			t.Spec.Containers[i].VolumeMounts = append(t.Spec.Containers[i].VolumeMounts,
				corev1.VolumeMount{Name: volume, MountPath: path, ReadOnly: readOnly})
		}
	}
	return t
}

func asServiceAccount(t corev1.PodTemplateSpec, name string) corev1.PodTemplateSpec {
	t.Spec.ServiceAccountName = name
	return t
}

func noAutomount(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
	t.Spec.AutomountServiceAccountToken = ptr(false)
	return t
}

func withInit(t corev1.PodTemplateSpec, c corev1.Container) corev1.PodTemplateSpec {
	t.Spec.InitContainers = append(t.Spec.InitContainers, c)
	return t
}

func app(name string) corev1.PodTemplateSpec {
	return template(map[string]string{"app": name}, container(name, true, true))
}

// hardeningCluster is the shared fixture: one object per shape the
// command has to get right.
//
//	prod       enforce=restricted, default SA unused
//	  Deployment/checkout   clean, runs as its own SA
//	  DaemonSet/cni         privileged, hostNetwork+hostPID, both hostPath forms
//	platform   no PSA labels at all, warn=baseline (dry-run), default SA used
//	  Deployment/mesh       CAP_SYS_ADMIN, no privileged flag
//	  CronJob/nightly       clean but on the default SA
//	  Job/adhoc             hostIPC
//	  Job/nightly-28        owned by the CronJob — must be judged once, at the owner
//	  Pod/debug             privileged, hand-rolled
//	  Pod/checkout-7f9      owned by a ReplicaSet — same
//	legacy     enforce=privileged, default SA automount disabled at the SA
//	  Deployment/vendor     read-only hostPath, on the default SA
func hardeningCluster() []runtime.Object {
	cni := hostPath(hostPath(hostNamespaces(privileged(
		template(map[string]string{"app": "cni"}, container("agent", true, true)),
		"agent"), true, true, false),
		"run", "/var/run", "agent", false),
		"cni-conf", "/etc/cni", "agent", true)

	return []runtime.Object{
		nsObj("prod", map[string]string{"pod-security.kubernetes.io/enforce": "restricted"}),
		nsObj("platform", map[string]string{"pod-security.kubernetes.io/warn": "baseline"}),
		nsObj("legacy", map[string]string{"pod-security.kubernetes.io/enforce": "privileged"}),

		serviceAccount("prod", "default", nil),
		serviceAccount("platform", "default", nil),
		serviceAccount("legacy", "default", ptr(false)),

		deploy("prod", "checkout", 3, asServiceAccount(app("checkout"), "checkout")),
		daemon("prod", "cni", asServiceAccount(cni, "cni")),

		deploy("platform", "mesh", 2, addCaps(app("mesh"), "mesh", "SYS_ADMIN")),
		cronJob("platform", "nightly", app("nightly")),
		job("platform", "adhoc", hostNamespaces(app("adhoc"), false, false, true)),
		owned(job("platform", "nightly-28", app("nightly")), "CronJob", "nightly"),
		barePod("platform", "debug", privileged(app("debug"), "debug")),
		owned(barePod("platform", "checkout-7f9", app("checkout")), "ReplicaSet", "checkout-7f9"),

		deploy("legacy", "vendor", 1, hostPath(app("vendor"), "tz", "/etc/localtime", "vendor", true)),
	}
}

func TestHardeningContract(t *testing.T) {
	checktest.VerifyContract(t, audit.HardeningCommand(testDeps(hardeningCluster()...)), "-A")
}

func TestHardeningGolden(t *testing.T) {
	res := checktest.Run(t, audit.HardeningCommand(testDeps(hardeningCluster()...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	checktest.Golden(t, "testdata/hardening.golden", res.Stdout)
}

// A namespace that enforces a Pod Security level and runs ordinary
// workloads produces nothing. Zero nominal state is what makes
// findings=0 readable as "hardened".
func TestHardeningQuietWhenHardened(t *testing.T) {
	objs := []runtime.Object{
		nsObj("prod", map[string]string{"pod-security.kubernetes.io/enforce": "baseline"}),
		serviceAccount("prod", "default", nil),
		deploy("prod", "checkout", 3, asServiceAccount(app("checkout"), "checkout")),
	}
	res := checktest.Run(t, audit.HardeningCommand(testDeps(objs...)), "-A")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.HasPrefix(res.Stdout, "scanned=1 findings=0 ") {
		t.Errorf("a hardened namespace must emit nothing, got:\n%s", res.Stdout)
	}
}

// The claim that motivates the second reason: a container adding
// CAP_SYS_ADMIN is root on the node without ever setting the flag a
// naive check reads, so matching only privileged: true reports it
// clean. The line is drawn at two capabilities on purpose — a check
// that fired on every mesh-injected NET_ADMIN sidecar is one people
// learn to skip.
func TestHardeningCapabilitiesAreNotThePrivilegedFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		caps []string
		want string // the capabilities detail, "" for no finding
	}{
		{"sys-admin", []string{"SYS_ADMIN"}, "SYS_ADMIN"},
		{"all", []string{"ALL"}, "ALL"},
		{"cap-prefixed", []string{"CAP_SYS_ADMIN"}, "SYS_ADMIN"},
		{"lowercase", []string{"sys_admin"}, "SYS_ADMIN"},
		{"both", []string{"ALL", "SYS_ADMIN"}, "ALL,SYS_ADMIN"},
		{"net-admin-is-not-flagged", []string{"NET_ADMIN"}, ""},
		{"ptrace-is-not-flagged", []string{"SYS_PTRACE"}, ""},
		{"mixed-only-reports-the-dangerous-one", []string{"NET_ADMIN", "SYS_ADMIN"}, "SYS_ADMIN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := hardenedNamespace(deploy("prod", "x", 1,
				asServiceAccount(addCaps(app("x"), "x", tc.caps...), "x")))
			res := checktest.Run(t, audit.HardeningCommand(testDeps(objs...)), "-A")
			var got map[string]string
			for _, r := range findingLines(t, res.Stdout) {
				if r["reason"] == "DangerousCapability" {
					got = r
				}
			}
			if (got != nil) != (tc.want != "") {
				t.Fatalf("finding present = %v, want %v:\n%s", got != nil, tc.want != "", res.Stdout)
			}
			if tc.want != "" && got["capabilities"] != tc.want {
				t.Errorf("capabilities = %q, want %q", got["capabilities"], tc.want)
			}
		})
	}
}

// An init container with privileged: true holds root on the node for
// as long as it runs, which is all the time it needs. Judging only
// spec.containers would report the commonest real shape of this
// defect — a privileged setup step — as clean.
func TestHardeningJudgesInitContainers(t *testing.T) {
	init := corev1.Container{
		Name:            "sysctl",
		Image:           "example.com/init:v1",
		SecurityContext: &corev1.SecurityContext{Privileged: ptr(true)},
	}
	objs := hardenedNamespace(deploy("prod", "x", 1,
		asServiceAccount(withInit(app("x"), init), "x")))
	res := checktest.Run(t, audit.HardeningCommand(testDeps(objs...)), "-A")
	var got map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.privileged_container" {
			got = r
		}
	}
	if got == nil {
		t.Fatalf("a privileged init container is a privileged container:\n%s", res.Stdout)
	}
	if got["container_names"] != "sysctl" || got["total_containers"] != "2" {
		t.Errorf("finding = %v, want the init container named as 1 of 2", got)
	}
}

// The three host namespaces are asked for separately, granted
// separately and removed separately, so they are separate classes: a
// rollup counting "how many workloads see every process on the node"
// must not have to parse a list.
func TestHardeningHostNamespacesAreSeparateClasses(t *testing.T) {
	objs := hardenedNamespace(deploy("prod", "x", 1,
		asServiceAccount(hostNamespaces(app("x"), true, true, true), "x")))
	res := checktest.Run(t, audit.HardeningCommand(testDeps(objs...)), "-A")
	reasons, prints := map[string]bool{}, map[string]bool{}
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.host_namespace" {
			reasons[r["reason"]] = true
			prints[r["fingerprint"]] = true
		}
	}
	if len(reasons) != 3 {
		t.Fatalf("want one finding per host namespace, got %v:\n%s", reasons, res.Stdout)
	}
	if len(prints) != 3 {
		t.Errorf("three remedies must not share a class: %v", prints)
	}
}

// A hostPath is judged by what can be written through it, and only
// when something actually mounts it: a declared but unmounted volume
// grants no access, and reporting it would be true and useless.
func TestHardeningHostPathMountSemantics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(corev1.PodTemplateSpec) corev1.PodTemplateSpec
		want   string // reason, "" for no finding
	}{
		{"writable", func(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
			return hostPath(t, "run", "/var/run", "x", false)
		}, "WritableHostPath"},
		{"read-only", func(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
			return hostPath(t, "tz", "/etc/localtime", "x", true)
		}, "ReadOnlyHostPath"},
		{"declared-but-unmounted", func(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
			return hostPath(t, "run", "/var/run", "", false)
		}, ""},
		// One writable mount is all it takes; the read-only mount of
		// the same path does not constrain it.
		{"writable-wins-over-read-only", func(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
			t = hostPath(t, "run", "/var/run", "x", true)
			t.Spec.Containers[0].VolumeMounts = append(t.Spec.Containers[0].VolumeMounts,
				corev1.VolumeMount{Name: "run", MountPath: "/host/run"})
			return t
		}, "WritableHostPath"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := hardenedNamespace(deploy("prod", "x", 1,
				asServiceAccount(tc.mutate(app("x")), "x")))
			res := checktest.Run(t, audit.HardeningCommand(testDeps(objs...)), "-A")
			var got []string
			for _, r := range findingLines(t, res.Stdout) {
				if r["kind"] == "audit.hostpath_mount" {
					got = append(got, r["reason"])
				}
			}
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("want no hostPath claim, got %v:\n%s", got, res.Stdout)
				}
				return
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Errorf("reasons = %v, want [%s]:\n%s", got, tc.want, res.Stdout)
			}
		})
	}
}

// A template mounting the same path twice reports the path once: the
// subject is the exposure, not the mount.
func TestHardeningHostPathDeduplicatesPaths(t *testing.T) {
	tpl := app("x")
	tpl = hostPath(tpl, "run-a", "/var/run", "x", false)
	tpl = hostPath(tpl, "run-b", "/var/run", "x", false)
	tpl = hostPath(tpl, "docker", "/var/lib/docker", "x", false)
	res := checktest.Run(t, audit.HardeningCommand(
		testDeps(hardenedNamespace(deploy("prod", "x", 1, asServiceAccount(tpl, "x")))...)), "-A")
	var got map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.hostpath_mount" {
			got = r
		}
	}
	if got == nil {
		t.Fatalf("want a hostPath finding:\n%s", res.Stdout)
	}
	if got["host_paths"] != "2" || got["host_path_names"] != "/var/lib/docker,/var/run" {
		t.Errorf("finding = %v, want the two distinct paths, sorted", got)
	}
}

// The default-ServiceAccount token is offered in every namespace of
// every cluster, so the offer alone is not a claim — it is the resting
// state of Kubernetes. The finding fires only where the token is both
// offered and taken, and either side can decline.
func TestHardeningDefaultSATokenMustBeOfferedAndTaken(t *testing.T) {
	for _, tc := range []struct {
		name      string
		automount *bool // on the default ServiceAccount
		template  func(corev1.PodTemplateSpec) corev1.PodTemplateSpec
		want      bool
	}{
		{"offered-and-taken", nil, func(t corev1.PodTemplateSpec) corev1.PodTemplateSpec { return t }, true},
		{"explicitly-offered", ptr(true), func(t corev1.PodTemplateSpec) corev1.PodTemplateSpec { return t }, true},
		{"named-explicitly-is-still-the-default-sa", nil,
			func(t corev1.PodTemplateSpec) corev1.PodTemplateSpec { return asServiceAccount(t, "default") }, true},
		{"declined-at-the-service-account", ptr(false),
			func(t corev1.PodTemplateSpec) corev1.PodTemplateSpec { return t }, false},
		// Pod-level wins where both are set, which is why the two
		// halves cannot be collapsed into one field read.
		{"declined-at-the-pod", nil, noAutomount, false},
		{"another-service-account", nil,
			func(t corev1.PodTemplateSpec) corev1.PodTemplateSpec { return asServiceAccount(t, "checkout") }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{
				nsObj("prod", map[string]string{"pod-security.kubernetes.io/enforce": "baseline"}),
				serviceAccount("prod", "default", tc.automount),
				deploy("prod", "x", 1, tc.template(app("x"))),
			}
			res := checktest.Run(t, audit.HardeningCommand(testDeps(objs...)), "-A")
			if got := kindSet(t, res.Stdout)["audit.default_sa_automount"]; got != tc.want {
				t.Errorf("default_sa_automount present = %v, want %v:\n%s", got, tc.want, res.Stdout)
			}
		})
	}
}

// The subject of the token finding is the ServiceAccount: one edit
// there fixes the whole namespace, where the per-pod remedy has to be
// repeated for every workload.
func TestHardeningDefaultSASubjectIsTheServiceAccount(t *testing.T) {
	res := checktest.Run(t, audit.HardeningCommand(testDeps(hardeningCluster()...)), "--namespace=platform")
	var got map[string]string
	for _, r := range findingLines(t, res.Stdout) {
		if r["kind"] == "audit.default_sa_automount" {
			got = r
		}
	}
	if got == nil {
		t.Fatalf("want the token finding:\n%s", res.Stdout)
	}
	if got["kind_of_object"] != "ServiceAccount" || got["name"] != "default" {
		t.Errorf("subject = %s/%s, want ServiceAccount/default", got["kind_of_object"], got["name"])
	}
	if got["mounting_workloads"] != "4" ||
		got["mounting_workload_names"] != "CronJob/nightly,Deployment/mesh,Job/adhoc,Pod/debug" {
		t.Errorf("finding = %v, want the four workloads that would receive the token", got)
	}
}

// Pod Security Admission has three states worth distinguishing: no
// label at all, a label that permits everything, and a level that
// enforces something.
func TestHardeningPodSecurityLevels(t *testing.T) {
	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   string // reason, "" for no finding
	}{
		{"unlabelled", nil, "NoPodSecurityEnforce"},
		{"privileged-is-a-deliberate-opt-out",
			map[string]string{"pod-security.kubernetes.io/enforce": "privileged"}, "PodSecurityEnforcePrivileged"},
		{"baseline", map[string]string{"pod-security.kubernetes.io/enforce": "baseline"}, ""},
		{"restricted", map[string]string{"pod-security.kubernetes.io/enforce": "restricted"}, ""},
		// warn/audit without enforce is a namespace set up for PSA and
		// left in dry-run: nothing is refused at admission.
		{"dry-run-only", map[string]string{"pod-security.kubernetes.io/warn": "restricted"}, "NoPodSecurityEnforce"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{nsObj("prod", tc.labels)}
			res := checktest.Run(t, audit.HardeningCommand(testDeps(objs...)), "-A")
			var got map[string]string
			for _, r := range findingLines(t, res.Stdout) {
				if r["kind"] == "audit.podsecurity_gaps" {
					got = r
				}
			}
			if (got != nil) != (tc.want != "") {
				t.Fatalf("finding present = %v, want %q:\n%s", got != nil, tc.want, res.Stdout)
			}
			if tc.want == "" {
				return
			}
			if got["reason"] != tc.want {
				t.Errorf("reason = %q, want %q", got["reason"], tc.want)
			}
			if got["kind_of_object"] != "Namespace" || got["name"] != "prod" {
				t.Errorf("subject = %s/%s, want Namespace/prod", got["kind_of_object"], got["name"])
			}
		})
	}
}

// An object with an owner is a copy of a template judged at that
// owner. Reporting both would double-count one defect and point at the
// object an operator cannot usefully edit.
func TestHardeningJudgesOwnedObjectsOnceAtTheOwner(t *testing.T) {
	res := checktest.Run(t, audit.HardeningCommand(testDeps(hardeningCluster()...)), "--namespace=platform")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, unwanted := range []string{"nightly-28", "checkout-7f9"} {
		if strings.Contains(res.Stdout, unwanted) {
			t.Errorf("owned object %s should be judged at its owner:\n%s", unwanted, res.Stdout)
		}
	}
	// The four surviving templates: mesh, nightly, adhoc, debug.
	if !strings.Contains(res.Stdout, "scanned=4 ") {
		t.Errorf("want scanned=4 (owned Job and Pod skipped):\n%s", res.Stdout)
	}
}

// The posture recipe: the fingerprint is the CLASS, so the same gap in
// two namespaces shares it and a fleet rollup can count them together.
func TestHardeningFingerprintIsClassNotInstance(t *testing.T) {
	objs := []runtime.Object{nsObj("a", nil), nsObj("b", nil)}
	res := checktest.Run(t, audit.HardeningCommand(testDeps(objs...)), "-A")
	recs := findingLines(t, res.Stdout)
	if len(recs) != 2 {
		t.Fatalf("want one podsecurity finding per namespace, got:\n%s", res.Stdout)
	}
	if recs[0]["fingerprint"] != recs[1]["fingerprint"] {
		t.Errorf("the same gap in two namespaces is one class: %s vs %s",
			recs[0]["fingerprint"], recs[1]["fingerprint"])
	}
	if !strings.HasPrefix(recs[0]["fingerprint"], "sha256:") {
		t.Errorf("no fingerprint: %v", recs[0])
	}
}

func TestHardeningScopes(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"all-namespaces", []string{"-A"}, "scanned=7 findings=12 elapsed=100ms namespaces=3"},
		{"one-namespace", []string{"--namespace=legacy"}, "scanned=1 findings=2 elapsed=100ms namespaces=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, audit.HardeningCommand(testDeps(hardeningCluster()...)), tc.args...)
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
			}
			if !strings.Contains(res.Stdout, tc.want) {
				t.Errorf("summary should be %q:\n%s", tc.want, res.Stdout)
			}
		})
	}
}

func TestHardeningScopeErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no-scope", nil, "no scope"},
		{"workload", []string{"--workload=Deployment/prod/checkout"}, "scoped by namespace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := checktest.Run(t, audit.HardeningCommand(testDeps(hardeningCluster()...)), tc.args...)
			if res.Code != emit.ExitUsage {
				t.Fatalf("exit %d, want %d (usage); stderr: %s", res.Code, emit.ExitUsage, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Errorf("stderr = %q, want it to mention %q", res.Stderr, tc.want)
			}
		})
	}
}

// A --namespace that does not exist is a runtime error, not an empty
// clean scan: "scanned=0 findings=0" would read as "this namespace is
// hardened".
func TestHardeningUnknownNamespaceIsAnError(t *testing.T) {
	res := checktest.Run(t, audit.HardeningCommand(testDeps(hardeningCluster()...)), "--namespace=ghost")
	if res.Code != emit.ExitRuntime {
		t.Fatalf("exit %d, want %d (runtime); stderr: %s", res.Code, emit.ExitRuntime, res.Stderr)
	}
	if res.Stdout != "" {
		t.Errorf("a failed run must not emit a summary line, got %q", res.Stdout)
	}
}

// hardenedNamespace wraps one workload in a namespace whose own
// posture is clean, so the only findings are the workload's.
func hardenedNamespace(objs ...runtime.Object) []runtime.Object {
	return append([]runtime.Object{
		nsObj("prod", map[string]string{"pod-security.kubernetes.io/enforce": "baseline"}),
		serviceAccount("prod", "default", ptr(false)),
	}, objs...)
}
