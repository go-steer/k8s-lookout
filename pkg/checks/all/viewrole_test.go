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

package all_test

import (
	"context"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/audit"
	"github.com/go-steer/k8s-lookout/pkg/checks/bundle"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/crd"
	"github.com/go-steer/k8s-lookout/pkg/checks/delta"
	"github.com/go-steer/k8s-lookout/pkg/checks/events"
	"github.com/go-steer/k8s-lookout/pkg/checks/health"
	"github.com/go-steer/k8s-lookout/pkg/checks/inventory"
	"github.com/go-steer/k8s-lookout/pkg/checks/logs"
	"github.com/go-steer/k8s-lookout/pkg/checks/netprobe"
	"github.com/go-steer/k8s-lookout/pkg/checks/scan"
	"github.com/go-steer/k8s-lookout/pkg/checks/stab"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/checks/top"
	"github.com/go-steer/k8s-lookout/pkg/checks/triage"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// The exact-`view` guard (#546). Every read-path command that reads
// the Kubernetes API runs here against checktest.ViewRole — a
// credential bound to exactly the built-in `view` ClusterRole — and
// must exit 0: a refused read degrades into an explicit unavailable
// answer, never an exit 1 with no output. TestEveryReadPathCommandIsViewGuarded
// keeps the table honest: a newly registered command has to appear
// either here or in notKubeReadPath, with the reason it does not read
// the cluster through a client this fixture can wrap.

var viewNow = time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

// viewFixture is one small cluster with an object of every kind the
// guarded commands read, so each one has something to judge (or be
// refused) rather than passing on an empty cluster.
func viewFixture() []runtime.Object {
	labels := map[string]string{"app": "api"}
	replicas := int32(1)
	created := metav1.NewTime(viewNow.Add(-48 * time.Hour))
	podSpec := corev1.PodSpec{
		NodeName: "n1",
		Containers: []corev1.Container{{
			Name:  "api",
			Image: "example/api:1",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
			},
			EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "api-creds"}}}},
		}},
		Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
		}}},
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api", UID: "dep-api", CreationTimestamp: created, Generation: 1},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: podSpec},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, ReadyReplicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1},
	}
	ctrl := true
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-6d4f", UID: "rs-api", CreationTimestamp: created, Labels: labels,
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": "1"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "api", UID: "dep-api", Controller: &ctrl}}},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: podSpec},
		},
		Status: appsv1.ReplicaSetStatus{Replicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-6d4f-aaaaa", UID: "pod-api", CreationTimestamp: created, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "api-6d4f", UID: "rs-api", Controller: &ctrl}}},
		Spec: podSpec,
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "api", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
		},
	}
	storageClass := "standard"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "data", UID: "pvc-data", CreationTimestamp: created},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:       "pv-data",
			StorageClassName: &storageClass,
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: "pv-data", CreationTimestamp: created},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: storageClass,
			ClaimRef:         &corev1.ObjectReference{Namespace: "prod", Name: "data"},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1", UID: "node-n1", CreationTimestamp: created,
			Labels: map[string]string{"topology.kubernetes.io/zone": "us-central1-a", "kubernetes.io/hostname": "n1"}},
		Status: corev1.NodeStatus{
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("16Gi"), corev1.ResourcePods: resource.MustParse("110")},
		},
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api", UID: "svc-api", CreationTimestamp: created},
		Spec:       corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{Port: 80}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-creds", UID: "sec-api"}}
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "pd.csi.storage.gke.io"}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod", UID: "ns-prod"}}
	failPolicy := admissionv1.Fail
	webhook := &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "policy", UID: "vwc-policy"},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name:          "policy.example.com",
			FailurePolicy: &failPolicy,
			ClientConfig: admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{
				Namespace: "prod", Name: "api",
			}},
		}},
	}
	return []runtime.Object{ns, dep, rs, pod, pvc, pv, node, svc, secret, sc, webhook}
}

// viewClients is one fixture's fakes, every one of them behind the
// exact `view` role.
type viewClients struct {
	cs      *fake.Clientset
	dyn     *dynamicfake.FakeDynamicClient
	metrics *metricsfake.Clientset
	// disc is API discovery, which no RBAC role gates (every
	// authenticated subject has system:discovery), so it is NOT behind
	// the view fixture. It serves no optional group.
	disc *fakediscovery.FakeDiscovery
}

func newViewClients() *viewClients {
	return newGuardedClients(func(f *k8stesting.Fake) { checktest.ViewRoleFake(f) })
}

// newGuardedClients builds the fixture's fakes with guard installed on
// each of them — the exact `view` role here, one refused core read in
// refuse_core_test.go.
func newGuardedClients(guard func(*k8stesting.Fake)) *viewClients {
	cs := fake.NewClientset(viewFixture()...)
	guard(&cs.Fake)
	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, viewDynamicListKinds())
	guard(&dyn.Fake)
	metrics := metricsfake.NewSimpleClientset()
	guard(&metrics.Fake)
	return &viewClients{cs: cs, dyn: dyn, metrics: metrics, disc: &fakediscovery.FakeDiscovery{Fake: &k8stesting.Fake{}}}
}

// viewDynamicListKinds registers every GVR a dynamic read may touch,
// so the fake answers an empty list for a granted kind instead of
// panicking; the refused ones never get that far.
func viewDynamicListKinds() map[schema.GroupVersionResource]string {
	out := map[schema.GroupVersionResource]string{}
	add := func(group, version, resource, kind string) {
		out[schema.GroupVersionResource{Group: group, Version: version, Resource: resource}] = kind + "List"
	}
	add("", "v1", "pods", "Pod")
	add("", "v1", "services", "Service")
	add("", "v1", "configmaps", "ConfigMap")
	add("", "v1", "secrets", "Secret")
	add("", "v1", "nodes", "Node")
	add("", "v1", "namespaces", "Namespace")
	add("", "v1", "events", "Event")
	add("", "v1", "endpoints", "Endpoints")
	add("", "v1", "persistentvolumeclaims", "PersistentVolumeClaim")
	add("", "v1", "persistentvolumes", "PersistentVolume")
	add("", "v1", "serviceaccounts", "ServiceAccount")
	add("", "v1", "resourcequotas", "ResourceQuota")
	add("", "v1", "limitranges", "LimitRange")
	add("", "v1", "replicationcontrollers", "ReplicationController")
	add("apps", "v1", "deployments", "Deployment")
	add("apps", "v1", "replicasets", "ReplicaSet")
	add("apps", "v1", "statefulsets", "StatefulSet")
	add("apps", "v1", "daemonsets", "DaemonSet")
	add("batch", "v1", "jobs", "Job")
	add("batch", "v1", "cronjobs", "CronJob")
	add("autoscaling", "v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler")
	add("policy", "v1", "poddisruptionbudgets", "PodDisruptionBudget")
	add("networking.k8s.io", "v1", "ingresses", "Ingress")
	add("networking.k8s.io", "v1", "networkpolicies", "NetworkPolicy")
	add("discovery.k8s.io", "v1", "endpointslices", "EndpointSlice")
	add("storage.k8s.io", "v1", "storageclasses", "StorageClass")
	return out
}

func (v *viewClients) client() func(context.Context) (kubernetes.Interface, error) {
	return func(context.Context) (kubernetes.Interface, error) { return v.cs, nil }
}

func (v *viewClients) dynamic() func(context.Context) (dynamic.Interface, error) {
	return func(context.Context) (dynamic.Interface, error) { return v.dyn, nil }
}

func noProvider(context.Context) (cloud.Provider, error) { return cloud.NoProvider, nil }

func viewClock() time.Time { return viewNow }

// emptyLogs answers every log stream with nothing.
type emptyLogs struct{}

func (emptyLogs) Stream(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

// viewCase is one guarded invocation: the command (built over the
// fixture's view-role fakes) and its arguments.
type viewCase struct {
	name string // registry name
	args []string
	cmd  func(v *viewClients) checks.Command
}

func viewCases() []viewCase {
	wl := "--workload=Deployment/prod/api"
	healthCmd := func(v *viewClients) checks.Command {
		return health.New(health.Deps{Client: v.client(), Provider: noProvider, Now: viewClock})
	}
	stateDeps := func(v *viewClients) state.Deps {
		disc := v.disc
		return state.Deps{Client: v.client(), Now: viewClock, Dynamic: v.dynamic(), CRD: crd.NewResolver(disc)}
	}
	stabDeps := func(v *viewClients) stab.Deps {
		return stab.Deps{Client: v.client(), Now: viewClock, Provider: noProvider}
	}
	auditDeps := func(v *viewClients) audit.Deps {
		return audit.Deps{Client: v.client(), Provider: noProvider, Now: viewClock}
	}
	triageDeps := func(v *viewClients) triage.Deps { return triage.Deps{Client: v.client(), Now: viewClock} }
	topCmd := func(v *viewClients) checks.Command {
		return top.New(top.Deps{
			Client:   v.client(),
			Metrics:  func(context.Context) (metricsv.Interface, error) { return v.metrics, nil },
			Provider: noProvider,
			Now:      viewClock,
		})
	}
	return []viewCase{
		{"health", nil, healthCmd},
		{"health", []string{"--namespace=prod"}, healthCmd},
		{"bundle", []string{wl}, func(v *viewClients) checks.Command {
			return bundle.New(bundle.Deps{Client: v.client(), Logs: emptyLogs{}, Now: viewClock})
		}},
		{"triage delta", nil, func(v *viewClients) checks.Command { return delta.New(v.client()) }},
		{"triage delta", []string{"--namespace=prod"}, func(v *viewClients) checks.Command { return delta.New(v.client()) }},
		{"triage events", []string{"-A"}, func(v *viewClients) checks.Command { return events.New(v.client()) }},
		{"triage events", []string{wl}, func(v *viewClients) checks.Command { return events.New(v.client()) }},
		{"triage top", []string{"--namespace=prod"}, topCmd},
		{"triage top", []string{"-A"}, topCmd},
		{"triage top", []string{wl}, topCmd},
		// Where metrics-server runs, its aggregated role adds
		// metrics.k8s.io to `view`: the container rows then answer and
		// only the node view (Nodes) drops out.
		{"triage top", []string{"-A", "--limit=5"}, func(v *viewClients) checks.Command {
			metrics := metricsfake.NewSimpleClientset()
			return top.New(top.Deps{
				Client:   v.client(),
				Metrics:  func(context.Context) (metricsv.Interface, error) { return metrics, nil },
				Provider: noProvider,
				Now:      viewClock,
			})
		}},
		{"triage radius", []string{wl}, func(v *viewClients) checks.Command { return triage.RadiusCommand(triageDeps(v)) }},
		{"triage changes", []string{wl}, func(v *viewClients) checks.Command { return triage.ChangesCommand(triageDeps(v)) }},
		{"triage changes", []string{"Pod/prod/api-6d4f-aaaaa"}, func(v *viewClients) checks.Command { return triage.ChangesCommand(triageDeps(v)) }},
		{"triage logs", []string{wl}, func(v *viewClients) checks.Command {
			return logs.New(logs.Deps{Client: v.client(), Logs: emptyLogs{}})
		}},
		{"triage list", []string{"--namespace=prod"}, func(v *viewClients) checks.Command {
			return inventory.New(inventory.Deps{
				Dynamic:   v.dynamic(),
				Discovery: func(context.Context) (discovery.DiscoveryInterface, error) { return v.disc, nil },
			})
		}},
		{"triage list", []string{"-A"}, func(v *viewClients) checks.Command {
			return inventory.New(inventory.Deps{
				Dynamic:   v.dynamic(),
				Discovery: func(context.Context) (discovery.DiscoveryInterface, error) { return v.disc, nil },
			})
		}},
		{"triage spec", []string{"Deployment/prod/api"}, specCmd},
		{"triage spec", []string{"Node/n1"}, specCmd},
		{"triage spec", []string{"Secret/prod/api-creds"}, specCmd},
		{"state edges", []string{wl}, func(v *viewClients) checks.Command { return state.EdgesCommand(stateDeps(v)) }},
		{"state edges", []string{"--workload=Service/prod/api"}, func(v *viewClients) checks.Command { return state.EdgesCommand(stateDeps(v)) }},
		{"state webhooks", nil, func(v *viewClients) checks.Command { return state.WebhooksCommand(stateDeps(v)) }},
		{"state volumes", nil, func(v *viewClients) checks.Command { return state.VolumesCommand(stateDeps(v)) }},
		{"state volumes", []string{"--namespace=prod"}, func(v *viewClients) checks.Command { return state.VolumesCommand(stateDeps(v)) }},
		{"state storage", nil, func(v *viewClients) checks.Command { return state.StorageCommand(stateDeps(v)) }},
		{"state keda", nil, func(v *viewClients) checks.Command { return state.KEDACommand(stateDeps(v)) }},
		{"state gateway", nil, func(v *viewClients) checks.Command { return state.GatewayCommand(stateDeps(v)) }},
		{"state wi", []string{wl}, func(v *viewClients) checks.Command {
			return state.WICommand(state.WIDeps{Client: v.client(), Provider: noProvider, Now: viewClock})
		}},
		{"stab drift", nil, func(v *viewClients) checks.Command { return stab.DriftCommand(stabDeps(v)) }},
		{"stab drain", []string{"-A"}, func(v *viewClients) checks.Command { return stab.DrainCommand(stabDeps(v)) }},
		{"stab drain", []string{"--node=n1"}, func(v *viewClients) checks.Command { return stab.DrainCommand(stabDeps(v)) }},
		{"stab scaledown", nil, func(v *viewClients) checks.Command { return stab.ScaledownCommand(stabDeps(v)) }},
		{"audit cluster", nil, func(v *viewClients) checks.Command { return audit.ClusterCommand(auditDeps(v)) }},
		{"audit hardening", []string{"-A"}, func(v *viewClients) checks.Command { return audit.HardeningCommand(auditDeps(v)) }},
		{"audit netpol", []string{"-A"}, func(v *viewClients) checks.Command { return audit.NetpolCommand(auditDeps(v)) }},
		{"audit rbac", []string{"-A"}, func(v *viewClients) checks.Command { return audit.RBACCommand(auditDeps(v)) }},
		{"audit upgrades", nil, func(v *viewClients) checks.Command { return audit.UpgradesCommand(auditDeps(v)) }},
		{"audit workloads", []string{"-A"}, func(v *viewClients) checks.Command { return audit.WorkloadsCommand(auditDeps(v)) }},
		// The one privileged command: under `view` it must change
		// nothing and answer with the shared refusal (#539).
		{"net probe-from", []string{"--pod=prod/api-6d4f-aaaaa", "--image=ghcr.io/go-steer/lookout@sha256:" + strings.Repeat("ab", 32), "--tcp=db.prod.svc:5432"}, func(v *viewClients) checks.Command {
			return netprobe.NewFrom(netprobe.FromDeps{Client: v.client(), Now: viewClock}, netprobe.FromConfig{})
		}},
		{"scan", nil, scanCmd},
	}
}

func specCmd(v *viewClients) checks.Command {
	return checks.SpecCommand(checks.SpecDeps{
		Typed:   v.client(),
		Dynamic: v.dynamic(),
	})
}

// scanCmd composes every other guarded command over the same fakes,
// the way the default registry composes the production ones.
func scanCmd(v *viewClients) checks.Command {
	reg := checks.NewRegistry()
	seen := map[string]bool{}
	for _, c := range viewCases() {
		if c.name == "scan" || seen[c.name] {
			continue
		}
		seen[c.name] = true
		reg.Register(c.cmd(v))
	}
	return scan.New(scan.Deps{Registry: reg, Client: v.client(), Now: viewClock})
}

// notKubeReadPath are the registered commands this guard does not run,
// each with why: none of them reads the Kubernetes API through a
// client checktest.ViewRole can wrap.
var notKubeReadPath = map[string]string{
	"audit exemptions": "reads the --exemptions file, not the cluster",
	"triage status":    "reads and writes the sentinel's SQLite store",
	"findings diff":    "diffs two saved reports",
	"findings ack":     "writes an ack into a findings state file",
	"net probe":        "active DNS/TCP/HTTP probes, no API reads",
	"perf probe":       "Cloud Monitoring through cloud.Provider",
	"cloud stockout":   "GCP through cloud.Provider",
	"cloud orphans":    "GCP through cloud.Provider (--only=nodepools also Lists nodes/pods; a refusal degrades to read.unavailable, pinned in cloudcheck TestOrphansNodePoolsListForbidden)",
	"cloud ipspace":    "GCP through cloud.Provider",
	"cloud quota":      "GCP through cloud.Provider",
}

// TestEveryReadPathCommandExitsZeroUnderView is the guard: under
// exactly the built-in `view` role, every read-path command answers
// (exit 0, with a summary line) and its output still satisfies its
// own contract — so every degradation record it emits is declared.
func TestEveryReadPathCommandExitsZeroUnderView(t *testing.T) {
	for _, tc := range viewCases() {
		t.Run(tc.name+" "+strings.Join(tc.args, " "), func(t *testing.T) {
			v := newViewClients()
			c := tc.cmd(v)
			if c.Name != tc.name {
				t.Fatalf("case %q builds command %q", tc.name, c.Name)
			}
			res := checktest.Run(t, c, tc.args...)
			if res.Code != emit.ExitData {
				t.Fatalf("exit %d under the built-in view role, want 0\nstderr: %s\nstdout: %s", res.Code, res.Stderr, res.Stdout)
			}
			if err := checktest.Verify(c, res.Stdout, emit.FormatLogfmt); err != nil {
				t.Errorf("contract under view: %v\n%s", err, res.Stdout)
			}
			// Every degradation line names what to grant: a refusal
			// that only says "forbidden" is what #546's third round
			// fixed.
			var unread []string
			for _, line := range strings.Split(res.Stdout, "\n") {
				if strings.Contains(line, "forbidden: ") && !strings.Contains(line, "; grant ") {
					t.Errorf("a forbidden line does not say what to grant:\n%s", line)
				}
				if strings.HasPrefix(line, "kind=read.unavailable ") {
					_, resource, _ := strings.Cut(line, " resource=")
					resource, _, _ = strings.Cut(resource, " ")
					unread = append(unread, resource)
				}
			}
			key := tc.name + " " + strings.Join(tc.args, " ")
			if got, want := strings.Join(unread, ","), strings.Join(viewUnread[key], ","); got != want {
				t.Errorf("read.unavailable resources = %q, want %q\n%s", got, want, res.Stdout)
			}
		})
	}
}

// viewUnread pins, per guarded invocation, the resources it names in
// read.unavailable records under exactly `view`, in order. A case
// absent here emits none — it either reads nothing `view` refuses, or
// reports the gap in its own established form (health's unavailable
// categories, bundle's skipped= note with the refusals in its head's
// message, scan's drilldown_skipped= note beside its stages' own
// records). triage list keeps its skipped= note and, since #584, also
// words each refusal as a record.
var viewUnread = map[string][]string{
	"triage list --namespace=prod":                  {"secrets"},
	"triage list -A":                                {"secrets"},
	"triage delta ":                                 {"nodes"},
	"triage top --namespace=prod":                   {"pods.metrics.k8s.io"},
	"triage top -A":                                 {"pods.metrics.k8s.io"},
	"triage top --workload=Deployment/prod/api":     {"pods.metrics.k8s.io"},
	"triage top -A --limit=5":                       {"nodes"},
	"triage radius --workload=Deployment/prod/api":  {"nodes", "secrets"},
	"triage changes --workload=Deployment/prod/api": {"nodes"},
	"triage changes Pod/prod/api-6d4f-aaaaa":        {"nodes"},
	"triage spec Node/n1":                           {"nodes"},
	"triage spec Secret/prod/api-creds":             {"secrets"},
	"state edges --workload=Deployment/prod/api": {
		"ingressclasses.networking.k8s.io", "secrets",
		"rolebindings.rbac.authorization.k8s.io", "roles.rbac.authorization.k8s.io",
		"clusterrolebindings.rbac.authorization.k8s.io", "clusterroles.rbac.authorization.k8s.io",
		"storageclasses.storage.k8s.io",
	},
	"state edges --workload=Service/prod/api": {"ingressclasses.networking.k8s.io", "secrets"},
	"state webhooks ":                         {"validatingwebhookconfigurations.admissionregistration.k8s.io"},
	"state volumes ":                          {"persistentvolumes", "volumeattachments.storage.k8s.io", "nodes"},
	"state volumes --namespace=prod":          {"persistentvolumes", "volumeattachments.storage.k8s.io", "nodes"},
	"state storage ":                          {"storageclasses.storage.k8s.io", "persistentvolumes"},
	"stab drain -A":                           {"nodes"},
	"stab drain --node=n1":                    {"nodes"},
	"stab scaledown ":                         {"nodes"},
	"audit rbac -A": {
		"rolebindings.rbac.authorization.k8s.io", "roles.rbac.authorization.k8s.io",
		"clusterrolebindings.rbac.authorization.k8s.io", "clusterroles.rbac.authorization.k8s.io",
	},
	"audit workloads -A": {"nodes"},
	"scan ": {
		"nodes",
		"validatingwebhookconfigurations.admissionregistration.k8s.io",
		"persistentvolumes", "volumeattachments.storage.k8s.io", "nodes",
		"storageclasses.storage.k8s.io", "persistentvolumes",
	},
}

// TestEveryReadPathCommandIsViewGuarded: every registered command is
// either run by the guard above or excused with a reason.
func TestEveryReadPathCommandIsViewGuarded(t *testing.T) {
	guarded := map[string]bool{}
	for _, c := range viewCases() {
		guarded[c.name] = true
	}
	var missing []string
	for _, c := range checks.Default().All() {
		if c.Hidden {
			continue
		}
		if !guarded[c.Name] && notKubeReadPath[c.Name] == "" {
			missing = append(missing, c.Name)
		}
		if guarded[c.Name] && notKubeReadPath[c.Name] != "" {
			t.Errorf("%q is both guarded and excused", c.Name)
		}
	}
	sort.Strings(missing)
	for _, name := range missing {
		t.Errorf("%q is registered but neither runs under the view-role guard (viewCases) nor is excused in notKubeReadPath", name)
	}
}
