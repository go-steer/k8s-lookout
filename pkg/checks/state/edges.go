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

// Package state implements the `lookout state` command group
// (DESIGN.md §5): dependency and configuration verification. Its
// first command, `state edges`, is the first pkg/graph consumer —
// traversal comes from the topology index (§6.4), per-edge *validity*
// checks live here.
package state

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/crd"
	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/graph"
	"github.com/go-steer/k8s-lookout/pkg/kube"
)

// Deps are the injectable dependencies of the state commands. The
// zero value gives production behavior; tests inject a fake clientset
// and a fixed clock.
type Deps struct {
	// Client builds the Kubernetes client. Nil means kube.BuildClient
	// with default resolution (in-cluster autodetect, then
	// $KUBECONFIG / ~/.kube/config).
	Client func(ctx context.Context) (kubernetes.Interface, error)
	// Now is the clock used for TLS expiry math. Nil means time.Now.
	Now func() time.Time
	// Dynamic builds the dynamic client the CRD-gated checks read
	// through (`state gateway`). Nil means kube.BuildDynamicClient.
	// Only those checks call it, so a cluster with no optional API
	// groups installed never builds one.
	Dynamic func(ctx context.Context) (dynamic.Interface, error)
	// CRD answers which optional API groups the cluster serves. Nil
	// means one fresh resolver per invocation over the client's own
	// discovery interface; a composition passes its own so several
	// CRD-gated checks share one discovery round trip per group.
	CRD *crd.Resolver
}

func (d Deps) client(ctx context.Context) (kubernetes.Interface, error) {
	if d.Client != nil {
		return d.Client(ctx)
	}
	return kube.BuildClient(kube.OptionsFrom(ctx))
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d Deps) dynamic(ctx context.Context) (dynamic.Interface, error) {
	if d.Dynamic != nil {
		return d.Dynamic(ctx)
	}
	return kube.BuildDynamicClient(kube.OptionsFrom(ctx))
}

func (d Deps) resolver(client kubernetes.Interface) *crd.Resolver {
	if d.CRD != nil {
		return d.CRD
	}
	return crd.NewResolver(client.Discovery())
}

func init() {
	checks.Register(EdgesCommand(Deps{}))
}

// ingressClassDefaultAnnotation marks the IngressClass an Ingress with
// no ingressClassName falls back to. It is still an annotation in
// networking/v1 — there is no field for it — so the string is the API.
const ingressClassDefaultAnnotation = "ingressclass.kubernetes.io/is-default-class"

// workloadKinds are the --workload kinds `state edges` accepts: the
// pod-owning workload kinds plus Pod itself.
var workloadKinds = map[string]graph.NodeKind{
	"Pod":         graph.KindPod,
	"Deployment":  graph.KindDeployment,
	"ReplicaSet":  graph.KindReplicaSet,
	"StatefulSet": graph.KindStatefulSet,
	"DaemonSet":   graph.KindDaemonSet,
	"Job":         graph.KindJob,
	"CronJob":     graph.KindCronJob,
}

func workloadKindNames() string {
	names := make([]string, 0, len(workloadKinds))
	for k := range workloadKinds {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(names, "|")
}

// EdgesCommand builds the `lookout state edges` command (§5 tool
// matrix row: absorbs v2's edge-tracer + endpoint-resolver, adds TLS
// expiry). One-shot: paged Lists build a pkg/graph snapshot, the
// graph answers "what does this workload depend on" (§6.4), and every
// edge is then validity-checked against the listed objects. Healthy
// edges emit nothing.
func EdgesCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:    "state edges",
		MCPName: "k8s_state_edges",
		Summary: "Verify every dependency edge of one workload — ConfigMap/Secret keys, imagePullSecrets, Service selectors and endpoints, Ingress backends and class, StatefulSet governing Service and volume classes, ServiceAccount/RBAC references, TLS expiry — reporting only the broken ones.",
		Flags: []emit.FlagSpec{
			{Name: "cert-warn", Type: emit.FlagDuration, Default: "720h",
				Help: "report TLS certificates expiring within this window"},
		},
		Kinds: EdgeKinds(),
		Output: []checks.OutputField{
			{Name: "workload", Doc: "the targeted workload as <Kind>/<namespace>/<name>, stamped on every finding"},
			{Name: "pods", Doc: "how many of the workload's pods carry the broken reference"},
			{Name: "container", Doc: "container declaring the broken env/envFrom reference"},
			{Name: "env", Doc: "environment variable whose valueFrom reference is broken"},
			{Name: "volume", Doc: "pod volume, or StatefulSet volumeClaimTemplate, whose reference is broken"},
			{Name: "key", Doc: "the referenced key that is missing from the ConfigMap/Secret"},
			{Name: "selector", Doc: "the Service label selector under scrutiny"},
			{Name: "selected", Doc: "pods the Service selector currently selects"},
			{Name: "ready", Doc: "ready count (selected pods or serving endpoints, per finding kind)"},
			{Name: "endpoints", Doc: "total endpoints across the Service's EndpointSlices"},
			{Name: "slices", Doc: "how many EndpointSlices back the Service"},
			{Name: "service", Doc: "the Service a slice, Ingress backend, or StatefulSet serviceName refers to"},
			{Name: "pod", Doc: "pod named by an orphaned endpoint targetRef"},
			{Name: "subject", Doc: "TLS certificate subject (CN when set); never key material"},
			{Name: "not_after", Doc: "TLS certificate NotAfter, RFC 3339"},
			{Name: "days_left", Doc: "whole days until NotAfter (negative = expired)"},
			{Name: "via", Doc: "how the broken reference is reached from the workload: mount, ingress, or imagePullSecret"},
			{Name: "ingress", Doc: "Ingress referencing the TLS secret, or the unserved Ingress itself"},
			{Name: "host", Doc: "Ingress rule host of the broken backend (empty for the default backend)"},
			{Name: "path", Doc: "Ingress rule path of the broken backend"},
			{Name: "port", Doc: "Service port (name or number) the Ingress backend asks for"},
			{Name: "service_account", Doc: "ServiceAccount the RBAC finding is about, or the one contributing an imagePullSecret"},
			{Name: "role_ref", Doc: "dangling roleRef as <Kind>/<name>"},
		},
		Examples: []string{
			"lookout state edges --workload=Deployment/prod/api",
			"lookout state edges --workload=Pod/prod/api-6d5f8c-x2v9k --format=json",
			"lookout state edges --workload=StatefulSet/db/postgres --cert-warn=336h",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runEdges(ctx, deps, inv)
		},
	}
}

func runEdges(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	wl := inv.Scope.Workload
	if wl.IsZero() {
		return 0, emit.UsageErrorf("state edges requires --workload=<Kind>/<namespace>/<name> (a single pod works too: --workload=Pod/<ns>/<name>)")
	}
	if _, ok := workloadKinds[wl.Kind]; !ok {
		return 0, emit.UsageErrorf("unsupported workload kind %q (want %s)", wl.Kind, workloadKindNames())
	}
	if inv.Scope.Namespace != "" && inv.Scope.Namespace != wl.Namespace {
		return 0, emit.UsageErrorf("--namespace=%s contradicts --workload namespace %s", inv.Scope.Namespace, wl.Namespace)
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	listNS := wl.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}
	// One List pass + one-shot graph build (§6.3 initial-sync path),
	// via the same Cluster seam `bundle` composes over.
	cluster, err := LoadCluster(ctx, client, listNS)
	if err != nil {
		return 0, err
	}
	findings, err := cluster.EdgeFindings(wl, inv.Flags.Duration("cert-warn"), deps.now())
	if err != nil {
		return 0, err
	}
	for _, f := range findings {
		if err := inv.Out.Emit(f); err != nil {
			return 0, err
		}
	}
	return cluster.Scanned(), nil
}

// index holds the typed objects from the List pass. The graph stores
// identities and edges only (it never keeps specs, §6.5), but the
// per-edge validity checks need the live objects: key-level
// ConfigMap/Secret checks, pod readiness, endpoint conditions,
// service ports, cert bytes, RBAC references.
type index struct {
	graphObjs []any // everything pkg/graph ingests, in list order
	scanned   int   // all listed objects, including RBAC kinds

	// skipped are the (group, resource) pairs the List pass did not
	// read this run — under a tolerant load (LoadCluster with
	// Tolerate/Preflight/Lists), a per-resource Forbidden/NotFound or a
	// deselected list drops the step instead of aborting the pass, and
	// the topology is a documented partial. Empty on a full pass.
	skipped []ListRequirement

	pods            map[string]*corev1.Pod       // ns/name
	configMaps      map[string]*corev1.ConfigMap // ns/name
	secrets         map[string]*corev1.Secret    // ns/name
	services        map[string]*corev1.Service   // ns/name
	slicesByService map[string][]*discoveryv1.EndpointSlice
	ingresses       []*netv1.Ingress
	statefulSets    map[string]*appsv1.StatefulSet // ns/name

	// ingressClasses and storageClasses are cluster-scoped name sets:
	// the checks only ask "does this class exist", never read a spec.
	// defaultIngressClasses names those annotated as the cluster
	// default — more than one is itself the misconfiguration, so the
	// count is kept rather than a bool.
	ingressClasses        map[string]bool
	defaultIngressClasses []string
	storageClasses        map[string]bool

	// serviceAccounts keeps the objects, not just their names: the
	// imagePullSecrets a pod inherits from its ServiceAccount are only
	// readable there (the kubelet merges them into the pod at pull
	// time, so the pod spec never shows them).
	serviceAccounts     map[string]*corev1.ServiceAccount // ns/name
	roles               map[string]bool                   // ns/name
	clusterRoles        map[string]bool                   // name
	roleBindings        []*rbacv1.RoleBinding
	clusterRoleBindings []*rbacv1.ClusterRoleBinding

	// templates carries pod-template labels + serviceAccountName per
	// workload ("Kind/ns/name") for scale-to-zero targets, where no
	// pod exists to read them from.
	templates map[string]podTemplate
}

type podTemplate struct {
	labels           map[string]string
	serviceAccount   string
	imagePullSecrets []string
}

func key(ns, name string) string { return ns + "/" + name }

// pageLimit is the paged-List page size (§6.3: "paged List, limit
// ~5000" is the resident sentinel's number; a one-shot CLI call keeps
// pages smaller to bound peak memory).
const pageLimit = 500

// listPages drives one paged List to exhaustion: list returns a
// page's items plus the continue token; each is called per item.
func listPages[T any](what string, list func(metav1.ListOptions) ([]T, string, error), each func(*T)) error {
	opts := metav1.ListOptions{Limit: pageLimit}
	for {
		items, cont, err := list(opts)
		if err != nil {
			return fmt.Errorf("listing %s: %w", what, err)
		}
		for i := range items {
			each(&items[i])
		}
		if cont == "" {
			return nil
		}
		opts.Continue = cont
	}
}

// listCluster runs the paged Lists over the pod-nexus kinds in ns
// (NamespaceAll with -A) plus the RBAC kinds the reference-integrity
// checks need. RBAC objects are not graph kinds and are indexed only.
//
// opts controls partial loads (§7.6 least-privilege): a resource may
// be dropped up front (Lists deselection, or an SSAR preflight denial)
// or reactively (a per-step Forbidden/NotFound under Tolerate). Every
// dropped (group, resource) lands in ix.skipped so consumers can be
// honest about the gap. A default (zero-value) opts reproduces the
// original all-or-nothing pass: nothing is deselected, tolerate is
// off, so the first error aborts.
func listCluster(ctx context.Context, client kubernetes.Interface, ns string, opts loadOptions) (*index, error) {
	ix := &index{
		pods:            map[string]*corev1.Pod{},
		configMaps:      map[string]*corev1.ConfigMap{},
		secrets:         map[string]*corev1.Secret{},
		services:        map[string]*corev1.Service{},
		slicesByService: map[string][]*discoveryv1.EndpointSlice{},
		statefulSets:    map[string]*appsv1.StatefulSet{},
		ingressClasses:  map[string]bool{},
		storageClasses:  map[string]bool{},
		serviceAccounts: map[string]*corev1.ServiceAccount{},
		roles:           map[string]bool{},
		clusterRoles:    map[string]bool{},
		templates:       map[string]podTemplate{},
	}
	graphObj := func(o any) {
		ix.graphObjs = append(ix.graphObjs, o)
		ix.scanned++
	}
	template := func(kind, ns, name string, tpl corev1.PodTemplateSpec) {
		pull := make([]string, 0, len(tpl.Spec.ImagePullSecrets))
		for _, r := range tpl.Spec.ImagePullSecrets {
			pull = append(pull, r.Name)
		}
		ix.templates[kind+"/"+key(ns, name)] = podTemplate{
			labels:           tpl.Labels,
			serviceAccount:   tpl.Spec.ServiceAccountName,
			imagePullSecrets: pull,
		}
	}

	steps := []func() error{
		func() error {
			return listPages("pods", func(o metav1.ListOptions) ([]corev1.Pod, string, error) {
				l, err := client.CoreV1().Pods(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(p *corev1.Pod) { ix.pods[key(p.Namespace, p.Name)] = p; graphObj(p) })
		},
		func() error {
			// Nodes are cluster-scoped and ingested for the graph
			// only: they resolve pods' RunsOn edges so blast-radius
			// consumers (`bundle`) see placement neighbors as
			// observed nodes, not dangling references.
			return listPages("nodes", func(o metav1.ListOptions) ([]corev1.Node, string, error) {
				l, err := client.CoreV1().Nodes().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(n *corev1.Node) { graphObj(n) })
		},
		func() error {
			return listPages("deployments", func(o metav1.ListOptions) ([]appsv1.Deployment, string, error) {
				l, err := client.AppsV1().Deployments(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.Deployment) { template("Deployment", d.Namespace, d.Name, d.Spec.Template); graphObj(d) })
		},
		func() error {
			return listPages("replicasets", func(o metav1.ListOptions) ([]appsv1.ReplicaSet, string, error) {
				l, err := client.AppsV1().ReplicaSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(r *appsv1.ReplicaSet) { template("ReplicaSet", r.Namespace, r.Name, r.Spec.Template); graphObj(r) })
		},
		func() error {
			return listPages("statefulsets", func(o metav1.ListOptions) ([]appsv1.StatefulSet, string, error) {
				l, err := client.AppsV1().StatefulSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *appsv1.StatefulSet) {
				template("StatefulSet", s.Namespace, s.Name, s.Spec.Template)
				// Kept whole, unlike the other workload kinds: the
				// governing-Service and volumeClaimTemplates checks read
				// spec fields no pod carries.
				ix.statefulSets[key(s.Namespace, s.Name)] = s
				graphObj(s)
			})
		},
		func() error {
			return listPages("daemonsets", func(o metav1.ListOptions) ([]appsv1.DaemonSet, string, error) {
				l, err := client.AppsV1().DaemonSets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(d *appsv1.DaemonSet) { template("DaemonSet", d.Namespace, d.Name, d.Spec.Template); graphObj(d) })
		},
		func() error {
			return listPages("jobs", func(o metav1.ListOptions) ([]batchv1.Job, string, error) {
				l, err := client.BatchV1().Jobs(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(j *batchv1.Job) { template("Job", j.Namespace, j.Name, j.Spec.Template); graphObj(j) })
		},
		func() error {
			return listPages("cronjobs", func(o metav1.ListOptions) ([]batchv1.CronJob, string, error) {
				l, err := client.BatchV1().CronJobs(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *batchv1.CronJob) {
				template("CronJob", c.Namespace, c.Name, c.Spec.JobTemplate.Spec.Template)
				graphObj(c)
			})
		},
		func() error {
			return listPages("services", func(o metav1.ListOptions) ([]corev1.Service, string, error) {
				l, err := client.CoreV1().Services(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *corev1.Service) { ix.services[key(s.Namespace, s.Name)] = s; graphObj(s) })
		},
		func() error {
			return listPages("endpointslices", func(o metav1.ListOptions) ([]discoveryv1.EndpointSlice, string, error) {
				l, err := client.DiscoveryV1().EndpointSlices(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *discoveryv1.EndpointSlice) {
				if svc := s.Labels[discoveryv1.LabelServiceName]; svc != "" {
					k := key(s.Namespace, svc)
					ix.slicesByService[k] = append(ix.slicesByService[k], s)
				}
				graphObj(s)
			})
		},
		func() error {
			return listPages("ingresses", func(o metav1.ListOptions) ([]netv1.Ingress, string, error) {
				l, err := client.NetworkingV1().Ingresses(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(i *netv1.Ingress) { ix.ingresses = append(ix.ingresses, i); graphObj(i) })
		},
		func() error {
			// IngressClasses are cluster-scoped and not a graph kind:
			// they answer "is anything configured to serve this
			// Ingress", which needs the name set and the default
			// annotation, nothing else.
			return listPages("ingressclasses", func(o metav1.ListOptions) ([]netv1.IngressClass, string, error) {
				l, err := client.NetworkingV1().IngressClasses().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *netv1.IngressClass) {
				ix.ingressClasses[c.Name] = true
				if c.Annotations[ingressClassDefaultAnnotation] == "true" {
					ix.defaultIngressClasses = append(ix.defaultIngressClasses, c.Name)
				}
				ix.scanned++
			})
		},
		func() error {
			return listPages("configmaps", func(o metav1.ListOptions) ([]corev1.ConfigMap, string, error) {
				l, err := client.CoreV1().ConfigMaps(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(c *corev1.ConfigMap) { ix.configMaps[key(c.Namespace, c.Name)] = c; graphObj(c) })
		},
		func() error {
			// Secrets are indexed for key-name and tls.crt checks
			// only; the graph ingests ObjectMeta alone (§6.5) and no
			// value ever reaches a finding.
			return listPages("secrets", func(o metav1.ListOptions) ([]corev1.Secret, string, error) {
				l, err := client.CoreV1().Secrets(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *corev1.Secret) { ix.secrets[key(s.Namespace, s.Name)] = s; graphObj(s) })
		},
		func() error {
			return listPages("serviceaccounts", func(o metav1.ListOptions) ([]corev1.ServiceAccount, string, error) {
				l, err := client.CoreV1().ServiceAccounts(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *corev1.ServiceAccount) { ix.serviceAccounts[key(s.Namespace, s.Name)] = s; ix.scanned++ })
		},
		func() error {
			return listPages("rolebindings", func(o metav1.ListOptions) ([]rbacv1.RoleBinding, string, error) {
				l, err := client.RbacV1().RoleBindings(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(r *rbacv1.RoleBinding) { ix.roleBindings = append(ix.roleBindings, r); ix.scanned++ })
		},
		func() error {
			return listPages("roles", func(o metav1.ListOptions) ([]rbacv1.Role, string, error) {
				l, err := client.RbacV1().Roles(ns).List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(r *rbacv1.Role) { ix.roles[key(r.Namespace, r.Name)] = true; ix.scanned++ })
		},
		func() error {
			return listPages("clusterrolebindings", func(o metav1.ListOptions) ([]rbacv1.ClusterRoleBinding, string, error) {
				l, err := client.RbacV1().ClusterRoleBindings().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(r *rbacv1.ClusterRoleBinding) {
				ix.clusterRoleBindings = append(ix.clusterRoleBindings, r)
				ix.scanned++
			})
		},
		func() error {
			return listPages("clusterroles", func(o metav1.ListOptions) ([]rbacv1.ClusterRole, string, error) {
				l, err := client.RbacV1().ClusterRoles().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(r *rbacv1.ClusterRole) { ix.clusterRoles[r.Name] = true; ix.scanned++ })
		},
		func() error {
			// StorageClasses, likewise name-only: a StatefulSet's
			// volumeClaimTemplates naming a class that does not exist
			// is a dangling reference like any other. Binding hygiene
			// across the whole cluster is `state storage`'s job, not
			// this workload-scoped pass.
			return listPages("storageclasses", func(o metav1.ListOptions) ([]storagev1.StorageClass, string, error) {
				l, err := client.StorageV1().StorageClasses().List(ctx, o)
				if err != nil {
					return nil, "", err
				}
				return l.Items, l.Continue, nil
			}, func(s *storagev1.StorageClass) { ix.storageClasses[s.Name] = true; ix.scanned++ })
		},
	}
	// steps run in lockstep with LoadClusterListRequirements() so each
	// closure is attributable to its (group, resource) for skip
	// accounting; the two lists must stay the same length and order.
	reqs := LoadClusterListRequirements()
	if len(reqs) != len(steps) {
		return nil, fmt.Errorf("listCluster: %d steps but %d requirements — keep listCluster and LoadClusterListRequirements in lockstep", len(steps), len(reqs))
	}

	// Preflight (§7.6 optional): one SelfSubjectAccessReview per
	// still-selected list, so known-denied resources are dropped
	// without a 403 in the logs. If SSAR itself is not permitted (or
	// otherwise errors) we abandon the preflight and lean on the
	// reactive Forbidden-skip below — "could not check" must not
	// silently drop a resource the caller can actually read.
	deniedUpFront := make([]bool, len(reqs))
	if opts.preflight {
		for i, req := range reqs {
			if !opts.selects(req) {
				continue // deselected below; no point reviewing it
			}
			allowed, err := canList(ctx, client, req)
			if err != nil {
				break // fall back to reactive skipping
			}
			if !allowed {
				deniedUpFront[i] = true
			}
		}
	}

	for i, step := range steps {
		req := reqs[i]
		if !opts.selects(req) || deniedUpFront[i] {
			// Deselected via Lists() or denied by the preflight — never
			// attempted, recorded as a documented gap.
			ix.skipped = append(ix.skipped, req)
			continue
		}
		if err := step(); err != nil {
			if opts.tolerate && (apierrors.IsForbidden(err) || apierrors.IsNotFound(err)) {
				// Least-privilege posture: the caller may list the other
				// resources but not this one. Drop it, record it, carry
				// on — the returned topology is a partial bundle.
				ix.skipped = append(ix.skipped, req)
				continue
			}
			return nil, err
		}
	}
	sort.Slice(ix.ingresses, func(i, j int) bool {
		return key(ix.ingresses[i].Namespace, ix.ingresses[i].Name) < key(ix.ingresses[j].Namespace, ix.ingresses[j].Name)
	})
	sort.Slice(ix.roleBindings, func(i, j int) bool {
		return key(ix.roleBindings[i].Namespace, ix.roleBindings[i].Name) < key(ix.roleBindings[j].Namespace, ix.roleBindings[j].Name)
	})
	sort.Slice(ix.clusterRoleBindings, func(i, j int) bool {
		return ix.clusterRoleBindings[i].Name < ix.clusterRoleBindings[j].Name
	})
	for _, s := range ix.slicesByService {
		sort.Slice(s, func(i, j int) bool { return s[i].Name < s[j].Name })
	}
	return ix, nil
}
