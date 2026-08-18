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

package state_test

// §13 testing conventions: fake.Clientset fixture clusters, exact
// findings asserted per broken check class, a golden mixed cluster,
// and the checktest contract round-trip. The healthy fixture proves
// zero nominal state: a fully wired workload emits nothing.
//
// TLS fixtures are generated in-test (self-signed, ECDSA): no
// committed certificate, and no private key anywhere — tls.key holds
// a clearly synthetic placeholder because the check never reads it.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/state"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// fixedNow anchors all TLS expiry math; certificates are generated
// relative to it so days_left is deterministic.
var fixedNow = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

func testCommand(objs ...runtime.Object) checks.Command {
	cs := fake.NewClientset(objs...)
	return state.EdgesCommand(state.Deps{
		Client: func(context.Context) (kubernetes.Interface, error) { return cs, nil },
		Now:    func() time.Time { return fixedNow },
	})
}

// cluster is the mutable healthy fixture: tests break exactly one
// class per variant by editing or nil-ing fields.
type cluster struct {
	deployment  *appsv1.Deployment
	rs          *appsv1.ReplicaSet
	podA, podB  *corev1.Pod
	cmApp       *corev1.ConfigMap
	cmFlags     *corev1.ConfigMap
	secDB       *corev1.Secret
	secTLS      *corev1.Secret
	sa          *corev1.ServiceAccount
	svc         *corev1.Service
	slice       *discoveryv1.EndpointSlice
	ingress     *netv1.Ingress
	ingClass    *netv1.IngressClass
	role        *rbacv1.Role
	rb          *rbacv1.RoleBinding
	clusterRole *rbacv1.ClusterRole
	crb         *rbacv1.ClusterRoleBinding
	extra       []runtime.Object
}

func (c *cluster) objects() []runtime.Object {
	var out []runtime.Object
	if c.deployment != nil {
		out = append(out, c.deployment)
	}
	if c.rs != nil {
		out = append(out, c.rs)
	}
	if c.podA != nil {
		out = append(out, c.podA)
	}
	if c.podB != nil {
		out = append(out, c.podB)
	}
	if c.cmApp != nil {
		out = append(out, c.cmApp)
	}
	if c.cmFlags != nil {
		out = append(out, c.cmFlags)
	}
	if c.secDB != nil {
		out = append(out, c.secDB)
	}
	if c.secTLS != nil {
		out = append(out, c.secTLS)
	}
	if c.sa != nil {
		out = append(out, c.sa)
	}
	if c.svc != nil {
		out = append(out, c.svc)
	}
	if c.slice != nil {
		out = append(out, c.slice)
	}
	if c.ingress != nil {
		out = append(out, c.ingress)
	}
	if c.ingClass != nil {
		out = append(out, c.ingClass)
	}
	if c.role != nil {
		out = append(out, c.role)
	}
	if c.rb != nil {
		out = append(out, c.rb)
	}
	if c.clusterRole != nil {
		out = append(out, c.clusterRole)
	}
	if c.crb != nil {
		out = append(out, c.crb)
	}
	return append(out, c.extra...)
}

const (
	ns   = "prod"
	hash = "7c9d8"
)

func healthy(t *testing.T) *cluster {
	t.Helper()
	podLabels := map[string]string{"app": "api", "pod-template-hash": hash}
	tmpl := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
		Spec:       corev1.PodSpec{ServiceAccountName: "api-sa"},
	}
	return &cluster{
		deployment: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "api"},
			Spec:       appsv1.DeploymentSpec{Template: tmpl},
		},
		rs: &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns, Name: "api-" + hash,
				OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "api"}},
			},
			Spec: appsv1.ReplicaSetSpec{Template: tmpl},
		},
		podA:  apiPod("api-"+hash+"-aaaaa", podLabels, true),
		podB:  apiPod("api-"+hash+"-bbbbb", podLabels, true),
		cmApp: configMap("app-config", "log.level", "config.yaml"),
		cmFlags: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "feature-flags"},
			Data:       map[string]string{"flags.yaml": "{}"},
		},
		secDB: &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "db-cred"},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{"password": []byte("fixture-not-a-real-credential")},
		},
		secTLS: tlsSecret(t, "api-tls", "api.example.com", fixedNow.Add(92*24*time.Hour)),
		sa:     &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "api-sa"}},
		svc: &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "api"},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "api"},
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80}},
			},
		},
		slice: apiSlice(
			endpoint("api-"+hash+"-aaaaa", true),
			endpoint("api-"+hash+"-bbbbb", true),
		),
		ingress: &netv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "api"},
			Spec: netv1.IngressSpec{
				TLS:   []netv1.IngressTLS{{Hosts: []string{"api.example.com"}, SecretName: "api-tls"}},
				Rules: []netv1.IngressRule{rule("api.example.com", "/", backendByName("api", "http"))},
			},
		},
		// The cluster's default ingress class. Without one, an Ingress
		// naming no class is served by nothing in particular — itself a
		// finding, so a healthy fixture has to have one.
		ingClass: &netv1.IngressClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "nginx",
				Annotations: map[string]string{"ingressclass.kubernetes.io/is-default-class": "true"},
			},
		},
		role: &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "api-role"}},
		rb: &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "api-rb"},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: ns, Name: "api-sa"}},
			RoleRef:    rbacv1.RoleRef{Kind: "Role", Name: "api-role"},
		},
		clusterRole: &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "api-cr"}},
		crb: &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "api-crb"},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: ns, Name: "api-sa"}},
			RoleRef:    rbacv1.RoleRef{Kind: "ClusterRole", Name: "api-cr"},
		},
	}
}

func apiPod(name string, labels map[string]string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "api-" + hash}},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "api-sa",
			Containers: []corev1.Container{{
				Name: "api",
				Env: []corev1.EnvVar{
					{Name: "LOG_LEVEL", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "app-config"}, Key: "log.level"}}},
					{Name: "DB_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "db-cred"}, Key: "password"}}},
				},
				EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "feature-flags"}}}},
			}},
			Volumes: []corev1.Volume{
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "app-config"},
					Items:                []corev1.KeyToPath{{Key: "config.yaml", Path: "config.yaml"}}}}},
				{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: "api-tls"}}},
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}

func configMap(name string, keys ...string) *corev1.ConfigMap {
	data := map[string]string{}
	for _, k := range keys {
		data[k] = "v"
	}
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Data: data}
}

func endpoint(pod string, ready bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses:  []string{"10.0.0.1"},
		Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: pod},
	}
}

func apiSlice(endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: "api-abc12",
			Labels: map[string]string{discoveryv1.LabelServiceName: "api"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   endpoints,
	}
}

func rule(host, path string, b netv1.IngressBackend) netv1.IngressRule {
	pt := netv1.PathTypePrefix
	return netv1.IngressRule{
		Host: host,
		IngressRuleValue: netv1.IngressRuleValue{HTTP: &netv1.HTTPIngressRuleValue{
			Paths: []netv1.HTTPIngressPath{{Path: path, PathType: &pt, Backend: b}},
		}},
	}
}

func backendByName(svc, port string) netv1.IngressBackend {
	return netv1.IngressBackend{Service: &netv1.IngressServiceBackend{
		Name: svc, Port: netv1.ServiceBackendPort{Name: port}}}
}

func backendByNumber(svc string, port int32) netv1.IngressBackend {
	return netv1.IngressBackend{Service: &netv1.IngressServiceBackend{
		Name: svc, Port: netv1.ServiceBackendPort{Number: port}}}
}

// newTestCert returns a PEM self-signed certificate expiring at
// notAfter. SYNTHETIC TEST FIXTURE — generated fresh per run, private
// key discarded here and never serialized.
func newTestCert(t *testing.T, cn string, notAfter time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    notAfter.Add(-365 * 24 * time.Hour),
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func tlsSecret(t *testing.T, name, cn string, notAfter time.Time) *corev1.Secret {
	t.Helper()
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey: newTestCert(t, cn, notAfter),
			// Not a key on purpose: the check never reads tls.key and
			// fixtures must not carry real-looking key material.
			corev1.TLSPrivateKeyKey: []byte("SYNTHETIC-FIXTURE-NOT-A-KEY"),
		},
	}
}

// findings returns the finding lines of a successful run (summary
// stripped), failing the test on non-zero exit.
func findings(t *testing.T, c *cluster, args ...string) []string {
	t.Helper()
	res := checktest.Run(t, testCommand(c.objects()...),
		append([]string{"--workload=Deployment/prod/api"}, args...)...)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[len(lines)-1], "scanned=") {
		t.Fatalf("no summary line: %q", res.Stdout)
	}
	return lines[:len(lines)-1]
}

func wantFindings(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d findings, want %d:\ngot:\n  %s\nwant:\n  %s",
			len(got), len(want), strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("finding %d:\n got: %s\nwant: %s", i, got[i], want[i])
		}
	}
}

const wl = "Deployment/prod/api"

func TestEdgesHealthyIsSilent(t *testing.T) {
	c := healthy(t)
	res := checktest.Run(t, testCommand(c.objects()...), "--workload="+wl)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	want := "scanned=17 findings=0 elapsed=100ms\n"
	if res.Stdout != want {
		t.Errorf("healthy cluster must emit only the summary:\ngot:  %qwant: %q", res.Stdout, want)
	}
}

func TestEdgesMissingRefs(t *testing.T) {
	c := healthy(t)
	c.cmApp = nil // referenced via env key and via volume items
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=ConfigMap name=app-config reason=CreateContainerConfigError message="configmap app-config not found (env LOG_LEVEL in container api)" workload=Deployment/prod/api container=api env=LOG_LEVEL key=log.level pods=2`,
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=ConfigMap name=app-config reason=FailedMount message="configmap app-config not found (volume config)" workload=Deployment/prod/api volume=config key=config.yaml pods=2`,
	})
}

func TestEdgesMissingKey(t *testing.T) {
	c := healthy(t)
	c.cmApp = configMap("app-config", "verbosity", "config.yaml") // log.level renamed
	c.secDB.Data = map[string][]byte{"passwd": []byte("x")}       // password renamed
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.missing_key severity=critical namespace=prod kind_of_object=ConfigMap name=app-config reason=CreateContainerConfigError message="key log.level not found in configmap app-config (env LOG_LEVEL in container api)" workload=Deployment/prod/api container=api env=LOG_LEVEL key=log.level pods=2`,
		`kind=edge.missing_key severity=critical namespace=prod kind_of_object=Secret name=db-cred reason=CreateContainerConfigError message="key password not found in secret db-cred (env DB_PASSWORD in container api)" workload=Deployment/prod/api container=api env=DB_PASSWORD key=password pods=2`,
	})
}

func TestEdgesOptionalRefsAreSilent(t *testing.T) {
	c := healthy(t)
	c.cmFlags = nil
	opt := true
	c.podA.Spec.Containers[0].EnvFrom[0].ConfigMapRef.Optional = &opt
	c.podB.Spec.Containers[0].EnvFrom[0].ConfigMapRef.Optional = &opt
	got := findings(t, c)
	wantFindings(t, got, nil)
}

// dockerSecret is a well-formed registry credential of the type the
// kubelet accepts. The payload is a literal, not a credential: the
// check reads the Secret's TYPE and never its data.
func dockerSecret(name string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{}}`)},
	}
}

func TestEdgesImagePullSecretMissing(t *testing.T) {
	c := healthy(t)
	ref := []corev1.LocalObjectReference{{Name: "regcred"}}
	c.podA.Spec.ImagePullSecrets = ref
	c.podB.Spec.ImagePullSecrets = ref
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=Secret name=regcred reason=FailedToRetrieveImagePullSecret message="imagePullSecret regcred not found (referenced by pod spec) — private images cannot be pulled" workload=Deployment/prod/api via=imagePullSecret pods=2`,
	})
}

// TestEdgesImagePullSecretFromServiceAccount is the case the pod spec
// hides: the kubelet merges the ServiceAccount's imagePullSecrets in
// at pull time, so `kubectl get pod -o yaml` shows nothing wrong.
func TestEdgesImagePullSecretFromServiceAccount(t *testing.T) {
	c := healthy(t)
	c.sa.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "sa-regcred"}}
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=Secret name=sa-regcred reason=FailedToRetrieveImagePullSecret message="imagePullSecret sa-regcred not found (referenced by serviceaccount api-sa) — private images cannot be pulled" workload=Deployment/prod/api via=imagePullSecret service_account=api-sa pods=2`,
	})
}

func TestEdgesImagePullSecretWrongType(t *testing.T) {
	c := healthy(t)
	ref := []corev1.LocalObjectReference{{Name: "regcred"}}
	c.podA.Spec.ImagePullSecrets = ref
	c.podB.Spec.ImagePullSecrets = ref
	// Exists, but Opaque — the kubelet ignores it silently.
	c.extra = append(c.extra, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "regcred"},
		Type:       corev1.SecretTypeOpaque,
	})
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.invalid_ref severity=warning namespace=prod kind_of_object=Secret name=regcred reason=InvalidImagePullSecret message="imagePullSecret is type Opaque, want kubernetes.io/dockerconfigjson or kubernetes.io/dockercfg — the kubelet ignores it (referenced by pod spec)" workload=Deployment/prod/api via=imagePullSecret pods=2`,
	})
}

func TestEdgesImagePullSecretHealthyIsSilent(t *testing.T) {
	c := healthy(t)
	ref := []corev1.LocalObjectReference{{Name: "regcred"}}
	c.podA.Spec.ImagePullSecrets = ref
	c.podB.Spec.ImagePullSecrets = ref
	c.sa.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "sa-regcred"}}
	c.extra = append(c.extra, dockerSecret("regcred"), dockerSecret("sa-regcred"))
	wantFindings(t, findings(t, c), nil)
}

// statefulCluster swaps the Deployment/ReplicaSet owner chain for a
// StatefulSet so the governing-Service and volumeClaimTemplate checks
// have a real target. Everything else stays the healthy fixture.
func statefulCluster(t *testing.T, mutate func(*appsv1.StatefulSet)) *cluster {
	t.Helper()
	c := healthy(t)
	c.deployment, c.rs = nil, nil
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "db"},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "api",
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
				Spec:       corev1.PodSpec{ServiceAccountName: "api-sa"},
			},
		},
	}
	mutate(sts)
	c.extra = append(c.extra, sts)
	for _, p := range []*corev1.Pod{c.podA, c.podB} {
		p.OwnerReferences = []metav1.OwnerReference{{Kind: "StatefulSet", Name: "db"}}
	}
	return c
}

func statefulFindings(t *testing.T, c *cluster) []string {
	t.Helper()
	res := checktest.Run(t, testCommand(c.objects()...), "--workload=StatefulSet/prod/db")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
	return lines[:len(lines)-1]
}

func TestEdgesStatefulSetGoverningServiceMissing(t *testing.T) {
	c := statefulCluster(t, func(s *appsv1.StatefulSet) { s.Spec.ServiceName = "db-headless" })
	wantFindings(t, statefulFindings(t, c), []string{
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=Service name=db-headless reason=MissingGoverningService message="governing service db-headless not found — statefulset pods have no stable DNS identity" workload=StatefulSet/prod/db service=db-headless`,
	})
}

func TestEdgesStatefulSetVolumeClaimStorageClassMissing(t *testing.T) {
	gone, fine := "gone", "standard"
	c := statefulCluster(t, func(s *appsv1.StatefulSet) {
		s.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
			{ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &gone}},
			{ObjectMeta: metav1.ObjectMeta{Name: "wal"},
				Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &fine}},
			// nil means "cluster default" — a different claim, and
			// `state storage` owns it.
			{ObjectMeta: metav1.ObjectMeta{Name: "logs"}},
		}
	})
	c.extra = append(c.extra, &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "standard"}})
	wantFindings(t, statefulFindings(t, c), []string{
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=StorageClass name=gone reason=MissingStorageClass message="volumeClaimTemplate data names storageclass gone, which does not exist — new replicas stay Pending" workload=StatefulSet/prod/db volume=data`,
	})
}

func TestEdgesStatefulSetHealthyIsSilent(t *testing.T) {
	fine := "standard"
	c := statefulCluster(t, func(s *appsv1.StatefulSet) {
		s.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
			{ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &fine}},
		}
	})
	c.extra = append(c.extra, &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "standard"}})
	wantFindings(t, statefulFindings(t, c), nil)
}

func TestEdgesIngressClassMissing(t *testing.T) {
	c := healthy(t)
	name := "traefik"
	c.ingress.Spec.IngressClassName = &name
	wantFindings(t, findings(t, c), []string{
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=IngressClass name=traefik reason=MissingIngressClass message="ingressClassName traefik does not exist — no controller will serve this ingress (cluster has: nginx)" workload=Deployment/prod/api ingress=api`,
	})
}

func TestEdgesIngressUnclassed(t *testing.T) {
	c := healthy(t)
	c.ingClass = nil // no class object at all, so no cluster default
	wantFindings(t, findings(t, c), []string{
		`kind=edge.unclassed severity=warning namespace=prod kind_of_object=Ingress name=api reason=NoIngressClass message="ingress names no class and no ingressclass declares itself the cluster default — unless a controller claims it by convention, nothing serves it" workload=Deployment/prod/api ingress=api`,
	})
}

// TestEdgesIngressClassEscapeHatches covers the three ways an Ingress
// is legitimately served without a matching default IngressClass
// object; each is silence, not a finding.
func TestEdgesIngressClassEscapeHatches(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*cluster)
	}{
		{"legacy annotation", func(c *cluster) {
			c.ingClass = nil
			c.ingress.Annotations = map[string]string{"kubernetes.io/ingress.class": "nginx"}
		}},
		{"GKE built-in class", func(c *cluster) {
			c.ingClass = nil
			name := "gce"
			c.ingress.Spec.IngressClassName = &name
		}},
		{"named class exists", func(c *cluster) {
			name := "nginx"
			c.ingress.Spec.IngressClassName = &name
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := healthy(t)
			tc.apply(c)
			wantFindings(t, findings(t, c), nil)
		})
	}
}

func TestEdgesSelectorEmpty(t *testing.T) {
	c := healthy(t)
	c.svc.Spec.Selector = map[string]string{"app": "api-v2"} // typo'd value, same key
	c.slice = apiSlice()                                     // controller mirrors: no endpoints
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.selector_empty severity=critical namespace=prod kind_of_object=Service name=api reason=NoMatchingPods message="selector app=api-v2 selects no pods (workload's pods carry the same label keys)" workload=Deployment/prod/api selector="app=api-v2"`,
	})
}

func TestEdgesSelectorUnready(t *testing.T) {
	c := healthy(t)
	c.podB = apiPod("api-"+hash+"-bbbbb", c.podB.Labels, false) // not Ready
	c.slice = apiSlice(                                         // slice agrees with pod state
		endpoint("api-"+hash+"-aaaaa", true),
		endpoint("api-"+hash+"-bbbbb", false),
	)
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.selector_unready severity=warning namespace=prod kind_of_object=Service name=api reason=PodsNotReady message="service selects 2 pod(s), 1 ready" workload=Deployment/prod/api selector="app=api" selected=2 ready=1`,
	})
}

func TestEdgesEndpointsMissing(t *testing.T) {
	c := healthy(t)
	c.slice = nil
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.endpoints_missing severity=critical namespace=prod kind_of_object=Service name=api reason=NoEndpointSlices message="no endpointslices exist for a service selecting 2 pod(s) — no traffic can flow" workload=Deployment/prod/api selected=2`,
	})
}

func TestEdgesEndpointsOrphanedAndMismatch(t *testing.T) {
	c := healthy(t)
	c.slice = apiSlice(
		endpoint("api-"+hash+"-aaaaa", true),
		endpoint("api-"+hash+"-bbbbb", true),
		endpoint("api-"+hash+"-ghost", false), // deleted pod still in the slice
	)
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.endpoints_orphaned severity=warning namespace=prod kind_of_object=EndpointSlice name=api-abc12 reason=OrphanedEndpoint message="endpoint targets pod api-7c9d8-ghost, which no longer exists" workload=Deployment/prod/api service=api pod=api-7c9d8-ghost`,
		`kind=edge.endpoints_unready severity=warning namespace=prod kind_of_object=Service name=api reason=EndpointMismatch message="2/3 endpoints ready across 1 slice(s); selector selects 2 pod(s), 2 ready" workload=Deployment/prod/api endpoints=3 ready=2 slices=1 selected=2`,
	})
}

func TestEdgesCerts(t *testing.T) {
	c := healthy(t)
	// Mounted cert expiring inside the default 720h window.
	c.secTLS = tlsSecret(t, "api-tls", "api.example.com", fixedNow.Add(15*24*time.Hour))
	// Ingress-only cert already expired.
	c.extra = append(c.extra, tlsSecret(t, "old-tls", "old.example.com", fixedNow.Add(-16*24*time.Hour)))
	// Ingress-only secret with garbage tls.crt.
	bad := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "bad-tls"},
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{corev1.TLSCertKey: []byte("not a certificate")},
	}
	c.extra = append(c.extra, bad)
	c.ingress.Spec.TLS = append(c.ingress.Spec.TLS,
		netv1.IngressTLS{Hosts: []string{"old.example.com"}, SecretName: "old-tls"},
		netv1.IngressTLS{Hosts: []string{"bad.example.com"}, SecretName: "bad-tls"},
	)
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.cert_expiring severity=warning namespace=prod kind_of_object=Secret name=api-tls reason=CertificateExpiringSoon message="certificate expires in 15d" workload=Deployment/prod/api via=mount ingress=api subject=api.example.com not_after=2026-07-16T00:00:00Z days_left=15`,
		`kind=edge.cert_invalid severity=warning namespace=prod kind_of_object=Secret name=bad-tls reason=InvalidCertificate message="tls.crt does not contain a parseable X.509 certificate" workload=Deployment/prod/api via=ingress ingress=api`,
		`kind=edge.cert_expired severity=critical namespace=prod kind_of_object=Secret name=old-tls reason=CertificateExpired message="certificate expired 16d ago" workload=Deployment/prod/api via=ingress ingress=api subject=old.example.com not_after=2026-06-15T00:00:00Z days_left=-16`,
	})
}

func TestEdgesCertWarnFlag(t *testing.T) {
	c := healthy(t)
	c.secTLS = tlsSecret(t, "api-tls", "api.example.com", fixedNow.Add(15*24*time.Hour))
	got := findings(t, c, "--cert-warn=240h") // 10d window: 15d out is healthy
	wantFindings(t, got, nil)
}

func TestEdgesMissingIngressTLSSecret(t *testing.T) {
	c := healthy(t)
	c.ingress.Spec.TLS[0].SecretName = "ghost-tls"
	got := findings(t, c)
	// api-tls remains mounted (healthy, far expiry); ghost-tls is
	// ingress-referenced and absent.
	wantFindings(t, got, []string{
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=Secret name=ghost-tls reason=MissingTLSSecret message="TLS secret ghost-tls referenced by ingress api not found" workload=Deployment/prod/api via=ingress ingress=api`,
	})
}

func TestEdgesRBAC(t *testing.T) {
	c := healthy(t)
	c.sa = nil          // ServiceAccount gone
	c.role = nil        // RoleBinding roleRef dangles
	c.clusterRole = nil // ClusterRoleBinding roleRef dangles
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=ServiceAccount name=api-sa reason=FailedCreate message="serviceaccount not found — new pods cannot be created" workload=Deployment/prod/api service_account=api-sa`,
		`kind=edge.rbac_dangling severity=warning namespace=prod kind_of_object=RoleBinding name=api-rb reason=DanglingRoleRef message="roleRef Role api-role not found (binds serviceaccount api-sa)" workload=Deployment/prod/api service_account=api-sa role_ref=Role/api-role`,
		`kind=edge.rbac_dangling severity=warning kind_of_object=ClusterRoleBinding name=api-crb reason=DanglingRoleRef message="roleRef ClusterRole api-cr not found (binds serviceaccount api-sa)" workload=Deployment/prod/api service_account=api-sa role_ref=ClusterRole/api-cr`,
	})
}

func TestEdgesIngressBackends(t *testing.T) {
	c := healthy(t)
	c.ingress.Spec.Rules = append(c.ingress.Spec.Rules,
		rule("api.example.com", "/v2", backendByName("ghost", "http")),
		rule("api.example.com", "/metrics", backendByNumber("api", 9999)),
	)
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.backend_missing severity=critical namespace=prod kind_of_object=Ingress name=api reason=BackendServiceMissing message="backend service ghost not found" workload=Deployment/prod/api service=ghost host=api.example.com path=/v2`,
		`kind=edge.backend_missing severity=critical namespace=prod kind_of_object=Ingress name=api reason=BackendPortMissing message="backend service api has no port 9999" workload=Deployment/prod/api service=api port=9999 host=api.example.com path=/metrics`,
	})
}

func TestEdgesPodTarget(t *testing.T) {
	c := healthy(t)
	c.cmApp = configMap("app-config", "verbosity", "config.yaml")
	res := checktest.Run(t, testCommand(c.objects()...), "--workload=Pod/prod/api-"+hash+"-aaaaa")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "workload=Pod/prod/api-7c9d8-aaaaa") ||
		!strings.Contains(res.Stdout, "pods=1") {
		t.Errorf("pod-targeted run should attribute findings to the pod:\n%s", res.Stdout)
	}
}

func TestEdgesScaledToZeroUsesTemplate(t *testing.T) {
	c := healthy(t)
	// No pods at all: the selector-intent and RBAC checks fall back
	// to the workload's pod template.
	c.podA, c.podB, c.rs = nil, nil, nil
	c.slice = apiSlice()
	c.sa = nil
	c.svc.Spec.Selector = map[string]string{"app": "api-v2"}
	got := findings(t, c)
	wantFindings(t, got, []string{
		`kind=edge.selector_empty severity=critical namespace=prod kind_of_object=Service name=api reason=NoMatchingPods message="selector app=api-v2 selects no pods (workload's pods carry the same label keys)" workload=Deployment/prod/api selector="app=api-v2"`,
		`kind=edge.missing_ref severity=critical namespace=prod kind_of_object=ServiceAccount name=api-sa reason=FailedCreate message="serviceaccount not found — new pods cannot be created" workload=Deployment/prod/api service_account=api-sa`,
	})
}

func TestEdgesUsageErrors(t *testing.T) {
	c := healthy(t)
	// A malformed or contradictory invocation is the operator's
	// mistake and exits 2 (§4.2); a well-formed invocation naming an
	// object that does not exist is a runtime lookup failure and
	// exits 1. The distinction is what lets a caller retry on 1 and
	// never retry on 2.
	tests := []struct {
		name    string
		args    []string
		stderr  string
		code    int
		useArgs bool
	}{
		{"workload required", []string{}, "requires --workload", emit.ExitUsage, true},
		{"unsupported kind", []string{"--workload=Service/prod/api"}, "unsupported workload kind", emit.ExitUsage, true},
		{"namespace contradiction", []string{"--workload=" + wl, "--namespace=other"}, "contradicts", emit.ExitUsage, true},
		{"workload not found", []string{"--workload=Deployment/prod/nonesuch"}, "not found", emit.ExitRuntime, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := checktest.Run(t, testCommand(c.objects()...), tt.args...)
			if res.Code != tt.code {
				t.Fatalf("exit %d, want %d (stderr: %s)", res.Code, tt.code, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tt.stderr) {
				t.Errorf("stderr %q does not mention %q", res.Stderr, tt.stderr)
			}
			if strings.Contains(res.Stdout, "scanned=") {
				t.Errorf("failed run must not emit a summary, stdout: %q", res.Stdout)
			}
		})
	}
}

// mixed breaks one edge of nearly every class at once — the golden
// file pins full-output ordering and formatting.
func mixed(t *testing.T) *cluster {
	t.Helper()
	c := healthy(t)
	c.cmApp = configMap("app-config", "verbosity", "config.yaml") // missing key
	c.podB = apiPod("api-"+hash+"-bbbbb", c.podB.Labels, false)   // unready pod
	c.slice = apiSlice(                                           // agrees on bbbbb, plus a ghost
		endpoint("api-"+hash+"-aaaaa", true),
		endpoint("api-"+hash+"-bbbbb", false),
		endpoint("api-"+hash+"-ghost", false),
	)
	c.secTLS = tlsSecret(t, "api-tls", "api.example.com", fixedNow.Add(15*24*time.Hour)) // expiring
	c.extra = append(c.extra, tlsSecret(t, "old-tls", "old.example.com", fixedNow.Add(-16*24*time.Hour)))
	c.ingress.Spec.TLS = append(c.ingress.Spec.TLS, netv1.IngressTLS{SecretName: "old-tls"}) // expired
	c.ingress.Spec.Rules = append(c.ingress.Spec.Rules,
		rule("api.example.com", "/v2", backendByName("ghost", "http"))) // missing backend
	c.clusterRole = nil // dangling CRB
	return c
}

func TestEdgesMixedGolden(t *testing.T) {
	res := checktest.Run(t, testCommand(mixed(t).objects()...), "--workload="+wl)
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	golden := filepath.Join("testdata", "edges-mixed.golden")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(golden, []byte(res.Stdout), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run with UPDATE_GOLDEN=1 to create)", err)
	}
	if res.Stdout != string(want) {
		t.Errorf("golden mismatch:\ngot:\n%s\nwant:\n%s", res.Stdout, want)
	}
}

func TestEdgesContract(t *testing.T) {
	checktest.VerifyContract(t, testCommand(mixed(t).objects()...), "--workload="+wl)
}

func TestEdgesRegisteredInDefaultRegistry(t *testing.T) {
	c, ok := checks.Lookup("state edges")
	if !ok {
		t.Fatal("state edges is not registered in the default registry")
	}
	if c.MCPName != "k8s_state_edges" {
		t.Errorf("MCP tool name = %q, want k8s_state_edges", c.MCPName)
	}
	if !strings.Contains(c.Help(), "--cert-warn") {
		t.Error("generated help does not document --cert-warn")
	}
}
