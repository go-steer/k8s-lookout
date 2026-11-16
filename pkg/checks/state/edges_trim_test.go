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

package state

import (
	"bytes"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTrimEdgeConfigMapDropsValuesPreservesKeys(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:     "prod",
			Name:          "cfg",
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "helm"}},
		},
		Data:       map[string]string{"app.yaml": "large-config-body"},
		BinaryData: map[string][]byte{"blob.bin": []byte("binary-payload")},
	}
	trimEdgeConfigMap(cm)

	if len(cm.ManagedFields) != 0 {
		t.Errorf("ManagedFields not trimmed: %+v", cm.ManagedFields)
	}
	if v, ok := cm.Data["app.yaml"]; !ok || v != "" {
		t.Errorf("Data[app.yaml] = (%q, %v), want (\"\", true)", v, ok)
	}
	if v, ok := cm.BinaryData["blob.bin"]; !ok || v != nil {
		t.Errorf("BinaryData[blob.bin] = (%v, %v), want (nil, true)", v, ok)
	}
}

func TestTrimEdgeSecretDropsNonCertValuesPreservesKeysAndTLSCert(t *testing.T) {
	certBytes := []byte("-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----\n")
	tlsSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:     "prod",
			Name:          "web-tls",
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "cert-manager"}},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       certBytes,
			corev1.TLSPrivateKeyKey: []byte("SUPER-SECRET-PRIVATE-KEY"),
		},
		StringData: map[string]string{"extra": "sensitive"},
	}
	trimEdgeSecret(tlsSec)

	if len(tlsSec.ManagedFields) != 0 {
		t.Errorf("ManagedFields not trimmed: %+v", tlsSec.ManagedFields)
	}
	if !bytes.Equal(tlsSec.Data[corev1.TLSCertKey], certBytes) {
		t.Errorf("tls.crt bytes were not preserved: %q", tlsSec.Data[corev1.TLSCertKey])
	}
	if v, ok := tlsSec.Data[corev1.TLSPrivateKeyKey]; !ok || v != nil {
		t.Errorf("tls.key = (%q, %v), want (nil, true)", v, ok)
	}
	if v, ok := tlsSec.StringData["extra"]; !ok || v != "" {
		t.Errorf("StringData[extra] = (%q, %v), want (\"\", true)", v, ok)
	}

	opaqueSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "helm-release"},
		Type:       corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"release":         []byte("huge-base64-helm-blob"),
			corev1.TLSCertKey: []byte("not-a-tls-secret"),
		},
	}
	trimEdgeSecret(opaqueSec)
	if v, ok := opaqueSec.Data["release"]; !ok || v != nil {
		t.Errorf("opaque release key = (%q, %v), want (nil, true)", v, ok)
	}
	if v, ok := opaqueSec.Data[corev1.TLSCertKey]; !ok || v != nil {
		t.Errorf("opaque tls.crt key = (%q, %v), want (nil, true)", v, ok)
	}
}
