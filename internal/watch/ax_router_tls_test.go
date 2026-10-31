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

package watch

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/internal/axsink"
)

// writeRouterMounts writes what the two projected volumes would hold: a
// podCertificate credential bundle (PKCS#8 key, then the chain) and a
// trust bundle. The handshake itself is covered against an mTLS router
// stand-in in internal/axsink; here only the files have to be readable.
func writeRouterMounts(t *testing.T) (bundle, trust string) {
	t.Helper()
	cert, caFile := selfSignedCert(t)
	der, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	out := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})...)
	bundle = filepath.Join(t.TempDir(), "credential-bundle.pem")
	if err := os.WriteFile(bundle, out, 0o600); err != nil {
		t.Fatal(err)
	}
	return bundle, caFile
}

func axRouterFlags(t *testing.T, routerURL, bundle, trust string) *flags {
	t.Helper()
	return &flags{
		axServer:       "127.0.0.1:1",
		axTaskTemplate: writeTemplate(t, "kind: Task\nspec:\n  image: agent@sha256:abc\n"),
		axRouterURL:    routerURL,
		axTaskScope:    axScopeIncident,
		axRouterBundle: bundle,
		axRouterTrust:  trust,
		axRouterName:   axsink.DefaultRouterServerName,
	}
}

// The default router URL is https, so the default deployment is mTLS: the
// sink builds the router client from the two mounts and says, at startup,
// which identity the router has to allow.
func TestNewAXSink_DefaultRouterIsMTLS(t *testing.T) {
	bundle, trust := writeRouterMounts(t)
	f := axRouterFlags(t, defaultAXRouterURL, bundle, trust)

	buf, restoreLog := captureLogOutput(t)
	defer restoreLog()
	c, closeRouter, err := axRouterHTTPClient(f)
	if err != nil {
		t.Fatalf("axRouterHTTPClient: %v", err)
	}
	defer func() { _ = closeRouter() }()
	if c == nil {
		t.Fatal("an https router with a credential bundle got the shared sink client, not an mTLS one")
	}
	if c.Timeout <= 0 {
		t.Error("the router client has no timeout")
	}
	out := buf.String()
	for _, want := range []string{"over mTLS", bundle, trust, axsink.DefaultRouterServerName, "ingressAllowedSPIFFEIDs"} {
		if !strings.Contains(out, want) {
			t.Errorf("startup log does not mention %q: %q", want, out)
		}
	}

	_, closeAX, err := newAXSink(f, "tok")
	if err != nil {
		t.Fatalf("newAXSink: %v", err)
	}
	closeAX()
}

// Without the mounts, an https router fails startup naming the file and
// both ways out, rather than failing each incident's readiness wait two
// minutes in.
func TestNewAXSink_HTTPSRouterWithoutTheMountsFailsAtStartup(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "podidentity", "credential-bundle.pem")
	_, trust := writeRouterMounts(t)
	f := axRouterFlags(t, defaultAXRouterURL, missing, trust)
	_, _, err := newAXSink(f, "tok")
	if err == nil {
		t.Fatal("newAXSink succeeded with no client credential mounted")
	}
	for _, want := range []string{missing, "podidentity podCertificate", "http:// --ax-router-url"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q: %v", want, err)
		}
	}
}

// http stays plaintext and an https router with no credential bundle stays
// one-way TLS on the system roots: both use the shared sink client and read
// no files, so existing deployments that name their URL keep working.
func TestAXRouterHTTPClient_PlainAndOneWayModesReadNoFiles(t *testing.T) {
	absent := filepath.Join(t.TempDir(), "absent.pem")
	for _, c := range []struct {
		name, url, bundle, wantLog string
	}{
		{"http", "http://atenet-router.ate-system.svc.cluster.local", absent, "only Agent Substrate v0.5 and earlier"},
		{"https without a client certificate", "https://router.example.internal", "", "no client certificate"},
	} {
		t.Run(c.name, func(t *testing.T) {
			buf, restoreLog := captureLogOutput(t)
			defer restoreLog()
			hc, closeRouter, err := axRouterHTTPClient(axRouterFlags(t, c.url, c.bundle, absent))
			if err != nil {
				t.Fatalf("axRouterHTTPClient: %v", err)
			}
			_ = closeRouter()
			if hc != nil {
				t.Error("want the shared sink client (nil)")
			}
			if !strings.Contains(buf.String(), c.wantLog) {
				t.Errorf("startup log %q does not say %q", buf.String(), c.wantLog)
			}
		})
	}
}

// deploy-ax/ is the in-repo example of the ax sink. Its task template must
// decode, and its two projected volumes must land where the --ax-router-*
// defaults look, or the overlay ships a pod that crash-loops on a missing
// credential bundle.
func TestDeployAXOverlayMatchesTheRouterDefaults(t *testing.T) {
	if _, err := loadAXTaskTemplate("../../deploy-ax/task.yaml"); err != nil {
		t.Fatalf("deploy-ax/task.yaml: %v", err)
	}
	raw, err := os.ReadFile("../../deploy-ax/kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var k struct {
		Patches []struct {
			Patch string `json:"patch"`
		} `json:"patches"`
	}
	if err := yaml.Unmarshal(raw, &k); err != nil {
		t.Fatal(err)
	}
	type volume struct {
		Name      string `json:"name"`
		MountPath string `json:"mountPath"`
		Projected struct {
			Sources []struct {
				PodCertificate *struct {
					SignerName           string `json:"signerName"`
					CredentialBundlePath string `json:"credentialBundlePath"`
				} `json:"podCertificate"`
				ClusterTrustBundle *struct {
					SignerName string `json:"signerName"`
					Path       string `json:"path"`
				} `json:"clusterTrustBundle"`
			} `json:"sources"`
		} `json:"projected"`
	}
	mounts := map[string]string{} // volume name -> mountPath
	files := map[string]string{}  // volume name -> projected file name
	for _, p := range k.Patches {
		var ops []struct {
			Op    string          `json:"op"`
			Path  string          `json:"path"`
			Value json.RawMessage `json:"value"`
		}
		if err := yaml.Unmarshal([]byte(p.Patch), &ops); err != nil {
			t.Fatal(err)
		}
		for _, op := range ops {
			var v volume
			switch {
			case strings.HasSuffix(op.Path, "/volumeMounts/-"):
				_ = json.Unmarshal(op.Value, &v)
				mounts[v.Name] = v.MountPath
			case strings.HasSuffix(op.Path, "/volumes/-"):
				_ = json.Unmarshal(op.Value, &v)
				for _, s := range v.Projected.Sources {
					if pc := s.PodCertificate; pc != nil && pc.SignerName == "podidentity.podcert.ate.dev/identity" {
						files[v.Name] = pc.CredentialBundlePath
					}
					if tb := s.ClusterTrustBundle; tb != nil && tb.SignerName == "servicedns.podcert.ate.dev/identity" {
						files[v.Name] = tb.Path
					}
				}
			}
		}
	}
	for _, want := range []string{axsink.DefaultRouterCredentialBundle, axsink.DefaultRouterTrustBundle} {
		found := false
		for name, file := range files {
			if filepath.Join(mounts[name], file) == want {
				found = true
			}
		}
		if !found {
			t.Errorf("deploy-ax mounts nothing at %s (mounts %v, projected files %v)", want, mounts, files)
		}
	}
}
