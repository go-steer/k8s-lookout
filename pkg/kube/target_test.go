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

package kube

import (
	"context"
	"net/http"
	"testing"

	"golang.org/x/oauth2"
	"k8s.io/client-go/rest"
)

func targetConfig() *rest.Config {
	cfg := &rest.Config{Host: "https://abc.us-central1.gke.goog", BearerToken: "lookout-adc"}
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper { return rt })
	return cfg
}

// TestBuildConfigUsesTheTarget: a target's config wins over every other
// way of finding a cluster, and is copied, so this call's QPS and burst
// never leak into the config every later call to that cluster shares.
func TestBuildConfigUsesTheTarget(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1") // in a pod: the target still wins
	shared := targetConfig()
	cfg, err := BuildConfig(Options{Target: &Target{Name: "c", Config: shared}, QPS: 7, Burst: 9})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != shared.Host || cfg.BearerToken != "lookout-adc" {
		t.Errorf("config = %s / %q, want the target's", cfg.Host, cfg.BearerToken)
	}
	if cfg.QPS != 7 || cfg.Burst != 9 {
		t.Errorf("QPS/burst = %v/%d", cfg.QPS, cfg.Burst)
	}
	if shared.QPS != 0 || shared.Burst != 0 {
		t.Error("BuildConfig changed the shared target config")
	}
}

// TestBuildConfigTargetWithCallerToken: the caller's token replaces the
// target config's credentials, wrapper included, as it would any
// other config's.
func TestBuildConfigTargetWithCallerToken(t *testing.T) {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "caller"})
	cfg, err := BuildConfig(Options{Target: &Target{Name: "c", Config: targetConfig()}, CallerToken: ts})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BearerToken != "caller" || cfg.WrapTransport != nil {
		t.Errorf("token %q, wrapper set %t: lookout's credentials survived", cfg.BearerToken, cfg.WrapTransport != nil)
	}
}

func TestBuildConfigTargetWithoutConfig(t *testing.T) {
	if _, err := BuildConfig(Options{Target: &Target{Name: "c"}}); err == nil {
		t.Error("a target without a config built a client")
	}
}

// TestWithTargetReachesOptions: the target rides the selection to every
// client accessor, next to the caller token.
func TestWithTargetReachesOptions(t *testing.T) {
	ctx := WithTarget(context.Background(), Target{Name: "c", Project: "p", Location: "l", Config: targetConfig()})
	opts := OptionsFrom(ctx)
	if opts.Target == nil || opts.Target.Name != "c" || opts.Target.Project != "p" {
		t.Errorf("options target = %+v", opts.Target)
	}
	if SelectionFrom(ctx).IsZero() {
		t.Error("a selection with a target reads as zero")
	}
}
