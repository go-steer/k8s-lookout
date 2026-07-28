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

// Auto defaults for the sentinel (--sources=auto / --storm=auto):
// probe-and-enable resolution at startup. The §11 charter is intact —
// nothing here is ever silently empty: every skip is one explicit
// line naming the source, the missing grant or API, and how to enable
// it, and the summary block ALWAYS prints (enabled lines included).
// Auto is the ONLY mode that downgrades a probe miss to a loud skip;
// an explicit --sources list or --storm=on keeps a miss fatal.
package watch

import (
	"context"
	"fmt"
	"log"
	"strings"

	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/k8s-lookout/pkg/sources"
	"github.com/go-steer/k8s-lookout/pkg/sources/capacity"
	"github.com/go-steer/k8s-lookout/pkg/sources/degradation"
	"github.com/go-steer/k8s-lookout/pkg/sources/expiry"
	"github.com/go-steer/k8s-lookout/pkg/sources/k8sevents"
	"github.com/go-steer/k8s-lookout/pkg/sources/objectstate"
	"github.com/go-steer/k8s-lookout/pkg/sources/rollout"
	"github.com/go-steer/k8s-lookout/pkg/sources/saturation"
)

// autoValue is the --sources sentinel value: resolve the portable set
// at startup instead of naming sources.
const autoValue = "auto"

// The --storm modes. validate() normalizes the bool-era true/false
// aliases onto on/off, so post-validate code switches on these three.
const (
	stormAuto = "auto"
	stormOn   = "on"
	stormOff  = "off"
)

// autoSourceNames is the auto-resolution candidate set, in §7.2 table
// order: the portable cluster sources — everything a single-cluster
// deployment can support with RBAC grants alone (plus metrics-server
// for saturation). Deliberately NOT candidates:
//   - quota: project tier (§11) — exactly one sentinel per GCP project
//     enables it, on a provider-tagged image; that is a deployment
//     decision, not a probeable cluster capability.
//   - token-burn: reads the core-agent daemon's cost stack, a paid
//     polling loop against the daemon — enabling it is a cost/topology
//     decision the operator makes explicitly.
var autoSourceNames = []string{
	k8sevents.Name, objectstate.Name, rollout.Name, saturation.Name,
	degradation.Name, expiry.Name, capacity.Name,
}

// metricsAPIGroupVersion is what the saturation availability check
// asks discovery for: the group/version its PodMetricses client reads.
const metricsAPIGroupVersion = "metrics.k8s.io/v1beta1"

// autoCandidateAccess returns each candidate's declared RBAC — the
// SAME RequiredAccess declarations the §11 probe verifies, obtained
// from throwaway constructions (construction is allocation only; no
// informer starts before Run). expiry narrows with
// --expiry-namespaces exactly like the real source, so auto probes
// precisely what an enabled source would read.
func autoCandidateAccess(f *flags, client kubernetes.Interface) map[string][]sources.Requirement {
	expiryCfg := expiry.DefaultConfig()
	expiryCfg.Namespaces = splitCSV(f.expiryNamespaces)
	return map[string][]sources.Requirement{
		k8sevents.Name:   k8sevents.New(client, 0).RequiredAccess(),
		objectstate.Name: objectstate.New(client, objectstate.DefaultConfig()).RequiredAccess(),
		rollout.Name:     rollout.New(client, rollout.DefaultConfig()).RequiredAccess(),
		saturation.Name:  saturation.New(saturation.DefaultConfig(), nil, nil).RequiredAccess(),
		degradation.Name: degradation.New(client, degradation.DefaultConfig()).RequiredAccess(),
		expiry.Name:      expiry.New(client, nil, expiryCfg).RequiredAccess(),
		capacity.Name:    capacity.New(client, nil, capacity.DefaultConfig()).RequiredAccess(),
	}
}

// grantHint names the manifest that carries a candidate's grants, for
// the skip line's "how to enable" clause.
func grantHint(name string) string {
	if name == capacity.Name {
		return "apply deploy/14-role-watcher-capacity.yaml + deploy/15-rolebinding-watcher-capacity.yaml"
	}
	return "grant it (deploy/12-clusterrole-watcher.yaml)"
}

// autoResolution is resolveSourcesAuto's outcome: the enabled source
// names (§7.2 order) plus the startup summary block, one line per
// entry. The block ALWAYS includes the enabled lines — the operator
// reads what auto decided, never infers it from silence (§11).
type autoResolution struct {
	enabled []string
	lines   []string
}

// resolveSourcesAuto resolves --sources=auto: per candidate, run its
// §11 RBAC probe (SelfSubjectAccessReview — the same machinery the
// explicit path uses) and, for saturation, additionally check that
// the metrics.k8s.io API is served (metricsCheck; discovery-backed in
// production, nil error = present). Pass → enabled; miss → skipped
// with one explicit line naming the source, the missing grant/API,
// and how to enable it.
//
// Two misses stay FATAL even under auto:
//   - k8s-events: a sentinel that cannot watch events is misdeployed —
//     there is no "auto" answer to that, only a fix.
//   - a probe that cannot be evaluated (reviewer error): "could not
//     verify" must not degrade into "assumed fine" (§11).
func resolveSourcesAuto(ctx context.Context, f *flags, client kubernetes.Interface, reviewer sources.AccessReviewer, metricsCheck func() error) (*autoResolution, error) {
	access := autoCandidateAccess(f, client)
	res := &autoResolution{
		lines: []string{
			"sources: auto — probing the portable set (RBAC per source; metrics.k8s.io for saturation); misses are skipped loudly — pin --sources explicitly to make a miss fatal (§11)",
		},
	}
	for _, name := range autoSourceNames {
		var missing *sources.Requirement
		for _, req := range access[name] {
			allowed, err := reviewer.Allowed(ctx, req)
			if err != nil {
				return nil, fmt.Errorf("sources: auto: capability probe for %q (source %q) failed: %w", req, name, err)
			}
			if !allowed {
				r := req
				missing = &r
				break
			}
		}
		switch {
		case missing != nil && name == k8sevents.Name:
			return nil, fmt.Errorf("source %q requires permission to %q and this ServiceAccount does not have it — a sentinel that cannot watch events is misdeployed; %s", name, *missing, grantHint(name))
		case missing != nil:
			res.lines = append(res.lines, fmt.Sprintf("source %s: disabled (missing %q — %s, or name it in --sources to make this fatal)", name, *missing, grantHint(name)))
		case name == saturation.Name && metricsCheck != nil && metricsCheck() != nil:
			res.lines = append(res.lines, "source saturation: disabled (metrics.k8s.io unavailable — install metrics-server)")
		case name == k8sevents.Name:
			res.enabled = append(res.enabled, name)
			res.lines = append(res.lines, fmt.Sprintf("source %s: enabled (always on — a sentinel that cannot watch events is misdeployed)", name))
		default:
			res.enabled = append(res.enabled, name)
			res.lines = append(res.lines, fmt.Sprintf("source %s: enabled", name))
		}
	}
	res.lines = append(res.lines, fmt.Sprintf("sources: auto resolved → %s (quota and token-burn stay explicit-only: project tier and the core-agent cost stack)", strings.Join(res.enabled, ",")))
	return res, nil
}

// resolveStormAuto resolves --storm=auto: probe the graph feed's
// informer grants (graphAccess — pods/nodes/replicasets list+watch,
// the same set probeGraphAccess enforces fatally for --storm=on).
// All present → on; a miss → off, with the returned line naming the
// grant either way.
//
// Deliberately INDEPENDENT of the source resolution: the graph feed
// registers its own pods/nodes/replicasets informers on the shared
// factory, so storm does not require the object-state source — when
// both run they share the factory's watches (§6.3), an optimization,
// not a dependency. A reviewer error is fatal (§11: "could not
// verify" is not "assumed fine").
func resolveStormAuto(ctx context.Context, f *flags, reviewer sources.AccessReviewer) (on bool, line string, err error) {
	if f.stormWindow <= 0 {
		return false, "storm: auto — off (--storm-window=0 disables correlation)", nil
	}
	for _, req := range graphAccess {
		allowed, aerr := reviewer.Allowed(ctx, req)
		if aerr != nil {
			return false, "", fmt.Errorf("storm: auto: capability probe for %q failed: %w", req, aerr)
		}
		if !allowed {
			return false, fmt.Sprintf("storm: auto — off (missing %q for the topology-graph informers — grant pods/nodes/replicasets list+watch (deploy/12-clusterrole-watcher.yaml), or set --storm=on to make this fatal)", req), nil
		}
	}
	return true, "storm: auto — on (pods/nodes/replicasets graph grants verified; independent of object-state — the graph feed runs its own informers, shared with the sources' when both are on)", nil
}

// resolveAutoDefaults is the realMain entry point: when --sources
// and/or --storm are auto, resolve them against the live cluster and
// rewrite the flags to the concrete values, printing the summary
// block. After this returns, the rest of startup behaves exactly as
// if the operator had pinned the resolved values — including the §11
// registry probe, which re-verifies the enabled sources' grants
// (cheap, and it keeps the explicit path's machinery untouched).
func resolveAutoDefaults(ctx context.Context, f *flags, client kubernetes.Interface) error {
	if !f.sourcesAuto() && f.storm != stormAuto {
		return nil
	}
	reviewer := sources.NewAccessReviewer(client)
	if f.sourcesAuto() {
		res, err := resolveSourcesAuto(ctx, f, client, reviewer, func() error {
			_, derr := client.Discovery().ServerResourcesForGroupVersion(metricsAPIGroupVersion)
			return derr
		})
		if err != nil {
			return err
		}
		for _, l := range res.lines {
			log.Printf("%s", l)
		}
		f.sources = strings.Join(res.enabled, ",")
	}
	if f.storm == stormAuto {
		on, line, err := resolveStormAuto(ctx, f, reviewer)
		if err != nil {
			return err
		}
		log.Printf("%s", line)
		if on {
			f.storm = stormOn
		} else {
			f.storm = stormOff
		}
	}
	return nil
}
