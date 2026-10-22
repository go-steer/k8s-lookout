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
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/scan"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// AllCommand builds `lookout audit all`: one posture pass across every
// detector in the audit group, backed by a single shared Snapshot so
// overlapping Kubernetes resource lists (Deployments, StatefulSets,
// DaemonSets, CronJobs, Jobs, Pods, Services, ServiceAccounts,
// Namespaces) are fetched at most once.
func AllCommand(deps Deps) checks.Command {
	return checks.Command{
		Name:           "audit all",
		MCPName:        "k8s_audit_all",
		MCPProfiles:    []string{"audit"},
		TimeoutDefault: 60 * time.Second,
		Summary:        "Run every posture detector in one pass over a shared cluster snapshot: workload availability and rollout safety (`audit workloads`), container and Pod Security hardening (`audit hardening`), RBAC over-permission (`audit rbac`), NetworkPolicy coverage (`audit netpol`), provider cluster configuration (`audit cluster`) and upgrade readiness (`audit upgrades`), plus exemption hygiene (`audit exemptions`) when --exemptions is given. Shared Kubernetes resources are listed at most once across all detectors; each finding carries check=<subcommand>. Scope with --namespace or -A.",
		Kinds:          allKinds(deps),
		Output:         allOutput(deps),
		Examples: []string{
			"lookout audit all -A",
			"lookout audit all --namespace=prod",
			"lookout audit all -A --exemptions=exemptions.yaml --format=json",
		},
		Run: func(ctx context.Context, inv emit.Invocation) (int, error) {
			return runAll(ctx, deps, inv)
		},
	}
}

func composedCommands(deps Deps, includeExemptions bool) []checks.Command {
	cmds := []checks.Command{
		WorkloadsCommand(deps),
		HardeningCommand(deps),
		RBACCommand(deps),
		NetpolCommand(deps),
		ClusterCommand(deps),
		UpgradesCommand(deps),
	}
	if includeExemptions {
		cmds = append(cmds, ExemptionsCommand())
	}
	seen := map[string]bool{
		"audit all":        true,
		"audit exemptions": true,
		"audit policy":     true,
	}
	for _, c := range cmds {
		seen[c.Name] = true
	}
	for _, c := range checks.Default().GroupCommands("audit") {
		if !seen[c.Name] {
			seen[c.Name] = true
			cmds = append(cmds, c)
		}
	}
	return cmds
}

func allKinds(deps Deps) []checks.KindField {
	out := []checks.KindField{
		checks.Kind(scan.KindCheckSkipped,
			"a stage declined this invocation because a zero-argument scan cannot supply something it needs — the coverage claim is smaller than it looks (§11)",
			emit.SeverityInfo),
		checks.Kind(scan.KindCheckFailed,
			"a stage errored; the scan continued without it, so this run saw less than a whole cluster — unless EVERY stage failed and none read anything, which is a runtime error (exit 1) rather than a scan",
			emit.SeverityWarning),
	}
	seen := map[string]bool{
		scan.KindCheckSkipped: true,
		scan.KindCheckFailed:  true,
	}
	for _, c := range composedCommands(deps, true) {
		for _, k := range c.Kinds {
			if seen[k.Name] {
				continue
			}
			seen[k.Name] = true
			out = append(out, k)
		}
	}
	return out
}

func allOutput(deps Deps) []checks.OutputField {
	out := []checks.OutputField{
		{Name: "check", Doc: "which registered command produced this finding — also the command to run for the full detail behind it"},
		{Name: "checks", Doc: "summary-line note: how many checks this scan actually ran"},
	}
	seen := map[string]bool{
		"check":  true,
		"checks": true,
	}
	for _, c := range composedCommands(deps, true) {
		for _, f := range c.Output {
			if seen[f.Name] {
				continue
			}
			seen[f.Name] = true
			out = append(out, f)
		}
	}
	return out
}

type allStageResult struct {
	name    string
	finding emit.Finding
}

func runAll(ctx context.Context, deps Deps, inv emit.Invocation) (int, error) {
	if !inv.Scope.Workload.IsZero() {
		return 0, emit.UsageErrorf("audit all is a cluster or namespace posture sweep; for one workload run `lookout audit workloads --workload=%s`", inv.Scope.Workload)
	}
	if inv.Scope.Namespace == "" && !inv.Scope.AllNamespaces {
		return 0, emit.UsageErrorf("no scope: pass --namespace=<ns> or -A")
	}
	listNS := inv.Scope.Namespace
	if inv.Scope.AllNamespaces {
		listNS = metav1.NamespaceAll
	}

	client, err := deps.client(ctx)
	if err != nil {
		return 0, err
	}
	ctx = WithSnapshot(ctx, NewSnapshot(client, listNS))

	var unavailable int
	inv.Out.Watch(func(f emit.Finding) {
		if strings.HasSuffix(f.Kind, ".unavailable") {
			unavailable++
		}
	})

	cmds := composedCommands(deps, inv.Exemptions != nil)
	var (
		scanned    int
		ran        int
		productive int
		failed     int
		firstErr   error
		results    []allStageResult
	)
	for _, c := range cmds {
		if ctx.Err() != nil {
			_ = inv.Out.Stamp("check", "")
			return scanned, ctx.Err()
		}
		flags, err := c.DefaultFlags()
		if err != nil {
			_ = inv.Out.Stamp("check", "")
			return scanned, err
		}
		if err := inv.Out.Stamp("check", c.Name); err != nil {
			return scanned, err
		}
		scope := emit.Scope{
			Namespace:     inv.Scope.Namespace,
			AllNamespaces: inv.Scope.AllNamespaces,
		}
		switch c.Name {
		case "audit cluster", "audit upgrades", "audit exemptions":
			scope = emit.Scope{}
		}
		unavailBefore := unavailable
		n, err := c.Run(ctx, emit.Invocation{
			Scope:      scope,
			Flags:      flags,
			In:         inv.In,
			Out:        inv.Out,
			Exemptions: inv.Exemptions,
		})
		scanned += n
		if err == nil {
			ran++
			if unavailable == unavailBefore {
				productive++
			}
			continue
		}
		if ctx.Err() != nil {
			_ = inv.Out.Stamp("check", "")
			return scanned, ctx.Err()
		}
		f := emit.Finding{
			Kind:     scan.KindCheckFailed,
			Severity: emit.SeverityWarning,
			Reason:   "CheckFailed",
			Message:  checks.WordRefusal(err).Error(),
		}
		if emit.IsUsageError(err) {
			f.Kind, f.Severity, f.Reason = scan.KindCheckSkipped, emit.SeverityInfo, "NotApplicable"
		} else {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
		results = append(results, allStageResult{name: c.Name, finding: f})
		ran++
	}
	_ = inv.Out.Stamp("check", "")
	if failed > 0 && productive == 0 && scanned == 0 {
		return scanned, fmt.Errorf("no check could read the cluster: all %d failed and nothing was scanned, "+
			"so this is a runtime error rather than a partial result: %w", failed, firstErr)
	}
	for _, r := range results {
		if err := inv.Out.Stamp("check", r.name); err != nil {
			return scanned, err
		}
		if err := inv.Out.Emit(r.finding); err != nil {
			return scanned, err
		}
	}
	if err := inv.Out.Stamp("check", ""); err != nil {
		return scanned, err
	}
	if err := inv.Out.Note("checks", itoa(ran)); err != nil {
		return scanned, err
	}
	return scanned, nil
}
