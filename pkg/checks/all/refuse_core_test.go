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
	"regexp"
	"sort"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"

	"github.com/go-steer/k8s-lookout/pkg/checks"
	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// The custom-role guard (#584). The exact-`view` guard above proves
// every command answers when the reads OUTSIDE `view` are refused; a
// custom role can just as well refuse one that `view` grants — pods,
// PVCs, events, a workload kind. Here every guarded invocation runs
// once per core read (every resource `view` grants), with that one
// read refused and everything else allowed, and must land in exactly
// one of three outcomes:
//
//   - degraded (the default): exit 0, its own contract intact, and the
//     refusal on stdout in the shared wording (checks.Refusal: what was
//     refused, why this identity lacks it, the grant that fixes it);
//   - fatal (refusedFatal): exit 1, the shared wording on stderr. Only
//     where the refused read is the command's subject or the one input
//     every claim it makes needs, so any answer would be a wrong one;
//   - untouched (refusedIrrelevant): exit 0 with no record, because the
//     command asked for the resource but nothing it reports depends on
//     it.
//
// An invocation that never asks for the resource is not judged.

// refusedFatal is, per guarded invocation, the core reads it cannot
// answer without, and why.
var refusedFatal = map[string]map[string]string{
	"bundle --workload=Deployment/prod/api": {
		"deployments.apps": "the target's own kind: there is no workload to bundle",
	},
	"triage events -A": {
		"events": "the timeline is made of events",
	},
	"triage events --workload=Deployment/prod/api": {
		"events":           "the timeline is made of events",
		"deployments.apps": "the target's own kind: there is no owner tree to walk",
	},
	"triage top --workload=Deployment/prod/api": {
		"deployments.apps": "the target's own kind",
		"replicasets.apps": "on the owner path to the member pods, and every row is a member pod",
		"pods":             "every row is a member pod",
	},
	"triage changes Pod/prod/api-6d4f-aaaaa": {
		"pods": "the target's own kind",
	},
	"triage logs --workload=Deployment/prod/api": {
		"deployments.apps": "the target is read to find its pods",
		"pods":             "the logs are the pods'",
	},
	"state edges --workload=Deployment/prod/api": {
		"deployments.apps": "the target's own kind",
	},
	"state edges --workload=Service/prod/api": {
		"services": "the target's own kind",
	},
	// A drain verdict is pods versus what refuses their eviction: a
	// blocker class left unread turns drainable= into a false "yes".
	"stab drain -A":        drainFatal,
	"stab drain --node=n1": drainFatal,
	"stab scaledown ":      drainFatal,
	// Coverage compares the policies against every pod template in the
	// namespace: without the policies every namespace reads unpoliced,
	// without a template kind a live policy reads as selecting nothing.
	"audit netpol -A": {
		"networkpolicies.networking.k8s.io": "the policies are what is judged",
		"namespaces":                        "every claim is made about a namespace",
		"deployments.apps":                  "part of the template population",
		"statefulsets.apps":                 "part of the template population",
		"daemonsets.apps":                   "part of the template population",
		"cronjobs.batch":                    "part of the template population",
		"jobs.batch":                        "part of the template population",
		"pods":                              "part of the template population",
	},
}

var drainFatal = map[string]string{
	"pods":                        "the pods are what a drain evicts",
	"poddisruptionbudgets.policy": "the blocker that refuses evictions",
	"replicasets.apps":            "resolves the singleton-controller blocker",
	"deployments.apps":            "resolves the singleton-controller blocker",
	"statefulsets.apps":           "resolves the singleton-controller blocker",
}

// refusedContext pins, for wrapped refusals, the caller's context that
// must survive ahead of the shared wording on stderr, so a diagnostic
// still says which read inside the command was refused.
var refusedContext = map[string]string{
	"triage logs --workload=Deployment/prod/api|pods":   "lookout triage logs: workload Deployment/prod/api: listing pods: forbidden: list pods — ",
	"stab drain -A|pods":                                "lookout stab drain: listing pods: forbidden: list pods — ",
	"audit netpol -A|networkpolicies.networking.k8s.io": "lookout audit netpol: listing networkpolicies: forbidden: list networkpolicies.networking.k8s.io — ",
}

// refusedIrrelevant is, per guarded invocation, the core reads it asks
// for but whose absence changes nothing it reports, and why.
var refusedIrrelevant = map[string]map[string]string{
	"triage top --workload=Deployment/prod/api": {
		"statefulsets.apps": "off the Deployment → ReplicaSet → Pod path",
		"daemonsets.apps":   "off the Deployment → ReplicaSet → Pod path",
		"jobs.batch":        "off the Deployment → ReplicaSet → Pod path",
		"cronjobs.batch":    "off the Deployment → ReplicaSet → Pod path",
	},
	"triage radius --workload=Deployment/prod/api": {
		"serviceaccounts": "not a graph kind: no neighbor is a ServiceAccount",
	},
	"triage changes --workload=Deployment/prod/api": {
		"serviceaccounts": "not a graph kind",
		"configmaps":      "config objects carry no change record the timeline reads",
	},
	"triage changes Pod/prod/api-6d4f-aaaaa": {
		"serviceaccounts": "not a graph kind",
		"configmaps":      "config objects carry no change record the timeline reads",
	},
	"state edges --workload=Service/prod/api": {
		"serviceaccounts": "the Service entry checks no workload-scoped edge",
		"configmaps":      "the Service entry checks no workload-scoped edge",
	},
}

// refusalNote is the established concise form a command reports a
// refused read in instead of a record: scan's drill-down names it on
// the summary line (drilldown_skipped=), whose documentation carries
// the grant and points at `state edges` for the full reason.
var refusalNote = map[string]string{
	"scan ": "drilldown_skipped",
}

func TestEveryReadPathCommandDegradesOrFailsWordedUnderARefusedCoreRead(t *testing.T) {
	var core []schema.GroupResource
	for gr := range checks.ViewRoleReads {
		core = append(core, gr)
	}
	sort.Slice(core, func(i, j int) bool { return core[i].String() < core[j].String() })

	used := map[string]bool{}
	for _, tc := range viewCases() {
		key := tc.name + " " + strings.Join(tc.args, " ")
		for _, gr := range core {
			target := checks.Refused("list", gr.Group, gr.Resource).Target()
			t.Run(key+" refused="+target, func(t *testing.T) {
				var asked []func() bool
				v := newGuardedClients(func(f *k8stesting.Fake) { asked = append(asked, checktest.RefuseRead(f, gr)) })
				c := tc.cmd(v)
				res := checktest.Run(t, c, tc.args...)
				hit := false
				for _, a := range asked {
					hit = hit || a()
				}
				if !hit {
					if res.Code != emit.ExitData {
						t.Fatalf("exit %d with %s never asked for\nstderr: %s", res.Code, target, res.Stderr)
					}
					return
				}
				// The shared wording, for whichever verb was refused.
				worded := regexp.MustCompile(`forbidden: (get|list|watch) ` + regexp.QuoteMeta(target) + ` — [^\n]*; grant `)
				if why, fatal := refusedFatal[key][target]; fatal {
					used[key+"|"+target] = true
					if res.Code != emit.ExitRuntime {
						t.Fatalf("exit %d, want 1 (fatal: %s)\nstdout: %s", res.Code, why, res.Stdout)
					}
					if !worded.MatchString(res.Stderr) {
						t.Errorf("the failure is not in the shared refusal wording:\n%s", res.Stderr)
					}
					if want, ok := refusedContext[key+"|"+target]; ok {
						used["context|"+key+"|"+target] = true
						if !strings.Contains(res.Stderr, want) {
							t.Errorf("the caller's context did not survive the rewording\n got: %s\nwant: %s", res.Stderr, want)
						}
					}
					return
				}
				if res.Code != emit.ExitData {
					t.Fatalf("exit %d, want 0: a refused %s should degrade (or be declared fatal in refusedFatal, with why)\nstderr: %s", res.Code, target, res.Stderr)
				}
				if err := checktest.Verify(c, res.Stdout, emit.FormatLogfmt); err != nil {
					t.Errorf("contract with %s refused: %v\n%s", target, err, res.Stdout)
				}
				if _, ok := refusedIrrelevant[key][target]; ok {
					used[key+"|"+target] = true
					return
				}
				if worded.MatchString(res.Stdout) {
					return
				}
				if note := refusalNote[key]; note != "" && noteNames(res.Stdout, note, target) {
					return
				}
				t.Errorf("exit 0 but nothing on stdout says %s was refused (declare it in refusedIrrelevant, with why, if nothing depends on it):\n%s", target, res.Stdout)
			})
		}
	}
	// The tables stay honest: an entry no run reaches is stale.
	for key := range refusedContext {
		if !used["context|"+key] {
			t.Errorf("stale refusedContext entry %q: no fatal run reaches it", key)
		}
	}
	for _, table := range []map[string]map[string]string{refusedFatal, refusedIrrelevant} {
		for key, byTarget := range table {
			for target := range byTarget {
				if !used[key+"|"+target] {
					t.Errorf("stale entry %q / %s: that invocation no longer asks for it", key, target)
				}
			}
		}
	}
}

// noteNames reports whether the summary line's note carries target.
func noteNames(stdout, note, target string) bool {
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	summary := lines[len(lines)-1]
	_, v, ok := strings.Cut(summary, " "+note+"=")
	if !ok {
		return false
	}
	v, _, _ = strings.Cut(v, " ")
	for _, r := range strings.Split(v, ",") {
		if r == target {
			return true
		}
	}
	return false
}
