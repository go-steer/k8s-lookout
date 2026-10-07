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

package scan_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/k8s-lookout/pkg/checks/checktest"
	"github.com/go-steer/k8s-lookout/pkg/checks/cloudcheck"
	"github.com/go-steer/k8s-lookout/pkg/cloud"
	"github.com/go-steer/k8s-lookout/pkg/emit"
)

// refusingOrphans serves one orphaned disk and refuses the address
// sweep, as an identity without compute.addresses.list sees it.
type refusingOrphans struct{}

func (refusingOrphans) OrphanDisks(context.Context) ([]cloud.OrphanDisk, error) {
	return []cloud.OrphanDisk{{Name: "idle-disk", Zone: "us-east1-b", SizeGB: 100, Type: "pd-ssd"}}, nil
}

func (refusingOrphans) OrphanLoadBalancers(context.Context) ([]cloud.OrphanLoadBalancer, error) {
	return nil, nil
}

func (refusingOrphans) OrphanAddresses(context.Context) ([]cloud.OrphanAddress, error) {
	return nil, &cloud.PermissionDeniedError{Permission: "compute.addresses.list", Err: context.Canceled}
}

type orphanProvider struct{ cloud.Provider }

func (orphanProvider) Orphans() (cloud.OrphanAPI, bool) { return refusingOrphans{}, true }

// TestScan_CloudOrphansRefusedClassDegrades: `scan --include=cloud`
// drives the real `cloud orphans` stage, so a refused address sweep
// (#231) behaves there as it does standalone — the disk finding
// survives, the class is an explicit cloud.unavailable rolled into
// scan's unavailable= note, and no scan.check_failed is raised.
func TestScan_CloudOrphansRefusedClassDegrades(t *testing.T) {
	orphans := cloudcheck.OrphansCommand(cloudcheck.Deps{
		Provider: func(context.Context) (cloud.Provider, error) { return orphanProvider{cloud.NoProvider}, nil },
		Now:      func() time.Time { return testClock },
	})
	c := newScan(t, nil, stage("triage delta", emits()), orphans)
	res := checktest.Run(t, c, "--include=cloud", "--max-drilldown=0")
	if res.Code != emit.ExitData {
		t.Fatalf("exit %d, stderr: %s", res.Code, res.Stderr)
	}
	for _, want := range []string{
		`kind=orphan.disk`,
		`kind=cloud.unavailable severity=info reason=PermissionDenied`,
		`check="cloud orphans" capability=orphans provider=none class=addresses permission=compute.addresses.list`,
		`unavailable="cloud orphans"`,
	} {
		if !strings.Contains(res.Stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, res.Stdout)
		}
	}
	if strings.Contains(res.Stdout, "scan.check_failed") {
		t.Errorf("a refused class must not fail the stage:\n%s", res.Stdout)
	}
}
