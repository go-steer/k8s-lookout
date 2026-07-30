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

package cloud

// NoProviderName is the Provider.Name() of the NoProvider sentinel,
// and the Config.Provider value that forces it.
const NoProviderName = "none"

// NoProviderReason is the unavailability reason every capability
// lookup on NoProvider reports (§2: "No provider configured →
// explicit, not broken").
const NoProviderReason = "no cloud provider configured"

// NoProvider is the sentinel Provider used when no cloud provider is
// compiled in or configured. It is a full Provider — a vanilla-k8s
// deployment runs every portable command untouched — whose capability
// lookups all return unavailable with NoProviderReason, so
// cloud-dependent commands degrade to an explicit `unavailable`
// marker instead of a nil check.
var NoProvider Provider = unconfigured{reason: NoProviderReason}

// unconfigured implements Provider with zero capabilities and a
// uniform reason. Besides NoProvider it backs the
// "multiple providers, none selected" case in New.
type unconfigured struct {
	reason string
}

func (u unconfigured) Name() string { return NoProviderName }

func (u unconfigured) Capabilities() []CapabilityStatus {
	all := AllCapabilities()
	statuses := make([]CapabilityStatus, 0, len(all))
	for _, c := range all {
		statuses = append(statuses, CapabilityStatus{Capability: c, Reason: u.reason})
	}
	return statuses
}

func (u unconfigured) Metrics() (MetricsBackend, bool)               { return nil, false }
func (u unconfigured) Capacity() (CapacityAPI, bool)                 { return nil, false }
func (u unconfigured) Quota() (QuotaAPI, bool)                       { return nil, false }
func (u unconfigured) Orphans() (OrphanAPI, bool)                    { return nil, false }
func (u unconfigured) IPSpace() (IPSpaceAPI, bool)                   { return nil, false }
func (u unconfigured) Stockouts() (StockoutAPI, bool)                { return nil, false }
func (u unconfigured) WorkloadIdentity() (WorkloadIdentityAPI, bool) { return nil, false }
func (u unconfigured) Audit() (AuditAPI, bool)                       { return nil, false }
func (u unconfigured) Notifications() (NotificationsAPI, bool)       { return nil, false }
