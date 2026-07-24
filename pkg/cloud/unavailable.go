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

import "fmt"

// Unavailability describes a cloud capability a command needed but
// could not get. It is the §2-mandated explicit degradation record:
// commands render Marker() into their summary line and/or emit it as
// a finding — absent cloud is reported, never silent.
//
// TODO(m1-emit): once pkg/emit (PR #18) is on main, add the
// emit.Finding adapter (kind "cloud.unavailable", Reason/Message from
// this struct, capability + provider as detail fields) so checks emit
// this through the standard envelope instead of hand-rolling.
type Unavailability struct {
	// Provider is the Name() of the provider that was asked.
	Provider string
	// Capability is what the command needed.
	Capability Capability
	// Reason is the operator-facing why.
	Reason string
}

// Unavailable resolves why capability c is not usable on p. Commands
// call it after a capability getter returned available=false:
//
//	q, ok := p.Quota()
//	if !ok {
//	    return cloud.Unavailable(p, cloud.CapabilityQuota) // → explicit marker
//	}
func Unavailable(p Provider, c Capability) Unavailability {
	u := Unavailability{Provider: p.Name(), Capability: c}
	for _, status := range p.Capabilities() {
		if status.Capability == c && !status.Available && status.Reason != "" {
			u.Reason = status.Reason
			return u
		}
	}
	u.Reason = fmt.Sprintf("provider %q does not provide %s", p.Name(), c)
	return u
}

// Marker renders the §2-prescribed summary-line marker:
//
//	unavailable reason="no cloud provider configured"
func (u Unavailability) Marker() string {
	return fmt.Sprintf("unavailable reason=%q", u.Reason)
}
