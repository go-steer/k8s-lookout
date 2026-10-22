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

package emit

import (
	"bytes"
	"fmt"
	"strings"
)

// encodeFindings renders one sanitized, policy-checked, annotated
// Finding as a structured Markdown block for --format=findings.
func encodeFindings(f Finding) []byte {
	var b bytes.Buffer
	sev := strings.ToUpper(f.Severity)
	if sev == "" {
		sev = "INFO"
	}
	fmt.Fprintf(&b, "### [%s] %s", sev, f.Kind)
	if f.Reason != "" {
		fmt.Fprintf(&b, " (`%s`)", f.Reason)
	}
	if subj := findingSubject(f); subj != "" {
		fmt.Fprintf(&b, " — `%s`", subj)
	}
	b.WriteByte('\n')

	status := "FAIL"
	if f.ExemptReason != "" {
		status = "EXEMPT"
	}
	fmt.Fprintf(&b, "- **Status:** %s\n", status)
	if f.Message != "" {
		fmt.Fprintf(&b, "- **Detail:** %s\n", f.Message)
	}
	if f.ExemptReason != "" {
		if f.ExemptExpires != "" {
			fmt.Fprintf(&b, "- **Exemption:** %s (expires `%s`)\n", f.ExemptReason, f.ExemptExpires)
		} else {
			fmt.Fprintf(&b, "- **Exemption:** %s\n", f.ExemptReason)
		}
	}
	var ev []string
	for _, d := range f.Details {
		if d.Value == "" {
			continue
		}
		ev = append(ev, fmt.Sprintf("`%s=%s`", d.Key, d.Value))
	}
	if f.ExemptReason != "" {
		ev = append(ev, fmt.Sprintf("`exempt_reason=%s`", f.ExemptReason))
		if f.ExemptExpires != "" {
			ev = append(ev, fmt.Sprintf("`exempt_expires=%s`", f.ExemptExpires))
		}
	}
	if len(ev) > 0 {
		fmt.Fprintf(&b, "- **Evidence:** %s\n", strings.Join(ev, ", "))
	}
	b.WriteByte('\n')
	return b.Bytes()
}

func findingSubject(f Finding) string {
	switch {
	case f.KindOfObject != "" && f.Namespace != "" && f.Name != "":
		return f.KindOfObject + "/" + f.Namespace + "/" + f.Name
	case f.KindOfObject != "" && f.Name != "":
		return f.KindOfObject + "/" + f.Name
	case f.Namespace != "" && f.Name != "":
		return f.Namespace + "/" + f.Name
	case f.Name != "":
		return f.Name
	case f.Namespace != "":
		return f.Namespace
	case f.KindOfObject != "":
		return f.KindOfObject
	default:
		return ""
	}
}

// encodeFindingsSummary renders a Markdown summary section followed by
// the mandatory machine-readable summary line at the very end so the
// §4.2 output contract holds across every format.
func encodeFindingsSummary(pairs []Field, _, _ int) []byte {
	var b bytes.Buffer
	b.WriteString("### Summary\n")
	for _, p := range pairs {
		fmt.Fprintf(&b, "- **%s:** `%s`\n", p.Key, p.Value)
	}
	b.WriteByte('\n')
	b.Write(EncodeLogfmt(pairs))
	return b.Bytes()
}
