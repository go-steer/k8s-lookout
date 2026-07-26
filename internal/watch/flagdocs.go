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
	"flag"
	"time"
)

// FlagDoc documents one `lookout watch` flag for generated reference
// surfaces (the docs site's sentinel page, dev/tools/gen-site-docs).
type FlagDoc struct {
	Name    string // flag name without dashes
	Type    string // bool | int | float | duration | string | repeatable
	Default string // the FlagSet's default value ("" for none)
	Help    string // the usage string, verbatim
}

// FlagInventory returns every `lookout watch` flag, sorted by name.
// It walks the SAME FlagSet parseFlags parses (newFlagSet), so the
// inventory cannot drift from the real surface: a flag added to
// newFlagSet appears here on the next docs regeneration, and the
// sitedoc drift test fails until it does. The M0-frozen subset is
// separately pinned by TestFlagSurfaceFrozen.
func FlagInventory() []FlagDoc {
	fs, _ := newFlagSet()
	var out []FlagDoc
	fs.VisitAll(func(fl *flag.Flag) {
		out = append(out, FlagDoc{
			Name:    fl.Name,
			Type:    flagTypeName(fl.Value),
			Default: fl.DefValue,
			Help:    fl.Usage,
		})
	})
	return out
}

// flagTypeName maps a flag.Value to a human type label via the
// flag.Getter it implements (every stdlib value and severityFlag do).
func flagTypeName(v flag.Value) string {
	g, ok := v.(flag.Getter)
	if !ok {
		return "string"
	}
	switch g.Get().(type) {
	case bool:
		return "bool"
	case int, int64, uint, uint64:
		return "int"
	case float64:
		return "float"
	case time.Duration:
		return "duration"
	case string:
		return "string"
	default:
		return "repeatable"
	}
}
