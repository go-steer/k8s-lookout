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
	"flag"
	"io"
	"testing"
	"time"
)

func TestParseWorkload(t *testing.T) {
	tests := []struct {
		in      string
		want    WorkloadRef
		wantErr bool
	}{
		{"", WorkloadRef{}, false},
		{"Deployment/prod/api", WorkloadRef{"Deployment", "prod", "api"}, false},
		{"Deployment/api", WorkloadRef{}, true},
		{"Deployment/prod/api/extra", WorkloadRef{}, true},
		{"Deployment//api", WorkloadRef{}, true},
		{"/prod/api", WorkloadRef{}, true},
	}
	for _, tt := range tests {
		got, err := ParseWorkload(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseWorkload(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseWorkload(%q) = %+v, want %+v", tt.in, got, tt.want)
		}
	}
	if s := (WorkloadRef{"Deployment", "prod", "api"}).String(); s != "Deployment/prod/api" {
		t.Errorf("String() = %q", s)
	}
	if !(WorkloadRef{}).IsZero() {
		t.Error("zero WorkloadRef should report IsZero")
	}
}

func TestValidateSpecs(t *testing.T) {
	tests := []struct {
		name    string
		specs   []FlagSpec
		wantErr bool
	}{
		{"empty is fine", nil, false},
		{"typed flags with defaults", []FlagSpec{
			{Name: "limit", Type: FlagInt, Default: "5", Help: "h"},
			{Name: "window", Type: FlagDuration, Default: "90m", Help: "h"},
			{Name: "verbose", Type: FlagBool, Default: "", Help: "h"},
		}, false},
		{"shadowing a common flag", []FlagSpec{
			{Name: "timeout", Type: FlagDuration, Default: "1s", Help: "h"},
		}, true},
		{"duplicate name", []FlagSpec{
			{Name: "limit", Type: FlagInt, Default: "1", Help: "h"},
			{Name: "limit", Type: FlagInt, Default: "2", Help: "h"},
		}, true},
		{"unparseable default", []FlagSpec{
			{Name: "limit", Type: FlagInt, Default: "many", Help: "h"},
		}, true},
		{"unknown type", []FlagSpec{
			{Name: "limit", Type: FlagType("float"), Default: "", Help: "h"},
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateSpecs(tt.specs); (err != nil) != tt.wantErr {
				t.Errorf("ValidateSpecs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFlagValuesTypedAccess(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	specs := []FlagSpec{
		{Name: "s", Type: FlagString, Default: "dft", Help: "h"},
		{Name: "b", Type: FlagBool, Default: "false", Help: "h"},
		{Name: "i", Type: FlagInt, Default: "7", Help: "h"},
		{Name: "d", Type: FlagDuration, Default: "3s", Help: "h"},
	}
	if err := registerSpecs(fs, specs); err != nil {
		t.Fatal(err)
	}
	if err := fs.Parse([]string{"-b", "-i=9"}); err != nil {
		t.Fatal(err)
	}
	v := FlagValues{fs: fs}
	if got := v.String("s"); got != "dft" {
		t.Errorf("String(s) = %q", got)
	}
	if !v.Bool("b") {
		t.Error("Bool(b) = false, want true")
	}
	if got := v.Int("i"); got != 9 {
		t.Errorf("Int(i) = %d", got)
	}
	if got := v.Duration("d"); got != 3*time.Second {
		t.Errorf("Duration(d) = %s", got)
	}

	defer func() {
		if recover() == nil {
			t.Error("access to an undeclared flag should panic")
		}
	}()
	v.String("nonesuch")
}
