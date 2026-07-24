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

package main

import (
	"context"
	"testing"
)

func TestRealMainExitCodes(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want int
	}{
		{"no args is a usage error", nil, 2},
		{"unknown command is a usage error", []string{"nonesuch"}, 2},
		{"help exits zero", []string{"--help"}, 0},
		{"version exits zero", []string{"version"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := realMain(tt.args); got != tt.want {
				t.Errorf("realMain(%v) = %d, want %d", tt.args, got, tt.want)
			}
		})
	}
}

func TestRegisterRejectsDuplicates(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("register of a duplicate name should panic")
		}
	}()
	register(command{name: "version", summary: "dup", run: func(context.Context, []string) int { return 0 }})
}
