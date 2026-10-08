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
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"

	"github.com/go-steer/k8s-lookout/internal/axapi"
	"github.com/go-steer/k8s-lookout/internal/axsink"
)

// newAXSink builds the ax sink from flags (docs/ax-sink-design.md). The AX
// API is plaintext gRPC inside the cluster, like the AX CLI's own tunnel.
func newAXSink(f *flags, token string) (*axsink.Sink, error) {
	tmpl, err := loadAXTaskTemplate(f.axTaskTemplate)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(f.axServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connecting to AX at %s: %w", f.axServer, err)
	}
	return axsink.New(axsink.Config{
		Client:         axapi.NewAXClient(conn),
		Template:       tmpl,
		RouterURL:      f.axRouterURL,
		BearerToken:    token,
		AssertedCaller: f.owner,
		Scope:          axsink.Scope(f.axTaskScope),
	})
}

// loadAXTaskTemplate reads an AX Task manifest. Unknown fields are errors, so
// a typo in the template fails at startup rather than on the first incident.
func loadAXTaskTemplate(path string) (*axapi.Task, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading AX task template: %w", err)
	}
	js, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("AX task template %s: %w", path, err)
	}
	var t axapi.Task
	if err := protojson.Unmarshal(js, &t); err != nil {
		return nil, fmt.Errorf("AX task template %s: %w", path, err)
	}
	if t.GetKind() != "" && t.GetKind() != "Task" {
		return nil, fmt.Errorf("AX task template %s: kind is %q, want Task", path, t.GetKind())
	}
	if t.GetSpec() == nil {
		return nil, fmt.Errorf("AX task template %s: spec is required", path)
	}
	return &t, nil
}
