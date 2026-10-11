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
	"context"

	"github.com/go-steer/k8s-lookout/pkg/emit"
	"github.com/go-steer/k8s-lookout/pkg/inject"
)

// §6.5 on the inject surface (issue #82, #635): pkg/inject and pkg/emit
// are both zero-intra-repo-dep leaves, so inject payloads are sanitized
// both at assembly time (through maskString/maskLabels) AND enforced
// centrally at the outbound Sink boundary via MaskingSink across both
// OpenIncident / OpenIncidentKeyed and Append.

// maskString applies the §6.5 value-shape heuristics (URL passwords,
// JWTs, auth headers, vendor tokens, credential flags, …) to one
// cluster-sourced string. Innocent text passes through byte-identical,
// so the frozen wire pins are unaffected.
func maskString(s string) string { return emit.MaskString(s) }

// maskLabels returns labels with every VALUE masked using key-aware
// credential matching plus value-shape heuristics; keys are preserved
// (never secret-shaped by k8s qualified-name validation).
// nil stays nil for omitempty parity.
func maskLabels(labels map[string]string) map[string]string {
	return emit.MaskMap(labels)
}

// MaskPayload sanitizes free text, labels, and annotations across all
// outbound inject payload shapes before serialization onto the wire.
func MaskPayload(payload any) any {
	switch p := payload.(type) {
	case inject.Payload:
		p.Message = emit.MaskString(p.Message)
		p.PullCause = emit.MaskString(p.PullCause)
		p.Context.Labels = emit.MaskMap(p.Context.Labels)
		if p.Enrichment != nil {
			b := emit.MaskString(p.Enrichment.Bundle)
			p.Enrichment = &inject.PayloadEnrichment{Bundle: b}
		}
		if p.QuotaIncreaseDraft != nil {
			d := *p.QuotaIncreaseDraft
			d.Justification = emit.MaskString(d.Justification)
			p.QuotaIncreaseDraft = &d
		}
		return p
	case inject.ResolvedPayload:
		p.Context.Labels = emit.MaskMap(p.Context.Labels)
		return p
	case inject.StormPayload:
		p.Message = emit.MaskString(p.Message)
		p.Context.Labels = emit.MaskMap(p.Context.Labels)
		if p.Enrichment != nil {
			b := emit.MaskString(p.Enrichment.Bundle)
			p.Enrichment = &inject.PayloadEnrichment{Bundle: b}
		}
		return p
	case inject.StormMemberPayload:
		p.Message = emit.MaskString(p.Message)
		return p
	case inject.StormUpdatePayload:
		p.Message = emit.MaskString(p.Message)
		return p
	case inject.TriageRegressedPayload:
		p.Message = emit.MaskString(p.Message)
		p.Context.Labels = emit.MaskMap(p.Context.Labels)
		return p
	case inject.FamilyMemberPayload:
		p.Message = emit.MaskString(p.Message)
		return p
	case map[string]string:
		return emit.MaskMap(p)
	case map[string]any:
		return maskAnyMap(p)
	default:
		return payload
	}
}

func maskAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		switch tv := v.(type) {
		case string:
			out[k] = emit.MaskKeyed(k, tv)
		case map[string]string:
			out[k] = emit.MaskMap(tv)
		case map[string]any:
			out[k] = maskAnyMap(tv)
		case []any:
			sliceOut := make([]any, len(tv))
			for i, elem := range tv {
				sliceOut[i] = MaskPayload(elem)
			}
			out[k] = sliceOut
		default:
			out[k] = v
		}
	}
	return out
}

type sinkUnwrapper interface {
	Unwrap() inject.Sink
}

func unwrapSink(s inject.Sink) inject.Sink {
	if u, ok := s.(sinkUnwrapper); ok {
		return u.Unwrap()
	}
	return s
}

// MaskingSink wraps an outbound inject.Sink so every payload delivered via
// OpenIncident, OpenIncidentKeyed, or Append is sanitized with MaskPayload
// before reaching the underlying sink. The wrapper preserves the exact
// capability tier (Sink, SessionOpener, KeyedOpener) of inner.
func MaskingSink(inner inject.Sink) inject.Sink {
	if inner == nil {
		return nil
	}
	if _, ok := inner.(sinkUnwrapper); ok {
		return inner
	}
	so, isOpener := inner.(inject.SessionOpener)
	ko, isKeyed := inner.(inject.KeyedOpener)
	switch {
	case isKeyed && isOpener:
		return &maskingKeyedSink{maskingOpenerSink: maskingOpenerSink{maskingSink: maskingSink{inner: inner}, opener: so}, keyed: ko}
	case isOpener:
		return &maskingOpenerSink{maskingSink: maskingSink{inner: inner}, opener: so}
	default:
		return &maskingSink{inner: inner}
	}
}

type maskingSink struct {
	inner inject.Sink
}

func (m *maskingSink) Unwrap() inject.Sink { return m.inner }

func (m *maskingSink) OpenIncident(ctx context.Context, payload any) (string, error) {
	return m.inner.OpenIncident(ctx, MaskPayload(payload))
}

func (m *maskingSink) Append(ctx context.Context, id string, payload any) error {
	return m.inner.Append(ctx, id, MaskPayload(payload))
}

type maskingOpenerSink struct {
	maskingSink
	opener inject.SessionOpener
}

func (m *maskingOpenerSink) CreateSession(ctx context.Context) (string, error) {
	return m.opener.CreateSession(ctx)
}

type maskingKeyedSink struct {
	maskingOpenerSink
	keyed inject.KeyedOpener
}

func (m *maskingKeyedSink) OpenIncidentKeyed(ctx context.Context, key inject.IncidentKey, payload any) (string, error) {
	return m.keyed.OpenIncidentKeyed(ctx, key, MaskPayload(payload))
}

func (m *maskingKeyedSink) CreateSessionKeyed(ctx context.Context, key inject.IncidentKey) (string, error) {
	return m.keyed.CreateSessionKeyed(ctx, key)
}

var (
	_ inject.Sink          = (*maskingSink)(nil)
	_ inject.Sink          = (*maskingOpenerSink)(nil)
	_ inject.SessionOpener = (*maskingOpenerSink)(nil)
	_ inject.Sink          = (*maskingKeyedSink)(nil)
	_ inject.SessionOpener = (*maskingKeyedSink)(nil)
	_ inject.KeyedOpener   = (*maskingKeyedSink)(nil)
)
