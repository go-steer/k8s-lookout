{{/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{- define "lookout.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
The standard fullname helper, which matters here for a specific reason:
the documented install is `helm install lookout-watch`, and because
that release name already contains the chart name, fullname resolves to
`lookout-watch` — the exact resource names in deploy/*.yaml. That is
what lets dev/tools/verify-helm-parity diff the two renders instead of
comparing them by eye.

Two of the resources this names are CLUSTER-scoped (the ClusterRole and
its binding), so a second release in the same cluster under a different
name gets its own; a second release under the SAME name would collide,
which is the ordinary Helm behaviour and the ordinary Helm error.
*/}}
{{- define "lookout.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Selector labels. Immutable on a Deployment once applied, so these are
deliberately the two the shipped manifests use and nothing else — a
chart that folds `app.kubernetes.io/instance` in here cannot be renamed
and cannot be migrated onto from the raw manifests without deleting the
Deployment first.
*/}}
{{- define "lookout.selectorLabels" -}}
app.kubernetes.io/name: {{ include "lookout.fullname" . }}
app.kubernetes.io/component: watcher
{{- end -}}

{{/*
Object labels: the selector labels plus Helm's provenance. The extra
keys are metadata only and never reach a selector.
*/}}
{{- define "lookout.labels" -}}
{{ include "lookout.selectorLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "lookout.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "lookout.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
The image reference. `image.tag` wins; otherwise appVersion, with
`image.flavor` appended as a suffix so switching to the GKE build is
one value rather than a version string the operator has to keep in step
with the chart.
*/}}
{{- define "lookout.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if and .Values.image.flavor (not .Values.image.tag) -}}
{{- $tag = printf "%s-%s" $tag .Values.image.flavor -}}
{{- end -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
