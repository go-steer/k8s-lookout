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
Validated operating mode: "watch" (default) or "mcp".
*/}}
{{- define "lookout.mode" -}}
{{- $mode := default "watch" .Values.mode -}}
{{- if and (ne $mode "watch") (ne $mode "mcp") -}}
{{- fail (printf "mode must be watch or mcp (got %q)" $mode) -}}
{{- end -}}
{{- $mode -}}
{{- end -}}

{{- define "lookout.mcpMode" -}}
{{- if eq (include "lookout.mode" .) "mcp" -}}
true
{{- end -}}
{{- end -}}

{{/*
Validated sink.type for watch mode: "core-agent" (default), "webhook", or "ax".
*/}}
{{- define "lookout.sinkType" -}}
{{- $sink := default "core-agent" .Values.sink.type -}}
{{- if and (ne $sink "core-agent") (ne $sink "webhook") (ne $sink "ax") -}}
{{- fail (printf "sink.type must be core-agent, webhook or ax (got %q)" $sink) -}}
{{- end -}}
{{- $sink -}}
{{- end -}}

{{- define "lookout.component" -}}
{{- if include "lookout.mcpMode" . -}}
mcp
{{- else -}}
watcher
{{- end -}}
{{- end -}}

{{/*
The standard fullname helper: `helm install lookout-watch` resolves to
`lookout-watch` and `helm install lookout-mcp --set mode=mcp` resolves to
`lookout-mcp`, matching the raw Kustomize object names so
dev/tools/verify-helm-parity can diff them directly.
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
deliberately the two the shipped manifests use and nothing else.
*/}}
{{- define "lookout.selectorLabels" -}}
app.kubernetes.io/name: {{ include "lookout.fullname" . }}
app.kubernetes.io/component: {{ include "lookout.component" . }}
{{- end -}}

{{/*
Helm provenance labels only (for objects whose raw Kustomize source carries
no app.kubernetes.io/name or component labels).
*/}}
{{- define "lookout.provenanceLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Object labels: the selector labels plus Helm's provenance.
*/}}
{{- define "lookout.labels" -}}
{{ include "lookout.selectorLabels" . }}
{{ include "lookout.provenanceLabels" . }}
{{- end -}}

{{- define "lookout.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "lookout.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Name of the AX task template ConfigMap when sink.type=ax.
*/}}
{{- define "lookout.axTaskConfigMapName" -}}
{{- if .Values.sink.ax.existingTaskConfigMap -}}
{{- .Values.sink.ax.existingTaskConfigMap -}}
{{- else -}}
{{- printf "%s-ax-task-%s" (trimSuffix "-watch" (include "lookout.fullname" .)) (sha256sum .Values.sink.ax.taskTemplate | trunc 10) -}}
{{- end -}}
{{- end -}}

{{/*
The image reference. `image.tag` wins; otherwise appVersion, with
`image.flavor` (or `mcp.imageFlavor` in mcp mode) appended as a suffix.
*/}}
{{- define "lookout.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- $flavor := .Values.image.flavor -}}
{{- if and (not $flavor) (include "lookout.mcpMode" .) -}}
{{- $flavor = .Values.mcp.imageFlavor -}}
{{- end -}}
{{- if and $flavor (not .Values.image.tag) -}}
{{- $tag = printf "%s-%s" $tag $flavor -}}
{{- end -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
"true" when rbac.scope is namespace (#407), "" for cluster; anything else
fails the render rather than silently choosing one.
*/}}
{{- define "lookout.namespaceScope" -}}
{{- $scope := default "cluster" .Values.rbac.scope -}}
{{- if eq $scope "namespace" -}}
true
{{- else if ne $scope "cluster" -}}
{{- fail (printf "rbac.scope must be cluster or namespace (got %q)" $scope) -}}
{{- end -}}
{{- end -}}
