{{/*
Expand the name of the chart.
*/}}
{{- define "rss2msg.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a fully qualified app name (max 63 chars for DNS).
*/}}
{{- define "rss2msg.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version, as used by the chart label.
*/}}
{{- define "rss2msg.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "rss2msg.labels" -}}
helm.sh/chart: {{ include "rss2msg.chart" . }}
{{ include "rss2msg.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "rss2msg.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rss2msg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name to use.
*/}}
{{- define "rss2msg.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "rss2msg.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image reference (tag falls back to the chart appVersion).
*/}}
{{- define "rss2msg.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}

{{/*
Name of the ConfigMap holding config.yaml.
*/}}
{{- define "rss2msg.configMapName" -}}
{{- if .Values.existingConfigMap }}
{{- .Values.existingConfigMap }}
{{- else }}
{{- include "rss2msg.fullname" . }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding env-injected values (empty if none configured).
*/}}
{{- define "rss2msg.secretName" -}}
{{- if .Values.existingSecret }}
{{- .Values.existingSecret }}
{{- else if .Values.secrets }}
{{- include "rss2msg.fullname" . }}
{{- end }}
{{- end }}

{{/*
Default container args per mode unless overridden by .Values.args.
*/}}
{{- define "rss2msg.args" -}}
{{- if .Values.args }}
{{- toYaml .Values.args }}
{{- else if eq .Values.mode "cronjob" }}
{{- toYaml (list "run-once" "--config" "/etc/rss2msg/config.yaml") }}
{{- else }}
{{- toYaml (list "serve" "--config" "/etc/rss2msg/config.yaml") }}
{{- end }}
{{- end }}
