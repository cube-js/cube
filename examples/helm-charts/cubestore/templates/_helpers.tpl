{{/*
Expand the name of the chart.
*/}}
{{- define "cubestore.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "cubestore.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "cubestore.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cubestore.labels" -}}
helm.sh/chart: {{ include "cubestore.chart" . }}
{{ include "cubestore.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cubestore.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cubestore.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of cubestore router service account to use
*/}}
{{- define "cubestore.router.serviceAccountName" -}}
{{- if .Values.router.serviceAccount.create -}}
  {{ default (printf "%s-router" (include "cubestore.fullname" .)) .Values.router.serviceAccount.name }}
{{- else -}}
  {{ default "default" .Values.router.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Create the name of cubestore workers service account to use
*/}}
{{- define "cubestore.workers.serviceAccountName" -}}
{{- if .Values.workers.serviceAccount.create -}}
  {{ default (printf "%s-workers" (include "cubestore.fullname" .)) .Values.workers.serviceAccount.name }}
{{- else -}}
  {{ default "default" .Values.workers.serviceAccount.name }}
{{- end -}}
{{- end -}}
