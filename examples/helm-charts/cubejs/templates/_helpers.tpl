{{/*
Expand the name of the chart.
*/}}
{{- define "cubejs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "cubejs.fullname" -}}
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
{{- define "cubejs.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cubejs.labels" -}}
helm.sh/chart: {{ include "cubejs.chart" . }}
{{ include "cubejs.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cubejs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cubejs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}


{{/*
Return the appropriate apiVersion for ingress.
*/}}
{{- define "cubejs.ingress.apiVersion" -}}
{{- if semverCompare "<1.14-0" .Capabilities.KubeVersion.Version -}}
{{- print "extensions/v1beta1" -}}
{{- else if semverCompare "<1.19-0" .Capabilities.KubeVersion.Version -}}
{{- print "networking.k8s.io/v1beta1" -}}
{{- else -}}
{{- print "networking.k8s.io/v1" -}}
{{- end -}}
{{- end -}}

{{/*
Return "true" if the API pathType field is supported
*/}}
{{- define "cubejs.ingress.supportsPathType" -}}
{{- if semverCompare "<1.18-0" .Capabilities.KubeVersion.Version -}}
{{- print "false" -}}
{{- else -}}
{{- print "true" -}}
{{- end -}}
{{- end -}}

{{/*
Return "true" if the API ingressClassName field is supported
*/}}
{{- define "cubejs.ingress.supportsIngressClassname" -}}
{{- if semverCompare "<1.18-0" .Capabilities.KubeVersion.Version -}}
{{- print "false" -}}
{{- else -}}
{{- print "true" -}}
{{- end -}}
{{- end -}}

{{/*
Create the name of cubejs master service account to use
*/}}
{{- define "cubejs.master.serviceAccountName" -}}
{{- if .Values.master.serviceAccount.create -}}
  {{ default (printf "%s-master" (include "cubejs.fullname" .)) .Values.master.serviceAccount.name }}
{{- else -}}
  {{ default "default" .Values.master.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Create the name of cubejs workers service account to use
*/}}
{{- define "cubejs.workers.serviceAccountName" -}}
{{- if .Values.workers.serviceAccount.create -}}
  {{ default (printf "%s-workers" (include "cubejs.fullname" .)) .Values.workers.serviceAccount.name }}
{{- else -}}
  {{ default "default" .Values.workers.serviceAccount.name }}
{{- end -}}
{{- end -}}
