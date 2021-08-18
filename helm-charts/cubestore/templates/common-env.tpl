{{- define "cubestore.common-env" -}}
{{- if .Values.global.logLevel }}
- name: CUBESTORE_LOG_LEVEL
  value: {{ .Values.global.logLevel | quote }}
{{- end}}
{{- if .Values.global.noUpload }}
- name: CUBESTORE_NO_UPLOAD
  value: {{ .Values.global.noUpload | quote }}
{{- end}}
{{- if .Values.global.jobRunners }}
- name: CUBESTORE_JOB_RUNNERS
  value: {{ .Values.global.jobRunners | quote }}
{{- end}}
{{- if .Values.global.queryTimeout }}
- name: CUBESTORE_QUERY_TIMEOUT
  value: {{ .Values.global.queryTimeout | quote }}
{{- end}}
{{- if .Values.global.walSplitThreshold }}
- name: CUBESTORE_WAL_SPLIT_THRESHOLD
  value: {{ .Values.global.walSplitThreshold | quote }}
{{- end}}
{{- if .Values.cloudStorage.gcp.bucket }}
- name: CUBESTORE_GCS_BUCKET
  value: {{ .Values.cloudStorage.gcp.bucket | quote }}
{{- end}}
{{- if .Values.cloudStorage.gcp.subPath }}
- name: CUBESTORE_GCS_SUB_PATH
  value: {{ .Values.cloudStorage.gcp.subPath | quote }}
{{- end}}
{{- if .Values.cloudStorage.gcp.credentials }}
- name: CUBESTORE_GCP_CREDENTIALS
  value: {{ .Values.cloudStorage.gcp.credentials | quote }}
{{- else if .Values.cloudStorage.gcp.credentialsFromSecret }}
- name: CUBESTORE_GCP_CREDENTIALS
  valueFrom:
    secretKeyRef:
      name: {{ .Values.cloudStorage.gcp.credentialsFromSecret.name | required "cloudStorage.gcp.credentialsFromSecret.name is required" }}
      key: {{ .Values.cloudStorage.gcp.credentialsFromSecret.key | required "cloudStorage.gcp.credentialsFromSecret.key is required" }}
{{- end}}
{{- if .Values.cloudStorage.aws.accessKeyID }}
- name: CUBESTORE_AWS_ACCESS_KEY_ID
  value: {{ .Values.cloudStorage.aws.accessKeyID | quote }}
{{- end}}
{{- if .Values.cloudStorage.aws.secretKey }}
- name: CUBESTORE_AWS_SECRET_ACCESS_KEY
  value: {{ .Values.cloudStorage.aws.secretKey | quote }}
{{- else if .Values.cloudStorage.aws.secretKeyFromSecret }}
- name: CUBESTORE_AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.cloudStorage.aws.secretKeyFromSecret.name | required "cloudStorage.aws.secretKeyFromSecret.name is required" }}
      key: {{ .Values.cloudStorage.aws.secretKeyFromSecret.key | required "cloudStorage.aws.secretKeyFromSecret.key is required" }}
{{- end}}
{{- if .Values.cloudStorage.aws.bucket }}
- name: CUBESTORE_S3_BUCKET
  value: {{ .Values.cloudStorage.aws.bucket | quote }}
{{- end}}
{{- if .Values.cloudStorage.aws.region }}
- name: CUBESTORE_S3_REGION
  value: {{ .Values.cloudStorage.aws.region | quote }}
{{- end}}
{{- if .Values.cloudStorage.aws.subPath }}
- name: CUBESTORE_S3_SUB_PATH
  value: {{ .Values.cloudStorage.aws.subPath | quote }}
{{- end}}
{{- end}}