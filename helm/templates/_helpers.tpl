{{/*
Common helpers for aiops-engine chart
*/}}

{{- define "aiops-engine.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "aiops-engine.fullname" -}}
{{- printf "%s-%s" .Release.Name "aiops" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "aiops-engine.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 }}
app.kubernetes.io/name: {{ include "aiops-engine.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "aiops-engine.selectorLabels" -}}
app.kubernetes.io/name: {{ include "aiops-engine.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "aiops-engine.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "aiops-engine.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
