{{/* Names, labels and images: one definition each, reused everywhere. */}}

{{- define "nebula.labels" -}}
app.kubernetes.io/part-of: nebula
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "nebula.selector" -}}
app.kubernetes.io/name: {{ . }}
{{- end -}}

{{/* image: (dict "root" $ "name" "gateway") → registry/nebula/gateway:tag */}}
{{- define "nebula.image" -}}
{{- $v := .root.Values.images -}}
{{- $tag := required "images.tag is required: images are always pinned, never latest" $v.tag -}}
{{- if eq $tag "latest" }}{{ fail "images.tag must not be latest" }}{{ end -}}
{{- $repo := index $v .name -}}
{{- if $v.registry -}}{{ printf "%s/%s:%s" $v.registry $repo $tag }}{{- else -}}{{ printf "%s:%s" $repo $tag }}{{- end -}}
{{- end -}}

{{/* The hardening every NEBULA container carries (docs/deployment-architecture.md §2.1). */}}
{{- define "nebula.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
capabilities:
  drop: ["ALL"]
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "nebula.podSecurity" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "nebula.probes" -}}
startupProbe:
  httpGet: {path: /readyz, port: http}
  periodSeconds: 2
  timeoutSeconds: 3
  failureThreshold: 60
readinessProbe:
  httpGet: {path: /readyz, port: http}
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3
livenessProbe:
  httpGet: {path: /livez, port: http}
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 3
{{- end -}}

{{- define "nebula.commonEnv" -}}
- name: NEBULA_ENV
  value: {{ .Values.env | quote }}
- name: NEBULA_LOG_FORMAT
  value: json
# /metrics on its own port, never the public one (docs/observability.md §1).
- name: NEBULA_METRICS_ADDR
  value: ":{{ .Values.telemetry.metricsPort }}"
{{- with include "nebula.otlpEndpoint" . }}
- name: NEBULA_OTLP_ENDPOINT
  value: {{ . | quote }}
{{- end }}
- name: NEBULA_TRACE_SAMPLE_RATIO
  value: {{ .Values.telemetry.traceSampleRatio | quote }}
{{- end -}}

{{/* Where spans go: the configured collector, else the in-cluster one when the
observability stack is installed, else nowhere. */}}
{{- define "nebula.otlpEndpoint" -}}
{{- if .Values.telemetry.otlpEndpoint -}}
{{ .Values.telemetry.otlpEndpoint }}
{{- else if .Values.observability.enabled -}}
otel-collector.{{ .Values.observability.namespace }}:4318
{{- end -}}
{{- end -}}

{{/* Pod annotations Prometheus discovers NEBULA's services by. */}}
{{- define "nebula.scrapeAnnotations" -}}
prometheus.io/scrape: "true"
prometheus.io/port: {{ .Values.telemetry.metricsPort | quote }}
prometheus.io/path: /metrics
{{- end -}}

{{/* The NATS URL services and workers use: the configured one, else the in-cluster
development server, else none (no heartbeats). */}}
{{- define "nebula.natsURL" -}}
{{- if .Values.nats.url -}}
{{ .Values.nats.url }}
{{- else if .Values.data.enabled -}}
nats://nats.{{ .Values.namespaces.data }}:4222
{{- end -}}
{{- end -}}
