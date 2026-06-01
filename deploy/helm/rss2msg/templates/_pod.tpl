{{/*
Container env (extraEnv only; secret keys arrive via envFrom).
*/}}
{{- define "rss2msg.env" -}}
{{- with .Values.extraEnv }}
env:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{/*
envFrom: the rendered/existing Secret (if any) plus any extras.
*/}}
{{- define "rss2msg.envFrom" -}}
{{- $secret := include "rss2msg.secretName" . -}}
{{- if or $secret .Values.extraEnvFrom }}
envFrom:
{{- if $secret }}
  - secretRef:
      name: {{ $secret }}
{{- end }}
{{- with .Values.extraEnvFrom }}
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Container volumeMounts: config (read-only) + optional SQLite persistence + extras.
*/}}
{{- define "rss2msg.volumeMounts" -}}
volumeMounts:
  - name: config
    mountPath: /etc/rss2msg
    readOnly: true
{{- if .Values.persistence.enabled }}
  - name: state
    mountPath: {{ .Values.persistence.mountPath }}
{{- end }}
{{- with .Values.extraVolumeMounts }}
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{/*
Pod volumes: config from ConfigMap + optional SQLite PVC + extras.
*/}}
{{- define "rss2msg.volumes" -}}
volumes:
  - name: config
    configMap:
      name: {{ include "rss2msg.configMapName" . }}
{{- if .Values.persistence.enabled }}
  - name: state
  {{- if .Values.persistence.existingClaim }}
    persistentVolumeClaim:
      claimName: {{ .Values.persistence.existingClaim }}
  {{- else }}
    persistentVolumeClaim:
      claimName: {{ include "rss2msg.fullname" . }}
  {{- end }}
{{- end }}
{{- with .Values.extraVolumes }}
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
