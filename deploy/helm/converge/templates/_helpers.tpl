{{/*
Chart name (respects nameOverride).
*/}}
{{- define "converge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully-qualified app name (respects fullnameOverride / nameOverride).
*/}}
{{- define "converge.fullname" -}}
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

{{- define "converge.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Tier fullname: "<fullname>-<suffix>" but truncated so the WHOLE name (incl. the
suffix) stays within the DNS-1123 63-char limit. Plain "<fullname>-stdworker"
could overflow for a long release name and be silently cut mid-suffix; here we
trunc the base to leave room for "-<suffix>". Call: (dict "ctx" . "suffix" "stdworker").
*/}}
{{- define "converge.tierFullname" -}}
{{- $full := include "converge.fullname" .ctx -}}
{{- $room := sub 63 (add 1 (len .suffix)) | int -}}
{{- printf "%s-%s" (trunc $room $full | trimSuffix "-") .suffix -}}
{{- end -}}

{{/*
Common labels stamped on every object. commonLabels merge UNDER the chart labels
(chart labels win on a key clash) so org/cost labels ride along without breaking
selectors.
*/}}
{{- define "converge.labels" -}}
{{- $chart := dict
  "helm.sh/chart" (include "converge.chart" .)
  "app.kubernetes.io/name" (include "converge.name" .)
  "app.kubernetes.io/instance" .Release.Name
  "app.kubernetes.io/managed-by" .Release.Service -}}
{{- if .Chart.AppVersion }}{{- $_ := set $chart "app.kubernetes.io/version" (.Chart.AppVersion | quote | trimAll "\"") -}}{{- end -}}
{{- toYaml (merge $chart (.Values.commonLabels | default dict)) -}}
{{- end -}}

{{/*
Common annotations for every object (empty → nothing). Call inside a metadata block:
  {{- include "converge.commonAnnotations" . | nindent 4 }}
*/}}
{{- define "converge.commonAnnotations" -}}
{{- with .Values.commonAnnotations }}
{{- toYaml . }}
{{- end }}
{{- end -}}

{{/*
Base selector labels (the app; a component is added per tier).
*/}}
{{- define "converge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "converge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Per-tier selector labels. Call: (dict "ctx" . "component" "control").
The component label is what the deployment.matchLabels / service.selector use to
target one tier — MUST be identical across the tier's Deployment, Service, PDB.
*/}}
{{- define "converge.tierSelectorLabels" -}}
{{ include "converge.selectorLabels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "converge.tierLabels" -}}
{{ include "converge.labels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
stdworker ServiceAccount name. The chart does NOT create the SA — you provision
it (with its IRSA annotation) out of band and name it here.
  serviceAccount.name set   → run as that SA (it must already exist).
  serviceAccount.name empty → the namespace "default" SA (always exists; no identity).
*/}}
{{- define "converge.stdWorkerServiceAccountName" -}}
{{- .Values.serviceAccount.name | default "default" -}}
{{- end -}}

{{/*
Image ref for a tier. Call: (dict "ctx" . "image" .Values.stdWorker.image) — the
per-tier image overrides repo/tag/digest when set, else falls back to the top-level
image. A digest (per-tier or top-level) pins immutably as repo@digest (tag ignored).
*/}}
{{- define "converge.image" -}}
{{- $top := .ctx.Values.image -}}
{{- $repo := $top.repository -}}
{{- $tag := $top.tag | default .ctx.Chart.AppVersion -}}
{{- $digest := $top.digest | default "" -}}
{{- with .image -}}
{{- if .repository }}{{ $repo = .repository }}{{ end -}}
{{- if .tag }}{{ $tag = .tag }}{{ end -}}
{{- if .digest }}{{ $digest = .digest }}{{ end -}}
{{- end -}}
{{- if $digest -}}
{{- printf "%s@%s" $repo $digest -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end -}}

{{/*
envFrom block from the global extraEnvFrom (empty → nothing). Wired into every tier
so a secretRef/configMapRef of shared config flows to all pods.
*/}}
{{- define "converge.envFrom" -}}
{{- with .Values.extraEnvFrom }}
envFrom:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
The three probes for a tier, all pointing at ONE health target. Call:
  (dict "probes" $tier.probes "port" "health")
Emits startupProbe + livenessProbe + readinessProbe (plaintext httpGet on the given
named port). A startupProbe gives slow migration/advisory-lock boot a generous
budget before liveness can SIGKILL; liveness/readiness use the per-tier tunables.
*/}}
{{- define "converge.probes" -}}
{{- $p := .probes -}}
{{- $port := .port -}}
startupProbe:
  httpGet: { path: /livez, port: {{ $port }} }
  failureThreshold: {{ $p.startup.failureThreshold }}
  periodSeconds: {{ $p.startup.periodSeconds }}
  timeoutSeconds: {{ $p.startup.timeoutSeconds }}
livenessProbe:
  httpGet: { path: /livez, port: {{ $port }} }
  initialDelaySeconds: {{ $p.liveness.initialDelaySeconds }}
  periodSeconds: {{ $p.liveness.periodSeconds }}
  timeoutSeconds: {{ $p.liveness.timeoutSeconds }}
  failureThreshold: {{ $p.liveness.failureThreshold }}
readinessProbe:
  httpGet: { path: /readyz, port: {{ $port }} }
  initialDelaySeconds: {{ $p.readiness.initialDelaySeconds }}
  periodSeconds: {{ $p.readiness.periodSeconds }}
  timeoutSeconds: {{ $p.readiness.timeoutSeconds }}
  failureThreshold: {{ $p.readiness.failureThreshold }}
{{- end -}}

{{/*
Merged pod labels / annotations for a tier: global (podLabels/podAnnotations) with
the per-tier map layered ON TOP (tier wins). Call: (dict "ctx" . "tier" $c).
*/}}
{{- define "converge.podLabels" -}}
{{- $merged := merge (.tier.podLabels | default dict) (.ctx.Values.podLabels | default dict) -}}
{{- with $merged }}{{ toYaml . }}{{- end }}
{{- end -}}
{{- define "converge.podAnnotations" -}}
{{- $merged := merge (.tier.podAnnotations | default dict) (.ctx.Values.podAnnotations | default dict) -}}
{{- with $merged }}{{ toYaml . }}{{- end }}
{{- end -}}

{{/*
The name of the Secret holding the writer DSN + optional reader DSN. Either the
user-provided existingSecret, or the chart-created one.
*/}}
{{- define "converge.dbSecretName" -}}
{{- if .Values.database.existingSecret.name -}}
{{- .Values.database.existingSecret.name -}}
{{- else -}}
{{- printf "%s-db" (include "converge.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
DATABASE_URL env entry sourced from the DB secret (writer). Used by control + broker.
*/}}
{{- define "converge.databaseUrlEnv" -}}
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "converge.dbSecretName" . }}
      key: {{ .Values.database.existingSecret.key | default "database-url" }}
{{- end -}}

{{/*
Optional DATABASE_READ_URL env (only when a reader DSN is configured).
*/}}
{{- define "converge.databaseReadUrlEnv" -}}
{{- if or .Values.database.readUrl .Values.database.readExistingSecret.name -}}
- name: DATABASE_READ_URL
  valueFrom:
    secretKeyRef:
      {{- if .Values.database.readExistingSecret.name }}
      name: {{ .Values.database.readExistingSecret.name }}
      key: {{ .Values.database.readExistingSecret.key | default "database-read-url" }}
      {{- else }}
      name: {{ include "converge.dbSecretName" . }}
      key: database-read-url
      {{- end }}
{{- end -}}
{{- end -}}

{{/*
Shared env every control/broker/worker pod gets: logging (+ optional global extraEnv).
*/}}
{{- define "converge.commonEnv" -}}
- name: LOG_LEVEL
  value: {{ .Values.log.level | quote }}
- name: LOG_FORMAT
  value: {{ .Values.log.format | quote }}
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
preStop lifecycle hook (endpoint-removal race close).
*/}}
{{- define "converge.preStop" -}}
lifecycle:
  preStop:
    exec:
      command: ["/bin/sleep", {{ .Values.preStopSleepSeconds | quote }}]
{{- end -}}

{{/*
── TLS/mTLS/SPIFFE ──────────────────────────────────────────────────────────
Server-side TLS_* env for control + broker (the app hot-reloads these paths). The
Secret is mounted at tls.mountPath by converge.tlsVolume/converge.tlsVolumeMount.
Emits nothing when tls.enabled is false. clientCAKey empty → one-way TLS (no
TLS_CLIENT_CA_FILE).

SPIFFE-ID authz is PER-AUDIENCE — each mTLS listener has its own allowlist, so a
worker cert can't reach the API and a peer-broker cert can't pull work. The env a
pod gets depends on its TIER (passed as the "tier" arg): the API listener runs on
control/all pods (tls.apiSpiffeIDs → API_AUTHZ_SPIFFE_IDS); the broker's two
services run on broker/all pods (tls.workerSpiffeIDs → WORKER_AUTHZ_SPIFFE_IDS,
tls.meshSpiffeIDs → MESH_AUTHZ_SPIFFE_IDS). An "all" pod emits all three. Any
allowlist requires mTLS (the app refuses SPIFFE authz without a client CA); the
chart validate below guards that at render time.

Call as: include "converge.tlsServerEnv" (dict "root" . "tier" "control"|"broker").
*/}}
{{- define "converge.tlsServerEnv" -}}
{{- $root := .root -}}
{{- $tier := .tier -}}
{{- with $root }}
{{- if .Values.tls.enabled }}
- name: TLS_CERT_FILE
  value: {{ printf "%s/tls.crt" .Values.tls.mountPath | quote }}
- name: TLS_KEY_FILE
  value: {{ printf "%s/tls.key" .Values.tls.mountPath | quote }}
{{- if .Values.tls.clientCAKey }}
- name: TLS_CLIENT_CA_FILE
  value: {{ printf "%s/%s" .Values.tls.mountPath .Values.tls.clientCAKey | quote }}
{{- end }}
{{- if or (eq $tier "control") (eq $tier "all") }}
{{- if .Values.tls.apiSpiffeIDs }}
- name: API_AUTHZ_SPIFFE_IDS
  value: {{ join "," .Values.tls.apiSpiffeIDs | quote }}
{{- end }}
{{- end }}
{{- if or (eq $tier "broker") (eq $tier "all") }}
{{- if .Values.tls.workerSpiffeIDs }}
- name: WORKER_AUTHZ_SPIFFE_IDS
  value: {{ join "," .Values.tls.workerSpiffeIDs | quote }}
{{- end }}
{{- if .Values.tls.meshSpiffeIDs }}
- name: MESH_AUTHZ_SPIFFE_IDS
  value: {{ join "," .Values.tls.meshSpiffeIDs | quote }}
{{- end }}
{{- end }}
- name: TLS_RELOAD_INTERVAL
  value: {{ .Values.tls.reloadInterval | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
The stdworker's client TLS Secret NAME + mount PATH when it auto-follows server
TLS. Prefers the worker's OWN client Secret (stdWorker.tls.secretName — a
worker-scoped SVID) and falls back to the server Secret for demo convenience. Two
tiny helpers so the env, volume, and mount all agree.
*/}}
{{- define "converge.workerTLSSecret" -}}
{{- .Values.stdWorker.tls.secretName | default .Values.tls.secretName -}}
{{- end -}}
{{- define "converge.workerTLSMount" -}}
{{- if .Values.stdWorker.tls.secretName -}}
{{- .Values.stdWorker.tls.mountPath | default .Values.tls.mountPath -}}
{{- else -}}
{{- .Values.tls.mountPath -}}
{{- end -}}
{{- end -}}

{{/*
Client-side TLS_* env for the stdworker when it AUTO-FOLLOWS server TLS: it
presents its client keypair (client-auth) and pins the broker CA. Emitted only when
tls.enabled. (BROKER_ADDR itself is switched to https:// by the stdworker template.)
*/}}
{{- define "converge.tlsWorkerClientEnv" -}}
{{- if .Values.tls.enabled }}
{{- $mount := include "converge.workerTLSMount" . }}
- name: TLS_CERT_FILE
  value: {{ printf "%s/tls.crt" $mount | quote }}
- name: TLS_KEY_FILE
  value: {{ printf "%s/tls.key" $mount | quote }}
{{- if .Values.tls.clientCAKey }}
- name: BROKER_CA_FILE
  value: {{ printf "%s/%s" $mount .Values.tls.clientCAKey | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
The read-only Secret volume + mount for the SERVER TLS material (control + broker).
Emit nothing when TLS is off. defaultMode 0400 with the pod fsGroup keeps the key
non-world-readable.
*/}}
{{- define "converge.tlsVolume" -}}
{{- if .Values.tls.enabled }}
- name: tls
  secret:
    secretName: {{ .Values.tls.secretName | quote }}
    defaultMode: 0400
{{- end }}
{{- end -}}
{{- define "converge.tlsVolumeMount" -}}
{{- if .Values.tls.enabled }}
- name: tls
  mountPath: {{ .Values.tls.mountPath | quote }}
  readOnly: true
{{- end }}
{{- end -}}

{{/*
The read-only Secret volume + mount for the stdworker's CLIENT TLS material
(its own SVID Secret, or the server Secret as fallback). Emit nothing when TLS off.
*/}}
{{- define "converge.workerTLSVolume" -}}
{{- if .Values.tls.enabled }}
- name: tls
  secret:
    secretName: {{ include "converge.workerTLSSecret" . | quote }}
    defaultMode: 0400
{{- end }}
{{- end -}}
{{- define "converge.workerTLSVolumeMount" -}}
{{- if .Values.tls.enabled }}
- name: tls
  mountPath: {{ include "converge.workerTLSMount" . | quote }}
  readOnly: true
{{- end }}
{{- end -}}

{{/*
The scheme a listener/dial uses given tls.enabled: "https" or "http". Used to build
LISTEN_ADDR / BROKER_ADDR_LISTEN / RELAY_ADVERTISE_ADDR / the worker's BROKER_ADDR,
and to set the probe scheme where a probe hits the TLS port.
*/}}
{{- define "converge.scheme" -}}
{{- if .Values.tls.enabled -}}https{{- else -}}http{{- end -}}
{{- end -}}

{{/*
prometheus.io/* scrape annotations for the pods (control + broker), emitted when
metrics.enabled && metrics.podAnnotations. Scrape targets the plaintext health
port so it never crosses the (m)TLS API/Connect listeners.
*/}}
{{- define "converge.metricsPodAnnotations" -}}
{{- if and .Values.metrics.enabled .Values.metrics.podAnnotations }}
prometheus.io/scrape: "true"
prometheus.io/port: {{ .Values.metrics.port | quote }}
prometheus.io/path: {{ .Values.metrics.path | quote }}
{{- end }}
{{- end -}}

{{/*
Metrics env for control + broker: turns the app's OTel /metrics on (served on the
plaintext health port). Emitted only when metrics.enabled — otherwise the app stays
metrics-off and nothing serves /metrics.
*/}}
{{- define "converge.metricsEnv" -}}
{{- if .Values.metrics.enabled }}
- name: OTEL_METRICS_ENABLED
  value: "true"
{{- end }}
{{- end -}}

{{/*
Render-time guard: the app serves /metrics on its plaintext health listener (:8081),
so metrics.port can't be retargeted without an app change. Fail fast rather than let
the scraper point at a port nothing listens on. Call once from a server tier.
*/}}
{{- define "converge.validateMetrics" -}}
{{- if and .Values.metrics.enabled (ne (int .Values.metrics.port) 8081) }}
{{- fail (printf "metrics.port must be 8081 (the app's fixed health/metrics listener); got %v" .Values.metrics.port) }}
{{- end }}
{{- end -}}

{{/*
checksum/config annotation value — a hash of the DB connection inputs, so an
inline-DSN change rolls control+broker pods (a byte-identical Deployment otherwise
wouldn't). Hashes the database.* VALUES (the Secret's inputs) rather than the
rendered sibling template, so it also works when a single template is rendered in
isolation (helm-unittest). No-op sentinel when an existingSecret is used (that
Secret is managed out of band; document `kubectl rollout restart` for its rotation).
*/}}
{{- define "converge.dbSecretChecksum" -}}
{{- if not .Values.database.existingSecret.name -}}
{{- toYaml .Values.database | sha256sum -}}
{{- else -}}
external
{{- end -}}
{{- end -}}

{{/*
NetworkPolicy egress rules for control/broker (DNS + the DB + any extra peers).
Shared so both tiers get the identical, correct egress allowlist.
*/}}
{{- define "converge.npEgressRules" -}}
{{- $e := .Values.networkPolicy.egress -}}
# DNS (kube-dns) — UDP + TCP on the configured ports.
- ports:
    {{- range $e.dnsPorts }}
    - { port: {{ . }}, protocol: UDP }
    - { port: {{ . }}, protocol: TCP }
    {{- end }}
# The database (Postgres/Aurora writer + reader).
- ports:
    - { port: {{ $e.dbPort }}, protocol: TCP }
{{- with $e.extraTo }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Render-time guards for the TLS block (fail fast with a clear message rather than a
confusing app boot error). Call once from a template.
*/}}
{{- define "converge.validateTLS" -}}
{{- if .Values.tls.enabled }}
{{- if not .Values.tls.secretName }}
{{- fail "tls.enabled=true requires tls.secretName (the out-of-band Secret with tls.crt/tls.key[/ca.crt])" }}
{{- end }}
{{- if and (or .Values.tls.apiSpiffeIDs .Values.tls.workerSpiffeIDs .Values.tls.meshSpiffeIDs) (not .Values.tls.clientCAKey) }}
{{- fail "a SPIFFE authz allowlist (tls.apiSpiffeIDs / tls.workerSpiffeIDs / tls.meshSpiffeIDs) requires mTLS — set tls.clientCAKey to the client-CA key in the Secret" }}
{{- end }}
{{- end }}
{{- end -}}
