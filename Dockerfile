# Runtime-only image: copies the PREBUILT static Linux binaries into a minimal
# base. It carries the two DB-less server-side binaries:
#   /app/converge     — control plane + broker (NO provider code); the default
#                       ENTRYPOINT. control/broker Deployments run it as-is.
#   /app/conctl       — the kubectl-style operator CLI (apply/get/list/delete over
#                       the REST API). Handy to `kubectl exec` and apply CRDs/config.
#
# The DEFAULT worker (stdworker) is a SEPARATE image — [Dockerfile.stdworker] — because
# it hosts the shell-out std* providers and must bundle their tooling (tofu, aws,
# gcloud, az, git, curl, bash, jq). This image deliberately does NOT include it; keep
# the control/broker image small and free of worker CLIs.
#
# Both binaries are built on the host by `just docker` (which cross-compiles for
# linux/amd64 with CGO_ENABLED=0 → bin/*.linux-amd64) BEFORE this `docker build`
# runs. We deliberately do NOT build Go inside the image — `just docker` produces
# the artifacts, this Dockerfile just packages them.
#
# The UI (ui/dist/*) and SQL migrations (db/migrations/*.sql) are embedded into the
# converge binary via go:embed, so no extra files are needed at runtime.
#
# Base: alpine — a tiny (~8 MB) public base with ca-certificates for outbound TLS
# (Postgres/providers). The Go binaries are pure-Go (CGO_ENABLED=0) static builds, so
# they run on alpine's musl userland without a libc dependency.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app

# Prebuilt by `just docker` for linux/amd64 (static, CGO_ENABLED=0).
COPY bin/converge.linux-amd64 /app/converge
COPY bin/conctl.linux-amd64 /app/conctl

# HTTP API (LISTEN_ADDR default :8080). Documentation only.
EXPOSE 8080

# Default entrypoint is the control/broker binary. ROLE/DATABASE_URL/etc. come
# from the Deployment env ("all" = control + broker; pass `migrate up` to run
# migrations only). Workers run the SEPARATE stdworker image (Dockerfile.stdworker).
ENTRYPOINT ["/app/converge"]
