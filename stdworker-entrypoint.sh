#!/usr/bin/env bash
# stdworker-entrypoint.sh — the ENTRYPOINT for the converge stdworker image
# (Dockerfile.stdworker). It points tfenv/tofuenv at a WRITABLE runtime CONFIG_DIR so the
# version managers work under Kubernetes' readOnlyRootFilesystem:true (the Helm worker's
# containerSecurityContext), then execs the worker binary.
#
# Why this is needed: tfenv/tofuenv keep ALL their runtime state — the installed-versions
# dir AND the active-`version` file that `use` writes — under *_CONFIG_DIR (which defaults
# to the read-only *_ROOT). No Terraform/OpenTofu version is baked into the image; run.sh
# installs the version each MODULE needs on first use. That install (and the version file)
# must land somewhere WRITABLE, so we point CONFIG_DIR at a writable dir (the Helm worker's
# /tmp "scratch" emptyDir; falls back to /tmp). The runtime `install` then writes a REAL
# version dir under CONFIG_DIR/versions, which `use`'s `find "$CONFIG_DIR/versions/" -type d`
# resolver finds. (This is why we install dynamically rather than mirror a pre-baked /opt
# version in: `find -type d` has no -L, so a dir-symlink to a read-only pre-bake would be
# SKIPPED — `use` would report "could not be found … pretty much impossible" even though
# `install` said it was present.) Installs are flock-guarded + idempotent, and the /tmp
# emptyDir caches the version across tasks on the same worker, so it installs once per
# (worker, version), not once per task.
set -euo pipefail

# Writable base: the Helm worker mounts an emptyDir at /tmp and sets TMPDIR=/tmp. Honor
# TMPDIR so a differently-mounted scratch path also works; default to /tmp.
writable="${TMPDIR:-/tmp}"

# FAIL LOUDLY if the writable base isn't actually writable. Under
# readOnlyRootFilesystem:true the root FS (incl. a bare /tmp) is read-only, so the
# version managers can't install a version or write their active-version file, and
# stdterraform would fail EVERY task with a confusing mid-run error. This turns that
# latent footgun into an immediate, clear boot failure. The Helm chart provides the
# writable path as an emptyDir mounted at /tmp with TMPDIR=/tmp (stdWorker.scratch);
# disabling scratch under a read-only root FS is the misconfiguration this catches.
if ! ( mkdir -p "$writable" && : > "$writable/.stdworker-writable-probe" ) 2>/dev/null; then
  echo "stdworker-entrypoint: FATAL: writable scratch dir '$writable' is not writable." >&2
  echo "  Under readOnlyRootFilesystem the version managers (tfenv/tofuenv) need a writable" >&2
  echo "  TMPDIR to install Terraform/OpenTofu on demand. In Helm, keep stdWorker.scratch" >&2
  echo "  enabled (it mounts an emptyDir at /tmp and sets TMPDIR); or set TMPDIR to a" >&2
  echo "  writable mount. Refusing to start rather than fail every task later." >&2
  exit 1
fi
rm -f "$writable/.stdworker-writable-probe" 2>/dev/null || true

# point_config_dir <root> <config-dir-var>: put the manager's writable state (installed
# versions + the active-version file) under a writable dir so install-on-demand + `use`
# work under a read-only root FS. The versions dir starts empty; run.sh's runtime install
# populates it with real per-version dirs.
point_config_dir() {
  local root="$1" var="$2" cfg
  [ -d "$root" ] || return 0
  cfg="$writable/$(basename "$root")"          # e.g. /tmp/tfenv, /tmp/tofuenv
  mkdir -p "$cfg/versions"
  export "$var=$cfg"
}

point_config_dir "${TFENV_ROOT:-/opt/tfenv}"   TFENV_CONFIG_DIR
point_config_dir "${TOFUENV_ROOT:-/opt/tofuenv}" TOFUENV_CONFIG_DIR

# run.sh flock-guards its version installs under LOCK_DIR (default /tmp); make it explicit
# so it lands on the writable scratch even if the process's /tmp differs.
export LOCK_DIR="${LOCK_DIR:-$writable}"

exec /usr/local/bin/stdworker "$@"
