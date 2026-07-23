#!/usr/bin/env bash
# run.sh — the whole terraform work/teardown step, run per resource by the Go
# worker (which shells out to it, reading the outputs JSON it writes on apply). The
# Go side embeds this via go:embed and materialises it at Setup, so the worker image
# needs only the terraform/tofu CLI plus the tools for the chosen backend/source on
# PATH (aws, az, gcloud, git, curl) — no Go SDK.
#
# It: (1) fetches the module from $TF_SOURCE by scheme (s3://, gs://, azblob://,
# https://, git::) into an empty work dir, (2) writes a state backend override (per $TF_BACKEND
# with that cloud's NATIVE locking, OR — when $TF_BACKEND=custom — a raw backend.tf the
# Go side already wrote with the per-resource key substituted), (3) `init`, then
# `apply` or `destroy` per $TF_ACTION, and (4) on apply, `output -json` into $OUT_FILE.
#
# The user's MODULE ships no backend block; the engine owns state placement + locking
# (the Terraform-Enterprise model). State is keyed per resource ($TF_STATE_KEY), so
# each resource is its own state + its own lock.
#
# Credentials are NEVER passed here — each backend/provider reads its cloud's standard
# chain (AWS_* / ARM_* / GOOGLE_*) the worker process inherits (env / instance profile
# / workload identity).
#
# The Terraform/OpenTofu VERSION is auto-resolved per module via tfenv (TF_BIN=terraform)
# or tofuenv (TF_BIN=tofu): TF_VERSION pins an explicit token, else a bundle-shipped
# .terraform-version/.opentofu-version is honored, else "latest-allowed" reads the
# module's required_version and picks the newest permitted. The version is installed
# (flock-guarded, idempotent) then activated, and `terraform`/`tofu` on PATH is the
# version-manager shim.
#
# Inputs are ENV (set by the Go worker) — no positional parsing:
#   TF_BIN           terraform | tofu — selects tfenv (terraform) or tofuenv (tofu)
#   TF_VERSION       tfenv/tofuenv token to pin (empty = auto: bundle file, else latest-allowed)
#   TF_ACTION        apply | destroy
#   TF_SOURCE        module location (s3://…tar.gz | gs://…tar.gz | azblob://acct/container/…tar.gz | https://…tar.gz | git::https://repo[//sub][?ref=REF])
#   TF_BACKEND       s3 | azurerm | gcs | custom  (custom = a raw backend.tf is already written)
#   TF_STATE_KEY     state object key/path for THIS resource (always per-resource)
#   TF_VARS          space-separated name=value pairs → -var flags (optional)
#   TF_WORKDIR       empty working dir the script runs in
#   OUT_FILE         path to write `terraform output -json` to (apply only)
# Per-backend (only the selected backend's vars are read):
#   s3:      TF_S3_BUCKET TF_S3_REGION TF_S3_ENDPOINT
#   azurerm: TF_AZ_STORAGE_ACCOUNT TF_AZ_CONTAINER TF_AZ_RESOURCE_GROUP TF_AZ_USE_AZUREAD
#   gcs:     TF_GCS_BUCKET
set -euo pipefail

: "${TF_BIN:?}" "${TF_ACTION:?}" "${TF_SOURCE:?}" "${TF_BACKEND:?}"
: "${TF_STATE_KEY:?}" "${TF_WORKDIR:?}"

export TF_IN_AUTOMATION=1 TF_INPUT=0

# An S3-compatible endpoint (LocalStack/MinIO — TF_S3_ENDPOINT, set when backend=s3
# has one) must reach BOTH the state backend AND the user module's own aws provider.
# The modern aws provider + aws CLI honour AWS_ENDPOINT_URL, so exporting it globally
# points the module's provider at LocalStack too (the demo runs everything against one
# LocalStack). Also pin the region so the provider doesn't probe for one. Harmless
# against real AWS (both unset). This does NOT set credentials — those come from the
# ambient chain the worker inherits.
if [ -n "${TF_S3_ENDPOINT:-}" ]; then
  export AWS_ENDPOINT_URL="$TF_S3_ENDPOINT"
  export AWS_REGION="${TF_S3_REGION:-us-east-1}" AWS_DEFAULT_REGION="${TF_S3_REGION:-us-east-1}"
fi

cd "$TF_WORKDIR"

# ── 1. fetch the module by scheme ────────────────────────────────────────────
# An s3:// SOURCE is fetched with the aws CLI; if TF_S3_ENDPOINT is set (LocalStack/
# MinIO) the CLI is pointed at it + a region. (This is independent of the state
# backend — a module can be fetched from S3 while state lives in gcs, etc.)
fetch_s3() {
  local uri="$1" ep=()
  [ -n "${TF_S3_ENDPOINT:-}" ] && ep=(--endpoint-url "$TF_S3_ENDPOINT")
  [ -n "${TF_S3_REGION:-}" ] && export AWS_REGION="$TF_S3_REGION" AWS_DEFAULT_REGION="$TF_S3_REGION"
  echo "terraform: fetching (s3) $uri"
  # ${ep[@]+"${ep[@]}"} expands to nothing when the array is empty — safe under
  # `set -u` even on older bash (a bare "${ep[@]}" on an empty array errors there).
  aws ${ep[@]+"${ep[@]}"} s3 cp "$uri" bundle.tar.gz
  tar -xzf bundle.tar.gz && rm -f bundle.tar.gz
}

fetch_https() {
  local uri="$1"
  echo "terraform: fetching (https) $uri"
  curl -fsSL "$uri" -o bundle.tar.gz
  tar -xzf bundle.tar.gz && rm -f bundle.tar.gz
}

# A gs:// SOURCE is fetched with `gcloud storage cp` (the current, recommended GCS CLI;
# gsutil is legacy). Auth is the ambient identity — the active gcloud credential /
# attached service account (GKE Workload Identity) / ADC — nothing in the command. For
# a fake-gcs emulator, set CLOUDSDK_API_ENDPOINT_OVERRIDES_STORAGE in the worker env.
fetch_gcs() {
  local uri="$1"
  echo "terraform: fetching (gcs) $uri"
  gcloud storage cp "$uri" bundle.tar.gz
  tar -xzf bundle.tar.gz && rm -f bundle.tar.gz
}

# An azblob://<account>/<container>/<blob-path> SOURCE is fetched with `az storage blob
# download`. We use a distinct azblob:// scheme (not the raw https blob URL) so it never
# collides with a generic https:// tarball fetch. Auth is the ambient identity via
# --auth-mode login (Entra ID: az login / managed identity / AKS workload identity) —
# nothing in the command. For Azurite, set AZURE_STORAGE_CONNECTION_STRING in the worker
# env (e.g. UseDevelopmentStorage=true); the CLI then uses the connection string and we
# drop --auth-mode login (Azurite is Shared-Key only).
fetch_azblob() {
  local rest="${1#azblob://}"
  local account container blob
  account="${rest%%/*}"; rest="${rest#*/}"
  container="${rest%%/*}"; blob="${rest#*/}"
  if [ -z "$account" ] || [ -z "$container" ] || [ "$blob" = "$rest" ] || [ -z "$blob" ]; then
    echo "terraform: azblob source must be azblob://<account>/<container>/<blob-path> — got '$1'" >&2
    exit 2
  fi
  echo "terraform: fetching (azblob) account=$account container=$container blob=$blob"
  if [ -n "${AZURE_STORAGE_CONNECTION_STRING:-}" ]; then
    # Emulator / connection-string path (Azurite): no --auth-mode login.
    az storage blob download --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
      --container-name "$container" --name "$blob" --file bundle.tar.gz --no-progress
  else
    az storage blob download --account-name "$account" \
      --container-name "$container" --name "$blob" --file bundle.tar.gz \
      --auth-mode login --no-progress
  fi
  tar -xzf bundle.tar.gz && rm -f bundle.tar.gz
}

# git::https://host/repo//subdir?ref=REF  → clone repo at REF, use subdir as module.
# The subdir separator is `//` in the PATH (Terraform's convention), which we must
# not confuse with the `//` in the scheme (https://). So we peel the "scheme://"
# prefix off before looking for the subdir `//`, then restore it.
fetch_git() {
  local repo="${1#git::}"
  local subdir="" ref="" scheme=""
  # Split off ?ref=… (a git ref: branch, tag, or commit).
  if [[ "$repo" == *"?"* ]]; then
    local query="${repo#*\?}"; repo="${repo%%\?*}"
    for kv in ${query//&/ }; do
      case "$kv" in ref=*) ref="${kv#ref=}";; esac
    done
  fi
  # Peel a leading "scheme://" so its `//` isn't mistaken for the subdir separator.
  if [[ "$repo" == *"://"* ]]; then
    scheme="${repo%%://*}://"; repo="${repo#*://}"
  fi
  # Now a `//` in what remains is the module-subdir separator (path//subdir).
  if [[ "$repo" == *"//"* ]]; then
    subdir="${repo##*//}"; repo="${repo%//*}"
  fi
  repo="${scheme}${repo}"
  echo "terraform: cloning (git) $repo ref='${ref:-HEAD}' subdir='${subdir:-.}'"
  git clone --quiet --depth 1 ${ref:+--branch "$ref"} "$repo" repo || {
    # --branch fails for a bare commit SHA; fall back to a full clone + checkout.
    rm -rf repo
    git clone --quiet "$repo" repo
    [ -n "$ref" ] && ( cd repo && git checkout --quiet "$ref" )
  }
  # Copy the selected module dir contents into the work dir root so the backend
  # override + terraform run against it. Guard against a subdir escaping the repo.
  local src="repo/${subdir:-.}"
  case "$subdir" in *..*) echo "terraform: subdir must not contain '..'" >&2; exit 2;; esac
  [ -d "$src" ] || { echo "terraform: module subdir '$subdir' not found in repo" >&2; exit 2; }
  shopt -s dotglob
  cp -R "$src"/. .
  shopt -u dotglob
  rm -rf repo
}

case "$TF_SOURCE" in
  s3://*)     fetch_s3     "$TF_SOURCE" ;;
  gs://*)     fetch_gcs    "$TF_SOURCE" ;;
  azblob://*) fetch_azblob "$TF_SOURCE" ;;
  https://*)  fetch_https  "$TF_SOURCE" ;;
  git::*)     fetch_git    "$TF_SOURCE" ;;
  *) echo "terraform: unsupported source scheme: $TF_SOURCE" >&2; exit 2 ;;
esac

# ── 1.5 resolve + install the TF/OpenTofu version for THIS module ─────────────
# Runs AFTER fetch (the module's *.tf must be present so latest-allowed/min-required
# can read required_version) and BEFORE init. TF_BIN selects the version manager:
#   terraform → tfenv   (version file .terraform-version)
#   tofu      → tofuenv (version file .opentofu-version)
# Precedence: TF_VERSION (explicit token) > a version file the bundle already ships >
# "latest-allowed" (the newest version the module's required_version permits; falls
# back to latest when none is declared). We write the chosen token into the version
# file (not the *_TERRAFORM_VERSION env), so `install` and `use` agree (their env-vs-
# file precedence differs). tfenv/tofuenv have NO install lock, so a shared root can
# race under concurrency — flock the install per token (idempotent: the manager skips
# an already-installed version).
case "$TF_BIN" in
  terraform) mgr="tfenv";   verfile=".terraform-version" ;;
  tofu)      mgr="tofuenv"; verfile=".opentofu-version"  ;;
  *) echo "terraform: TF_BIN must be terraform or tofu, got '$TF_BIN'" >&2; exit 2 ;;
esac

if [ -n "${TF_VERSION:-}" ]; then
  # Explicit spec pin wins — overwrite any bundle-shipped file.
  printf '%s\n' "$TF_VERSION" > "$verfile"
  echo "terraform: version = $TF_VERSION (spec.tf_version)"
elif [ -f "$verfile" ]; then
  echo "terraform: version = $(cat "$verfile") (from bundle $verfile)"
else
  # Auto: if the module declares a required_version, pick the newest version it
  # allows (latest-allowed); otherwise there's nothing to constrain to, so use latest.
  # (tfenv/tofuenv's latest-allowed ERRORS on an absent/empty constraint rather than
  # falling back, so we must choose the token ourselves by scanning the module's *.tf.)
  # Case-SENSITIVE (HCL's key is lowercase) and anchored to the start of a line (after
  # optional indent) so a commented-out `# required_version = …` or a doc mention in a
  # string does NOT count as a real declaration.
  if grep -rEq '^[[:space:]]*required_version[[:space:]]*=' --include='*.tf' .; then
    printf 'latest-allowed\n' > "$verfile"
    echo "terraform: version = latest-allowed (auto — newest the module's required_version allows)"
  else
    printf 'latest\n' > "$verfile"
    echo "terraform: version = latest (auto — module declares no required_version)"
  fi
fi

# Install (idempotent) + activate the resolved token. tfenv/tofuenv have NO install
# lock, so concurrent tasks installing the SAME version into a shared root can corrupt
# it — serialize the install per token with flock WHEN AVAILABLE (util-linux; present
# in the Linux worker image). Where flock is absent (e.g. a macOS dev/test host) fall
# back to a plain install: a single run is safe, and the image path keeps the lock.
# The token is passed explicitly to both install + use so they never diverge (their
# env-vs-file precedence differs). LOCK_DIR is a shared writable dir (default /tmp).
token="$(cat "$verfile")"
echo "terraform: $mgr install $token"
# Lock on the RESOLVED concrete version, not the raw token, so two tasks that reach the
# SAME version via DIFFERENT tokens (e.g. one pins "1.10.6" while another resolves
# "latest-allowed" → 1.10.6) still serialize their install into the shared root. tfenv/
# tofuenv `resolve-version` maps the token/constraint to a concrete version WITHOUT
# installing; fall back to the raw token if it can't (older manager, or a token like
# "latest" it won't resolve offline) — the lock is then merely coarser, never wrong.
lockkey="$("$mgr" resolve-version "$token" 2>/dev/null || true)"
[ -n "$lockkey" ] || lockkey="$token"
if command -v flock >/dev/null 2>&1; then
  lock="${LOCK_DIR:-/tmp}/${mgr}-$(printf '%s' "$lockkey" | tr -c 'A-Za-z0-9._-' '_').lock"
  flock "$lock" "$mgr" install "$token"
else
  "$mgr" install "$token"
fi
"$mgr" use "$token"

# ── 2. backend override — the engine owns state placement + locking ──────────
# The module ships no backend block; we inject one keyed per resource ($TF_STATE_KEY)
# with the chosen cloud's NATIVE state locking. Credentials come from each cloud's env
# chain (never written here). When TF_BACKEND=custom the Go side already wrote a raw
# zz_backend_override.tf (with the per-resource key substituted) — leave it as-is.
case "$TF_BACKEND" in
  custom)
    [ -f zz_backend_override.tf ] || { echo "terraform: TF_BACKEND=custom but no backend override was written" >&2; exit 2; }
    echo "terraform: using custom backend override (raw HCL from the providerconfig bundle)"
    ;;
  s3)
    # Native S3 locking: use_lockfile=true writes a .tflock object beside the state
    # (no DynamoDB table; needs terraform/tofu >= 1.10). skip_*/use_path_style + the
    # endpoints block let it talk to LocalStack/MinIO; harmless against real AWS.
    : "${TF_S3_BUCKET:?terraform: backend=s3 needs s3.bucket}"
    cat >zz_backend_override.tf <<EOF
terraform {
  backend "s3" {
    bucket       = "${TF_S3_BUCKET}"
    key          = "${TF_STATE_KEY}"
    region       = "${TF_S3_REGION:-us-east-1}"
    use_lockfile = true
$( [ -n "${TF_S3_ENDPOINT:-}" ] && cat <<EP
    endpoints                   = { s3 = "${TF_S3_ENDPOINT}" }
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    use_path_style              = true
EP
)
  }
}
EOF
    ;;
  azurerm)
    # Azure Blob: locking is AUTOMATIC via blob leases — nothing to configure. Auth
    # comes from the ARM_* / workload-identity chain; use_azuread_auth reaches the
    # state blob with the worker's identity instead of a storage account key.
    : "${TF_AZ_STORAGE_ACCOUNT:?terraform: backend=azurerm needs azurerm.storage_account}"
    : "${TF_AZ_CONTAINER:?terraform: backend=azurerm needs azurerm.container}"
    cat >zz_backend_override.tf <<EOF
terraform {
  backend "azurerm" {
    storage_account_name = "${TF_AZ_STORAGE_ACCOUNT}"
    container_name       = "${TF_AZ_CONTAINER}"
    key                  = "${TF_STATE_KEY}"
$( [ -n "${TF_AZ_RESOURCE_GROUP:-}" ] && printf '    resource_group_name  = "%s"\n' "$TF_AZ_RESOURCE_GROUP" )
$( [ -n "${TF_AZ_USE_AZUREAD:-}" ] && printf '    use_azuread_auth     = true\n' )
  }
}
EOF
    ;;
  gcs)
    # GCS: locking is AUTOMATIC via an atomic lock object — nothing to configure. Auth
    # comes from Application Default Credentials (GOOGLE_* env / GKE Workload Identity).
    # gcs uses `prefix` (not `key`); the state object becomes <prefix>/<workspace>.tfstate.
    : "${TF_GCS_BUCKET:?terraform: backend=gcs needs gcs.bucket}"
    cat >zz_backend_override.tf <<EOF
terraform {
  backend "gcs" {
    bucket = "${TF_GCS_BUCKET}"
    prefix = "${TF_STATE_KEY}"
  }
}
EOF
    ;;
  *)
    echo "terraform: unknown backend '$TF_BACKEND' (want s3, azurerm, gcs, or custom)" >&2
    exit 2
    ;;
esac

# ── 3. build -var flags from TF_VARS ("k1=v1 k2=v2"); word-splitting intentional ─
var_flags=()
for kv in ${TF_VARS:-}; do
  var_flags+=(-var "$kv")
done

# ── 4. init + apply|destroy (+ output on apply) ──────────────────────────────
# ${var_flags[@]+"${var_flags[@]}"} expands to the flags when the array is non-empty
# and to NOTHING when empty — safe under `set -u` even on older bash (macOS 3.2),
# where a bare "${var_flags[@]}" on an empty array is an "unbound variable" error.
echo "terraform: $TF_BIN init"
"$TF_BIN" init -input=false -no-color
case "$TF_ACTION" in
  apply)
    echo "terraform: $TF_BIN apply"
    "$TF_BIN" apply -input=false -no-color -auto-approve ${var_flags[@]+"${var_flags[@]}"}
    echo "terraform: $TF_BIN output"
    "$TF_BIN" output -json -no-color >"$OUT_FILE"
    ;;
  destroy)
    echo "terraform: $TF_BIN destroy"
    "$TF_BIN" destroy -input=false -no-color -auto-approve ${var_flags[@]+"${var_flags[@]}"}
    ;;
  *) echo "terraform: unknown action: $TF_ACTION" >&2; exit 2 ;;
esac
echo "terraform: done ($TF_ACTION)"
