#!/bin/sh
# install.sh — download and install the Converge binaries (converge, conctl,
# stdworker) from a GitHub Release.
#
#   curl -fsSL https://raw.githubusercontent.com/salesforce/converge/main/install.sh | sh
#
# It detects your OS/arch, downloads the matching per-platform tarball from the
# release, verifies its SHA-256 against the release's SHA256SUMS, and installs the
# binaries to a bin dir on your PATH. POSIX sh (no bashisms) so it runs anywhere.
#
# Environment overrides:
#   CONVERGE_VERSION   release tag to install (default: latest)   e.g. v0.1.0
#   CONVERGE_BIN_DIR   install dir (default: /usr/local/bin, or ~/.local/bin if
#                      /usr/local/bin isn't writable)
#   CONVERGE_BINARIES  space-separated subset to install (default: all three)
#                      e.g. CONVERGE_BINARIES="conctl" to install only the CLI
#
# The release assets are public — no auth needed. (If you host a private mirror,
# set GITHUB_TOKEN and this script sends it as a Bearer token on every request.)
set -eu

REPO="salesforce/converge"
VERSION="${CONVERGE_VERSION:-latest}"
BINARIES="${CONVERGE_BINARIES:-converge conctl stdworker}"

# ── detect platform ─────────────────────────────────────────────────────────
os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
  Linux)  os="linux" ;;
  Darwin) os="darwin" ;;
  *) echo "unsupported OS: $os (this release ships linux and darwin only)" >&2; exit 1 ;;
esac
case "$arch" in
  x86_64|amd64)  arch="amd64" ;;
  arm64|aarch64) arch="arm64" ;;
  *) echo "unsupported architecture: $arch (this release ships amd64 and arm64 only)" >&2; exit 1 ;;
esac
platform="${os}_${arch}"

# ── optional auth header ─────────────────────────────────────────────────────
# The public release needs none; a GITHUB_TOKEN (e.g. for a private mirror or to
# avoid API rate limits) is sent as a Bearer header on every API + asset request.
#
# The token is passed to curl by PREPENDING "-H Authorization: Bearer …" as its
# OWN positional args inside the wrapper, NOT via an unquoted `${auth:+-H "$auth"}`
# expansion — that idiom word-splits the header VALUE (which contains spaces) into
# separate argv words, so curl would receive a mangled/space-prefixed header and
# the token would never reach the server. Building argv positionally keeps the
# whole header as one word.
curl_dl() {
  # curl_dl <curl-args...> — a plain download (used for release assets).
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    curl -fsSL -H "Authorization: Bearer ${GITHUB_TOKEN}" "$@"
  else
    curl -fsSL "$@"
  fi
}
curl_api() {
  # curl_api <curl-args...> — a GitHub REST call (adds the API Accept header).
  curl_dl -H "Accept: application/vnd.github+json" "$@"
}

# release_json holds the release payload for $VERSION once fetched (see below), so
# asset lookups don't re-hit the API per file.
release_json=""

# download_asset <asset-name> <dest-path> — download one release asset, handling
# BOTH repo visibilities:
#   - PUBLIC (no token): the CDN `browser_download_url`
#     (github.com/<repo>/releases/download/<tag>/<name>) needs no auth and is fast.
#   - PRIVATE (token set): that CDN URL 404s even WITH a Bearer header — GitHub only
#     serves a private repo's assets through the API asset endpoint
#     (api.github.com/repos/<repo>/releases/assets/<id>) requested with
#     `Accept: application/octet-stream`. So resolve the asset's API `url` by name
#     from the release JSON and fetch THAT. This is the fix for `GITHUB_TOKEN` not
#     working: the token now reaches an endpoint that actually honors it.
download_asset() {
  asset_name="$1"; dest="$2"
  if [ -z "${GITHUB_TOKEN:-}" ]; then
    # Public path: straight CDN download, no auth.
    curl -fsSL -o "$dest" "https://github.com/${REPO}/releases/download/${VERSION}/${asset_name}"
    return
  fi
  # Private path: find the asset's API url (the "url" field, NOT browser_download_url)
  # in the block whose "name" matches, then download with the octet-stream Accept.
  # The awk walks the release JSON: it remembers the most recent "url": line and,
  # when it sees the matching "name": "<asset_name>", prints that remembered url.
  api_url="$(printf '%s\n' "$release_json" | awk -v want="\"name\": \"${asset_name}\"" '
    /"url":/ { u = $0 }
    index($0, want) { print u; exit }
  ' | sed -E 's/.*"url": *"([^"]+)".*/\1/')"
  if [ -z "$api_url" ]; then
    echo "asset ${asset_name} not found in release ${VERSION}" >&2
    exit 1
  fi
  curl -fsSL -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "Accept: application/octet-stream" \
    -o "$dest" "$api_url"
}

# ── resolve the version tag ──────────────────────────────────────────────────
if [ "$VERSION" = "latest" ]; then
  echo "resolving latest release…"
  VERSION="$(curl_api "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
  if [ -z "$VERSION" ]; then
    echo "could not resolve the latest release tag." >&2
    echo "  (check network access to api.github.com — a PRIVATE repo needs GITHUB_TOKEN;" >&2
    echo "   or pass CONVERGE_VERSION=vX.Y.Z)" >&2
    exit 1
  fi
fi
echo "installing Converge ${VERSION} for ${platform}"

tarball="converge_${VERSION}_${platform}.tar.gz"

# Fetch the release payload for this tag ONCE — download_asset resolves each asset's
# API url from it (private repos), and it also confirms the release exists. Only
# needed when a token is set (the public path downloads straight from the CDN and
# never consults this); skip the call otherwise.
if [ -n "${GITHUB_TOKEN:-}" ]; then
  release_json="$(curl_api "https://api.github.com/repos/${REPO}/releases/tags/${VERSION}")"
  if [ -z "$release_json" ] || ! printf '%s' "$release_json" | grep -q '"assets":'; then
    echo "could not fetch release ${VERSION} from the API (check the tag exists and the token has repo read)" >&2
    exit 1
  fi
fi

# ── choose an install dir on PATH ────────────────────────────────────────────
bindir="${CONVERGE_BIN_DIR:-}"
if [ -z "$bindir" ]; then
  if [ -w /usr/local/bin ] 2>/dev/null; then
    bindir="/usr/local/bin"
  else
    bindir="${HOME}/.local/bin"
  fi
fi
mkdir -p "$bindir"

# ── download the tarball + checksums into a temp dir ─────────────────────────
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
echo "downloading ${tarball}…"
download_asset "${tarball}"   "${tmp}/${tarball}"
download_asset "SHA256SUMS"   "${tmp}/SHA256SUMS"

# ── verify the checksum ──────────────────────────────────────────────────────
echo "verifying checksum…"
( cd "$tmp"
  # Match the tarball's line as a FIXED string (grep -F) anchored on the two-space
  # separator `shasum -a 256` emits ("<hash>  <name>"), so the dots in the filename
  # are literal, not regex wildcards, and a similarly-named asset can't cross-match.
  want="$(grep -F "  ${tarball}" SHA256SUMS | awk '{print $1}')"
  if [ -z "$want" ]; then echo "no checksum for ${tarball} in SHA256SUMS" >&2; exit 1; fi
  if command -v sha256sum >/dev/null 2>&1; then
    got="$(sha256sum "$tarball" | awk '{print $1}')"
  else
    got="$(shasum -a 256 "$tarball" | awk '{print $1}')"
  fi
  if [ "$want" != "$got" ]; then
    echo "checksum MISMATCH for ${tarball}:" >&2
    echo "  expected $want" >&2
    echo "  got      $got"  >&2
    exit 1
  fi
)

# ── extract + install the requested binaries ─────────────────────────────────
tar -xzf "${tmp}/${tarball}" -C "$tmp"
for b in $BINARIES; do
  if [ ! -f "${tmp}/${b}" ]; then
    echo "warning: ${b} not found in ${tarball}, skipping" >&2
    continue
  fi
  install -m 0755 "${tmp}/${b}" "${bindir}/${b}"
  echo "installed ${b} -> ${bindir}/${b}"
done

# ── PATH hint ────────────────────────────────────────────────────────────────
case ":${PATH}:" in
  *":${bindir}:"*) : ;;
  *) echo "note: ${bindir} is not on your PATH — add it, e.g.:  export PATH=\"${bindir}:\$PATH\"" >&2 ;;
esac
echo "done. Try:  conctl --help"
