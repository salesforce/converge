#!/usr/bin/env bash
# gen-bundle.sh — pack the sample HCL module (hcl-bundle/) into bundle.tar.gz, the
# artifact the demo uploads to LocalStack S3 and a terraform resource points its
# spec.bundle_uri at. The tar stores the .tf files at the ROOT (not under
# hcl-bundle/) so the worker unpacks them straight into its terraform working dir.
#
# Run from anywhere; paths resolve relative to this script. `just demo` runs it
# automatically, so you rarely call it by hand.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="$here/bundle.tar.gz"

# -C into the module dir so entries are main.tf (not hcl-bundle/main.tf). COPYFILE
# guard keeps macOS from adding ._ AppleDouble entries the untar would choke on.
COPYFILE_DISABLE=1 tar -czf "$out" -C "$here/hcl-bundle" .

echo "wrote $out ($(wc -c <"$out" | tr -d ' ') bytes): $(tar -tzf "$out" | tr '\n' ' ')"
