#!/bin/sh
# One-shot seed: apply the CRDs + the statussink reactor wiring the classicbom composition
# needs, via conctl against the control API (CONVERGE_SERVER). Run by the `seed` compose
# service after the control API is healthy. Backstage applies the BOM RESOURCE afterward —
# this only sets up the kinds (the out-of-band "operator" step).
#
# Idempotent: conctl apply is create-or-update, so re-running `docker compose up` re-seeds
# cleanly (every apply reports created/unchanged).
set -eu

CONCTL=/app/conctl

echo "== applying CRDs (kind manifests) from /fixtures/classic =="
for k in /fixtures/classic/*.kind.json; do
  [ -e "$k" ] || continue
  echo "  apply manifest: $k"
  "$CONCTL" apply --type manifest -f "$k"
done

echo "== applying the reactor subscription (classicbom 'synced' -> statussink) =="
"$CONCTL" apply --type reactorbinding \
  -f /fixtures/classic/reactor-binding-classicbom-to-statussink.json \
  || echo "  (subscription skipped)"

echo "== applying the statussink reactor config =="
if "$CONCTL" apply --type providerconfig \
     -f /fixtures/classic/providerconfig-statussink.json >/dev/null 2>&1; then
  echo "  applied statussink config"
else
  echo "  (statussink config skipped — status upload disabled, compose still works)"
fi

echo "== seed complete. Backstage's 'Provision a deployment' template will apply the BOM. =="
