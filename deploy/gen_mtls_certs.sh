#!/usr/bin/env bash
# gen_mtls_certs.sh — issue a throwaway CA + server/client leaf certs for the
# local mTLS dev fleet (deploy/dev-local.sh, MTLS_DIR=<outdir>).
#
# Every leaf carries a SPIFFE ID in its URI SAN (NOT the CN — the authz check
# reads leaf.URIs), so the same certs also exercise the per-audience SPIFFE
# allowlists (API_/WORKER_/MESH_AUTHZ_SPIFFE_IDS). The dev fleet shares ONE
# client cert across conctl + workers + the broker mesh leg, so its SPIFFE ID is
# put in all three allowlists by dev-local.sh; cross-audience denial is proven by
# the unit test (broker.TestSpiffeGatePerServiceAuthz) and can be shown live by
# presenting the SERVER cert's id to the API (it is not on the API allowlist).
#
# SCOPE: the server cert's SAN is localhost + 127.0.0.1 only. That covers the HOST
# dev fleet (deploy/dev-local.sh — every process dials localhost) and a single-broker
# k8s install. It does NOT cover the broker↔broker MESH on multi-broker k8s: brokers
# dial peers by POD IP (RELAY_ADVERTISE_ADDR), and TLS hostname verification then
# fails ("remote error: tls: bad certificate") because the pod IPs aren't in this
# SAN. That is a limitation of THIS throwaway helper, not the chart/code — real
# deployments issue per-pod certs via cert-manager/SPIRE whose SAN covers the pod
# IP (or a per-pod DNS name). See docs/tls.md.
#
# Usage: deploy/gen_mtls_certs.sh [outdir]   (default: ./bin/dev-certs)
# Emits: ca.crt ca.key server.crt server.key client.crt client.key
set -euo pipefail

OUT="${1:-bin/dev-certs}"
TRUST_DOMAIN="${SPIFFE_TRUST_DOMAIN:-converge.local}"
SERVER_SPIFFE="spiffe://${TRUST_DOMAIN}/dev/server"
CLIENT_SPIFFE="spiffe://${TRUST_DOMAIN}/dev/client"

mkdir -p "$OUT"
cd "$OUT"

echo "--- CA ---"
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -keyout ca.key -out ca.crt -subj "/CN=converge-dev-ca" 2>/dev/null

# issue <name> <spiffe-id> <server|client>
issue() {
  name="$1"; spiffe="$2"; kind="$3"
  if [ "$kind" = "server" ]; then
    # serverAuth for the listeners + clientAuth so a broker can dial peer brokers.
    san="subjectAltName=URI:${spiffe},DNS:localhost,IP:127.0.0.1"
    eku="extendedKeyUsage=serverAuth,clientAuth"
  else
    san="subjectAltName=URI:${spiffe}"
    eku="extendedKeyUsage=clientAuth"
  fi
  openssl req -newkey rsa:2048 -nodes -keyout "${name}.key" -out "${name}.csr" \
    -subj "/CN=${name}" 2>/dev/null
  openssl x509 -req -in "${name}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 30 -out "${name}.crt" \
    -extfile <(printf "%s\n%s\n" "$san" "$eku") 2>/dev/null
  rm -f "${name}.csr"
  echo "--- ${name}.crt  (${spiffe}) ---"
}

issue server "$SERVER_SPIFFE" server
issue client "$CLIENT_SPIFFE" client

# Emit the SPIFFE IDs so dev-local.sh (or a human) can build the allowlists.
{
  echo "SERVER_SPIFFE=${SERVER_SPIFFE}"
  echo "CLIENT_SPIFFE=${CLIENT_SPIFFE}"
} > spiffe-ids.env

echo "--- done: certs in $(pwd) (SPIFFE ids in spiffe-ids.env) ---"
