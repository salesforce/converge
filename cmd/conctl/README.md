# conctl — the converge control-plane CLI

`conctl` is a kubectl-style client for the converge REST API. It applies, gets,
lists, and deletes the control-plane objects — resources, kind manifests (CRDs),
provider configs, reactor bindings — and shows the running cluster registry.

## Build

```sh
just build          # builds bin/conctl (+ converge, worker, stdworker, UI)
# or:
go build -o bin/conctl ./cmd/conctl
```

## Server endpoint

Every command talks to one API base URL, resolved in this order:

1. `--server <url>`
2. `$CONVERGE_SERVER`
3. `http://localhost:8080` (the default bind / demo target)

## TLS

For an `https://` server, TLS is configured with these persistent flags (each
also reads an env var, mirroring the server's `TLS_*_FILE` scheme):

| flag | env | purpose |
|------|-----|---------|
| `--tls-ca <pem>`   | `TLS_CA_FILE`   | CA bundle to verify the **server** cert against (default: system roots) |
| `--tls-cert <pem>` | `TLS_CERT_FILE` | client certificate for **mTLS** (needs `--tls-key`) |
| `--tls-key <pem>`  | `TLS_KEY_FILE`  | client private key for mTLS (needs `--tls-cert`) |
| `--insecure`       | —               | skip server-cert verification (dev/self-signed only) |

`--tls-cert`/`--tls-key` must be given together; `--insecure` and `--tls-ca` are
mutually exclusive. Examples:

```sh
conctl --server https://api:8443 --tls-ca ca.pem cluster        # verify server vs a private CA
conctl --server https://api:8443 --insecure cluster             # self-signed dev server
conctl --server https://api:8443 \                              # mTLS
  --tls-cert client.pem --tls-key client.key --tls-ca ca.pem list resources
```

To bring up a **local HTTPS** control plane for testing, pass a cert/key to the
dev launcher (or a demo, which delegates to it):

```sh
CONTROL_TLS_CERT=server.pem CONTROL_TLS_KEY=server.key just dev
```

## Commands

```sh
conctl apply -f FILE [-f FILE …]   # create-or-update from a manifest (JSON or YAML)
conctl get   TYPE NAME             # fetch one object
conctl list  TYPE                  # list objects of a type
conctl delete TYPE NAME            # delete an object
conctl cluster                     # the running fleet (like `kubectl get nodes`)
```

`TYPE` is one of `resource`, `manifest` (kind CRD), `providerconfig`,
`reactorbinding`, `cluster` — singular, plural, and short aliases (`res`, `pc`,
`rb`, `crd`, `nodes`, …) all resolve.

`-o table` (default) | `json` | `yaml` selects the output. `json`/`yaml` emit the
full API object (for `jq`/`yq`); `table` is the human view.

### apply

Each manifest routes to an endpoint by its **type**, given either as a top-level
field in the document or with `--type` on the command line (resource and
providerconfig share `kind`/`name`/`spec`, so the type is required to tell them
apart):

```jsonc
// providerconfig with a base64 bundle in `data`
{ "type": "providerconfig", "name": "cc", "kind": "stdstarlark",
  "is_default": true, "spec": {}, "data": "<base64-zip-of-.star>" }
```

```sh
conctl apply --type resource -f bom.yaml     # type via flag; whole file is a resource
conctl apply -f pc.json -f binding.json      # multiple files, each self-typed
cat bom.json | conctl apply --type resource -f -   # stdin
```

The API returns `201 Created` on first create and `200` on update; conctl prints
the `X-Apply-Result` verb (`created`/`configured`/`unchanged`).

### examples

```sh
conctl cluster
conctl list resources --kind account --limit 50
conctl list resources --name demo-region -o json | jq '.resources | length'
conctl get resource <kind>/<name>             # addressed by kind+name; incl. the live work-queue claim
conctl get providerconfig cc -o json | jq -r '.data' | base64 -d | funzip
conctl list manifests
conctl delete reactorbinding my-binding
```

## Implementation

- The wire layer (`internal/conctl/apiclient/client.gen.go`) is **generated** from
  the server's OpenAPI golden spec, so the CLI can never drift from the API
  contract. Regenerate after any API change with **`just codegen-cli`**.
- The generation is a two-step pipeline (see `internal/conctl/apiclient`):
  `spectool` down-converts the OpenAPI 3.1 golden spec to a 3.0.3 copy that
  oapi-codegen v2 can parse, then oapi-codegen emits the typed client. Do not
  hand-edit the generated file.
- The command tree, flags, and rendering live in `internal/conctl`.
