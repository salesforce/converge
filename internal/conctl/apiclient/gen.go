// Package apiclient holds the OpenAPI-generated types + HTTP client for the
// converge control-plane REST API, consumed by the conctl CLI.
//
// The client is GENERATED from internal/api/openapi.golden.yaml — the same
// golden spec the API server emits and the OpenAPI golden test guards — so the
// CLI's wire types never drift from the server's. Regenerate after any API
// change with `just codegen-cli` (which re-runs oapi-codegen over the golden
// spec). Do NOT hand-edit client.gen.go / types.gen.go.
package apiclient

//go:generate go run ./spectool ../../api/openapi.golden.yaml openapi.3.0.yaml
//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1 -config codegen.yaml openapi.3.0.yaml
