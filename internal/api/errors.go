package api

import (
	"context"
	"log/slog"

	"github.com/danielgtaylor/huma/v2"
)

// internalError logs the real cause server-side (with request context) and
// returns a GENERIC 500 to the client. Handlers must never surface err.Error()
// verbatim: a raw Postgres error leaks schema details (table/column/constraint
// names, query shape) that aid reconnaissance. Full diagnostics stay in the
// server log; the client sees only "internal server error".
//
// Use as: `return nil, internalError(ctx, err)`.
func internalError(ctx context.Context, err error) huma.StatusError {
	slog.ErrorContext(ctx, "internal server error", "error", err)
	return huma.Error500InternalServerError("internal server error")
}
