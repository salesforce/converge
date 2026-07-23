package api

import (
	"context"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/model"
)

// Raw-bytes download handlers. They are ordinary Huma operations (registered in
// registerRoutes like everything else — no separate mux), but each returns a
// *huma.StreamResponse: Huma then SKIPS its usual output-struct marshal +
// schema validation and just invokes the Body closure, which sets the download
// headers and writes the raw column bytes verbatim. So a caller gets the exact
// stored bytes — re-appliable as-is — with an attachment filename, at any size,
// while the endpoint still shares Huma's routing, param parsing, error shape,
// and rate limiting.
//
// These endpoints intentionally use dedicated single-column SQL queries
// (GetResourceManifest / GetResourceStatus) instead of GetResource: GetResource
// elides oversize columns server-side using pg_column_size + CASE so the pgx
// wire transfer stays small. The download path WANTS the full bytes regardless
// of size, so it fetches the column directly.

// rawDownloadInput is the shared path input for the raw downloads, addressed by
// the public (kind, name). The path fields are declared directly (not via an
// embedded kindNamePath): Huma's StreamResponse operations don't promote an
// embedded struct's path tags into the OpenAPI parameters, which left the spec
// with an undeclared path param and broke conctl client codegen.
type rawDownloadInput struct {
	Kind string `path:"kind" doc:"Resource kind."`
	Name string `path:"name" doc:"Resource name."`
}

// downloadResourceManifest streams a synthesized ResourceManifest
// ({kind, name, labels, spec}) as a downloadable JSON file. The whole envelope
// is assembled DB-side by GetResourceManifest's jsonb_build_object, so the bytes
// are never reassembled app-side and the file re-applies as-is through Apply.
// Pairs with the elision in rowToFull(): when a spec exceeds the inline limit
// the structured API ships an _elided marker; this endpoint returns the actual
// bytes. No size cap — root specs can run into tens of MB and that's the whole
// point of a dedicated download path.
func (s *Server) downloadResourceManifest(ctx context.Context, in *rawDownloadInput) (*huma.StreamResponse, error) {
	row, err := s.readRepo.GetResourceManifest(ctx, model.Kind(in.Kind), in.Name)
	if err != nil {
		return nil, huma.Error404NotFound("resource not found")
	}
	return jsonAttachment(string(row.Kind), row.Name, "manifest", row.Manifest), nil
}

// downloadResourceStatus mirrors downloadResourceManifest for the bare status
// column (no manifest envelope — status isn't applied back).
func (s *Server) downloadResourceStatus(ctx context.Context, in *rawDownloadInput) (*huma.StreamResponse, error) {
	row, err := s.readRepo.GetResourceStatus(ctx, model.Kind(in.Kind), in.Name)
	if err != nil {
		return nil, huma.Error404NotFound("resource not found")
	}
	return jsonAttachment(string(row.Kind), row.Name, "status", row.Status), nil
}

// jsonAttachment builds a StreamResponse that writes body verbatim as a
// downloadable JSON file named "<kind>-<name>-<suffix>.json". An empty body
// writes JSON null (a valid, re-appliable document). The bytes are written
// straight to the wire — no re-marshal, no size cap.
func jsonAttachment(kind, name, suffix string, body []byte) *huma.StreamResponse {
	return &huma.StreamResponse{
		Body: func(hctx huma.Context) {
			hctx.SetHeader("Content-Type", "application/json")
			hctx.SetHeader("Content-Disposition",
				"attachment; filename=\""+kind+"-"+name+"-"+suffix+".json\"")
			if len(body) == 0 {
				_, _ = hctx.BodyWriter().Write([]byte("null"))
				return
			}
			_, _ = hctx.BodyWriter().Write(body)
		},
	}
}
