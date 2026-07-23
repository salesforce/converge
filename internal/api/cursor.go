package api

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// cursor.go holds the OPAQUE keyset-pagination codec. The list/page/events
// queries keyset-paginate on a (created_at, id) tuple where id is the internal
// resource uuid (or, for events, the bigint sequence). Emitting that id bare in
// an `X-Next-Cursor-Id` header (and accepting it back as `cursor_id`) would leak
// an internal id onto the external surface. Instead we pack the tuple into one
// opaque base64 token the client echoes verbatim: the id stays INSIDE the token
// and is never an addressable/parseable field. The token is not encrypted —
// it's just not a bare id, and it self-documents as an opaque pagination handle.
//
// Wire format (before base64url): "<RFC3339Nano created_at>|<id>". The id half
// is a uuid string for resource pages and a decimal for event pages; the codec
// is id-shape-agnostic (the caller parses its half).

// encodeCursor packs a (created_at, id-string) keyset into an opaque token.
// Returns "" for a zero time so an empty last page yields no next cursor.
func encodeCursor(createdAt time.Time, id string) string {
	if createdAt.IsZero() {
		return ""
	}
	raw := createdAt.Format(time.RFC3339Nano) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor unpacks an opaque token into its (created_at, id-string) parts.
// A malformed/empty token yields a zero time + "" (treated as "from the start"),
// so a bad cursor degrades to the first page rather than erroring.
func decodeCursor(tok string) (time.Time, string) {
	if tok == "" {
		return time.Time{}, ""
	}
	b, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return time.Time{}, ""
	}
	at, id, ok := strings.Cut(string(b), "|")
	if !ok {
		return time.Time{}, ""
	}
	t, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return time.Time{}, ""
	}
	return t, id
}

// decodeCursorUUID unpacks a resource-page cursor into (created_at, uuid). A
// missing/invalid id half yields the zero uuid (valid=false at the call site).
func decodeCursorUUID(tok string) (time.Time, uuid.UUID, bool) {
	at, idStr := decodeCursor(tok)
	if idStr == "" {
		return at, uuid.UUID{}, false
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return at, uuid.UUID{}, false
	}
	return at, id, true
}

// decodeCursorInt64 unpacks an event-page cursor into (created_at, int64). A
// missing/invalid id half yields 0 (no lower bound at the call site).
func decodeCursorInt64(tok string) (time.Time, int64) {
	at, idStr := decodeCursor(tok)
	if idStr == "" {
		return at, 0
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return at, 0
	}
	return at, id
}

// The topology-leaves page keysets on (name, id) rather than (created_at, id),
// so it gets its own string-tuple codec. Same opaque envelope, "<name>|<uuid>".
// The id half is ALWAYS a uuid (never contains '|'), so we split on the LAST
// '|' — a resource name is free-form TEXT and MAY contain '|', and splitting on
// the first '|' there would corrupt the keyset (truncating the name + failing to
// parse the id → zero id → the prior page's last row re-appears, a duplicate/
// non-advancing boundary). Splitting on the last '|' is unambiguous.

// encodeCursorNameID packs a (name, id) keyset into an opaque token. Returns ""
// for an empty name (no next page).
func encodeCursorNameID(name, id string) string {
	if name == "" {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(name + "|" + id))
}

// decodeCursorNameID unpacks a leaves cursor into (name, uuid). A malformed
// token yields ("", zero uuid) — the first page.
func decodeCursorNameID(tok string) (string, uuid.UUID) {
	if tok == "" {
		return "", uuid.UUID{}
	}
	b, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return "", uuid.UUID{}
	}
	s := string(b)
	// Split on the LAST '|': the uuid tail can't contain one, but the name head can.
	sep := strings.LastIndex(s, "|")
	if sep < 0 {
		return "", uuid.UUID{}
	}
	id, err := uuid.Parse(s[sep+1:])
	if err != nil {
		return "", uuid.UUID{}
	}
	return s[:sep], id
}
