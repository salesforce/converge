package api

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// The leaves cursor must survive a resource NAME containing '|' (names are
// free-form TEXT). Splitting on the last '|' (the uuid tail never contains one)
// keeps the (name, id) keyset intact — a first-'|' split would corrupt it.
func TestCursorNameID_PipeInName(t *testing.T) {
	id := uuid.New()
	for _, name := range []string{"plain", "a|b", "a|b|c", "|leading", "trailing|", "fd|team|role"} {
		tok := encodeCursorNameID(name, id.String())
		gotName, gotID := decodeCursorNameID(tok)
		if gotName != name {
			t.Fatalf("name %q round-tripped as %q", name, gotName)
		}
		if gotID != id {
			t.Fatalf("id for name %q round-tripped as %v (want %v)", name, gotID, id)
		}
	}
	// Empty / malformed tokens degrade to the first page (no error, zero values).
	if n, i := decodeCursorNameID(""); n != "" || i != uuid.Nil {
		t.Fatalf("empty token should yield (\"\", Nil), got (%q, %v)", n, i)
	}
	if n, i := decodeCursorNameID("!!!not-base64!!!"); n != "" || i != uuid.Nil {
		t.Fatalf("garbage token should yield (\"\", Nil), got (%q, %v)", n, i)
	}
}

// The (created_at, id) cursors are safe because the RFC3339Nano time half never
// contains '|'; assert the round-trip for both uuid and int64 id halves.
func TestCursorRoundTrip(t *testing.T) {
	at := time.Date(2026, 7, 7, 12, 0, 0, 123456789, time.UTC)
	id := uuid.New()

	tok := encodeCursor(at, id.String())
	gotAt, gotID, ok := decodeCursorUUID(tok)
	if !ok || gotID != id || gotAt.IsZero() {
		t.Fatalf("uuid cursor round-trip failed: ok=%v id=%v at=%v", ok, gotID, gotAt)
	}

	tok2 := encodeCursor(at, "12345")
	gotAt2, seq := decodeCursorInt64(tok2)
	if seq != 12345 || gotAt2.IsZero() {
		t.Fatalf("int64 cursor round-trip failed: seq=%d at=%v", seq, gotAt2)
	}

	// A zero time yields no token (empty last page → no next cursor).
	if encodeCursor(time.Time{}, id.String()) != "" {
		t.Fatal("zero time should encode to empty token")
	}
}
