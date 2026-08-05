package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/specschema"
)

// fakeValidator rejects any kind/spec in `reject`, else passes. Lets the
// gateway-wiring test run with NO database: ApplySpec validates BEFORE the tx,
// so a rejection returns before s.db is ever touched.
type fakeValidator struct{ reject map[model.Kind]bool }

func (f fakeValidator) ValidateSpec(kind model.Kind, _ int, _ json.RawMessage) error {
	if f.reject[kind] {
		return &specschema.Error{Kind: kind, What: "spec", Details: []string{"forced reject"}}
	}
	return nil
}
func (f fakeValidator) ValidateSpecPartial(kind model.Kind, kindVersion int, raw json.RawMessage, _ []string) error {
	return f.ValidateSpec(kind, kindVersion, raw)
}
func (f fakeValidator) ValidateConfig(model.Kind, int, json.RawMessage) error       { return nil }
func (f fakeValidator) ValidateVerbInput(model.Kind, string, json.RawMessage) error { return nil }

// TestApplySpec_RejectsInvalidBeforeDB proves the data-gateway check: a store
// carrying a validator rejects an invalid spec via ApplySpec WITHOUT reaching
// the DB (s.db is nil here — any DB access would panic). The error is
// ErrInvalidSpec-wrapped (so the API maps it to 400) and carries the typed
// *specschema.Error (so callers get the field details). This is the path an
// ingestion duty / reactor hits when authoring a resource outside the HTTP API.
func TestApplySpec_RejectsInvalidBeforeDB(t *testing.T) {
	s := (&Store{}).WithValidator(fakeValidator{reject: map[model.Kind]bool{"bom": true}})

	_, err := s.ApplySpec(context.Background(), "bom", "x", json.RawMessage(`{"bad":true}`), nil)
	if err == nil {
		t.Fatal("invalid spec must be rejected at the gateway")
	}
	if !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("error must wrap ErrInvalidSpec; got %v", err)
	}
	var ve *specschema.Error
	if !errors.As(err, &ve) {
		t.Fatalf("error must carry the typed *specschema.Error; got %T", err)
	}
}

// TestApplySpec_NilValidatorSkips confirms the default store (no validator,
// e.g. tests) does NOT validate — it would proceed to the DB. We can't run the
// full apply without a DB, but we CAN assert it gets PAST validation (a
// validating store would short-circuit with ErrInvalidSpec; a nil-validator
// store does not, so the failure, if any, is NOT ErrInvalidSpec).
func TestApplySpec_NilValidatorSkips(t *testing.T) {
	s := &Store{} // no validator
	// db is nil → the call will fail when it tries to begin a tx, but the point
	// is it must NOT fail with ErrInvalidSpec (validation was skipped).
	_, err := s.ApplySpec(context.Background(), "bom", "x", json.RawMessage(`{"anything":true}`), nil)
	if errors.Is(err, ErrInvalidSpec) {
		t.Fatal("a nil-validator store must NOT validate (skip), but it returned ErrInvalidSpec")
	}
}
