package conctl

import (
	"net/http"
	"testing"

	"github.com/salesforce/converge/internal/conctl/apiclient"
)

// TestOkBodyPrefersTyped: when the client already decoded the body (status 200),
// okBody returns it as-is without touching the raw bytes.
func TestOkBodyPrefersTyped(t *testing.T) {
	typed := &apiclient.ProviderConfigBody{Name: "cc"}
	got, err := okBody(typed, nil, &http.Response{StatusCode: 200}, nil)
	if err != nil {
		t.Fatalf("okBody: %v", err)
	}
	if got != typed {
		t.Error("okBody should return the already-decoded typed body verbatim")
	}
}

// TestOkBodyDecodes201: the converge apply endpoints return 201 Created on first
// create, which the spec doesn't enumerate, so oapi-codegen leaves JSON200 nil
// and the body lands in raw .Body. okBody must still treat this as success by
// decoding the raw bytes — the bug that made a successful create look like an
// error ("Error: 201 Created").
func TestOkBodyDecodes201(t *testing.T) {
	raw := []byte(`{"name":"cc","kind":"account","is_default":false}`)
	got, err := okBody[apiclient.ProviderConfigBody](nil, raw, &http.Response{StatusCode: 201, Status: "201 Created"}, nil)
	if err != nil {
		t.Fatalf("okBody on 201: %v", err)
	}
	if got.Name != "cc" || got.Kind != "account" {
		t.Errorf("okBody decoded 201 body wrong: %+v", got)
	}
}

// TestOkBodyErrorsOnNon2xx: a 4xx with an RFC7807 problem body surfaces as an
// error carrying the title/detail.
func TestOkBodyErrorsOnNon2xx(t *testing.T) {
	title := "Unprocessable Entity"
	detail := "spec does not match config_schema"
	problem := &apiclient.ErrorModel{Title: &title, Detail: &detail}
	_, err := okBody[apiclient.ProviderConfigBody](nil, nil,
		&http.Response{StatusCode: 422, Status: "422 Unprocessable Entity"}, problem)
	if err == nil {
		t.Fatal("okBody should error on a 422")
	}
	if got := err.Error(); got != title+": "+detail {
		t.Errorf("okBody error = %q, want %q", got, title+": "+detail)
	}
}

// TestOkBodyEmpty2xx: a 2xx with an empty body (e.g. a 204-style delete) is a
// success and yields a zero-value object, not an error.
func TestOkBodyEmpty2xx(t *testing.T) {
	got, err := okBody[apiclient.DeleteProviderConfigOutputBody](nil, nil,
		&http.Response{StatusCode: 200}, nil)
	if err != nil {
		t.Fatalf("okBody on empty 2xx: %v", err)
	}
	if got == nil {
		t.Error("okBody should return a non-nil zero object on empty 2xx")
	}
}
