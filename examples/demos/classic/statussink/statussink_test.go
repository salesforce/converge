package statussink

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/salesforce/converge/sdk-go/converge"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// newReactor builds a reactor with the given default config doc and a dialed
// (fake) store — mirrors fakeevent's newWorker.
func newReactor(t *testing.T, defaultDoc string) (reactor, *fakeStore) {
	t.Helper()
	store, err := dialStore("mem://bucket")
	if err != nil {
		t.Fatalf("dialStore: %v", err)
	}
	return reactor{store: store, defaultSpec: json.RawMessage(defaultDoc)}, store
}

func runReact(t *testing.T, r reactor, req converge.ReactionRequest) error {
	t.Helper()
	if req.Env == nil {
		req.Env = &converge.Env{Logger: discardLogger()}
	}
	_, err := r.React(context.Background(), req)
	return err
}

func TestDialStoreRejectsEmptyAndInvalid(t *testing.T) {
	if _, err := dialStore(""); err == nil {
		t.Fatal("empty endpoint should fail (BOOTSTRAP)")
	}
	if _, err := dialStore("not a url with spaces"); err == nil {
		t.Fatal("invalid endpoint should fail")
	}
	if _, err := dialStore("mem://bucket"); err != nil {
		t.Fatalf("valid endpoint should dial: %v", err)
	}
}

func TestRunReactUploadsStatusKeyedByGeneration(t *testing.T) {
	r, store := newReactor(t, `{"endpoint":"mem://bucket","prefix":"dump/"}`)
	status := json.RawMessage(`{"teams":[{"team":"alpha"}]}`)
	req := converge.ReactionRequest{
		Resource:   converge.Resource{Name: "proj", Status: status, Generation: 7},
		Transition: converge.TransitionSynced,
		Generation: 7,
		DedupToken: "id:synced:7",
	}
	if err := runReact(t, r, req); err != nil {
		t.Fatalf("RunReact: %v", err)
	}
	body, ok := store.Get("dump/proj-7.json")
	if !ok {
		t.Fatal("object should be uploaded at dump/proj-7.json")
	}
	if string(body) != string(status) {
		t.Fatalf("uploaded body = %s, want %s", body, status)
	}
}

func TestRunReactIsIdempotentOnRedelivery(t *testing.T) {
	r, store := newReactor(t, `{"endpoint":"mem://bucket","prefix":"dump/"}`)
	req := converge.ReactionRequest{
		Resource:   converge.Resource{Name: "proj", Status: json.RawMessage(`{"v":1}`), Generation: 3},
		Transition: converge.TransitionSynced, Generation: 3,
	}
	// Two deliveries of the same (resource, transition, generation) → same key,
	// overwrite, not a duplicate object — at-least-once is safe.
	if err := runReact(t, r, req); err != nil {
		t.Fatal(err)
	}
	if err := runReact(t, r, req); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get("dump/proj-3.json"); !ok {
		t.Fatal("object should exist after redelivery")
	}
}

func TestRunReactOverridePrefixWins(t *testing.T) {
	// Per-binding override re-points the prefix; endpoint stays the BOOTSTRAP one.
	r, store := newReactor(t, `{"endpoint":"mem://bucket","prefix":"default/"}`)
	req := converge.ReactionRequest{
		Resource:   converge.Resource{Name: "proj", Status: json.RawMessage(`{}`), Generation: 1},
		Transition: converge.TransitionSynced, Generation: 1,
		// Per-binding sink config now rides on Env.ProviderConfig (was req.Config).
		Env: &converge.Env{Logger: discardLogger(), ProviderConfig: json.RawMessage(`{"prefix":"override/"}`)},
	}
	if err := runReact(t, r, req); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get("override/proj-1.json"); !ok {
		t.Fatal("override prefix should win")
	}
	if _, ok := store.Get("default/proj-1.json"); ok {
		t.Fatal("default prefix should not be used when overridden")
	}
}
