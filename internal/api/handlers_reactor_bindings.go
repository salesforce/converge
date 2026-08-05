package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// Handlers for /api/reactor-bindings — the runtime-editable SUBSCRIPTION table,
// the SOLE place the "when X transitions, run reactor R" wiring lives. A binding
// says "when a <watch_kind> resource crosses <transition> (optionally
// label-scoped), run the <reactor> kind's reaction". It is NOT a resource: it
// parameterises the ReactorDispatcher, which reads bindings fresh on every
// claim, so an edit takes effect live with no restart. There is no derived /
// manual split — every binding is an explicit, editable subscription. The
// reactor's WHERE-to-deliver config comes from the reactor kind's DEFAULT
// providerconfig (pulled by the worker), so the whole wiring for a "when a root
// syncs, run a reactor" case is one POST here plus the reactor's providerconfig.

type reactorBindingBody struct {
	Name      string `json:"name"`
	WatchKind string `json:"watch_kind"`
	// WatchKindVersion optionally scopes the subscription to ONE version of the
	// WATCHED kind; 0/omitted = all versions.
	WatchKindVersion int             `json:"watch_kind_version,omitempty"`
	Transition       string          `json:"transition"`
	LabelMatch       json.RawMessage `json:"label_match,omitempty"`
	Reactor          string          `json:"reactor"`
	// ReactorVersion optionally pins which reactor kind_version this binding invokes;
	// 0/omitted = unpinned (resolve the reactor's highest published version at claim
	// time, so a reactor upgrade takes effect automatically).
	ReactorVersion int  `json:"reactor_version,omitempty"`
	Enabled        bool `json:"enabled"`
}

func reactorBindingBodyFromStore(b store.ReactorBinding) reactorBindingBody {
	deref := func(p *int) int {
		if p != nil {
			return *p
		}
		return 0
	}
	return reactorBindingBody{
		Name:             b.Name,
		WatchKind:        string(b.WatchKind),
		WatchKindVersion: deref(b.WatchKindVersion),
		Transition:       b.Transition,
		LabelMatch:       b.LabelMatch,
		Reactor:          string(b.Reactor),
		ReactorVersion:   deref(b.ReactorVersion),
		Enabled:          b.Enabled,
	}
}

type applyReactorBindingInput struct {
	Body struct {
		// Type is the OPTIONAL self-describing object tag (applyType*). Only
		// "reactorbinding" here; accepted + validated, never affects routing.
		Type      string `json:"type,omitempty" enum:"reactorbinding" doc:"Optional self-describing tag; must be \"reactorbinding\" if set. Ignored for routing."`
		Name      string `json:"name" minLength:"1" doc:"Unique subscription name (operator-chosen; opaque)."`
		WatchKind string `json:"watch_kind" minLength:"1" doc:"Resource kind to watch."`
		// WatchKindVersion scopes the subscription to ONE web-API version of the
		// watched kind. 0/omitted = ALL versions.
		WatchKindVersion int `json:"watch_kind_version,omitempty" minimum:"0" doc:"Optional: fire only for this web-API version (v1, v2, …) of the watched kind. 0/omitted = all versions."`
		// enum MUST list the model.Transition constants (a huma struct tag can't reference
		// them directly — keep in sync); huma rejects an out-of-set/empty/omitted value at
		// the edge (422), so it is the sole validation gate for this field.
		Transition string          `json:"transition" enum:"created,synced,degraded,failed,deleted" doc:"Lifecycle transition that fires the subscription."`
		LabelMatch json.RawMessage `json:"label_match,omitempty" doc:"Optional label predicate: {} (or omitted) matches all; else the resource's labels must contain these."`
		Reactor    string          `json:"reactor" minLength:"1" doc:"Reactor kind whose reaction runs when the subscription fires (the broker ships STAGE_REACT to a worker advertising this kind). Must be a registered reactor kind, distinct from the watched kind."`
		// ReactorVersion pins which published kind_version of the reactor this binding
		// invokes. 0/omitted = unpinned (resolve the reactor's HIGHEST published
		// version at claim time). A concrete value makes the binding immune to a
		// later reactor publish — the predictability knob for a versioned reactor.
		ReactorVersion int   `json:"reactor_version,omitempty" minimum:"0" doc:"Optional: pin the reactor's web-API version (v1, v2, …). 0/omitted resolves the reactor's highest published version at claim time."`
		Enabled        *bool `json:"enabled,omitempty" doc:"Whether the subscription is active. Defaults true."`
	}
}

type reactorBindingOutput struct {
	Status int                `header:"-"`
	Body   reactorBindingBody `json:"body"`
}

func (s *Server) applyReactorBinding(ctx context.Context, in *applyReactorBindingInput) (*reactorBindingOutput, error) {
	if s.bindings == nil {
		return nil, huma.Error500InternalServerError("reactor binding store not configured")
	}
	// The watched kind must be one this deployment declares (registered or
	// pending) — same gate as a providerconfig write, so a typo fails fast.
	if !s.kindKnown(model.Kind(in.Body.WatchKind)) {
		return nil, huma.Error422UnprocessableEntity("unknown watch_kind: " + in.Body.WatchKind)
	}
	// transition (enum) and reactor (minLength:"1") are validated at the edge by their
	// huma tags — an out-of-set, empty, or omitted value is a 422 before this handler
	// runs (the enum set is AllTransitions; the tag is derived from it, so they can't
	// drift). The reactor must be a registered REACTOR kind — a binding wires a WATCHED
	// kind to a REACTOR kind, and the two are always distinct (a reactor owns no
	// resource of its own, so watching itself is meaningless).
	if !s.reactorKindKnown(model.Kind(in.Body.Reactor)) {
		return nil, huma.Error422UnprocessableEntity("unknown reactor kind: " + in.Body.Reactor)
	}
	if in.Body.Reactor == in.Body.WatchKind {
		return nil, huma.Error422UnprocessableEntity("reactor must differ from watch_kind (a reactor owns no resource of its own)")
	}
	// RETIRE gate (freeze-new for reactors): reject a NEW/updated binding whose
	// TARGET reactor version is retired — the version the delivery would actually
	// run: the pin if set, else the reactor's HIGHEST published version (what the
	// claim resolves for an unpinned binding). EXISTING bindings keep firing so a
	// retired reactor version drains, exactly like the resource freeze-new/drain
	// gate. A retired reactor version can then be migrated off (re-point the binding
	// to a live version) and GC'd once no binding targets it.
	targetReactorVersion := in.Body.ReactorVersion
	if targetReactorVersion <= 0 {
		vs := s.kindVersionsForKind(model.Kind(in.Body.Reactor))
		targetReactorVersion = vs[len(vs)-1] // highest published (kindVersionsForKind is ascending, never empty)
	}
	if retired, cerr := s.kindVersionRetired(ctx, model.Kind(in.Body.Reactor), targetReactorVersion); cerr == nil && retired {
		return nil, huma.Error422UnprocessableEntity(fmt.Sprintf(
			"reactor %s/v%d is retired: cannot bind onto it (existing bindings keep firing to drain; pin a live version)",
			in.Body.Reactor, targetReactorVersion))
	}

	enabled := true
	if in.Body.Enabled != nil {
		enabled = *in.Body.Enabled
	}
	// reactor_version pins which reactor kind_version this binding invokes; 0/omitted
	// leaves it unpinned (the claim resolves the reactor's highest published version).
	// The reactor KIND is already gated above (reactorKindKnown); a pin to a version
	// the reactor has not YET published is intentionally NOT rejected here — it
	// resolves to a waiting delivery the reaper retries once that version is applied,
	// exactly like an unapplied reactor CRD (so a binding can be authored ahead of a
	// planned reactor publish). Huma's minimum:"0" already rejects a negative value.
	var reactorVersion *int
	if in.Body.ReactorVersion > 0 {
		v := in.Body.ReactorVersion
		reactorVersion = &v
	}
	// watch_kind_version scopes the subscription to one version of the WATCHED kind;
	// 0/omitted = all versions. Not validated against published versions here — a
	// scope to a version with no resources simply never fires (harmless), and a
	// version may be authored ahead of the watched kind's publish.
	var watchKindVersion *int
	if in.Body.WatchKindVersion > 0 {
		v := in.Body.WatchKindVersion
		watchKindVersion = &v
	}
	b := store.ReactorBinding{
		Name:             in.Body.Name,
		WatchKind:        model.Kind(in.Body.WatchKind),
		WatchKindVersion: watchKindVersion,
		Transition:       in.Body.Transition,
		LabelMatch:       in.Body.LabelMatch,
		Reactor:          model.Kind(in.Body.Reactor),
		ReactorVersion:   reactorVersion,
		Enabled:          enabled,
	}
	if err := s.bindings.UpsertReactorBinding(ctx, b); err != nil {
		return nil, internalError(ctx, err)
	}
	out := &reactorBindingOutput{Status: http.StatusOK}
	out.Body = reactorBindingBodyFromStore(b)
	return out, nil
}

type listReactorBindingsOutput struct {
	Body struct {
		Bindings []reactorBindingBody `json:"bindings"`
	}
}

func (s *Server) listReactorBindings(ctx context.Context, _ *struct{}) (*listReactorBindingsOutput, error) {
	if s.bindings == nil {
		return nil, huma.Error500InternalServerError("reactor binding store not configured")
	}
	rows, err := s.bindings.ListReactorBindings(ctx)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	rows = capList(ctx, "reactor_bindings", rows)
	out := &listReactorBindingsOutput{}
	out.Body.Bindings = make([]reactorBindingBody, len(rows))
	for i, r := range rows {
		out.Body.Bindings[i] = reactorBindingBodyFromStore(r)
	}
	return out, nil
}

type reactorBindingNameInput struct {
	Name string `path:"name" doc:"Binding name."`
}

type deleteReactorBindingOutput struct {
	Status int `header:"-"`
	Body   struct {
		Deleted bool `json:"deleted"`
	}
}

func (s *Server) deleteReactorBinding(ctx context.Context, in *reactorBindingNameInput) (*deleteReactorBindingOutput, error) {
	if s.bindings == nil {
		return nil, huma.Error500InternalServerError("reactor binding store not configured")
	}
	deleted, err := s.bindings.DeleteReactorBinding(ctx, in.Name)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	if !deleted {
		return nil, huma.Error404NotFound("no such reactor binding: " + in.Name)
	}
	out := &deleteReactorBindingOutput{Status: http.StatusOK}
	out.Body.Deleted = true
	return out, nil
}
