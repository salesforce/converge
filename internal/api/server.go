// Package api is the HTTP layer for Converge.
//
// Every entity is a resource: a row with owner_id IS NULL is a root,
// and a row with owner_id NOT NULL is owned by another resource. The
// API surface is uniformly /api/<version>/resources/* — the version
// (apiPrefix, currently /api/v1) is in the path and echoed on every
// response via X-API-Version. A breaking change ships a /api/v2 surface
// alongside v1 rather than mutating v1 in place. The endpoint paths in the
// table below are shown without the version prefix for brevity.
//
// Read/write split: the Server depends on two Repo interfaces (injected via
// Deps). Every pure GET handler reads from `s.readRepo` so UI traffic can be
// served by a Postgres read replica. Mutations (POST/PUT/DELETE, op invocations,
// audit-log emits) and any read-after-write that must observe the just-written
// row use `s.repo` (primary). When no replica is configured DepsFromPools aliases
// read = primary, so the split is a no-op.
//
// File map (all in package api):
//
//	server.go               Server struct, NewServer, Handler, OpenAPISpec,
//	                        registerRoutes (the API surface in one table),
//	                        humaConfig, spaHandler, bigBody.
//	ports.go                Repo + role interfaces + compile-time conformance check.
//	schema_registry.go      schemaRegistry: declared kind/reactor manifests +
//	                        JSON-Schema cache + kindKnown/servableSchema queries.
//	dto.go                  Every huma input/output struct (resourceInfo,
//	                        resourceFull, page inputs, topology types, …).
//	projection.go           dbq.Resource → DTO mappers, label decoding,
//	                        owner-kind cache, summary aggregators.
//	middleware.go           gzip, decompress, CORS.
//	schemas.go              Per-kind JSON Schema cache, server-side spec
//	                        validation, /api/kinds* endpoints,
//	                        registerOpenAPIComponents (named refs in /docs).
//	handlers_objects.go     CRUD + children for a resource by id.
//	handlers_pages.go       Filtered + paginated roots and children pages.
//	handlers_summary.go     Per-root and cross-cluster readiness/kind aggregates.
//	handlers_topology.go    Group-by-label drill-down + label-key autocomplete.
//	handlers_subgraph.go    N-hop dependency walk from a starting resource.
//	handlers_events.go      Per-resource audit log.
//	handlers_graph.go       Children + dependency edges of a single root.
//	handlers_ops.go         Subresource verb invocations + listings.
//	handlers_clusterinfo.go Running-fleet registry (cluster view).
//
// Endpoints exposed:
//
//	/api/version                                   GET                 build version + API version(s) + Go runtime
//	/api/resources                                 GET, POST           paginated/filtered cross-cluster list; POST applies a spec (create-or-update by kind+name; 201 on create, 200 on update)
//	/api/resources/roots                           GET                 paginated/filtered roots (owner_id IS NULL)
//	/api/resources/summary                         GET                 cross-cluster readiness/kind aggregates
//	/api/cluster-members                           GET                 running-fleet registry (members, roles, shards, liveness)
//	/api/kinds                                     GET                 every kind the providers declare (registered or still pending)
//	/api/kinds/{kind}/schema                       GET                 JSON Schema for a kind's spec/status
//	A resource is addressed by its PUBLIC (kind, name) — never the internal uuid
//	(k8s identity model; the uuid surfaces only as `uid` on the full detail read).
//	/api/resources/{kind}/{name}                   GET, DELETE         full resource with spec; DELETE requests soft-delete
//	/api/resources/{kind}/{name}/reconcile         POST                re-pend a resource
//	/api/resources/{kind}/{name}/children          GET                 paginated/filtered children of a resource
//	/api/resources/{kind}/{name}/dependencies      GET                 upstream resources this one depends on
//	/api/resources/{kind}/{name}/graph             GET                 children + dep edges (join by kind/name ref)
//	/api/resources/{kind}/{name}/summary           GET                 readiness/kind aggregates under this owner
//	/api/resources/{kind}/{name}/changes           GET                 children updated_at > since
//	/api/resources/{kind}/{name}/topology          GET                 group-by-label hierarchy
//	/api/resources/{kind}/{name}/topology/leaves   GET                 leaves at a topology path
//	/api/resources/{kind}/{name}/topology/keys     GET                 label-key autocomplete
//	/api/resources/{kind}/{name}/subgraph          GET                 N-hop dep walk
//	/api/resources/{kind}/{name}/events            GET                 audit log per resource
//	/api/resources/{kind}/{name}/ops               GET, POST           list operations; POST invokes a verb (verb+input in body → 202 + created op row)
//	/api/resources/{kind}/{name}/ops/{op_uid}      GET                 single operation row (opaque op handle)
//	/openapi.yaml, /openapi.json                   GET                 OpenAPI 3.1 document (huma-emitted)
//	/docs                                          GET                 huma's built-in Stoplight Elements page
package api

import (
	"io/fs"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/health"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/ui"
)

// Server depends on two Repo interfaces (injected via Deps):
//
//   - repo: primary (read-write). Every mutation handler (POST/PUT/DELETE, op
//     invocations, audit-log emits) goes here, plus any read-after-write that
//     must observe the just-written row.
//   - readRepo: read replica when one is configured. Every pure-GET handler
//     reads from here so UI traffic doesn't compete with engine writes for
//     primary connections.
//
// When no replica is configured, DepsFromPools aliases read = primary so the
// split is a no-op at runtime.
type Server struct {
	repo        Repo                // primary: writes + read-after-write
	readRepo    Repo                // replica (or primary): UI GETs
	commands    ResourceCommands    // write use-cases (create/patch/reconcile/delete)
	configs     ProviderConfigStore // providerconfigs CRUD (runtime-editable config)
	kindConfigs KindConfigStore     // kind_config read (per-kind cap + resync) for the unified config view
	bindings    ReactorBindingStore // reactor_bindings CRUD (lifecycle-reactor "what reacts to what")
	manifests   KindManifestStore   // kind_manifest CRUD (the CRD: schemas + reactions + policy)

	// readyProbe backs /readyz: a control/all pod that can't reach Postgres
	// reports NotReady. It is the ONLY raw-pool dependency the API keeps, and it
	// is injected as the narrow health.Pinger (satisfied by *pgxpool.Pool) rather
	// than the concrete pool — the server never issues a query outside the Repo
	// interfaces above. /livez + /healthz stay DB-free (a blip can't restart us).
	readyProbe health.Pinger

	// schemaRegistry owns the per-kind SCHEMA SURFACE — the declared work + reactor
	// manifests, the JSON-Schema cache, and every kindKnown/servableSchema/
	// SetDeclaredSchemas query. Embedded BY VALUE so a zero Server{} (tests that
	// build the struct directly with fake stores) has a ready empty registry with
	// no separate init — its pointer-receiver methods bind because a *Server is
	// always addressable. Handlers reach it via promotion: s.kindKnown(...),
	// s.schema(), s.SetDeclaredSchemas(...). See schema_registry.go.
	schemaRegistry

	// onManifestApply, if set, is invoked after a successful manifest PUT so the
	// deployment can rebuild its schema validator from the new manifest set
	// (cmd/converge wires a specschema reload here). nil-safe.
	onManifestApply func()

	// corsAllowedOrigins is the explicit cross-origin allowlist. Empty (the
	// default) sends NO CORS headers at all — the API is then same-origin only
	// (the embedded SPA is served from this same server, so it needs none). A
	// non-empty list reflects a request's Origin back ONLY when it matches, or
	// the single literal "*" opts into allow-any behavior for dev. Set
	// from CORS_ALLOWED_ORIGINS via SetCORSAllowedOrigins.
	corsAllowedOrigins []string

	// rateLimit, when non-nil, enforces a per-client-IP token-bucket limit on
	// every API OPERATION (registered as a Huma middleware — so it covers all
	// huma-routed operations, INCLUDING the raw-bytes downloads, while the k8s
	// probes and the SPA, which are plain mux handlers, are exempt by
	// construction). nil (the default) = no limiting. Set from API_RATE_LIMIT_*
	// via SetRateLimit; its janitor goroutine is stopped by Close.
	rateLimit *rateLimiter

	// buildVersion is the binary's stamped version string, surfaced by the
	// GET /version endpoint. Empty defaults to "dev" in the response. Set via
	// SetBuildVersion from main's BuildVersion.
	buildVersion string

	// live holds the CURRENT inner handler (mux + huma API + SPA). Handler()
	// returns a thin outer handler that delegates to whatever live points at, so
	// a manifest apply can swap in a freshly-built inner handler whose OpenAPI
	// document advertises the new kind's schemas. The swap is needed (not an
	// in-place component-map mutation) because huma serializes the spec lazily and
	// then CACHES the bytes for the API instance's life — so /docs only reflects a
	// runtime CRD if it's served by a brand-new huma instance. atomic.Pointer makes
	// the swap lock-free and visible to in-flight requests (each request reads the
	// pointer once at dispatch). It holds *liveHandler (a struct around the
	// interface) so the atomic stores a concrete pointer, not a pointer-to-interface.
	live atomic.Pointer[liveHandler]
}

// liveHandler wraps the inner http.Handler so Server.live can be an
// atomic.Pointer over a concrete type (atomic.Pointer can't hold an interface
// directly). One allocation per RebuildHandler — negligible (rare control-plane
// event).
type liveHandler struct{ h http.Handler }

// SetCORSAllowedOrigins configures the cross-origin allowlist (see the field
// doc). Call before Handler(). Empty = no CORS headers (same-origin only).
func (s *Server) SetCORSAllowedOrigins(origins []string) {
	s.corsAllowedOrigins = origins
}

// SetRateLimit enables per-client-IP API rate limiting: rps tokens/sec with a
// burst bucket. rps <= 0 (the default) leaves limiting OFF — no limiter is built
// and buildHandler registers no middleware, so there is zero cost. burst <= 0
// defaults to rps (at least 1). trustProxy makes the client key come from
// X-Forwarded-For / X-Real-IP (set it only behind a proxy that overwrites those,
// else a client can forge them). Call before Handler(); Close stops the
// limiter's idle-eviction janitor.
func (s *Server) SetRateLimit(rps float64, burst int, trustProxy bool) {
	if rps <= 0 {
		return
	}
	if burst <= 0 {
		burst = int(rps)
	}
	s.rateLimit = newRateLimiter(rps, burst, trustProxy)
}

// Close releases the server's background resources (currently the rate limiter's
// janitor goroutine). Safe to call on a server that never enabled a limiter, and
// safe to call more than once. Wire it to the process shutdown path.
func (s *Server) Close() {
	if s.rateLimit != nil {
		s.rateLimit.close()
	}
}

// SetBuildVersion sets the binary version string the /version endpoint reports.
// Call before Handler().
func (s *Server) SetBuildVersion(v string) { s.buildVersion = v }

// SetOnManifestApply registers a callback invoked after a successful
// /api/kinds/{kind}/manifest PUT, so the deployment can rebuild its schema
// validator from the new manifest set. nil-safe; tests may skip it.
func (s *Server) SetOnManifestApply(fn func()) { s.onManifestApply = fn }

// SetDeclaredSchemas records the full set of kind MANIFESTS this deployment
// declares (registered now OR still pending a successful Setup) and (re)builds
// the immutable schema cache from them. Call once after NewServer, before
// serving — and again from onManifestApply when a manifest PUT changes the set:
// it makes /docs + /api/kinds advertise — and the providerconfig/spec write
// paths validate — every declared kind, not just the boot-registered ones. Every
// shape (spec/status/config), plus the finalizer and verb set, comes from these
// manifests; the API never consults the live registry.
// The per-kind SCHEMA SURFACE (SetDeclaredSchemas / SetReactorSchemas / kindKnown
// / schema() / servableSchema / … ) lives on the embedded *schemaRegistry (see
// schema_registry.go). Handlers reach it via promotion (s.kindKnown(...),
// s.schema()).

// Deps is the API server's injected dependency set: the data-access ROLE
// INTERFACES the handlers use (never a concrete *store.Store or *pgxpool.Pool)
// plus the readiness pinger for /readyz. The composition root (cmd/converge)
// constructs the concrete store(s) once and hands them in here as interfaces, so
// the API layer can be exercised against fakes with no live Postgres and the
// dependency graph is visible at the wiring site.
//
// Write and read are split across two pools upstream: Primary carries writes +
// read-after-write, Read carries UI GETs (a replica when configured, else the
// same primary handle). ReadyProbe pings the PRIMARY (a pod that can't write
// can't do its job).
type Deps struct {
	Primary     Repo                // writes + read-after-write
	Read        Repo                // UI GETs (replica or primary)
	Commands    ResourceCommands    // write use-cases (create/patch/reconcile/delete)
	Configs     ProviderConfigStore // providerconfigs CRUD (runtime-editable config)
	KindConfigs KindConfigStore     // kind_config read (per-kind cap + resync)
	Bindings    ReactorBindingStore // reactor_bindings CRUD ("what reacts to what")
	Manifests   KindManifestStore   // kind_manifest CRUD (the CRD)
	ReadyProbe  health.Pinger       // /readyz DB ping (nil → readiness always OK)
}

// DepsFromPools builds a Deps from concrete pgx pools — the ONE place the API
// layer calls store.New. Every field is satisfied by *store.Store, but the
// caller (and the resulting Server) only ever sees the role interfaces. Pass
// read == nil (or read == primary) when no replica is configured; the read
// surface then routes to the primary. This is the production/integration
// convenience; unit tests can build Deps{} directly from fakes for any subset
// of the interfaces they exercise.
func DepsFromPools(primary, read *pgxpool.Pool) Deps {
	if read == nil {
		read = primary
	}
	// store.New(primary) satisfies every write/primary-read role; one handle is
	// reused across the roles it serves rather than allocating a store per role.
	primaryStore := store.New(primary)
	return Deps{
		Primary:     primaryStore,
		Read:        store.New(read),
		Commands:    primaryStore,
		Configs:     primaryStore,
		KindConfigs: primaryStore, // primary: kind_config reads are tiny/rare + this is the write path
		Bindings:    primaryStore,
		Manifests:   primaryStore,
		ReadyProbe:  primary,
	}
}

// NewServer wires the API from an injected Deps bundle of role interfaces (see
// Deps). The per-kind schema surface (validation, finalizer, and verb lookups)
// is supplied separately via SetDeclaredSchemas — the API depends only on
// declared schemas, never the live provider registry.
func NewServer(deps Deps) *Server {
	return &Server{
		repo:        deps.Primary,
		readRepo:    deps.Read,
		commands:    deps.Commands,
		configs:     deps.Configs,
		kindConfigs: deps.KindConfigs,
		bindings:    deps.Bindings,
		manifests:   deps.Manifests,
		readyProbe:  deps.ReadyProbe,
		// The zero-value schemaRegistry is an empty registry that accepts any kind
		// until SetDeclaredSchemas populates it (cmd/converge does so at boot).
	}
}

// APIVersion is the current API version. It is carried in the URL path
// (apiPrefix) and advertised on every response via the X-API-Version header, so
// clients can pin a version and detect mismatches. A breaking change bumps this
// to v2 and registers the new surface under /api/v2 alongside v1, rather than
// mutating v1 in place.
const APIVersion = "v1"

// apiPrefix is the single source of truth for the versioned API base path.
// Every huma route and raw-bytes endpoint hangs off it; changing the version
// here moves the whole surface.
const apiPrefix = "/api/" + APIVersion

// apiDocVersion is the OpenAPI document's `info.version`. It is DERIVED from
// APIVersion (the URL contract version) so the spec's version and the /v1 paths
// never disagree: "v1" → "1.0.0". Kept as its own package var (not a literal at
// the DefaultConfig call) so the coupling to APIVersion is explicit and there is
// no stray magic version string. It is the API CONTRACT version, distinct from
// the binary's BuildVersion (the /version endpoint) — bumping APIVersion to v2
// makes this "2.0.0" automatically. (A var, not a const, because strings.TrimPrefix
// is a runtime call.)
var apiDocVersion = strings.TrimPrefix(APIVersion, "v") + ".0.0"

func humaConfig() huma.Config {
	cfg := huma.DefaultConfig("Converge API", apiDocVersion)
	cfg.OpenAPIPath = "/openapi"
	// Many response DTOs carry pgtype.Timestamptz timestamp fields (resource
	// created_at/updated_at, work claimed_at/heartbeat_at, cluster member
	// last_heartbeat, …). pgtype.Timestamptz MarshalJSONs to an RFC3339 STRING,
	// but Huma's reflection sees the struct shape {Time, Valid, InfinityModifier}
	// and would otherwise emit an OBJECT schema — a lie about the wire that breaks
	// generated clients (they'd try to unmarshal a string into the object). Alias
	// it to a string/date-time schema so the spec matches the bytes. Plain
	// time.Time fields already reflect correctly (format: date-time); this makes
	// the nullable-DB-timestamp wrapper behave identically.
	cfg.Components.Schemas.RegisterTypeAlias(
		reflect.TypeOf(pgtype.Timestamptz{}),
		reflect.TypeOf(rfc3339Timestamp{}),
	)
	return cfg
}

// rfc3339Timestamp is the OpenAPI schema stand-in for pgtype.Timestamptz: a
// nullable RFC3339 date-time string, matching pgtype.Timestamptz.MarshalJSON.
// It is only ever used as a schema alias (see humaConfig) — never marshaled — so
// it needs no fields, just the SchemaProvider hook.
type rfc3339Timestamp struct{}

// Schema implements huma.SchemaProvider, returning the string/date-time schema
// the aliased pgtype.Timestamptz actually serializes as.
func (rfc3339Timestamp) Schema(huma.Registry) *huma.Schema {
	return &huma.Schema{Type: huma.TypeString, Format: "date-time", Nullable: true}
}

// Handler returns the long-lived HTTP handler to mount on the listener. It is a
// thin, stable delegator: it reads the CURRENT inner handler from s.live on each
// request and serves it. The inner handler is built by buildHandler and can be
// atomically replaced by RebuildHandler (e.g. after a manifest apply) so the
// OpenAPI surface reflects a runtime-applied CRD — without re-binding the
// listener. Call once; subsequent calls return a delegator over the same holder.
func (s *Server) Handler() http.Handler {
	s.live.Store(&liveHandler{h: s.buildHandler()})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.live.Load().h.ServeHTTP(w, r)
	})
}

// RebuildHandler rebuilds the inner handler from the server's CURRENT schema
// surface and atomically swaps it into the live holder, so the next /docs and
// /openapi request serializes a fresh OpenAPI document that includes a
// runtime-applied kind's schemas. huma caches the serialized spec for an API
// instance's life, so a new instance (a new mux + huma.New) is the only way to
// surface a post-boot CRD in /docs — an in-place component-map mutation would be
// masked by that cache. Safe to call concurrently with serving (in-flight
// requests keep the handler they read at dispatch). No-op before Handler().
func (s *Server) RebuildHandler() {
	if s.live.Load() == nil {
		return // Handler() not yet called; nothing serving to swap
	}
	s.live.Store(&liveHandler{h: s.buildHandler()})
}

// buildHandler constructs one complete inner handler: a fresh mux, a fresh huma
// API with the routes (incl. the raw-bytes downloads) + per-kind OpenAPI
// components registered from the server's current schema surface, the probe
// endpoints + SPA fallback (plain mux handlers), and the middleware chain. Each
// call yields an independent handler (and an independent huma spec cache), so
// RebuildHandler picks up schema changes.
func (s *Server) buildHandler() http.Handler {
	mux := http.NewServeMux()
	api := humago.New(mux, humaConfig())
	// Per-client-IP rate limiting on the huma-routed API operations (a Huma
	// middleware — so it covers every operation registerRoutes adds, including
	// the raw-bytes downloads; the k8s probes + SPA below are plain mux handlers
	// and stay exempt). MUST be registered BEFORE the routes: huma captures the
	// middleware chain at each operation's Handle time, so UseMiddleware only
	// applies to operations registered AFTER it. nil = limiting off (SetRateLimit
	// not called).
	if s.rateLimit != nil {
		api.UseMiddleware(s.rateLimit.middleware(api))
	}
	s.registerRoutes(api)
	registerOpenAPIComponents(api, s.servableSchemas())

	// Kubernetes probes. /readyz pings the PRIMARY pool — a control/all pod
	// that can't reach Postgres can't drain/reap or serve writes, so it
	// reports NotReady; /livez and /healthz stay DB-free so a DB blip can't
	// trigger a liveness restart. Registered on the mux (not via huma) so
	// they're bare text/200, outside the OpenAPI contract, and ahead of the
	// SPA's "/" catch-all below.
	// nil metrics handler: /metrics is NOT served on the API mux (which is the
	// (m)TLS port under TLS — a scraper can't present a client cert). Metrics ride
	// the plaintext health listener instead (see cmd/converge, HealthAddr).
	health.Register(mux, s.readyProbe, nil)

	uiFS, err := fs.Sub(ui.DistFS, "dist")
	if err == nil {
		mux.Handle("/", spaHandler(uiFS))
	}

	// recoverMiddleware is OUTERMOST so it catches a panic from any layer below
	// (incl. the other middlewares) and turns it into a 500 instead of a dropped
	// connection. securityHeadersMiddleware then hardens every response (the SPA
	// served from "/" needs CSP + X-Frame-Options; JSON responses get nosniff).
	return recoverMiddleware(securityHeadersMiddleware(apiVersionMiddleware(
		timeoutMiddleware(decompressRequestMiddleware(
			corsMiddleware(s.corsAllowedOrigins, gzipMiddleware(mux)))))))
}

// spaHandler serves the React build, falling back to index.html for
// any path the dist doesn't recognize so client-side routing works
// on direct page loads.
func spaHandler(distFS fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(distFS))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := strings.TrimPrefix(r.URL.Path, "/")
		if clean == "" {
			clean = "index.html"
		}
		if _, err := fs.Stat(distFS, clean); err != nil {
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/"
			fileServer.ServeHTTP(w, r2)
			return
		}
		fileServer.ServeHTTP(w, r)
	})
}

// OpenAPISpec returns the assembled spec. Used by the golden test in
// openapi_test.go; not part of the runtime serving path.
func (s *Server) OpenAPISpec() *huma.OpenAPI {
	mux := http.NewServeMux()
	api := humago.New(mux, humaConfig())
	s.registerRoutes(api)
	registerOpenAPIComponents(api, s.servableSchemas())
	return api.OpenAPI()
}

// Per-operation request-body caps. Huma's DEFAULT is 1 MiB, which applies to
// every endpoint that sets no limit (kind config, kind manifest PUT, reactor
// bindings, operation invoke, reconcile/quarantine/rollback) — those carry only
// small documents, so the 1 MiB default is the correct anti-abuse ceiling for
// them and they intentionally set nothing. The two endpoints below accept large
// documents and raise the ceiling explicitly; the cap is enforced by Huma BEFORE
// the body is buffered, so an oversized payload is rejected at ingress (413) and
// never reaches the handler or the DB — the first line of defense against a
// resource-exhaustion attack on the API.
const (
	// maxResourceApplyBytes caps a resource Apply body (the ResourceManifest:
	// kind + name + labels + spec). Composer root specs are genuinely large
	// (tens of MB), so 100 MB; beyond that is almost certainly abuse.
	maxResourceApplyBytes = 100 * 1024 * 1024
	// maxProviderConfigApplyBytes caps the WHOLE providerconfig Apply body (spec +
	// the base64 `data` bundle). One body limit covers it — no separate per-field
	// cap on `data` vs the rest.
	maxProviderConfigApplyBytes = 50 * 1024 * 1024
)

var (
	bigBody    = func(o *huma.Operation) { o.MaxBodyBytes = maxResourceApplyBytes }
	configBody = func(o *huma.Operation) { o.MaxBodyBytes = maxProviderConfigApplyBytes }
)

// defaultStatus makes an operation DECLARE `code` as its primary success status in
// the OpenAPI contract (huma otherwise defaults to 200). Used for the async
// endpoints that return 202 Accepted at runtime, so the contract matches the wire.
func defaultStatus(code int) func(*huma.Operation) {
	return func(o *huma.Operation) { o.DefaultStatus = code }
}

// documentCreated declares a 201 Created ALONGSIDE the 200 for a create-or-update
// POST that returns 201-on-create / 200-on-update (via the response Status field +
// X-Apply-Result header) — so the contract stops lying (it advertised only 200).
// Runs AFTER the routes register, reading the built 200 back from the OpenAPI doc so
// the 201 carries the SAME body content (a bare description-only 201 makes
// oapi-codegen drop the default error response — mirroring the content keeps the
// generated client's typed success + problem body intact). No-op if the path/op
// isn't found or has no 200.
func documentCreated(api huma.API, path string) {
	item := api.OpenAPI().Paths[path]
	if item == nil || item.Post == nil {
		return
	}
	op := item.Post
	r200 := op.Responses["200"]
	if r200 == nil {
		return
	}
	op.Responses["201"] = &huma.Response{
		Description: "Created — a new resource was inserted (X-Apply-Result: created).",
		Content:     r200.Content, // same payload schema as the 200 update case
	}
}

// registerRoutes is the single source of truth for the API surface.
// Every line below is one handler exposed to clients. Roots are rows
// with owner_id IS NULL; owned resources point at a parent. Root-only
// listings live under /api/resources/roots.
func (s *Server) registerRoutes(api huma.API) {
	// Resource collection: apply a manifest (create-or-update a resource
	// keyed by kind+name), paged cross-resource list, summary. Roots are
	// listed via the paginated /api/resources/roots/page (type-to-search);
	// there is no unpaginated full-roots endpoint.
	// Build info — unversioned-shape but registered under the versioned prefix
	// so it's in the contract; lets clients read the running binary version and
	// the API versions this server speaks.
	huma.Get(api, apiPrefix+"/version", s.getVersion)

	huma.Post(api, apiPrefix+"/resources", s.applyResourceManifest, bigBody)
	// The cross-resource collection list is the DEFAULT GET on the collection —
	// pagination (cursor/limit) + filters (kind/kv/phase/label/name) + optional
	// repeatable ?owner=kind/name scoping are QUERY params, not a /page action
	// segment. Zero owners = whole cluster.
	huma.Get(api, apiPrefix+"/resources", s.listResourcesPageMulti)
	huma.Get(api, apiPrefix+"/resources/summary", s.getSummaryScoped)
	huma.Get(api, apiPrefix+"/kinds", s.listKindSchemas)
	huma.Get(api, apiPrefix+"/kinds/{kind}/schema", s.getKindSchema)
	// Per-kind OPERATIONAL config (kind_config): cap + resync, runtime-editable.
	// Distinct from the providerconfigs store (the config DOCUMENT); read via the
	// kind schema endpoints above, written here. PUT — an idempotent FULL REPLACE of
	// the single (kind, kind_version) config row, whose identity is entirely in the
	// URL (no body-identity); the body carries effective values, not deltas.
	huma.Put(api, apiPrefix+"/kinds/{kind}/versions/{kind_version}/config", s.applyKindConfig)
	// Per-kind MANIFEST (kind_manifest): the CRD — JSON Schemas + declared
	// reactions + finalizer/policy. PUT applies (schema-change gated 409); GET reads
	// one; the list shows every applied manifest. A DB trigger derives kind_config,
	// so this is the single "define a kind" surface. (Reactor wiring is separate — an
	// editable reactor_bindings subscription, below.) PUT keeps kind_version in the
	// BODY (the CRD document's identity — the verbatim round-trip + content-hash read
	// it there); GET/DELETE address an ALREADY-published version, so its identity is a
	// PATH segment.
	huma.Put(api, apiPrefix+"/kinds/{kind}/manifest", s.putKindManifest)
	huma.Get(api, apiPrefix+"/kinds/{kind}/versions/{kind_version}/manifest", s.getKindManifest)
	huma.Delete(api, apiPrefix+"/kinds/{kind}/versions/{kind_version}/manifest", s.deleteKindManifest)
	huma.Get(api, apiPrefix+"/kinds/manifests", s.listKindManifests)
	// Roots collection (owner_id IS NULL): a DISTINCT first-class path (it returns
	// resource_count + X-Total, which the cross-cluster list omits), paginated by the
	// same cursor/limit/filter query params — no /page action segment.
	huma.Get(api, apiPrefix+"/resources/roots", s.listRootsPage)

	// providerconfigs: the runtime-editable per-kind config store. POST
	// upserts by name (default or custom override), validated against the
	// kind's config_schema; the others list/get/delete by name.
	huma.Post(api, apiPrefix+"/providerconfigs", s.applyProviderConfig, configBody)
	huma.Get(api, apiPrefix+"/providerconfigs", s.listProviderConfigs)
	huma.Get(api, apiPrefix+"/providerconfigs/{name}", s.getProviderConfig)
	huma.Delete(api, apiPrefix+"/providerconfigs/{name}", s.deleteProviderConfig)

	// reactor-bindings: the runtime-editable reactor subscription table
	// ("when a <watch_kind> crosses <transition>, run <reactor>") — the SOLE
	// reactor wiring surface. POST upserts by name; the dispatcher reads them
	// fresh on every claim, so an edit is live.
	huma.Post(api, apiPrefix+"/reactor-bindings", s.applyReactorBinding)
	huma.Get(api, apiPrefix+"/reactor-bindings", s.listReactorBindings)
	huma.Delete(api, apiPrefix+"/reactor-bindings/{name}", s.deleteReactorBinding)

	// Cluster members registry: every running app instance (control + worker,
	// keyed by role) that wrote a heartbeat. A plural-noun COLLECTION (it lists
	// members), so /cluster-members — matching the /reactor-bindings +
	// /providerconfigs collection convention. Listed with a k8s-style soft
	// Ready/NotReady computed from heartbeat age.
	huma.Get(api, apiPrefix+"/cluster-members", s.listClusterInfo)

	// Per-resource: a resource is addressed by its PUBLIC (kind, name) — unique
	// per uniq_resource_meta — never by the internal uuid (k8s identity model).
	// Each handler resolves (kind,name)→id once and dispatches to its kind's
	// provider. Mirrors the name-keyed /providerconfigs/{name} pattern.
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}", s.getResource)
	huma.Delete(api, apiPrefix+"/resources/{kind}/{name}", s.deleteResource)
	huma.Post(api, apiPrefix+"/resources/{kind}/{name}/reconcile", s.reconcileResource, defaultStatus(http.StatusAccepted))
	huma.Post(api, apiPrefix+"/resources/{kind}/{name}/quarantine", s.quarantineResource)
	huma.Post(api, apiPrefix+"/resources/{kind}/{name}/unquarantine", s.unquarantineResource)
	// Owned children: ONE paginated collection (cursor/limit + filters as query
	// params) — the former unpaginated /children was folded in, since the paginated
	// query subsumes it.
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/children", s.listResourcesPage)
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/dependencies", s.listResourceDependencies)
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/graph", s.getGraph)
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/summary", s.getSummary)
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/changes", s.getChanges)
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/topology", s.getTopology)
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/topology/leaves", s.getTopologyLeaves)
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/topology/keys", s.getTopologyKeys)
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/subgraph", s.getSubgraph)
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/events", s.listEvents)

	// Spec revision history + rollback (roots). History lists revisions
	// (bodies elided — fetch a body via the raw spec endpoint); rollback
	// re-applies a chosen revision as a new live revision.
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/spec-history", s.listSpecHistory)
	huma.Post(api, apiPrefix+"/resources/{kind}/{name}/rollback", s.rollbackResource)

	// Operations sub-collection. An operation is a transient sub-resource addressed
	// by its opaque {op_uid}. POST /ops CREATES one (verb + input in the body) and
	// returns 202 with the created row; GET /ops lists; GET /ops/{op_uid} reads one.
	// The verb rides the BODY (not a path segment) so the {op_uid} ITEM slot is free
	// of a template collision (a future DELETE /ops/{op_uid} cancels).
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/ops", s.listOperations)
	huma.Post(api, apiPrefix+"/resources/{kind}/{name}/ops", s.invokeOperation, defaultStatus(http.StatusAccepted))
	huma.Get(api, apiPrefix+"/resources/{kind}/{name}/ops/{op_uid}", s.getOperation)

	// Raw-bytes downloads (huma.StreamResponse: raw column bytes verbatim, no
	// envelope/validation, attachment filename, no size cap — see handlers_raw.go).
	// Kept under a /raw/ prefix to signal "opaque download, not the JSON
	// contract", but routed through huma like every other operation (one router,
	// shared param parsing + errors + rate limiting). No bigBody: these are GETs,
	// so there's no request body to size, and StreamResponse writes the response
	// verbatim with no Huma-imposed cap. /manifest is the re-appliable
	// {kind,name,labels,spec}; /status the bare status; /spec/{N} a historic spec.
	huma.Get(api, apiPrefix+"/raw/resources/{kind}/{name}/manifest", s.downloadResourceManifest)
	huma.Get(api, apiPrefix+"/raw/resources/{kind}/{name}/status", s.downloadResourceStatus)
	huma.Get(api, apiPrefix+"/raw/resources/{kind}/{name}/spec/{generation}", s.downloadSpecRevision)

	// The create-or-update applies return 201 on create / 200 on update — declare
	// the 201 in the contract too (post-registration, so it mirrors the built 200's
	// body). Keeps the OpenAPI honest without a description-only 201 that would break
	// the generated client's default-error modeling.
	documentCreated(api, apiPrefix+"/resources")
	documentCreated(api, apiPrefix+"/providerconfigs")
}
