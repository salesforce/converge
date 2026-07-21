package api

import (
	"context"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/salesforce/converge/internal/store"
)

// Pagination handlers: /api/resources/roots/page, /api/resources/{id}/children/page,
// /api/resources/page. Each runs the matching dbq filter+page query
// plus a count for the X-Total header.

// parseKVPairs turns the repeatable ?kv= filter (the Resources-page kind/version
// chips, each "kind/vN" or "kind/N") into the two parallel arrays the sargable
// pair-filter query wants: kvKinds (the DISTINCT kind list, for the index-pruning
// `r.kind = ANY(...)`) and kvPairs (the "kind:version" strings, for the exact
// per-pair equality). Malformed entries (no version, non-numeric, version out of
// the smallint range) are skipped — a bad chip is ignored, never a 500. Returns
// (nil, nil) when nothing valid was supplied, so the query's NULL branch (no pair
// filter) is taken.
func parseKVPairs(kv []string) (kvKinds, kvPairs []string) {
	if len(kv) == 0 {
		return nil, nil
	}
	seenKind := make(map[string]struct{}, len(kv))
	for _, s := range kv {
		slash := strings.LastIndexByte(s, '/')
		if slash <= 0 || slash == len(s)-1 {
			continue // no "kind/version" shape
		}
		kind := s[:slash]
		vs := strings.TrimPrefix(s[slash+1:], "v") // accept "vpc/v2" and "vpc/2"
		n, err := strconv.Atoi(vs)
		if err != nil || n < 1 || n > 32767 {
			continue
		}
		if _, dup := seenKind[kind]; !dup {
			seenKind[kind] = struct{}{}
			kvKinds = append(kvKinds, kind)
		}
		kvPairs = append(kvPairs, kind+":"+strconv.Itoa(n))
	}
	return kvKinds, kvPairs
}

// mergeLegacyKindVersion supports the DEPRECATED single ?kind_version=N (with
// ?kind=…) query param by folding it into the ?kv= pair filter: it synthesizes a
// "kind/N" entry for each selected kind and appends it to kv, so one code path
// serves both the kv= chips and the deprecated param (no separate single-version
// query predicate). A 0/out-of-range version or no kinds → kv is returned unchanged.
func mergeLegacyKindVersion(kv []string, kinds []string, kindVersion int) []string {
	if kindVersion < 1 || kindVersion > 32767 || len(kinds) == 0 {
		return kv
	}
	out := kv
	for _, k := range kinds {
		out = append(out, k+"/"+strconv.Itoa(kindVersion))
	}
	return out
}

func (s *Server) listRootsPage(ctx context.Context, in *listRootsPageInput) (*listRootsPageOutput, error) {
	q := s.readRepo

	params := store.ListRootResourcesPageParams{
		Limit: int32(in.Limit),
	}
	if len(in.Kinds) > 0 {
		params.Kinds = in.Kinds
	}
	// (kind, version) PAIR filter — the kind/version chips. NULL kv_kinds when none
	// valid, so the query takes its no-pair-filter branch. A legacy single
	// ?kind_version=N (with ?kind=…) is folded into the same pair set for backward
	// compat, so old links keep working without a separate query predicate.
	params.KvKinds, params.KvPairs = parseKVPairs(mergeLegacyKindVersion(in.KV, in.Kinds, in.KindVersion))
	if in.NameLike != "" {
		params.NameLike = &in.NameLike
	}
	if !in.CreatedAfter.IsZero() {
		params.CreatedAfter = pgtype.Timestamptz{Time: in.CreatedAfter, Valid: true}
	}
	if !in.CreatedBefore.IsZero() {
		params.CreatedBefore = pgtype.Timestamptz{Time: in.CreatedBefore, Valid: true}
	}
	if !in.UpdatedAfter.IsZero() {
		params.UpdatedAfter = pgtype.Timestamptz{Time: in.UpdatedAfter, Valid: true}
	}
	if !in.UpdatedBefore.IsZero() {
		params.UpdatedBefore = pgtype.Timestamptz{Time: in.UpdatedBefore, Valid: true}
	}
	if in.GenMin > 0 {
		params.GenMin = &in.GenMin
	}
	if in.GenMax > 0 {
		params.GenMax = &in.GenMax
	}
	if in.CountMin > 0 {
		params.CountMin = &in.CountMin
	}
	if in.CountMax > 0 {
		params.CountMax = &in.CountMax
	}
	if in.Phase != "" {
		params.Phase = &in.Phase
	}
	if cursorAt, cursorID, ok := decodeCursorUUID(in.Cursor); ok {
		params.CursorAfterAt = pgtype.Timestamptz{Time: cursorAt, Valid: true}
		params.CursorAfterID = pgtype.UUID{Bytes: cursorID, Valid: true}
	}

	rows, err := q.ListRootResourcesPage(ctx, params)
	if err != nil {
		return nil, internalError(ctx, err)
	}

	countParams := store.CountRootResourcesFilteredParams{
		Kinds:         params.Kinds,
		KvKinds:       params.KvKinds,
		KvPairs:       params.KvPairs,
		NameLike:      params.NameLike,
		CreatedAfter:  params.CreatedAfter,
		CreatedBefore: params.CreatedBefore,
		UpdatedAfter:  params.UpdatedAfter,
		UpdatedBefore: params.UpdatedBefore,
		GenMin:        params.GenMin,
		GenMax:        params.GenMax,
		CountMin:      params.CountMin,
		CountMax:      params.CountMax,
		Phase:         params.Phase,
	}
	total, _ := q.CountRootResourcesFiltered(ctx, countParams)

	items := make([]rootPageItem, len(rows))
	for i, r := range rows {
		items[i] = rootPageItem{
			ResourceInfo: ResourceInfo{
				Kind:                string(r.Kind),
				KindVersion:         int(r.KindVersion),
				Name:                r.Name,
				Generation:          r.Generation,
				SyncedGen:           r.SyncedGen,
				IsReady:             r.IsReady,
				HealthOK:            r.HealthOk,
				Phase:               r.Phase,
				Conditions:          synthesizeConditions(r.Generation, r.SyncedGen, r.HealthOk, r.Phase, nil),
				DeletionRequestedAt: r.DeletionRequestedAt,
				Labels:              decodeJSONMap(r.Labels),
				CreatedAt:           r.CreatedAt,
				UpdatedAt:           r.UpdatedAt,
			},
			ResourceCount: r.ChildCount,
		}
	}

	out := &listRootsPageOutput{}
	out.Body.Roots = items
	out.Total = strconv.FormatInt(total, 10)
	// Opaque next cursor: packs the last row's (created_at, id) keyset so the
	// resource uuid never appears bare in the header.
	if n := len(rows); n > 0 {
		out.NextCursor = encodeCursor(rows[n-1].CreatedAt.Time, rows[n-1].ID.String())
	}
	return out, nil
}

func (s *Server) listResourcesPage(ctx context.Context, in *listResourcesPageInput) (*listResourcesPageOutput, error) {
	parent, id, err := s.loadParentInfo(ctx, in.Kind, in.Name)
	if err != nil {
		return nil, err
	}
	rows, nextCursor, err := s.queryResourcesPage(ctx,
		[]uuid.UUID{id}, in.Kinds, in.Phases,
		in.NameLike, in.Labels, in.KindVersion, in.KV, in.Limit, in.Cursor)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	owners := make([]pgtype.UUID, len(rows))
	for i, r := range rows {
		owners[i] = r.OwnerID
	}
	cache := s.loadOwnerCacheFromIDs(ctx, owners)
	out := &listResourcesPageOutput{}
	out.NextCursor = nextCursor
	out.Body.Owner = parent
	out.Body.Resources = make([]resourceListItem, len(rows))
	for i, r := range rows {
		out.Body.Resources[i] = rowToListItem(r, cache)
	}
	return out, nil
}

func (s *Server) listResourcesPageMulti(ctx context.Context, in *listResourcesPageMultiInput) (*listResourcesPageMultiOutput, error) {
	ownerIDs := s.resolveOwnerIDs(ctx, in.Owners)
	rows, nextCursor, err := s.queryResourcesPage(ctx,
		ownerIDs, in.Kinds, in.Phases,
		in.NameLike, in.Labels, in.KindVersion, in.KV, in.Limit, in.Cursor)
	if err != nil {
		return nil, internalError(ctx, err)
	}

	// Resolve the owner row (id, kind, name, …) for every DISTINCT owner
	// present in this page in ONE GetResourcesInfo call. The result serves
	// two jobs: it's the kind cache for the per-row synth owner_kind label,
	// AND it's the Owners echo the UI groups rows under. The cross-cluster
	// view (no owner_id scope) groups by owner_id too, so it needs the
	// names just as much as a scoped view — without this echo the group
	// headers fell back to raw UUIDs.
	ownerCols := make([]pgtype.UUID, len(rows))
	for i, r := range rows {
		ownerCols[i] = r.OwnerID
	}
	infos := s.loadOwnerInfos(ctx, ownerCols)
	cache := make(map[uuid.UUID]ownerRef, len(infos))
	out := &listResourcesPageMultiOutput{}
	out.NextCursor = nextCursor
	out.Body.Owners = make([]resourceInfo, len(infos))
	for i, m := range infos {
		cache[m.ID] = ownerRef{Kind: string(m.Kind), Name: m.Name}
		out.Body.Owners[i] = infoFromGetResource(m)
	}
	out.Body.Resources = make([]resourceListItem, len(rows))
	for i, r := range rows {
		out.Body.Resources[i] = rowToListItem(r, cache)
	}
	return out, nil
}

// queryResourcesPage runs one keyset page of the resources list. It returns
// the rows plus the next-page cursor (created_at, id of the last row) and
// does NOT compute a total: the list is token-paginated (Next/Prev), so a
// COUNT(*) over the whole filtered set on every request would be pure waste
// at the 10M-row scale target — and it can lag/contradict the page under
// live writes. The cursor pages are exact and self-consistent on their own.
func (s *Server) queryResourcesPage(
	ctx context.Context,
	ownerIDs []uuid.UUID,
	kinds []string,
	phases []string,
	nameLike string,
	labels []string,
	kindVersion int,
	kv []string,
	limit int,
	cursor string,
) ([]store.ListResourcesPageRow, string, error) {
	q := s.readRepo

	// Synth-label filter chip (owner_kind) comes in via the ?label= channel
	// because the UI treats it as a label for chip-management purposes. It isn't
	// stored in resources.labels — route it to the native kinds filter and strip
	// it from the residual label list before building the @> jsonb match. (Owner
	// scoping by identity is done via ?owner=kind/name, resolved to ids upstream,
	// not through a label.)
	cleanedLabels, synthKinds := extractSynthLabelFilters(labels)
	if len(synthKinds) > 0 {
		kinds = append(kinds, synthKinds...)
	}

	listParams := store.ListResourcesPageParams{
		Limit: int32(limit),
	}
	if len(ownerIDs) > 0 {
		listParams.OwnerIds = ownerIDs
	}
	if len(kinds) > 0 {
		listParams.Kinds = kinds
	}
	// (kind, version) PAIR filter — the kind/version chips. A legacy single
	// ?kind_version=N (with ?kind=…) is folded into the pair set for backward compat.
	listParams.KvKinds, listParams.KvPairs = parseKVPairs(mergeLegacyKindVersion(kv, kinds, kindVersion))
	if len(phases) > 0 {
		listParams.Phases = phases
	}
	if nameLike != "" {
		listParams.NameLike = &nameLike
	}
	if labelMatch := parseLabels(cleanedLabels); len(labelMatch) > 0 {
		listParams.LabelMatch = labelMatch
	}
	if cursorAt, cursorID, ok := decodeCursorUUID(cursor); ok {
		listParams.CursorAfterAt = pgtype.Timestamptz{Time: cursorAt, Valid: true}
		listParams.CursorAfterID = pgtype.UUID{Bytes: cursorID, Valid: true}
	}
	rows, err := q.ListResourcesPage(ctx, listParams)
	if err != nil {
		return nil, "", err
	}
	// next-page cursor = opaque token of the last row's (created_at, id) keyset,
	// matching the query's ORDER BY created_at DESC, id DESC. Empty when this page
	// didn't fill — the client treats an empty X-Next-Cursor as "no further pages".
	var nextCursor string
	if n := len(rows); n == limit {
		nextCursor = encodeCursor(rows[n-1].CreatedAt.Time, rows[n-1].ID.String())
	}
	return rows, nextCursor, nil
}
