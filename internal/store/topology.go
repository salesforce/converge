package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/dbq"
)

// Topology helpers backing GET /api/resources/{id}/topology*. The
// {id} is the owner whose children are being grouped.
//
// The topology view groups resources by an arbitrary ordered list of
// label keys (group_by). The request also carries a `path` — pairs of
// (key, value) representing how deep into the hierarchy the user has
// drilled. The server returns aggregate buckets at the next level OR
// leaf resources when path has reached the bottom.
//
// The SQL templates live in topology_sql.go.

// TopologyPathSegment is one (label_key, label_value) the request has
// already drilled past — these are AND-ed into the WHERE clause.
type TopologyPathSegment struct {
	Key   string
	Value string // empty means "the (no <key>) bucket"
}

// TopologyBucket is one row of the aggregate response. ByReadiness
// counts rows grouped by the `phase` scalar (Ready / Reconciling /
// Degraded / Failed / Deleting) — the rollup the SQL template produces.
type TopologyBucket struct {
	Bucket      string
	ByReadiness map[string]int
	Total       int
}

func buildPathClause(args []any, path []TopologyPathSegment) (string, []any, error) {
	if len(path) == 0 {
		return "", args, nil
	}
	clauses := make([]string, 0, len(path))
	for _, seg := range path {
		if seg.Key == "" {
			return "", args, fmt.Errorf("topology: path segment has empty key")
		}
		args = append(args, seg.Key, seg.Value)
		clauses = append(clauses, fmt.Sprintf(
			"COALESCE(m.labels->>$%d, '') = $%d",
			len(args)-1, len(args),
		))
	}
	return " AND " + strings.Join(clauses, " AND "), args, nil
}

// AggregateTopology returns one TopologyBucket per distinct value of
// `nextKey`, scoped to ownerID + path.
func (s *Store) AggregateTopology(
	ctx context.Context,
	ownerID uuid.UUID,
	path []TopologyPathSegment,
	nextKey string,
	cursor string,
	limit int,
) ([]TopologyBucket, error) {
	if nextKey == "" {
		return nil, fmt.Errorf("AggregateTopology: nextKey is required")
	}
	if limit <= 0 || limit > 500 {
		limit = 200
	}

	args := []any{ownerID, nextKey}
	whereExtra, args, err := buildPathClause(args, path)
	if err != nil {
		return nil, err
	}

	cursorClause := ""
	if cursor != "" {
		args = append(args, cursor)
		cursorClause = fmt.Sprintf(
			" AND COALESCE(m.labels->>$2, '') > $%d",
			len(args),
		)
	}

	// Bound the DISTINCT bucket set in SQL (the keys CTE) to the page limit, so
	// the per-phase aggregate never groups more than `limit` buckets regardless
	// of label cardinality. +1 so the caller can still detect "there is a next
	// page" (it trims back to `limit` after scanning).
	args = append(args, limit+1)
	q := fmt.Sprintf(dbq.TopologyAggregateSQLTemplate, whereExtra, cursorClause, len(args))
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]TopologyBucket, 0, 16)
	byName := make(map[string]int, 16)
	for rows.Next() {
		var bucket, readiness string
		var n int64
		if err := rows.Scan(&bucket, &readiness, &n); err != nil {
			return nil, err
		}
		idx, ok := byName[bucket]
		if !ok {
			idx = len(out)
			byName[bucket] = idx
			out = append(out, TopologyBucket{Bucket: bucket, ByReadiness: map[string]int{}})
		}
		out[idx].ByReadiness[readiness] = int(n)
		out[idx].Total += int(n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) CountTopologyBuckets(
	ctx context.Context,
	ownerID uuid.UUID,
	path []TopologyPathSegment,
	nextKey string,
) (int, error) {
	if nextKey == "" {
		return 0, fmt.Errorf("CountTopologyBuckets: nextKey is required")
	}
	args := []any{ownerID, nextKey}
	whereExtra, args, err := buildPathClause(args, path)
	if err != nil {
		return 0, err
	}
	q := fmt.Sprintf(dbq.TopologyCountSQLTemplate, whereExtra)
	var n int64
	if err := s.db.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		return 0, err
	}
	return int(n), nil
}

// TopologyLeaves returns the resource rows at the bottom of the
// hierarchy. Returns the same slim row shape that the sqlc-generated
// list/page queries use: only the columns the leaf graph view needs
// (kind/name/phase/labels). Heavy fields (spec/status/finalizers/
// annotations/shard_id) are intentionally not selected; the detail
// panel re-fetches via GET /api/resources/{id} on click.
func (s *Store) TopologyLeaves(
	ctx context.Context,
	ownerID uuid.UUID,
	path []TopologyPathSegment,
	cursorName string,
	cursorID uuid.UUID,
	limit int,
) ([]dbq.GetChildrenByOwnerRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}

	args := []any{ownerID}
	whereExtra, args, err := buildPathClause(args, path)
	if err != nil {
		return nil, err
	}

	cursorClause := ""
	if cursorName != "" || cursorID != (uuid.UUID{}) {
		args = append(args, cursorName, cursorID)
		cursorClause = fmt.Sprintf(
			" AND (name, id) > ($%d, $%d)",
			len(args)-1, len(args),
		)
	}

	args = append(args, limit)
	q := fmt.Sprintf(dbq.TopologyLeavesSQLTemplate, whereExtra, cursorClause, len(args))

	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []dbq.GetChildrenByOwnerRow{}
	for rows.Next() {
		var r dbq.GetChildrenByOwnerRow
		// Scan order matches dbq.TopologyLeavesSQLTemplate columns.
		if err := rows.Scan(
			&r.ID, &r.Kind, &r.Name, &r.OwnerID,
			&r.Generation, &r.SyncedGen,
			&r.IsReady, &r.Phase,
			&r.DeletionRequestedAt,
			&r.Labels,
			&r.CreatedAt, &r.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// TopologyLabelKeys returns the union of label keys actually present
// on resources owned by ownerID, capped at `limit`.
func (s *Store) TopologyLabelKeys(
	ctx context.Context,
	ownerID uuid.UUID,
	limit int,
) ([]string, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	keys, err := s.queries().TopologyLabelKeys(ctx, dbq.TopologyLabelKeysParams{
		OwnerID: toUUID(ownerID),
		Limit:   int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k != nil {
			out = append(out, *k)
		}
	}
	return out, nil
}
