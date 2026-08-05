package engine

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/shardutil"
)

// verifyShardModulus confirms the schema's shard hash divisor equals
// shardutil.NumShards, fail-fasting on drift between schema and Go. Called once
// at control-plane boot (NewControlPlane).
//
// Two hash sources exist and must agree:
//   - resources.shard_id is a GENERATED column whose expression embeds
//     the modulus (read via pg_get_expr).
//   - work_queue / work_outbox are RANGE-partitioned by a PLAIN shard_id
//     column (a partition key can't be generated); they're populated by
//     the shard_of() SQL function, whose body embeds the same modulus
//     (read via pg_get_functiondef). We check that function too so the
//     partitioned tables' hash can't silently drift.
func verifyShardModulus(ctx context.Context, pool *pgxpool.Pool) error {
	// Both hash sources have the shape `abs(mod(hashtext(...), N))` — the
	// generated-column expr is `abs(mod(hashtext((id)::text), 256))` and the
	// shard_of() body is `... abs(mod(hashtext(rid::text), 256))::smallint`.
	// Anchor on the `mod(hashtext(...), N)` call so we always capture the
	// SHARD modulus and never some unrelated mod() that drift might add. The
	// `[^,]*` between hashtext's close-paren and the divisor tolerates the
	// inner casts / whitespace pg_get_expr / pg_get_functiondef emit across
	// PG versions. (abs() is the outer call now — mod-first avoids the
	// abs(INT_MIN) overflow; see shard_of in 00001_schema.sql.)
	modRe := regexp.MustCompile(`mod\(hashtext\([^)]*\)[^,]*,\s*(\d+)\s*\)`)

	check := func(label, expr string) error {
		m := modRe.FindStringSubmatch(expr)
		if m == nil {
			return fmt.Errorf("%s expression %q has no mod(hashtext(...), N); update verifyShardModulus", label, expr)
		}
		got, err := strconv.Atoi(m[1])
		if err != nil {
			return fmt.Errorf("%s modulus parse %q: %w", label, m[1], err)
		}
		if got != shardutil.NumShards {
			return fmt.Errorf("%s modulus %d != shardutil.NumShards %d — schema and Go are out of sync", label, got, shardutil.NumShards)
		}
		return nil
	}

	// These two reads are PG CATALOG INTROSPECTION (pg_get_expr / pg_get_functiondef)
	// for a boot-time schema-integrity assert — NOT data-layer queries, so they stay
	// inline here rather than in the db/queries + dbq query layer (which is for the
	// application's resource/work_queue/… queries). sqlc can't model catalog reads.

	// resources.shard_id GENERATED expression.
	var genExpr string
	const genQ = `
		SELECT pg_get_expr(adbin, adrelid)
		FROM pg_attribute a
		JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		WHERE a.attrelid = 'resources'::regclass AND a.attname = 'shard_id'`
	if err := pool.QueryRow(ctx, genQ).Scan(&genExpr); err != nil {
		return fmt.Errorf("read resources.shard_id default: %w", err)
	}
	if err := check("resources.shard_id", genExpr); err != nil {
		return err
	}

	// shard_of() function body (drives work_queue / work_outbox shard_id). Same
	// check() as the generated column — both hash sources must agree on the modulus.
	var fnDef string
	if err := pool.QueryRow(ctx, `SELECT pg_get_functiondef('shard_of(uuid)'::regprocedure)`).Scan(&fnDef); err != nil {
		return fmt.Errorf("read shard_of() definition: %w", err)
	}
	return check("shard_of()", fnDef)
}
