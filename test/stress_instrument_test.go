package test

// Instrumentation for the stress tests, built to answer ONE question:
// when 200 workers are slower than 20 workers at the same total
// in-flight parallelism, WHERE does the extra time go?
//
// Two competing hypotheses:
//
//	(A) client-side: more workers => more claim round-trips / pool
//	    starvation. Signal: pgxpool EmptyAcquireCount + AcquireDuration
//	    rise with worker count.
//	(B) server-side: more concurrent writers contend on the hot tables
//	    (work_queue, work_outbox). Signal: pg_stat_activity wait events
//	    shift toward Lock / LWLock (lock_manager, buffer_content) /
//	    BufferPin, and per-statement mean_ms on the claim/insert paths
//	    rises.
//
// We sample both so a single run tells us which hypothesis holds.

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// waitEventSampler polls pg_stat_activity on a fixed cadence and
// accumulates a histogram of (wait_event_type / wait_event) across all
// non-idle backends. The histogram counts SAMPLES, not time — but at a
// fixed interval, sample-count is proportional to time-spent-waiting,
// which is exactly what we want for an apples-to-apples comparison
// between two runs sampled the same way.
type waitEventSampler struct {
	mu      sync.Mutex
	hist    map[string]int64 // "type/event" -> sample count
	samples int64            // total sampling passes (for normalisation)

	// blocked aggregates, per sampling pass, how many backends were BLOCKED
	// waiting on a lock held by another backend (pg_blocking_pids non-empty),
	// keyed by the normalized query text of the blocked statement. This
	// pinpoints WHICH statements are losing time to lock contention (the
	// "locks of locks" symptom) — distinct from the wait-event histogram,
	// which only says a Lock/LWLock wait happened, not on which query.
	blocked map[string]int64
}

func newWaitEventSampler() *waitEventSampler {
	return &waitEventSampler{hist: make(map[string]int64), blocked: make(map[string]int64)}
}

// start launches the sampler goroutine. It samples every `every` until
// ctx is cancelled. Uses its own short-lived connection from `pool` so
// it doesn't perturb the worker pools' connection budgets.
func (s *waitEventSampler) start(ctx context.Context, pool *pgxpool.Pool, every time.Duration) {
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.sampleOnce(ctx, pool)
			}
		}
	}()
}

func (s *waitEventSampler) sampleOnce(ctx context.Context, pool *pgxpool.Pool) {
	// Active backends only (state='active'); a NULL wait_event means
	// the backend is on-CPU running, which we bucket as "Running/CPU"
	// so we can see compute vs wait balance. Exclude our own sampling
	// query and any idle connections.
	rows, err := pool.Query(ctx, `
		SELECT
		    COALESCE(wait_event_type, 'Running') AS et,
		    COALESCE(wait_event, 'CPU')          AS ev,
		    count(*)
		FROM pg_stat_activity
		WHERE state = 'active'
		  AND backend_type = 'client backend'
		  AND query NOT LIKE '%pg_stat_activity%'
		GROUP BY 1, 2
	`)
	if err != nil {
		return
	}
	defer rows.Close()
	s.mu.Lock()
	s.samples++
	for rows.Next() {
		var et, ev string
		var c int64
		if err := rows.Scan(&et, &ev, &c); err != nil {
			continue
		}
		s.hist[et+"/"+ev] += c
	}
	s.mu.Unlock()

	// Blocked-by-lock snapshot: which active statements are waiting on a lock
	// held by another backend right now. pg_blocking_pids() returns the PIDs
	// blocking each backend; a non-empty array means real lock contention.
	brows, err := pool.Query(ctx, `
		SELECT left(regexp_replace(query, E'\\s+', ' ', 'g'), 80) AS q, count(*)
		FROM pg_stat_activity
		WHERE state = 'active'
		  AND backend_type = 'client backend'
		  AND cardinality(pg_blocking_pids(pid)) > 0
		  AND query NOT LIKE '%pg_stat_activity%'
		GROUP BY 1
	`)
	if err != nil {
		return
	}
	defer brows.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for brows.Next() {
		var q string
		var c int64
		if err := brows.Scan(&q, &c); err != nil {
			continue
		}
		s.blocked[q] += c
	}
}

// report logs the histogram sorted by descending sample count. The
// "avg concurrent" column = total samples of that wait / number of
// sampling passes = the mean number of backends in that state at any
// instant — directly comparable between the 20- and 200-worker runs.
func (s *waitEventSampler) report(t *testing.T, label string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	type row struct {
		key   string
		count int64
	}
	rows := make([]row, 0, len(s.hist))
	var total int64
	for k, c := range s.hist {
		rows = append(rows, row{k, c})
		total += c
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].count > rows[j].count })

	passes := s.samples
	if passes == 0 {
		passes = 1
	}
	t.Logf("=== wait-event histogram [%s] (%d sampling passes) ===", label, passes)
	t.Logf("%-34s %-12s %-14s %s", "wait_event_type/event", "samples", "avg_concurrent", "share")
	for _, r := range rows {
		avg := float64(r.count) / float64(passes)
		share := 100 * float64(r.count) / float64(total)
		t.Logf("%-34s %-12d %-14.2f %.1f%%", r.key, r.count, avg, share)
	}

	// Blocked-by-lock attribution: avg_concurrent here = mean number of
	// backends BLOCKED on a lock (held by another backend) at any instant,
	// per blocked statement. A high value is the "locks of locks" smoking gun.
	brows := make([]row, 0, len(s.blocked))
	for k, c := range s.blocked {
		brows = append(brows, row{k, c})
	}
	sort.Slice(brows, func(i, j int) bool { return brows[i].count > brows[j].count })
	t.Logf("=== blocked-on-lock by statement [%s] ===", label)
	if len(brows) == 0 {
		t.Logf("(none observed — no backend was ever waiting on a lock held by another)")
	}
	for _, r := range brows {
		t.Logf("blocked avg_concurrent=%-6.2f samples=%-8d  %s", float64(r.count)/float64(passes), r.count, r.key)
	}
}

// logPoolStats dumps the client-side pool counters for hypothesis (A).
// EmptyAcquireCount > 0 means goroutines blocked waiting for a free
// connection — i.e. the pool was a bottleneck. AcquireDuration is the
// cumulative time spent inside Acquire (includes establishing new
// conns). If both are near-zero, client-side round-trips / pool
// starvation is NOT the cause and hypothesis (B) is favoured.
func logPoolStats(t *testing.T, label string, pools []*pgxpool.Pool) {
	t.Helper()
	var (
		acquired     int64
		emptyAcquire int64
		canceled     int64
		acquireDur   time.Duration
		newConns     int64
		maxTotal     int32
		curTotal     int32
	)
	for _, p := range pools {
		st := p.Stat()
		acquired += st.AcquireCount()
		emptyAcquire += st.EmptyAcquireCount()
		canceled += st.CanceledAcquireCount()
		acquireDur += st.AcquireDuration()
		newConns += st.NewConnsCount()
		maxTotal += st.MaxConns()
		curTotal += st.TotalConns()
	}
	var meanAcquireUs float64
	if acquired > 0 {
		meanAcquireUs = float64(acquireDur.Microseconds()) / float64(acquired)
	}
	t.Logf("=== pool stats [%s] across %d pools ===", label, len(pools))
	t.Logf("acquires=%d empty_acquires=%d canceled=%d new_conns=%d",
		acquired, emptyAcquire, canceled, newConns)
	t.Logf("mean_acquire=%.1fus total_acquire_wait=%v cur_conns=%d max_conns=%d",
		meanAcquireUs, acquireDur.Round(time.Millisecond), curTotal, maxTotal)
	if emptyAcquire == 0 {
		t.Logf("-> NO empty acquires: pool was never a bottleneck (favours hypothesis B: server-side contention)")
	} else {
		pct := 100 * float64(emptyAcquire) / float64(acquired)
		t.Logf("-> %.2f%% of acquires blocked on an empty pool (hypothesis A: client/pool starvation in play)", pct)
	}
}
