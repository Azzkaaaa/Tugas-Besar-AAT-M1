package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const orderedListReadyKey = "ordered_list_v1"

// Revisions, rather than node clocks, order both active and superseded headers.
// Keep an inactive old-position row so a delayed lower revision cannot resurrect
// it after a fire moves to an earlier time. This metadata is bounded per revision
// and contains no detection arrays or other large bodies.
func addOrderedListProjection(b *gocql.Batch, h *domain.HazardRecord, bucket, previous time.Time) {
	addOrderedListHeader(b, h, bucket, previous, int64(domain.FullProjectionSize(h)), !domain.Withdrawn(h))
}

func addOrderedListHeader(b *gocql.Batch, h *domain.HazardRecord, bucket, previous time.Time, bytes int64, active bool) {
	for _, scope := range []string{scopeAll, h.Source} {
		if !previous.IsZero() && !previous.Equal(h.OccurredAt) {
			b.Query(`INSERT INTO list_headers_by_shard (scope,shard,occurred_at,hazard_id,active,revision)
                VALUES (?,?,?,?,?,?) USING TIMESTAMP ?`, scope, projectionShard(h.HazardID), previous, h.HazardID, false, h.Revision, int64(max(1, h.Revision)))
		}
		b.Query(`INSERT INTO list_headers_by_shard (scope,shard,occurred_at,hazard_id,source,hazard_type,severity,area_name,ingested_at,revision,bucket,full_bytes,active)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) USING TIMESTAMP ?`, scope, projectionShard(h.HazardID), h.OccurredAt, h.HazardID,
			h.Source, h.HazardType, h.Severity, h.AreaName, h.IngestedAt, h.Revision, bucket, bytes, active, int64(max(1, h.Revision)))
	}
}

func (db *DB) orderedListReady(ctx context.Context) bool {
	if db.listIndexReady.Load() {
		return true
	}
	var state string
	if err := db.session.Query(`SELECT value FROM store_meta WHERE key=?`, orderedListReadyKey).WithContext(ctx).Scan(&state); err == nil && state == "ready" {
		db.listIndexReady.Store(true)
		return true
	}
	return false
}

func (s hazards) listOrdered(ctx context.Context, q storage.HazardQuery, page storage.PageRequest, cursor *pageCursor) (storage.HazardPage, error) {
	var sizes sync.Map
	unitCtx, cancel := context.WithTimeout(ctx, readUnitTimeout)
	defer cancel()
	rows, err := readBucketShards(unitCtx, page.Size+1, func(ctx context.Context, shard int) ([]*domain.HazardRecord, error) {
		return s.readOrderedShard(ctx, q, cursor, shard, page.Size+1, &sizes)
	})
	out := storage.HazardPage{Items: rows}
	if err != nil {
		out.Partial = true
		var failed *shardFailure
		if errors.As(err, &failed) {
			for _, n := range failed.shards {
				out.UnavailableUnits = append(out.UnavailableUnits, fmt.Sprintf("list-shard-%02d", n))
			}
		} else {
			out.UnavailableUnits = []string{"list-index"}
		}
	}
	if len(out.Items) > page.Size {
		out.NextToken = encodeCursor(out.Items[page.Size-1])
		out.Items = out.Items[:page.Size]
	}
	if out.Partial {
		out.NextToken = page.Token
		if out.NextToken == "" {
			out.NextToken = restartCursor()
		}
	}
	return s.loadListPage(ctx, q, page, out, &sizes)
}

func (s hazards) readOrderedShard(ctx context.Context, q storage.HazardQuery, cursor *pageCursor, shard, want int, sizes *sync.Map) ([]*domain.HazardRecord, error) {
	scope := q.Source
	if scope == "" {
		scope = scopeAll
	}
	stmt := `SELECT hazard_id,source,hazard_type,severity,area_name,occurred_at,ingested_at,revision,bucket,full_bytes,active FROM list_headers_by_shard WHERE scope=? AND shard=?`
	args := []any{scope, shard}
	until := msUTC(q.Until)
	if !q.Until.IsZero() && !q.Until.Equal(until) {
		until = until.Add(time.Millisecond)
	}
	switch {
	case cursor != nil && (until.IsZero() || cursor.OccurredAt.Before(until)):
		stmt += ` AND occurred_at<=?`
		args = append(args, cursor.OccurredAt)
	case !until.IsZero():
		stmt += ` AND occurred_at<?`
		args = append(args, until)
	}
	if !q.Since.IsZero() {
		stmt += ` AND occurred_at>=?`
		args = append(args, msUTC(q.Since))
	}
	iter := s.db.session.Query(stmt, args...).WithContext(ctx).PageSize(64).Iter()
	var out []*domain.HazardRecord
	for len(out) < want {
		var h domain.HazardRecord
		var bucket time.Time
		var bytes int64
		var active bool
		if !iter.Scan(&h.HazardID, &h.Source, &h.HazardType, &h.Severity, &h.AreaName, &h.OccurredAt, &h.IngestedAt, &h.Revision, &bucket, &bytes, &active) {
			break
		}
		if !active {
			continue
		}
		h.OccurredAt = h.OccurredAt.UTC()
		h.IngestedAt = h.IngestedAt.UTC()
		if cursor != nil && h.OccurredAt.Equal(cursor.OccurredAt) && h.HazardID <= cursor.HazardID {
			continue
		}
		if !storage.MatchesHazardQuery(&h, q) {
			continue
		}
		sizes.Store(&h, listMetadata{bytes: int(bytes), bucket: bucket.UTC()})
		out = append(out, &h)
	}
	return out, iter.Close()
}

// PrepareOrderedList is an explicit post-upgrade cutover: ALL writers must first
// run migration 19's dual-writing build. Existing readers continue using the old
// index until this succeeds. Canonical revisions make the backfill retryable and
// prevent it from overwriting concurrent newer mutations. No canonical data is
// deleted or changed. Warm rows copy metadata only. Legacy body fallbacks share
// the existing single-body admission slot. Four workers bound network latency
// without retaining a catalog of payloads in memory.
func (db *DB) PrepareOrderedList(ctx context.Context) error {
	return db.PrepareOrderedListWith(ctx, OrderedListOptions{})
}

// OrderedListOptions tunes the maintenance backfill. Zero values keep the
// conservative defaults; Workers is clamped to [1, MaxOrderedListWorkers].
type OrderedListOptions struct {
	Workers int
	// Progress, when set, is called periodically and at the end of each phase
	// with the number of rows scanned so far in that phase.
	Progress func(phase string, rows int64, done bool)
}

const (
	defaultOrderedListWorkers = 4
	MaxOrderedListWorkers     = 32
	orderedProgressEvery      = 1000
)

func (o OrderedListOptions) workers() int {
	switch {
	case o.Workers <= 0:
		return defaultOrderedListWorkers
	case o.Workers > MaxOrderedListWorkers:
		return MaxOrderedListWorkers
	}
	return o.Workers
}

func (o OrderedListOptions) report(phase string, rows int64, done bool) {
	if o.Progress != nil && (done || rows%orderedProgressEvery == 0) {
		o.Progress(phase, rows, done)
	}
}

// PrepareOrderedListWith is PrepareOrderedList with explicit concurrency and
// progress reporting. Every write is idempotent, so a rerun is always safe.
func (db *DB) PrepareOrderedListWith(ctx context.Context, opts OrderedListOptions) error {
	var scanned int64
	err := orderedWork(ctx, opts.workers(), func(work context.Context, submit func(func(context.Context) error) error) error {
		iter := db.session.Query(`SELECT hazard_id,source,hazard_type,severity,area_name,occurred_at,ingested_at,revision,bucket,prev_occurred_at,summary_active,summary_revision FROM canonical_events`).WithContext(work).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 3}).Idempotent(true).PageSize(64).Iter()
		defer iter.Close()
		for {
			var h domain.HazardRecord
			var bucket, previous time.Time
			var active *bool
			var cachedRevision *int
			if !iter.Scan(&h.HazardID, &h.Source, &h.HazardType, &h.Severity, &h.AreaName, &h.OccurredAt, &h.IngestedAt, &h.Revision, &bucket, &previous, &active, &cachedRevision) {
				break
			}
			scanned++
			opts.report("headers", scanned, false)
			if h.HazardID == "" || criticalInternalKey(h.HazardID) {
				continue
			}
			if err := submit(func(work context.Context) error {
				bytes := int64(0) // Unknown size is safe: full reads enforce their actual byte budget.
				if active == nil || cachedRevision == nil || *cachedRevision != h.Revision {
					st, err := db.orderedLegacy(work, h.HazardID)
					if err != nil {
						return err
					}
					h = *st.record
					bucket = st.bucket
					previous = st.previous
					value := st.active
					active = &value
					bytes = st.fullBytes
				}
				batch := db.session.Batch(gocql.LoggedBatch).WithContext(work)
				addOrderedListHeader(batch, &h, bucket, previous, bytes, *active)
				return batch.Exec()
			}); err != nil {
				return err
			}
		}
		return iter.Close()
	})
	if err != nil {
		return err
	}
	opts.report("headers", scanned, true)
	scanned = 0
	// Remove stale positions created before every writer was upgraded. Reading
	// the current canonical revision AFTER an index row makes concurrent updates
	// safe: a subsequent revision always wins this inactive marker.
	err = orderedWork(ctx, opts.workers(), func(work context.Context, submit func(func(context.Context) error) error) error {
		rows := db.session.Query(`SELECT scope,shard,occurred_at,hazard_id FROM list_headers_by_shard`).WithContext(work).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 3}).Idempotent(true).PageSize(64).Iter()
		defer rows.Close()
		for {
			var scope, id string
			var shard int
			var occurred time.Time
			if !rows.Scan(&scope, &shard, &occurred, &id) {
				break
			}
			scanned++
			opts.report("stale-positions", scanned, false)
			if err := submit(func(work context.Context) error {
				var current time.Time
				var revision int
				var active *bool
				var cachedRevision *int
				if err := db.session.Query(`SELECT occurred_at,revision,summary_active,summary_revision FROM canonical_events WHERE shard=? AND hazard_id=?`, auditShard(id), id).WithContext(work).Scan(&current, &revision, &active, &cachedRevision); err != nil {
					return err
				}
				if active == nil || cachedRevision == nil || *cachedRevision != revision {
					st, err := db.orderedLegacy(work, id)
					if err != nil {
						return err
					}
					value := st.active
					active = &value
					current = st.record.OccurredAt
					revision = st.record.Revision
				}
				if *active && current.Equal(occurred) {
					return nil
				}
				return db.session.Query(`INSERT INTO list_headers_by_shard (scope,shard,occurred_at,hazard_id,active,revision) VALUES (?,?,?,?,?,?) USING TIMESTAMP ?`, scope, shard, occurred, id, false, revision, int64(max(1, revision))).WithContext(work).Exec()
			}); err != nil {
				return err
			}
		}
		return rows.Close()
	})
	if err != nil {
		return err
	}
	opts.report("stale-positions", scanned, true)
	if err := orderedRetry(ctx, func(work context.Context) error {
		return db.session.Query(`INSERT INTO store_meta (key,value) VALUES (?,?)`, orderedListReadyKey, "ready").WithContext(work).Exec()
	}); err != nil {
		return err
	}
	db.listIndexReady.Store(true)
	return nil
}

type orderedLegacyHeader struct {
	record           *domain.HazardRecord
	bucket, previous time.Time
	active           bool
	fullBytes        int64
}

func (db *DB) orderedLegacy(ctx context.Context, id string) (*orderedLegacyHeader, error) {
	release, err := db.acquireSummaryFallback(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	st, err := (hazards{db}).readStoredTable(ctx, id, false, false)
	if err == nil && st == nil {
		err = fmt.Errorf("canonical row missing: %s", id)
	}
	if err != nil {
		return nil, err
	}
	return &orderedLegacyHeader{record: storage.HazardSummaryHeader(st.rec), bucket: st.bucket, previous: st.prevOccurredAt, active: !domain.Withdrawn(st.rec), fullBytes: int64(domain.FullProjectionSize(st.rec))}, nil
}

func orderedWork(ctx context.Context, workerCount int, produce func(context.Context, func(func(context.Context) error) error) error) error {
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan func(context.Context) error, 4*workerCount)
	var once sync.Once
	var first error
	var workers sync.WaitGroup
	fail := func(err error) { once.Do(func() { first = err; cancel() }) }
	for range workerCount {
		workers.Go(func() {
			for job := range jobs {
				if work.Err() != nil {
					return
				}
				if err := orderedRetry(work, job); err != nil {
					fail(err)
					return
				}
			}
		})
	}
	err := produce(work, func(job func(context.Context) error) error {
		select {
		case jobs <- job:
			return nil
		case <-work.Done():
			return work.Err()
		}
	})
	if err != nil {
		fail(err)
	}
	close(jobs)
	workers.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}

// These maintenance writes use canonical logical timestamps, so an uncertain
// quorum result can be repeated without overwriting a newer revision. Never
// extend the caller deadline or enable retries for ordinary fenced writes here.
func orderedRetry(ctx context.Context, job func(context.Context) error) error {
	for attempt := 0; ; attempt++ {
		err := job(ctx)
		if err == nil || ctx.Err() != nil || attempt == 3 {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
