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
	for _, scope := range []string{scopeAll, h.Source} {
		if !previous.IsZero() && !previous.Equal(h.OccurredAt) {
			b.Query(`INSERT INTO list_headers_by_shard (scope,shard,occurred_at,hazard_id,active,revision)
                VALUES (?,?,?,?,?,?) USING TIMESTAMP ?`, scope, projectionShard(h.HazardID), previous, h.HazardID, false, h.Revision, int64(max(1, h.Revision)))
		}
		b.Query(`INSERT INTO list_headers_by_shard (scope,shard,occurred_at,hazard_id,source,hazard_type,severity,area_name,ingested_at,revision,bucket,full_bytes,active)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) USING TIMESTAMP ?`, scope, projectionShard(h.HazardID), h.OccurredAt, h.HazardID,
			h.Source, h.HazardType, h.Severity, h.AreaName, h.IngestedAt, h.Revision, bucket, int64(domain.FullProjectionSize(h)), !domain.Withdrawn(h), int64(max(1, h.Revision)))
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
// deleted or changed. Full bodies are held singly, as in the Audit backfill.
func (db *DB) PrepareOrderedList(ctx context.Context) error {
	iter := db.session.Query(`SELECT ` + storedColumns + ` FROM canonical_events`).WithContext(ctx).PageSize(1).Iter()
	scanner := iter.Scanner()
	for scanner.Next() {
		var bucket, prevBucket, previous time.Time
		var hash *string
		h, err := scanHazard(func(dest ...any) error { return scanner.Scan(append(dest, &bucket, &hash, &prevBucket, &previous)...) })
		if err != nil {
			_ = scanner.Err()
			return err
		}
		if h.HazardID == "" || criticalInternalKey(h.HazardID) {
			continue
		}
		b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
		addOrderedListProjection(b, h, bucket, previous)
		if err := b.Exec(); err != nil {
			_ = scanner.Err()
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	// Remove stale positions created before every writer was upgraded. Reading
	// the current canonical revision AFTER an index row makes concurrent updates
	// safe: a subsequent revision always wins this inactive marker.
	rows := db.session.Query(`SELECT scope,shard,occurred_at,hazard_id FROM list_headers_by_shard`).WithContext(ctx).PageSize(64).Iter()
	var scope, id string
	var shard int
	var occurred time.Time
	for rows.Scan(&scope, &shard, &occurred, &id) {
		var current time.Time
		var revision int
		var active *bool
		err := db.session.Query(`SELECT occurred_at,revision,summary_active FROM canonical_events WHERE shard=? AND hazard_id=?`, auditShard(id), id).WithContext(ctx).Scan(&current, &revision, &active)
		if err != nil {
			_ = rows.Close()
			return err
		}
		// Pre-cache canonical rows can have a NULL summary_active. Resolve only
		// those through the existing bounded full-body fallback.
		if active == nil {
			h, e := (hazards{db}).Get(ctx, id)
			if e != nil {
				_ = rows.Close()
				return e
			}
			if h == nil {
				_ = rows.Close()
				return fmt.Errorf("canonical row missing: %s", id)
			}
			value := !domain.Withdrawn(h)
			active = &value
			current = h.OccurredAt
			revision = h.Revision
		}
		if *active && current.Equal(occurred) {
			continue
		}
		if err := db.session.Query(`INSERT INTO list_headers_by_shard (scope,shard,occurred_at,hazard_id,active,revision) VALUES (?,?,?,?,?,?) USING TIMESTAMP ?`, scope, shard, occurred, id, false, revision, int64(max(1, revision))).WithContext(ctx).Exec(); err != nil {
			_ = rows.Close()
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := db.session.Query(`INSERT INTO store_meta (key,value) VALUES (?,?)`, orderedListReadyKey, "ready").WithContext(ctx).Exec(); err != nil {
		return err
	}
	db.listIndexReady.Store(true)
	return nil
}
