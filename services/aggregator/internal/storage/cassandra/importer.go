package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// The Import functions write records as they are, for cmd/storecopy. Unlike ApplyRevision they
// emit no outbox event, so they are not part of the storage ports. Each is idempotent.

// ImportHazard stores rec at its own revision and ingested_at, under its derived hazard ID (the
// source's hazard_id is ignored; storecopy reads from an already re-keyed Postgres). The revision
// must be carried over: resetting it would reuse event IDs consumers have seen.
func (db *DB) ImportHazard(ctx context.Context, rec *domain.HazardRecord) error {
	h := *rec
	h.HazardID = domain.HazardID(rec.Source, rec.SourceRefID)
	h.Attributes = domain.NormalizeAttributes(rec.Attributes)
	h.OccurredAt = msUTC(rec.OccurredAt)
	h.IngestedAt = msUTC(rec.IngestedAt)
	if h.Revision < 1 {
		h.Revision = 1
	}
	hash, err := contentHash(&h)
	if err != nil {
		return err
	}
	s := hazards{db}
	bucket := db.bucketOf(h.OccurredAt)

	st, err := s.readStored(ctx, h.HazardID, true)
	if err != nil {
		return err
	}
	if st != nil && st.legacy {
		if err := s.adoptLegacy(ctx, st); err != nil {
			return err
		}
		st, err = s.readStored(ctx, h.HazardID, true)
		if err != nil {
			return err
		}
	}
	if st == nil {
		applied, err := s.insertStored(ctx, &h, bucket, hash, time.Time{}, time.Time{}, nil)
		if err != nil {
			return err
		}
		if !applied {
			return fmt.Errorf("hazard %s was created while importing it", h.HazardID)
		}
		_, _, err = s.writeOut(ctx, &h, bucket, time.Time{}, time.Time{}, nil)
		return err
	}
	if st.rec.Revision > h.Revision {
		return nil
	}
	if st.rec.Revision == h.Revision {
		if st.hash != hash {
			return fmt.Errorf("hazard %s changes content without a new revision", h.HazardID)
		}
		if err := db.ensureCritical(ctx, st.rec); err != nil {
			return err
		}
		_, _, err = s.writeOut(ctx, st.rec, st.bucket, st.prevBucket, st.prevOccurredAt, nil)
		return err
	}
	// Re-run of the copy: bring the row to the copied revision, moving its List row if needed.
	b := db.guardBatch(ctx, canonicalGuard(h.HazardID))
	b.Query(`UPDATE canonical_events SET
			hazard_type = ?, severity = ?, area_name = ?, latitude = ?, longitude = ?, occurred_at = ?,
			ingested_at = ?, attributes = ?, revision = ?, bucket = ?, content_hash = ?, prev_bucket = ?, prev_occurred_at = ?,
			summary_active = ?, summary_revision = ?
		WHERE shard=? AND hazard_id = ? IF revision = ?`,
		h.HazardType, h.Severity, h.AreaName, h.Latitude, h.Longitude, h.OccurredAt,
		h.IngestedAt, mustJSON(h.Attributes), h.Revision, bucket, hash, st.bucket, st.rec.OccurredAt,
		!domain.Withdrawn(&h), h.Revision,
		auditShard(h.HazardID), h.HazardID, st.rec.Revision)
	if err := db.addCriticalEvent(ctx, b, &h, nil); err != nil {
		return err
	}
	if applied, err := db.guardedCAS(ctx, canonicalGuard(h.HazardID), b); err != nil {
		return err
	} else if !applied {
		return fmt.Errorf("hazard %s changed while importing it", h.HazardID)
	}
	_, _, err = s.writeOut(ctx, &h, bucket, st.bucket, st.rec.OccurredAt, nil)
	return err
}

func (db *DB) ImportSourceState(ctx context.Context, key domain.SourceKey, st storage.SourceState) error {
	var errMsg any
	if st.ErrorMessage != "" {
		errMsg = st.ErrorMessage
	}
	return db.session.Query(`INSERT INTO source_state
			(source_key, watermark_time, last_success_at, last_attempt_at, status, error_message)
		VALUES (?, ?, ?, ?, ?, ?)`,
		string(key), nullableTime(st.Watermark), nullableTime(st.LastSuccessAt), nullableTime(st.LastAttemptAt),
		st.Status, errMsg).WithContext(ctx).Exec()
}

// ImportPendingEvent enqueues ev with its original creation time and retry count, due at once.
func (db *DB) ImportPendingEvent(ctx context.Context, ev *storage.ClaimedEvent) error {
	createdAt := msUTC(ev.CreatedAt)
	traceContext := encodeTraceContext(ev.TraceContext)
	b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	b.Query(`INSERT INTO outbox_events (event_id, hazard_id, source, payload, trace_context, created_at, retry_count)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, ev.EventID, ev.HazardID, ev.Source, ev.Payload, traceContext, createdAt, ev.RetryCount)
	b.Query(`INSERT INTO outbox_pending (shard, created_at, event_id, hazard_id, source, payload, trace_context,
			retry_count, next_attempt_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		pendingPartition(ev.Source), createdAt, ev.EventID, ev.HazardID, ev.Source, ev.Payload, traceContext, ev.RetryCount, msUTC(time.Now()))
	return b.Exec()
}

func (db *DB) ImportDeadLetter(ctx context.Context, ev *storage.ClaimedEvent, failedAt time.Time, lastErr string) error {
	return db.session.Query(`INSERT INTO outbox_dead_letters (event_id, hazard_id, source, payload, trace_context,
			created_at, failed_at, last_error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.EventID, ev.HazardID, ev.Source, ev.Payload, encodeTraceContext(ev.TraceContext),
		msUTC(ev.CreatedAt), msUTC(failedAt), lastErr).WithContext(ctx).Exec()
}

// DropForTest drops the keyspace. Only for tests.
func (db *DB) DropForTest() error {
	return db.session.Query(`DROP KEYSPACE IF EXISTS ` + db.keyspace).Exec()
}

// TruncateForTest empties one table. Only for tests.
func (db *DB) TruncateForTest(table string) error {
	return db.session.Query(`TRUNCATE ` + table).Exec()
}
