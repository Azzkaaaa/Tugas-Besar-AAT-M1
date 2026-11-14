package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// The Import functions write records as they are, for cmd/storecopy. Unlike ApplyRevision they
// emit no outbox event, so they are not part of the storage ports. Each is idempotent.

// ErrIdentityConflict is returned by ImportHazard when (source, source_ref_id) already belongs to
// another hazard_id in Cassandra.
var ErrIdentityConflict = fmt.Errorf("source reference already mapped to another hazard")

// ImportHazard stores h with its own hazard_id and ingested_at.
func (db *DB) ImportHazard(ctx context.Context, rec *domain.HazardRecord) error {
	h := *rec
	h.Attributes = domain.NormalizeAttributes(rec.Attributes)
	h.OccurredAt = msUTC(rec.OccurredAt)
	h.IngestedAt = msUTC(rec.IngestedAt)

	existing := map[string]any{}
	applied, err := db.session.Query(`INSERT INTO hazard_by_source_ref (source, source_ref_id, hazard_id, ingested_at)
		VALUES (?, ?, ?, ?) IF NOT EXISTS`, h.Source, h.SourceRefID, h.HazardID, h.IngestedAt).WithContext(ctx).MapScanCAS(existing)
	if err != nil {
		return err
	}
	if !applied {
		if id, _ := existing["hazard_id"].(string); id != h.HazardID {
			return fmt.Errorf("%w: %s/%s is %s, not %s", ErrIdentityConflict, h.Source, h.SourceRefID, id, h.HazardID)
		}
	}

	s := hazards{db}
	current, oldBucket, err := s.getStored(ctx, h.HazardID)
	if err != nil {
		return err
	}
	attrJSON, err := json.Marshal(h.Attributes)
	if err != nil {
		return err
	}
	b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	s.addHazardRows(b, &h, current, oldBucket, string(attrJSON))
	return b.Exec()
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
		pendingShard, createdAt, ev.EventID, ev.HazardID, ev.Source, ev.Payload, traceContext, ev.RetryCount, msUTC(time.Now()))
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
