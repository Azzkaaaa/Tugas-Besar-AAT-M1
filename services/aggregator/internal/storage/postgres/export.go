package postgres

import (
	"context"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

// Export functions read what cmd/storecopy copies besides hazards (which it reads through List).
// They are not part of the storage ports.

// SourceStateRecord is one ingest stream's state.
type SourceStateRecord struct {
	Key   domain.SourceKey
	State storage.SourceState
}

// DeadLetter is a dead-lettered outbox event.
type DeadLetter struct {
	Event     storage.ClaimedEvent
	FailedAt  time.Time
	LastError string
}

func (db *DB) ExportSourceStates(ctx context.Context) ([]SourceStateRecord, error) {
	rows, err := db.Pool.Query(ctx, `SELECT source_key FROM ingest_watermarks ORDER BY source_key`)
	if err != nil {
		return nil, err
	}
	var keys []domain.SourceKey
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return nil, err
		}
		keys = append(keys, domain.SourceKey(k))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []SourceStateRecord
	for _, k := range keys {
		st, err := sources{db}.Get(ctx, k)
		if err != nil {
			return nil, err
		}
		out = append(out, SourceStateRecord{Key: k, State: st})
	}
	return out, nil
}

// ExportPendingEvents returns the outbox events not yet published or dead-lettered, oldest first.
func (db *DB) ExportPendingEvents(ctx context.Context) ([]*storage.ClaimedEvent, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT event_id, hazard_id, source, payload, created_at, retry_count, trace_context
		FROM outbox_events WHERE published_at IS NULL AND failed_at IS NULL
		ORDER BY created_at ASC, event_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*storage.ClaimedEvent
	for rows.Next() {
		var ev storage.ClaimedEvent
		var traceContext []byte
		if err := rows.Scan(&ev.EventID, &ev.HazardID, &ev.Source, &ev.Payload, &ev.CreatedAt, &ev.RetryCount, &traceContext); err != nil {
			return nil, err
		}
		ev.CreatedAt = ev.CreatedAt.UTC()
		ev.TraceContext = decodeTraceContext(traceContext)
		out = append(out, &ev)
	}
	return out, rows.Err()
}

func (db *DB) ExportDeadLetters(ctx context.Context) ([]DeadLetter, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT event_id, hazard_id, source, payload, created_at, failed_at, COALESCE(last_error, ''), trace_context
		FROM outbox_dead_letters ORDER BY created_at ASC, event_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeadLetter
	for rows.Next() {
		var dl DeadLetter
		var traceContext []byte
		if err := rows.Scan(&dl.Event.EventID, &dl.Event.HazardID, &dl.Event.Source, &dl.Event.Payload,
			&dl.Event.CreatedAt, &dl.FailedAt, &dl.LastError, &traceContext); err != nil {
			return nil, err
		}
		dl.Event.CreatedAt = dl.Event.CreatedAt.UTC()
		dl.FailedAt = dl.FailedAt.UTC()
		dl.Event.TraceContext = decodeTraceContext(traceContext)
		out = append(out, dl)
	}
	return out, rows.Err()
}
