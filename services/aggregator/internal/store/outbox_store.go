package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	"github.com/jackc/pgx/v5"
)

type OutboxRecord struct {
	EventID    string
	HazardID   string
	Source     string
	Payload    []byte
	CreatedAt  time.Time
	RetryCount int
	// TraceContext is the W3C carrier of the span that enqueued the event; the publisher
	// continues that trace, so consumers join the ingest poll that found the hazard.
	TraceContext map[string]string
}

// EnqueueOutbox stores the event together with ctx's trace context.
func (db *DB) EnqueueOutbox(ctx context.Context, tx pgx.Tx, eventID, hazardID, source string, payload []byte) error {
	query := `
		INSERT INTO outbox_events (event_id, hazard_id, source, payload, trace_context, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (event_id) DO NOTHING
	`
	traceContext, err := encodeTraceContext(telemetry.TraceCarrier(ctx))
	if err != nil {
		return err
	}
	if tx != nil {
		_, err = tx.Exec(ctx, query, eventID, hazardID, source, payload, traceContext)
	} else {
		_, err = db.Pool.Exec(ctx, query, eventID, hazardID, source, payload, traceContext)
	}
	return err
}

// encodeTraceContext returns the carrier as JSON, or nil (SQL NULL) when there is no trace.
func encodeTraceContext(carrier map[string]string) ([]byte, error) {
	if len(carrier) == 0 {
		return nil, nil
	}
	return json.Marshal(carrier)
}

// decodeTraceContext ignores malformed values: a lost trace link must never block publishing.
func decodeTraceContext(raw []byte) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	var carrier map[string]string
	if err := json.Unmarshal(raw, &carrier); err != nil {
		return nil
	}
	return carrier
}

func (db *DB) FetchUnpublishedOutbox(ctx context.Context, limit int) ([]*OutboxRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
		SELECT event_id, hazard_id, source, payload, created_at, retry_count, trace_context
		FROM outbox_events
		WHERE published_at IS NULL AND failed_at IS NULL
		ORDER BY created_at ASC
		LIMIT $1
	`
	rows, err := db.Pool.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*OutboxRecord
	for rows.Next() {
		var rec OutboxRecord
		var traceContext []byte
		if err := rows.Scan(&rec.EventID, &rec.HazardID, &rec.Source, &rec.Payload, &rec.CreatedAt, &rec.RetryCount, &traceContext); err != nil {
			return nil, err
		}
		rec.TraceContext = decodeTraceContext(traceContext)
		records = append(records, &rec)
	}
	return records, nil
}

func (db *DB) MarkOutboxPublished(ctx context.Context, eventID string) error {
	query := `
		UPDATE outbox_events
		SET published_at = NOW()
		WHERE event_id = $1
	`
	_, err := db.Pool.Exec(ctx, query, eventID)
	return err
}

func (db *DB) IncrementOutboxRetry(ctx context.Context, eventID, lastErr string) error {
	query := `
		UPDATE outbox_events
		SET retry_count = retry_count + 1, last_error = $2
		WHERE event_id = $1
	`
	_, err := db.Pool.Exec(ctx, query, eventID, lastErr)
	return err
}

func (db *DB) FailOutboxRecord(ctx context.Context, rec *OutboxRecord, reason string) error {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		UPDATE outbox_events
		SET failed_at = NOW(), last_error = $2
		WHERE event_id = $1
	`, rec.EventID, reason)
	if err != nil {
		return err
	}

	traceContext, err := encodeTraceContext(rec.TraceContext)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_dead_letters (event_id, hazard_id, source, payload, created_at, failed_at, last_error, trace_context)
		VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7)
		ON CONFLICT (event_id) DO NOTHING
	`, rec.EventID, rec.HazardID, rec.Source, rec.Payload, rec.CreatedAt, reason, traceContext)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

