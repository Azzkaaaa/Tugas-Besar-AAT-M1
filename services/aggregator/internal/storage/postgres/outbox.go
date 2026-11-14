package postgres

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/jackc/pgx/v5"
)

type outbox struct{ db *DB }

// enqueueOutbox stores ev inside tx (see hazards.ApplyRevision).
func enqueueOutbox(ctx context.Context, tx pgx.Tx, ev storage.OutboxEvent) error {
	traceContext, err := encodeTraceContext(ev.TraceContext)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (event_id, hazard_id, source, payload, trace_context, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (event_id) DO NOTHING
	`, ev.EventID, ev.HazardID, ev.Source, ev.Payload, traceContext)
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

// ClaimDue leases up to limit due records for lease. SKIP LOCKED makes concurrent publishers
// (replicas, or both pods of a rolling update) take disjoint rows; a publisher that dies keeps its
// rows only until the lease runs out. Records come back oldest first.
func (o outbox) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]*storage.ClaimedEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
		UPDATE outbox_events
		SET next_attempt_at = NOW() + $2 * INTERVAL '1 millisecond'
		WHERE event_id IN (
			SELECT event_id FROM outbox_events
			WHERE published_at IS NULL AND failed_at IS NULL AND next_attempt_at <= NOW()
			ORDER BY created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING event_id, hazard_id, source, payload, created_at, retry_count, trace_context
	`
	rows, err := o.db.Pool.Query(ctx, query, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*storage.ClaimedEvent
	for rows.Next() {
		var rec storage.ClaimedEvent
		var traceContext []byte
		if err := rows.Scan(&rec.EventID, &rec.HazardID, &rec.Source, &rec.Payload, &rec.CreatedAt, &rec.RetryCount, &traceContext); err != nil {
			return nil, err
		}
		rec.TraceContext = decodeTraceContext(traceContext)
		rec.CreatedAt = rec.CreatedAt.UTC()
		records = append(records, &rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// RETURNING has no order.
	sort.Slice(records, func(i, j int) bool { return records[i].CreatedAt.Before(records[j].CreatedAt) })
	return records, nil
}

func (o outbox) Ack(ctx context.Context, eventID string) error {
	query := `
		UPDATE outbox_events
		SET published_at = NOW()
		WHERE event_id = $1
	`
	_, err := o.db.Pool.Exec(ctx, query, eventID)
	return err
}

// RetryLater records a failed attempt; the record is due again after backoff, so it does not
// hold up the records behind it.
func (o outbox) RetryLater(ctx context.Context, eventID, lastErr string, backoff time.Duration) error {
	query := `
		UPDATE outbox_events
		SET retry_count = retry_count + 1, last_error = $2, next_attempt_at = NOW() + $3 * INTERVAL '1 millisecond'
		WHERE event_id = $1
	`
	_, err := o.db.Pool.Exec(ctx, query, eventID, lastErr, backoff.Milliseconds())
	return err
}

// Release makes claimed but unattempted records due at once (e.g. after the broker
// connection dropped mid-batch) instead of waiting out their lease.
func (o outbox) Release(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	query := `
		UPDATE outbox_events
		SET next_attempt_at = NOW()
		WHERE event_id = ANY($1) AND published_at IS NULL AND failed_at IS NULL
	`
	_, err := o.db.Pool.Exec(ctx, query, eventIDs)
	return err
}

// Prune deletes up to limit records published or dead-lettered before retention ago
// (dead letters stay in outbox_dead_letters). It returns how many it deleted.
func (o outbox) Prune(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	query := `
		DELETE FROM outbox_events
		WHERE event_id IN (
			SELECT event_id FROM outbox_events
			WHERE published_at < NOW() - $1 * INTERVAL '1 millisecond'
			   OR failed_at < NOW() - $1 * INTERVAL '1 millisecond'
			LIMIT $2
		)
	`
	tag, err := o.db.Pool.Exec(ctx, query, retention.Milliseconds(), limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (o outbox) DeadLetter(ctx context.Context, rec *storage.ClaimedEvent, reason string) error {
	tx, err := o.db.Pool.Begin(ctx)
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
