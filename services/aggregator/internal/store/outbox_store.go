package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type OutboxRecord struct {
	EventID    string
	HazardID   string
	Source     string
	Payload    []byte
	CreatedAt  time.Time
	RetryCount int
}

func (db *DB) EnqueueOutbox(ctx context.Context, tx pgx.Tx, eventID, hazardID, source string, payload []byte) error {
	query := `
		INSERT INTO outbox_events (event_id, hazard_id, source, payload, created_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (event_id) DO NOTHING
	`
	var err error
	if tx != nil {
		_, err = tx.Exec(ctx, query, eventID, hazardID, source, payload)
	} else {
		_, err = db.Pool.Exec(ctx, query, eventID, hazardID, source, payload)
	}
	return err
}

func (db *DB) FetchUnpublishedOutbox(ctx context.Context, limit int) ([]*OutboxRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
		SELECT event_id, hazard_id, source, payload, created_at, retry_count
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
		if err := rows.Scan(&rec.EventID, &rec.HazardID, &rec.Source, &rec.Payload, &rec.CreatedAt, &rec.RetryCount); err != nil {
			return nil, err
		}
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

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_dead_letters (event_id, hazard_id, source, payload, created_at, failed_at, last_error)
		VALUES ($1, $2, $3, $4, $5, NOW(), $6)
		ON CONFLICT (event_id) DO NOTHING
	`, rec.EventID, rec.HazardID, rec.Source, rec.Payload, rec.CreatedAt, reason)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

