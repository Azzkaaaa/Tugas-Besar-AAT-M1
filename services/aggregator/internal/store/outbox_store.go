package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type OutboxRecord struct {
	EventID   string
	HazardID  string
	Source    string
	Payload   []byte
	CreatedAt time.Time
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
		SELECT event_id, hazard_id, source, payload, created_at
		FROM outbox_events
		WHERE published_at IS NULL
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
		if err := rows.Scan(&rec.EventID, &rec.HazardID, &rec.Source, &rec.Payload, &rec.CreatedAt); err != nil {
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
