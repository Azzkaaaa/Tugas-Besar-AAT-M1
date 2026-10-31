package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// sqliteTime is fixed-width, so the TEXT timestamps order lexicographically (RFC3339Nano trims
// trailing zeros and would not).
const sqliteTime = "2006-01-02T15:04:05.000000000Z07:00"

// SQLiteStore is the single-replica backend, used by tests and local runs.
type SQLiteStore struct {
	db *sql.DB
}

// OpenSQLite opens the SQLite database and applies the schema.
func OpenSQLite(_ context.Context, dsn string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed opening sqlite: %w", err)
	}
	// SQLite works best single-writer; keep pool small.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &SQLiteStore{db: db}
	if err := store.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed running migrations: %w", err)
	}
	return store, nil
}

// Close releases the database connection.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) migrate() error {
	schema := `
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS webhook_subscriptions (
    webhook_id       TEXT PRIMARY KEY,
    owner_client_id  TEXT NOT NULL,
    url              TEXT NOT NULL,
    secret           TEXT NOT NULL,
    filter_source    INTEGER NOT NULL DEFAULT 0,
    filter_hazard    INTEGER NOT NULL DEFAULT 0,
    filter_severity  INTEGER NOT NULL DEFAULT 0,
    filter_since     DATETIME,
    filter_until     DATETIME,
    view             INTEGER NOT NULL DEFAULT 1,
    description      TEXT NOT NULL DEFAULT '',
    is_active        INTEGER NOT NULL DEFAULT 1,
    created_at       DATETIME NOT NULL,
    updated_at       DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    delivery_id   TEXT PRIMARY KEY,
    webhook_id    TEXT NOT NULL REFERENCES webhook_subscriptions(webhook_id) ON DELETE CASCADE,
    event_id      TEXT NOT NULL,
    hazard_id     TEXT NOT NULL,
    status_code   INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL DEFAULT 'RETRYING',
    attempt       INTEGER NOT NULL DEFAULT 1,
    duration_ms   INTEGER NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    delivered_at  DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_deliveries_webhook
    ON webhook_deliveries (webhook_id, delivered_at DESC);

CREATE INDEX IF NOT EXISTS idx_deliveries_event
    ON webhook_deliveries (event_id, webhook_id);
`
	_, err := s.db.Exec(schema)
	return err
}

// ---------------------------------------------------------------------------
// Subscription CRUD
// ---------------------------------------------------------------------------

// CreateSubscription inserts a new webhook subscription.
func (s *SQLiteStore) CreateSubscription(ctx context.Context, sub *Subscription) error {
	now := time.Now().UTC()
	sub.CreatedAt = now
	sub.UpdatedAt = now

	var filterSince, filterUntil *string
	if sub.FilterSince != nil {
		t := sub.FilterSince.UTC().Format(sqliteTime)
		filterSince = &t
	}
	if sub.FilterUntil != nil {
		t := sub.FilterUntil.UTC().Format(sqliteTime)
		filterUntil = &t
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webhook_subscriptions
			(webhook_id, owner_client_id, url, secret, filter_source, filter_hazard,
			 filter_severity, filter_since, filter_until, view, description, is_active, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		sub.WebhookID, sub.OwnerClientID, sub.URL, sub.Secret,
		sub.FilterSource, sub.FilterHazard, sub.FilterSeverity,
		filterSince, filterUntil,
		sub.View, sub.Description, boolToInt(sub.IsActive),
		now.Format(sqliteTime), now.Format(sqliteTime),
	)
	return err
}

// GetSubscription retrieves a subscription by ID.
func (s *SQLiteStore) GetSubscription(ctx context.Context, webhookID string) (*Subscription, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT webhook_id, owner_client_id, url, secret, filter_source, filter_hazard,
		       filter_severity, filter_since, filter_until, view, description, is_active,
		       created_at, updated_at
		FROM webhook_subscriptions WHERE webhook_id = ?`, webhookID)
	sub, err := scanSQLiteSubscription(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return sub, err
}

// ListSubscriptions returns subscriptions owned by ownerClientID with cursor-based pagination.
func (s *SQLiteStore) ListSubscriptions(ctx context.Context, ownerClientID, pageToken string, pageSize int) ([]*Subscription, string, error) {
	pageSize = clampPageSize(pageSize)
	cursor, err := decodeSubscriptionCursor(pageToken)
	if err != nil {
		return nil, "", err
	}

	query := `
		SELECT webhook_id, owner_client_id, url, secret, filter_source, filter_hazard,
		       filter_severity, filter_since, filter_until, view, description, is_active,
		       created_at, updated_at
		FROM webhook_subscriptions
		WHERE owner_client_id = ?`
	args := []any{ownerClientID}

	if cursor != "" {
		query += " AND webhook_id > ?"
		args = append(args, cursor)
	}
	query += " ORDER BY webhook_id ASC LIMIT ?"
	args = append(args, pageSize+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var subs []*Subscription
	for rows.Next() {
		sub, err := scanSQLiteSubscription(rows)
		if err != nil {
			return nil, "", err
		}
		subs = append(subs, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var nextToken string
	if len(subs) > pageSize {
		nextToken = encodeSubscriptionCursor(subs[pageSize-1].WebhookID)
		subs = subs[:pageSize]
	}
	return subs, nextToken, nil
}

// DeleteSubscription removes a subscription by ID.
func (s *SQLiteStore) DeleteSubscription(ctx context.Context, webhookID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_subscriptions WHERE webhook_id = ?`, webhookID)
	return err
}

// ListActiveSubscriptions returns all active subscriptions (used by the consumer).
func (s *SQLiteStore) ListActiveSubscriptions(ctx context.Context) ([]*Subscription, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT webhook_id, owner_client_id, url, secret, filter_source, filter_hazard,
		       filter_severity, filter_since, filter_until, view, description, is_active,
		       created_at, updated_at
		FROM webhook_subscriptions WHERE is_active = 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var subs []*Subscription
	for rows.Next() {
		sub, err := scanSQLiteSubscription(rows)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	return subs, rows.Err()
}

// ---------------------------------------------------------------------------
// Delivery log
// ---------------------------------------------------------------------------

// InsertDelivery records a delivery attempt.
func (s *SQLiteStore) InsertDelivery(ctx context.Context, d *Delivery) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webhook_deliveries
			(delivery_id, webhook_id, event_id, hazard_id, status_code, status,
			 attempt, duration_ms, error_message, delivered_at)
		VALUES (?,?,?,?,?,?,?,?,?,?)`,
		d.DeliveryID, d.WebhookID, d.EventID, d.HazardID,
		d.StatusCode, d.Status, d.Attempt, d.DurationMS,
		d.ErrorMessage, d.DeliveredAt.UTC().Format(sqliteTime),
	)
	return err
}

// ListDeliveries returns delivery log entries for a webhook, newest first, with cursor-based pagination.
func (s *SQLiteStore) ListDeliveries(ctx context.Context, webhookID, pageToken string, pageSize int) ([]*Delivery, string, error) {
	pageSize = clampPageSize(pageSize)
	cursor, err := decodeDeliveryCursor(pageToken)
	if err != nil {
		return nil, "", err
	}

	query := `
		SELECT delivery_id, webhook_id, event_id, hazard_id, status_code, status,
		       attempt, duration_ms, error_message, delivered_at
		FROM webhook_deliveries WHERE webhook_id = ?`
	args := []any{webhookID}

	if cursor != nil {
		at := cursor.DeliveredAt.UTC().Format(sqliteTime)
		query += " AND (delivered_at < ? OR (delivered_at = ? AND delivery_id < ?))"
		args = append(args, at, at, cursor.DeliveryID)
	}
	query += " ORDER BY delivered_at DESC, delivery_id DESC LIMIT ?"
	args = append(args, pageSize+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var deliveries []*Delivery
	for rows.Next() {
		d, err := scanSQLiteDelivery(rows)
		if err != nil {
			return nil, "", err
		}
		deliveries = append(deliveries, d)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var nextToken string
	if len(deliveries) > pageSize {
		nextToken = encodeDeliveryCursor(deliveries[pageSize-1])
		deliveries = deliveries[:pageSize]
	}
	return deliveries, nextToken, nil
}

// ---------------------------------------------------------------------------
// Internal scan helpers
// ---------------------------------------------------------------------------

func scanSQLiteSubscription(row rowScanner) (*Subscription, error) {
	sub := &Subscription{}
	var filterSince, filterUntil sql.NullString
	var createdAt, updatedAt string
	var isActive int

	err := row.Scan(
		&sub.WebhookID, &sub.OwnerClientID, &sub.URL, &sub.Secret,
		&sub.FilterSource, &sub.FilterHazard, &sub.FilterSeverity,
		&filterSince, &filterUntil,
		&sub.View, &sub.Description, &isActive,
		&createdAt, &updatedAt,
	)
	if err != nil {
		return nil, err
	}

	sub.IsActive = isActive == 1

	if filterSince.Valid {
		t, err := time.Parse(time.RFC3339Nano, filterSince.String)
		if err == nil {
			sub.FilterSince = &t
		}
	}
	if filterUntil.Valid {
		t, err := time.Parse(time.RFC3339Nano, filterUntil.String)
		if err == nil {
			sub.FilterUntil = &t
		}
	}

	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		sub.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
		sub.UpdatedAt = t
	}

	return sub, nil
}

func scanSQLiteDelivery(row rowScanner) (*Delivery, error) {
	d := &Delivery{}
	var deliveredAt string

	err := row.Scan(
		&d.DeliveryID, &d.WebhookID, &d.EventID, &d.HazardID,
		&d.StatusCode, &d.Status, &d.Attempt, &d.DurationMS,
		&d.ErrorMessage, &deliveredAt,
	)
	if err != nil {
		return nil, err
	}
	if t, err := time.Parse(time.RFC3339Nano, deliveredAt); err == nil {
		d.DeliveredAt = t
	}
	return d, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
