package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	webhookv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/webhook/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	_ "modernc.org/sqlite"
)

// DB wraps the SQLite connection.
type DB struct {
	db *sql.DB
}

// Subscription mirrors the proto WebhookSubscription for internal use.
type Subscription struct {
	WebhookID      string
	OwnerClientID  string
	URL            string
	Secret         string
	FilterSource   int32
	FilterHazard   int32
	FilterSeverity int32
	FilterSince    *time.Time
	FilterUntil    *time.Time
	View           int32
	Description    string
	IsActive       bool
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Delivery mirrors the proto WebhookDelivery for internal use.
type Delivery struct {
	DeliveryID   string
	WebhookID    string
	EventID      string
	HazardID     string
	StatusCode   int32
	Status       string // "SUCCESS", "FAILED", "RETRYING"
	Attempt      int32
	DurationMS   int64
	ErrorMessage string
	DeliveredAt  time.Time
}

// Connect opens the SQLite database and applies the schema.
func Connect(_ context.Context, dsn string) (*DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed opening sqlite: %w", err)
	}
	// SQLite works best single-writer; keep pool small.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &DB{db: db}
	if err := store.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed running migrations: %w", err)
	}
	return store, nil
}

// Close releases the database connection.
func (s *DB) Close() error {
	return s.db.Close()
}

func (s *DB) migrate() error {
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

-- Tracks successfully delivered (event_id, webhook_id) pairs for deduplication.
CREATE TABLE IF NOT EXISTS delivery_dedup (
    event_id     TEXT NOT NULL,
	webhook_id   TEXT NOT NULL REFERENCES webhook_subscriptions(webhook_id) ON DELETE CASCADE,
    delivered_at DATETIME NOT NULL,
    PRIMARY KEY (event_id, webhook_id)
);
`
	_, err := s.db.Exec(schema)
	return err
}

// ---------------------------------------------------------------------------
// Subscription CRUD
// ---------------------------------------------------------------------------

// CreateSubscription inserts a new webhook subscription.
func (s *DB) CreateSubscription(ctx context.Context, sub *Subscription) error {
	now := time.Now().UTC()
	sub.CreatedAt = now
	sub.UpdatedAt = now

	var filterSince, filterUntil *string
	if sub.FilterSince != nil {
		t := sub.FilterSince.UTC().Format(time.RFC3339Nano)
		filterSince = &t
	}
	if sub.FilterUntil != nil {
		t := sub.FilterUntil.UTC().Format(time.RFC3339Nano)
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
		now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano),
	)
	return err
}

// GetSubscription retrieves a subscription by ID.
func (s *DB) GetSubscription(ctx context.Context, webhookID string) (*Subscription, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT webhook_id, owner_client_id, url, secret, filter_source, filter_hazard,
		       filter_severity, filter_since, filter_until, view, description, is_active,
		       created_at, updated_at
		FROM webhook_subscriptions WHERE webhook_id = ?`, webhookID)
	return scanSubscription(row)
}

// ListSubscriptions returns subscriptions owned by ownerClientID with cursor-based pagination.
// pageToken is base64(last_webhook_id); empty string starts from the beginning.
func (s *DB) ListSubscriptions(ctx context.Context, ownerClientID, pageToken string, pageSize int) ([]*Subscription, string, error) {
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}

	var cursor string
	if pageToken != "" {
		b, err := base64.StdEncoding.DecodeString(pageToken)
		if err != nil {
			return nil, "", fmt.Errorf("invalid page token")
		}
		cursor = string(b)
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
		sub, err := scanSubscriptionRow(rows)
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
		last := subs[pageSize-1]
		nextToken = base64.StdEncoding.EncodeToString([]byte(last.WebhookID))
		subs = subs[:pageSize]
	}
	return subs, nextToken, nil
}

// DeleteSubscription removes a subscription by ID.
func (s *DB) DeleteSubscription(ctx context.Context, webhookID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_subscriptions WHERE webhook_id = ?`, webhookID)
	return err
}

// ListActiveSubscriptions returns all active subscriptions (used by the consumer).
func (s *DB) ListActiveSubscriptions(ctx context.Context) ([]*Subscription, error) {
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
		sub, err := scanSubscriptionRow(rows)
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
func (s *DB) InsertDelivery(ctx context.Context, d *Delivery) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO webhook_deliveries
			(delivery_id, webhook_id, event_id, hazard_id, status_code, status,
			 attempt, duration_ms, error_message, delivered_at)
		VALUES (?,?,?,?,?,?,?,?,?,?)`,
		d.DeliveryID, d.WebhookID, d.EventID, d.HazardID,
		d.StatusCode, d.Status, d.Attempt, d.DurationMS,
		d.ErrorMessage, d.DeliveredAt.UTC().Format(time.RFC3339Nano),
	)
	return err
}

// ListDeliveries returns delivery log entries for a webhook with cursor-based pagination.
// pageToken is base64(last_delivery_id); empty string starts from the beginning.
func (s *DB) ListDeliveries(ctx context.Context, webhookID, pageToken string, pageSize int) ([]*Delivery, string, error) {
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}

	var cursor string
	if pageToken != "" {
		b, err := base64.StdEncoding.DecodeString(pageToken)
		if err != nil {
			return nil, "", fmt.Errorf("invalid page token")
		}
		cursor = string(b)
	}

	query := `
		SELECT delivery_id, webhook_id, event_id, hazard_id, status_code, status,
		       attempt, duration_ms, error_message, delivered_at
		FROM webhook_deliveries WHERE webhook_id = ?`
	args := []any{webhookID}

	if cursor != "" {
		// Cursor is (delivered_at, delivery_id) encoded together for stable ordering
		query += " AND (delivered_at < ? OR (delivered_at = ? AND delivery_id < ?))"
		args = append(args, cursor, cursor, cursor)
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
		d, err := scanDelivery(rows)
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
		last := deliveries[pageSize-1]
		nextToken = base64.StdEncoding.EncodeToString([]byte(last.DeliveryID))
		deliveries = deliveries[:pageSize]
	}
	return deliveries, nextToken, nil
}

// ---------------------------------------------------------------------------
// Deduplication
// ---------------------------------------------------------------------------

// IsDelivered returns true if this (event_id, webhook_id) has already been
// successfully delivered.
func (s *DB) IsDelivered(ctx context.Context, eventID, webhookID string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM delivery_dedup WHERE event_id = ? AND webhook_id = ?`,
		eventID, webhookID,
	).Scan(&count)
	return count > 0, err
}

// MarkDelivered records a successful delivery for deduplication.
func (s *DB) MarkDelivered(ctx context.Context, eventID, webhookID string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO delivery_dedup (event_id, webhook_id, delivered_at) VALUES (?,?,?)`,
		eventID, webhookID, time.Now().UTC().Format(time.RFC3339Nano),
	)
	return err
}

// ---------------------------------------------------------------------------
// Proto conversion helpers
// ---------------------------------------------------------------------------

// ToProto converts a Subscription to the proto WebhookSubscription (with secret redacted).
func (sub *Subscription) ToProto(redactSecret bool) *webhookv1.WebhookSubscription {
	secret := sub.Secret
	if redactSecret {
		secret = ""
	}
	p := &webhookv1.WebhookSubscription{
		WebhookId:     sub.WebhookID,
		OwnerClientId: sub.OwnerClientID,
		Url:           sub.URL,
		Secret:        secret,
		View:          commonv1.View(sub.View),
		Description:   sub.Description,
		IsActive:      sub.IsActive,
		CreatedAt:     timestamppb.New(sub.CreatedAt),
		UpdatedAt:     timestamppb.New(sub.UpdatedAt),
		Filter: &commonv1.HazardFilter{
			Source:     commonv1.Source(sub.FilterSource),
			HazardType: commonv1.HazardType(sub.FilterHazard),
			Severity:   commonv1.Severity(sub.FilterSeverity),
		},
	}
	if sub.FilterSince != nil {
		p.Filter.Since = timestamppb.New(*sub.FilterSince)
	}
	if sub.FilterUntil != nil {
		p.Filter.Until = timestamppb.New(*sub.FilterUntil)
	}
	return p
}

// ToProto converts a Delivery to the proto WebhookDelivery.
func (d *Delivery) ToProto() *webhookv1.WebhookDelivery {
	status := webhookv1.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED
	switch d.Status {
	case "SUCCESS":
		status = webhookv1.DeliveryStatus_DELIVERY_STATUS_SUCCESS
	case "FAILED":
		status = webhookv1.DeliveryStatus_DELIVERY_STATUS_FAILED
	case "RETRYING":
		status = webhookv1.DeliveryStatus_DELIVERY_STATUS_RETRYING
	}
	return &webhookv1.WebhookDelivery{
		DeliveryId:   d.DeliveryID,
		WebhookId:    d.WebhookID,
		EventId:      d.EventID,
		HazardId:     d.HazardID,
		StatusCode:   d.StatusCode,
		Status:       status,
		Attempt:      d.Attempt,
		DurationMs:   d.DurationMS,
		ErrorMessage: d.ErrorMessage,
		DeliveredAt:  timestamppb.New(d.DeliveredAt),
	}
}

// ---------------------------------------------------------------------------
// Internal scan helpers
// ---------------------------------------------------------------------------

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSubscription(row rowScanner) (*Subscription, error) {
	return scanSubscriptionRow(row)
}

func scanSubscriptionRow(row rowScanner) (*Subscription, error) {
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
	if err == sql.ErrNoRows {
		return nil, nil
	}
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

func scanDelivery(row rowScanner) (*Delivery, error) {
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
