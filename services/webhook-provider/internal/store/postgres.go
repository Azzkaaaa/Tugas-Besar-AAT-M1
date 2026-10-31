package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/postgres/*.sql
var postgresMigrations embed.FS

// PostgresStore is the deployed backend; any number of replicas can share it.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// OpenPostgres connects and applies pending migrations. Migrations hold a Postgres session lock,
// so replicas starting together apply them once.
func OpenPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database DSN: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	// One client span per query (named by operation, no parameters).
	config.ConnConfig.Tracer = otelpgx.NewTracer(otelpgx.WithDisableAcquireTracer())

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	if err := migratePostgres(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed running migrations: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

func migratePostgres(ctx context.Context, pool *pgxpool.Pool) error {
	migrations, err := fs.Sub(postgresMigrations, "migrations/postgres")
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations, goose.WithSessionLocker(locker))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}

// Close releases the connection pool.
func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

const subscriptionColumns = `
	webhook_id, owner_client_id, url, secret, filter_source, filter_hazard,
	filter_severity, filter_since, filter_until, view, description, is_active,
	created_at, updated_at`

const deliveryColumns = `
	delivery_id, webhook_id, event_id, hazard_id, status_code, status,
	attempt, duration_ms, error_message, delivered_at`

// ---------------------------------------------------------------------------
// Subscription CRUD
// ---------------------------------------------------------------------------

// CreateSubscription inserts a new webhook subscription.
func (s *PostgresStore) CreateSubscription(ctx context.Context, sub *Subscription) error {
	// Microseconds: what TIMESTAMPTZ keeps, so the returned value equals the stored one.
	now := time.Now().UTC().Truncate(time.Microsecond)
	sub.CreatedAt = now
	sub.UpdatedAt = now

	_, err := s.pool.Exec(ctx, `
		INSERT INTO webhook_subscriptions (`+subscriptionColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		sub.WebhookID, sub.OwnerClientID, sub.URL, sub.Secret,
		sub.FilterSource, sub.FilterHazard, sub.FilterSeverity,
		sub.FilterSince, sub.FilterUntil,
		sub.View, sub.Description, sub.IsActive,
		now, now,
	)
	return err
}

// GetSubscription retrieves a subscription by ID.
func (s *PostgresStore) GetSubscription(ctx context.Context, webhookID string) (*Subscription, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+subscriptionColumns+` FROM webhook_subscriptions WHERE webhook_id = $1`, webhookID)
	sub, err := scanPostgresSubscription(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return sub, err
}

// ListSubscriptions returns subscriptions owned by ownerClientID with cursor-based pagination.
func (s *PostgresStore) ListSubscriptions(ctx context.Context, ownerClientID, pageToken string, pageSize int) ([]*Subscription, string, error) {
	pageSize = clampPageSize(pageSize)
	cursor, err := decodeSubscriptionCursor(pageToken)
	if err != nil {
		return nil, "", err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT `+subscriptionColumns+`
		FROM webhook_subscriptions
		WHERE owner_client_id = $1 AND ($2 = '' OR webhook_id > $2)
		ORDER BY webhook_id ASC
		LIMIT $3`,
		ownerClientID, cursor, pageSize+1,
	)
	if err != nil {
		return nil, "", err
	}
	subs, err := collectSubscriptions(rows)
	if err != nil {
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
func (s *PostgresStore) DeleteSubscription(ctx context.Context, webhookID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM webhook_subscriptions WHERE webhook_id = $1`, webhookID)
	return err
}

// ListActiveSubscriptions returns all active subscriptions (used by the consumer).
func (s *PostgresStore) ListActiveSubscriptions(ctx context.Context) ([]*Subscription, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+subscriptionColumns+` FROM webhook_subscriptions WHERE is_active`)
	if err != nil {
		return nil, err
	}
	return collectSubscriptions(rows)
}

// ---------------------------------------------------------------------------
// Delivery log
// ---------------------------------------------------------------------------

// InsertDelivery records a delivery attempt.
func (s *PostgresStore) InsertDelivery(ctx context.Context, d *Delivery) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO webhook_deliveries (`+deliveryColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		d.DeliveryID, d.WebhookID, d.EventID, d.HazardID,
		d.StatusCode, d.Status, d.Attempt, d.DurationMS,
		d.ErrorMessage, d.DeliveredAt.UTC(),
	)
	return err
}

// ListDeliveries returns delivery log entries for a webhook, newest first, with cursor-based pagination.
func (s *PostgresStore) ListDeliveries(ctx context.Context, webhookID, pageToken string, pageSize int) ([]*Delivery, string, error) {
	pageSize = clampPageSize(pageSize)
	cursor, err := decodeDeliveryCursor(pageToken)
	if err != nil {
		return nil, "", err
	}

	var rows pgx.Rows
	if cursor == nil {
		rows, err = s.pool.Query(ctx, `
			SELECT `+deliveryColumns+`
			FROM webhook_deliveries
			WHERE webhook_id = $1
			ORDER BY delivered_at DESC, delivery_id DESC
			LIMIT $2`,
			webhookID, pageSize+1,
		)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT `+deliveryColumns+`
			FROM webhook_deliveries
			WHERE webhook_id = $1 AND (delivered_at, delivery_id) < ($2, $3)
			ORDER BY delivered_at DESC, delivery_id DESC
			LIMIT $4`,
			webhookID, cursor.DeliveredAt, cursor.DeliveryID, pageSize+1,
		)
	}
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var deliveries []*Delivery
	for rows.Next() {
		d := &Delivery{}
		if err := rows.Scan(
			&d.DeliveryID, &d.WebhookID, &d.EventID, &d.HazardID,
			&d.StatusCode, &d.Status, &d.Attempt, &d.DurationMS,
			&d.ErrorMessage, &d.DeliveredAt,
		); err != nil {
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

func collectSubscriptions(rows pgx.Rows) ([]*Subscription, error) {
	defer rows.Close()
	var subs []*Subscription
	for rows.Next() {
		sub, err := scanPostgresSubscription(rows)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	return subs, rows.Err()
}

func scanPostgresSubscription(row rowScanner) (*Subscription, error) {
	sub := &Subscription{}
	err := row.Scan(
		&sub.WebhookID, &sub.OwnerClientID, &sub.URL, &sub.Secret,
		&sub.FilterSource, &sub.FilterHazard, &sub.FilterSeverity,
		&sub.FilterSince, &sub.FilterUntil,
		&sub.View, &sub.Description, &sub.IsActive,
		&sub.CreatedAt, &sub.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return sub, nil
}
