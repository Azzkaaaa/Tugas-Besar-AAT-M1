package store

import (
	"context"
	"fmt"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool
}

func Connect(ctx context.Context, dsn string) (*DB, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database DSN: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	// One client span per query (named by operation, no parameters), only under a caller's span.
	config.ConnConfig.Tracer = otelpgx.NewTracer(otelpgx.WithDisableAcquireTracer(),
		otelpgx.WithTracerProvider(telemetry.ParentOnlyTracerProvider()))

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	db := &DB{Pool: pool}
	if err := db.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("failed running database migrations: %w", err)
	}

	return db, nil
}

// Session advisory lock keys. Arbitrary, but fixed: every replica must use the same values.
const (
	migrationLockKey int64 = 0x6167675f6d696772 // "agg_migr"
	ingestLeaderKey  int64 = 0x6167675f6c656164 // "agg_lead"
)

// Migrate applies the schema under a session advisory lock, so replicas starting together do not
// race on CREATE ... IF NOT EXISTS (which can still fail when run concurrently).
func (db *DB) Migrate(ctx context.Context) error {
	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockKey)
	}()

	schema := `
	CREATE TABLE IF NOT EXISTS volcano_references (
		volcano_id VARCHAR(64) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		latitude DOUBLE PRECISION NOT NULL,
		longitude DOUBLE PRECISION NOT NULL
	);

	CREATE TABLE IF NOT EXISTS hazard_events (
		hazard_id VARCHAR(128) PRIMARY KEY,
		source VARCHAR(32) NOT NULL,
		source_ref_id VARCHAR(128) NOT NULL,
		hazard_type VARCHAR(32) NOT NULL,
		severity VARCHAR(32) NOT NULL,
		area_name VARCHAR(255) NOT NULL,
		latitude DOUBLE PRECISION NOT NULL,
		longitude DOUBLE PRECISION NOT NULL,
		occurred_at TIMESTAMPTZ NOT NULL,
		ingested_at TIMESTAMPTZ NOT NULL,
		attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
		CONSTRAINT uq_source_ref UNIQUE (source, source_ref_id)
	);

	CREATE INDEX IF NOT EXISTS idx_hazard_pagination ON hazard_events (occurred_at DESC, hazard_id ASC);
	CREATE INDEX IF NOT EXISTS idx_hazard_source ON hazard_events (source);
	CREATE INDEX IF NOT EXISTS idx_hazard_type ON hazard_events (hazard_type);
	CREATE INDEX IF NOT EXISTS idx_hazard_severity ON hazard_events (severity);

	CREATE TABLE IF NOT EXISTS outbox_events (
		event_id VARCHAR(128) PRIMARY KEY,
		hazard_id VARCHAR(128) NOT NULL,
		source VARCHAR(32) NOT NULL,
		payload BYTEA NOT NULL,
		published_at TIMESTAMPTZ NULL,
		retry_count INTEGER NOT NULL DEFAULT 0,
		failed_at TIMESTAMPTZ NULL,
		last_error TEXT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
	ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ NULL;
	ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS last_error TEXT NULL;

	-- W3C trace context of the ingest poll that enqueued the event (see EnqueueOutbox).
	ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS trace_context JSONB NULL;

	CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events (created_at ASC) WHERE published_at IS NULL AND failed_at IS NULL;

	-- When a record is next due: its claim lease while a publisher holds it, its backoff after a
	-- failed attempt. Defaults to NOW() so rows from older releases are due at once.
	ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
	CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox_events (next_attempt_at, created_at) WHERE published_at IS NULL AND failed_at IS NULL;
	CREATE INDEX IF NOT EXISTS idx_outbox_published ON outbox_events (published_at) WHERE published_at IS NOT NULL;
	CREATE INDEX IF NOT EXISTS idx_outbox_failed ON outbox_events (failed_at) WHERE failed_at IS NOT NULL;

	CREATE TABLE IF NOT EXISTS outbox_dead_letters (
		event_id VARCHAR(128) PRIMARY KEY,
		hazard_id VARCHAR(128) NOT NULL,
		source VARCHAR(32) NOT NULL,
		payload BYTEA NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		last_error TEXT
	);

	ALTER TABLE outbox_dead_letters ADD COLUMN IF NOT EXISTS trace_context JSONB NULL;

	CREATE TABLE IF NOT EXISTS ingest_watermarks (
		source_key VARCHAR(64) PRIMARY KEY,
		watermark_time TIMESTAMPTZ NOT NULL,
		last_success_at TIMESTAMPTZ NULL,
		last_attempt_at TIMESTAMPTZ NOT NULL,
		status VARCHAR(32) NOT NULL DEFAULT 'UNAVAILABLE',
		error_message TEXT NULL
	);

	ALTER TABLE ingest_watermarks ALTER COLUMN last_success_at DROP NOT NULL;
	`

	if _, err := conn.Exec(ctx, schema); err != nil {
		return err
	}

	// Seed default volcanoes if empty
	for _, v := range domain.GetAllVolcanoes() {
		_, err := conn.Exec(ctx, `
			INSERT INTO volcano_references (volcano_id, name, latitude, longitude)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (volcano_id) DO NOTHING
		`, v.ID, v.Name, v.Latitude, v.Longitude)
		if err != nil {
			return err
		}
	}

	return nil
}
