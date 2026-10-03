package store

import (
	"context"
	"fmt"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
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

func (db *DB) Migrate(ctx context.Context) error {
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
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events (created_at ASC) WHERE published_at IS NULL;

	CREATE TABLE IF NOT EXISTS ingest_watermarks (
		source_key VARCHAR(64) PRIMARY KEY,
		watermark_time TIMESTAMPTZ NOT NULL,
		last_success_at TIMESTAMPTZ NOT NULL,
		last_attempt_at TIMESTAMPTZ NOT NULL,
		status VARCHAR(32) NOT NULL DEFAULT 'UNAVAILABLE',
		error_message TEXT NULL
	);
	`

	if _, err := db.Pool.Exec(ctx, schema); err != nil {
		return err
	}

	// Seed default volcanoes if empty
	for _, v := range domain.GetAllVolcanoes() {
		_, err := db.Pool.Exec(ctx, `
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
