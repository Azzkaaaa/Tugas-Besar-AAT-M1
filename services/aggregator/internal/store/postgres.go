package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/telemetry"
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
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

// ingestLeaderKey is the session advisory lock of the ingest leader (see RunAsIngestLeader).
// Arbitrary, but fixed: every replica must use the same value.
const ingestLeaderKey int64 = 0x6167675f6c656164 // "agg_lead"

//go:embed migrations/postgres/*.sql
var postgresMigrations embed.FS

// Migrate applies pending goose migrations, then seeds reference data. goose holds a Postgres
// session lock while migrating, so replicas starting together apply each migration once. An
// older build skips migrations it does not know, so a rollback never rewrites a newer schema.
func (db *DB) Migrate(ctx context.Context) error {
	migrations, err := fs.Sub(postgresMigrations, "migrations/postgres")
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	sqlDB := stdlib.OpenDBFromPool(db.Pool)
	defer sqlDB.Close()

	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, migrations, goose.WithSessionLocker(locker))
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
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
