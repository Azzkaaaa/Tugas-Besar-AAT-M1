//go:build integration

// Integration tests: compiled only with -tags integration (see services/aggregator/README.md).
// They need real databases and fail when the AGG_TEST_* variables are missing.

package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/storetest"
)

// TestContract runs the storage contract against AGG_TEST_POSTGRES_DSN. The database's
// aggregator tables are truncated before every case: never point it at real data.
func TestContract(t *testing.T) {
	dsn := os.Getenv("AGG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN not set")
	}
	db, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Pool.Close)

	storetest.Run(t, func(t *testing.T) *storage.Backend {
		if _, err := db.Pool.Exec(context.Background(),
			`TRUNCATE hazard_events, outbox_events, outbox_dead_letters, ingest_watermarks`); err != nil {
			t.Fatal(err)
		}
		return db.Backend()
	})
}
