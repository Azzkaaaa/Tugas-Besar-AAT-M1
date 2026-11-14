//go:build integration

// Integration tests: compiled only with -tags integration (see services/aggregator/README.md).
// They need real databases and fail when the AGG_TEST_* variables are missing.

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader/leasetest"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/rekey"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/storetest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/proto"
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

func TestLeases(t *testing.T) {
	dsn := os.Getenv("AGG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN not set")
	}
	db, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Pool.Close()
	if _, err := db.Pool.Exec(context.Background(), `DELETE FROM leader_leases WHERE name = 'test-lease'`); err != nil {
		t.Fatal(err)
	}
	leasetest.Run(t, db.Leases(), "test-lease", time.Second)
}

// Hazards stored under random IDs are re-keyed by migration 3 (run here directly) with their
// revisions and pending outbox payloads; a second run changes nothing.
func TestRekeyMigration(t *testing.T) {
	dsn := os.Getenv("AGG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	db, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Pool.Close()
	if _, err := db.Pool.Exec(ctx, `TRUNCATE hazard_events, outbox_events, outbox_dead_letters`); err != nil {
		t.Fatal(err)
	}
	oldID := "hz-" + uuid.New().String()
	payload, _ := proto.Marshal(&eventv1.HazardEventPublished{EventId: "pub-old", SchemaVersion: 1,
		Payload: &commonv1.HazardEvent{HazardId: oldID, Source: commonv1.Source_SOURCE_BMKG, SourceRefId: "legacy"}})
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO hazard_events (hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude,
			occurred_at, ingested_at, attributes, revision) VALUES ($1, 'BMKG', 'legacy', 'SEISMIC', 'WASPADA', 'A', 0, 0, now(), now(), '{}', 3)`,
			[]any{oldID}},
		{`INSERT INTO outbox_events (event_id, hazard_id, source, payload) VALUES ('pub-old', $1, 'BMKG', $2)`, []any{oldID, payload}},
		{`INSERT INTO outbox_dead_letters (event_id, hazard_id, source, payload, created_at) VALUES ('pub-dead', $1, 'BMKG', $2, now())`,
			[]any{oldID, payload}},
	} {
		if _, err := db.Pool.Exec(ctx, q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}

	sqlDB := stdlib.OpenDBFromPool(db.Pool)
	defer sqlDB.Close()
	for run := 0; run < 2; run++ {
		tx, err := sqlDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := rekeyHazardIDs(ctx, tx); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	newID := domain.HazardID(domain.SourceBMKG, "legacy")
	b := db.Backend()
	if old, _ := b.Hazards.Get(ctx, oldID); old != nil {
		t.Fatal("old hazard ID still resolves")
	}
	if got, err := b.Hazards.Get(ctx, newID); err != nil || got == nil || got.Revision != 3 {
		t.Fatalf("re-keyed hazard = %+v, %v", got, err)
	}
	evs, _ := b.Outbox.ClaimDue(ctx, 10, time.Minute)
	if len(evs) != 1 || evs[0].HazardID != newID {
		t.Fatalf("pending events %+v", evs)
	}
	if _, _, payloadID, _ := rekey.Identity(evs[0].Payload); payloadID != newID {
		t.Fatalf("pending payload names %s, want %s", payloadID, newID)
	}
	var deadID string
	var deadPayload []byte
	if err := db.Pool.QueryRow(ctx, `SELECT hazard_id, payload FROM outbox_dead_letters WHERE event_id = 'pub-dead'`).Scan(&deadID, &deadPayload); err != nil {
		t.Fatal(err)
	}
	if _, _, payloadID, _ := rekey.Identity(deadPayload); deadID != newID || payloadID != newID {
		t.Fatalf("dead letter hazard %s / payload %s, want %s", deadID, payloadID, newID)
	}
}
