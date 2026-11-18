//go:build integration

package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/storetest"
)

func TestCriticalJournalSurvivesDownstreamErasure(t *testing.T) {
	dsn := os.Getenv("AGG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN not set")
	}
	db, err := Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Pool.Close()
	if _, err := db.Pool.Exec(context.Background(), `TRUNCATE hazard_events,outbox_events,outbox_dead_letters,critical_event_journal`); err != nil {
		t.Fatal(err)
	}
	storetest.CriticalJournal(t, db.Backend(), func() {
		if _, err := db.Pool.Exec(context.Background(), `TRUNCATE outbox_events`); err != nil {
			t.Fatal(err)
		}
	})
}
