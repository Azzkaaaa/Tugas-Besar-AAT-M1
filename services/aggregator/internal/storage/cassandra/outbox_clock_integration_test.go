//go:build integration

package cassandra

import (
	"context"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/google/uuid"
)

func TestOutboxReleaseAndRetryRemainOrderedAcrossConditionalWrites(t *testing.T) {
	db, err := openTest(t, "agg_test_outbox_clock_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	b := db.Backend()
	for i := range 20 {
		h := testHazard(uuid.NewString(), domain.SeveritySiaga)
		if _, _, err := b.Hazards.ApplyRevision(ctx, h, revEvent); err != nil {
			t.Fatal(err)
		}
		events, err := b.Outbox.ClaimDue(ctx, 1, time.Minute)
		if err != nil || len(events) != 1 {
			t.Fatal("initial claim", len(events), err)
		}
		id := events[0].EventID
		if err := b.Outbox.RetryLater(ctx, id, "retry", time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := b.Outbox.Release(ctx, []string{id}); err != nil {
			t.Fatal(err)
		}
		// Release is conditional while enqueue/retry/claim also update the row.
		// Their timestamps must share storage's clock, rather than mixing an
		// Aggregator wall clock with the coordinator's Paxos clock.
		again, err := b.Outbox.ClaimDue(ctx, 1, time.Minute)
		if err != nil || len(again) != 1 || again[0].EventID != id {
			t.Fatalf("iteration %d release lost: %+v %v", i, again, err)
		}
		if err := b.Outbox.RetryLater(ctx, id, "retry", 0); err != nil {
			t.Fatal(err)
		}
		again, err = b.Outbox.ClaimDue(ctx, 1, time.Minute)
		if err != nil || len(again) != 1 || again[0].RetryCount != 2 {
			t.Fatal("retry after conditional release lost", again, err)
		}
		if err := b.Outbox.Ack(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
}
