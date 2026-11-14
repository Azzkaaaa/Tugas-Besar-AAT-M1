//go:build integration

// Integration tests: compiled only with -tags integration (see services/aggregator/README.md).
// They need real databases and fail when the AGG_TEST_* variables are missing.

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/cassandra"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/postgres"
)

// TestCopy seeds Postgres, copies twice (idempotence) and compares what Cassandra serves.
// Needs AGG_TEST_POSTGRES_DSN and AGG_TEST_CASSANDRA_HOSTS; the Postgres tables are truncated.
func TestCopy(t *testing.T) {
	dsn, hosts := os.Getenv("AGG_TEST_POSTGRES_DSN"), os.Getenv("AGG_TEST_CASSANDRA_HOSTS")
	if dsn == "" || hosts == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN and AGG_TEST_CASSANDRA_HOSTS must be set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	src, err := postgres.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Pool.Close()
	if _, err := src.Pool.Exec(ctx, `TRUNCATE hazard_events, outbox_events, outbox_dead_letters, ingest_watermarks`); err != nil {
		t.Fatal(err)
	}
	dst, err := cassandra.Open(ctx, cassandra.Config{
		Hosts: strings.Split(hosts, ","), Keyspace: "agg_test_storecopy", Consistency: "ONE",
		BucketPeriod: time.Minute, OutboxRetention: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	pg, cs := src.Backend(), dst.Backend()
	// Cleanups run last-registered first: drop the keyspace, then close the session.
	t.Cleanup(cs.Close)
	t.Cleanup(func() { _ = dst.DropForTest() })
	// Start empty even if an earlier run left data behind.
	for _, table := range []string{"hazards_by_id", "hazard_by_source_ref", "hazards_by_bucket", "hazard_buckets",
		"source_state", "outbox_events", "outbox_pending", "outbox_dead_letters"} {
		if err := dst.TruncateForTest(table); err != nil {
			t.Fatal(err)
		}
	}

	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	n := 0
	for i := 0; i < 30; i++ {
		h := &domain.HazardRecord{
			Source: domain.SourceBMKG, SourceRefID: fmt.Sprintf("ev-%d", i), HazardType: domain.HazardTypeSeismic,
			Severity: domain.SeverityNormal, AreaName: "Area", OccurredAt: base.Add(time.Duration(i) * 7 * time.Minute),
			Attributes: map[string]any{"magnitude": 4.0 + float64(i)/10},
		}
		_, _, err := pg.Hazards.ApplyRevision(ctx, h, func(s *domain.HazardRecord) (storage.OutboxEvent, error) {
			n++
			return storage.OutboxEvent{EventID: fmt.Sprintf("pub-%d", n), HazardID: s.HazardID, Source: s.Source, Payload: []byte("p")}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// 10 published, 1 dead letter, 19 pending.
	claimed, err := pg.Outbox.ClaimDue(ctx, 11, time.Minute)
	if err != nil || len(claimed) != 11 {
		t.Fatalf("claim: %d, %v", len(claimed), err)
	}
	for _, ev := range claimed[:10] {
		_ = pg.Outbox.Ack(ctx, ev.EventID)
	}
	_ = pg.Outbox.DeadLetter(ctx, claimed[10], "nack")
	_ = pg.Sources.RecordSuccess(ctx, domain.SourceKeyBMKGSeismic, base.Add(203*time.Minute))
	_ = pg.Sources.RecordFailure(ctx, domain.SourceKeyPVMBGVolcanic, "down")

	for i := 0; i < 2; i++ {
		if err := run(ctx, src, dst); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}

	want, _ := pg.Hazards.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 100})
	got, err := cs.Hazards.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != 30 || len(want.Items) != 30 {
		t.Fatalf("copied %d of %d hazards", len(got.Items), len(want.Items))
	}
	for i := range want.Items {
		w, g := want.Items[i], got.Items[i]
		if g.HazardID != w.HazardID || !g.IngestedAt.Equal(w.IngestedAt) || !g.OccurredAt.Equal(w.OccurredAt) ||
			fmt.Sprint(g.Attributes) != fmt.Sprint(w.Attributes) {
			t.Fatalf("hazard %d differs:\n got %+v\nwant %+v", i, g, w)
		}
	}
	// A copied hazard is recognized by identity: re-applying it is not a change.
	_, changed, err := cs.Hazards.ApplyRevision(ctx, want.Items[0], func(*domain.HazardRecord) (storage.OutboxEvent, error) {
		t.Fatal("unchanged copied hazard emitted an event")
		return storage.OutboxEvent{}, nil
	})
	if err != nil || changed {
		t.Fatalf("re-apply copied hazard: changed=%v err=%v", changed, err)
	}

	st, _ := cs.Sources.Get(ctx, domain.SourceKeyBMKGSeismic)
	if st.Status != domain.StatusFresh || !st.Watermark.Equal(base.Add(203*time.Minute)) {
		t.Fatalf("bmkg state %+v", st)
	}
	st, _ = cs.Sources.Get(ctx, domain.SourceKeyPVMBGVolcanic)
	if st.Status != domain.StatusStale || st.ErrorMessage != "down" || !st.Watermark.IsZero() {
		t.Fatalf("pvmbg state %+v", st)
	}

	pending, err := cs.Outbox.ClaimDue(ctx, 100, time.Minute)
	if err != nil || len(pending) != 19 {
		t.Fatalf("pending events in cassandra: %d, %v", len(pending), err)
	}
	if pending[0].EventID != "pub-12" || pending[18].EventID != "pub-30" {
		t.Fatalf("pending order: first %s last %s", pending[0].EventID, pending[18].EventID)
	}
}
