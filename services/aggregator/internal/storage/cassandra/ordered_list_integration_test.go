//go:build integration

package cassandra

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

func TestOrderedListCutoverMovesAndPagination(t *testing.T) {
	db, err := openTest(t, "agg_test_ordered_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer dropKeyspace(db)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	base := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	var records []*domain.HazardRecord
	for i := range 18 {
		source := []string{domain.SourceBMKG, domain.SourcePVMBG}[i%2]
		h := &domain.HazardRecord{Source: source, SourceRefID: fmt.Sprintf("ordered-%02d", i), HazardType: domain.HazardTypeSeismic,
			Severity: domain.SeverityWaspada, AreaName: "Padang", OccurredAt: base.Add(-time.Duration(i/2) * time.Minute), IngestedAt: base, Revision: 1,
			Attributes: map[string]any{"fixture": true}}
		h.HazardID = domain.HazardID(h.Source, h.SourceRefID)
		if err := db.ImportHazard(ctx, h); err != nil {
			t.Fatal(err)
		}
		records = append(records, h)
	}
	// Simulate a pre-feature store and hundreds of empty newer minute buckets.
	if err := db.session.Query(`TRUNCATE list_headers_by_shard`).WithContext(ctx).Exec(); err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		if err := db.session.Query(`INSERT INTO hazard_buckets(scope,bucket) VALUES(?,?)`, scopeAll, base.Add(time.Duration(i+1)*time.Minute)).WithContext(ctx).Exec(); err != nil {
			t.Fatal(err)
		}
	}
	if db.orderedListReady(ctx) {
		t.Fatal("cutover happened before all writers were upgraded")
	}
	// A pre-cache canonical row still requires one bounded body fallback; it
	// must not be hidden as inactive when the new header-only scan sees NULL.
	legacy := records[10]
	if err := db.session.Query(`DELETE summary_active,summary_revision FROM canonical_events WHERE shard=? AND hazard_id=?`, auditShard(legacy.HazardID), legacy.HazardID).WithContext(ctx).Exec(); err != nil {
		t.Fatal(err)
	}
	if err := db.PrepareOrderedList(ctx); err != nil {
		t.Fatal(err)
	}
	// A move and withdrawal must survive delayed writes/backfills of revision 1.
	old := *records[0]
	moved := old
	moved.Revision = 2
	moved.OccurredAt = base.Add(-20 * time.Minute)
	if err := db.ImportHazard(ctx, &moved); err != nil {
		t.Fatal(err)
	}
	b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	addOrderedListProjection(b, &old, db.bucketOf(old.OccurredAt), time.Time{})
	withdrawn := *records[1]
	withdrawn.Revision = 2
	withdrawn.Attributes = map[string]any{"withdrawn": true}
	if err := db.ImportHazard(ctx, &withdrawn); err != nil {
		t.Fatal(err)
	}
	addOrderedListProjection(b, records[1], db.bucketOf(records[1].OccurredAt), time.Time{})
	if err := b.Exec(); err != nil {
		t.Fatal(err)
	}
	records[0] = &moved
	records = append(records[:1], records[2:]...)
	sort.Slice(records, func(i, j int) bool { return listOrderBefore(records[i], records[j]) })
	for _, summary := range []bool{true, false} {
		var got []*domain.HazardRecord
		token := ""
		for pageNo := 0; pageNo < 10; pageNo++ {
			deadline, stop := context.WithTimeout(ctx, 1500*time.Millisecond)
			page, err := (hazards{db}).List(deadline, storage.HazardQuery{SummaryOnly: summary}, storage.PageRequest{Size: 5, Token: token})
			stop()
			if err != nil || page.Partial {
				t.Fatalf("ordered page: err=%v partial=%v units=%v", err, page.Partial, page.UnavailableUnits)
			}
			got = append(got, page.Items...)
			token = page.NextToken
			if token == "" {
				break
			}
		}
		if len(got) != len(records) {
			t.Fatalf("summary=%v got %d records, want %d", summary, len(got), len(records))
		}
		for i, h := range got {
			if h.HazardID != records[i].HazardID || !h.OccurredAt.Equal(records[i].OccurredAt) {
				t.Fatalf("wrong order or duplicate at %d", i)
			}
			if summary && h.Attributes != nil {
				t.Fatal("summary hydrated a full body")
			}
			if !summary && h.Attributes["fixture"] != true {
				t.Fatal("full body was omitted")
			}
		}
	}
	q := storage.HazardQuery{Source: domain.SourceBMKG, Since: base.Add(-3 * time.Minute), Until: base.Add(time.Nanosecond), SummaryOnly: true}
	page, err := (hazards{db}).List(ctx, q, storage.PageRequest{Size: 100})
	if err != nil || page.Partial || len(page.Items) != 3 {
		t.Fatalf("source/time bounds: %d partial=%v err=%v", len(page.Items), page.Partial, err)
	}
	if err := db.PrepareOrderedList(ctx); err != nil {
		t.Fatal("repeated preparation", err)
	}
}
