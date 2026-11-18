//go:build integration

package cassandra

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Rows written to the legacy projection before migration 0014 must be listed
// after migration 15, in List order, exactly once, and spread over shards.
func TestShardedProjectionBackfillFromLegacyRows(t *testing.T) {
	db, err := openTest(t, "agg_test_projection_backfill", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	truncate(t, db)
	ctx := context.Background()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	bucket := db.bucketOf(base)
	var want []string
	for i := 0; i < 40; i++ {
		h := &domain.HazardRecord{Source: domain.SourceBMKG, SourceRefID: fmt.Sprint("legacy-", i), HazardType: domain.HazardTypeSeismic,
			Severity: domain.SeverityNormal, AreaName: "A", OccurredAt: base.Add(time.Duration(i%7) * time.Minute), IngestedAt: base, Revision: 1,
			Attributes: map[string]any{}}
		h.HazardID = domain.HazardID(h.Source, h.SourceRefID)
		b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
		addLegacyProjection(b, h, bucket)
		if err := b.Exec(); err != nil {
			t.Fatal(err)
		}
		want = append(want, h.HazardID)
	}
	for run := 0; run < 2; run++ { // idempotent
		if err := db.backfillShardedProjection(ctx); err != nil {
			t.Fatal(err)
		}
	}
	shards := map[int]bool{}
	var got []string
	token := ""
	for {
		page, err := db.Backend().Hazards.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 7, Token: token})
		if err != nil || page.Partial {
			t.Fatalf("list: %v partial=%v", err, page.Partial)
		}
		for i, h := range page.Items {
			got = append(got, h.HazardID)
			shards[projectionShard(h.HazardID)] = true
			if i > 0 && listOrderBefore(h, page.Items[i-1]) {
				t.Fatalf("page out of order at %d", i)
			}
		}
		if page.NextToken == "" {
			break
		}
		token = page.NextToken
	}
	if len(got) != len(want) {
		t.Fatalf("listed %d of %d backfilled rows", len(got), len(want))
	}
	seen := map[string]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("%s listed twice", id)
		}
		seen[id] = true
	}
	if len(shards) < projectionShards/2 {
		t.Fatalf("one bucket's rows landed in only %d shards", len(shards))
	}
}
