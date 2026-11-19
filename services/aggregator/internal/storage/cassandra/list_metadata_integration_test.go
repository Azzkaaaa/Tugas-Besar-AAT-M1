//go:build integration

package cassandra

import (
	"context"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

func TestListMetadataHandlesMissingAndRollingWriterCache(t *testing.T) {
	db, err := openTest(t, "agg_test_list_meta_"+uuid.NewString()[:8], time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer dropKeyspace(db)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	store := hazards{db}
	h := largeSatelliteRecord(t)
	keyCount := len(h.Attributes["detection_keys"].([]string))
	saved, _, err := store.ApplyRevision(ctx, h, largeSatelliteEvent)
	if err != nil {
		t.Fatal(err)
	}
	bucket := db.bucketOf(saved.OccurredAt)
	key := []any{bucket, projectionShard(saved.HazardID), saved.OccurredAt, saved.HazardID}
	check := func(want int, active bool, revision int) {
		t.Helper()
		page, err := store.List(ctx, storage.HazardQuery{SummaryOnly: true}, storage.PageRequest{Size: 100})
		if err != nil || page.Partial || len(page.Items) != want {
			t.Fatalf("summary count=%d partial=%v err=%v", len(page.Items), page.Partial, err)
		}
		if want > 0 && page.Items[0].Attributes != nil {
			t.Fatal("summary retained attributes")
		}
		var gotActive bool
		var gotRev int
		if err := db.session.Query(`SELECT list_active,list_revision FROM hazards_by_bucket_shard WHERE bucket=? AND shard=? AND occurred_at=? AND hazard_id=?`, key...).Scan(&gotActive, &gotRev); err != nil {
			t.Fatal(err)
		}
		if gotActive != active || gotRev != revision {
			t.Fatal("cache not refreshed to matching revision", gotActive, gotRev)
		}
	}
	check(1, true, 1)
	// A pre-upgrade row has no cache. Its bounded fallback fills metadata only.
	if err := db.session.Query(`DELETE list_active,list_revision,list_wire_bytes FROM hazards_by_bucket_shard WHERE bucket=? AND shard=? AND occurred_at=? AND hazard_id=?`, key...).Exec(); err != nil {
		t.Fatal(err)
	}
	check(1, true, 1)
	// Emulate a rolling prior binary updating the projection's revision/body
	// without knowing the new cache columns. Both withdrawal directions matter.
	for revision := 2; revision <= 3; revision++ {
		h.Attributes["withdrawn"] = revision == 2
		args := append([]any{mustJSON(h.Attributes), revision}, key...)
		if err := db.session.Query(`UPDATE hazards_by_bucket_shard SET attributes=?,revision=? WHERE bucket=? AND shard=? AND occurred_at=? AND hazard_id=?`, args...).Exec(); err != nil {
			t.Fatal(err)
		}
		want := 1
		if revision == 2 {
			want = 0
		}
		check(want, revision != 2, revision)
	}
	page, err := store.List(ctx, storage.HazardQuery{}, storage.PageRequest{Size: 100})
	if err != nil || page.Partial || len(page.Items) != 1 || page.Items[0].Revision != 3 || len(page.Items[0].Attributes["detection_keys"].([]any)) != keyCount {
		t.Fatal("full read after rolling cache repair", err, page.Partial, len(page.Items))
	}
	t.Run("SubMillisecondBounds", func(t *testing.T) {
		page, err := store.List(ctx, storage.HazardQuery{Since: saved.OccurredAt, Until: saved.OccurredAt.Add(time.Nanosecond), SummaryOnly: true}, storage.PageRequest{Size: 100})
		if err != nil || page.Partial || len(page.Items) != 1 {
			t.Fatal("exclusive bound rounded away a matching millisecond", err, page.Partial, len(page.Items))
		}
	})
}
