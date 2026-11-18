//go:build integration

package postgres

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func TestAuditIndexedPaginationChecksumAndLocation(t *testing.T) {
	dsn := os.Getenv("AGG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("AGG_TEST_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Pool.Close()
	if _, err := db.Pool.Exec(ctx, `TRUNCATE hazard_events, outbox_events, outbox_dead_letters, ingest_watermarks`); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	for i := range 30 {
		h := &domain.HazardRecord{Source: domain.SourceBMKG, SourceRefID: fmt.Sprintf("ref-%02d", 29-i), HazardType: domain.HazardTypeSeismic,
			Severity: domain.SeverityWaspada, OccurredAt: base.Add(-time.Duration(i) * time.Hour), IngestedAt: base.Add(time.Duration(i/3) * time.Millisecond)}
		h.AreaName = []string{"Padang", "Bali"}[i%2]
		if i%3 == 0 {
			h.Attributes = map[string]any{"tsunami_warnings": []any{map[string]any{"area": "Padang"}}}
		}
		if _, _, err := (hazards{db}).ApplyRevision(ctx, h, func(h *domain.HazardRecord) (storage.OutboxEvent, error) {
			return storage.OutboxEvent{EventID: h.HazardID, Source: h.Source, Payload: []byte(`{}`)}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	reader := audit{db}
	after := storage.AuditPosition{}
	ids := map[string]bool{}
	var last time.Time
	for {
		page, err := reader.Events(ctx, storage.AuditQuery{Source: "BMKG", Since: base.Add(2 * time.Millisecond), After: after, Limit: 4})
		if err != nil || len(page.UnavailableUnits) > 0 {
			t.Fatalf("%v %v", page.UnavailableUnits, err)
		}
		for _, h := range page.Items {
			if ids[h.HazardID] || h.IngestedAt.Before(last) {
				t.Fatal("duplicate/out-of-order audit row")
			}
			ids[h.HazardID] = true
			last = h.IngestedAt
		}
		if !page.More {
			break
		}
		tail := page.Items[len(page.Items)-1]
		after = storage.AuditPosition{At: tail.IngestedAt, ID: tail.HazardID}
	}
	if len(ids) != 24 {
		t.Fatalf("since uses the wrong clock: %d", len(ids))
	}
	refs := []string{}
	units, err := reader.References(ctx, "BMKG", func(ref string) error { refs = append(refs, ref); return nil })
	if err != nil || len(units) > 0 || len(refs) != 30 || !sort.StringsAreSorted(refs) {
		t.Fatalf("references %v %v", units, err)
	}
	aggregate := storage.Aggregate(ctx, reader, storage.AnalyticsQuery{Interval: "day"})
	if aggregate.Partial || aggregate.Total != int64(len(refs)) || aggregate.Categories["SEISMIC"] != 30 || aggregate.Categories["TSUNAMI"] != 10 || aggregate.Severity["WASPADA"] != 30 || len(aggregate.Areas) != 2 || aggregate.Areas[0].Count != 15 {
		t.Fatalf("analytics does not match Audit: %+v", aggregate)
	}
	filtered := storage.Aggregate(ctx, reader, storage.AnalyticsQuery{Since: base.Add(-4 * time.Hour), Until: base, Interval: "hour"})
	if filtered.Partial || filtered.Total != 4 || filtered.Categories["TSUNAMI"] != 1 || len(filtered.Series) != 4 {
		t.Fatalf("analytics range incorrect: %+v", filtered)
	}
	location, err := reader.Locate(ctx, domain.HazardID("BMKG", "ref-01"))
	if err != nil || !location.Exists || len(location.Locations) != 1 || location.Locations[0].Node != "integration-node" {
		t.Fatalf("%+v %v", location, err)
	}
}
