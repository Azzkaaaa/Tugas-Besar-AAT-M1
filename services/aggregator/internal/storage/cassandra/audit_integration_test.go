//go:build integration

package cassandra

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

func TestAuditIndexedPaginationChecksumWithdrawalAndLocation(t *testing.T) {
	db, err := openTest(t, "agg_test_audit_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	db.nodeFromRack = true
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	base := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	want := []*domain.HazardRecord{}
	for i := range 1050 {
		source := []string{domain.SourcePVMBG, domain.SourceBMKG, domain.SourceSatellite, domain.SourceCoastal}[i%4]
		kind := []string{domain.HazardTypeVolcanic, domain.HazardTypeSeismic, domain.HazardTypeWildfire, domain.HazardTypeCoastal}[i%4]
		h := &domain.HazardRecord{Source: source, SourceRefID: fmt.Sprintf("ref-%04d", 1049-i), HazardType: kind, Severity: domain.SeverityWaspada,
			OccurredAt: base.Add(-time.Duration(i) * time.Hour), IngestedAt: base.Add(time.Duration(i/3) * time.Millisecond), Revision: 1}
		h.HazardID = domain.HazardID(h.Source, h.SourceRefID)
		h.AreaName = []string{"Padang", "Bali", "Kalimantan"}[i%3]
		if i%8 == 1 {
			h.Attributes = map[string]any{"tsunami_warnings": []any{map[string]any{"area": "Padang"}, map[string]any{"area": "Bali"}}}
		}
		if err := db.ImportHazard(ctx, h); err != nil {
			t.Fatal(err)
		}
		want = append(want, h)
	}
	// Verify that a stale projection/backfill cannot reactivate a withdrawn event.
	withdrawn := *want[5]
	withdrawn.Revision = 5
	withdrawn.Attributes = map[string]any{"withdrawn": true}
	if err := db.ImportHazard(ctx, &withdrawn); err != nil {
		t.Fatal(err)
	}
	b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	addAuditProjection(b, want[5])
	if err := b.Exec(); err != nil {
		t.Fatal(err)
	}
	since := base.Add(12 * time.Millisecond)
	filtered := []*domain.HazardRecord{}
	for i, h := range want {
		if i != 5 && !h.IngestedAt.Before(since) {
			filtered = append(filtered, h)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].IngestedAt.Equal(filtered[j].IngestedAt) {
			return filtered[i].HazardID < filtered[j].HazardID
		}
		return filtered[i].IngestedAt.Before(filtered[j].IngestedAt)
	})
	reader := audit{db}
	got := []string{}
	after := storage.AuditPosition{}
	for {
		page, err := reader.Events(ctx, storage.AuditQuery{Since: since, After: after, Limit: 117})
		if err != nil || len(page.UnavailableUnits) > 0 {
			t.Fatalf("page: %v %v", page.UnavailableUnits, err)
		}
		for _, h := range page.Items {
			got = append(got, h.HazardID)
		}
		if !page.More {
			break
		}
		if len(page.Items) == 0 {
			t.Fatal("empty continuation")
		}
		last := page.Items[len(page.Items)-1]
		after = storage.AuditPosition{At: last.IngestedAt, ID: last.HazardID}
	}
	ids := []string{}
	for _, h := range filtered {
		ids = append(ids, h.HazardID)
	}
	if !reflect.DeepEqual(got, ids) {
		t.Fatalf("incorrect indexed pagination: %d/%d", len(got), len(ids))
	}
	refs := []string{}
	units, err := reader.References(ctx, "", func(ref string) error { refs = append(refs, ref); return nil })
	if err != nil || len(units) > 0 || len(refs) != 1049 || !sort.StringsAreSorted(refs) {
		t.Fatalf("references: %d %v %v", len(refs), units, err)
	}
	for _, ref := range refs {
		if ref == withdrawn.SourceRefID {
			t.Fatal("withdrawal was resurrected by stale backfill")
		}
	}
	// Compare each aggregate dimension directly with the current Audit catalog,
	// including more than 1,000 rows and all five analytics categories.
	counts := map[string]int64{}
	areas := map[string]int64{}
	for i, h := range want {
		if i == 5 {
			continue
		}
		counts[h.HazardType]++
		if storage.HasTsunami(h) {
			counts["TSUNAMI"]++
		}
		areas[h.AreaName]++
	}
	aggregate := storage.Aggregate(ctx, reader, storage.AnalyticsQuery{Interval: "day"})
	if aggregate.Partial || aggregate.Total != int64(len(refs)) {
		t.Fatalf("analytics does not match Audit: %+v", aggregate)
	}
	for key, n := range counts {
		if aggregate.Categories[key] != n {
			t.Fatalf("category %s: %d/%d", key, aggregate.Categories[key], n)
		}
	}
	for _, area := range aggregate.Areas {
		if areas[area.AreaName] != area.Count {
			t.Fatalf("area mismatch: %+v", area)
		}
	}
	if aggregate.Severity["WASPADA"] != aggregate.Total {
		t.Fatal("severity mismatch")
	}
	var seriesTotal int64
	for _, point := range aggregate.Series {
		for key, n := range point.Counts {
			if key != "TSUNAMI" {
				seriesTotal += n
			}
		}
	}
	if seriesTotal != aggregate.Total {
		t.Fatal("time series does not reconcile")
	}
	location, err := reader.Locate(ctx, want[0].HazardID)
	// The single-container fixture's rack is "integration-node"; on a real ring set
	// AGG_TEST_CASSANDRA_NODES to the comma-separated Kubernetes node names.
	nodes := map[string]bool{"integration-node": true}
	if v := os.Getenv("AGG_TEST_CASSANDRA_NODES"); v != "" {
		nodes = map[string]bool{}
		for _, n := range strings.Split(v, ",") {
			nodes[strings.TrimSpace(n)] = true
		}
	}
	if err != nil || !location.Exists || len(location.Locations) != 1 || location.Locations[0].Role != "primary" || !nodes[location.Locations[0].Node] {
		t.Fatalf("location: %+v %v", location, err)
	}
}
