//go:build integration

package cassandra

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

func TestGetSummaryMetadataAndSharedFallback(t *testing.T) {
	db, err := openTest(t, "agg_test_get_meta_"+uuid.NewString()[:8], time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer dropKeyspace(db)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store := hazards{db}
	h := largeSatelliteRecord(t)
	saved, _, err := store.ApplyRevision(ctx, h, largeSatelliteEvent)
	if err != nil {
		t.Fatal(err)
	}
	id := saved.HazardID
	key := []any{auditShard(id), id}
	check := func(want *domain.HazardRecord) {
		t.Helper()
		got, err := store.GetSummary(ctx, id)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("summary mismatch", got, err)
		}
	}
	// A warm read must finish even while the cold-body slot is held.
	release, err := db.acquireSummaryFallback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	check(storage.HazardSummaryHeader(saved))
	release()
	// Cold Get and List requests on different backend handles share this slot.
	if err := db.session.Query(`DELETE summary_active,summary_revision FROM canonical_events WHERE shard=? AND hazard_id=?`, key...).Exec(); err != nil {
		t.Fatal(err)
	}
	bucket := db.bucketOf(saved.OccurredAt)
	if err := db.session.Query(`DELETE list_active,list_revision,list_wire_bytes FROM hazards_by_bucket_shard WHERE bucket=? AND shard=? AND occurred_at=? AND hazard_id=?`,
		bucket, projectionShard(id), saved.OccurredAt, id).Exec(); err != nil {
		t.Fatal(err)
	}
	release, err = db.acquireSummaryFallback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			readCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			defer stop()
			if got, err := db.Backend().Hazards.GetSummary(readCtx, id); got != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Error("cold Get bypassed shared admission", got, err)
			}
		})
	}
	wg.Go(func() {
		readCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		defer stop()
		page, _ := db.Backend().Hazards.List(readCtx, storage.HazardQuery{SummaryOnly: true}, storage.PageRequest{Size: 100})
		if !page.Partial || len(page.Items) != 0 || len(page.UnavailableUnits) == 0 || page.NextToken == "" {
			t.Error("cold List bypassed shared admission or lost partial marker", page)
		}
	})
	wg.Wait()
	release()
	check(storage.HazardSummaryHeader(saved))
	var active bool
	var rev int
	if err := db.session.Query(`SELECT summary_active,summary_revision FROM canonical_events WHERE shard=? AND hazard_id=?`, key...).Scan(&active, &rev); err != nil || !active || rev != 1 {
		t.Fatal("missing cache not repaired", active, rev, err)
	}
	// An older active row must not resurrect a withdrawn authoritative row.
	if err := db.session.Query(`INSERT INTO hazards_by_id (`+storedColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		storedValues(saved, bucket, "", time.Time{}, time.Time{})...).Exec(); err != nil {
		t.Fatal(err)
	}
	for _, withdrawn := range []bool{true, false} {
		rev++
		h.Attributes["withdrawn"] = withdrawn
		// A prior writer changes revision but knows nothing about the cache.
		if err := db.session.Query(`UPDATE canonical_events SET attributes=?,revision=? WHERE shard=? AND hazard_id=?`, mustJSON(h.Attributes), rev, key[0], id).Exec(); err != nil {
			t.Fatal(err)
		}
		saved.Revision = rev
		var want *domain.HazardRecord
		if !withdrawn {
			want = storage.HazardSummaryHeader(saved)
		}
		check(want)
	}
	// Legacy-only records also remain readable and use revision-checked fill.
	if err := db.session.Query(`DELETE FROM canonical_events WHERE shard=? AND hazard_id=?`, key...).Exec(); err != nil {
		t.Fatal(err)
	}
	saved.Revision = 1
	check(storage.HazardSummaryHeader(saved))
	release, err = db.acquireSummaryFallback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	check(storage.HazardSummaryHeader(saved))
	release()
	// Pre-revision legacy rows remain readable; null cannot masquerade as a
	// matching cache revision and must retain the normal revision-one view.
	if err := db.session.Query(`DELETE revision FROM hazards_by_id WHERE hazard_id=?`, id).Exec(); err != nil {
		t.Fatal(err)
	}
	check(storage.HazardSummaryHeader(saved))
	// Imports must populate activity atomically on both INSERT and UPDATE.
	imported := *saved
	imported.SourceRefID += "-import"
	imported.HazardID = domain.HazardID(imported.Source, imported.SourceRefID)
	for revision := 4; revision <= 5; revision++ {
		imported.Revision = revision
		imported.Attributes = map[string]any{"withdrawn": revision == 4}
		if err := db.ImportHazard(ctx, &imported); err != nil {
			t.Fatal(err)
		}
		release, err := db.acquireSummaryFallback(ctx)
		if err != nil {
			t.Fatal(err)
		}
		readCtx, stop := context.WithTimeout(ctx, time.Second)
		got, readErr := store.GetSummary(readCtx, imported.HazardID)
		stop()
		release()
		var want *domain.HazardRecord
		if revision == 5 {
			want = storage.HazardSummaryHeader(&imported)
		}
		if readErr != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("import summary metadata", revision, got, readErr)
		}
	}
	if got, err := store.GetSummary(ctx, criticalPrefix+"private"); got != nil || err != nil {
		t.Fatal("internal canonical row exposed", got, err)
	}
	var rss, peak string
	if value, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(value), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				rss = strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
			}
		}
	}
	if value, err := os.ReadFile("/sys/fs/cgroup/memory.peak"); err == nil {
		peak = strings.TrimSpace(string(value))
	}
	t.Logf("keys=%d concurrent_cold_waiters=13 peak_rss=%s container_peak_bytes=%s", len(h.Attributes["detection_keys"].([]string)), rss, peak)
}
