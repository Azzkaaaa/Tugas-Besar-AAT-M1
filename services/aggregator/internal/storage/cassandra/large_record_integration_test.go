//go:build integration

package cassandra

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/ingest"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func largeSatelliteRecord(t *testing.T) *domain.HazardRecord {
	t.Helper()
	n := 2000
	if raw := os.Getenv("AGG_TEST_LARGE_FIRE_KEYS"); raw != "" {
		var err error
		n, err = strconv.Atoi(raw)
		if err != nil || n < 1000 || n > 200000 {
			t.Fatal("AGG_TEST_LARGE_FIRE_KEYS must be 1000..200000")
		}
	}
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("N20|2026-11-19|0600|%.8f|106.12345678", -6.1+float64(i)*.00000001)
	}
	return &domain.HazardRecord{Source: domain.SourceSatellite, SourceRefID: "large-" + uuid.NewString(), HazardType: domain.HazardTypeWildfire, Severity: domain.SeverityAwas, AreaName: "test", Latitude: -6.1, Longitude: 106.1, OccurredAt: time.Now().UTC().Truncate(time.Millisecond), IngestedAt: time.Now().UTC().Truncate(time.Millisecond), Attributes: map[string]any{"detection_keys": keys, "detection_count": n, "satellites": []string{"N20"}, "max_confidence": "n", "total_frp_mw": float64(n) * 10, "stream_sequence": "1", "withdrawn": false}}
}

func largeSatelliteEvent(h *domain.HazardRecord) (storage.OutboxEvent, error) {
	id, payload, err := ingest.BuildPublishedEnvelope(h, "large-fire-test")
	return storage.OutboxEvent{EventID: id, HazardID: h.HazardID, Source: h.Source, Payload: payload}, err
}

func TestLargeSatelliteRecordAndOutbox(t *testing.T) {
	db, err := openTest(t, "agg_test_large_fire_"+uuid.NewString()[:8], time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer dropKeyspace(db)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	b := db.Backend()
	record := largeSatelliteRecord(t)
	n := len(record.Attributes["detection_keys"].([]string))
	started := time.Now()
	saved, changed, err := b.Hazards.ApplyRevision(ctx, record, largeSatelliteEvent)
	if err != nil || !changed {
		t.Fatal("large canonical/projection write failed", changed, err)
	}
	if got, err := b.Hazards.Get(ctx, saved.HazardID); err != nil || got == nil || len(got.Attributes["detection_keys"].([]any)) != n {
		t.Fatal("large record read lost keys", err)
	}
	events, err := b.Outbox.ClaimDue(ctx, 1, time.Minute)
	if err != nil || len(events) != 1 {
		t.Fatal("large outbox claim failed", len(events), err)
	}
	var envelope eventv1.HazardEventPublished
	if err := proto.Unmarshal(events[0].Payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.GetPayload().GetAttributes().GetFields()["detection_keys"].GetListValue().GetValues()) != n {
		t.Fatal("published envelope lost keys")
	}
	if err := b.Outbox.Ack(ctx, events[0].EventID); err != nil {
		t.Fatal("large outbox acknowledgment failed", err)
	}
	if events, err := b.Outbox.ClaimDue(ctx, 1, time.Minute); err != nil || len(events) != 0 {
		t.Fatal("acknowledged large event reappeared", len(events), err)
	}
	if got, changed, err := b.Hazards.ApplyRevision(ctx, record, largeSatelliteEvent); err != nil || changed || got.Revision != 1 {
		t.Fatal("large replay changed revision", changed, err)
	}
	// A later large revision must support retry/release and permanent parking,
	// without making pending work disappear if the dead-letter store fails.
	record.Attributes["stream_sequence"] = "2"
	record.Attributes["total_frp_mw"] = float64(n) * 20
	second, changed, err := b.Hazards.ApplyRevision(ctx, record, largeSatelliteEvent)
	if err != nil || !changed || second.Revision != 2 {
		t.Fatal("large revision failed", changed, err)
	}
	events, err = b.Outbox.ClaimDue(ctx, 1, time.Minute)
	if err != nil || len(events) != 1 {
		t.Fatal(len(events), err)
	}
	if err := b.Outbox.RetryLater(ctx, events[0].EventID, "transient", 0); err != nil {
		t.Fatal(err)
	}
	events, err = b.Outbox.ClaimDue(ctx, 1, time.Minute)
	if err != nil || len(events) != 1 || events[0].RetryCount != 1 {
		t.Fatal("large retry unavailable", len(events), err)
	}
	if err := b.Outbox.Release(ctx, []string{events[0].EventID}); err != nil {
		t.Fatal(err)
	}
	if err := db.session.Query(`DROP TABLE outbox_dead_letters`).WithContext(ctx).Exec(); err != nil {
		t.Fatal(err)
	}
	if err := b.Outbox.DeadLetter(ctx, events[0], "permanent"); err == nil {
		t.Fatal("dead-letter outage ignored")
	}
	var pending int
	if err := db.session.Query(`SELECT COUNT(*) FROM outbox_pending WHERE shard=?`, pendingPartition(record.Source)).WithContext(ctx).Scan(&pending); err != nil || pending != 1 {
		t.Fatal("failed dead-letter removed pending work", pending, err)
	}
	restoreBaselineTable(t, ctx, db, "outbox_dead_letters")
	if err := b.Outbox.DeadLetter(ctx, events[0], "permanent"); err != nil {
		t.Fatal(err)
	}
	var parked []byte
	if err := db.session.Query(`SELECT payload FROM outbox_dead_letters WHERE event_id=?`, events[0].EventID).WithContext(ctx).Scan(&parked); err != nil {
		t.Fatal(err)
	}
	var parkedEnvelope eventv1.HazardEventPublished
	if err := proto.Unmarshal(parked, &parkedEnvelope); err != nil || len(parkedEnvelope.GetPayload().GetAttributes().GetFields()["detection_keys"].GetListValue().GetValues()) != n {
		t.Fatal("large dead-letter payload changed", err)
	}
	if events, err := b.Outbox.ClaimDue(ctx, 1, time.Minute); err != nil || len(events) != 0 {
		t.Fatal("dead-lettered event reappeared", len(events), err)
	}
	var rss, peak string
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				rss = strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
			}
		}
	}
	if value, err := os.ReadFile("/sys/fs/cgroup/memory.peak"); err == nil {
		peak = strings.TrimSpace(string(value))
	}
	t.Logf("keys=%d protobuf_bytes=%d peak_rss=%s container_peak_bytes=%s elapsed=%s", n, proto.Size(&envelope), rss, peak, time.Since(started))
}

// A failed write to the second durable outbox copy must leave the projection
// completion marker absent. Retrying the canonical revision then repairs it.
func TestLargeSatelliteInterruptedOutboxRecovers(t *testing.T) {
	db, err := openTest(t, "agg_test_large_recover_"+uuid.NewString()[:8], time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer dropKeyspace(db)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	b := db.Backend()
	record := largeSatelliteRecord(t)
	if err := db.session.Query(`DROP TABLE outbox_pending`).WithContext(ctx).Exec(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Hazards.ApplyRevision(ctx, record, largeSatelliteEvent); err == nil {
		t.Fatal("missing pending table did not fail")
	}
	hazardID := domain.HazardID(record.Source, record.SourceRefID)
	canonical, err := b.Hazards.Get(ctx, hazardID)
	if err != nil || canonical == nil || canonical.Revision != 1 {
		t.Fatal("canonical commit unavailable", err)
	}
	st, err := (hazards{db}).readStored(ctx, hazardID, false)
	if err != nil {
		t.Fatal(err)
	}
	if done, err := (hazards{db}).writtenOut(ctx, st); err != nil || done {
		t.Fatal("incomplete outbox marked complete", done, err)
	}
	restoreBaselineTable(t, ctx, db, "outbox_pending")
	saved, _, err := b.Hazards.ApplyRevision(ctx, record, largeSatelliteEvent)
	if err != nil || saved.Revision != 1 {
		t.Fatal("partial write did not recover at same revision", err)
	}
	events, err := b.Outbox.ClaimDue(ctx, 1, time.Minute)
	if err != nil || len(events) != 1 {
		t.Fatal("recovery lost outbox event", len(events), err)
	}
	if err := b.Outbox.Ack(ctx, events[0].EventID); err != nil {
		t.Fatal(err)
	}
}

// Recreate the exact versioned table only inside this disposable keyspace.
func restoreBaselineTable(t *testing.T, ctx context.Context, db *DB, table string) {
	t.Helper()
	baseline, err := migrationFiles.ReadFile("migrations/0001_baseline.cql")
	if err != nil {
		t.Fatal(err)
	}
	created := false
	for _, stmt := range splitStatements(string(baseline)) {
		if strings.HasPrefix(stmt, "CREATE TABLE IF NOT EXISTS "+table+" (") {
			if err := db.session.Query(stmt).WithContext(ctx).Exec(); err != nil {
				t.Fatal(err)
			}
			created = true
		}
	}
	if !created {
		t.Fatal("baseline schema missing", table)
	}
}
