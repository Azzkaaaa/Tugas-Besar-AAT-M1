//go:build integration

package cassandra

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/ingest"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

func TestM2SharedStorageWithProblem3(t *testing.T) {
	db, err := openTest(t, "agg_test_p345_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	b := db.Backend()
	entry := storage.StreamEntry{Sequence: 1, Kind: "detection", Cursor: "0", Payload: []byte("original"), ReceivedAt: time.Now().UTC()}
	if _, err := b.Streams.Append(ctx, "test-stream", entry); err != nil {
		t.Fatal(err)
	}
	entry.Payload = []byte("must not overwrite")
	stored, err := b.Streams.Append(ctx, "test-stream", entry)
	if err != nil || string(stored.Payload) != "original" {
		t.Fatalf("journal lost immutability: %+v %v", stored, err)
	}
	if err := b.Streams.Commit(ctx, "test-stream", 0, storage.StreamCheckpoint{Sequence: 1, Cursor: "0"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Streams.Commit(ctx, "test-stream", 0, storage.StreamCheckpoint{Sequence: 2, Cursor: "1"}); !errors.Is(err, storage.ErrCheckpointConflict) {
		t.Fatalf("stale checkpoint accepted: %v", err)
	}
	for _, seq := range []int64{1000, 1001} {
		entry.Sequence = seq
		if _, err := b.Streams.Append(ctx, "partition-boundary", entry); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := b.Streams.Read(ctx, "partition-boundary", 999, 10)
	if err != nil || len(rows) != 2 || rows[1].Sequence != 1001 {
		t.Fatalf("cross-partition journal: %+v %v", rows, err)
	}

	fire := testHazard("fire", domain.SeveritySiaga)
	fire.Source, fire.HazardType = domain.SourceSatellite, domain.HazardTypeWildfire
	fire.Attributes = map[string]any{"stream_sequence": "2"}
	first, _, err := b.Hazards.ApplyRevision(ctx, fire, revEvent)
	if err != nil {
		t.Fatal(err)
	}
	fire.Attributes = map[string]any{"stream_sequence": "1"}
	fire.Severity = domain.SeverityWaspada
	got, changed, err := b.Hazards.ApplyRevision(ctx, fire, revEvent)
	if err != nil || changed || got.Revision != first.Revision || got.Severity != domain.SeveritySiaga {
		t.Fatalf("satellite sequence regressed: %+v %v", got, err)
	}
	fire.Attributes = map[string]any{"stream_sequence": "3", "withdrawn": true}
	fire.Severity = domain.SeverityNormal
	if _, _, err := b.Hazards.ApplyRevision(ctx, fire, revEvent); err != nil {
		t.Fatal(err)
	}
	page, err := b.Hazards.List(ctx, storage.HazardQuery{Source: domain.SourceSatellite}, storage.PageRequest{Size: 10})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("withdrawn fire still listed: %+v %v", page, err)
	}

	sequenced := b.Hazards.(storage.SequencedHazardStore)
	if _, _, err := sequenced.ApplyIngestRevision(ctx, testHazard("p3", domain.SeveritySiaga), 2, revEvent); err != nil {
		t.Fatal(err)
	}
	got, changed, err = sequenced.ApplyIngestRevision(ctx, testHazard("p3", domain.SeverityNormal), 1, revEvent)
	if err != nil || changed || got.Severity != domain.SeveritySiaga {
		t.Fatalf("P3 ordering regressed: %+v %v", got, err)
	}
	coast := testHazard("critical", domain.SeverityAwas)
	coast.Source, coast.HazardType = domain.SourceCoastal, domain.HazardTypeCoastal
	if _, _, err := b.Hazards.ApplyRevision(ctx, coast, revEvent); err != nil {
		t.Fatal(err)
	}
	events, err := b.Outbox.ClaimDue(ctx, 2, time.Minute)
	if err != nil || len(events) != 2 || events[0].Source != domain.SourceCoastal || events[1].Source == domain.SourceCoastal {
		t.Fatalf("coastal priority/regular fairness: %+v %v", events, err)
	}

	raw := bytes.Repeat([]byte{0xff, 0, '\n'}, 30000)
	for range 2 {
		if err := ingest.ParkExternal(ctx, b.Inbox, domain.SourceCoastal, raw, "invalid_utf8", "test", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	quarantine, err := b.Inbox.ListMessages(ctx, domain.SourceCoastal, storage.IngestCursor{}, 10)
	if err != nil || len(quarantine.Items) != 1 || len(quarantine.UnavailableUnits) != 0 {
		t.Fatalf("shared quarantine: %+v %v", quarantine, err)
	}
	m, err := b.Inbox.GetMessage(ctx, quarantine.Items[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	var captured bytes.Buffer
	if err := b.Inbox.WritePayload(ctx, *m, &captured); err != nil || !bytes.Equal(captured.Bytes(), raw) {
		t.Fatalf("quarantined bytes changed: %v", err)
	}
}
