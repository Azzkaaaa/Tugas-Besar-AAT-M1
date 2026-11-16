//go:build integration

package cassandra

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

func TestProblem3DurableFieldReport(t *testing.T) {
	keyspace := "agg_test_field_reports_" + uuid.NewString()[:8]
	db, err := openTest(t, keyspace, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	report := storage.FieldReport{ID: "fr-" + uuid.NewString(), Actor: "field-test", ObservedAt: time.Now().UTC(),
		AreaName: "  Kota   Padang ", Description: " Rising water ", ReceivedAt: time.Now().UTC()}
	if err := db.Backend().Reports.Accept(ctx, report); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	worker, err := openTest(t, keyspace, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(worker) })
	reports := worker.Backend().Reports
	if err := reports.Accept(ctx, report); err != nil {
		t.Fatal(err)
	}
	conflict := report
	conflict.Description = "different"
	if err := reports.Accept(ctx, conflict); !errors.Is(err, storage.ErrReportConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
	if count, err := reports.ProcessReports(ctx); err != nil || count != 1 {
		t.Fatalf("expected one completion, got %d, %v", count, err)
	}
	if count, err := reports.ProcessReports(ctx); err != nil || count != 0 {
		t.Fatalf("worker processed a completed report again: %d, %v", count, err)
	}
	processed, err := reports.GetReport(ctx, report.ID)
	if err != nil || processed == nil || processed.ProcessedAt.IsZero() || !processed.ObservedAt.Equal(report.ObservedAt) ||
		processed.AreaName != "Kota Padang" || processed.Description != "Rising water" {
		t.Fatalf("processed report missing or changed: %+v, %v", processed, err)
	}
}

func TestProblem3InboxRawAndState(t *testing.T) {
	db, err := openTest(t, "agg_test_ingest_inbox_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	ctx := context.Background()
	inbox := db.Backend().Inbox
	id := uuid.NewString()
	chunks := [][]byte{[]byte("first"), {0, 0xff, '\n'}}
	for i, chunk := range chunks {
		if err := inbox.ArchiveChunk(ctx, id, i, chunk); err != nil {
			t.Fatal(err)
		}
	}
	message := storage.IngestMessage{ID: "msg-00" + strings.Repeat("a", 62), Source: "PVMBG", Stream: "pvmbg_volcanic", SourceRefID: "r1",
		ReceivedAt: time.Now().UTC(), State: storage.IngestParked, Reason: "invalid_utf8", Attempts: 1,
		ArchiveID: id, ArchiveChunks: len(chunks)}
	if err := inbox.Put(ctx, message); err != nil {
		t.Fatal(err)
	}
	stored, err := inbox.GetMessage(ctx, message.ID)
	if err != nil || stored == nil || stored.Sequence != 1 {
		t.Fatalf("message missing: %+v, %v", stored, err)
	}
	later := message
	later.ID = "msg-00" + strings.Repeat("b", 62)
	later.ArchiveID, later.ArchiveChunks = "", 0
	if err := inbox.Put(ctx, later); err != nil {
		t.Fatal(err)
	}
	second, err := inbox.GetMessage(ctx, later.ID)
	if err != nil || second == nil || second.Sequence != stored.Sequence+1 {
		t.Fatalf("incorrect committed sequence: %+v, %v", second, err)
	}
	var raw bytes.Buffer
	if err := inbox.WritePayload(ctx, *stored, &raw); err != nil || !bytes.Equal(raw.Bytes(), bytes.Join(chunks, nil)) {
		t.Fatalf("archive bytes changed: %x, %v", raw.Bytes(), err)
	}
	if ok, err := inbox.ChangeState(ctx, *stored, storage.IngestResolved, "", 2, time.Time{}); err != nil || !ok {
		t.Fatalf("state change failed: %v", err)
	}
	page, err := inbox.ListMessages(ctx, "PVMBG", storage.IngestCursor{}, 100)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != later.ID || len(page.UnavailableUnits) != 0 {
		t.Fatalf("resolved message still listed: %+v, %v", page, err)
	}
}
