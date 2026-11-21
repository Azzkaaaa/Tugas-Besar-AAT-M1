//go:build integration

package cassandra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
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

func TestProblem3InboxDiscoveryPastResolvedHistory(t *testing.T) {
	db, err := openTest(t, "agg_test_inbox_pages_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer dropKeyspace(db)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var want []string
	for offset := 0; offset < 270; offset += 30 {
		batch := db.session.Batch(gocql.UnloggedBatch).WithContext(ctx)
		for i := offset; i < min(270, offset+30); i++ {
			id := fmt.Sprintf("msg-00%062x", i)
			source, state := "PVMBG", storage.IngestResolved
			if i >= 220 && i%3 == 0 {
				state = storage.IngestParked
				want = append(want, id)
			} else if i%2 == 0 {
				source, state = "BMKG", storage.IngestPending
			}
			// Discovery-only fixture, in its own disposable keyspace; no raw
			// blobs or CAS/sequence setup are needed for this SELECT contract.
			batch.Query(`INSERT INTO ingest_inbox (shard,message_id,source,state,received_at) VALUES (?,?,?,?,?)`, 0, id, source, state, time.Now().UTC())
		}
		if err := batch.Exec(); err != nil {
			t.Fatal(err)
		}
	}
	for _, limit := range []int{7, 1000} {
		cursor := storage.IngestCursor{}
		var got []string
		empty := 0
		for calls := 0; ; calls++ {
			if calls > 100 {
				t.Fatal("pagination never completed")
			}
			page, err := (ingestInbox{db}).ListMessages(ctx, "PVMBG", cursor, limit)
			if err != nil || len(page.UnavailableUnits) > 0 {
				t.Fatalf("healthy bounded scan was partial: %+v %v", page, err)
			}
			for _, m := range page.Items {
				got = append(got, m.ID)
			}
			if page.Next == nil {
				break
			}
			if *page.Next == cursor {
				t.Fatal("cursor stalled")
			}
			if len(page.Items) == 0 {
				empty++
			}
			cursor = *page.Next
		}
		if strings.Join(got, ",") != strings.Join(want, ",") || empty < 3 {
			t.Fatalf("limit=%d history hid or duplicated pending messages: got=%v want=%v empty=%d", limit, got, want, empty)
		}
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
