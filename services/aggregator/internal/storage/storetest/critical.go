package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func CriticalJournal(t *testing.T, b *storage.Backend, eraseDownstream func()) {
	t.Helper()
	ctx := context.Background()
	publication := func(h *domain.HazardRecord) (storage.OutboxEvent, error) {
		return storage.OutboxEvent{EventID: domain.EventID(h.HazardID, h.Revision), HazardID: h.HazardID, Source: h.Source, Payload: []byte("outbox"), TraceContext: map[string]string{"traceparent": "fixture"}}, nil
	}
	h := &domain.HazardRecord{Source: domain.SourceCoastal, SourceRefID: "critical-history", HazardType: domain.HazardTypeCoastal, Severity: domain.SeverityAwas, OccurredAt: time.Now().UTC(), Attributes: map[string]any{"critical": true, "wave_height_m": 2.0}}
	for i := range 2 {
		h.Attributes["wave_height_m"] = float64(2 + i)
		if _, _, err := b.Hazards.ApplyRevision(ctx, h, publication); err != nil {
			t.Fatal(err)
		}
	}
	h.Attributes["critical"] = false
	h.Severity = domain.SeverityNormal
	if _, _, err := b.Hazards.ApplyRevision(ctx, h, publication); err != nil {
		t.Fatal(err)
	}
	bad := *h
	bad.SourceRefID = "rolled-back-critical"
	bad.Attributes = map[string]any{"critical": true}
	_, _, err := b.Hazards.ApplyRevision(ctx, &bad, func(*domain.HazardRecord) (storage.OutboxEvent, error) {
		return storage.OutboxEvent{}, errors.New("build failed")
	})
	if err == nil {
		t.Fatal("fixture error was swallowed")
	}
	eraseDownstream()
	read := func(pending bool) []storage.CriticalEvent {
		t.Helper()
		cursor := storage.CriticalCursor{}
		all := []storage.CriticalEvent{}
		for pages := 0; pages < 40; pages++ {
			var page storage.CriticalPage
			var err error
			if pending {
				page, err = b.Critical.Pending(ctx, cursor, 1)
			} else {
				page, err = b.Critical.List(ctx, cursor, 1)
			}
			if err != nil || len(page.UnavailableUnits) > 0 {
				t.Fatal(page, err)
			}
			all = append(all, page.Events...)
			if page.Next == nil {
				return all
			}
			cursor = *page.Next
		}
		t.Fatal("critical pagination never completed")
		return nil
	}
	events := read(true)
	if len(events) != 2 {
		t.Fatalf("lost/synthetic critical revisions after downstream erasure: %+v", events)
	}
	for _, e := range events {
		var body struct {
			Data domain.HazardRecord `json:"data"`
		}
		if err := json.Unmarshal(e.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if body.Data.Attributes["critical"] != true || body.Data.Revision > 2 || e.EventID != domain.EventID(body.Data.HazardID, body.Data.Revision) || e.CreatedAt.IsZero() {
			t.Fatal("historical critical content lost", e)
		}
		if e.TraceContext["traceparent"] != "fixture" {
			t.Fatal("trace lost", e)
		}
		receipt := storage.CriticalReceipt{EventID: e.EventID, HazardID: e.HazardID, ReceiverID: "field-team", PayloadSHA256: "wrong", ReceivedAt: time.Now().UTC(), Signature: "verified-by-api"}
		if err := b.Critical.RecordReceipt(ctx, receipt); !errors.Is(err, storage.ErrCriticalReceiptConflict) {
			t.Fatal("wrong payload acknowledged", err)
		}
		receipt.PayloadSHA256 = e.PayloadSHA256
		if err := b.Critical.RecordReceipt(ctx, receipt); err != nil {
			t.Fatal(err)
		}
		if err := b.Critical.RecordReceipt(ctx, receipt); err != nil {
			t.Fatal("receipt retry is not idempotent", err)
		}
		got, err := b.Critical.Get(ctx, e.HazardID, e.EventID)
		if err != nil || got.Receipt == nil || got.Receipt.RecordedAt.IsZero() {
			t.Fatal("durable receiver receipt missing", got, err)
		}
	}
	if len(read(true)) != 0 || len(read(false)) != 2 {
		t.Fatal("receipt erased history or left delivery pending")
	}
	if _, _, err := b.Hazards.ApplyRevision(ctx, h, publication); err != nil {
		t.Fatal(err)
	}
	if len(read(true)) != 0 {
		t.Fatal("source replay resurrected acknowledged delivery")
	}
	versions, err := b.Critical.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	for _, v := range versions {
		count += v
	}
	if count != 2 {
		t.Fatal("critical generations did not commit atomically", versions)
	}
	h.Attributes["critical"] = true
	h.Severity = domain.SeverityAwas
	if _, _, err := b.Hazards.ApplyRevision(ctx, h, publication); err != nil {
		t.Fatal(err)
	}
	snapshot := []storage.CriticalEvent{}
	cursor := storage.CriticalCursor{}
	for pages := 0; pages < 40; pages++ {
		page, err := b.Critical.ListSnapshot(ctx, cursor, 1, versions)
		if err != nil || len(page.UnavailableUnits) > 0 {
			t.Fatal(page, err)
		}
		snapshot = append(snapshot, page.Events...)
		if page.Next == nil {
			break
		}
		cursor = *page.Next
	}
	if len(snapshot) != 2 {
		t.Fatal("live write changed an immutable snapshot", snapshot)
	}
	for _, e := range read(true) {
		if err := b.Critical.RecordReceipt(ctx, storage.CriticalReceipt{EventID: e.EventID, HazardID: e.HazardID, ReceiverID: "field-team", PayloadSHA256: e.PayloadSHA256, ReceivedAt: time.Now(), Signature: "verified-by-api"}); err != nil {
			t.Fatal(err)
		}
	}
}
