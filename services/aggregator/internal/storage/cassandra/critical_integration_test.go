//go:build integration

package cassandra

import (
	"context"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage/storetest"
	"github.com/google/uuid"
)

func TestCriticalJournalSurvivesDownstreamErasure(t *testing.T) {
	db, err := openTest(t, "agg_test_critical_"+uuid.NewString()[:8], time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropKeyspace(db) })
	storetest.CriticalJournal(t, db.Backend(), func() {
		for _, table := range []string{"outbox_events", "outbox_pending"} {
			if err := db.TruncateForTest(table); err != nil {
				t.Fatal(err)
			}
		}
	})
	ctx := context.Background()
	h := &domain.HazardRecord{HazardID: "ignored", Source: domain.SourceCoastal, SourceRefID: "imported-critical", HazardType: domain.HazardTypeCoastal, Severity: domain.SeverityAwas, Revision: 7, OccurredAt: time.Now().UTC(), IngestedAt: time.Now().UTC(), Attributes: map[string]any{"critical": true}}
	for i := range 2 {
		if i == 1 {
			h.Revision = 8
		}
		if err := db.ImportHazard(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := db.Backend().Critical.Pending(ctx, storage.CriticalCursor{}, 100)
	if err != nil || len(pending.Events) != 2 {
		t.Fatal("imported critical revisions lost", pending, err)
	}
	for _, e := range pending.Events {
		if err := db.Backend().Critical.RecordReceipt(ctx, storage.CriticalReceipt{EventID: e.EventID, HazardID: e.HazardID, ReceiverID: "field-team", PayloadSHA256: e.PayloadSHA256, ReceivedAt: time.Now(), Signature: "checked-by-api"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.ImportHazard(ctx, h); err != nil {
		t.Fatal("idempotent import failed", err)
	}
	pending, err = db.Backend().Critical.Pending(ctx, storage.CriticalCursor{}, 100)
	if err != nil || len(pending.Events) != 0 {
		t.Fatal("import resurrected acknowledged delivery", pending, err)
	}
	if err := db.backfillAudit(ctx); err != nil {
		t.Fatal(err)
	}
	auditPage, err := db.Backend().Audit.Events(ctx, storage.AuditQuery{Source: domain.SourceCoastal, Limit: 100})
	if err != nil || len(auditPage.Items) != 2 {
		t.Fatal("internal journal rows leaked into Audit", auditPage, err)
	}
}
