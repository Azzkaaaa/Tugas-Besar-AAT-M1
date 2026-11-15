package grpc

import (
	"context"
	"testing"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testFieldReports struct {
	storage.FieldReports
	reports map[string]storage.FieldReport
}

func (s testFieldReports) Accept(_ context.Context, report storage.FieldReport) error {
	if prior, exists := s.reports[report.ID]; exists {
		if prior.Description != report.Description {
			return storage.ErrReportConflict
		}
		return nil
	}
	s.reports[report.ID] = report
	return nil
}

func TestFieldReportSubjectAndPermission(t *testing.T) {
	store := testFieldReports{reports: make(map[string]storage.FieldReport)}
	svc := fieldReportService{store: store}
	req := &aggregatorv1.SubmitFieldReportRequest{IdempotencyKey: "1428bc5d-eb41-4c95-b3f2-976ee413ff03",
		ObservedAt: "2026-11-15T08:00:00Z", AreaName: "Padang", Description: "Rising water"}
	caller := func(sub, role string) context.Context {
		return context.WithValue(context.Background(), auth.CallerContextKey, &auth.CallerContextData{
			Subject: sub, Role: role, ClientID: "shared-application", Scopes: []string{"field-reports:write"},
		})
	}
	if _, err := svc.SubmitFieldReport(caller("media", "MEDIA"), req); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("media unexpectedly allowed: %v", err)
	}
	first, err := svc.SubmitFieldReport(caller("team1", "FIELD_TEAM"), req)
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{"team1", "team2"} {
		next, err := svc.SubmitFieldReport(caller(subject, "FIELD_TEAM"), req)
		if err != nil || (next.GetReportId() == first.GetReportId()) != (subject == "team1") {
			t.Fatalf("incorrect subject-specific deduplication: %v %v", next, err)
		}
	}
	req.Description = "Changed payload"
	if _, err := svc.SubmitFieldReport(caller("team1", "FIELD_TEAM"), req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("idempotency conflict not detected: %v", err)
	}
	if len(store.reports) != 2 {
		t.Fatalf("want 2 distinct rows, got %d", len(store.reports))
	}
}
