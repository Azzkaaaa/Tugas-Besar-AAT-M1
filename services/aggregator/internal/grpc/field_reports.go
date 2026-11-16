package grpc

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fieldReportService struct {
	aggregatorv1.UnimplementedFieldReportServiceServer
	store storage.FieldReports
}

func (s fieldReportService) SubmitFieldReport(ctx context.Context, req *aggregatorv1.SubmitFieldReportRequest) (*aggregatorv1.SubmitFieldReportResponse, error) {
	actor, _ := ctx.Value(auth.CallerContextKey).(*auth.CallerContextData)
	if actor == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	if actor.Subject == "" || actor.Role != "FIELD_TEAM" || !actor.HasScope("field-reports:write") {
		return nil, status.Error(codes.PermissionDenied, "field report writer required")
	}
	key, err := uuid.Parse(req.GetIdempotencyKey())
	if err != nil || !strings.EqualFold(key.String(), req.GetIdempotencyKey()) {
		return nil, status.Error(codes.InvalidArgument, "Idempotency-Key must be a UUID")
	}
	observed, err := time.Parse(time.RFC3339Nano, req.GetObservedAt())
	area := strings.TrimSpace(req.GetAreaName())
	description := strings.TrimSpace(req.GetDescription())
	if err != nil || area == "" || len(req.GetAreaName()) > 256 || description == "" ||
		len(req.GetDescription())+len(req.GetAreaName())+len(req.GetObservedAt()) > 16<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid field report")
	}
	if s.store == nil {
		return nil, status.Error(codes.Unavailable, "field report store is unavailable")
	}
	// The fixed-width canonical UUID suffix makes the subject/key encoding unambiguous.
	id := "fr-" + uuid.NewSHA1(uuid.NameSpaceOID, []byte("bnpb/field-report/"+actor.Subject+"/"+key.String())).String()
	correlation := metadata.ValueFromIncomingContext(ctx, "x-correlation-id")
	correlationID := ""
	if len(correlation) == 1 && len(correlation[0]) <= 128 {
		correlationID = correlation[0]
	}
	report := storage.FieldReport{ID: id, Actor: actor.Subject, ObservedAt: observed.UTC(), AreaName: req.GetAreaName(),
		Description: req.GetDescription(), ReceivedAt: time.Now().UTC(), CorrelationID: correlationID}
	if err := s.store.Accept(ctx, report); errors.Is(err, storage.ErrReportConflict) {
		return nil, status.Error(codes.AlreadyExists, "idempotency key already used for different report")
	} else if err != nil {
		return nil, status.Error(codes.Unavailable, "field report durability could not be confirmed")
	}
	slog.InfoContext(ctx, "field report receipt", "report_id", id, "correlation_id", correlationID)
	return &aggregatorv1.SubmitFieldReportResponse{ReportId: id}, nil
}
