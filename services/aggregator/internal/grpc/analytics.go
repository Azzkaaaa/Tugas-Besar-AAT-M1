package grpc

import (
	"context"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) SetAnalytics(reader storage.AnalyticsReader) { s.analytics = reader }

func (s *Server) GetAnalytics(ctx context.Context, req *aggregatorv1.GetAnalyticsRequest) (*aggregatorv1.GetAnalyticsResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if !caller.HasScope("hazards:read:summary") {
		return nil, status.Error(codes.PermissionDenied, "hazards:read:summary required")
	}
	if req.Source != "" && domain.SourceKeyFor(req.Source) == "" {
		return nil, status.Error(codes.InvalidArgument, "unknown source")
	}
	q := storage.AnalyticsQuery{Source: req.Source, Interval: req.Interval}
	if req.Since != nil {
		if !req.Since.IsValid() {
			return nil, status.Error(codes.InvalidArgument, "invalid since")
		}
		q.Since = req.Since.AsTime()
	}
	if req.Until != nil {
		if !req.Until.IsValid() {
			return nil, status.Error(codes.InvalidArgument, "invalid until")
		}
		q.Until = req.Until.AsTime()
	}
	if !q.Since.IsZero() && !q.Until.IsZero() && !q.Since.Before(q.Until) {
		return nil, status.Error(codes.InvalidArgument, "until must follow since")
	}
	switch q.Interval {
	case "", "hour", "day", "month", "year":
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid interval")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// One metadata scan per replica protects the ingest/write path from expensive
	// concurrent all-catalog queries. Saturation remains explicit and retryable.
	select {
	case s.analyticsSlots <- struct{}{}:
		defer func() { <-s.analyticsSlots }()
	default:
		return &aggregatorv1.GetAnalyticsResponse{Partial: true, UnavailableUnits: []string{"analytics-busy"}, AsOf: timestamppb.Now()}, nil
	}
	result := storage.Aggregate(ctx, s.analytics, q)
	out := &aggregatorv1.GetAnalyticsResponse{Total: result.Total, Categories: result.Categories, Severity: result.Severity, Partial: result.Partial, UnavailableUnits: result.UnavailableUnits, AsOf: timestamppb.New(result.AsOf)}
	for _, area := range result.Areas {
		out.Areas = append(out.Areas, &aggregatorv1.AnalyticsArea{AreaName: area.AreaName, Count: area.Count})
	}
	for _, point := range result.Series {
		out.Series = append(out.Series, &aggregatorv1.AnalyticsPoint{Bucket: timestamppb.New(point.Bucket), Counts: point.Counts})
	}
	return out, nil
}
