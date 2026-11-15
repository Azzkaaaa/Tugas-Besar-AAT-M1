package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type Server struct {
	cfg     *config.Config
	hazards storage.HazardStore
	sources storage.SourceStateStore
	auth    *auth.JWTVerifier
	reports storage.FieldReports
	aggregatorv1.UnimplementedAggregatorServiceServer
}

func (s *Server) SetFieldReports(reports storage.FieldReports) { s.reports = reports }

func NewServer(cfg *config.Config, hazards storage.HazardStore, sources storage.SourceStateStore, verifier *auth.JWTVerifier) *Server {
	return &Server{cfg: cfg, hazards: hazards, sources: sources, auth: verifier}
}

func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", s.cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("listen on %s failed: %w", s.cfg.GRPCPort, err)
	}
	// OTel + JWT interceptor chain & stats handler.
	grpcSrv := grpc.NewServer(
		// One span per RPC from the stats handler; probes of grpc.health.v1 are not traced.
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(filters.Not(filters.HealthCheck())))),
		grpc.ChainUnaryInterceptor(
			s.auth.UnaryServerInterceptor(),
		),
	)
	// register service implementation.
	aggregatorv1.RegisterAggregatorServiceServer(grpcSrv, s)
	aggregatorv1.RegisterFieldReportServiceServer(grpcSrv, fieldReportService{store: s.reports})
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcSrv, healthSrv)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			fmt.Printf("gRPC server stopped: %v\n", err)
		}
	}()
	<-ctx.Done()
	grpcSrv.GracefulStop()
	return nil
}

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

// hazardQuery converts the API filter into the store's query.
func hazardQuery(f *commonv1.HazardFilter) storage.HazardQuery {
	var q storage.HazardQuery
	if f == nil {
		return q
	}
	q.Source = domain.SourceFromProto(f.Source)
	q.HazardType = domain.HazardTypeFromProto(f.HazardType)
	q.Severity = domain.SeverityFromProto(f.Severity)
	if f.Since != nil && f.Since.IsValid() {
		q.Since = f.Since.AsTime()
	}
	if f.Until != nil && f.Until.IsValid() {
		q.Until = f.Until.AsTime()
	}
	return q
}

// sourceFreshness reports the freshness of one source's ingest stream.
func (s *Server) sourceFreshness(ctx context.Context, source string) (*domain.SourceStatusInfo, error) {
	state, err := s.sources.Get(ctx, domain.SourceKeyFor(source))
	if err != nil {
		return nil, err
	}
	info := &domain.SourceStatusInfo{
		Source:        source,
		Status:        state.Status,
		LastSuccessAt: state.LastSuccessAt,
		LastAttemptAt: state.LastAttemptAt,
		ErrorMessage:  state.ErrorMessage,
	}
	hasCached, _ := s.hazards.HasData(ctx, source)
	info.CalculateFreshness(s.cfg.SourceStaleThreshold, hasCached)
	return info, nil
}

func (s *Server) ListHazards(ctx context.Context, req *aggregatorv1.ListHazardsRequest) (*aggregatorv1.ListHazardsResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if req.View == commonv1.View_VIEW_FULL && !caller.HasScope("hazards:read:full") {
		return nil, status.Error(codes.PermissionDenied, "insufficient scope for full view")
	}
	pageSize := int(req.Page.GetPageSize())
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = defaultPageSize
	}
	q := hazardQuery(req.Filter)
	page, err := s.hazards.List(ctx, q, storage.PageRequest{Size: pageSize, Token: req.Page.GetPageToken()})
	if errors.Is(err, storage.ErrInvalidPageToken) {
		return nil, status.Errorf(codes.InvalidArgument, "list hazards failed: %v", err)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list hazards failed: %v", err)
	}
	items := make([]*commonv1.HazardProjection, 0, len(page.Items))
	for _, r := range page.Items {
		proj, err := r.ToProjection(req.View)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "projection error: %v", err)
		}
		items = append(items, proj)
	}
	freshness := []*commonv1.SourceFreshness{}
	partial := false
	for _, src := range domain.AllSources {
		if q.Source != "" && q.Source != src.Source {
			continue
		}
		info, err := s.sourceFreshness(ctx, src.Source)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "watermark error: %v", err)
		}
		if info.Status != domain.StatusFresh {
			partial = true
		}
		freshness = append(freshness, info.ToProto())
	}
	return &aggregatorv1.ListHazardsResponse{Items: items, NextPageToken: page.NextToken, Freshness: freshness, Partial: partial}, nil
}

func (s *Server) GetHazard(ctx context.Context, req *aggregatorv1.GetHazardRequest) (*aggregatorv1.GetHazardResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if req.View == commonv1.View_VIEW_FULL && !caller.HasScope("hazards:read:full") {
		return nil, status.Error(codes.PermissionDenied, "insufficient scope for full view")
	}
	rec, err := s.hazards.Get(ctx, req.HazardId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get hazard failed: %v", err)
	}
	if rec == nil {
		return nil, status.Errorf(codes.NotFound, "hazard %s not found", req.HazardId)
	}
	proj, err := rec.ToProjection(req.View)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "projection error: %v", err)
	}
	info, err := s.sourceFreshness(ctx, rec.Source)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "watermark error: %v", err)
	}
	return &aggregatorv1.GetHazardResponse{
		Item:      proj,
		Freshness: []*commonv1.SourceFreshness{info.ToProto()},
		Partial:   info.Status != domain.StatusFresh,
	}, nil
}

func (s *Server) GetSourceStatuses(ctx context.Context, req *aggregatorv1.GetSourceStatusesRequest) (*aggregatorv1.GetSourceStatusesResponse, error) {
	freshness := []*commonv1.SourceFreshness{}
	for _, src := range domain.AllSources {
		info, err := s.sourceFreshness(ctx, src.Source)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "watermark fetch error: %v", err)
		}
		freshness = append(freshness, info.ToProto())
	}
	return &aggregatorv1.GetSourceStatusesResponse{Freshness: freshness}, nil
}
