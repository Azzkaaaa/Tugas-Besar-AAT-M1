package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

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
	"google.golang.org/protobuf/proto"
)

type Server struct {
	cfg            *config.Config
	hazards        storage.HazardStore
	sources        storage.SourceStateStore
	auth           *auth.JWTVerifier
	reports        storage.FieldReports
	analytics      storage.AnalyticsReader
	analyticsSlots chan struct{}
	aggregatorv1.UnimplementedAggregatorServiceServer
}

func (s *Server) SetFieldReports(reports storage.FieldReports) { s.reports = reports }

func NewServer(cfg *config.Config, hazards storage.HazardStore, sources storage.SourceStateStore, verifier *auth.JWTVerifier) *Server {
	return &Server{cfg: cfg, hazards: hazards, sources: sources, auth: verifier, analyticsSlots: make(chan struct{}, 1)}
}

func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", s.cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("listen on %s failed: %w", s.cfg.GRPCPort, err)
	}
	// OTel + JWT interceptor chain & stats handler.
	options := []grpc.ServerOption{
		// One span per RPC from the stats handler; probes of grpc.health.v1 are not traced.
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(filters.Not(filters.HealthCheck())))),
		grpc.ChainUnaryInterceptor(
			s.auth.UnaryServerInterceptor(),
		),
	}
	options = append(options, BoundedReadOptions()...)
	grpcSrv := grpc.NewServer(options...)
	// register service implementation.
	aggregatorv1.RegisterAggregatorServiceServer(grpcSrv, s)
	aggregatorv1.RegisterFieldReportServiceServer(grpcSrv, fieldReportService{store: s.reports})
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(grpcSrv, healthSrv)
	readinessDone := make(chan struct{})
	go func() {
		defer close(readinessDone)
		monitorReadiness(ctx, healthSrv, s.hazards, 2*time.Second, 800*time.Millisecond)
	}()
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			fmt.Printf("gRPC server stopped: %v\n", err)
		}
	}()
	<-ctx.Done()
	<-readinessDone
	healthSrv.Shutdown()
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

const (
	listHazardsBudget = 1500 * time.Millisecond
	// freshnessReserve covers one parallel round of 500ms source probes and
	// leaves storage its usual one-second read unit.
	freshnessReserve = 500 * time.Millisecond
)

// sourceFreshness reports the freshness of one source's ingest stream.
func (s *Server) sourceFreshness(ctx context.Context, source string) (*domain.SourceStatusInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	state, err := s.sources.Get(ctx, domain.SourceKeyFor(source))
	if err != nil {
		return &domain.SourceStatusInfo{Source: source, Status: domain.StatusUnavailable}, err
	}
	info := &domain.SourceStatusInfo{
		Source:        source,
		Status:        state.Status,
		LastSuccessAt: state.LastSuccessAt,
		LastAttemptAt: state.LastAttemptAt,
		ErrorMessage:  state.ErrorMessage,
	}
	hasCached, cacheErr := s.hazards.HasData(ctx, source)
	info.CalculateFreshness(s.cfg.SourceStaleThreshold, hasCached)
	if cacheErr != nil {
		info.Status = domain.StatusUnavailable
	}
	return info, cacheErr
}

func (s *Server) ListHazards(ctx context.Context, req *aggregatorv1.ListHazardsRequest) (*aggregatorv1.ListHazardsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, listHazardsBudget)
	defer cancel()
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
	q.SummaryOnly = req.View != commonv1.View_VIEW_FULL
	// Reserve the tail of the deadline for the freshness probes that follow.
	// Otherwise a slow FULL hydration consumes the whole budget and every
	// source is reported UNAVAILABLE. Storage turns the earlier stop into a
	// short complete page with a continuation.
	listCtx, listCancel := context.WithTimeout(ctx, listHazardsBudget-freshnessReserve)
	page, err := s.hazards.List(listCtx, q, storage.PageRequest{Size: pageSize, Token: req.Page.GetPageToken()})
	listCancel()
	if errors.Is(err, storage.ErrHazardProjectionTooLarge) {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}
	if errors.Is(err, storage.ErrInvalidPageToken) {
		return nil, status.Errorf(codes.InvalidArgument, "list hazards failed: %v", err)
	}
	if err != nil {
		page.Partial = true
		page.UnavailableUnits = append(page.UnavailableUnits, "canonical")
		page.NextToken = req.Page.GetPageToken()
	}
	items := make([]*commonv1.HazardProjection, 0, len(page.Items))
	for _, r := range page.Items {
		proj, err := r.ToProjection(req.View)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "projection error: %v", err)
		}
		items = append(items, proj)
	}
	freshness, unavailable, stale := s.freshnessFor(ctx, q.Source)
	partial := page.Partial || stale
	page.UnavailableUnits = append(page.UnavailableUnits, unavailable...)
	response := &aggregatorv1.ListHazardsResponse{Items: items, NextPageToken: page.NextToken, Freshness: freshness, Partial: partial, UnavailableUnits: page.UnavailableUnits}
	if proto.Size(response) > storage.MaxHazardProjectionBytes {
		return nil, status.Error(codes.ResourceExhausted, "complete hazard response exceeds the transport byte budget")
	}
	return response, nil
}

func (s *Server) GetHazard(ctx context.Context, req *aggregatorv1.GetHazardRequest) (*aggregatorv1.GetHazardResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if req.View == commonv1.View_VIEW_FULL && !caller.HasScope("hazards:read:full") {
		return nil, status.Error(codes.PermissionDenied, "insufficient scope for full view")
	}
	get := s.hazards.GetSummary
	if req.View == commonv1.View_VIEW_FULL {
		get = s.hazards.Get
	}
	rec, err := get(ctx, req.HazardId)
	if err != nil {
		return &aggregatorv1.GetHazardResponse{Partial: true, UnavailableUnits: []string{"canonical"}}, nil
	}
	if rec == nil || domain.Withdrawn(rec) {
		return nil, status.Errorf(codes.NotFound, "hazard %s not found", req.HazardId)
	}
	proj, err := rec.ToProjection(req.View)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "projection error: %v", err)
	}
	info, err := s.sourceFreshness(ctx, rec.Source)
	units := []string{}
	if err != nil {
		units = append(units, "source-state:"+string(domain.SourceKeyFor(rec.Source)))
	}
	response := &aggregatorv1.GetHazardResponse{
		Item:             proj,
		Freshness:        []*commonv1.SourceFreshness{info.ToProto()},
		Partial:          len(units) > 0 || info.Status != domain.StatusFresh,
		UnavailableUnits: units,
	}
	if proto.Size(response) > storage.MaxHazardProjectionBytes {
		return nil, status.Error(codes.ResourceExhausted, "complete hazard response exceeds the transport byte budget")
	}
	return response, nil
}

func (s *Server) GetSourceStatuses(ctx context.Context, req *aggregatorv1.GetSourceStatusesRequest) (*aggregatorv1.GetSourceStatusesResponse, error) {
	freshness, _, _ := s.freshnessFor(ctx, "")
	return &aggregatorv1.GetSourceStatusesResponse{Freshness: freshness}, nil
}

// Four independent probes share the caller deadline; WAN quorum latency must
// not multiply by the source count. Keep the established response order.
func (s *Server) freshnessFor(ctx context.Context, filter string) ([]*commonv1.SourceFreshness, []string, bool) {
	infos := make([]*domain.SourceStatusInfo, len(domain.AllSources))
	errs := make([]error, len(domain.AllSources))
	var work sync.WaitGroup
	for i, src := range domain.AllSources {
		if filter != "" && filter != src.Source {
			continue
		}
		work.Go(func() { infos[i], errs[i] = s.sourceFreshness(ctx, src.Source) })
	}
	work.Wait()
	freshness := []*commonv1.SourceFreshness{}
	var unavailable []string
	partial := false
	for i, info := range infos {
		if info == nil {
			continue
		}
		if errs[i] != nil {
			unavailable = append(unavailable, "source-state:"+string(domain.AllSources[i].Key))
			partial = true
		}
		partial = partial || info.Status != domain.StatusFresh
		freshness = append(freshness, info.ToProto())
	}
	return freshness, unavailable, partial
}
