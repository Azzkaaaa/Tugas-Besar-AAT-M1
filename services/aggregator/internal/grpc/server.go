package grpc

import (
	"context"
	"fmt"
	"net"
	"strings"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/store"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type Server struct {
	cfg  *config.Config
	db   *store.DB
	auth *auth.JWTVerifier
	aggregatorv1.UnimplementedAggregatorServiceServer
}

func NewServer(cfg *config.Config, db *store.DB, verifier *auth.JWTVerifier) *Server {
	return &Server{cfg: cfg, db: db, auth: verifier}
}

func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", s.cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("listen on %s failed: %w", s.cfg.GRPCPort, err)
	}
	// OTel + JWT interceptor chain & stats handler.
	grpcSrv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			otelgrpc.UnaryServerInterceptor(),
			s.auth.UnaryServerInterceptor(),
		),
	)
	// register service implementation.
	aggregatorv1.RegisterAggregatorServiceServer(grpcSrv, s)
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

func (s *Server) ListHazards(ctx context.Context, req *aggregatorv1.ListHazardsRequest) (*aggregatorv1.ListHazardsResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if req.View == commonv1.View_VIEW_FULL && !caller.HasScope("hazards:read:full") {
		return nil, status.Error(codes.PermissionDenied, "insufficient scope for full view")
	}
	pageSize := int(req.Page.GetPageSize())
	if pageSize <= 0 {
		pageSize = 20
	}
	records, nextToken, err := s.db.ListHazards(ctx, req.Filter, req.Page.GetPageToken(), pageSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list hazards failed: %v", err)
	}
	items := make([]*commonv1.HazardProjection, 0, len(records))
	for _, r := range records {
		proj, err := r.ToProjection(req.View)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "projection error: %v", err)
		}
		items = append(items, proj)
	}
	freshness := []*commonv1.SourceFreshness{}
	relevant := []string{"BMKG", "PVMBG"}
	if req.Filter != nil && req.Filter.Source != commonv1.Source_SOURCE_UNSPECIFIED {
		if req.Filter.Source == commonv1.Source_SOURCE_BMKG {
			relevant = []string{"BMKG"}
		} else if req.Filter.Source == commonv1.Source_SOURCE_PVMBG {
			relevant = []string{"PVMBG"}
		}
	}
	for _, src := range relevant {
		var key string
		if src == "BMKG" {
			key = "bmkg_seismic"
		} else {
			key = "pvmbg_volcanic"
		}
		info, _, err := s.db.GetWatermark(ctx, key)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "watermark error: %v", err)
		}
		info.Source = src
		hasCached, _ := s.db.HasCachedData(ctx, src)
		info.CalculateFreshness(s.cfg.SourceStaleThreshold, hasCached)
		freshness = append(freshness, info.ToProto())
	}
	partial := false
	for _, f := range freshness {
		if f.Status != commonv1.SourceStatus_SOURCE_STATUS_FRESH {
			partial = true
			break
		}
	}
	return &aggregatorv1.ListHazardsResponse{Items: items, NextPageToken: nextToken, Freshness: freshness, Partial: partial}, nil
}

func (s *Server) GetHazard(ctx context.Context, req *aggregatorv1.GetHazardRequest) (*aggregatorv1.GetHazardResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if req.View == commonv1.View_VIEW_FULL && !caller.HasScope("hazards:read:full") {
		return nil, status.Error(codes.PermissionDenied, "insufficient scope for full view")
	}
	rec, err := s.db.GetHazardByID(ctx, req.HazardId)
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
	key := strings.ToLower(rec.Source) + "_" + strings.ToLower(rec.HazardType)
	statusInfo, _, err := s.db.GetWatermark(ctx, key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "watermark error: %v", err)
	}
	statusInfo.Source = rec.Source
	hasCached, _ := s.db.HasCachedData(ctx, statusInfo.Source)
	statusInfo.CalculateFreshness(s.cfg.SourceStaleThreshold, hasCached)
	freshness := []*commonv1.SourceFreshness{statusInfo.ToProto()}
	partial := statusInfo.Status != "FRESH"
	return &aggregatorv1.GetHazardResponse{Item: proj, Freshness: freshness, Partial: partial}, nil
}

func (s *Server) GetSourceStatuses(ctx context.Context, req *aggregatorv1.GetSourceStatusesRequest) (*aggregatorv1.GetSourceStatusesResponse, error) {
	keys := []string{"bmkg_seismic", "pvmbg_volcanic"}
	freshness := []*commonv1.SourceFreshness{}
	for _, k := range keys {
		info, _, err := s.db.GetWatermark(ctx, k)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "watermark fetch error: %v", err)
		}
		info.Source = strings.ToUpper(strings.Split(k, "_")[0])
		hasCached, _ := s.db.HasCachedData(ctx, info.Source)
		info.CalculateFreshness(s.cfg.SourceStaleThreshold, hasCached)
		freshness = append(freshness, info.ToProto())
	}
	return &aggregatorv1.GetSourceStatusesResponse{Freshness: freshness}, nil
}
