package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	webhookv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/webhook/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/dispatcher"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/webhook-provider/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Server implements the WebhookService gRPC server.
type Server struct {
	cfg  *config.Config
	db   *store.DB
	auth *auth.JWTVerifier
	disp *dispatcher.Dispatcher
	webhookv1.UnimplementedWebhookServiceServer
}

// NewServer creates a new Server.
func NewServer(cfg *config.Config, db *store.DB, verifier *auth.JWTVerifier, disp *dispatcher.Dispatcher) *Server {
	return &Server{cfg: cfg, db: db, auth: verifier, disp: disp}
}

// Start binds the gRPC listener and blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", s.cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("listen on port %s failed: %w", s.cfg.GRPCPort, err)
	}

	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(s.auth.UnaryServerInterceptor()))
	webhookv1.RegisterWebhookServiceServer(grpcSrv, s)

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcSrv, healthSrv)

	go func() {
		log.Printf("[WebhookProvider] gRPC server listening on :%s", s.cfg.GRPCPort)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("[WebhookProvider] gRPC server stopped: %v", err)
		}
	}()

	<-ctx.Done()
	grpcSrv.GracefulStop()
	return nil
}

// ---------------------------------------------------------------------------
// CreateWebhook
// ---------------------------------------------------------------------------

func (s *Server) CreateWebhook(ctx context.Context, req *webhookv1.CreateWebhookRequest) (*webhookv1.CreateWebhookResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if !caller.HasScope("webhooks:write") {
		return nil, status.Error(codes.PermissionDenied, "scope webhooks:write required")
	}

	// Validate URL.
	rawURL := strings.TrimSpace(req.GetUrl())
	if rawURL == "" {
		return nil, status.Error(codes.InvalidArgument, "url is required")
	}
	if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		return nil, status.Error(codes.InvalidArgument, "url must start with http:// or https://")
	}

	// Enforce view: FULL requires hazards:read:full scope.
	view := req.GetView()
	if view == 0 {
		view = 1 // default VIEW_SUMMARY
	}
	if view == 2 && !caller.HasScope("hazards:read:full") {
		return nil, status.Error(codes.PermissionDenied, "full view requires hazards:read:full scope")
	}

	// Generate or use supplied secret.
	secret := ""
	if req.Secret != nil {
		secret = strings.TrimSpace(req.GetSecret())
	}
	if secret == "" {
		secret = uuid.NewString()
	}

	// Map filter fields.
	var filterSource, filterHazard, filterSeverity int32
	if f := req.GetFilter(); f != nil {
		filterSource = int32(f.GetSource())
		filterHazard = int32(f.GetHazardType())
		filterSeverity = int32(f.GetSeverity())
	}

	sub := &store.Subscription{
		WebhookID:      uuid.NewString(),
		OwnerClientID:  caller.ClientID,
		URL:            rawURL,
		Secret:         secret,
		FilterSource:   filterSource,
		FilterHazard:   filterHazard,
		FilterSeverity: filterSeverity,
		View:           int32(view),
		Description:    req.GetDescription(),
		IsActive:       true,
	}

	// Optional filter time bounds.
	if f := req.GetFilter(); f != nil {
		if f.GetSince() != nil {
			t := f.GetSince().AsTime()
			sub.FilterSince = &t
		}
		if f.GetUntil() != nil {
			t := f.GetUntil().AsTime()
			sub.FilterUntil = &t
		}
	}

	if err := s.db.CreateSubscription(ctx, sub); err != nil {
		log.Printf("[WebhookProvider] CreateSubscription DB error: %v", err)
		return nil, status.Errorf(codes.Internal, "failed creating subscription")
	}

	return &webhookv1.CreateWebhookResponse{
		Subscription: sub.ToProto(false), // include secret on creation response
	}, nil
}

// ---------------------------------------------------------------------------
// ListWebhooks
// ---------------------------------------------------------------------------

func (s *Server) ListWebhooks(ctx context.Context, req *webhookv1.ListWebhooksRequest) (*webhookv1.ListWebhooksResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if !caller.HasScope("webhooks:read") {
		return nil, status.Error(codes.PermissionDenied, "scope webhooks:read required")
	}

	pageSize := int(req.GetPage().GetPageSize())
	if pageSize <= 0 {
		pageSize = 20
	}

	subs, nextToken, err := s.db.ListSubscriptions(ctx, caller.ClientID, req.GetPage().GetPageToken(), pageSize)
	if err != nil {
		log.Printf("[WebhookProvider] ListSubscriptions DB error: %v", err)
		return nil, status.Errorf(codes.Internal, "failed listing subscriptions")
	}

	items := make([]*webhookv1.WebhookSubscription, 0, len(subs))
	for _, sub := range subs {
		items = append(items, sub.ToProto(true)) // redact secret on list
	}

	return &webhookv1.ListWebhooksResponse{
		Items:         items,
		NextPageToken: nextToken,
	}, nil
}

// ---------------------------------------------------------------------------
// GetWebhook
// ---------------------------------------------------------------------------

func (s *Server) GetWebhook(ctx context.Context, req *webhookv1.GetWebhookRequest) (*webhookv1.GetWebhookResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if !caller.HasScope("webhooks:read") {
		return nil, status.Error(codes.PermissionDenied, "scope webhooks:read required")
	}

	sub, err := s.db.GetSubscription(ctx, req.GetWebhookId())
	if err != nil {
		log.Printf("[WebhookProvider] GetSubscription DB error: %v", err)
		return nil, status.Errorf(codes.Internal, "failed fetching subscription")
	}
	if sub == nil || sub.OwnerClientID != caller.ClientID {
		// NOT_FOUND to avoid leaking existence of another client's subscription.
		return nil, status.Errorf(codes.NotFound, "webhook %s not found", req.GetWebhookId())
	}

	return &webhookv1.GetWebhookResponse{
		Subscription: sub.ToProto(true), // redact secret
	}, nil
}

// ---------------------------------------------------------------------------
// DeleteWebhook
// ---------------------------------------------------------------------------

func (s *Server) DeleteWebhook(ctx context.Context, req *webhookv1.DeleteWebhookRequest) (*webhookv1.DeleteWebhookResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if !caller.HasScope("webhooks:write") {
		return nil, status.Error(codes.PermissionDenied, "scope webhooks:write required")
	}

	sub, err := s.db.GetSubscription(ctx, req.GetWebhookId())
	if err != nil {
		log.Printf("[WebhookProvider] GetSubscription DB error: %v", err)
		return nil, status.Errorf(codes.Internal, "failed fetching subscription")
	}
	if sub == nil || sub.OwnerClientID != caller.ClientID {
		return nil, status.Errorf(codes.NotFound, "webhook %s not found", req.GetWebhookId())
	}

	if err := s.db.DeleteSubscription(ctx, req.GetWebhookId()); err != nil {
		log.Printf("[WebhookProvider] DeleteSubscription DB error: %v", err)
		return nil, status.Errorf(codes.Internal, "failed deleting subscription")
	}

	return &webhookv1.DeleteWebhookResponse{Success: true}, nil
}

// ---------------------------------------------------------------------------
// ListDeliveries
// ---------------------------------------------------------------------------

func (s *Server) ListDeliveries(ctx context.Context, req *webhookv1.ListDeliveriesRequest) (*webhookv1.ListDeliveriesResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if !caller.HasScope("webhooks:read") {
		return nil, status.Error(codes.PermissionDenied, "scope webhooks:read required")
	}

	// Verify ownership before listing.
	sub, err := s.db.GetSubscription(ctx, req.GetWebhookId())
	if err != nil {
		log.Printf("[WebhookProvider] GetSubscription DB error: %v", err)
		return nil, status.Errorf(codes.Internal, "failed fetching subscription")
	}
	if sub == nil || sub.OwnerClientID != caller.ClientID {
		return nil, status.Errorf(codes.NotFound, "webhook %s not found", req.GetWebhookId())
	}

	pageSize := int(req.GetPage().GetPageSize())
	if pageSize <= 0 {
		pageSize = 20
	}

	deliveries, nextToken, err := s.db.ListDeliveries(ctx, req.GetWebhookId(), req.GetPage().GetPageToken(), pageSize)
	if err != nil {
		log.Printf("[WebhookProvider] ListDeliveries DB error: %v", err)
		return nil, status.Errorf(codes.Internal, "failed listing deliveries")
	}

	items := make([]*webhookv1.WebhookDelivery, 0, len(deliveries))
	for _, d := range deliveries {
		items = append(items, d.ToProto())
	}

	return &webhookv1.ListDeliveriesResponse{
		Items:         items,
		NextPageToken: nextToken,
	}, nil
}

// ---------------------------------------------------------------------------
// PingWebhook
// ---------------------------------------------------------------------------

func (s *Server) PingWebhook(ctx context.Context, req *webhookv1.PingWebhookRequest) (*webhookv1.PingWebhookResponse, error) {
	caller := auth.GetCallerContext(ctx)
	if caller == nil {
		return nil, status.Error(codes.Unauthenticated, "missing caller context")
	}
	if !caller.HasScope("webhooks:write") {
		return nil, status.Error(codes.PermissionDenied, "scope webhooks:write required")
	}

	sub, err := s.db.GetSubscription(ctx, req.GetWebhookId())
	if err != nil {
		log.Printf("[WebhookProvider] GetSubscription DB error: %v", err)
		return nil, status.Errorf(codes.Internal, "failed fetching subscription")
	}
	if sub == nil || sub.OwnerClientID != caller.ClientID {
		return nil, status.Errorf(codes.NotFound, "webhook %s not found", req.GetWebhookId())
	}

	pingPayload, err := dispatcher.BuildPingPayload(sub.WebhookID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed building ping payload")
	}

	result := s.disp.DispatchOnce(ctx, sub.URL, sub.Secret, pingPayload)

	msg := "ok"
	if result.Err != nil {
		msg = result.Err.Error()
	} else if result.StatusCode < 200 || result.StatusCode >= 300 {
		msg = fmt.Sprintf("endpoint returned non-2xx status %d", result.StatusCode)
	}

	return &webhookv1.PingWebhookResponse{
		StatusCode: int32(result.StatusCode),
		DurationMs: result.DurationMS,
		Success:    result.Err == nil && result.StatusCode >= 200 && result.StatusCode < 300,
		Message:    msg,
	}, nil
}
