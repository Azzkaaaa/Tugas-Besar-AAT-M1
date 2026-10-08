package server_test

// Contract suite: every OpenAPI operation runs through the real router against in-process
// gRPC fakes, and every request/response is validated with kin-openapi against the same
// embedded contract the gateway was generated from.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	feedv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/feed/v1"
	notificationv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/notification/v1"
	webhookv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/webhook/v1"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/contract"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/handlers"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/server"
)

// ---- gRPC fakes ------------------------------------------------------------------------

// backend implements all four services. Each test sets the reply for the RPCs it calls.
type backend struct {
	aggregatorv1.UnimplementedAggregatorServiceServer
	notificationv1.UnimplementedDangerNotificationServiceServer
	webhookv1.UnimplementedWebhookServiceServer
	feedv1.UnimplementedEventFeedServiceServer

	mu       sync.Mutex
	replies  map[string]func(req any) (any, error)
	stream   func(req *feedv1.SubscribeEventsRequest, send func(*feedv1.FeedEvent) error) error
	requests map[string]any
	metadata metadata.MD
}

func (b *backend) call(ctx context.Context, method string, req any) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	b.mu.Lock()
	b.requests[method] = req
	b.metadata = md
	reply := b.replies[method]
	b.mu.Unlock()
	if reply == nil {
		return nil, status.Error(codes.Unimplemented, method)
	}
	return reply(req)
}

func (b *backend) reply(method string, fn func(req any) (any, error)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.replies[method] = fn
}

func (b *backend) request(method string) any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.requests[method]
}

func typed[T any](b *backend, ctx context.Context, method string, req any) (T, error) {
	resp, err := b.call(ctx, method, req)
	if err != nil {
		var zero T
		return zero, err
	}
	return resp.(T), nil
}

func (b *backend) ListHazards(ctx context.Context, r *aggregatorv1.ListHazardsRequest) (*aggregatorv1.ListHazardsResponse, error) {
	return typed[*aggregatorv1.ListHazardsResponse](b, ctx, "ListHazards", r)
}
func (b *backend) GetHazard(ctx context.Context, r *aggregatorv1.GetHazardRequest) (*aggregatorv1.GetHazardResponse, error) {
	return typed[*aggregatorv1.GetHazardResponse](b, ctx, "GetHazard", r)
}
func (b *backend) GetSourceStatuses(ctx context.Context, r *aggregatorv1.GetSourceStatusesRequest) (*aggregatorv1.GetSourceStatusesResponse, error) {
	return typed[*aggregatorv1.GetSourceStatusesResponse](b, ctx, "GetSourceStatuses", r)
}
func (b *backend) ListNotifications(ctx context.Context, r *notificationv1.ListNotificationsRequest) (*notificationv1.ListNotificationsResponse, error) {
	return typed[*notificationv1.ListNotificationsResponse](b, ctx, "ListNotifications", r)
}
func (b *backend) GetNotification(ctx context.Context, r *notificationv1.GetNotificationRequest) (*notificationv1.GetNotificationResponse, error) {
	return typed[*notificationv1.GetNotificationResponse](b, ctx, "GetNotification", r)
}
func (b *backend) RegisterPushToken(ctx context.Context, r *notificationv1.RegisterPushTokenRequest) (*notificationv1.RegisterPushTokenResponse, error) {
	return typed[*notificationv1.RegisterPushTokenResponse](b, ctx, "RegisterPushToken", r)
}
func (b *backend) UnregisterPushToken(ctx context.Context, r *notificationv1.UnregisterPushTokenRequest) (*notificationv1.UnregisterPushTokenResponse, error) {
	return typed[*notificationv1.UnregisterPushTokenResponse](b, ctx, "UnregisterPushToken", r)
}
func (b *backend) CreateWebhook(ctx context.Context, r *webhookv1.CreateWebhookRequest) (*webhookv1.CreateWebhookResponse, error) {
	return typed[*webhookv1.CreateWebhookResponse](b, ctx, "CreateWebhook", r)
}
func (b *backend) ListWebhooks(ctx context.Context, r *webhookv1.ListWebhooksRequest) (*webhookv1.ListWebhooksResponse, error) {
	return typed[*webhookv1.ListWebhooksResponse](b, ctx, "ListWebhooks", r)
}
func (b *backend) GetWebhook(ctx context.Context, r *webhookv1.GetWebhookRequest) (*webhookv1.GetWebhookResponse, error) {
	return typed[*webhookv1.GetWebhookResponse](b, ctx, "GetWebhook", r)
}
func (b *backend) DeleteWebhook(ctx context.Context, r *webhookv1.DeleteWebhookRequest) (*webhookv1.DeleteWebhookResponse, error) {
	return typed[*webhookv1.DeleteWebhookResponse](b, ctx, "DeleteWebhook", r)
}
func (b *backend) ListDeliveries(ctx context.Context, r *webhookv1.ListDeliveriesRequest) (*webhookv1.ListDeliveriesResponse, error) {
	return typed[*webhookv1.ListDeliveriesResponse](b, ctx, "ListDeliveries", r)
}
func (b *backend) PingWebhook(ctx context.Context, r *webhookv1.PingWebhookRequest) (*webhookv1.PingWebhookResponse, error) {
	return typed[*webhookv1.PingWebhookResponse](b, ctx, "PingWebhook", r)
}

func (b *backend) SubscribeEvents(r *feedv1.SubscribeEventsRequest, s grpc.ServerStreamingServer[feedv1.SubscribeEventsResponse]) error {
	md, _ := metadata.FromIncomingContext(s.Context())
	b.mu.Lock()
	b.requests["SubscribeEvents"] = r
	b.metadata = md
	fn := b.stream
	b.mu.Unlock()
	if fn == nil {
		return status.Error(codes.Unimplemented, "SubscribeEvents")
	}
	return fn(r, func(event *feedv1.FeedEvent) error {
		return s.Send(&feedv1.SubscribeEventsResponse{Event: event})
	})
}

// ---- harness -----------------------------------------------------------------------------

type harness struct {
	t       *testing.T
	backend *backend
	server  *httptest.Server
	router  routers.Router
	doc     *openapi3.T
	covered map[string]bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	b := &backend{replies: map[string]func(any) (any, error){}, requests: map[string]any{}}
	grpcServer := grpc.NewServer()
	aggregatorv1.RegisterAggregatorServiceServer(grpcServer, b)
	notificationv1.RegisterDangerNotificationServiceServer(grpcServer, b)
	webhookv1.RegisterWebhookServiceServer(grpcServer, b)
	feedv1.RegisterEventFeedServiceServer(grpcServer, b)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	spec, err := contract.Load()
	if err != nil {
		t.Fatal(err)
	}
	handler := server.New(server.Options{
		Clients: handlers.Clients{
			Aggregator:   aggregatorv1.NewAggregatorServiceClient(conn),
			Notification: notificationv1.NewDangerNotificationServiceClient(conn),
			Webhook:      webhookv1.NewWebhookServiceClient(conn),
			Feed:         feedv1.NewEventFeedServiceClient(conn),
		},
		Contract:          spec,
		RequestTimeout:    2 * time.Second,
		MaxInFlight:       10,
		RetryAfterSeconds: 7,
		Heartbeat:         50 * time.Millisecond,
	})
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	// A separate copy of the contract for validation, matched without its servers list.
	validation, err := contract.Load()
	if err != nil {
		t.Fatal(err)
	}
	validation.Doc.Servers = nil
	router, err := gorillamux.NewRouter(validation.Doc)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, backend: b, server: httpServer, router: router, doc: validation.Doc, covered: map[string]bool{}}
}

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) json(t *testing.T) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(r.body, &out); err != nil {
		t.Fatalf("body is not a JSON object: %v: %s", err, r.body)
	}
	return out
}

// do sends a request and validates request and response against the contract.
func (h *harness) do(method, target string, body any, headers map[string]string) response {
	h.t.Helper()
	var payload []byte
	if body != nil {
		switch v := body.(type) {
		case string:
			payload = []byte(v)
		default:
			payload, _ = json.Marshal(v)
		}
	}
	req, _ := http.NewRequest(method, h.server.URL+target, bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer test-token")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := h.server.Client().Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	h.validate(req, payload, resp, data)
	return response{status: resp.StatusCode, header: resp.Header, body: data}
}

// validate checks the response (status documented, headers, body schema) for routes in the
// contract, and that every error is problem+json carrying the echoed correlation ID.
func (h *harness) validate(req *http.Request, payload []byte, resp *http.Response, data []byte) {
	h.t.Helper()
	if resp.Header.Get("X-Correlation-ID") == "" {
		h.t.Errorf("%s %s: response without X-Correlation-ID", req.Method, req.URL.Path)
	}
	if resp.StatusCode >= 400 {
		if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
			h.t.Errorf("%s %s: error content type %q", req.Method, req.URL.Path, ct)
		}
		var problem map[string]any
		if json.Unmarshal(data, &problem) != nil || problem["correlation_id"] != resp.Header.Get("X-Correlation-ID") {
			h.t.Errorf("%s %s: problem without the echoed correlation_id: %s", req.Method, req.URL.Path, data)
		}
	}
	validationReq := req.Clone(context.Background())
	validationReq.Body = io.NopCloser(bytes.NewReader(payload))
	route, pathParams, err := h.router.FindRoute(validationReq)
	if err != nil {
		return // not a contract path (404 tests)
	}
	h.covered[route.Method+" "+route.Path] = true
	input := &openapi3filter.RequestValidationInput{
		Request:    validationReq,
		PathParams: pathParams,
		Route:      route,
		Options:    &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
	}
	responseInput := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: input,
		Status:                 resp.StatusCode,
		Header:                 resp.Header,
		Options:                &openapi3filter.Options{IncludeResponseStatus: true, MultiError: true},
	}
	responseInput.SetBodyBytes(data)
	if err := openapi3filter.ValidateResponse(context.Background(), responseInput); err != nil {
		h.t.Errorf("%s %s → %d violates the contract: %v\nbody: %s", req.Method, req.URL.Path, resp.StatusCode, err, data)
	}
}

func init() {
	// SSE bodies are validated frame by frame (validateFrames); the raw body is a string.
	openapi3filter.RegisterBodyDecoder("text/event-stream", func(body io.Reader, _ http.Header, _ *openapi3.SchemaRef, _ openapi3filter.EncodingFn) (any, error) {
		data, err := io.ReadAll(body)
		return string(data), err
	})
}

// ---- fixtures ----------------------------------------------------------------------------

var now = timestamppb.New(time.Date(2026, 9, 22, 6, 0, 0, 0, time.UTC))

func summary(id string) *commonv1.HazardProjection {
	return &commonv1.HazardProjection{Data: &commonv1.HazardProjection_Summary{Summary: &commonv1.HazardSummary{
		HazardId: id, Source: commonv1.Source_SOURCE_BMKG, HazardType: commonv1.HazardType_HAZARD_TYPE_SEISMIC,
		Severity: commonv1.Severity_SEVERITY_SIAGA, AreaName: "", OccurredAt: now, IngestedAt: now,
	}}}
}

func full(id string) *commonv1.HazardProjection {
	zero := 0.0
	attributes, _ := structpb.NewStruct(map[string]any{"magnitude": 5.1})
	return &commonv1.HazardProjection{Data: &commonv1.HazardProjection_Full{Full: &commonv1.HazardEvent{
		HazardId: id, Source: commonv1.Source_SOURCE_PVMBG, SourceRefId: "ref-1", HazardType: commonv1.HazardType_HAZARD_TYPE_VOLCANIC,
		Severity: commonv1.Severity_SEVERITY_AWAS, AreaName: "Merapi", Latitude: &zero, Longitude: &zero,
		OccurredAt: now, IngestedAt: now, Attributes: attributes,
	}}}
}

func freshness() []*commonv1.SourceFreshness {
	return []*commonv1.SourceFreshness{
		{Source: commonv1.Source_SOURCE_BMKG, Status: commonv1.SourceStatus_SOURCE_STATUS_FRESH, LastSuccessAt: now},
		{Source: commonv1.Source_SOURCE_PVMBG, Status: commonv1.SourceStatus_SOURCE_STATUS_UNAVAILABLE},
	}
}

func notification(view commonv1.View) *notificationv1.DangerNotification {
	hazard := summary("haz-1")
	if view == commonv1.View_VIEW_FULL {
		hazard = full("haz-1")
	}
	return &notificationv1.DangerNotification{NotificationId: "ntf-1", Hazard: hazard, Message: "Waspada", CreatedAt: now}
}

func webhook() *webhookv1.WebhookSubscription {
	return &webhookv1.WebhookSubscription{
		WebhookId: "wh-1", Url: "https://example.org/hook", Secret: "super-secret",
		Filter: &commonv1.HazardFilter{Severity: commonv1.Severity_SEVERITY_AWAS}, View: commonv1.View_VIEW_SUMMARY,
		IsActive: true, OwnerClientId: "client-1", CreatedAt: now, UpdatedAt: now,
	}
}

func fail(code codes.Code) func(any) (any, error) {
	return func(any) (any, error) { return nil, status.Error(code, "backend detail that must not leak") }
}

func reply[T any](value T) func(any) (any, error) {
	return func(any) (any, error) { return value, nil }
}

func feedEvent(seq uint64, projection *commonv1.HazardProjection) *feedv1.FeedEvent {
	return &feedv1.FeedEvent{EventId: "evt-" + string(rune('0'+seq)), Sequence: seq, Hazard: projection, PublishedAt: now, ResumeCursor: "cursor-" + string(rune('0'+seq))}
}

// ---- tests -------------------------------------------------------------------------------

func TestContract(t *testing.T) {
	h := newHarness(t)
	b := h.backend

	t.Run("hazards", func(t *testing.T) {
		b.reply("ListHazards", reply(&aggregatorv1.ListHazardsResponse{Items: []*commonv1.HazardProjection{summary("haz-1")}, Freshness: freshness(), Partial: true}))
		r := h.do("GET", "/api/v1/hazards?source=BMKG&since=2026-09-01T00:00:00Z&page_size=5", nil, map[string]string{"X-Correlation-ID": "corr-1"})
		if r.status != 200 || r.header.Get("X-Correlation-ID") != "corr-1" {
			t.Fatalf("list: %d %v", r.status, r.header)
		}
		page := r.json(t)
		if _, ok := page["next_page_token"]; ok {
			t.Error("empty next_page_token must be omitted")
		}
		item := page["items"].([]any)[0].(map[string]any)
		if len(item) != 7 {
			t.Errorf("summary must have exactly 7 fields: %v", item)
		}
		req := b.request("ListHazards").(*aggregatorv1.ListHazardsRequest)
		if req.GetFilter().GetSource() != commonv1.Source_SOURCE_BMKG || req.GetPage().GetPageSize() != 5 || req.GetView() != commonv1.View_VIEW_SUMMARY || req.GetFilter().GetSince() == nil {
			t.Errorf("request mapping: %v", req)
		}
		if got := b.metadata.Get("authorization"); len(got) != 1 || got[0] != "Bearer test-token" {
			t.Errorf("authorization not forwarded: %v", got)
		}
		if got := b.metadata.Get("x-correlation-id"); len(got) != 1 || got[0] != "corr-1" {
			t.Errorf("correlation not forwarded: %v", got)
		}

		b.reply("ListHazards", reply(&aggregatorv1.ListHazardsResponse{Items: []*commonv1.HazardProjection{full("haz-2")}, NextPageToken: "next", Freshness: freshness()}))
		if r := h.do("GET", "/api/v1/hazards?view=full", nil, nil); r.status != 200 || !strings.Contains(string(r.body), `"latitude":0`) {
			t.Errorf("full list: %d %s", r.status, r.body)
		}
		// The backend answering FULL to a SUMMARY request must never leak full fields.
		if r := h.do("GET", "/api/v1/hazards", nil, nil); r.status != 503 || strings.Contains(string(r.body), "source_ref_id") {
			t.Errorf("projection mismatch: %d %s", r.status, r.body)
		}
		b.reply("ListHazards", fail(codes.OutOfRange))
		if r := h.do("GET", "/api/v1/hazards?page_token=old", nil, nil); r.status != 409 || strings.Contains(string(r.body), "must not leak") {
			t.Errorf("expired snapshot: %d %s", r.status, r.body)
		}
		for _, query := range []string{"source=SOURCE_BMKG", "page_size=0", "page_size=101", "view=FULL", "since=yesterday"} {
			if r := h.do("GET", "/api/v1/hazards?"+query, nil, nil); r.status != 400 {
				t.Errorf("%s: want 400, got %d %s", query, r.status, r.body)
			}
		}
		if r := h.do("GET", "/api/v1/hazards?since=2026-09-02T00:00:00Z&until=2026-09-01T00:00:00Z", nil, nil); r.status != 400 {
			t.Errorf("since after until: %d", r.status)
		}

		b.reply("GetHazard", reply(&aggregatorv1.GetHazardResponse{Item: summary("haz-1"), Freshness: freshness()[:1]}))
		if r := h.do("GET", "/api/v1/hazards/haz-1", nil, nil); r.status != 200 {
			t.Errorf("get: %d %s", r.status, r.body)
		}
		b.reply("GetHazard", fail(codes.NotFound))
		if r := h.do("GET", "/api/v1/hazards/missing", nil, nil); r.status != 404 {
			t.Errorf("not found: %d", r.status)
		}
		b.reply("GetHazard", fail(codes.PermissionDenied))
		if r := h.do("GET", "/api/v1/hazards/haz-1?view=full", nil, nil); r.status != 403 {
			t.Errorf("forbidden: %d", r.status)
		}

		b.reply("GetSourceStatuses", reply(&aggregatorv1.GetSourceStatusesResponse{Freshness: freshness()}))
		if r := h.do("GET", "/api/v1/hazards/sources/statuses", nil, nil); r.status != 200 || strings.Contains(string(r.body), "0001-01-01") {
			t.Errorf("statuses: %d %s", r.status, r.body)
		}
		b.reply("GetSourceStatuses", fail(codes.Unauthenticated))
		if r := h.do("GET", "/api/v1/hazards/sources/statuses", nil, nil); r.status != 401 || r.header.Get("WWW-Authenticate") != "Bearer" {
			t.Errorf("unauthenticated: %d %v", r.status, r.header)
		}
		// 429 is not documented for this operation: it degrades to 503 with Retry-After.
		b.reply("GetSourceStatuses", fail(codes.ResourceExhausted))
		if r := h.do("GET", "/api/v1/hazards/sources/statuses", nil, nil); r.status != 503 || r.header.Get("Retry-After") != "7" {
			t.Errorf("exhausted: %d %v", r.status, r.header)
		}
		b.reply("GetSourceStatuses", fail(codes.Unavailable))
		if r := h.do("GET", "/api/v1/hazards/sources/statuses", nil, nil); r.status != 503 {
			t.Errorf("unavailable: %d", r.status)
		}
	})

	t.Run("notifications", func(t *testing.T) {
		b.reply("ListNotifications", reply(&notificationv1.ListNotificationsResponse{Items: []*notificationv1.DangerNotification{notification(commonv1.View_VIEW_SUMMARY)}, Freshness: freshness()}))
		if r := h.do("GET", "/api/v1/notifications?severity=AWAS", nil, nil); r.status != 200 {
			t.Errorf("list: %d %s", r.status, r.body)
		}
		b.reply("GetNotification", reply(&notificationv1.GetNotificationResponse{Item: notification(commonv1.View_VIEW_FULL), Freshness: freshness()[:1]}))
		if r := h.do("GET", "/api/v1/notifications/ntf-1?view=full", nil, nil); r.status != 200 || r.header.Get("Cache-Control") != "no-store" {
			t.Errorf("get: %d %v %s", r.status, r.header, r.body)
		}
		b.reply("RegisterPushToken", reply(&notificationv1.RegisterPushTokenResponse{}))
		if r := h.do("POST", "/api/v1/notifications/push/register", map[string]any{"fcm_token": "tok"}, nil); r.status != 204 || len(r.body) != 0 {
			t.Errorf("register: %d %q", r.status, r.body)
		}
		if got := b.request("RegisterPushToken").(*notificationv1.RegisterPushTokenRequest).GetFcmToken(); got != "tok" {
			t.Errorf("fcm_token: %q", got)
		}
		for _, body := range []any{map[string]any{"fcm_token": ""}, map[string]any{"fcm_token": "tok", "extra": 1}, map[string]any{}, "{"} {
			if r := h.do("POST", "/api/v1/notifications/push/register", body, nil); r.status != 400 {
				t.Errorf("register %v: want 400, got %d %s", body, r.status, r.body)
			}
		}
		b.reply("UnregisterPushToken", reply(&notificationv1.UnregisterPushTokenResponse{}))
		if r := h.do("POST", "/api/v1/notifications/push/unregister", map[string]any{"fcm_token": "tok"}, nil); r.status != 204 {
			t.Errorf("unregister: %d", r.status)
		}
	})

	t.Run("webhooks", func(t *testing.T) {
		b.reply("ListWebhooks", reply(&webhookv1.ListWebhooksResponse{Items: []*webhookv1.WebhookSubscription{webhook()}}))
		if r := h.do("GET", "/api/v1/webhooks", nil, nil); r.status != 200 || strings.Contains(string(r.body), "secret") {
			t.Errorf("list: %d %s", r.status, r.body)
		}
		b.reply("CreateWebhook", reply(&webhookv1.CreateWebhookResponse{Subscription: webhook()}))
		create := map[string]any{"url": "https://example.org/hook", "secret": "s3cret", "filter": map[string]any{"severity": "AWAS"}, "view": "full"}
		if r := h.do("POST", "/api/v1/webhooks", create, nil); r.status != 201 || strings.Contains(string(r.body), "secret") {
			t.Errorf("create: %d %s", r.status, r.body)
		}
		req := b.request("CreateWebhook").(*webhookv1.CreateWebhookRequest)
		if req.GetSecret() != "s3cret" || req.GetView() != commonv1.View_VIEW_FULL || req.GetFilter().GetSeverity() != commonv1.Severity_SEVERITY_AWAS {
			t.Errorf("create mapping: %v", req)
		}
		if r := h.do("POST", "/api/v1/webhooks", map[string]any{"url": "https://example.org", "view": "everything"}, nil); r.status != 400 {
			t.Errorf("bad view: %d", r.status)
		}
		b.reply("GetWebhook", reply(&webhookv1.GetWebhookResponse{Subscription: webhook()}))
		if r := h.do("GET", "/api/v1/webhooks/wh-1", nil, nil); r.status != 200 || strings.Contains(string(r.body), "secret") {
			t.Errorf("get: %d %s", r.status, r.body)
		}
		b.reply("DeleteWebhook", reply(&webhookv1.DeleteWebhookResponse{Success: true}))
		if r := h.do("DELETE", "/api/v1/webhooks/wh-1", nil, nil); r.status != 204 || len(r.body) != 0 {
			t.Errorf("delete: %d %q", r.status, r.body)
		}
		b.reply("DeleteWebhook", fail(codes.NotFound))
		if r := h.do("DELETE", "/api/v1/webhooks/wh-2", nil, nil); r.status != 404 {
			t.Errorf("delete missing: %d", r.status)
		}
		b.reply("ListDeliveries", reply(&webhookv1.ListDeliveriesResponse{Items: []*webhookv1.WebhookDelivery{{
			DeliveryId: "d-1", WebhookId: "wh-1", EventId: "evt-1", HazardId: "haz-1", StatusCode: 200,
			Status: webhookv1.DeliveryStatus_DELIVERY_STATUS_SUCCESS, Attempt: 1, DurationMs: 42, DeliveredAt: now,
		}}}))
		if r := h.do("GET", "/api/v1/webhooks/wh-1/deliveries?page_size=10", nil, nil); r.status != 200 || !strings.Contains(string(r.body), `"duration_ms":42`) {
			t.Errorf("deliveries: %d %s", r.status, r.body)
		}
		b.reply("PingWebhook", reply(&webhookv1.PingWebhookResponse{StatusCode: 500, DurationMs: 7, Success: false, Message: "server error"}))
		if r := h.do("POST", "/api/v1/webhooks/wh-1/test", nil, nil); r.status != 200 || !strings.Contains(string(r.body), `"success":false`) {
			t.Errorf("test: %d %s", r.status, r.body)
		}
	})

	t.Run("feeds", func(t *testing.T) {
		b.mu.Lock()
		b.stream = func(req *feedv1.SubscribeEventsRequest, send func(*feedv1.FeedEvent) error) error {
			if req.GetResumeAfter() == "expired" {
				return status.Error(codes.OutOfRange, "gone")
			}
			if req.GetView() == commonv1.View_VIEW_FULL {
				return status.Error(codes.PermissionDenied, "no full")
			}
			for seq := uint64(1); seq <= 2; seq++ {
				if err := send(feedEvent(seq, summary("haz-1"))); err != nil {
					return err
				}
			}
			time.Sleep(120 * time.Millisecond) // long enough for a heartbeat
			return status.Error(codes.OutOfRange, "evicted")
		}
		b.mu.Unlock()

		r := h.do("GET", "/api/v1/feeds/events?severity=SIAGA", nil, map[string]string{"Last-Event-ID": "cursor-0"})
		if r.status != 200 || r.header.Get("Content-Type") != "text/event-stream" || r.header.Get("Cache-Control") != "no-cache, no-transform" {
			t.Fatalf("stream: %d %v", r.status, r.header)
		}
		frames := h.validateFrames(string(r.body))
		if len(frames) != 3 || frames[0].id != "cursor-1" || frames[1].id != "cursor-2" || frames[2].event != "reset_required" || frames[2].id != "" {
			t.Errorf("frames: %+v", frames)
		}
		if !strings.Contains(string(r.body), ": keepalive") {
			t.Error("no heartbeat comment")
		}
		req := b.request("SubscribeEvents").(*feedv1.SubscribeEventsRequest)
		if req.GetResumeAfter() != "cursor-0" || req.GetFilter().GetSeverity() != commonv1.Severity_SEVERITY_SIAGA {
			t.Errorf("subscribe mapping: %v", req)
		}

		if r := h.do("GET", "/api/v1/feeds/seismic", nil, map[string]string{"Last-Event-ID": "expired"}); r.status != 200 || !strings.HasPrefix(string(r.body), "event: reset_required\n") {
			t.Errorf("expired cursor: %d %s", r.status, r.body)
		} else {
			h.validateFrames(string(r.body))
		}
		if got := b.request("SubscribeEvents").(*feedv1.SubscribeEventsRequest).GetFilter().GetHazardType(); got != commonv1.HazardType_HAZARD_TYPE_SEISMIC {
			t.Errorf("seismic alias filter: %v", got)
		}
		if r := h.do("GET", "/api/v1/feeds/volcanic?view=full", nil, nil); r.status != 403 {
			t.Errorf("forbidden before stream: %d %s", r.status, r.body)
		}
		if r := h.do("GET", "/api/v1/feeds/volcanic?source=PVMBG", nil, nil); r.status != 400 {
			t.Errorf("alias filter param: %d", r.status)
		}
	})

	t.Run("gateway", func(t *testing.T) {
		if r := h.do("GET", "/api/v1/nope", nil, nil); r.status != 404 {
			t.Errorf("unknown path: %d", r.status)
		}
		if r := h.do("GET", "/api/v1/webhooks", nil, map[string]string{"X-Correlation-ID": "bad id!"}); r.status != 400 {
			t.Errorf("bad correlation id: %d", r.status)
		}
		r := h.do("GET", "/api/v1/webhooks", nil, nil)
		if len(r.header.Get("X-Correlation-ID")) != 36 {
			t.Errorf("generated correlation id: %q", r.header.Get("X-Correlation-ID"))
		}
	})

	// Every contract operation must have been exercised and validated above.
	spec, _ := contract.Load()
	for _, op := range spec.Operations() {
		if !h.covered[op.Method+" "+op.Path] {
			t.Errorf("operation %s (%s %s) not covered by the contract suite", op.OperationID, op.Method, op.Path)
		}
	}
}

type frame struct{ id, event, data string }

// validateFrames parses SSE frames and validates each data payload against its schema.
func (h *harness) validateFrames(body string) []frame {
	h.t.Helper()
	schemas := map[string]*openapi3.SchemaRef{
		"hazard.upsert":  h.doc.Components.Schemas["HazardFeedEvent"],
		"reset_required": h.doc.Components.Schemas["ResetRequiredEvent"],
	}
	var frames []frame
	var current frame
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if current.event != "" {
				frames = append(frames, current)
			}
			current = frame{}
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "id: "):
			current.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			current.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			current.data = strings.TrimPrefix(line, "data: ")
		default:
			h.t.Errorf("unexpected SSE line %q", line)
		}
	}
	for _, f := range frames {
		schema := schemas[f.event]
		if schema == nil {
			h.t.Errorf("unknown SSE event %q", f.event)
			continue
		}
		var value any
		if err := json.Unmarshal([]byte(f.data), &value); err != nil {
			h.t.Errorf("SSE data is not JSON: %s", f.data)
			continue
		}
		if err := schema.Value.VisitJSON(value); err != nil {
			h.t.Errorf("SSE %s data violates the contract: %v\n%s", f.event, err, f.data)
		}
	}
	return frames
}
