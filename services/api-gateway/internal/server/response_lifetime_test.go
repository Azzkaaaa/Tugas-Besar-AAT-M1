package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/contract"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/handlers"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lifetimeClient struct {
	aggregatorv1.AggregatorServiceClient
	fullCalls atomic.Int32
}

func (c *lifetimeClient) item(view commonv1.View) *commonv1.HazardProjection {
	when := timestamppb.New(time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC))
	if view != commonv1.View_VIEW_FULL {
		return &commonv1.HazardProjection{Data: &commonv1.HazardProjection_Summary{Summary: &commonv1.HazardSummary{HazardId: "fire", Source: commonv1.Source_SOURCE_SATELITE,
			HazardType: commonv1.HazardType_HAZARD_TYPE_WILDFIRE, Severity: commonv1.Severity_SEVERITY_AWAS, OccurredAt: when, IngestedAt: when}}}
	}
	c.fullCalls.Add(1)
	zero := 0.0
	attrs, _ := structpb.NewStruct(map[string]any{"detection_keys": []any{"key-1", "key-2"}})
	return &commonv1.HazardProjection{Data: &commonv1.HazardProjection_Full{Full: &commonv1.HazardEvent{HazardId: "fire", SourceRefId: "ref", Source: commonv1.Source_SOURCE_SATELITE,
		HazardType: commonv1.HazardType_HAZARD_TYPE_WILDFIRE, Severity: commonv1.Severity_SEVERITY_AWAS, OccurredAt: when, IngestedAt: when, Latitude: &zero, Longitude: &zero, Attributes: attrs}}}
}
func (c *lifetimeClient) GetHazard(_ context.Context, req *aggregatorv1.GetHazardRequest, _ ...grpc.CallOption) (*aggregatorv1.GetHazardResponse, error) {
	return &aggregatorv1.GetHazardResponse{Item: c.item(req.GetView())}, nil
}
func (c *lifetimeClient) ListHazards(_ context.Context, req *aggregatorv1.ListHazardsRequest, _ ...grpc.CallOption) (*aggregatorv1.ListHazardsResponse, error) {
	return &aggregatorv1.ListHazardsResponse{Items: []*commonv1.HazardProjection{c.item(req.GetView())}}, nil
}

type stalledHTTPWriter struct {
	header  http.Header
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	failure string
}

func (w *stalledHTTPWriter) Header() http.Header { return w.header }
func (*stalledHTTPWriter) WriteHeader(int)       {}
func (w *stalledHTTPWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	switch w.failure {
	case "error":
		return 0, io.ErrClosedPipe
	case "panic":
		panic(http.ErrAbortHandler)
	}
	return len(b), nil
}

func lifetimeHandler(t *testing.T, capacity int) (http.Handler, *lifetimeClient) {
	t.Helper()
	doc, err := contract.Load()
	if err != nil {
		t.Fatal(err)
	}
	client := &lifetimeClient{}
	return New(Options{Contract: doc, Clients: handlers.Clients{Aggregator: client}, MaxInFlight: capacity, ReservedInFlight: 1,
		RequestTimeout: time.Second, PriorityAllowed: func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer verified-field" }}), client
}

func TestFullHTTPAdmissionCoversGeneratedEncodingAndWrites(t *testing.T) {
	for _, failure := range []string{"", "error", "panic"} {
		t.Run(failure, func(t *testing.T) {
			handler, backend := lifetimeHandler(t, 4)
			writer := &stalledHTTPWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{}), failure: failure}
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() {
					if p := recover(); p != nil && p != http.ErrAbortHandler {
						t.Errorf("unexpected panic: %v", p)
					}
				}()
				handler.ServeHTTP(writer, httptest.NewRequest("GET", "/api/v1/hazards/fire?view=full", nil))
			}()
			select {
			case <-writer.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("response did not reach HTTP writer")
			}
			// List and Get share one full-payload credit, even though the first
			// business handler has finished and general capacity remains free.
			for range 3 {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/hazards?view=full", nil))
				if response.Code != 429 {
					t.Fatal("queued HTTP response released full credit", response.Code, response.Body.String())
				}
			}
			if backend.fullCalls.Load() != 1 {
				t.Fatal("shed full requests reached upstream", backend.fullCalls.Load())
			}
			// Notifications can embed the same full wildfire. Their unary reads
			// must share this credit rather than multiply the pod's payloads.
			for _, path := range []string{"/api/v1/notifications?view=full", "/api/v1/notifications/ntf-1?view=full"} {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
				if response.Code != 429 {
					t.Fatal("full notification bypassed shared credit", path, response.Code)
				}
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/hazards/fire?view=summary", nil))
			if response.Code != 200 {
				t.Fatal("full response blocked summary", response.Code, response.Body.String())
			}
			close(writer.release)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("HTTP completion did not release credit")
			}
			response = httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/hazards?view=full", nil))
			if response.Code != 200 || backend.fullCalls.Load() != 2 {
				t.Fatal("HTTP write outcome leaked full credit", response.Code, response.Body.String())
			}
		})
	}
}

func TestPriorityCapacityIncludesSlowHTTPResponseWrites(t *testing.T) {
	handler, _ := lifetimeHandler(t, 2)
	writer := &stalledHTTPWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(writer, httptest.NewRequest("GET", "/api/v1/hazards/fire?view=summary", nil))
	}()
	select {
	case <-writer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("summary did not reach writer")
	}
	defer func() { close(writer.release); <-done }()
	for _, test := range []struct {
		bearer string
		want   int
	}{{"Bearer media", 429}, {"Bearer verified-field", 200}, {"Bearer media", 429}} {
		request := httptest.NewRequest("GET", "/api/v1/hazards/fire?view=summary", nil)
		request.Header.Set("Authorization", test.bearer)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.want {
			t.Fatal("incorrect reserved-capacity result while response blocked", test.bearer, response.Code)
		}
	}
}
