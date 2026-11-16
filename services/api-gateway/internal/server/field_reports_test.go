package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fieldReportClient struct {
	code   codes.Code
	called bool
}

func (c *fieldReportClient) SubmitFieldReport(_ context.Context, req *aggregatorv1.SubmitFieldReportRequest, _ ...grpc.CallOption) (*aggregatorv1.SubmitFieldReportResponse, error) {
	c.called = true
	if req.GetIdempotencyKey() == "" || req.GetAreaName() != "Padang" {
		return nil, status.Error(codes.InvalidArgument, "request was not forwarded")
	}
	if c.code != codes.OK {
		return nil, status.Error(c.code, "test status")
	}
	return &aggregatorv1.SubmitFieldReportResponse{ReportId: "fr-test"}, nil
}

func TestFieldReportResponses(t *testing.T) {
	const body = `{"area_name":"Padang","description":"Rising water","observed_at":"2026-11-15T08:00:00Z"}`
	for _, test := range []struct {
		name    string
		body    string
		bearer  string
		backend codes.Code
		want    int
		called  bool
	}{
		{"accepted", body, "Bearer test", codes.OK, 202, true},
		{"unavailable", body, "Bearer test", codes.Unavailable, 503, true},
		{"conflict", body, "Bearer test", codes.AlreadyExists, 409, true},
		{"forbidden", body, "Bearer test", codes.PermissionDenied, 403, true},
		{"anonymous", body, "", codes.OK, 401, false},
		{"oversized", strings.Repeat("x", (16<<10)+1), "Bearer test", codes.OK, 413, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &fieldReportClient{code: test.backend}
			g := &gateway{inFlight: make(chan struct{}, 1)}
			req := httptest.NewRequest(http.MethodPost, "/field-reports", strings.NewReader(test.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "1428bc5d-eb41-4c95-b3f2-976ee413ff03")
			req.Header.Set("Authorization", test.bearer)
			result := httptest.NewRecorder()
			g.correlation(g.submitFieldReport(client, time.Second)).ServeHTTP(result, req)
			if result.Code != test.want || client.called != test.called {
				t.Fatalf("status=%d, called=%t, body=%s", result.Code, client.called, result.Body)
			}
			if test.want == 202 && !strings.Contains(result.Body.String(), `"report_id":"fr-test"`) {
				t.Fatalf("incorrect receipt: %s", result.Body)
			}
		})
	}
}

func TestFieldReportsUseReservedCapacityDuringMediaLoad(t *testing.T) {
	client := &fieldReportClient{}
	g := &gateway{inFlight: make(chan struct{}, 1), reserved: make(chan struct{}, 1),
		priorityAllowed: func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer verified-field" }}
	// Model an in-flight Media read holding the only common permit.
	g.inFlight <- struct{}{}
	for _, tc := range []struct {
		bearer string
		status int
	}{{"Bearer media", 429}, {"Bearer verified-field", 202}, {"Bearer verified-field", 202}} {
		client.called = false
		req := httptest.NewRequest("POST", "/field-reports", strings.NewReader(`{"area_name":"Padang","description":"Rising water","observed_at":"2026-11-15T08:00:00Z"}`))
		req.Header.Set("Authorization", tc.bearer)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "1428bc5d-eb41-4c95-b3f2-976ee413ff03")
		response := httptest.NewRecorder()
		g.correlation(g.submitFieldReport(client, time.Second)).ServeHTTP(response, req)
		if response.Code != tc.status || client.called != (tc.status == 202) {
			t.Fatalf("%s: status %d called=%t", tc.bearer, response.Code, client.called)
		}
		if len(g.reserved) != 0 || len(g.inFlight) != 1 {
			t.Fatal("permit leak or common capacity corrupted")
		}
	}
}
