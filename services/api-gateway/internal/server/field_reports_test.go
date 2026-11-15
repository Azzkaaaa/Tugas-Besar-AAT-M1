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
