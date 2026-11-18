package grpc

import (
	"context"
	"testing"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type analyticsReader struct{}

func (analyticsReader) ScanAnalytics(_ context.Context, _ string, visit func(storage.AnalyticsRow) error) ([]string, error) {
	for range 1200 {
		if err := visit(storage.AnalyticsRow{OccurredAt: time.Now(), HazardType: "SEISMIC", Severity: "AWAS", Area: "Padang", Tsunami: true}); err != nil {
			return nil, err
		}
	}
	return []string{"analytics-shard-02"}, nil
}
func TestAnalyticsRPCScopesWholeCatalogAndPartial(t *testing.T) {
	srv := NewServer(&config.Config{}, nil, nil, nil)
	srv.SetAnalytics(analyticsReader{})
	if _, err := srv.GetAnalytics(context.Background(), &aggregatorv1.GetAnalyticsRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	out, err := srv.GetAnalytics(partialCaller(), &aggregatorv1.GetAnalyticsRequest{})
	if err != nil || out.Total != 1200 || out.Categories["TSUNAMI"] != 1200 || !out.Partial || len(out.UnavailableUnits) != 1 {
		t.Fatalf("%+v %v", out, err)
	}
	if _, err := srv.GetAnalytics(partialCaller(), &aggregatorv1.GetAnalyticsRequest{Interval: "bad"}); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	srv.analyticsSlots <- struct{}{}
	defer func() { <-srv.analyticsSlots }()
	busy, err := srv.GetAnalytics(partialCaller(), &aggregatorv1.GetAnalyticsRequest{})
	if err != nil || !busy.Partial || busy.UnavailableUnits[0] != "analytics-busy" {
		t.Fatal(busy, err)
	}
}
