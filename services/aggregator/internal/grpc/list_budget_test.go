package grpc

import (
	"context"
	"testing"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

// slowHydration uses its whole context, as a sequential FULL hydration over
// WAN quorum does, then serves the prefix it completed as a short page.
type slowHydration struct{ wanFreshnessHazards }

func (slowHydration) List(ctx context.Context, _ storage.HazardQuery, _ storage.PageRequest) (storage.HazardPage, error) {
	<-ctx.Done()
	at := time.Now().UTC()
	return storage.HazardPage{NextToken: "after-prefix", Items: []*domain.HazardRecord{
		{HazardID: "prefix", Source: "BMKG", HazardType: "SEISMIC", Severity: "WASPADA", OccurredAt: at, IngestedAt: at},
	}}, nil
}

type freshSources struct{ storage.SourceStateStore }

func (freshSources) Get(ctx context.Context, _ domain.SourceKey) (storage.SourceState, error) {
	select {
	case <-time.After(70 * time.Millisecond):
	case <-ctx.Done():
		return storage.SourceState{}, ctx.Err()
	}
	now := time.Now().UTC()
	return storage.SourceState{Status: domain.StatusFresh, LastSuccessAt: now, LastAttemptAt: now, Watermark: now}, nil
}

func TestListHazardsReservesFreshnessAfterSlowHydration(t *testing.T) {
	srv := NewServer(&config.Config{SourceStaleThreshold: time.Minute}, slowHydration{}, freshSources{}, nil)
	start := time.Now()
	out, err := srv.ListHazards(partialCaller(), &aggregatorv1.ListHazardsRequest{View: commonv1.View_VIEW_SUMMARY})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > listHazardsBudget+100*time.Millisecond {
		t.Fatalf("exceeded RPC budget: %s", elapsed)
	}
	if out.Partial || len(out.Items) != 1 || out.NextPageToken != "after-prefix" || len(out.UnavailableUnits) != 0 {
		t.Fatalf("short page not served complete: %+v", out)
	}
	for _, f := range out.Freshness {
		if f.Status != commonv1.SourceStatus_SOURCE_STATUS_FRESH {
			t.Fatalf("freshness starved by hydration: %v", f)
		}
	}
}
