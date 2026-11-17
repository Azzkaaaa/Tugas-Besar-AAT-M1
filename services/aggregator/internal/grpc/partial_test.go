package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type partialHazards struct {
	storage.HazardStore
	page    storage.HazardPage
	failure error
}

func (s partialHazards) List(context.Context, storage.HazardQuery, storage.PageRequest) (storage.HazardPage, error) {
	return s.page, s.failure
}
func (s partialHazards) Get(context.Context, string) (*domain.HazardRecord, error) {
	return nil, s.failure
}
func (s partialHazards) HasData(context.Context, string) (bool, error) { return true, nil }

type unavailableSources struct{ storage.SourceStateStore }

func (unavailableSources) Get(ctx context.Context, _ domain.SourceKey) (storage.SourceState, error) {
	<-ctx.Done()
	return storage.SourceState{}, ctx.Err()
}

func partialCaller() context.Context {
	return context.WithValue(context.Background(), auth.CallerContextKey, &auth.CallerContextData{Scopes: []string{"hazards:read:summary"}})
}

func TestPartialRPCPreservesRecordsAndBoundsFreshnessFailures(t *testing.T) {
	at := time.Now().UTC()
	store := partialHazards{page: storage.HazardPage{Partial: true, UnavailableUnits: []string{"bucket-2"}, Items: []*domain.HazardRecord{
		{HazardID: "available", Source: "BMKG", HazardType: "SEISMIC", Severity: "WASPADA", OccurredAt: at, IngestedAt: at},
	}}}
	srv := NewServer(&config.Config{SourceStaleThreshold: time.Minute}, store, unavailableSources{}, nil)
	start := time.Now()
	out, err := srv.ListHazards(partialCaller(), &aggregatorv1.ListHazardsRequest{View: commonv1.View_VIEW_SUMMARY})
	if err != nil || !out.Partial || len(out.Items) != 1 || len(out.UnavailableUnits) != 5 {
		t.Fatalf("%+v %v", out, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("freshness failures blocked response")
	}
	for _, f := range out.Freshness {
		if f.Status != commonv1.SourceStatus_SOURCE_STATUS_UNAVAILABLE {
			t.Fatal(f)
		}
	}
}

func TestGetStorageFailureIsPartialRatherThanNotFound(t *testing.T) {
	for _, failure := range []error{errors.New("node unavailable"), nil} {
		srv := NewServer(&config.Config{}, partialHazards{failure: failure}, unavailableSources{}, nil)
		out, err := srv.GetHazard(partialCaller(), &aggregatorv1.GetHazardRequest{HazardId: "unknown"})
		if failure != nil {
			if err != nil || !out.Partial || out.Item != nil || len(out.UnavailableUnits) == 0 {
				t.Fatalf("%+v %v", out, err)
			}
		} else if status.Code(err) != codes.NotFound {
			t.Fatalf("confirmed absence: %v", err)
		}
	}
}
