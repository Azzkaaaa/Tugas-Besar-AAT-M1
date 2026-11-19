package grpc

import (
	"context"
	"strings"
	"testing"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/auth"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type projectionHazards struct {
	partialHazards
	query storage.HazardQuery
}

func TestListChecksExactCompleteResponseBudget(t *testing.T) {
	h := &domain.HazardRecord{HazardID: "large", Source: domain.SourceSatellite, Attributes: map[string]any{"body": strings.Repeat("x", (16<<20)-(2<<10))}}
	store := &projectionHazards{partialHazards: partialHazards{page: storage.HazardPage{Items: []*domain.HazardRecord{h}}}}
	srv := NewServer(&config.Config{}, store, unavailableSources{}, nil)
	caller := context.WithValue(context.Background(), auth.CallerContextKey, &auth.CallerContextData{Scopes: []string{"hazards:read:full"}})
	req := &aggregatorv1.ListHazardsRequest{View: commonv1.View_VIEW_FULL, Filter: &commonv1.HazardFilter{Source: commonv1.Source_SOURCE_SATELITE}}
	response, err := srv.ListHazards(caller, req)
	if err != nil || proto.Size(response) > 16<<20 {
		t.Fatal("complete record fitting the transport was rejected", err)
	}
	h.Attributes["body"] = strings.Repeat("x", 16<<20)
	if _, err := srv.ListHazards(caller, req); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized whole response accepted", err)
	}
}

func (p *projectionHazards) List(_ context.Context, q storage.HazardQuery, _ storage.PageRequest) (storage.HazardPage, error) {
	p.query = q
	return p.page, p.failure
}

func TestListRequestsProjectionBeforeStorageReads(t *testing.T) {
	store := &projectionHazards{}
	srv := NewServer(&config.Config{}, store, unavailableSources{}, nil)
	caller := context.WithValue(context.Background(), auth.CallerContextKey, &auth.CallerContextData{Scopes: []string{"hazards:read:full", "hazards:read:summary"}})
	for _, view := range []commonv1.View{commonv1.View_VIEW_UNSPECIFIED, commonv1.View_VIEW_SUMMARY, commonv1.View_VIEW_FULL} {
		if _, err := srv.ListHazards(caller, &aggregatorv1.ListHazardsRequest{View: view, Filter: &commonv1.HazardFilter{Source: commonv1.Source_SOURCE_SATELITE}}); err != nil {
			t.Fatal(err)
		}
		if store.query.SummaryOnly != (view != commonv1.View_VIEW_FULL) {
			t.Fatal("projection applied after payload read", view)
		}
	}
	store.failure = storage.ErrHazardProjectionTooLarge
	if _, err := srv.ListHazards(caller, &aggregatorv1.ListHazardsRequest{View: commonv1.View_VIEW_FULL}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
}
