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

type gatedSources struct {
	storage.SourceStateStore
	started chan domain.SourceKey
	release chan struct{}
}

func (s gatedSources) Get(ctx context.Context, key domain.SourceKey) (storage.SourceState, error) {
	s.started <- key
	select {
	case <-s.release:
	case <-ctx.Done():
		return storage.SourceState{}, ctx.Err()
	}
	select {
	case <-time.After(70 * time.Millisecond):
	case <-ctx.Done():
		return storage.SourceState{}, ctx.Err()
	}
	now := time.Now().UTC()
	return storage.SourceState{Status: domain.StatusFresh, LastSuccessAt: now, LastAttemptAt: now, Watermark: now}, nil
}

type wanFreshnessHazards struct{ partialHazards }

func (wanFreshnessHazards) HasData(ctx context.Context, _ string) (bool, error) {
	select {
	case <-time.After(70 * time.Millisecond):
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func TestFreshnessProbesOverlapAndAllowTwoWANQuorumReads(t *testing.T) {
	sources := gatedSources{started: make(chan domain.SourceKey, 4), release: make(chan struct{})}
	srv := NewServer(&config.Config{SourceStaleThreshold: time.Minute}, wanFreshnessHazards{}, sources, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan *aggregatorv1.GetSourceStatusesResponse, 1)
	go func() { out, _ := srv.GetSourceStatuses(ctx, &aggregatorv1.GetSourceStatusesRequest{}); done <- out }()
	gate := time.NewTimer(250 * time.Millisecond)
	defer gate.Stop()
	for range domain.AllSources {
		select {
		case <-sources.started:
		case <-gate.C:
			t.Fatal("source probes serialized instead of overlapping")
		}
	}
	close(sources.release)
	select {
	case out := <-done:
		if len(out.Freshness) != len(domain.AllSources) {
			t.Fatal(out)
		}
		for i, info := range out.Freshness {
			if info.Status != commonv1.SourceStatus_SOURCE_STATUS_FRESH || domain.SourceFromProto(info.Source) != domain.AllSources[i].Source {
				t.Fatalf("healthy WAN source was unavailable or reordered: %+v", info)
			}
		}
	case <-ctx.Done():
		t.Fatal("freshness response exceeded its parent deadline")
	}
}
