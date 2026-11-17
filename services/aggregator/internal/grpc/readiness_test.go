package grpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type readinessStore struct {
	storage.HazardStore
	unreachable  atomic.Bool
	deadlineSeen atomic.Bool
}

func (s *readinessStore) CheckReadiness(ctx context.Context) error {
	_, ok := ctx.Deadline()
	s.deadlineSeen.Store(ok)
	if s.unreachable.Load() {
		return errors.New("storage network partition")
	}
	return nil
}

func TestReadinessTracksReachabilityWithoutFailingLiveness(t *testing.T) {
	store := &readinessStore{}
	srv := health.NewServer()
	srv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	srv.SetServingStatus("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorReadiness(ctx, srv, store, time.Millisecond, 100*time.Millisecond)
	}()
	t.Cleanup(func() { cancel(); <-done })
	wait := func(want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		until := time.Now().Add(time.Second)
		for time.Now().Before(until) {
			result, err := srv.Check(ctx, &healthpb.HealthCheckRequest{Service: "readiness"})
			if err == nil && result.Status == want {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("readiness never became %s", want)
	}
	wait(healthpb.HealthCheckResponse_SERVING)
	store.unreachable.Store(true)
	wait(healthpb.HealthCheckResponse_NOT_SERVING)
	live, err := srv.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || live.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatal("dependency failure changed liveness")
	}
	store.unreachable.Store(false)
	wait(healthpb.HealthCheckResponse_SERVING)
	if !store.deadlineSeen.Load() {
		t.Fatal("storage probe had no deadline")
	}
}
