package grpc

import (
	"context"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Liveness (the empty health service) is independent of dependency health. A
// storage partition removes this pod from routing without restarting it, while
// another pod with connectivity can keep serving. Cassandra probes check a
// reachable coordinator rather than demanding every data range be available.
func monitorReadiness(ctx context.Context, srv *health.Server, store storage.HazardStore,
	interval, timeout time.Duration,
) {
	probe, ok := store.(storage.ReadinessChecker)
	if !ok {
		return // Unknown backends remain NOT_SERVING until they supply a probe.
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for ctx.Err() == nil {
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		err := probe.CheckReadiness(checkCtx)
		cancel()
		if ctx.Err() != nil {
			break
		}
		state := healthpb.HealthCheckResponse_SERVING
		if err != nil {
			state = healthpb.HealthCheckResponse_NOT_SERVING
		}
		srv.SetServingStatus("readiness", state)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
