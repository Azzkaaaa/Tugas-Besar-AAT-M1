package telemetry

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
)

// The container images are CGO-free and run without $USER; telemetry must still start.
func TestInitWithoutUserEnvironment(t *testing.T) {
	prevLog, prevTP, prevMP := slog.Default(), otel.GetTracerProvider(), otel.GetMeterProvider()
	t.Cleanup(func() { slog.SetDefault(prevLog); otel.SetTracerProvider(prevTP); otel.SetMeterProvider(prevMP) })
	t.Setenv("USER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	shutdown, err := Init(context.Background(), "aggregator-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = shutdown(ctx)
}
