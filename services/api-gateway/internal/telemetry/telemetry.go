// Package telemetry sets up OTLP/HTTP traces (the protocol compose configures) and the W3C
// propagator, following services/aggregator/internal/telemetry without metrics.
package telemetry

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Init returns a shutdown function. Without OTEL_EXPORTER_OTLP_ENDPOINT tracing is a no-op,
// but traceparent is still propagated to the backends.
func Init(ctx context.Context, defaultServiceName string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) == "" {
		slog.Info("OTEL_EXPORTER_OTLP_ENDPOINT not set; tracing disabled")
		return func(context.Context) error { return nil }, nil
	}
	serviceName := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	if serviceName == "" {
		serviceName = defaultServiceName
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}
