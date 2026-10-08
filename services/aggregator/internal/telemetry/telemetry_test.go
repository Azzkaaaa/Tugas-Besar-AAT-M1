package telemetry

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestTelemetry_InitNoop(t *testing.T) {
	// No-op mode is selected by an empty endpoint; keep the test independent of the shell.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := Init(context.Background(), "test-aggregator")
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown function")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown returned error: %v", err)
	}
}

func TestAMQPHeaders_Propagation(t *testing.T) {
	// Restore the globals so other tests in the package see the defaults.
	prevProvider, prevPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})

	// Setup tracer provider with propagator
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "parent-span")
	defer span.End()

	// Inject into AMQP headers
	headers := InjectAMQPHeaders(ctx, make(amqp.Table))
	if _, ok := headers["traceparent"]; !ok {
		t.Fatalf("expected traceparent in headers, got: %+v", headers)
	}

	// Extract from AMQP headers
	extractedCtx := ExtractAMQPHeaders(context.Background(), headers)
	// The extracted context must carry the parent span as a remote parent.
	extracted := trace.SpanContextFromContext(extractedCtx)
	if !extracted.IsValid() || !extracted.IsRemote() {
		t.Fatalf("expected a valid remote span context, got %+v", extracted)
	}
	parent := span.SpanContext()
	if extracted.TraceID() != parent.TraceID() || extracted.SpanID() != parent.SpanID() {
		t.Fatalf("extracted %s/%s, want %s/%s", extracted.TraceID(), extracted.SpanID(), parent.TraceID(), parent.SpanID())
	}
}
