package telemetry

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestTelemetry_InitNoop(t *testing.T) {
	shutdown, err := Init(context.Background(), "test-webhook-provider")
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
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracer := tp.Tracer("test-webhook")
	ctx, span := tracer.Start(context.Background(), "parent-span")
	defer span.End()

	headers := InjectAMQPHeaders(ctx, make(amqp.Table))
	if _, ok := headers["traceparent"]; !ok {
		t.Fatalf("expected traceparent in headers, got: %+v", headers)
	}

	extractedCtx := ExtractAMQPHeaders(context.Background(), headers)
	extractedSpan := sdktrace.SpanFromContext(extractedCtx)
	if extractedSpan == nil {
		t.Fatal("expected valid span context extracted from AMQP headers")
	}
}
