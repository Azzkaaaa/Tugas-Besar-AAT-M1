package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTelemetry_InitNoop(t *testing.T) {
	// No-op mode is selected by an empty endpoint; keep the test independent of the shell.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
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
	// Restore the globals so other tests in the package see the defaults.
	prevProvider, prevPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})

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

// Requests to third parties keep their client span but carry no trace context.
func TestExternalTransport_InjectsNoTraceContext(t *testing.T) {
	prevProvider, prevPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	var received http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = r.Header.Clone()
	}))
	defer server.Close()

	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := (&http.Client{Transport: ExternalTransport(http.DefaultTransport)}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	span.End()

	for _, name := range []string{"Traceparent", "Tracestate", "Baggage"} {
		if value := received.Get(name); value != "" {
			t.Errorf("third party received %s: %q", name, value)
		}
	}
	if len(recorder.Ended()) != 2 {
		t.Fatalf("got %d spans, want the parent and the client span", len(recorder.Ended()))
	}
}
