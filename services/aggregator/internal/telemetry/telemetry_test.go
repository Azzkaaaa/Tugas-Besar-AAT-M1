package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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

// The outbox stores the ingest span's carrier; the publish span must continue that trace, with
// baggage intact, even though the publisher runs from the poll loop's empty context.
func TestTraceCarrier_RelaysThroughOutbox(t *testing.T) {
	prevProvider, prevPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	tracer := tp.Tracer("test")

	member, _ := baggage.NewMember("hazard.source", "BMKG")
	bag, _ := baggage.New(member)
	pollCtx, poll := tracer.Start(baggage.ContextWithBaggage(context.Background(), bag), "bmkg.poll")
	carrier := TraceCarrier(pollCtx)
	poll.End()
	if carrier["traceparent"] == "" || carrier["baggage"] == "" {
		t.Fatalf("carrier lacks traceparent or baggage: %+v", carrier)
	}

	parent := ContextFromCarrier(context.Background(), carrier)
	if got := baggage.FromContext(parent).Member("hazard.source").Value(); got != "BMKG" {
		t.Fatalf("baggage hazard.source = %q, want BMKG", got)
	}
	_, publish := tracer.Start(parent, "rabbitmq.publish", trace.WithSpanKind(trace.SpanKindProducer))
	publish.End()

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	pollSpan, publishSpan := spans[0], spans[1]
	if publishSpan.SpanContext().TraceID() != pollSpan.SpanContext().TraceID() {
		t.Fatalf("publish trace %s, want poll trace %s", publishSpan.SpanContext().TraceID(), pollSpan.SpanContext().TraceID())
	}
	if publishSpan.Parent().SpanID() != pollSpan.SpanContext().SpanID() {
		t.Fatalf("publish parent %s, want poll span %s", publishSpan.Parent().SpanID(), pollSpan.SpanContext().SpanID())
	}
}

func TestContextFromCarrier_EmptyKeepsContext(t *testing.T) {
	ctx := context.Background()
	if got := ContextFromCarrier(ctx, nil); got != ctx {
		t.Fatal("an empty carrier must return the context unchanged")
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
