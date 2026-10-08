package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

// ParentOnlyTracerProvider wraps the global provider so spans start only under an existing span.
// Database queries from the 200 ms outbox poll run without a parent and would otherwise add a
// root trace each time; queries made while handling an ingest poll, RPC, or publish are traced.
func ParentOnlyTracerProvider() trace.TracerProvider {
	return parentOnlyProvider{provider: otel.GetTracerProvider()}
}

type parentOnlyProvider struct {
	embedded.TracerProvider
	provider trace.TracerProvider
}

func (p parentOnlyProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return parentOnlyTracer{tracer: p.provider.Tracer(name, opts...)}
}

type parentOnlyTracer struct {
	embedded.Tracer
	tracer trace.Tracer
}

func (t parentOnlyTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, trace.SpanFromContext(ctx)
	}
	return t.tracer.Start(ctx, name, opts...)
}
