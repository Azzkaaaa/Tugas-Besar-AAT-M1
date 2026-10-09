package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	Tracer trace.Tracer
	Meter  metric.Meter

	// Metric instruments
	HazardsIngestedCounter  metric.Int64Counter
	IngestErrorsCounter     metric.Int64Counter
	IngestDurationHistogram metric.Float64Histogram
	OutboxPublishedCounter  metric.Int64Counter
	OutboxErrorsCounter     metric.Int64Counter
	OutboxDurationHistogram metric.Float64Histogram
)

// Init initializes the OpenTelemetry SDK (TracerProvider, MeterProvider, and Propagator).
// It respects standard OTel env vars:
//   - OTEL_EXPORTER_OTLP_ENDPOINT (e.g. http://otel-lgtm:4318 or otel-lgtm:4317)
//   - OTEL_EXPORTER_OTLP_PROTOCOL (e.g. http/protobuf, grpc)
//   - OTEL_SERVICE_NAME (overrides defaultServiceName)
//
// Returns a shutdown function to flush pending spans and metrics.
func Init(ctx context.Context, defaultServiceName string) (func(context.Context) error, error) {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if strings.TrimSpace(serviceName) == "" {
		serviceName = defaultServiceName
	}

	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		log.Printf("[Telemetry] OTEL_EXPORTER_OTLP_ENDPOINT not set — telemetry running in no-op mode.")
		initNoop(serviceName)
		return func(context.Context) error { return nil }, nil
	}

	protocol := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	appEnv := os.Getenv("APP_ENV")
	if appEnv == "" {
		appEnv = "development"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String("1.0.0"),
			attribute.String("deployment.environment", appEnv),
		),
		resource.WithProcess(),
		resource.WithOS(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create otel resource: %w", err)
	}

	var (
		traceExp sdktrace.SpanExporter
		metricExp sdkmetric.Exporter
	)

	// Determine if gRPC or HTTP should be used based on protocol or endpoint
	isGRPC := protocol == "grpc" || (!strings.Contains(protocol, "http") && strings.HasSuffix(endpoint, ":4317"))

	if isGRPC {
		// gRPC exporter expects host:port without URL scheme
		grpcEndpoint := endpoint
		if strings.HasPrefix(grpcEndpoint, "http://") {
			grpcEndpoint = strings.TrimPrefix(grpcEndpoint, "http://")
		} else if strings.HasPrefix(grpcEndpoint, "https://") {
			grpcEndpoint = strings.TrimPrefix(grpcEndpoint, "https://")
		}

		traceExp, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(grpcEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create otlp trace grpc exporter: %w", err)
		}

		metricExp, err = otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(grpcEndpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create otlp metric grpc exporter: %w", err)
		}
		log.Printf("[Telemetry] Initialized OTLP gRPC exporters targeting %s (service=%s)", grpcEndpoint, serviceName)
	} else {
		// HTTP exporter (default http/protobuf)
		optsTrace := []otlptracehttp.Option{}
		optsMetric := []otlpmetrichttp.Option{}

		// Parse URL if full URI is given
		if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
			optsTrace = append(optsTrace, otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"))
			optsMetric = append(optsMetric, otlpmetrichttp.WithEndpointURL(endpoint+"/v1/metrics"))
		} else {
			optsTrace = append(optsTrace, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
			optsMetric = append(optsMetric, otlpmetrichttp.WithEndpoint(endpoint), otlpmetrichttp.WithInsecure())
		}

		traceExp, err = otlptrace.New(ctx, otlptracehttp.NewClient(optsTrace...))
		if err != nil {
			return nil, fmt.Errorf("failed to create otlp trace http exporter: %w", err)
		}

		metricExp, err = otlpmetrichttp.New(ctx, optsMetric...)
		if err != nil {
			return nil, fmt.Errorf("failed to create otlp metric http exporter: %w", err)
		}
		log.Printf("[Telemetry] Initialized OTLP HTTP exporters targeting %s (service=%s)", endpoint, serviceName)
	}

	// TracerProvider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp,
			sdktrace.WithBatchTimeout(2*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	// MeterProvider
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(3*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	// Global TextMap Propagator (W3C TraceContext + Baggage)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	Tracer = tp.Tracer(serviceName)
	Meter = mp.Meter(serviceName)

	if err := initInstruments(Meter); err != nil {
		log.Printf("[Telemetry] Warning: failed initializing metric instruments: %v", err)
	}

	shutdown := func(shutdownCtx context.Context) error {
		var errs []error
		if err := tp.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("tracer shutdown: %w", err))
		}
		if err := mp.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("meter shutdown: %w", err))
		}
		return errors.Join(errs...)
	}

	return shutdown, nil
}

func initNoop(serviceName string) {
	Tracer = otel.GetTracerProvider().Tracer(serviceName)
	Meter = otel.GetMeterProvider().Meter(serviceName)
	_ = initInstruments(Meter)
}

func initInstruments(m metric.Meter) error {
	var err error
	HazardsIngestedCounter, err = m.Int64Counter(
		"bnpb_hazards_ingested_total",
		metric.WithDescription("Total number of hazard events ingested from upstream sources"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	IngestErrorsCounter, err = m.Int64Counter(
		"bnpb_ingest_poll_errors_total",
		metric.WithDescription("Total number of polling errors from upstream sources"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return err
	}

	IngestDurationHistogram, err = m.Float64Histogram(
		"bnpb_ingest_poll_duration_seconds",
		metric.WithDescription("Duration of upstream polling calls in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	OutboxPublishedCounter, err = m.Int64Counter(
		"bnpb_outbox_published_total",
		metric.WithDescription("Total number of outbox records published to RabbitMQ"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return err
	}

	OutboxErrorsCounter, err = m.Int64Counter(
		"bnpb_outbox_publish_errors_total",
		metric.WithDescription("Total number of RabbitMQ publish failures"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return err
	}

	OutboxDurationHistogram, err = m.Float64Histogram(
		"bnpb_outbox_publish_duration_seconds",
		metric.WithDescription("Duration of outbox publishing operations in seconds"),
		metric.WithUnit("s"),
	)
	return err
}

// ExternalTransport records client spans for requests that leave the system but injects no
// trace context (traceparent, tracestate, baggage): third parties never learn trace ids.
func ExternalTransport(base http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(base, otelhttp.WithPropagators(propagation.NewCompositeTextMapPropagator()))
}

// AMQPHeaderCarrier adapts amqp.Table to propagation.TextMapCarrier.
type AMQPHeaderCarrier amqp.Table

func (c AMQPHeaderCarrier) Get(key string) string {
	if val, ok := c[key]; ok {
		switch v := val.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		}
		return fmt.Sprintf("%v", val)
	}
	return ""
}

func (c AMQPHeaderCarrier) Set(key string, value string) {
	c[key] = value
}

func (c AMQPHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectAMQPHeaders injects the trace context from ctx into amqp.Table headers.
func InjectAMQPHeaders(ctx context.Context, headers amqp.Table) amqp.Table {
	if headers == nil {
		headers = make(amqp.Table)
	}
	carrier := AMQPHeaderCarrier(headers)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return amqp.Table(carrier)
}

// TraceCarrier captures ctx's trace context (W3C traceparent, tracestate and baggage) so it can
// be stored and restored later, e.g. on an outbox row. Empty when ctx carries no span.
func TraceCarrier(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

// ContextFromCarrier restores a context saved by TraceCarrier as the remote parent of ctx.
func ContextFromCarrier(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

// ExtractAMQPHeaders extracts trace context from amqp.Table headers into a new context.
func ExtractAMQPHeaders(ctx context.Context, headers amqp.Table) context.Context {
	if headers == nil {
		return ctx
	}
	carrier := AMQPHeaderCarrier(headers)
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// CleanEndpointURL strips paths and returns the base URL or host.
func CleanEndpointURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if u.Host != "" {
		return u.Host
	}
	return raw
}
