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

	// Webhook metric instruments
	EventsConsumedCounter     metric.Int64Counter
	ConsumerErrorsCounter     metric.Int64Counter
	DeliveriesCounter         metric.Int64Counter
	DeliveryDurationHistogram metric.Float64Histogram
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
		traceExp  sdktrace.SpanExporter
		metricExp sdkmetric.Exporter
	)

	isGRPC := protocol == "grpc" || (!strings.Contains(protocol, "http") && strings.HasSuffix(endpoint, ":4317"))

	if isGRPC {
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
		optsTrace := []otlptracehttp.Option{}
		optsMetric := []otlpmetrichttp.Option{}

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

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp,
			sdktrace.WithBatchTimeout(2*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(3*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

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
	EventsConsumedCounter, err = m.Int64Counter(
		"bnpb_webhook_events_consumed_total",
		metric.WithDescription("Total number of hazard events consumed from RabbitMQ"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	ConsumerErrorsCounter, err = m.Int64Counter(
		"bnpb_webhook_consumer_errors_total",
		metric.WithDescription("Total number of errors encountered while consuming events"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return err
	}

	DeliveriesCounter, err = m.Int64Counter(
		"bnpb_webhook_deliveries_total",
		metric.WithDescription("Total number of webhook deliveries dispatched to subscriber endpoints"),
		metric.WithUnit("{delivery}"),
	)
	if err != nil {
		return err
	}

	DeliveryDurationHistogram, err = m.Float64Histogram(
		"bnpb_webhook_delivery_duration_seconds",
		metric.WithDescription("Duration of HTTP webhook delivery POST requests in seconds"),
		metric.WithUnit("s"),
	)
	return err
}

// Logf returns log.Printf, prefixed with ctx's trace id when there is one, so log lines can be
// matched to their trace.
func Logf(ctx context.Context) func(format string, args ...any) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return log.Printf
	}
	prefix := "trace_id=" + sc.TraceID().String() + " "
	return func(format string, args ...any) { log.Printf(prefix+format, args...) }
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

// InjectAMQPHeaders injects trace context from ctx into amqp.Table headers.
func InjectAMQPHeaders(ctx context.Context, headers amqp.Table) amqp.Table {
	if headers == nil {
		headers = make(amqp.Table)
	}
	carrier := AMQPHeaderCarrier(headers)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return amqp.Table(carrier)
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
