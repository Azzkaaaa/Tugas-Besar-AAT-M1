use lapin::types::{AMQPValue, FieldTable};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{Context, global};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tonic::metadata::{KeyRef, MetadataMap};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// JSON logs filtered by LOG_LEVEL, plus OTLP traces when OTEL_EXPORTER_OTLP_ENDPOINT is set.
/// Must run outside the Tokio runtime: the HTTP exporter uses a blocking client.
pub fn init() -> Option<SdkTracerProvider> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let filter = EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info"));
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default();
    let provider = (!endpoint.trim().is_empty())
        .then(|| SpanExporter::builder().with_http().build())
        .and_then(|exporter| exporter.inspect_err(|error| eprintln!("OTLP exporter disabled: {error}")).ok())
        .map(|exporter| {
            // The resource reads OTEL_SERVICE_NAME; the fallback covers local runs.
            let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "event-live-feed".into());
            let resource = Resource::builder().with_service_name(service_name).build();
            SdkTracerProvider::builder().with_batch_exporter(exporter).with_resource(resource).build()
        });
    let otel = provider.as_ref().map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer("event-live-feed")));
    tracing_subscriber::registry().with(filter).with(tracing_subscriber::fmt::layer().json()).with(otel).init();
    provider
}

pub fn grpc_parent(metadata: &MetadataMap) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&MetadataExtractor(metadata)))
}

/// Trace context the aggregator injects into AMQP headers when publishing.
pub fn amqp_parent(headers: &FieldTable) -> Context {
    TraceContextPropagator::new().extract(&AmqpExtractor(headers))
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|key| match key {
                KeyRef::Ascii(key) => Some(key.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

struct AmqpExtractor<'a>(&'a FieldTable);

impl Extractor for AmqpExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        match self.0.inner().get(key)? {
            AMQPValue::LongString(value) => std::str::from_utf8(value.as_bytes()).ok(),
            AMQPValue::ShortString(value) => Some(value.as_str()),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        self.0.inner().keys().map(|key| key.as_str()).collect()
    }
}
