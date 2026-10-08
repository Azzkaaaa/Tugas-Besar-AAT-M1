use axum::http::HeaderMap;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{Context, global};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// OTLP traces when OTEL_EXPORTER_OTLP_ENDPOINT is set; the JSON request logs stay on println.
/// Must run outside the Tokio runtime: the HTTP exporter uses a blocking client.
pub fn init() -> Option<SdkTracerProvider> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default();
    if endpoint.trim().is_empty() {
        return None;
    }
    let exporter = SpanExporter::builder().with_http().build().inspect_err(|error| eprintln!("OTLP exporter disabled: {error}")).ok()?;
    // The resource reads OTEL_SERVICE_NAME; the fallback covers local runs.
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "zitadel-claims".into());
    let resource = Resource::builder().with_service_name(service_name).build();
    let provider = SdkTracerProvider::builder().with_batch_exporter(exporter).with_resource(resource).build();
    global::set_text_map_propagator(TraceContextPropagator::new());
    let otel = tracing_opentelemetry::layer().with_tracer(provider.tracer("zitadel-claims"));
    tracing_subscriber::registry().with(otel).init();
    Some(provider)
}

/// Trace context Zitadel sends with the Actions target call.
pub fn http_parent(headers: &HeaderMap) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(headers)))
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}
