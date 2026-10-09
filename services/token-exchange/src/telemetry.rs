use opentelemetry::propagation::{Extractor, Injector, TextMapCompositePropagator};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{Context, global};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tonic::metadata::{KeyRef, MetadataMap};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// JSON logs filtered by LOG_LEVEL, plus OTLP traces when OTEL_EXPORTER_OTLP_ENDPOINT is set.
/// Spans exist even without an exporter, so Envoy's trace context still reaches Zitadel.
/// Must run outside the Tokio runtime: the HTTP exporter uses a blocking client.
pub fn init() -> SdkTracerProvider {
    global::set_text_map_propagator(propagator());
    let filter = EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info"));
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default();
    let exporter = (!endpoint.trim().is_empty())
        .then(|| SpanExporter::builder().with_http().build())
        .and_then(|exporter| exporter.inspect_err(|error| eprintln!("OTLP exporter disabled: {error}")).ok());
    // The resource reads OTEL_SERVICE_NAME; the fallback covers local runs.
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "token-exchange".into());
    let mut builder = SdkTracerProvider::builder().with_resource(Resource::builder().with_service_name(service_name).build());
    if let Some(exporter) = exporter {
        builder = builder.with_batch_exporter(exporter);
    }
    let provider = builder.build();
    let otel = tracing_opentelemetry::layer().with_tracer(provider.tracer("token-exchange"));
    tracing_subscriber::registry().with(filter).with(tracing_subscriber::fmt::layer().json()).with(otel).init();
    provider
}

/// W3C trace context and baggage, the same pair the other services propagate.
fn propagator() -> TextMapCompositePropagator {
    TextMapCompositePropagator::new(vec![Box::new(TraceContextPropagator::new()), Box::new(BaggagePropagator::new())])
}

/// Trace context Envoy sends with the ext_authz Check call (child of its ext_authz span).
pub fn grpc_parent(metadata: &MetadataMap) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&MetadataExtractor(metadata)))
}

/// Adds `span`'s trace context to an outgoing HTTP request; a no-op while tracing is disabled.
pub fn inject(span: &tracing::Span, headers: &mut http::HeaderMap) {
    let context = span.context();
    global::get_text_map_propagator(|propagator| propagator.inject_context(&context, &mut HeaderInjector(headers)));
}

struct HeaderInjector<'a>(&'a mut http::HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(value)) = (http::HeaderName::from_bytes(key.as_bytes()), http::HeaderValue::from_str(&value)) {
            self.0.insert(name, value);
        }
    }
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

/// Records spans in memory. One process-wide subscriber: thread-scoped ones race with tests on
/// other threads over tracing's cached callsite interest. Tests share it, so they pick their
/// spans by trace id.
#[cfg(test)]
pub mod testing {
    use std::sync::OnceLock;

    use opentelemetry::global;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use tracing_subscriber::layer::SubscriberExt;

    pub struct Capture {
        exporter: InMemorySpanExporter,
        provider: SdkTracerProvider,
    }

    impl Capture {
        pub fn finished_spans(&self) -> Vec<SpanData> {
            self.provider.force_flush().unwrap();
            self.exporter.get_finished_spans().unwrap()
        }
    }

    pub fn capture() -> &'static Capture {
        static CAPTURE: OnceLock<Capture> = OnceLock::new();
        CAPTURE.get_or_init(|| {
            global::set_text_map_propagator(super::propagator());
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build();
            let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("test"));
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer)).unwrap();
            Capture { exporter, provider }
        })
    }
}
