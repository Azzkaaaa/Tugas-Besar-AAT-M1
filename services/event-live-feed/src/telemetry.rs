use lapin::types::{AMQPValue, FieldTable};
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

use crate::core::models::TraceCarrier;

/// JSON logs filtered by LOG_LEVEL, plus OTLP traces when OTEL_EXPORTER_OTLP_ENDPOINT is set.
/// Must run outside the Tokio runtime: the HTTP exporter uses a blocking client.
pub fn init() -> Option<SdkTracerProvider> {
    global::set_text_map_propagator(propagator());
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

/// W3C trace context and baggage, the same pair the Go and .NET services propagate.
fn propagator() -> TextMapCompositePropagator {
    TextMapCompositePropagator::new(vec![Box::new(TraceContextPropagator::new()), Box::new(BaggagePropagator::new())])
}

pub fn grpc_parent(metadata: &MetadataMap) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&MetadataExtractor(metadata)))
}

/// Trace context the aggregator injects into AMQP headers when publishing. Missing or malformed
/// headers give an empty context: the consumer span starts a new trace and the message is
/// processed as usual.
pub fn amqp_parent(headers: &FieldTable) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&AmqpExtractor(headers)))
}

/// `span`'s trace context as plain propagation fields, to store with a publication.
pub fn carrier(span: &tracing::Span) -> TraceCarrier {
    let context = span.context();
    let mut carrier = TraceCarrier::new();
    global::get_text_map_propagator(|propagator| propagator.inject_context(&context, &mut CarrierInjector(&mut carrier)));
    carrier
}

/// The context saved by [`carrier`]; empty when the carrier is.
pub fn carrier_parent(carrier: &TraceCarrier) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&CarrierExtractor(carrier)))
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

struct CarrierInjector<'a>(&'a mut TraceCarrier);

impl Injector for CarrierInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.retain(|(name, _)| name != key);
        self.0.push((key.to_owned(), value));
    }
}

struct CarrierExtractor<'a>(&'a TraceCarrier);

impl Extractor for CarrierExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.iter().find(|(name, _)| name == key).map(|(_, value)| value.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(name, _)| name.as_str()).collect()
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

struct AmqpExtractor<'a>(&'a FieldTable);

impl Extractor for AmqpExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        match self.0.inner().get(key)? {
            AMQPValue::LongString(value) => std::str::from_utf8(value.as_bytes()).ok(),
            AMQPValue::ShortString(value) => Some(value.as_str()),
            AMQPValue::ByteArray(value) => std::str::from_utf8(value.as_slice()).ok(),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        self.0.inner().keys().map(|key| key.as_str()).collect()
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

#[cfg(test)]
mod tests {
    use lapin::types::LongString;
    use opentelemetry::baggage::BaggageExt;
    use opentelemetry::trace::TraceContextExt;
    use opentelemetry::{Context, KeyValue};

    use super::*;

    const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    fn amqp_headers(traceparent: &str) -> FieldTable {
        let mut headers = FieldTable::default();
        headers.insert("traceparent".into(), AMQPValue::LongString(LongString::from(traceparent)));
        headers.insert("baggage".into(), AMQPValue::LongString(LongString::from("hazard.source=BMKG")));
        headers
    }

    #[test]
    fn amqp_parent_reads_trace_context_and_baggage() {
        let _capture = testing::capture();
        let context = amqp_parent(&amqp_headers(TRACEPARENT));
        assert_eq!(context.span().span_context().trace_id().to_string(), "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(context.baggage().get("hazard.source").map(|value| value.to_string()), Some("BMKG".into()));
    }

    #[test]
    fn malformed_amqp_trace_context_starts_a_new_trace() {
        let _capture = testing::capture();
        let context = amqp_parent(&amqp_headers("00-not-a-trace-01"));
        assert!(!context.span().span_context().is_valid());
    }

    #[test]
    fn carrier_round_trips_span_context_and_baggage() {
        let _capture = testing::capture();
        let parent = Context::new().with_baggage(vec![KeyValue::new("hazard.source", "PVMBG")]);
        let span = tracing::info_span!("rabbitmq.consume");
        let _ = span.set_parent(parent);
        let saved = carrier(&span);
        let restored = carrier_parent(&saved);
        assert_eq!(restored.span().span_context().trace_id(), span.context().span().span_context().trace_id());
        assert_eq!(restored.span().span_context().span_id(), span.context().span().span_context().span_id());
        assert_eq!(restored.baggage().get("hazard.source").map(|value| value.to_string()), Some("PVMBG".into()));
        assert!(!carrier_parent(&Vec::new()).span().span_context().is_valid());
    }
}
