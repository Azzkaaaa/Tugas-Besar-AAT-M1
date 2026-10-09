use std::sync::Arc;
use std::time::Duration;

use bnpb_common::bnpb::common::v1::Source;
use bnpb_event::bnpb::event::v1::HazardEventPublished;
use futures_util::StreamExt;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, LongString};
use lapin::{Connection, ConnectionProperties, ExchangeKind};
use prost::Message;
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::core::feed::FeedService;
use crate::core::models::Publication;
use crate::core::ports::ReplayStore;
use crate::telemetry;

pub const FEED_SERVICE_NAME: &str = "bnpb.feed.v1.EventFeedService";
const CONSUMER_TAG: &str = "event-live-feed";
const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

pub struct Consumer<S> {
    pub feed: Arc<FeedService<S>>,
    pub url: String,
    pub exchange: String,
    pub queue: String,
    pub dead_letter_queue: Option<String>,
    pub health: HealthReporter,
}

impl<S: ReplayStore> Consumer<S> {
    /// Consumes forever, reconnecting with backoff. Health is SERVING only while connected.
    pub async fn run(self) {
        if self.dead_letter_queue.is_none() {
            tracing::warn!("EVENT_FEED_CONSUMER_DLQ is not set; rejected messages will be dropped");
        }
        let mut backoff = RECONNECT_MIN;
        loop {
            match self.consume(&mut backoff).await {
                Ok(()) => tracing::warn!("broker cancelled the consumer; reconnecting"),
                Err(error) => tracing::warn!(%error, "broker connection failed; reconnecting"),
            }
            self.set_health(ServingStatus::NotServing).await;
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    }

    async fn consume(&self, backoff: &mut Duration) -> Result<(), lapin::Error> {
        let connection = Connection::connect(&self.url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        // This service owns its queue and binding; the producer only declares the exchange.
        // The exchange declaration must match every other declarer, or PRECONDITION_FAILED.
        let exchange = ExchangeDeclareOptions { durable: true, ..Default::default() };
        channel.exchange_declare(&self.exchange, ExchangeKind::Fanout, exchange, FieldTable::default()).await?;
        let durable = QueueDeclareOptions { durable: true, ..Default::default() };
        let mut arguments = FieldTable::default();
        if let Some(dead_letter_queue) = &self.dead_letter_queue {
            channel.queue_declare(dead_letter_queue, durable, FieldTable::default()).await?;
            arguments.insert("x-dead-letter-exchange".into(), AMQPValue::LongString(LongString::from("")));
            arguments.insert("x-dead-letter-routing-key".into(), AMQPValue::LongString(dead_letter_queue.as_str().into()));
        }
        channel.queue_declare(&self.queue, durable, arguments).await?;
        channel.queue_bind(&self.queue, &self.exchange, "", QueueBindOptions::default(), FieldTable::default()).await?;
        channel.basic_qos(1, BasicQosOptions::default()).await?;
        let mut deliveries =
            channel.basic_consume(&self.queue, CONSUMER_TAG, BasicConsumeOptions::default(), FieldTable::default()).await?;
        tracing::info!(queue = %self.queue, "consuming hazard events");
        *backoff = RECONNECT_MIN;
        self.set_health(ServingStatus::Serving).await;
        while let Some(delivery) = deliveries.next().await {
            self.handle(delivery?).await?;
        }
        Ok(())
    }

    async fn handle(&self, delivery: Delivery) -> Result<(), lapin::Error> {
        let span = tracing::info_span!("rabbitmq.consume", otel.kind = "consumer", messaging.system = "rabbitmq", messaging.destination = %self.queue);
        if let Some(headers) = delivery.properties.headers() {
            let _ = span.set_parent(telemetry::amqp_parent(headers));
        }
        async {
            match decode(&delivery.data) {
                Ok(publication) => {
                    let event_id = publication.event_id.clone();
                    let stored = self.feed.ingest(publication);
                    // Ack only after the entry is in the replay buffer; redeliveries are deduplicated.
                    delivery.ack(BasicAckOptions::default()).await?;
                    tracing::debug!(%event_id, duplicate = !stored, "hazard event processed");
                }
                Err(reason) => {
                    // Malformed envelopes never succeed on retry: dead-letter immediately.
                    tracing::warn!(%reason, "rejecting hazard event");
                    delivery.nack(BasicNackOptions { requeue: false, ..Default::default() }).await?;
                }
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn set_health(&self, status: ServingStatus) {
        self.health.set_service_status("", status).await;
        self.health.set_service_status(FEED_SERVICE_NAME, status).await;
    }
}

/// Decodes and validates a `HazardEventPublished` envelope (schema version 1).
pub fn decode(data: &[u8]) -> Result<Publication, String> {
    let envelope = HazardEventPublished::decode(data).map_err(|error| format!("not a HazardEventPublished: {error}"))?;
    if envelope.schema_version != 1 {
        return Err(format!("unsupported schema_version {}", envelope.schema_version));
    }
    if envelope.publisher != "aggregator" {
        return Err("publisher must be aggregator".into());
    }
    if envelope.event_id.is_empty() {
        return Err("event_id is empty".into());
    }
    let hazard = envelope.payload.ok_or("payload is missing")?;
    if hazard.hazard_id.is_empty() {
        return Err("payload.hazard_id is empty".into());
    }
    if envelope.source == Source::Unspecified as i32 || envelope.source != hazard.source {
        return Err("source must be set and match payload.source".into());
    }
    Ok(Publication { event_id: envelope.event_id, published_at: envelope.occurred_at, hazard })
}

#[cfg(test)]
mod tests {
    use bnpb_common::bnpb::common::v1::HazardEvent;

    use super::*;

    fn envelope() -> HazardEventPublished {
        HazardEventPublished {
            event_id: "evt-1".into(),
            schema_version: 1,
            source: Source::Bmkg as i32,
            publisher: "aggregator".into(),
            payload: Some(HazardEvent { hazard_id: "h1".into(), source: Source::Bmkg as i32, ..Default::default() }),
            ..Default::default()
        }
    }

    #[test]
    fn accepts_valid_envelopes() {
        let publication = decode(&envelope().encode_to_vec()).unwrap();
        assert_eq!((publication.event_id.as_str(), publication.hazard.hazard_id.as_str()), ("evt-1", "h1"));
    }

    #[test]
    fn rejects_invalid_envelopes() {
        assert!(decode(b"\xff\xff\xff").is_err());
        let cases: [fn(&mut HazardEventPublished); 6] = [
            |e| e.schema_version = 2,
            |e| e.publisher = "someone".into(),
            |e| e.event_id.clear(),
            |e| e.payload = None,
            |e| e.source = Source::Pvmbg as i32,
            |e| e.payload.as_mut().unwrap().hazard_id.clear(),
        ];
        for mutate in cases {
            let mut invalid = envelope();
            mutate(&mut invalid);
            assert!(decode(&invalid.encode_to_vec()).is_err(), "{invalid:?}");
        }
    }
}
