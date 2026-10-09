use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bnpb_common::bnpb::common::v1::View;
use bnpb_feed::bnpb::feed::v1::event_feed_service_server::EventFeedService;
use bnpb_feed::bnpb::feed::v1::{FeedEvent, SubscribeEventsRequest, SubscribeEventsResponse};
use futures_util::Stream;
use opentelemetry::trace::{SpanContext, TraceContextExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::adapters::inbound::cursor::{Binding, CursorCodec, CursorError};
use crate::adapters::inbound::hazard::{Filter, authorize, project, resolve_view};
use crate::adapters::inbound::session::{SessionPolicy, supervise};
use crate::core::feed::{FeedError, FeedService, FeedSubscription};
use crate::core::models::FeedEntry;
use crate::core::ports::{ActorVerifier, AuthError, ReplayStore, TokenRenewer};
use crate::telemetry;

const CORRELATION_HEADER: &str = "x-correlation-id";
const STREAM_BUFFER: usize = 16;

pub struct FeedGrpc<S, V, R> {
    pub feed: Arc<FeedService<S>>,
    pub verifier: Arc<V>,
    pub renewer: Option<Arc<R>>,
    pub cursors: Arc<CursorCodec>,
    pub policy: SessionPolicy,
}

#[tonic::async_trait]
impl<S: ReplayStore, V: ActorVerifier, R: TokenRenewer> EventFeedService for FeedGrpc<S, V, R> {
    type SubscribeEventsStream = FeedStream;

    async fn subscribe_events(&self, request: Request<SubscribeEventsRequest>) -> Result<Response<FeedStream>, Status> {
        let span = tracing::info_span!("bnpb.feed.v1.EventFeedService/SubscribeEvents", otel.kind = "server", rpc.system = "grpc");
        let _ = span.set_parent(telemetry::grpc_parent(request.metadata()));
        let correlation_id = request.metadata().get(CORRELATION_HEADER).cloned();

        let actor = match self.verifier.verify(bearer(request.metadata())?).instrument(span.clone()).await {
            Ok(actor) => actor,
            Err(AuthError::Rejected(_)) => return Err(Status::unauthenticated("invalid or expired access token")),
            Err(AuthError::Unavailable(reason)) => {
                tracing::warn!(parent: &span, %reason, "token validation unavailable");
                return Err(Status::unavailable("token validation is unavailable"));
            }
        };
        let request = request.into_inner();
        let view = resolve_view(request.view)?;
        authorize(&actor.scopes, view)?;
        let filter = Filter::parse(request.filter)?;
        let binding = Binding { subject: &actor.subject, view, filter: &filter };
        let after = match request.resume_after.as_deref().filter(|cursor| !cursor.is_empty()) {
            None => None,
            Some(cursor) => Some(self.cursors.decode(cursor, &binding).map_err(|error| match error {
                CursorError::Invalid => Status::invalid_argument("resume_after is not a cursor of this feed, client and view"),
                CursorError::Expired => expired(),
            })?),
        };
        let subscription = self.feed.subscribe(after).map_err(feed_status)?;

        let (events, data) = mpsc::channel(STREAM_BUFFER);
        let (end, ended) = mpsc::channel(2);
        let session = {
            let end = end.clone();
            let session = supervise(actor.clone(), view, self.verifier.clone(), self.renewer.clone(), self.policy);
            tokio::spawn(async move {
                let _ = end.try_send(session.await);
            })
        };
        let forward = Forward { cursors: self.cursors.clone(), subject: actor.subject, view, filter };
        let pump = tokio::spawn(forward.run(subscription, events, end).instrument(span));

        let mut response = Response::new(FeedStream { data, ended, _tasks: [AbortOnDrop(session), AbortOnDrop(pump)], done: false });
        if let Some(correlation_id) = correlation_id {
            response.metadata_mut().insert(CORRELATION_HEADER, correlation_id);
        }
        Ok(response)
    }
}

fn bearer(metadata: &MetadataMap) -> Result<&str, Status> {
    let header = metadata.get("authorization").and_then(|value| value.to_str().ok());
    let token = header.and_then(|value| value.split_once(' ')).filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"));
    token.map(|(_, token)| token.trim()).filter(|token| !token.is_empty()).ok_or_else(|| Status::unauthenticated("bearer token required"))
}

fn expired() -> Status {
    Status::out_of_range("replay no longer covers this cursor; take a new snapshot")
}

fn feed_status(error: FeedError) -> Status {
    match error {
        FeedError::Expired => expired(),
        FeedError::Closed => Status::unavailable("feed is shutting down"),
    }
}

/// Filters, projects and forwards entries for one subscription.
struct Forward {
    cursors: Arc<CursorCodec>,
    subject: String,
    view: View,
    filter: Filter,
}

impl Forward {
    async fn run<S: ReplayStore>(
        self,
        mut subscription: FeedSubscription<S>,
        events: mpsc::Sender<SubscribeEventsResponse>,
        end: mpsc::Sender<Status>,
    ) {
        // The SubscribeEvents span this task is instrumented with.
        let stream = tracing::Span::current().context().span().span_context().clone();
        loop {
            let entry = match subscription.next().await {
                Ok(entry) => entry,
                Err(error) => {
                    let _ = end.try_send(feed_status(error));
                    return;
                }
            };
            if !self.filter.matches(&entry.publication.hazard) {
                continue;
            }
            if events.send(self.response(&entry)).instrument(self.deliver_span(&entry, &stream)).await.is_err() {
                return;
            }
        }
    }

    /// A child of the event's rabbitmq.consume span, linked to this subscriber's stream: the
    /// hazard's trace shows its fan-out, and an hour-long stream trace doesn't collect every
    /// delivery. It covers the hand-off into the stream buffer (backpressure from a slow client).
    fn deliver_span(&self, entry: &FeedEntry, stream: &SpanContext) -> tracing::Span {
        let span = tracing::info_span!(
            parent: None,
            "feed.deliver",
            feed.sequence = entry.sequence,
            feed.view = self.view.as_str_name(),
            feed.lag_ms = u64::try_from(entry.stored_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            event.id = %entry.publication.event_id,
            hazard.id = %entry.publication.hazard.hazard_id,
        );
        let _ = span.set_parent(telemetry::carrier_parent(&entry.publication.trace));
        if stream.is_valid() {
            span.add_link(stream.clone());
        }
        span
    }

    fn response(&self, entry: &FeedEntry) -> SubscribeEventsResponse {
        let binding = Binding { subject: &self.subject, view: self.view, filter: &self.filter };
        SubscribeEventsResponse {
            event: Some(FeedEvent {
                event_id: entry.publication.event_id.clone(),
                sequence: entry.sequence,
                hazard: Some(project(&entry.publication.hazard, self.view)),
                published_at: entry.publication.published_at,
                resume_cursor: self.cursors.encode(entry.sequence, &binding),
            }),
        }
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Response stream. A terminal status (expiry, revocation, lost replay) takes priority over
/// buffered events, so nothing is delivered after authorization ends. Dropping the stream
/// (client gone) aborts the session and forwarding tasks.
pub struct FeedStream {
    data: mpsc::Receiver<SubscribeEventsResponse>,
    ended: mpsc::Receiver<Status>,
    _tasks: [AbortOnDrop; 2],
    done: bool,
}

impl Stream for FeedStream {
    type Item = Result<SubscribeEventsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if let Poll::Ready(status) = self.ended.poll_recv(cx) {
            self.done = true;
            return Poll::Ready(Some(Err(status.unwrap_or_else(|| Status::internal("feed stream ended unexpectedly")))));
        }
        match self.data.poll_recv(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(Some(Ok(event))),
            // The forwarder only stops after reporting a status, which `ended` yields next.
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bnpb_common::bnpb::common::v1::hazard_projection::Data;
    use bnpb_common::bnpb::common::v1::{HazardEvent, HazardFilter, HazardType, Source};
    use futures_util::StreamExt;
    use tokio::time::Instant;
    use tonic::Code;

    use super::*;
    use crate::adapters::inbound::hazard::{SCOPE_FULL, SCOPE_SUMMARY};
    use crate::adapters::outbound::memory_store::MemoryStore;
    use crate::core::models::{Actor, Publication};

    struct Verifier;

    impl ActorVerifier for Verifier {
        async fn verify(&self, token: &str) -> Result<Actor, AuthError> {
            let scopes: &[&str] = match token {
                "media" => &[SCOPE_SUMMARY],
                "field" => &[SCOPE_SUMMARY, SCOPE_FULL],
                "down" => return Err(AuthError::Unavailable("down".into())),
                _ => return Err(AuthError::Rejected("bad".into())),
            };
            Ok(Actor {
                subject: token.into(),
                scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
                expires_at: Instant::now() + Duration::from_secs(60),
                token: token.into(),
            })
        }
    }

    struct NoRenewer;

    impl TokenRenewer for NoRenewer {
        async fn renew(&self, _token: &str) -> Result<String, AuthError> {
            Err(AuthError::Rejected("unused".into()))
        }
    }

    fn service() -> (FeedGrpc<MemoryStore, Verifier, NoRenewer>, Arc<FeedService<MemoryStore>>) {
        let feed = Arc::new(FeedService::new(Arc::new(MemoryStore::with_epoch(5, Duration::from_secs(600), 100))));
        let policy = SessionPolicy { renew_before: Duration::from_secs(15), max_stream: Duration::from_secs(3600) };
        let grpc =
            FeedGrpc { feed: feed.clone(), verifier: Arc::new(Verifier), renewer: None, cursors: Arc::new(CursorCodec::new(5)), policy };
        (grpc, feed)
    }

    fn publication(id: &str, source: Source, hazard_type: HazardType) -> Publication {
        Publication {
            event_id: id.into(),
            published_at: None,
            hazard: HazardEvent {
                hazard_id: id.into(),
                source: source as i32,
                hazard_type: hazard_type as i32,
                source_ref_id: "secret-ref".into(),
                latitude: Some(1.0),
                ..Default::default()
            },
            trace: Vec::new(),
        }
    }

    fn request(token: &str, message: SubscribeEventsRequest) -> Request<SubscribeEventsRequest> {
        let mut request = Request::new(message);
        request.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
        request.metadata_mut().insert(CORRELATION_HEADER, "corr-1".parse().unwrap());
        request
    }

    async fn code(grpc: &FeedGrpc<MemoryStore, Verifier, NoRenewer>, request: Request<SubscribeEventsRequest>) -> Code {
        grpc.subscribe_events(request).await.err().map_or(Code::Ok, |status| status.code())
    }

    #[tokio::test]
    async fn rejects_before_streaming() {
        let (grpc, _) = service();
        assert_eq!(code(&grpc, Request::new(SubscribeEventsRequest::default())).await, Code::Unauthenticated);
        assert_eq!(code(&grpc, request("bad", Default::default())).await, Code::Unauthenticated);
        assert_eq!(code(&grpc, request("down", Default::default())).await, Code::Unavailable);
        let full = SubscribeEventsRequest { view: View::Full as i32, ..Default::default() };
        assert_eq!(code(&grpc, request("media", full)).await, Code::PermissionDenied);
        let cursor = SubscribeEventsRequest { resume_after: Some("garbage".into()), ..Default::default() };
        assert_eq!(code(&grpc, request("media", cursor)).await, Code::InvalidArgument);
    }

    #[tokio::test]
    async fn streams_filtered_projection_and_resumes_from_cursor() {
        let (grpc, feed) = service();
        let seismic = || SubscribeEventsRequest {
            filter: Some(HazardFilter { hazard_type: HazardType::Seismic as i32, ..Default::default() }),
            ..Default::default()
        };
        let response = grpc.subscribe_events(request("media", seismic())).await.unwrap();
        assert_eq!(response.metadata().get(CORRELATION_HEADER).unwrap(), "corr-1");
        let mut stream = response.into_inner();
        feed.ingest(publication("v1", Source::Pvmbg, HazardType::Volcanic));
        feed.ingest(publication("s1", Source::Bmkg, HazardType::Seismic));
        feed.ingest(publication("s2", Source::Bmkg, HazardType::Seismic));

        let first = stream.next().await.unwrap().unwrap().event.unwrap();
        assert_eq!((first.event_id.as_str(), first.sequence), ("s1", 2));
        assert!(matches!(first.hazard.unwrap().data, Some(Data::Summary(_))));
        drop(stream);

        // Resuming after s1 replays s2; the cursor is bound to this client, view and filter.
        let resume = SubscribeEventsRequest { resume_after: Some(first.resume_cursor.clone()), ..seismic() };
        let mut stream = grpc.subscribe_events(request("media", resume)).await.unwrap().into_inner();
        assert_eq!(stream.next().await.unwrap().unwrap().event.unwrap().event_id, "s2");
        let other_client = SubscribeEventsRequest { resume_after: Some(first.resume_cursor.clone()), ..seismic() };
        assert_eq!(code(&grpc, request("field", other_client)).await, Code::InvalidArgument);
        let other_feed = SubscribeEventsRequest { resume_after: Some(first.resume_cursor), ..Default::default() };
        assert_eq!(code(&grpc, request("media", other_feed)).await, Code::InvalidArgument);
    }

    #[tokio::test(start_paused = true)]
    async fn ends_with_unauthenticated_at_token_expiry_while_idle() {
        let (grpc, _) = service();
        let mut stream = grpc.subscribe_events(request("field", Default::default())).await.unwrap().into_inner();
        let status = stream.next().await.unwrap().unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);
        assert!(stream.next().await.is_none());
    }

    /// Each delivered event gets a feed.deliver span in the event's trace, linked to the stream;
    /// filtered-out events get none.
    #[tokio::test]
    async fn delivery_joins_the_event_trace_and_links_the_stream() {
        let capture = crate::telemetry::testing::capture();
        let (grpc, feed) = service();
        let seismic = SubscribeEventsRequest {
            filter: Some(HazardFilter { hazard_type: HazardType::Seismic as i32, ..Default::default() }),
            ..Default::default()
        };
        let mut stream = grpc.subscribe_events(request("media", seismic)).await.unwrap().into_inner();

        let consume = tracing::info_span!("rabbitmq.consume");
        let trace_id = consume.context().span().span_context().trace_id();
        let mut matching = publication("s1", Source::Bmkg, HazardType::Seismic);
        matching.trace = crate::telemetry::carrier(&consume);
        let mut filtered = publication("v1", Source::Pvmbg, HazardType::Volcanic);
        filtered.trace = matching.trace.clone();
        feed.ingest(filtered);
        feed.ingest(matching);
        assert_eq!(stream.next().await.unwrap().unwrap().event.unwrap().event_id, "s1");
        drop(stream);
        drop(consume);
        tokio::task::yield_now().await;

        let spans = capture.finished_spans();
        let consume = spans.iter().find(|span| span.span_context.trace_id() == trace_id && span.name == "rabbitmq.consume").unwrap();
        let deliveries: Vec<_> =
            spans.iter().filter(|span| span.span_context.trace_id() == trace_id && span.name == "feed.deliver").collect();
        assert_eq!(deliveries.len(), 1, "one delivery span, none for the filtered event");
        let delivery = deliveries[0];
        assert_eq!(delivery.parent_span_id, consume.span_context.span_id());
        assert_eq!(delivery.links.len(), 1);
        let linked = delivery.links.iter().next().unwrap().span_context.span_id();
        let stream = spans.iter().find(|span| span.span_context.span_id() == linked).unwrap();
        assert_eq!(stream.name, "bnpb.feed.v1.EventFeedService/SubscribeEvents");
    }
}
