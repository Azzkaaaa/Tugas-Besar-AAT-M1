use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bnpb_common::bnpb::common::v1::{HazardEvent, HazardType, Severity, Source};
use bnpb_feed::bnpb::feed::v1::event_feed_service_server::EventFeedServiceServer;
use prost_types::{ListValue, Struct, Timestamp, Value, value::Kind};
use tonic::Code;

use super::tests::{NoRenewer, Verifier, request, service};
use super::*;
use crate::adapters::inbound::stream_admission::StreamAdmissionLayer;
use crate::adapters::outbound::memory_store::MemoryStore;
use crate::core::models::Publication;

#[tokio::test]
async fn full_admission_preserves_summary_and_checks_scope_before_capacity() {
    let (grpc, _) = service();
    let full = || SubscribeEventsRequest { view: View::Full as i32, ..Default::default() };
    let first = grpc.subscribe_events(request("field", full())).await.unwrap();
    assert_eq!(grpc.subscribe_events(request("field", full())).await.err().unwrap().code(), Code::ResourceExhausted);
    assert_eq!(grpc.subscribe_events(request("media", full())).await.err().unwrap().code(), Code::PermissionDenied);
    let summary = grpc.subscribe_events(request("media", SubscribeEventsRequest::default())).await.unwrap();
    drop(summary);
    drop(first);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            tokio::task::yield_now().await;
            match grpc.subscribe_events(request("field", full())).await {
                Ok(response) => {
                    drop(response);
                    break;
                }
                Err(status) if status.code() == Code::ResourceExhausted => {}
                Err(status) => panic!("unexpected recovery status: {status}"),
            }
        }
    })
    .await
    .unwrap();
}

/// Test-only actual Tonic feed for capped gateway/HTTP2 transport fixtures.
/// Fixed test scopes are used here; deployed OIDC/JWT is separate acceptance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "external streaming fixture; FEED_TEST_SERVE=1, stop with /tmp/m2-stream-stop"]
async fn serve_stream_memory_fixture() {
    assert_eq!(std::env::var("FEED_TEST_SERVE").as_deref(), Ok("1"));
    let values = (0..200000)
        .map(|i| Value { kind: Some(Kind::StringValue(format!("N20|2026-11-19|0600|{:.8}|106.12345678", -6.1 + i as f64 * 0.00000001))) })
        .collect();
    let fixture = HazardEvent {
        hazard_id: "fire".into(),
        source_ref_id: "ref".into(),
        source: Source::Satelite as i32,
        hazard_type: HazardType::Wildfire as i32,
        severity: Severity::Awas as i32,
        area_name: "memory fixture".into(),
        latitude: Some(-6.1),
        longitude: Some(106.1),
        attributes: Some(Struct {
            fields: BTreeMap::from([
                ("detection_keys".into(), Value { kind: Some(Kind::ListValue(ListValue { values })) }),
                ("detection_count".into(), Value { kind: Some(Kind::NumberValue(200000.0)) }),
            ]),
        }),
        ..Default::default()
    };
    let feed = Arc::new(FeedService::new(Arc::new(MemoryStore::new(Duration::from_secs(600), 10000))));
    let grpc = FeedGrpc {
        feed: feed.clone(),
        verifier: Arc::new(Verifier),
        renewer: None::<Arc<NoRenewer>>,
        policy: SessionPolicy { renew_before: Duration::from_secs(15), max_stream: Duration::from_secs(120) },
        full_streams: FullStreams::default(),
    };
    let publisher = tokio::spawn(async move {
        let mut sequence = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            sequence += 1;
            let mut hazard = fixture.clone();
            let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let now = Timestamp { seconds: elapsed.as_secs() as i64, nanos: elapsed.subsec_nanos() as i32 };
            hazard.occurred_at = Some(now);
            hazard.ingested_at = Some(now);
            feed.ingest(Publication { event_id: format!("event-{sequence}"), published_at: Some(now), hazard, trace: Vec::new() })
                .await
                .unwrap();
        }
    });
    let address = "0.0.0.0:19093".parse().unwrap();
    let stop = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        while !std::path::Path::new("/tmp/m2-stream-stop").exists() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    println!("STREAM_FIXTURE_READY address={address} keys=200000");
    tonic::transport::Server::builder()
        .layer(StreamAdmissionLayer)
        .add_service(EventFeedServiceServer::new(grpc))
        .serve_with_shutdown(address, stop)
        .await
        .unwrap();
    publisher.abort();
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let rss = status.lines().find(|line| line.starts_with("VmHWM:")).unwrap_or("");
    let peak = std::fs::read_to_string("/sys/fs/cgroup/memory.peak").unwrap_or_default();
    println!("STREAM_FIXTURE_DONE peak_rss={rss} container_peak_bytes={}", peak.trim());
}
