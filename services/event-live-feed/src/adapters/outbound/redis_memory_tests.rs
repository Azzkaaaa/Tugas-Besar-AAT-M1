use std::collections::BTreeMap;

use bnpb_common::bnpb::common::v1::{HazardType, Source};
use prost_types::{ListValue, Struct, Value, value::Kind};

use super::*;
use crate::core::feed::{FeedError, FeedService};

fn publication(count: usize) -> Publication {
    let values = (0..count)
        .map(|i| Value { kind: Some(Kind::StringValue(format!("N20|2026-11-19|0600|{:.8}|106.12345678", -6.1 + i as f64 * 0.00000001))) })
        .collect();
    Publication {
        event_id: "fire-0".into(),
        published_at: Some(Timestamp { seconds: 1_795_000_000, nanos: 0 }),
        trace: vec![("traceparent".into(), "test-trace".into())],
        hazard: HazardEvent {
            hazard_id: "fire".into(),
            source_ref_id: "private-ref".into(),
            source: Source::Satelite as i32,
            hazard_type: HazardType::Wildfire as i32,
            latitude: Some(-6.1),
            longitude: Some(106.1),
            attributes: Some(Struct {
                fields: BTreeMap::from([("detection_keys".into(), Value { kind: Some(Kind::ListValue(ListValue { values })) })]),
            }),
            ..Default::default()
        },
    }
}

fn summary(entry: &FeedEntry) {
    assert!(entry.publication.hazard.attributes.is_none());
    assert!(entry.publication.hazard.source_ref_id.is_empty());
    assert!(entry.publication.hazard.latitude.is_none());
    assert!(entry.publication.hazard.longitude.is_none());
    assert_eq!(entry.publication.hazard.hazard_id, format!("fire-{}", entry.sequence));
}

async fn output_bytes(commands: &mut ConnectionManager) -> u64 {
    let info: String = redis::cmd("INFO").arg("stats").query_async(commands).await.unwrap();
    info.lines().find_map(|line| line.strip_prefix("total_net_output_bytes:")).unwrap().trim().parse().unwrap()
}

#[tokio::test]
#[ignore = "requires an empty disposable Redis via FEED_TEST_REDIS_URL; stress with FEED_TEST_LARGE_KEYS=200000"]
async fn bounded_large_replay_and_legacy_repair() {
    let started = std::time::Instant::now();
    let url = std::env::var("FEED_TEST_REDIS_URL").expect("disposable Redis URL required");
    let count = std::env::var("FEED_TEST_LARGE_KEYS").unwrap_or_else(|_| "2000".into()).parse::<usize>().unwrap();
    assert!((1000..=200000).contains(&count));
    let client = Client::open(url.clone()).unwrap();
    let mut commands = ConnectionManager::new(client).await.unwrap();
    let db_size: usize = redis::cmd("DBSIZE").query_async(&mut commands).await.unwrap();
    assert_eq!(db_size, 0, "refusing to modify a Redis database that is not empty");
    let store = RedisStore::connect(&url, Duration::from_secs(600), 50000).await.unwrap();
    let epoch = store.meta().epoch;
    let position = move |sequence| Position { epoch, sequence };
    // A distinct replica must follow publications consumed elsewhere using
    // metadata notices and the same shared epoch, without payload fanout.
    let reader_store = RedisStore::connect(&url, Duration::from_secs(600), 50000).await.unwrap();
    assert_eq!(reader_store.meta(), store.meta());
    let feed = FeedService::new(reader_store);
    let mut live = feed.subscribe(None, true).await.unwrap();
    let fixture = publication(count);
    let mut wire_bytes = fixture.hazard.encoded_len();
    let first_waiter = tokio::spawn(async move {
        let first = live.next().await.unwrap();
        summary(&first);
        assert_eq!(first.sequence, 1);
        live
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut retained = 0;
    let mut max_usage = 0;
    for i in 1..=32 {
        let mut next = fixture.clone();
        next.event_id = format!("fire-{i}");
        next.hazard.hazard_id = format!("fire-{i}");
        next.hazard.source_ref_id = format!("ref-{i}");
        wire_bytes = next.hazard.encoded_len();
        let entry = store.append(next).await.expect("large replay append must remain writable").unwrap();
        assert_eq!(entry.sequence, i);
        let usage: usize =
            redis::cmd("MEMORY").arg("USAGE").arg(entries_key(epoch)).arg("SAMPLES").arg(0).query_async(&mut commands).await.unwrap();
        max_usage = max_usage.max(usage);
        assert!(usage <= MAX_REPLAY_BYTES, "replay bytes exceeded target: {usage}");
        retained = commands.xlen::<_, usize>(entries_key(epoch)).await.unwrap();
        let keys: usize = redis::cmd("DBSIZE").query_async(&mut commands).await.unwrap();
        assert!(keys <= 3 + 2 * retained, "trim leaked summary/dedup keys: {keys} keys for {retained} retained events");
    }
    let mut live = tokio::time::timeout(Duration::from_secs(2), first_waiter).await.unwrap().unwrap();
    if retained < 31 {
        assert_eq!(live.next().await, Err(FeedError::Expired));
        assert!(matches!(store.subscribe(Some(position(0)), true).await, Err(ReplayError::Expired)));
    } else {
        for sequence in 2..=32 {
            assert_eq!(live.next().await.unwrap().sequence, sequence);
        }
    }
    let oldest_after = 32 - retained as u64;
    let snapshot = store.subscribe(Some(position(oldest_after)), true).await.unwrap();
    assert_eq!(snapshot.backlog.len(), 1);
    summary(&snapshot.backlog[0]);

    // Cached summaries bypass a held cold-body slot; their network replies are
    // small even though the retained fires each have a multi-megabyte payload.
    let before = output_bytes(&mut commands).await;
    let held = store.summary_fallback.lock().await;
    let mut readers = Vec::new();
    for _ in 0..24 {
        let store = store.clone();
        readers.push(tokio::spawn(async move {
            let entries = store.read_after(position(oldest_after), true).await.unwrap();
            assert_eq!(entries.len(), 1);
            summary(&entries[0]);
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), futures_util::future::join_all(readers))
        .await
        .unwrap()
        .into_iter()
        .for_each(|r| r.unwrap());
    drop(held);
    let warm_bytes = output_bytes(&mut commands).await - before;
    assert!(warm_bytes < 512 * 1024, "summary transferred full bodies: {warm_bytes}");

    for after in oldest_after..32 {
        let full = store.read_after(position(after), false).await.unwrap();
        assert_eq!(full[0].publication.hazard.hazard_id, format!("fire-{}", after + 1));
        let got = &full[0].publication.hazard.attributes.as_ref().unwrap().fields["detection_keys"];
        assert!(got == &fixture.hazard.attributes.as_ref().unwrap().fields["detection_keys"], "full replay payload lost detection keys");
    }

    let _: () = commands.del(summary_key(epoch, 32)).await.unwrap();
    let held = store.summary_fallback.lock().await;
    let mut waiters = Vec::new();
    for _ in 0..12 {
        let store = store.clone();
        waiters.push(tokio::spawn(async move {
            assert!(tokio::time::timeout(Duration::from_millis(30), store.read_after(position(31), true)).await.is_err());
        }));
    }
    futures_util::future::join_all(waiters).await.into_iter().for_each(|r| r.unwrap());
    drop(held);
    let before = output_bytes(&mut commands).await;
    let mut readers = Vec::new();
    for _ in 0..24 {
        let store = store.clone();
        readers.push(tokio::spawn(async move {
            summary(&store.read_after(position(31), true).await.unwrap()[0]);
        }));
    }
    futures_util::future::join_all(readers).await.into_iter().for_each(|r| r.unwrap());
    let repaired_bytes = output_bytes(&mut commands).await - before;
    assert!(repaired_bytes < wire_bytes as u64 * 2 + 512 * 1024, "cold summary bodies were not serialized/cached: {repaired_bytes}");
    let _: () = commands.hset(summary_key(epoch, 32), "hazard", vec![255u8]).await.unwrap();
    summary(&store.read_after(position(31), true).await.unwrap()[0]);

    // Dropping the subscriber must not return body admission while the Redis
    // response is outstanding. Pause the server after the metadata hint, then
    // abort the caller while its owned repair task is opening/reading a socket.
    let _: () = commands.del(summary_key(epoch, 32)).await.unwrap();
    let held = store.summary_fallback.lock().await;
    let caller_store = store.clone();
    let caller = tokio::spawn(async move { caller_store.read_after(position(31), true).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _: () = redis::cmd("CLIENT").arg("PAUSE").arg(400).arg("ALL").query_async(&mut commands).await.unwrap();
    drop(held);
    tokio::time::timeout(Duration::from_millis(200), async {
        while Arc::strong_count(&store.summary_fallback) < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("repair did not take ownership of its admission credit");
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(store.summary_fallback.try_lock().is_err(), "caller cancellation released a pending body read");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(store.summary_fallback.try_lock().is_err(), "body credit released before paused Redis completed");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if store.summary_fallback.try_lock().is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("completed cancelled repair leaked admission");
    summary(&store.read_after(position(31), true).await.unwrap()[0]);

    // Losing/resetting the stream identity expires existing cursors, even if
    // old header keys survived. No old position can enter the new epoch.
    let next_meta = new_meta();
    let _: () = commands.hset(META_KEY, "epoch", next_meta.epoch.to_string()).await.unwrap();
    assert_eq!(store.read_after(position(31), true).await, Err(ReplayError::Expired));

    // An XADD rejection after SET/INCR must remove the marker and restore the
    // head, so retrying that publication neither loses it nor creates a gap.
    let bad_entries = entries_key(next_meta.epoch);
    let _: String =
        redis::cmd("XADD").arg(&bad_entries).arg("100-0").arg("event_id").arg("fault").query_async(&mut commands).await.unwrap();
    let mut retry = publication(1000);
    retry.event_id = "retry-after-xadd".into();
    assert!(store.append(retry.clone()).await.is_err());
    let head_key = format!("feed:{}:head", next_meta.epoch);
    let seen_key = format!("feed:{}:seen:{}", next_meta.epoch, retry.event_id);
    assert_eq!(commands.get::<_, u64>(&head_key).await.unwrap(), 0);
    assert!(!commands.exists::<_, bool>(&seen_key).await.unwrap());
    let _: () = commands.del(&bad_entries).await.unwrap();
    assert_eq!(store.append(retry.clone()).await.unwrap().unwrap().sequence, 1);
    assert!(store.append(retry).await.unwrap().is_none());
    let _: () = commands.set(&head_key, "not-an-integer").await.unwrap();
    let mut retry = publication(1000);
    retry.event_id = "retry-after-incr".into();
    assert!(store.append(retry.clone()).await.is_err());
    assert!(!commands.exists::<_, bool>(format!("feed:{}:seen:{}", next_meta.epoch, retry.event_id)).await.unwrap());
    let _: () = commands.set(&head_key, 1).await.unwrap();
    assert_eq!(store.append(retry).await.unwrap().unwrap().sequence, 2);
    let rss = std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .unwrap_or("")
        .to_string();
    let peak = std::fs::read_to_string("/sys/fs/cgroup/memory.peak").unwrap_or_default();
    println!(
        "feed_replay records=32 keys_per_record={count} wire_bytes={wire_bytes} retained={retained} max_stream_bytes={max_usage} warm_summary_bytes={warm_bytes} cold_repair_bytes={repaired_bytes} peak_rss={rss} container_peak_bytes={} elapsed={:?}",
        peak.trim(),
        started.elapsed()
    );
}
