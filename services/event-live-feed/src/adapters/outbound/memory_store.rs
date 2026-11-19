use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use tokio::sync::broadcast;
use tokio::time::Instant;

use crate::core::models::{FeedEntry, Publication, SharedEntry};
use crate::core::ports::{Position, ReplayError, ReplayStore, Snapshot, StoreUnavailable, StreamMeta};

/// Live broadcast depth per subscriber; laggards refill from the replay buffer instead.
pub const LIVE_CAPACITY: usize = 256;
const MAX_REPLAY_RETAINED_BYTES: usize = 32 << 20;

/// Process-local replay buffer for a single replica. A restart starts a new random epoch and
/// cursor key, which invalidates all earlier cursors (OUT_OF_RANGE) instead of persisting
/// sequence state.
pub struct MemoryStore {
    meta: StreamMeta,
    retention: Duration,
    max_entries: usize,
    live: broadcast::Sender<u64>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    head: u64,
    // Monotonic store time next to each entry, for retention.
    entries: VecDeque<(Instant, SharedEntry, usize)>,
    retained_bytes: usize,
    // Event IDs of the entries above; deduplication spans the replay window.
    seen: HashSet<String>,
}

impl MemoryStore {
    pub fn new(retention: Duration, max_entries: usize) -> Self {
        Self::with_epoch(rand::random(), retention, max_entries)
    }

    pub fn with_epoch(epoch: u64, retention: Duration, max_entries: usize) -> Self {
        let (live, _) = broadcast::channel(LIVE_CAPACITY);
        let meta = StreamMeta { epoch, cursor_key: rand::random() };
        Self { meta, retention, max_entries: max_entries.max(1), live, state: Mutex::default() }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        while state.entries.front().is_some_and(|(stored_at, _, _)| now.duration_since(*stored_at) > self.retention) {
            state.evict_front();
        }
        state
    }

    fn check_epoch(&self, position: Position) -> Result<(), ReplayError> {
        if position.epoch == self.meta.epoch { Ok(()) } else { Err(ReplayError::Expired) }
    }
}

impl State {
    fn evict_front(&mut self) {
        if let Some((_, entry, bytes)) = self.entries.pop_front() {
            self.retained_bytes -= bytes;
            self.seen.remove(&entry.publication.event_id);
        }
    }

    /// Entries strictly after `after`, or expired when some of them were already evicted.
    fn after(&self, after: u64, summary: bool) -> Result<Vec<SharedEntry>, ReplayError> {
        let oldest = self.entries.front().map_or(self.head + 1, |(_, entry, _)| entry.sequence);
        if after > self.head || after + 1 < oldest {
            return Err(ReplayError::Expired);
        }
        let skip = (after + 1 - oldest) as usize;
        Ok(self
            .entries
            .iter()
            .skip(skip)
            .take(1)
            .map(|(_, entry, _)| {
                if summary {
                    Arc::new(FeedEntry { sequence: entry.sequence, stored_at: entry.stored_at, publication: entry.publication.summary() })
                } else {
                    entry.clone()
                }
            })
            .collect())
    }
}

impl ReplayStore for MemoryStore {
    fn meta(&self) -> StreamMeta {
        self.meta
    }

    async fn append(&self, publication: Publication) -> Result<Option<SharedEntry>, StoreUnavailable> {
        let mut state = self.lock();
        if state.seen.contains(&publication.event_id) {
            return Ok(None);
        }
        let bytes = publication.retained_bytes();
        // One intact boundary record is retained even when larger than the
        // target. Older windows expire explicitly; never truncate a wildfire.
        while !state.entries.is_empty()
            && (state.entries.len() >= self.max_entries || state.retained_bytes.saturating_add(bytes) > MAX_REPLAY_RETAINED_BYTES)
        {
            state.evict_front();
        }
        state.head += 1;
        let entry = Arc::new(FeedEntry { sequence: state.head, stored_at: SystemTime::now(), publication });
        state.seen.insert(entry.publication.event_id.clone());
        state.entries.push_back((Instant::now(), entry.clone(), bytes));
        state.retained_bytes += bytes;
        // Sent under the lock so subscribe() sees each entry either in its backlog or live.
        let _ = self.live.send(entry.sequence);
        Ok(Some(entry))
    }

    async fn subscribe(&self, after: Option<Position>, summary: bool) -> Result<Snapshot, ReplayError> {
        let state = self.lock();
        let backlog = match after {
            Some(after) => {
                self.check_epoch(after)?;
                state.after(after.sequence, summary)?
            }
            None => Vec::new(),
        };
        Ok(Snapshot { meta: self.meta, backlog, head: state.head, live: self.live.subscribe() })
    }

    async fn read_after(&self, after: Position, summary: bool) -> Result<Vec<SharedEntry>, ReplayError> {
        self.check_epoch(after)?;
        self.lock().after(after.sequence, summary)
    }
}

#[cfg(test)]
mod tests {
    use bnpb_common::bnpb::common::v1::HazardEvent;

    use super::*;
    use crate::core::feed::{FeedError, FeedService};

    fn publication(id: &str) -> Publication {
        Publication {
            event_id: id.into(),
            published_at: None,
            hazard: HazardEvent { hazard_id: format!("hazard-{id}"), ..Default::default() },
            trace: Vec::new(),
        }
    }

    fn store(max_entries: usize) -> Arc<MemoryStore> {
        Arc::new(MemoryStore::with_epoch(7, Duration::from_secs(600), max_entries))
    }

    fn at(sequence: u64) -> Position {
        Position { epoch: 7, sequence }
    }

    #[tokio::test]
    async fn byte_eviction_drops_payloads_held_by_old_summary_snapshots_and_notices() {
        use prost_types::{Struct, Value, value::Kind};
        let store = store(1000);
        let mut first = publication("a");
        first.hazard.source_ref_id = "private".into();
        first.hazard.attributes =
            Some(Struct { fields: [("body".into(), Value { kind: Some(Kind::StringValue("x".repeat(3 << 20))) })].into() });
        let entry = store.append(first.clone()).await.unwrap().unwrap();
        let weak = Arc::downgrade(&entry);
        let snapshot = store.subscribe(Some(at(0)), true).await.unwrap();
        assert_eq!(snapshot.backlog.len(), 1);
        assert!(snapshot.backlog[0].publication.hazard.attributes.is_none());
        assert!(snapshot.backlog[0].publication.hazard.source_ref_id.is_empty());
        drop(entry);
        for i in 0..16 {
            let mut next = first.clone();
            next.event_id = format!("next-{i}");
            store.append(next).await.unwrap();
        }
        assert!(weak.upgrade().is_none(), "old summary snapshots/live notices retained the evicted full record");
        assert!(store.lock().retained_bytes <= MAX_REPLAY_RETAINED_BYTES);
        assert_eq!(store.read_after(at(0), false).await, Err(ReplayError::Expired));
    }

    #[tokio::test]
    async fn deduplicates_and_assigns_increasing_sequences() {
        let store = store(10);
        assert_eq!(store.append(publication("a")).await.unwrap().unwrap().sequence, 1);
        assert!(store.append(publication("a")).await.unwrap().is_none());
        assert_eq!(store.append(publication("b")).await.unwrap().unwrap().sequence, 2);
        let ids: Vec<_> = store.read_after(at(0), false).await.unwrap().iter().map(|e| e.publication.event_id.clone()).collect();
        assert_eq!(ids, ["a"]);
        assert_eq!(store.read_after(at(1), false).await.unwrap()[0].publication.event_id, "b");
    }

    #[tokio::test]
    async fn cursor_bounds() {
        let store = store(2);
        for id in ["a", "b", "c"] {
            store.append(publication(id)).await.unwrap();
        }
        // "a" (1) was evicted by the size cap: resuming after 0 would skip it.
        assert_eq!(store.read_after(at(0), false).await, Err(ReplayError::Expired));
        assert_eq!(store.read_after(at(1), false).await.unwrap().len(), 1);
        assert_eq!(store.read_after(at(3), false).await.unwrap().len(), 0);
        assert_eq!(store.read_after(at(4), false).await, Err(ReplayError::Expired));
    }

    #[tokio::test(start_paused = true)]
    async fn evicts_by_retention_and_forgets_event_ids() {
        let store = Arc::new(MemoryStore::with_epoch(7, Duration::from_secs(10), 100));
        store.append(publication("a")).await.unwrap();
        tokio::time::advance(Duration::from_secs(11)).await;
        store.append(publication("b")).await.unwrap();
        assert_eq!(store.read_after(at(0), false).await, Err(ReplayError::Expired));
        assert_eq!(store.read_after(at(1), false).await.unwrap().len(), 1);
        tokio::time::advance(Duration::from_secs(11)).await;
        // Outside the window a redelivery is a new entry; clients dedupe by event_id.
        assert_eq!(store.append(publication("a")).await.unwrap().unwrap().sequence, 3);
    }

    #[tokio::test]
    async fn replay_joins_live_without_gaps() {
        let store = store(1000);
        let feed = FeedService::new(store.clone());
        for i in 0..5 {
            feed.ingest(publication(&format!("old-{i}"))).await.unwrap();
        }
        let mut subscription = feed.subscribe(Some(at(2)), false).await.unwrap();
        let producer = {
            let feed = FeedService::new(store.clone());
            tokio::spawn(async move {
                for i in 0..50 {
                    feed.ingest(publication(&format!("new-{i}"))).await.unwrap();
                    tokio::task::yield_now().await;
                }
            })
        };
        for expected in 3..=55 {
            assert_eq!(subscription.next().await.unwrap().sequence, expected);
        }
        producer.await.unwrap();
    }

    #[tokio::test]
    async fn live_only_subscription_starts_at_head() {
        let store = store(10);
        let feed = FeedService::new(store);
        feed.ingest(publication("a")).await.unwrap();
        let mut subscription = feed.subscribe(None, false).await.unwrap();
        feed.ingest(publication("b")).await.unwrap();
        assert_eq!(subscription.next().await.unwrap().publication.event_id, "b");
    }

    #[tokio::test]
    async fn lagging_subscriber_refills_from_replay() {
        let store = store(10_000);
        let feed = FeedService::new(store);
        let mut subscription = feed.subscribe(None, false).await.unwrap();
        for i in 0..(LIVE_CAPACITY * 2) {
            feed.ingest(publication(&i.to_string())).await.unwrap();
        }
        for expected in 1..=(LIVE_CAPACITY * 2) as u64 {
            assert_eq!(subscription.next().await.unwrap().sequence, expected);
        }
    }

    #[tokio::test]
    async fn lagging_past_the_buffer_is_expired() {
        let store = store(LIVE_CAPACITY);
        let feed = FeedService::new(store);
        let mut subscription = feed.subscribe(None, false).await.unwrap();
        for i in 0..(LIVE_CAPACITY * 3) {
            feed.ingest(publication(&i.to_string())).await.unwrap();
        }
        assert_eq!(subscription.next().await.unwrap_err(), FeedError::Expired);
    }

    #[tokio::test]
    async fn positions_of_another_epoch_are_expired() {
        let store = store(10);
        store.append(publication("a")).await.unwrap();
        let other = Position { epoch: 8, sequence: 0 };
        assert_eq!(store.read_after(other, false).await, Err(ReplayError::Expired));
        assert!(matches!(store.subscribe(Some(other), false).await, Err(ReplayError::Expired)));
    }
}
