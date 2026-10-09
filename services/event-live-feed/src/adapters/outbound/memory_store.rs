use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::time::Instant;

use crate::core::models::{FeedEntry, Publication, SharedEntry};
use crate::core::ports::{ReplayExpired, ReplayStore, Snapshot};

/// Live broadcast depth per subscriber; laggards refill from the replay buffer instead.
const LIVE_CAPACITY: usize = 256;

/// Process-local replay buffer. A restart starts a new random epoch, which invalidates all
/// earlier cursors (OUT_OF_RANGE) instead of persisting sequence state.
pub struct MemoryStore {
    epoch: u64,
    retention: Duration,
    max_entries: usize,
    live: broadcast::Sender<SharedEntry>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    head: u64,
    entries: VecDeque<SharedEntry>,
    // Event IDs of the entries above; deduplication spans the replay window.
    seen: HashSet<String>,
}

impl MemoryStore {
    pub fn new(retention: Duration, max_entries: usize) -> Self {
        Self::with_epoch(rand::random(), retention, max_entries)
    }

    pub fn with_epoch(epoch: u64, retention: Duration, max_entries: usize) -> Self {
        let (live, _) = broadcast::channel(LIVE_CAPACITY);
        Self { epoch, retention, max_entries: max_entries.max(1), live, state: Mutex::default() }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        while state.entries.front().is_some_and(|entry| now.duration_since(entry.stored_at) > self.retention) {
            state.evict_front();
        }
        state
    }
}

impl State {
    fn evict_front(&mut self) {
        if let Some(entry) = self.entries.pop_front() {
            self.seen.remove(&entry.publication.event_id);
        }
    }

    /// Entries strictly after `after`, or expired when some of them were already evicted.
    fn after(&self, after: u64) -> Result<Vec<SharedEntry>, ReplayExpired> {
        let oldest = self.entries.front().map_or(self.head + 1, |entry| entry.sequence);
        if after > self.head || after + 1 < oldest {
            return Err(ReplayExpired);
        }
        let skip = (after + 1 - oldest) as usize;
        Ok(self.entries.iter().skip(skip).cloned().collect())
    }
}

impl ReplayStore for MemoryStore {
    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn append(&self, publication: Publication) -> Option<SharedEntry> {
        let mut state = self.lock();
        if state.seen.contains(&publication.event_id) {
            return None;
        }
        if state.entries.len() >= self.max_entries {
            state.evict_front();
        }
        state.head += 1;
        let entry = Arc::new(FeedEntry { sequence: state.head, stored_at: Instant::now(), publication });
        state.seen.insert(entry.publication.event_id.clone());
        state.entries.push_back(entry.clone());
        // Sent under the lock so subscribe() sees each entry either in its backlog or live.
        let _ = self.live.send(entry.clone());
        Some(entry)
    }

    fn subscribe(&self, after: Option<u64>) -> Result<Snapshot, ReplayExpired> {
        let state = self.lock();
        let backlog = match after {
            Some(after) => state.after(after)?,
            None => Vec::new(),
        };
        Ok(Snapshot { backlog, head: state.head, live: self.live.subscribe() })
    }

    fn read_after(&self, after: u64) -> Result<Vec<SharedEntry>, ReplayExpired> {
        self.lock().after(after)
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

    #[tokio::test]
    async fn deduplicates_and_assigns_increasing_sequences() {
        let store = store(10);
        assert_eq!(store.append(publication("a")).unwrap().sequence, 1);
        assert!(store.append(publication("a")).is_none());
        assert_eq!(store.append(publication("b")).unwrap().sequence, 2);
        let ids: Vec<_> = store.read_after(0).unwrap().iter().map(|e| e.publication.event_id.clone()).collect();
        assert_eq!(ids, ["a", "b"]);
    }

    #[tokio::test]
    async fn cursor_bounds() {
        let store = store(2);
        for id in ["a", "b", "c"] {
            store.append(publication(id));
        }
        // "a" (1) was evicted by the size cap: resuming after 0 would skip it.
        assert_eq!(store.read_after(0), Err(ReplayExpired));
        assert_eq!(store.read_after(1).unwrap().len(), 2);
        assert_eq!(store.read_after(3).unwrap().len(), 0);
        assert_eq!(store.read_after(4), Err(ReplayExpired));
    }

    #[tokio::test(start_paused = true)]
    async fn evicts_by_retention_and_forgets_event_ids() {
        let store = Arc::new(MemoryStore::with_epoch(7, Duration::from_secs(10), 100));
        store.append(publication("a"));
        tokio::time::advance(Duration::from_secs(11)).await;
        store.append(publication("b"));
        assert_eq!(store.read_after(0), Err(ReplayExpired));
        assert_eq!(store.read_after(1).unwrap().len(), 1);
        tokio::time::advance(Duration::from_secs(11)).await;
        // Outside the window a redelivery is a new entry; clients dedupe by event_id.
        assert_eq!(store.append(publication("a")).unwrap().sequence, 3);
    }

    #[tokio::test]
    async fn replay_joins_live_without_gaps() {
        let store = store(1000);
        let feed = FeedService::new(store.clone());
        for i in 0..5 {
            feed.ingest(publication(&format!("old-{i}")));
        }
        let mut subscription = feed.subscribe(Some(2)).unwrap();
        let producer = {
            let feed = FeedService::new(store.clone());
            tokio::spawn(async move {
                for i in 0..50 {
                    feed.ingest(publication(&format!("new-{i}")));
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
        feed.ingest(publication("a"));
        let mut subscription = feed.subscribe(None).unwrap();
        feed.ingest(publication("b"));
        assert_eq!(subscription.next().await.unwrap().publication.event_id, "b");
    }

    #[tokio::test]
    async fn lagging_subscriber_refills_from_replay() {
        let store = store(10_000);
        let feed = FeedService::new(store);
        let mut subscription = feed.subscribe(None).unwrap();
        for i in 0..(LIVE_CAPACITY * 2) {
            feed.ingest(publication(&i.to_string()));
        }
        for expected in 1..=(LIVE_CAPACITY * 2) as u64 {
            assert_eq!(subscription.next().await.unwrap().sequence, expected);
        }
    }

    #[tokio::test]
    async fn lagging_past_the_buffer_is_expired() {
        let store = store(LIVE_CAPACITY);
        let feed = FeedService::new(store);
        let mut subscription = feed.subscribe(None).unwrap();
        for i in 0..(LIVE_CAPACITY * 3) {
            feed.ingest(publication(&i.to_string()));
        }
        assert_eq!(subscription.next().await.unwrap_err(), FeedError::Expired);
    }
}
