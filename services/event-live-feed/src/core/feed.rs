use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;

use crate::core::models::{Publication, SharedEntry};
use crate::core::ports::{Position, ReplayError, ReplayStore, StoreUnavailable, StreamMeta};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedError {
    /// Events the subscriber has not seen were evicted, or the stream was reset; it must take
    /// a new snapshot.
    Expired,
    /// The feed is shutting down.
    Closed,
    /// The replay store could not be reached.
    Unavailable,
}

impl From<ReplayError> for FeedError {
    fn from(error: ReplayError) -> Self {
        match error {
            ReplayError::Expired => FeedError::Expired,
            ReplayError::Unavailable => FeedError::Unavailable,
        }
    }
}

pub struct FeedService<S> {
    store: Arc<S>,
}

impl<S: ReplayStore> FeedService<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    pub fn meta(&self) -> StreamMeta {
        self.store.meta()
    }

    /// Returns `true` when the publication was new, `false` for a broker redelivery.
    pub async fn ingest(&self, publication: Publication) -> Result<bool, StoreUnavailable> {
        Ok(self.store.append(publication).await?.is_some())
    }

    /// Starts after `after`, or at the live head when `None`.
    pub async fn subscribe(&self, after: Option<Position>, summary: bool) -> Result<FeedSubscription<S>, FeedError> {
        let snapshot = self.store.subscribe(after, summary).await?;
        if after.is_some_and(|after| after.epoch != snapshot.meta.epoch) {
            return Err(FeedError::Expired);
        }
        Ok(FeedSubscription {
            store: self.store.clone(),
            meta: snapshot.meta,
            backlog: snapshot.backlog.into(),
            last: after.map_or(snapshot.head, |after| after.sequence),
            live: snapshot.live,
            summary,
        })
    }
}

/// Yields every entry after the starting position exactly once, in sequence order.
pub struct FeedSubscription<S> {
    store: Arc<S>,
    meta: StreamMeta,
    backlog: VecDeque<SharedEntry>,
    last: u64,
    live: Receiver<u64>,
    summary: bool,
}

impl<S: ReplayStore> FeedSubscription<S> {
    /// The stream this subscription reads; its entries' cursors belong to it.
    pub fn meta(&self) -> StreamMeta {
        self.meta
    }

    pub async fn next(&mut self) -> Result<SharedEntry, FeedError> {
        loop {
            let Some(entry) = self.backlog.pop_front() else {
                self.refill().await?;
                if !self.backlog.is_empty() {
                    continue;
                }
                // Notices can coalesce or lag: always refill from the last
                // committed subscriber position, never skip to a live head.
                loop {
                    match self.live.recv().await {
                        // Replayed/coalesced notices must not amplify storage
                        // traffic after the subscriber has already caught up.
                        Ok(head) if head <= self.last => continue,
                        Ok(_) | Err(RecvError::Lagged(_)) => break,
                        Err(RecvError::Closed) if self.store.meta().epoch != self.meta.epoch => return Err(FeedError::Expired),
                        Err(RecvError::Closed) => return Err(FeedError::Closed),
                    }
                }
                continue;
            };
            if entry.sequence <= self.last {
                // Already delivered from the backlog before the live copy arrived.
                continue;
            }
            if entry.sequence != self.last + 1 {
                return Err(FeedError::Expired);
            }
            self.last = entry.sequence;
            return Ok(entry);
        }
    }

    async fn refill(&mut self) -> Result<(), FeedError> {
        let entries = self.store.read_after(Position { epoch: self.meta.epoch, sequence: self.last }, self.summary).await?;
        if entries.first().is_some_and(|first| first.sequence != self.last + 1) {
            return Err(FeedError::Expired);
        }
        self.backlog = entries.into();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::models::FeedEntry;
    use crate::core::ports::Snapshot;
    use bnpb_common::bnpb::common::v1::HazardEvent;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime};
    use tokio::sync::broadcast;

    struct CountReads {
        live: broadcast::Sender<u64>,
        reads: AtomicUsize,
        published: AtomicBool,
    }

    fn entry() -> SharedEntry {
        Arc::new(FeedEntry {
            sequence: 2,
            stored_at: SystemTime::now(),
            publication: Publication { event_id: "new".into(), published_at: None, hazard: HazardEvent::default(), trace: Vec::new() },
        })
    }

    impl ReplayStore for CountReads {
        fn meta(&self) -> StreamMeta {
            StreamMeta { epoch: 7, cursor_key: [0; 32] }
        }
        async fn append(&self, _: Publication) -> Result<Option<SharedEntry>, StoreUnavailable> {
            self.published.store(true, Ordering::SeqCst);
            let _ = self.live.send(2);
            Ok(Some(entry()))
        }
        async fn subscribe(&self, _: Option<Position>, _: bool) -> Result<Snapshot, ReplayError> {
            Ok(Snapshot { meta: self.meta(), backlog: Vec::new(), head: 1, live: self.live.subscribe() })
        }
        async fn read_after(&self, after: Position, _: bool) -> Result<Vec<SharedEntry>, ReplayError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(if after.sequence == 1 && self.published.load(Ordering::SeqCst) { vec![entry()] } else { Vec::new() })
        }
    }

    #[tokio::test]
    async fn stale_notices_do_not_repeat_storage_reads() {
        let (live, _) = broadcast::channel(256);
        let store = Arc::new(CountReads { live, reads: AtomicUsize::new(0), published: AtomicBool::new(false) });
        let feed = FeedService::new(store.clone());
        let mut subscription = feed.subscribe(None, true).await.unwrap();
        for _ in 0..128 {
            store.live.send(1).unwrap();
        }
        let next = tokio::spawn(async move { subscription.next().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.reads.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(store.reads.load(Ordering::SeqCst), 1, "stale notices amplified replay reads");
        feed.ingest(entry().publication.clone()).await.unwrap();
        assert_eq!(tokio::time::timeout(Duration::from_secs(1), next).await.unwrap().unwrap().unwrap().sequence, 2);
        assert_eq!(store.reads.load(Ordering::SeqCst), 2);
    }
}
