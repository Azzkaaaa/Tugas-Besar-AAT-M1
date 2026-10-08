use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;

use crate::core::models::{Publication, SharedEntry};
use crate::core::ports::{ReplayExpired, ReplayStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedError {
    /// Events the subscriber has not seen were evicted; it must take a new snapshot.
    Expired,
    /// The feed is shutting down.
    Closed,
}

impl From<ReplayExpired> for FeedError {
    fn from(_: ReplayExpired) -> Self {
        FeedError::Expired
    }
}

pub struct FeedService<S> {
    store: Arc<S>,
}

impl<S: ReplayStore> FeedService<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    pub fn epoch(&self) -> u64 {
        self.store.epoch()
    }

    /// Returns `true` when the publication was new, `false` for a broker redelivery.
    pub fn ingest(&self, publication: Publication) -> bool {
        self.store.append(publication).is_some()
    }

    /// Starts after `after`, or at the live head when `None`.
    pub fn subscribe(&self, after: Option<u64>) -> Result<FeedSubscription<S>, FeedError> {
        let snapshot = self.store.subscribe(after)?;
        Ok(FeedSubscription {
            store: self.store.clone(),
            backlog: snapshot.backlog.into(),
            last: after.unwrap_or(snapshot.head),
            live: snapshot.live,
        })
    }
}

/// Yields every entry after the starting position exactly once, in sequence order.
pub struct FeedSubscription<S> {
    store: Arc<S>,
    backlog: VecDeque<SharedEntry>,
    last: u64,
    live: Receiver<SharedEntry>,
}

impl<S: ReplayStore> FeedSubscription<S> {
    pub async fn next(&mut self) -> Result<SharedEntry, FeedError> {
        loop {
            let entry = match self.backlog.pop_front() {
                Some(entry) => entry,
                None => match self.live.recv().await {
                    Ok(entry) => entry,
                    // A slow subscriber fell behind the broadcast buffer: refill from replay.
                    Err(RecvError::Lagged(_)) => {
                        self.refill()?;
                        continue;
                    }
                    Err(RecvError::Closed) => return Err(FeedError::Closed),
                },
            };
            if entry.sequence <= self.last {
                // Already delivered from the backlog before the live copy arrived.
                continue;
            }
            if entry.sequence != self.last + 1 {
                self.backlog.push_front(entry);
                self.refill()?;
                continue;
            }
            self.last = entry.sequence;
            return Ok(entry);
        }
    }

    fn refill(&mut self) -> Result<(), FeedError> {
        let entries = self.store.read_after(self.last)?;
        if entries.first().is_some_and(|first| first.sequence != self.last + 1) {
            return Err(FeedError::Expired);
        }
        self.backlog = entries.into();
        Ok(())
    }
}
