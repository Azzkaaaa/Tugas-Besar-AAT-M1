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
    pub async fn subscribe(&self, after: Option<Position>) -> Result<FeedSubscription<S>, FeedError> {
        let snapshot = self.store.subscribe(after).await?;
        if after.is_some_and(|after| after.epoch != snapshot.meta.epoch) {
            return Err(FeedError::Expired);
        }
        Ok(FeedSubscription {
            store: self.store.clone(),
            meta: snapshot.meta,
            backlog: snapshot.backlog.into(),
            last: after.map_or(snapshot.head, |after| after.sequence),
            live: snapshot.live,
        })
    }
}

/// Yields every entry after the starting position exactly once, in sequence order.
pub struct FeedSubscription<S> {
    store: Arc<S>,
    meta: StreamMeta,
    backlog: VecDeque<SharedEntry>,
    last: u64,
    live: Receiver<SharedEntry>,
}

impl<S: ReplayStore> FeedSubscription<S> {
    /// The stream this subscription reads; its entries' cursors belong to it.
    pub fn meta(&self) -> StreamMeta {
        self.meta
    }

    pub async fn next(&mut self) -> Result<SharedEntry, FeedError> {
        loop {
            let entry = match self.backlog.pop_front() {
                Some(entry) => entry,
                None => match self.live.recv().await {
                    Ok(entry) => entry,
                    // A slow subscriber fell behind the broadcast buffer: refill from replay.
                    Err(RecvError::Lagged(_)) => {
                        self.refill().await?;
                        continue;
                    }
                    // The store ends a stream's broadcast when it moves to a new epoch.
                    Err(RecvError::Closed) if self.store.meta().epoch != self.meta.epoch => return Err(FeedError::Expired),
                    Err(RecvError::Closed) => return Err(FeedError::Closed),
                },
            };
            if entry.sequence <= self.last {
                // Already delivered from the backlog before the live copy arrived.
                continue;
            }
            if entry.sequence != self.last + 1 {
                self.backlog.push_front(entry);
                self.refill().await?;
                continue;
            }
            self.last = entry.sequence;
            return Ok(entry);
        }
    }

    async fn refill(&mut self) -> Result<(), FeedError> {
        let entries = self.store.read_after(Position { epoch: self.meta.epoch, sequence: self.last }).await?;
        if entries.first().is_some_and(|first| first.sequence != self.last + 1) {
            return Err(FeedError::Expired);
        }
        self.backlog = entries.into();
        Ok(())
    }
}
