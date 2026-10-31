use std::future::Future;

use tokio::sync::broadcast;

use crate::core::models::{Actor, Publication, SharedEntry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayError {
    /// The requested position is no longer (or was never) in the replay buffer, or belongs to
    /// an earlier stream epoch.
    Expired,
    /// The store could not be reached.
    Unavailable,
}

/// The replay store could not be reached; the publication was not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreUnavailable;

/// Identity of the replay stream. Sequences only mean something within one epoch, and the key
/// signs cursors, so every replica sharing the stream verifies the same cursors. A reset of
/// the store starts a new epoch with a new key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamMeta {
    pub epoch: u64,
    pub cursor_key: [u8; 32],
}

/// A sequence number within an epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub epoch: u64,
    pub sequence: u64,
}

/// Replay entries after a position plus a live receiver that starts no later than them. The
/// live stream may repeat backlog entries; subscribers skip sequences they already have.
pub struct Snapshot {
    pub meta: StreamMeta,
    pub backlog: Vec<SharedEntry>,
    pub head: u64,
    pub live: broadcast::Receiver<SharedEntry>,
}

/// Bounded replay storage owned by the feed: deduplication, sequence and buffer.
pub trait ReplayStore: Send + Sync + 'static {
    /// The stream the store currently serves.
    fn meta(&self) -> StreamMeta;
    /// Stores and broadcasts a new publication; `None` when the event ID was already stored.
    fn append(&self, publication: Publication) -> impl Future<Output = Result<Option<SharedEntry>, StoreUnavailable>> + Send;
    /// Entries after `after` (all live entries from the head when `None`), together with a
    /// subscription to later ones, so replay joins the live stream without gaps.
    fn subscribe(&self, after: Option<Position>) -> impl Future<Output = Result<Snapshot, ReplayError>> + Send;
    fn read_after(&self, after: Position) -> impl Future<Output = Result<Vec<SharedEntry>, ReplayError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The token is invalid, expired or revoked.
    Rejected(String),
    /// The identity provider could not be reached; never treated as success.
    Unavailable(String),
}

pub trait ActorVerifier: Send + Sync + 'static {
    fn verify(&self, token: &str) -> impl Future<Output = Result<Actor, AuthError>> + Send;
}

/// Exchanges the actor's current token for a fresh one before it expires.
pub trait TokenRenewer: Send + Sync + 'static {
    fn renew(&self, token: &str) -> impl Future<Output = Result<String, AuthError>> + Send;
}
