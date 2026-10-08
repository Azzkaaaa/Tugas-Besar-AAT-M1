use std::future::Future;

use tokio::sync::broadcast;

use crate::core::models::{Actor, Publication, SharedEntry};

/// The requested position is no longer (or was never) in the replay buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayExpired;

/// Replay entries after a position plus a live receiver that starts right after them.
pub struct Snapshot {
    pub backlog: Vec<SharedEntry>,
    pub head: u64,
    pub live: broadcast::Receiver<SharedEntry>,
}

/// Bounded replay storage owned by the feed: deduplication, sequence and buffer.
pub trait ReplayStore: Send + Sync + 'static {
    fn epoch(&self) -> u64;
    /// Stores and broadcasts a new publication; `None` when the event ID was already stored.
    fn append(&self, publication: Publication) -> Option<SharedEntry>;
    /// Entries after `after` (all live entries from the head when `None`), atomically with
    /// subscribing to later ones, so replay joins the live stream without gaps.
    fn subscribe(&self, after: Option<u64>) -> Result<Snapshot, ReplayExpired>;
    fn read_after(&self, after: u64) -> Result<Vec<SharedEntry>, ReplayExpired>;
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
