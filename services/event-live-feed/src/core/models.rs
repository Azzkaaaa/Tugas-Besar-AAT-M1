use std::sync::Arc;

use bnpb_common::bnpb::common::v1::HazardEvent;
use prost_types::Timestamp;
use tokio::time::Instant;

/// Propagation fields (W3C text map) set by the inbound adapter. The core only carries them, so
/// deliveries can rejoin the trace of the broker message that brought the publication.
pub type TraceCarrier = Vec<(String, String)>;

/// A broker publication accepted by the feed. The hazard stays the full canonical record;
/// projection to the caller's view happens in the inbound adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct Publication {
    pub event_id: String,
    pub published_at: Option<Timestamp>,
    pub hazard: HazardEvent,
    pub trace: TraceCarrier,
}

/// A publication with its position in the current stream epoch.
#[derive(Debug, PartialEq)]
pub struct FeedEntry {
    pub sequence: u64,
    pub stored_at: Instant,
    pub publication: Publication,
}

pub type SharedEntry = Arc<FeedEntry>;

/// An authenticated caller. `token` is the bearer itself, kept only so the session can renew it.
#[derive(Clone, PartialEq)]
pub struct Actor {
    pub subject: String,
    pub scopes: Vec<String>,
    pub expires_at: Instant,
    pub token: String,
}

impl std::fmt::Debug for Actor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Actor").field("subject", &self.subject).field("scopes", &self.scopes).finish_non_exhaustive()
    }
}
