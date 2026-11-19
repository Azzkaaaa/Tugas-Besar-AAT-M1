use std::sync::Arc;
use std::time::SystemTime;

use bnpb_common::bnpb::common::v1::HazardEvent;
use prost_types::{Timestamp, Value, value::Kind};
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

impl Publication {
    /// Conservative retained allocation accounting for the memory replay ring,
    /// including collection capacities, map nodes, queue and dedup overhead.
    pub fn retained_bytes(&self) -> usize {
        let h = &self.hazard;
        std::mem::size_of::<Self>()
            + 256
            + self.event_id.capacity()
            + h.hazard_id.capacity()
            + h.source_ref_id.capacity()
            + h.area_name.capacity()
            + self.trace.capacity() * std::mem::size_of::<(String, String)>()
            + self.trace.iter().map(|(k, v)| k.capacity() + v.capacity()).sum::<usize>()
            + h.attributes.as_ref().map_or(0, |attrs| struct_bytes(&attrs.fields))
    }

    /// Copy only fields used by SUMMARY/filtering. Never clone attributes and
    /// then discard them: a single wildfire can contain hundreds of thousands
    /// of detection keys.
    pub fn summary(&self) -> Self {
        Self {
            event_id: self.event_id.clone(),
            published_at: self.published_at,
            trace: self.trace.clone(),
            hazard: HazardEvent {
                hazard_id: self.hazard.hazard_id.clone(),
                source: self.hazard.source,
                hazard_type: self.hazard.hazard_type,
                severity: self.hazard.severity,
                area_name: self.hazard.area_name.clone(),
                occurred_at: self.hazard.occurred_at,
                ingested_at: self.hazard.ingested_at,
                ..Default::default()
            },
        }
    }
}

fn struct_bytes(fields: &std::collections::BTreeMap<String, Value>) -> usize {
    fields.iter().map(|(key, value)| key.capacity() + std::mem::size_of::<Value>() + 96 + value_bytes(value)).sum()
}

fn value_bytes(value: &Value) -> usize {
    match &value.kind {
        Some(Kind::StringValue(s)) => s.capacity(),
        Some(Kind::StructValue(s)) => struct_bytes(&s.fields),
        Some(Kind::ListValue(list)) => {
            list.values.capacity() * std::mem::size_of::<Value>() + list.values.iter().map(value_bytes).sum::<usize>()
        }
        _ => 0,
    }
}

/// A publication with its position in the current stream epoch.
#[derive(Debug, PartialEq)]
pub struct FeedEntry {
    pub sequence: u64,
    /// Wall clock, so it means the same on every replica.
    pub stored_at: SystemTime,
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
