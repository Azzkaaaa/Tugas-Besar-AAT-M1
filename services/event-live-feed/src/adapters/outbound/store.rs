use std::sync::Arc;

use crate::adapters::outbound::memory_store::MemoryStore;
use crate::adapters::outbound::redis_store::RedisStore;
use crate::core::models::{Publication, SharedEntry};
use crate::core::ports::{Position, ReplayError, ReplayStore, Snapshot, StoreUnavailable, StreamMeta};

/// The replay store picked at startup: Redis when FEED_REDIS_URL is set (any number of
/// replicas), otherwise process memory (one replica).
pub enum Store {
    Memory(MemoryStore),
    Redis(Arc<RedisStore>),
}

impl ReplayStore for Store {
    fn meta(&self) -> StreamMeta {
        match self {
            Store::Memory(store) => store.meta(),
            Store::Redis(store) => store.meta(),
        }
    }

    async fn append(&self, publication: Publication) -> Result<Option<SharedEntry>, StoreUnavailable> {
        match self {
            Store::Memory(store) => store.append(publication).await,
            Store::Redis(store) => store.append(publication).await,
        }
    }

    async fn subscribe(&self, after: Option<Position>) -> Result<Snapshot, ReplayError> {
        match self {
            Store::Memory(store) => store.subscribe(after).await,
            Store::Redis(store) => store.subscribe(after).await,
        }
    }

    async fn read_after(&self, after: Position) -> Result<Vec<SharedEntry>, ReplayError> {
        match self {
            Store::Memory(store) => store.read_after(after).await,
            Store::Redis(store) => store.read_after(after).await,
        }
    }
}
