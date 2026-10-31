use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bnpb_common::bnpb::common::v1::HazardEvent;
use prost::Message;
use prost_types::Timestamp;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::streams::{StreamId, StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, AsyncConnectionConfig, Client, Script};
use tokio::sync::broadcast;

use crate::adapters::outbound::memory_store::LIVE_CAPACITY;
use crate::core::models::{FeedEntry, Publication, SharedEntry, TraceCarrier};
use crate::core::ports::{Position, ReplayError, ReplayStore, Snapshot, StoreUnavailable, StreamMeta};

const META_KEY: &str = "feed:meta";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
/// How long the follower blocks on XREAD; also how quickly it notices a reset.
const FOLLOW_BLOCK: Duration = Duration::from_secs(1);
const FOLLOW_BATCH: usize = 512;
const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// Returns the stored meta, creating it from the candidate (ARGV) when Redis lost it.
static ENSURE_META: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local epoch = redis.call('HGET', KEYS[1], 'epoch')
if not epoch then
  redis.call('HSET', KEYS[1], 'epoch', ARGV[1], 'key', ARGV[2])
  return {ARGV[1], ARGV[2]}
end
return {epoch, redis.call('HGET', KEYS[1], 'key')}
",
    )
});

/// Dedupes, sequences and stores one publication atomically. The stream keys derive from the
/// stored epoch inside the script, so this needs a single (non-cluster) Redis.
static APPEND: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local epoch = redis.call('HGET', KEYS[1], 'epoch')
if not epoch then
  redis.call('HSET', KEYS[1], 'epoch', ARGV[2], 'key', ARGV[3])
  epoch = ARGV[2]
end
if epoch ~= ARGV[1] then
  return {'moved', epoch}
end
local prefix = 'feed:' .. epoch .. ':'
if not redis.call('SET', prefix .. 'seen:' .. ARGV[4], '1', 'NX', 'PX', ARGV[5]) then
  return {'duplicate'}
end
local sequence = redis.call('INCR', prefix .. 'head')
redis.call('XADD', prefix .. 'entries', 'MAXLEN', ARGV[6], sequence .. '-0',
  'event_id', ARGV[4], 'published_at', ARGV[7], 'hazard', ARGV[8], 'trace', ARGV[9], 'stored_at_ms', ARGV[10])
return {'stored', tostring(sequence)}
",
    )
});

/// Replay buffer shared by every replica through Redis:
///
/// - `feed:meta` — hash `{epoch, key}`: the stream identity and cursor key.
/// - `feed:{epoch}:head` — the last sequence.
/// - `feed:{epoch}:seen:{event_id}` — dedup markers, expiring with the replay retention.
/// - `feed:{epoch}:entries` — the stream, entry IDs `{sequence}-0`, capped at max_entries.
///
/// Redis runs without persistence. When it loses the keys, the next writer or follower creates
/// a new epoch, so earlier cursors expire (OUT_OF_RANGE) instead of pointing into a new stream.
/// Each replica follows the stream with XREAD and fans entries out to its own subscribers, so a
/// subscriber sees events consumed by any replica.
pub struct RedisStore {
    commands: ConnectionManager,
    retention: Duration,
    max_entries: usize,
    live: RwLock<Live>,
}

/// The epoch this replica follows and the broadcast of its entries. Replaced together, so a
/// subscription's receiver always belongs to the meta it was given.
struct Live {
    meta: StreamMeta,
    sender: broadcast::Sender<SharedEntry>,
}

impl RedisStore {
    /// Connects, loads (or creates) the stream identity and starts following the stream.
    pub async fn connect(url: &str, retention: Duration, max_entries: usize) -> Result<Arc<Self>, String> {
        let client = Client::open(url).map_err(|error| format!("invalid FEED_REDIS_URL: {error}"))?;
        let config = ConnectionManagerConfig::new().set_response_timeout(Some(COMMAND_TIMEOUT));
        let mut commands =
            ConnectionManager::new_with_config(client.clone(), config).await.map_err(|error| format!("cannot reach Redis: {error}"))?;
        let meta = ensure_meta(&mut commands).await?;
        let head = head(&mut commands, meta.epoch).await.map_err(|error| error.to_string())?;
        let (sender, _) = broadcast::channel(LIVE_CAPACITY);
        let store = Arc::new(Self { commands, retention, max_entries: max_entries.max(1), live: RwLock::new(Live { meta, sender }) });
        tokio::spawn(store.clone().follow(client, head));
        tracing::info!(epoch = format_args!("{:016x}", meta.epoch), head, "replay store connected to Redis");
        Ok(store)
    }

    fn live(&self) -> std::sync::RwLockReadGuard<'_, Live> {
        self.live.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Forwards stream entries to local subscribers forever, reconnecting with backoff.
    async fn follow(self: Arc<Self>, client: Client, mut last: u64) {
        let mut backoff = RETRY_MIN;
        loop {
            match self.follow_once(&client, &mut last, &mut backoff).await {
                Ok(()) => {}
                Err(error) => tracing::warn!(%error, "replay stream follower lost Redis; reconnecting"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RETRY_MAX);
        }
    }

    async fn follow_once(&self, client: &Client, last: &mut u64, backoff: &mut Duration) -> Result<(), String> {
        let config = AsyncConnectionConfig::new().set_response_timeout(Some(FOLLOW_BLOCK + COMMAND_TIMEOUT));
        let mut connection = client.get_multiplexed_async_connection_with_config(&config).await.map_err(|error| error.to_string())?;
        let options = StreamReadOptions::default().block(FOLLOW_BLOCK.as_millis() as usize).count(FOLLOW_BATCH);
        loop {
            let meta = ensure_meta(&mut connection).await?;
            *backoff = RETRY_MIN;
            let current = self.live().meta;
            if meta.epoch != current.epoch {
                tracing::warn!(
                    from = format_args!("{:016x}", current.epoch),
                    to = format_args!("{:016x}", meta.epoch),
                    "replay stream was reset; ending subscriptions of the old epoch"
                );
                // Dropping the old sender closes its receivers; subscribers then see the new epoch.
                let (sender, _) = broadcast::channel(LIVE_CAPACITY);
                *self.live.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = Live { meta, sender };
                *last = 0;
            }

            let stream = entries_key(meta.epoch);
            let reply: Option<StreamReadReply> =
                connection.xread_options(&[stream.as_str()], &[format!("{last}-0")], &options).await.map_err(|error| error.to_string())?;
            let Some(reply) = reply else { continue };
            let live = self.live();
            for id in reply.keys.iter().flat_map(|key| &key.ids) {
                let Some(entry) = decode_entry(id) else { continue };
                *last = entry.sequence;
                let _ = live.sender.send(Arc::new(entry));
            }
        }
    }

    async fn read_after_in(&self, after: Position) -> Result<Vec<SharedEntry>, ReplayError> {
        let mut commands = self.commands.clone();
        let head = head(&mut commands, after.epoch).await.map_err(unavailable)?;
        if after.sequence > head {
            return Err(ReplayError::Expired);
        }
        if after.sequence == head {
            return Ok(Vec::new());
        }
        let reply: StreamRangeReply =
            commands.xrange(entries_key(after.epoch), format!("{}-0", after.sequence + 1), "+").await.map_err(unavailable)?;
        let cutoff = SystemTime::now().checked_sub(self.retention).unwrap_or(UNIX_EPOCH);
        let entries: Vec<SharedEntry> =
            reply.ids.iter().filter_map(decode_entry).skip_while(|entry| entry.stored_at < cutoff).map(Arc::new).collect();
        // Trimmed by size or retention: resuming would skip events.
        if entries.first().map(|entry| entry.sequence) != Some(after.sequence + 1) {
            return Err(ReplayError::Expired);
        }
        Ok(entries)
    }
}

impl ReplayStore for RedisStore {
    fn meta(&self) -> StreamMeta {
        self.live().meta
    }

    async fn append(&self, publication: Publication) -> Result<Option<SharedEntry>, StoreUnavailable> {
        let stored_at = SystemTime::now();
        let stored_at_ms = stored_at.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let published_at = publication.published_at.map(|timestamp| timestamp.encode_to_vec()).unwrap_or_default();
        let hazard = publication.hazard.encode_to_vec();
        let trace = serde_json::to_string(&publication.trace).unwrap_or_else(|_| "[]".into());
        let mut commands = self.commands.clone();
        let mut expected = self.meta().epoch;
        // A second attempt follows a reset that happened since the follower last checked.
        for _ in 0..2 {
            let candidate = new_meta();
            let reply: Vec<String> = APPEND
                .key(META_KEY)
                .arg(expected.to_string())
                .arg(candidate.epoch.to_string())
                .arg(URL_SAFE_NO_PAD.encode(candidate.cursor_key))
                .arg(&publication.event_id)
                .arg(self.retention.as_millis() as u64)
                .arg(self.max_entries)
                .arg(&published_at)
                .arg(&hazard)
                .arg(&trace)
                .arg(stored_at_ms)
                .invoke_async(&mut commands)
                .await
                .map_err(|error| {
                    tracing::warn!(%error, "replay append failed");
                    StoreUnavailable
                })?;
            match reply.as_slice() {
                [tag, sequence] if tag == "stored" => {
                    let sequence = sequence.parse().map_err(|_| StoreUnavailable)?;
                    // Local subscribers get it from the follower, like entries of other replicas.
                    return Ok(Some(Arc::new(FeedEntry { sequence, stored_at, publication })));
                }
                [tag] if tag == "duplicate" => return Ok(None),
                [tag, epoch] if tag == "moved" => expected = epoch.parse().map_err(|_| StoreUnavailable)?,
                _ => {
                    tracing::warn!(?reply, "unexpected replay append reply");
                    return Err(StoreUnavailable);
                }
            }
        }
        Err(StoreUnavailable)
    }

    async fn subscribe(&self, after: Option<Position>) -> Result<Snapshot, ReplayError> {
        // Subscribe before reading, so nothing appended meanwhile is missed; repeats are skipped.
        let (meta, live) = {
            let live = self.live();
            (live.meta, live.sender.subscribe())
        };
        match after {
            Some(after) => {
                if after.epoch != meta.epoch {
                    return Err(ReplayError::Expired);
                }
                let backlog = self.read_after_in(after).await?;
                let head = backlog.last().map_or(after.sequence, |entry| entry.sequence);
                Ok(Snapshot { meta, backlog, head, live })
            }
            None => {
                let head = head(&mut self.commands.clone(), meta.epoch).await.map_err(unavailable)?;
                Ok(Snapshot { meta, backlog: Vec::new(), head, live })
            }
        }
    }

    async fn read_after(&self, after: Position) -> Result<Vec<SharedEntry>, ReplayError> {
        if after.epoch != self.meta().epoch {
            return Err(ReplayError::Expired);
        }
        self.read_after_in(after).await
    }
}

fn unavailable(error: redis::RedisError) -> ReplayError {
    tracing::warn!(%error, "replay read failed");
    ReplayError::Unavailable
}

fn entries_key(epoch: u64) -> String {
    format!("feed:{epoch}:entries")
}

async fn head(connection: &mut impl redis::aio::ConnectionLike, epoch: u64) -> redis::RedisResult<u64> {
    let head: Option<u64> = redis::cmd("GET").arg(format!("feed:{epoch}:head")).query_async(connection).await?;
    Ok(head.unwrap_or(0))
}

fn new_meta() -> StreamMeta {
    StreamMeta { epoch: rand::random(), cursor_key: rand::random() }
}

async fn ensure_meta(connection: &mut impl redis::aio::ConnectionLike) -> Result<StreamMeta, String> {
    let candidate = new_meta();
    let (epoch, key): (String, String) = ENSURE_META
        .key(META_KEY)
        .arg(candidate.epoch.to_string())
        .arg(URL_SAFE_NO_PAD.encode(candidate.cursor_key))
        .invoke_async(connection)
        .await
        .map_err(|error| format!("cannot load replay stream identity: {error}"))?;
    let epoch = epoch.parse().map_err(|_| format!("{META_KEY} has a malformed epoch"))?;
    let cursor_key = URL_SAFE_NO_PAD
        .decode(key)
        .ok()
        .and_then(|key| <[u8; 32]>::try_from(key).ok())
        .ok_or_else(|| format!("{META_KEY} has a malformed cursor key"))?;
    Ok(StreamMeta { epoch, cursor_key })
}

fn decode_entry(id: &StreamId) -> Option<FeedEntry> {
    let entry = (|| {
        let sequence = id.id.split_once('-')?.0.parse().ok()?;
        let event_id: String = id.get("event_id")?;
        let published_at: Vec<u8> = id.get("published_at")?;
        let published_at = if published_at.is_empty() { None } else { Some(Timestamp::decode(published_at.as_slice()).ok()?) };
        let hazard = HazardEvent::decode(id.get::<Vec<u8>>("hazard")?.as_slice()).ok()?;
        let trace: TraceCarrier = serde_json::from_str(&id.get::<String>("trace")?).ok()?;
        let stored_at = UNIX_EPOCH + Duration::from_millis(id.get("stored_at_ms")?);
        Some(FeedEntry { sequence, stored_at, publication: Publication { event_id, published_at, hazard, trace } })
    })();
    if entry.is_none() {
        tracing::warn!(id = %id.id, "skipping malformed replay entry");
    }
    entry
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bnpb_common::bnpb::common::v1::Source;
    use redis::Value;

    use super::*;

    fn stream_id(id: &str, fields: &[(&str, Vec<u8>)]) -> StreamId {
        StreamId {
            id: id.into(),
            map: fields.iter().map(|(name, value)| (name.to_string(), Value::BulkString(value.clone()))).collect::<HashMap<_, _>>(),
            ..Default::default()
        }
    }

    #[test]
    fn decodes_what_append_writes() {
        let hazard = HazardEvent { hazard_id: "h1".into(), source: Source::Bmkg as i32, ..Default::default() };
        let published_at = Timestamp { seconds: 1_700_000_000, nanos: 5 };
        let trace: TraceCarrier = vec![("traceparent".into(), "00-abc-def-01".into())];
        let id = stream_id(
            "42-0",
            &[
                ("event_id", b"evt-1".to_vec()),
                ("published_at", published_at.encode_to_vec()),
                ("hazard", hazard.encode_to_vec()),
                ("trace", serde_json::to_vec(&trace).unwrap()),
                ("stored_at_ms", b"1700000000123".to_vec()),
            ],
        );
        let entry = decode_entry(&id).unwrap();
        assert_eq!(entry.sequence, 42);
        assert_eq!(entry.stored_at, UNIX_EPOCH + Duration::from_millis(1_700_000_000_123));
        assert_eq!(entry.publication, Publication { event_id: "evt-1".into(), published_at: Some(published_at), hazard, trace });
    }

    #[test]
    fn missing_publication_time_decodes_as_none_and_garbage_is_skipped() {
        let fields = |hazard: Vec<u8>| {
            vec![
                ("event_id", b"evt-1".to_vec()),
                ("published_at", Vec::new()),
                ("hazard", hazard),
                ("trace", b"[]".to_vec()),
                ("stored_at_ms", b"1".to_vec()),
            ]
        };
        let entry = decode_entry(&stream_id("1-0", &fields(HazardEvent::default().encode_to_vec()))).unwrap();
        assert_eq!(entry.publication.published_at, None);
        assert!(decode_entry(&stream_id("1-0", &fields(vec![0xff, 0xff]))).is_none());
        assert!(decode_entry(&stream_id("x-0", &fields(Vec::new()))).is_none());
    }
}
