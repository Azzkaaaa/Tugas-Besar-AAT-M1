use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bnpb_common::bnpb::common::v1::HazardEvent;
use prost::Message;
use prost_types::Timestamp;
#[cfg(test)]
use redis::AsyncCommands;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
#[cfg(test)]
use redis::streams::StreamId;
use redis::{AsyncConnectionConfig, Client, Script};
use tokio::sync::{Mutex, broadcast};

use crate::adapters::outbound::memory_store::LIVE_CAPACITY;
use crate::core::models::{FeedEntry, Publication, SharedEntry, TraceCarrier};
use crate::core::ports::{Position, ReplayError, ReplayStore, Snapshot, StoreUnavailable, StreamMeta};

const META_KEY: &str = "feed:meta";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
/// Poll only stream identity/head, never a batch of full wildfire payloads.
const FOLLOW_INTERVAL: Duration = Duration::from_millis(100);
const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
const MAX_REPLAY_BYTES: usize = 64 << 20;

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
local seen = prefix .. 'seen:' .. ARGV[4]
if redis.call('EXISTS', seen) == 1 then
  return {'duplicate'}
end
local entries = prefix .. 'entries'
local incoming = 2 * (#ARGV[4] + #ARGV[7] + #ARGV[8] + #ARGV[9]) + 8192
local count = redis.call('XLEN', entries)
local function trim(keep)
  local last = tonumber(redis.call('GET', prefix .. 'head') or '0')
  for sequence = last - count + 1, last - keep do
    local key = prefix .. 'summary:' .. sequence
    local id = redis.call('HGET', key, 'event_id')
    if id then redis.call('DEL', prefix .. 'seen:' .. id) end
    redis.call('DEL', key)
  end
  redis.call('XTRIM', entries, 'MAXLEN', keep)
  count = keep
end
-- Bound retained stream allocations, including rolling writers' old entries.
-- Halving takes logarithmic trim steps and never materializes old bodies.
if count >= tonumber(ARGV[6]) then trim(tonumber(ARGV[6]) - 1) end
while count > 0 and (redis.call('MEMORY', 'USAGE', entries, 'SAMPLES', 0) or 0) + incoming > tonumber(ARGV[11]) do
  trim(math.floor(count / 2))
end
local marked = redis.pcall('SET', seen, '1', 'NX', 'PX', ARGV[5])
if type(marked) == 'table' and marked.err then return redis.error_reply(marked.err) end
if not marked then return {'duplicate'} end
local sequence = redis.pcall('INCR', prefix .. 'head')
if type(sequence) == 'table' and sequence.err then
  redis.call('DEL', seen)
  return redis.error_reply(sequence.err)
end
local written = redis.pcall('XADD', entries, sequence .. '-0',
  'event_id', ARGV[4], 'published_at', ARGV[7], 'hazard', ARGV[8], 'trace', ARGV[9], 'stored_at_ms', ARGV[10])
if type(written) == 'table' and written.err then
  redis.call('DECR', prefix .. 'head')
  redis.call('DEL', seen)
  return redis.error_reply(written.err)
end
count = redis.call('XLEN', entries)
local summary_key = prefix .. 'summary:' .. sequence
local cached = redis.pcall('HSET', summary_key, 'event_id', ARGV[4], 'published_at', ARGV[7],
  'hazard', ARGV[12], 'trace', ARGV[9], 'stored_at_ms', ARGV[10])
if type(cached) ~= 'table' or not cached.err then redis.pcall('PEXPIRE', summary_key, ARGV[5]) end
while count > 1 and (redis.call('MEMORY', 'USAGE', entries, 'SAMPLES', 0) or 0) > tonumber(ARGV[11]) do
  trim(math.max(1, math.floor(count / 2)))
end
return {'stored', tostring(sequence)}
",
    )
});

static CACHE_SUMMARY: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1] then return 0 end
local prefix = 'feed:' .. ARGV[1] .. ':'
local last = tonumber(redis.call('GET', prefix .. 'head') or '0')
local sequence = tonumber(ARGV[2])
if sequence > last or sequence <= last - redis.call('XLEN', prefix .. 'entries') then return 0 end
local key = prefix .. 'summary:' .. ARGV[2]
redis.call('HSET', key, 'event_id', ARGV[3], 'published_at', ARGV[4], 'hazard', ARGV[5], 'trace', ARGV[6], 'stored_at_ms', ARGV[7])
redis.call('PEXPIRE', key, ARGV[8])
return 1
",
    )
});

// New and repaired rows have a small summary hash. Missing hashes are only a
// hint: acquire the shared fallback slot before asking Redis for any full body.
// The script checks the epoch/head at the same instant as the selected row.
static READ_ONE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1] then return {'expired'} end
local head = tonumber(redis.call('GET', ARGV[2] .. 'head') or '0')
local after = tonumber(ARGV[3])
if after > head then return {'expired'} end
if after == head then return {'head'} end
-- A byte trim can shorten the window before max_entries is reached.
if head - after > redis.call('XLEN', ARGV[2] .. 'entries') then return {'expired'} end
local sequence = after + 1
if ARGV[4] == '1' then
  local fields = redis.call('HMGET', ARGV[2] .. 'summary:' .. sequence,
    'event_id', 'published_at', 'hazard', 'trace', 'stored_at_ms')
  if fields[1] and fields[2] and fields[3] and fields[4] and fields[5] then
    if tonumber(fields[5]) < tonumber(ARGV[7]) then return {'expired'} end
    return {'summary', tostring(sequence), fields[1], fields[2], fields[3], fields[4], fields[5]}
  end
  if ARGV[5] == '0' then return {'legacy'} end
end
local rows = redis.call('XRANGE', ARGV[2] .. 'entries', sequence .. '-0', sequence .. '-0', 'COUNT', 1)
if #rows ~= 1 then return {'expired'} end
local values = {}
for i = 1, #rows[1][2], 2 do values[rows[1][2][i]] = rows[1][2][i+1] end
if not values['stored_at_ms'] or tonumber(values['stored_at_ms']) < tonumber(ARGV[7]) then return {'expired'} end
return {'full', tostring(sequence), values['event_id'], values['published_at'], values['hazard'], values['trace'], values['stored_at_ms']}
",
    )
});

/// Replay buffer shared by every replica through Redis:
///
/// - `feed:meta` — hash `{epoch, key}`: the stream identity and cursor key.
/// - `feed:{epoch}:head` — the last sequence.
/// - `feed:{epoch}:seen:{event_id}` — dedup markers, expiring with the replay retention.
/// - `feed:{epoch}:entries` — the stream, entry IDs `{sequence}-0`, capped at max_entries.
/// - `feed:{epoch}:summary:{sequence}` — small projection metadata, evicted
///   with its entry or by retention. Legacy/missing caches are repaired singly.
///
/// When Redis loses the replay keys, the next writer or follower creates
/// a new epoch, so earlier cursors expire (OUT_OF_RANGE) instead of pointing into a new stream.
/// Each replica follows small head notices. Subscribers fetch one projection
/// at a time, including events consumed by other replicas.
#[derive(Clone)]
pub struct RedisStore {
    client: Client,
    commands: ConnectionManager,
    retention: Duration,
    max_entries: usize,
    live: Arc<RwLock<Live>>,
    summary_fallback: Arc<Mutex<()>>,
}

/// The epoch this replica follows and the broadcast of its entries. Replaced together, so a
/// subscription's receiver always belongs to the meta it was given.
struct Live {
    meta: StreamMeta,
    sender: broadcast::Sender<u64>,
}

enum ReadOne {
    Head,
    Legacy,
    Entry(Box<FeedEntry>, bool),
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
        let store = Arc::new(Self {
            client: client.clone(),
            commands,
            retention,
            max_entries: max_entries.max(1),
            live: Arc::new(RwLock::new(Live { meta, sender })),
            summary_fallback: Arc::new(Mutex::new(())),
        });
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
        let config = AsyncConnectionConfig::new().set_connection_timeout(Some(COMMAND_TIMEOUT)).set_response_timeout(Some(COMMAND_TIMEOUT));
        let mut connection = client.get_multiplexed_async_connection_with_config(&config).await.map_err(|error| error.to_string())?;
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

            let current_head = head(&mut connection, meta.epoch).await.map_err(|error| error.to_string())?;
            if current_head > *last {
                *last = current_head;
                let _ = self.live().sender.send(current_head);
            }
            tokio::time::sleep(FOLLOW_INTERVAL).await;
        }
    }

    async fn read_one(&self, after: Position, summary: bool, fallback: bool) -> Result<ReadOne, ReplayError> {
        self.read_one_on(&mut self.commands.clone(), after, summary, fallback).await
    }

    async fn read_one_on<C: redis::aio::ConnectionLike + Send>(
        &self,
        commands: &mut C,
        after: Position,
        summary: bool,
        fallback: bool,
    ) -> Result<ReadOne, ReplayError> {
        let cutoff =
            SystemTime::now().checked_sub(self.retention).unwrap_or(UNIX_EPOCH).duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
                as u64;
        let fields: Vec<Vec<u8>> = READ_ONE
            .key(META_KEY)
            .arg(after.epoch.to_string())
            .arg(format!("feed:{}:", after.epoch))
            .arg(after.sequence)
            .arg(if summary { "1" } else { "0" })
            .arg(if fallback { "1" } else { "0" })
            .arg(self.max_entries)
            .arg(cutoff)
            .invoke_async(&mut *commands)
            .await
            .map_err(unavailable)?;
        match fields.first().map(Vec::as_slice) {
            Some(b"head") => Ok(ReadOne::Head),
            Some(b"legacy") => Ok(ReadOne::Legacy),
            Some(b"summary" | b"full") if fields.len() == 7 => {
                let cached = fields[0] == b"summary";
                match decode_fields(&fields) {
                    Some(entry) => Ok(ReadOne::Entry(Box::new(entry), cached)),
                    None if cached => {
                        // Derived cache corruption is repairable from the immutable entry.
                        let _: () = redis::cmd("DEL")
                            .arg(summary_key(after.epoch, after.sequence + 1))
                            .query_async(&mut *commands)
                            .await
                            .map_err(unavailable)?;
                        Ok(ReadOne::Legacy)
                    }
                    None => Err(ReplayError::Expired),
                }
            }
            _ => Err(ReplayError::Expired),
        }
    }

    async fn cache_summary(&self, epoch: u64, entry: &FeedEntry) {
        let header = entry.publication.summary();
        let stored_at_ms = entry.stored_at.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        // Cache fill cannot stall ingestion or change the authoritative entry.
        let mut commands = self.commands.clone();
        let mut fill = CACHE_SUMMARY.prepare_invoke();
        fill.key(META_KEY)
            .arg(epoch.to_string())
            .arg(entry.sequence)
            .arg(&header.event_id)
            .arg(header.published_at.map(|t| t.encode_to_vec()).unwrap_or_default())
            .arg(header.hazard.encode_to_vec())
            .arg(serde_json::to_string(&header.trace).unwrap_or_else(|_| "[]".into()))
            .arg(stored_at_ms)
            .arg(self.retention.as_millis() as u64);
        let _ = tokio::time::timeout(Duration::from_millis(50), fill.invoke_async::<i32>(&mut commands)).await;
    }

    async fn read_after_in(&self, after: Position, summary: bool) -> Result<Vec<SharedEntry>, ReplayError> {
        if !summary {
            // A FULL subscription owns its admission until this future drops.
            // Cancellation must close the actual large-response socket rather
            // than leave work outstanding on the shared multiplexer.
            let config =
                AsyncConnectionConfig::new().set_connection_timeout(Some(COMMAND_TIMEOUT)).set_response_timeout(Some(COMMAND_TIMEOUT));
            let mut connection = self.client.get_multiplexed_async_connection_with_config(&config).await.map_err(unavailable)?;
            return match self.read_one_on(&mut connection, after, false, false).await? {
                ReadOne::Head => Ok(Vec::new()),
                ReadOne::Entry(entry, _) => Ok(vec![Arc::new(*entry)]),
                ReadOne::Legacy => Err(ReplayError::Unavailable),
            };
        }
        match self.read_one(after, summary, false).await? {
            ReadOne::Head => Ok(Vec::new()),
            ReadOne::Entry(entry, _) => Ok(vec![Arc::new(*entry)]),
            ReadOne::Legacy => {
                let slot = self.summary_fallback.clone().lock_owned().await;
                let owned = self.clone();
                // A cancelled RPC must not return credit while redis-rs is
                // still reading its queued large response. The owned task
                // finishes/cleans up even if the caller drops its JoinHandle.
                tokio::spawn(async move {
                    let _slot = slot;
                    let config = AsyncConnectionConfig::new()
                        .set_connection_timeout(Some(COMMAND_TIMEOUT))
                        .set_response_timeout(Some(COMMAND_TIMEOUT));
                    // Last-drop closes this dedicated connection (including
                    // its driver on timeout) before admission is returned.
                    let mut connection = owned.client.get_multiplexed_async_connection_with_config(&config).await.map_err(unavailable)?;
                    match owned.read_one_on(&mut connection, after, true, true).await? {
                        ReadOne::Head => Ok(Vec::new()),
                        ReadOne::Entry(entry, cached) => {
                            if !cached {
                                owned.cache_summary(after.epoch, &entry).await;
                            }
                            Ok(vec![Arc::new(FeedEntry {
                                sequence: entry.sequence,
                                stored_at: entry.stored_at,
                                publication: entry.publication.summary(),
                            })])
                        }
                        ReadOne::Legacy => Err(ReplayError::Unavailable),
                    }
                })
                .await
                .map_err(|_| ReplayError::Unavailable)?
            }
        }
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
        let summary = publication.summary().hazard.encode_to_vec();
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
                .arg(MAX_REPLAY_BYTES)
                .arg(&summary)
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
                    let entry = FeedEntry { sequence, stored_at, publication };
                    return Ok(Some(Arc::new(entry)));
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

    async fn subscribe(&self, after: Option<Position>, summary: bool) -> Result<Snapshot, ReplayError> {
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
                let backlog = self.read_after_in(after, summary).await?;
                let head = backlog.last().map_or(after.sequence, |entry| entry.sequence);
                Ok(Snapshot { meta, backlog, head, live })
            }
            None => {
                let head = head(&mut self.commands.clone(), meta.epoch).await.map_err(unavailable)?;
                Ok(Snapshot { meta, backlog: Vec::new(), head, live })
            }
        }
    }

    async fn read_after(&self, after: Position, summary: bool) -> Result<Vec<SharedEntry>, ReplayError> {
        if after.epoch != self.meta().epoch {
            return Err(ReplayError::Expired);
        }
        self.read_after_in(after, summary).await
    }
}

fn unavailable(error: redis::RedisError) -> ReplayError {
    tracing::warn!(%error, "replay read failed");
    ReplayError::Unavailable
}

#[cfg(test)]
fn entries_key(epoch: u64) -> String {
    format!("feed:{epoch}:entries")
}

#[cfg(test)]
#[path = "redis_memory_tests.rs"]
mod memory_tests;

fn summary_key(epoch: u64, sequence: u64) -> String {
    format!("feed:{epoch}:summary:{sequence}")
}

fn decode_fields(fields: &[Vec<u8>]) -> Option<FeedEntry> {
    let sequence = std::str::from_utf8(&fields[1]).ok()?.parse().ok()?;
    let event_id = String::from_utf8(fields[2].clone()).ok()?;
    let published_at = if fields[3].is_empty() { None } else { Some(Timestamp::decode(fields[3].as_slice()).ok()?) };
    let hazard = HazardEvent::decode(fields[4].as_slice()).ok()?;
    let trace: TraceCarrier = serde_json::from_slice(&fields[5]).ok()?;
    let stored_at = UNIX_EPOCH + Duration::from_millis(std::str::from_utf8(&fields[6]).ok()?.parse().ok()?);
    Some(FeedEntry { sequence, stored_at, publication: Publication { event_id, published_at, hazard, trace } })
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

#[cfg(test)]
fn decode_entry(id: &StreamId) -> Option<FeedEntry> {
    // Translate the historical row fixtures into READ_ONE's wire tuple and
    // exercise the actual decoder, rather than keep an obsolete decoder in tests.
    decode_fields(&[
        b"full".to_vec(),
        id.id.split_once('-')?.0.as_bytes().to_vec(),
        id.get("event_id")?,
        id.get("published_at")?,
        id.get("hazard")?,
        id.get("trace")?,
        id.get("stored_at_ms")?,
    ])
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
