use std::env;
use std::time::Duration;

/// Confidential client used to re-exchange actor tokens (RFC 8693) before they expire.
#[derive(Clone)]
pub struct ClientCredentials {
    pub client_id: String,
    pub client_secret: String,
}

pub struct Config {
    pub port: u16,
    pub amqp_url: String,
    pub exchange: String,
    pub queue: String,
    pub dead_letter_queue: Option<String>,
    pub replay_retention: Duration,
    pub replay_max_entries: usize,
    /// Shared replay store; unset keeps the replay buffer in process memory (one replica).
    pub redis_url: Option<String>,
    pub auth_authority: String,
    pub exchange_client: Option<ClientCredentials>,
    pub renew_before: Duration,
    pub max_stream: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            port: number("EVENT_FEED_GRPC_PORT", 9093)?,
            amqp_url: required("EVENT_FEED_AMQP_URL")?,
            exchange: optional("RABBITMQ_EXCHANGE")?.unwrap_or_else(|| "bnpb.hazards.v1".into()),
            queue: optional("EVENT_FEED_CONSUMER_QUEUE")?.unwrap_or_else(|| "bnpb.event-live-feed.v1".into()),
            dead_letter_queue: optional("EVENT_FEED_CONSUMER_DLQ")?,
            replay_retention: Duration::from_secs(number("FEED_REPLAY_RETENTION_SECONDS", 600)?),
            replay_max_entries: number("FEED_REPLAY_MAX_ENTRIES", 10_000)?,
            redis_url: optional("FEED_REDIS_URL")?,
            auth_authority: optional("FEED_AUTH_AUTHORITY")?
                .unwrap_or_else(|| "http://auth.localhost:8080".into())
                .trim_end_matches('/')
                .into(),
            exchange_client: exchange_client()?,
            renew_before: Duration::from_secs(number("FEED_TOKEN_RENEW_BEFORE_SECONDS", 15)?),
            max_stream: Duration::from_secs(number("FEED_MAX_STREAM_SECONDS", 3600)?),
        })
    }
}

/// Explicit env values win; otherwise the client provisioned by auth-bootstrap in oidc.json.
/// Without either, rotation is disabled and streams end when the actor token expires.
fn exchange_client() -> Result<Option<ClientCredentials>, String> {
    if let (Some(client_id), Some(client_secret)) =
        (optional("FEED_TOKEN_EXCHANGE_CLIENT_ID")?, optional("FEED_TOKEN_EXCHANGE_CLIENT_SECRET")?)
    {
        return Ok(Some(ClientCredentials { client_id, client_secret }));
    }
    let Some(path) = optional("OIDC_CONFIG_FILE")? else { return Ok(None) };
    let text = std::fs::read_to_string(&path).map_err(|error| format!("cannot read OIDC_CONFIG_FILE {path}: {error}"))?;
    let json: serde_json::Value = serde_json::from_str(&text).map_err(|_| format!("OIDC_CONFIG_FILE {path} is not valid JSON"))?;
    let field = |key: &str| json.get(key).and_then(serde_json::Value::as_str).filter(|value| !value.is_empty()).map(String::from);
    Ok(match (field("feed_client_id"), field("feed_client_secret")) {
        (Some(client_id), Some(client_secret)) => Some(ClientCredentials { client_id, client_secret }),
        _ => None,
    })
}

fn optional(name: &str) -> Result<Option<String>, String> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) if value.contains("REPLACE_") => Err(format!("{name} still holds a placeholder value")),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(format!("{name} is not valid UTF-8")),
    }
}

fn required(name: &str) -> Result<String, String> {
    optional(name)?.ok_or_else(|| format!("{name} is required"))
}

fn number<T: std::str::FromStr>(name: &str, default: T) -> Result<T, String> {
    optional(name)?.map_or(Ok(default), |value| value.trim().parse().map_err(|_| format!("{name} must be a non-negative integer")))
}
