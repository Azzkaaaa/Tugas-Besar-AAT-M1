use std::env;
use std::time::Duration;

const JWT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:jwt";

/// Confidential client that performs the RFC 8693 exchange (Zitadel's "BNPB gateway token exchange").
pub struct ClientCredentials {
    pub client_id: String,
    pub client_secret: String,
}

pub struct Config {
    pub port: u16,
    pub token_url: String,
    pub requested_token_type: String,
    pub timeout: Duration,
    pub client: ClientCredentials,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            port: number("TOKEN_EXCHANGE_GRPC_PORT", 9000)?,
            token_url: required("TOKEN_EXCHANGE_URL")?,
            requested_token_type: optional("TOKEN_EXCHANGE_REQUESTED_TOKEN_TYPE")?.unwrap_or_else(|| JWT_TOKEN_TYPE.into()),
            timeout: Duration::from_millis(number("TOKEN_EXCHANGE_TIMEOUT_MS", 5000)?),
            client: client()?,
        })
    }
}

/// Explicit env values win; otherwise the gateway client provisioned by auth-bootstrap in oidc.json.
/// Compose starts this service only after auth-bootstrap has written the file.
fn client() -> Result<ClientCredentials, String> {
    if let (Some(client_id), Some(client_secret)) = (optional("TOKEN_EXCHANGE_CLIENT_ID")?, optional("TOKEN_EXCHANGE_CLIENT_SECRET")?) {
        return Ok(ClientCredentials { client_id, client_secret });
    }
    let path =
        optional("OIDC_CONFIG_FILE")?.ok_or("TOKEN_EXCHANGE_CLIENT_ID/TOKEN_EXCHANGE_CLIENT_SECRET or OIDC_CONFIG_FILE is required")?;
    let text = std::fs::read_to_string(&path).map_err(|error| format!("cannot read OIDC_CONFIG_FILE {path}: {error}"))?;
    let json: serde_json::Value = serde_json::from_str(&text).map_err(|_| format!("OIDC_CONFIG_FILE {path} is not valid JSON"))?;
    let field = |key: &str| json.get(key).and_then(serde_json::Value::as_str).filter(|value| !value.is_empty()).map(String::from);
    match (field("gateway_client_id"), field("gateway_client_secret")) {
        (Some(client_id), Some(client_secret)) => Ok(ClientCredentials { client_id, client_secret }),
        _ => Err(format!("OIDC_CONFIG_FILE {path} has no gateway_client_id/gateway_client_secret")),
    }
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

pub fn number<T: std::str::FromStr>(name: &str, default: T) -> Result<T, String> {
    optional(name)?.map_or(Ok(default), |value| value.trim().parse().map_err(|_| format!("{name} must be a non-negative integer")))
}
