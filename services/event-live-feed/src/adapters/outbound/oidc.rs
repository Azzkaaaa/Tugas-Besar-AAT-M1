use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tokio::sync::OnceCell;
use tokio::time::Instant;
use tracing::Instrument;

use crate::config::ClientCredentials;
use crate::core::models::Actor;
use crate::core::ports::{ActorVerifier, AuthError, TokenRenewer};
use crate::telemetry;

const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
/// Unknown `kid`s trigger a JWKS refetch at most this often, so forged headers cannot flood Zitadel.
const JWKS_MIN_REFRESH: Duration = Duration::from_secs(10);
const ASYMMETRIC: [Algorithm; 9] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
];

#[derive(Deserialize)]
struct Metadata {
    jwks_uri: String,
    token_endpoint: String,
}

/// Sends a Zitadel request in a client span that carries `traceparent` upstream.
/// Only the path is recorded: the query and form body may hold tokens.
async fn traced(request: reqwest::RequestBuilder) -> reqwest::Result<reqwest::Response> {
    let (client, request) = request.build_split();
    let mut request = request?;
    let (method, url) = (request.method().clone(), request.url().clone());
    let span = tracing::info_span!("zitadel_request", otel.name = %format!("{method} {}", url.path()), otel.kind = "client",
        http.request.method = %method, server.address = url.host_str().unwrap_or_default(), url.path = url.path(),
        http.response.status_code = tracing::field::Empty, otel.status_code = tracing::field::Empty);
    telemetry::inject(&span, request.headers_mut());
    let response = client.execute(request).instrument(span.clone()).await;
    match &response {
        Ok(response) => {
            span.record("http.response.status_code", response.status().as_u16());
            if response.status().is_server_error() {
                span.record("otel.status_code", "error");
            }
        }
        Err(_) => {
            span.record("otel.status_code", "error");
        }
    }
    response
}

/// Zitadel endpoints from OIDC discovery, fetched on first use.
pub struct Oidc {
    http: reqwest::Client,
    issuer: String,
    metadata: OnceCell<Metadata>,
}

impl Oidc {
    pub fn new(issuer: String) -> Self {
        let http = reqwest::Client::builder().timeout(HTTP_TIMEOUT).build().expect("HTTP client configuration is static");
        Self { http, issuer, metadata: OnceCell::new() }
    }

    async fn metadata(&self) -> Result<&Metadata, AuthError> {
        self.metadata
            .get_or_try_init(|| async {
                let url = format!("{}/.well-known/openid-configuration", self.issuer);
                let response = traced(self.http.get(url)).await.and_then(reqwest::Response::error_for_status);
                let response = response.map_err(|error| AuthError::Unavailable(format!("OIDC discovery failed: {error}")))?;
                response.json().await.map_err(|error| AuthError::Unavailable(format!("OIDC discovery is malformed: {error}")))
            })
            .await
    }
}

/// Verifies Zitadel JWT access tokens locally against the issuer's JWKS.
pub struct JwksVerifier {
    oidc: Arc<Oidc>,
    keys: RwLock<(JwkSet, Option<Instant>)>,
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
    exp: u64,
    #[serde(default)]
    scopes: Scopes,
}

/// The claims action appends `scopes` as an array; a space-separated string is accepted too.
#[derive(Deserialize, Default)]
#[serde(untagged)]
enum Scopes {
    List(Vec<String>),
    Text(String),
    #[default]
    None,
}

impl JwksVerifier {
    pub fn new(oidc: Arc<Oidc>) -> Self {
        Self { oidc, keys: RwLock::new((JwkSet { keys: Vec::new() }, None)) }
    }

    fn cached(&self, kid: &str) -> Option<Result<DecodingKey, AuthError>> {
        let keys = self.keys.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        keys.0.find(kid).map(|jwk| DecodingKey::from_jwk(jwk).map_err(|_| AuthError::Rejected("unsupported signing key".into())))
    }

    async fn key(&self, kid: &str) -> Result<DecodingKey, AuthError> {
        if let Some(key) = self.cached(kid) {
            return key;
        }
        let fetched_at = self.keys.read().unwrap_or_else(|poisoned| poisoned.into_inner()).1;
        if fetched_at.is_none_or(|at| at.elapsed() >= JWKS_MIN_REFRESH) {
            let url = &self.oidc.metadata().await?.jwks_uri;
            let response = traced(self.oidc.http.get(url)).await.and_then(reqwest::Response::error_for_status);
            let response = response.map_err(|error| AuthError::Unavailable(format!("JWKS fetch failed: {error}")))?;
            let set: JwkSet = response.json().await.map_err(|error| AuthError::Unavailable(format!("JWKS is malformed: {error}")))?;
            *self.keys.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = (set, Some(Instant::now()));
        }
        self.cached(kid).unwrap_or_else(|| Err(AuthError::Rejected("unknown signing key".into())))
    }
}

impl ActorVerifier for JwksVerifier {
    async fn verify(&self, token: &str) -> Result<Actor, AuthError> {
        let rejected = |reason: &str| AuthError::Rejected(reason.into());
        let header = jsonwebtoken::decode_header(token).map_err(|_| rejected("malformed token"))?;
        if !ASYMMETRIC.contains(&header.alg) {
            return Err(rejected("unsupported token algorithm"));
        }
        let key = self.key(header.kid.as_deref().ok_or_else(|| rejected("token has no key ID"))?).await?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.oidc.issuer]);
        validation.set_required_spec_claims(&["exp", "iss", "sub"]);
        // Same as danger-notification: audience is the shared project, not per service.
        validation.validate_aud = false;
        validation.leeway = 1;
        let claims = jsonwebtoken::decode::<Claims>(token, &key, &validation).map_err(|_| rejected("invalid or expired token"))?.claims;
        let scopes = match claims.scopes {
            Scopes::List(scopes) => scopes,
            Scopes::Text(text) => text.split_whitespace().map(String::from).collect(),
            Scopes::None => Vec::new(),
        };
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        Ok(Actor {
            subject: claims.sub,
            scopes,
            expires_at: Instant::now() + Duration::from_secs(claims.exp.saturating_sub(now)),
            token: token.into(),
        })
    }
}

/// RFC 8693 exchange of the actor's current Zitadel JWT for a fresh one, as the feed's own client.
pub struct ZitadelTokenExchange {
    oidc: Arc<Oidc>,
    client: ClientCredentials,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize, Default)]
struct TokenError {
    #[serde(default)]
    error: String,
}

impl ZitadelTokenExchange {
    pub fn new(oidc: Arc<Oidc>, client: ClientCredentials) -> Self {
        Self { oidc, client }
    }
}

impl TokenRenewer for ZitadelTokenExchange {
    async fn renew(&self, token: &str) -> Result<String, AuthError> {
        let endpoint = &self.oidc.metadata().await?.token_endpoint;
        let form = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
            ("subject_token", token),
            ("subject_token_type", "urn:ietf:params:oauth:token-type:access_token"),
            ("requested_token_type", "urn:ietf:params:oauth:token-type:jwt"),
        ];
        let request = self.oidc.http.post(endpoint).basic_auth(&self.client.client_id, Some(&self.client.client_secret)).form(&form);
        let response = traced(request).await.map_err(|error| AuthError::Unavailable(format!("token exchange failed: {error}")))?;
        let status = response.status();
        if status.is_success() {
            let body: TokenResponse =
                response.json().await.map_err(|error| AuthError::Unavailable(format!("token exchange response is malformed: {error}")))?;
            return Ok(body.access_token);
        }
        if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(AuthError::Unavailable(format!("token exchange returned {status}")));
        }
        // Error codes only; the provider body may echo request data.
        let error = response.json::<TokenError>().await.unwrap_or_default().error;
        Err(AuthError::Rejected(format!("token exchange returned {status} {error}")))
    }
}
