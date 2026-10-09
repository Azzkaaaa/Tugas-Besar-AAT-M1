use std::time::Duration;

use reqwest::StatusCode;
use serde::Deserialize;
use tracing::Instrument;

use crate::config::ClientCredentials;
use crate::telemetry;

const GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const SUBJECT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";

/// Why an exchange failed. Rejected tokens make the SPA refresh (401); anything else is the
/// provider's fault (502). The messages are the ones the Built On Envoy filter returned.
#[derive(Debug, PartialEq, Eq)]
pub enum ExchangeError {
    Rejected(String),
    Unavailable(String),
}

/// RFC 8693 exchange of the caller's opaque Zitadel access token for a JWT.
pub struct Exchanger {
    http: reqwest::Client,
    token_url: String,
    requested_token_type: String,
    client: ClientCredentials,
}

#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    token_type: String,
    #[serde(default)]
    issued_token_type: String,
}

#[derive(Deserialize, Default)]
struct TokenError {
    #[serde(default)]
    error: String,
}

impl Exchanger {
    pub fn new(token_url: String, requested_token_type: String, timeout: Duration, client: ClientCredentials) -> Self {
        let http = reqwest::Client::builder().timeout(timeout).build().expect("HTTP client configuration is static");
        Self { http, token_url, requested_token_type, client }
    }

    /// Returns the upstream `Authorization` value (`<token_type> <access_token>`).
    pub async fn exchange(&self, subject_token: &str) -> Result<String, ExchangeError> {
        let form = [
            ("grant_type", GRANT_TYPE),
            ("subject_token_type", SUBJECT_TOKEN_TYPE),
            ("requested_token_type", &self.requested_token_type),
            ("subject_token", subject_token),
        ];
        let request = self.http.post(&self.token_url).basic_auth(&self.client.client_id, Some(&self.client.client_secret)).form(&form);
        let response = traced(request).await.map_err(|error| {
            // Without the URL: reqwest prints it, and logs only need the error kind.
            tracing::error!(error = %error.without_url(), "token exchange request failed");
            ExchangeError::Unavailable("token exchange unavailable".into())
        })?;
        let status = response.status();
        // 429 is the provider being overloaded, not a bad token: a 401 would only make the SPA refresh.
        if status.is_client_error() && status != StatusCode::TOO_MANY_REQUESTS {
            // Error code only; the provider body may echo request data.
            let error = response.json::<TokenError>().await.unwrap_or_default().error;
            tracing::warn!(status = status.as_u16(), %error, "token exchange rejected");
            return Err(ExchangeError::Rejected(format!("token exchange rejected: STS returned {}", status.as_u16())));
        }
        if status != StatusCode::OK {
            tracing::error!(status = status.as_u16(), "token exchange error");
            return Err(ExchangeError::Unavailable(format!("token exchange error: STS returned {}", status.as_u16())));
        }
        let invalid = |reason: &str| {
            tracing::error!(reason, "invalid token exchange response");
            ExchangeError::Unavailable(format!("invalid token exchange response: {reason}"))
        };
        let body: TokenResponse = response.json().await.map_err(|_| invalid("malformed JSON"))?;
        if body.access_token.is_empty() {
            return Err(invalid("missing required access_token"));
        }
        if body.token_type.is_empty() {
            return Err(invalid("missing required token_type"));
        }
        if body.issued_token_type.is_empty() {
            return Err(invalid("missing required issued_token_type"));
        }
        if body.token_type.eq_ignore_ascii_case("N_A") {
            return Err(invalid("returned token_type N_A"));
        }
        Ok(format!("{} {}", body.token_type, body.access_token))
    }
}

/// Sends a Zitadel request in a client span that carries `traceparent` upstream.
/// Only the path is recorded: the form body holds the subject token and the client secret.
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

/// One-shot HTTP/1.1 stub for the token endpoint: records the raw request and sends a canned response.
#[cfg(test)]
pub mod stub {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    pub struct Stub {
        pub url: String,
        pub request: oneshot::Receiver<String>,
    }

    pub async fn serve(status: u16, body: &'static str, delay: Duration) -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/oauth/v2/token", listener.local_addr().unwrap());
        let (sender, request) = oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut raw = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                raw.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&raw).to_string();
                if let Some(end) = text.find("\r\n\r\n") {
                    let length = text[..end]
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase().strip_prefix("content-length:").map(|value| value.trim().parse().unwrap())
                        })
                        .unwrap_or(0usize);
                    if raw.len() >= end + 4 + length || read == 0 {
                        let _ = sender.send(text);
                        break;
                    }
                }
            }
            tokio::time::sleep(delay).await;
            let response = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
        Stub { url, request }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    use super::*;
    use crate::telemetry::testing;

    const OK: &str = r#"{"access_token":"jwt.value","token_type":"Bearer","issued_token_type":"urn:ietf:params:oauth:token-type:jwt"}"#;

    fn exchanger(url: String) -> Exchanger {
        let client = ClientCredentials { client_id: "gateway".into(), client_secret: "s3cret".into() };
        Exchanger::new(url, "urn:ietf:params:oauth:token-type:jwt".into(), Duration::from_millis(500), client)
    }

    async fn run(status: u16, body: &'static str) -> Result<String, ExchangeError> {
        let server = stub::serve(status, body, Duration::ZERO).await;
        exchanger(server.url).exchange("opaque").await
    }

    #[tokio::test]
    async fn sends_rfc8693_form_with_basic_auth_and_returns_authorization() {
        let server = stub::serve(200, OK, Duration::ZERO).await;
        let result = exchanger(server.url).exchange("opaque token+/").await;
        assert_eq!(result, Ok("Bearer jwt.value".into()));
        let request = server.request.await.unwrap();
        assert!(request.starts_with("POST /oauth/v2/token HTTP/1.1"));
        // base64("gateway:s3cret")
        assert!(request.to_ascii_lowercase().contains("authorization: basic z2f0zxdhetpzm2nyzxq="));
        assert!(request.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange"));
        assert!(request.contains("subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token"));
        assert!(request.contains("requested_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Ajwt"));
        assert!(request.contains("subject_token=opaque+token%2B%2F"));
    }

    #[tokio::test]
    async fn client_errors_reject_the_token() {
        assert_eq!(
            run(400, r#"{"error":"invalid_grant"}"#).await,
            Err(ExchangeError::Rejected("token exchange rejected: STS returned 400".into()))
        );
        assert!(matches!(run(401, "").await, Err(ExchangeError::Rejected(_))));
    }

    #[tokio::test]
    async fn provider_failures_are_unavailable() {
        assert_eq!(run(500, "").await, Err(ExchangeError::Unavailable("token exchange error: STS returned 500".into())));
        assert!(matches!(run(429, "").await, Err(ExchangeError::Unavailable(_))));
        assert!(matches!(run(201, OK).await, Err(ExchangeError::Unavailable(_))));
        assert!(matches!(run(200, "not json").await, Err(ExchangeError::Unavailable(_))));
        let missing = Err(ExchangeError::Unavailable("invalid token exchange response: missing required access_token".into()));
        assert_eq!(run(200, r#"{"token_type":"Bearer","issued_token_type":"x"}"#).await, missing);
        assert!(matches!(run(200, r#"{"access_token":"a","issued_token_type":"x"}"#).await, Err(ExchangeError::Unavailable(_))));
        assert!(matches!(run(200, r#"{"access_token":"a","token_type":"Bearer"}"#).await, Err(ExchangeError::Unavailable(_))));
        assert!(matches!(
            run(200, r#"{"access_token":"a","token_type":"n_a","issued_token_type":"x"}"#).await,
            Err(ExchangeError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn timeout_and_unreachable_are_unavailable() {
        let server = stub::serve(200, OK, Duration::from_secs(2)).await;
        let unavailable = Err(ExchangeError::Unavailable("token exchange unavailable".into()));
        assert_eq!(exchanger(server.url).exchange("opaque").await, unavailable);
        assert_eq!(exchanger("http://127.0.0.1:1/oauth/v2/token".into()).exchange("opaque").await, unavailable);
    }

    #[tokio::test]
    async fn propagates_trace_context_and_records_a_client_span() {
        let capture = testing::capture();
        let server = stub::serve(200, OK, Duration::ZERO).await;
        let parent = tracing::info_span!("parent");
        let trace_id = parent.context().span().span_context().trace_id();
        exchanger(server.url).exchange("opaque").instrument(parent.clone()).await.unwrap();
        drop(parent);
        let request = server.request.await.unwrap();
        assert!(request.contains(&format!("traceparent: 00-{trace_id}-")), "{request}");
        let spans = capture.finished_spans();
        let client = spans.iter().find(|span| span.span_context.trace_id() == trace_id && span.name == "POST /oauth/v2/token");
        let client = client.expect("client span");
        assert!(client.attributes.iter().all(|attribute| !attribute.value.as_str().contains("opaque")));
    }
}
