mod claims;
mod config;
mod signature;

use std::{
    process::ExitCode,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    signal::unix::{SignalKind, signal},
};

use config::Config;

fn problem(status: StatusCode, code: &str) -> Response {
    let body = json!({ "type": format!("urn:bnpb:claims:{code}"), "title": code, "status": status.as_u16(), "detail": code });
    (status, [(header::CONTENT_TYPE, "application/problem+json")], body.to_string()).into_response()
}

/// Zitadel Actions v2 REST_CALL target for the preuserinfo/preaccesstoken functions.
async fn claims(State(config): State<Arc<Config>>, headers: HeaderMap, body: Bytes) -> Response {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_secs());
    let signature = headers.get("zitadel-signature").and_then(|value| value.to_str().ok());
    if !signature::verify(&body, signature, &config.signing_key, now) {
        return problem(StatusCode::UNAUTHORIZED, "invalid_signature");
    }
    match serde_json::from_slice::<Value>(&body) {
        Ok(payload) => Json(claims::claims_for(&payload, &config)).into_response(),
        Err(_) => problem(StatusCode::BAD_REQUEST, "invalid_payload"),
    }
}

async fn log(request: Request, next: Next) -> Response {
    let (method, path, start) = (request.method().to_string(), request.uri().path().to_owned(), Instant::now());
    let response = next.run(request).await;
    println!(
        "{}",
        json!({ "event": "http_request", "method": method, "path": path,
        "status": response.status().as_u16(), "duration_ms": start.elapsed().as_millis() as u64 })
    );
    response
}

fn router(config: Arc<Config>) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({ "status": "ok" })) }))
        .route("/claims", post(claims))
        .fallback(|| async { problem(StatusCode::NOT_FOUND, "not_found") })
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(middleware::from_fn(log))
        .with_state(config)
}

/// Container healthcheck for the distroless image, which has no curl or wget.
async fn healthcheck() -> ExitCode {
    let check = async {
        let mut stream = TcpStream::connect(("127.0.0.1", config::port().ok()?)).await.ok()?;
        stream.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await.ok()?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.ok()?;
        response.starts_with(b"HTTP/1.1 200").then_some(())
    };
    match tokio::time::timeout(std::time::Duration::from_secs(3), check).await {
        Ok(Some(())) => ExitCode::SUCCESS,
        _ => ExitCode::FAILURE,
    }
}

async fn shutdown() {
    let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("--healthcheck") {
        return healthcheck().await;
    }
    let config = match config::provisioned().await {
        Ok(config) => Arc::new(config),
        Err(error) => {
            // Never print configuration values; the file holds the signing key.
            eprintln!("Claims service could not start: {error}");
            return ExitCode::FAILURE;
        }
    };
    let port = config.port;
    let listener = match TcpListener::bind(("0.0.0.0", port)).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("Claims service could not listen on port {port}: {error}");
            return ExitCode::FAILURE;
        }
    };
    println!("{}", json!({ "event": "started", "port": port }));
    match axum::serve(listener, router(config)).with_graceful_shutdown(shutdown()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Claims service stopped: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    fn now() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    async fn post(body: &'static str, signature: Option<String>) -> (StatusCode, Value) {
        let config = Arc::new(Config {
            signing_key: "signing-test".into(),
            client_ids: vec!["bridge-test".into(), "api-test".into(), "gateway-test".into()],
            project_id: "project-test".into(),
            organization_id: "org-test".into(),
            port: 0,
        });
        let mut request = Request::post("/claims").header(header::CONTENT_TYPE, "application/json");
        if let Some(signature) = signature {
            request = request.header("ZITADEL-Signature", signature);
        }
        let response = router(config).oneshot(request.body(Body::from(body)).unwrap()).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn claims_endpoint_requires_a_valid_zitadel_signature() {
        const BODY: &str = r#"{"application":{"client_id":"gateway-test"},"user_grants":[{"project_id":"project-test","resource_owner":"org-test","state":1,"roles":["MEDIA"]}]}"#;
        let (status, body) = post(BODY, Some(signature::sign(BODY.as_bytes(), "signing-test", now()))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({ "append_claims": [{ "key": "role", "value": "MEDIA" },
            { "key": "scopes", "value": ["hazards:read:summary", "notifications:read", "webhooks:read"] }] })
        );
        assert_eq!(post(BODY, None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(post(BODY, Some(signature::sign(BODY.as_bytes(), "wrong-key", now()))).await.0, StatusCode::UNAUTHORIZED);
        let (status, body) = post(BODY, Some(signature::sign(BODY.as_bytes(), "signing-test", now() - 600))).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["type"], "urn:bnpb:claims:invalid_signature");
    }

    #[tokio::test]
    async fn signed_malformed_payload_is_rejected() {
        const BODY: &str = "{not json";
        let (status, body) = post(BODY, Some(signature::sign(BODY.as_bytes(), "signing-test", now()))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["title"], "invalid_payload");
    }
}
