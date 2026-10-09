use std::sync::Arc;

use envoy_types::ext_authz::v3::pb::{Authorization, CheckRequest, CheckResponse, HeaderAppendAction, HttpStatusCode};
use envoy_types::ext_authz::v3::{CheckRequestExt, CheckResponseExt, DeniedHttpResponseBuilder, OkHttpResponseBuilder};
use tonic::{Request, Response, Status};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::exchange::{ExchangeError, Exchanger};
use crate::telemetry;

/// Envoy ext_authz (gRPC, v3) for the API routes: swaps the caller's opaque bearer token for the
/// exchanged JWT on the upstream request, or denies with the status the SPA expects (401 → refresh).
pub struct TokenExchangeAuthz {
    pub exchanger: Arc<Exchanger>,
}

#[tonic::async_trait]
impl Authorization for TokenExchangeAuthz {
    async fn check(&self, request: Request<CheckRequest>) -> Result<Response<CheckResponse>, Status> {
        let span = tracing::info_span!(
            "envoy.service.auth.v3.Authorization/Check",
            otel.kind = "server",
            rpc.system = "grpc",
            http.response.status_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty
        );
        let _ = span.set_parent(telemetry::grpc_parent(request.metadata()));
        let authorization = request.get_ref().get_client_headers().and_then(|headers| headers.get("authorization")).cloned();
        let response = match subject_token(authorization.as_deref()) {
            Err(reason) => deny(HttpStatusCode::Unauthorized, reason),
            Ok(subject) => match self.exchanger.exchange(subject).instrument(span.clone()).await {
                Ok(authorization) => allow(authorization),
                Err(ExchangeError::Rejected(reason)) => deny(HttpStatusCode::Unauthorized, &reason),
                Err(ExchangeError::Unavailable(reason)) => {
                    span.record("otel.status_code", "error");
                    deny(HttpStatusCode::BadGateway, &reason)
                }
            },
        };
        span.record("http.response.status_code", status_of(&response));
        Ok(Response::new(response))
    }
}

/// The bearer token Envoy received; cookie_to_bearer.lua has already moved the access cookie here.
fn subject_token(authorization: Option<&str>) -> Result<&str, &'static str> {
    let header = authorization.filter(|value| !value.is_empty()).ok_or("missing Authorization header")?;
    let token = header.strip_prefix("Bearer ").ok_or("invalid Authorization header")?;
    if token.is_empty() { Err("empty bearer token") } else { Ok(token) }
}

fn allow(authorization: String) -> CheckResponse {
    let mut upstream = OkHttpResponseBuilder::new();
    upstream.add_header("authorization", authorization, Some(HeaderAppendAction::OverwriteIfExistsOrAdd), false);
    let mut response = CheckResponse::with_status(Status::ok("token exchanged"));
    response.set_http_response(upstream);
    response
}

/// Envoy turns the denial into a local reply, which the gateway's problem+json mapper wraps.
fn deny(status: HttpStatusCode, reason: &str) -> CheckResponse {
    let mut denied = DeniedHttpResponseBuilder::new();
    denied.set_http_status(status).set_body(reason);
    let code = if status == HttpStatusCode::Unauthorized { Status::unauthenticated(reason) } else { Status::unavailable(reason) };
    let mut response = CheckResponse::with_status(code);
    response.set_http_response(denied);
    response
}

fn status_of(response: &CheckResponse) -> u16 {
    use envoy_types::ext_authz::v3::pb::HttpResponse;
    match &response.http_response {
        Some(HttpResponse::DeniedResponse(denied)) => denied.status.as_ref().map_or(403, |status| status.code as u16),
        _ => 200,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use envoy_types::ext_authz::v3::pb::HttpResponse;
    use envoy_types::pb::envoy::service::auth::v3::AttributeContext;
    use envoy_types::pb::envoy::service::auth::v3::attribute_context::{HttpRequest, Request as AttributeRequest};

    use super::*;
    use crate::config::ClientCredentials;
    use crate::exchange::stub;
    use crate::telemetry::testing;

    const OK: &str = r#"{"access_token":"jwt.value","token_type":"Bearer","issued_token_type":"urn:ietf:params:oauth:token-type:jwt"}"#;
    /// Only the tracing test sends a trace context: the capture is shared by every test.
    const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    fn authz(url: String) -> TokenExchangeAuthz {
        let client = ClientCredentials { client_id: "gateway".into(), client_secret: "s3cret".into() };
        let exchanger = Exchanger::new(url, "urn:ietf:params:oauth:token-type:jwt".into(), Duration::from_millis(500), client);
        TokenExchangeAuthz { exchanger: Arc::new(exchanger) }
    }

    fn check_request(authorization: Option<&str>) -> Request<CheckRequest> {
        let headers: HashMap<String, String> =
            authorization.map(|value| ("authorization".to_owned(), value.to_owned())).into_iter().collect();
        let http = HttpRequest { headers, ..Default::default() };
        let attributes =
            AttributeContext { request: Some(AttributeRequest { http: Some(http), ..Default::default() }), ..Default::default() };
        Request::new(CheckRequest { attributes: Some(attributes) })
    }

    fn denied(response: &CheckResponse) -> (i32, String) {
        match &response.http_response {
            Some(HttpResponse::DeniedResponse(denied)) => (denied.status.as_ref().unwrap().code, denied.body.clone()),
            other => panic!("expected a denial, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_credentials_are_denied_without_calling_zitadel() {
        let authz = authz("http://127.0.0.1:1/oauth/v2/token".into());
        for (header, reason) in [
            (None, "missing Authorization header"),
            (Some("Basic abc"), "invalid Authorization header"),
            (Some("Bearer "), "empty bearer token"),
        ] {
            let response = authz.check(check_request(header)).await.unwrap().into_inner();
            assert_eq!(denied(&response), (HttpStatusCode::Unauthorized as i32, reason.into()));
            assert_eq!(response.status.unwrap().code, tonic::Code::Unauthenticated as i32);
        }
    }

    #[tokio::test]
    async fn exchanged_token_overwrites_upstream_authorization() {
        let server = stub::serve(200, OK, Duration::ZERO).await;
        let response = authz(server.url).check(check_request(Some("Bearer opaque"))).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, tonic::Code::Ok as i32);
        let Some(HttpResponse::OkResponse(ok)) = response.http_response else { panic!("expected OK") };
        let header = ok.headers[0].header.as_ref().unwrap();
        assert_eq!((header.key.as_str(), header.value.as_str()), ("authorization", "Bearer jwt.value"));
        assert_eq!(ok.headers[0].append_action, HeaderAppendAction::OverwriteIfExistsOrAdd as i32);
        assert!(server.request.await.unwrap().contains("subject_token=opaque"));
    }

    #[tokio::test]
    async fn rejected_and_unavailable_map_to_401_and_502() {
        let server = stub::serve(400, r#"{"error":"invalid_grant"}"#, Duration::ZERO).await;
        let response = authz(server.url).check(check_request(Some("Bearer revoked"))).await.unwrap().into_inner();
        assert_eq!(denied(&response).0, HttpStatusCode::Unauthorized as i32);

        let server = stub::serve(503, "", Duration::ZERO).await;
        let response = authz(server.url).check(check_request(Some("Bearer opaque"))).await.unwrap().into_inner();
        assert_eq!(denied(&response), (HttpStatusCode::BadGateway as i32, "token exchange error: STS returned 503".into()));
        assert_eq!(response.status.unwrap().code, tonic::Code::Unavailable as i32);
    }

    #[tokio::test]
    async fn check_continues_envoys_trace() {
        let capture = testing::capture();
        let server = stub::serve(200, OK, Duration::ZERO).await;
        let mut check = check_request(Some("Bearer opaque"));
        check.metadata_mut().insert("traceparent", TRACEPARENT.parse().unwrap());
        authz(server.url).check(check).await.unwrap();
        let request = server.request.await.unwrap();
        assert!(request.contains("traceparent: 00-0af7651916cd43dd8448eb211c80319c-"), "{request}");
        let spans: Vec<_> = capture
            .finished_spans()
            .into_iter()
            .filter(|span| span.span_context.trace_id().to_string() == "0af7651916cd43dd8448eb211c80319c")
            .collect();
        let server_span = spans.iter().find(|span| span.name == "envoy.service.auth.v3.Authorization/Check").expect("server span");
        assert_eq!(server_span.parent_span_id.to_string(), "b7ad6b7169203331");
        let client_span = spans.iter().find(|span| span.name == "POST /oauth/v2/token").expect("client span");
        assert_eq!(client_span.parent_span_id, server_span.span_context.span_id());
    }
}
