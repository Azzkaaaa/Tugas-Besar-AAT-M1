mod authz;
mod config;
mod exchange;
mod telemetry;

use std::net::{Ipv4Addr, SocketAddr};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use envoy_types::ext_authz::v3::pb::AuthorizationServer;
use tonic::transport::Server;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as CheckStatus;
use tonic_health::pb::health_client::HealthClient;

use crate::authz::TokenExchangeAuthz;
use crate::config::Config;
use crate::exchange::Exchanger;

fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("--healthcheck") {
        return healthcheck();
    }
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("token-exchange: {error}");
            return ExitCode::FAILURE;
        }
    };
    let provider = telemetry::init();
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Tokio runtime");
    let result = runtime.block_on(run(config));
    drop(runtime);
    let _ = provider.shutdown();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "token-exchange stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: Config) -> Result<(), tonic::transport::Error> {
    let exchanger = Exchanger::new(config.token_url, config.requested_token_type, config.timeout, config.client);
    let authz = TokenExchangeAuthz { exchanger: Arc::new(exchanger) };
    // Credentials are loaded before this point, so the service is ready as soon as it listens.
    let (health, health_service) = tonic_health::server::health_reporter();
    health.set_serving::<AuthorizationServer<TokenExchangeAuthz>>().await;
    let address = SocketAddr::from((Ipv4Addr::UNSPECIFIED, config.port));
    tracing::info!(%address, "token-exchange listening");
    Server::builder()
        .add_service(health_service)
        .add_service(AuthorizationServer::new(authz))
        .serve_with_shutdown(address, shutdown_signal())
        .await
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}

/// Container healthcheck for the distroless image: standard gRPC health of the server.
fn healthcheck() -> ExitCode {
    let port = config::number("TOKEN_EXCHANGE_GRPC_PORT", 9000u16).unwrap_or(9000);
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("Tokio runtime");
    let serving = runtime.block_on(async {
        let check = async {
            let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}")).ok()?.connect().await.ok()?;
            let mut client = HealthClient::new(channel);
            let response = client.check(HealthCheckRequest { service: String::new() }).await.ok()?;
            Some(response.into_inner().status == CheckStatus::Serving as i32)
        };
        tokio::time::timeout(Duration::from_secs(3), check).await.ok().flatten().unwrap_or(false)
    });
    if serving { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}
