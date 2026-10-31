mod adapters;
mod config;
mod core;
mod telemetry;

use std::net::{Ipv4Addr, SocketAddr};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use bnpb_feed::bnpb::feed::v1::event_feed_service_server::EventFeedServiceServer;
use tonic::transport::Server;
use tonic_health::ServingStatus;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as CheckStatus;
use tonic_health::pb::health_client::HealthClient;

use crate::adapters::inbound::grpc::FeedGrpc;
use crate::adapters::inbound::rabbitmq::{Consumer, FEED_SERVICE_NAME};
use crate::adapters::inbound::session::SessionPolicy;
use crate::adapters::outbound::memory_store::MemoryStore;
use crate::adapters::outbound::oidc::{JwksVerifier, Oidc, ZitadelTokenExchange};
use crate::adapters::outbound::redis_store::RedisStore;
use crate::adapters::outbound::store::Store;
use crate::config::Config;
use crate::core::feed::FeedService;

/// Open streams get this long to end after SIGTERM before the process exits anyway.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("--healthcheck") {
        return healthcheck();
    }
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("event-live-feed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let provider = telemetry::init();
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("Tokio runtime");
    let result = runtime.block_on(run(config));
    drop(runtime);
    if let Some(provider) = provider {
        let _ = provider.shutdown();
    }
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "event-live-feed stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: Config) -> Result<(), String> {
    let store = match &config.redis_url {
        Some(url) => Store::Redis(RedisStore::connect(url, config.replay_retention, config.replay_max_entries).await?),
        None => {
            tracing::warn!("FEED_REDIS_URL is not set; the replay buffer is in process memory (single replica only)");
            Store::Memory(MemoryStore::new(config.replay_retention, config.replay_max_entries))
        }
    };
    let feed = Arc::new(FeedService::new(Arc::new(store)));
    let oidc = Arc::new(Oidc::new(config.auth_authority));
    let verifier = Arc::new(JwksVerifier::new(oidc.clone()));
    let renewer = config.exchange_client.map(|client| Arc::new(ZitadelTokenExchange::new(oidc, client)));
    if renewer.is_none() {
        tracing::warn!("no feed token-exchange client configured; streams end when the actor token expires");
    }

    let (health, health_service) = tonic_health::server::health_reporter();
    health.set_service_status("", ServingStatus::NotServing).await;
    health.set_service_status(FEED_SERVICE_NAME, ServingStatus::NotServing).await;
    let consumer = tokio::spawn(
        Consumer {
            feed: feed.clone(),
            store_down: Default::default(),
            url: config.amqp_url,
            exchange: config.exchange,
            queue: config.queue,
            dead_letter_queue: config.dead_letter_queue,
            health,
        }
        .run(),
    );

    let grpc =
        FeedGrpc { feed, verifier, renewer, policy: SessionPolicy { renew_before: config.renew_before, max_stream: config.max_stream } };
    let address = SocketAddr::from((Ipv4Addr::UNSPECIFIED, config.port));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let mut server = tokio::spawn(
        Server::builder().add_service(health_service).add_service(EventFeedServiceServer::new(grpc)).serve_with_shutdown(address, async {
            let _ = stopped.await;
        }),
    );
    tracing::info!(%address, "event-live-feed listening");

    let result = tokio::select! {
        result = &mut server => result,
        () = shutdown_signal() => {
            tracing::info!("shutting down");
            let _ = stop.send(());
            // Graceful shutdown waits for streams, which only end at token expiry.
            match tokio::time::timeout(SHUTDOWN_GRACE, &mut server).await {
                Ok(result) => result,
                Err(_) => Ok(Ok(())),
            }
        }
    };
    consumer.abort();
    server.abort();
    result.unwrap_or(Ok(())).map_err(|error| error.to_string())
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
}

/// Container healthcheck for the distroless image: standard gRPC health of the feed service.
fn healthcheck() -> ExitCode {
    let port = std::env::var("EVENT_FEED_GRPC_PORT").ok().and_then(|port| port.parse::<u16>().ok()).unwrap_or(9093);
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("Tokio runtime");
    let serving = runtime.block_on(async {
        let check = async {
            let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}")).ok()?.connect().await.ok()?;
            let mut client = HealthClient::new(channel);
            let response = client.check(HealthCheckRequest { service: FEED_SERVICE_NAME.into() }).await.ok()?;
            Some(response.into_inner().status == CheckStatus::Serving as i32)
        };
        tokio::time::timeout(Duration::from_secs(3), check).await.ok().flatten().unwrap_or(false)
    });
    if serving { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}
