use std::sync::Arc;
use std::time::Duration;

use bnpb_common::bnpb::common::v1::View;
use tokio::time::{Instant, sleep, sleep_until, timeout_at};
use tonic::Status;

use crate::adapters::inbound::hazard::authorize;
use crate::core::models::Actor;
use crate::core::ports::{ActorVerifier, AuthError, TokenRenewer};

const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
pub struct SessionPolicy {
    /// How long before expiry the actor token is re-exchanged.
    pub renew_before: Duration,
    /// Upper bound on one stream, however often its token is renewed.
    pub max_stream: Duration,
}

/// Keeps a subscription authorized and resolves with the status that must end it. Without a
/// renewer the stream ends when the actor token expires. With one, the token is exchanged for
/// a fresh JWT shortly before expiry; Zitadel re-checking the token on every exchange is what
/// detects revocation while the stream is idle. Every failure path ends the stream (fail closed).
pub async fn supervise<V: ActorVerifier, R: TokenRenewer>(
    mut actor: Actor,
    view: View,
    verifier: Arc<V>,
    renewer: Option<Arc<R>>,
    policy: SessionPolicy,
) -> Status {
    let end_of_stream = Instant::now() + policy.max_stream;
    let Some(renewer) = renewer else {
        return wait_until(actor.expires_at, end_of_stream).await;
    };
    loop {
        let now = Instant::now();
        let remaining = actor.expires_at.saturating_duration_since(now);
        // Halfway through very short tokens, so a fresh token never causes a renewal storm.
        let renew_at = (actor.expires_at.checked_sub(policy.renew_before).unwrap_or(now)).max(now + remaining / 2);
        if renew_at >= end_of_stream {
            return wait_until(actor.expires_at, end_of_stream).await;
        }
        sleep_until(renew_at).await;
        actor = match renew(&actor, view, &*verifier, &*renewer).await {
            Ok(renewed) => renewed,
            Err(status) => return status,
        };
    }
}

async fn wait_until(expires_at: Instant, end_of_stream: Instant) -> Status {
    if end_of_stream < expires_at {
        sleep_until(end_of_stream).await;
        return Status::unauthenticated("stream lifetime reached; reconnect with a current token");
    }
    sleep_until(expires_at).await;
    Status::unauthenticated("access token expired")
}

/// Retries transient failures until the current token expires.
async fn renew<V: ActorVerifier, R: TokenRenewer>(actor: &Actor, view: View, verifier: &V, renewer: &R) -> Result<Actor, Status> {
    let mut backoff = RETRY_MIN;
    loop {
        let attempt = async {
            let token = renewer.renew(&actor.token).await?;
            verifier.verify(&token).await
        };
        let error = match timeout_at(actor.expires_at, attempt).await {
            Err(_) => return Err(Status::unauthenticated("access token expired")),
            Ok(Ok(renewed)) if renewed.subject != actor.subject => {
                return Err(Status::unauthenticated("renewed token belongs to another subject"));
            }
            Ok(Ok(renewed)) => {
                authorize(&renewed.scopes, view)?;
                tracing::debug!("actor token renewed");
                return Ok(renewed);
            }
            Ok(Err(AuthError::Rejected(reason))) => {
                tracing::info!(%reason, "actor token renewal rejected");
                return Err(Status::unauthenticated("session is no longer valid"));
            }
            Ok(Err(AuthError::Unavailable(reason))) => reason,
        };
        tracing::warn!(reason = %error, "actor token renewal failed; retrying");
        if Instant::now() + backoff >= actor.expires_at {
            sleep_until(actor.expires_at).await;
            return Err(Status::unauthenticated("access token expired"));
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(RETRY_MAX);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tonic::Code;

    use super::*;
    use crate::adapters::inbound::hazard::{SCOPE_FULL, SCOPE_SUMMARY};

    const TTL: Duration = Duration::from_secs(60);

    fn actor(subject: &str, scopes: &[&str]) -> Actor {
        Actor {
            subject: subject.into(),
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            expires_at: Instant::now() + TTL,
            token: format!("token-{subject}"),
        }
    }

    /// Hands out a fresh actor per verified token; the token text selects the subject/scopes.
    struct FakeVerifier;

    impl ActorVerifier for FakeVerifier {
        async fn verify(&self, token: &str) -> Result<Actor, AuthError> {
            Ok(match token {
                "other" => actor("someone-else", &[SCOPE_SUMMARY]),
                "media" => actor("user", &[SCOPE_SUMMARY]),
                _ => actor("user", &[SCOPE_SUMMARY, SCOPE_FULL]),
            })
        }
    }

    struct FakeRenewer {
        results: Mutex<VecDeque<Result<String, AuthError>>>,
        calls: AtomicUsize,
    }

    impl FakeRenewer {
        fn new(results: Vec<Result<&str, AuthError>>) -> Arc<Self> {
            let results = results.into_iter().map(|result| result.map(String::from)).collect();
            Arc::new(Self { results: Mutex::new(results), calls: AtomicUsize::new(0) })
        }
    }

    impl TokenRenewer for FakeRenewer {
        async fn renew(&self, _token: &str) -> Result<String, AuthError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.results.lock().unwrap().pop_front().unwrap_or(Err(AuthError::Rejected("exhausted".into())))
        }
    }

    fn policy(max_stream: u64) -> SessionPolicy {
        SessionPolicy { renew_before: Duration::from_secs(15), max_stream: Duration::from_secs(max_stream) }
    }

    async fn run(renewer: Option<Arc<FakeRenewer>>, view: View, max_stream: u64) -> (Status, Duration) {
        let started = Instant::now();
        let status =
            supervise(actor("user", &[SCOPE_SUMMARY, SCOPE_FULL]), view, Arc::new(FakeVerifier), renewer, policy(max_stream)).await;
        (status, started.elapsed())
    }

    #[tokio::test(start_paused = true)]
    async fn without_renewer_ends_at_expiry() {
        let (status, elapsed) = run(None, View::Full, 3600).await;
        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(elapsed, TTL);
    }

    #[tokio::test(start_paused = true)]
    async fn renews_before_expiry_until_rejected() {
        let renewer = FakeRenewer::new(vec![Ok("full"), Ok("full"), Err(AuthError::Rejected("revoked".into()))]);
        let (status, elapsed) = run(Some(renewer.clone()), View::Full, 3600).await;
        assert_eq!(status.code(), Code::Unauthenticated);
        // Renewals at 45s and 90s; the third, at 135s, is rejected.
        assert_eq!(elapsed, Duration::from_secs(135));
        assert_eq!(renewer.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn renewed_token_must_keep_subject_and_scopes() {
        let (status, _) = run(Some(FakeRenewer::new(vec![Ok("other")])), View::Summary, 3600).await;
        assert_eq!(status.code(), Code::Unauthenticated);
        let (status, elapsed) = run(Some(FakeRenewer::new(vec![Ok("media")])), View::Full, 3600).await;
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(elapsed, Duration::from_secs(45));
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failures_retry_then_fail_closed_at_expiry() {
        let unavailable = || Err(AuthError::Unavailable("down".into()));
        let (status, _) = run(Some(FakeRenewer::new(vec![unavailable(), Ok("full"), Ok("full")])), View::Full, 100).await;
        // Recovered on retry, then the 100s lifetime cap ends the stream.
        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(status.message(), "stream lifetime reached; reconnect with a current token");

        let renewer = FakeRenewer::new(vec![unavailable(); 100]);
        let (status, elapsed) = run(Some(renewer), View::Full, 3600).await;
        assert_eq!(status.message(), "access token expired");
        assert_eq!(elapsed, TTL);
    }

    #[tokio::test(start_paused = true)]
    async fn lifetime_cap_applies_with_renewal() {
        let renewer = FakeRenewer::new(vec![Ok("full"); 100]);
        let (status, elapsed) = run(Some(renewer), View::Full, 120).await;
        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(elapsed, Duration::from_secs(120));
    }
}
