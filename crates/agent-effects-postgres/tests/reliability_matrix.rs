//! Required-service PostgreSQL fault matrix with caller-clock deterministic expiry.
#[path = "../../agent-effects/tests/support/reliability.rs"]
mod reliability;

use agent_effects_postgres::{ClockSource, PostgresOptions, PostgresStore};

fn url() -> Option<String> {
    let url = std::env::var("AGENT_EFFECTS_POSTGRES_URL").ok();
    assert!(
        url.is_some() || std::env::var_os("AGENT_EFFECTS_REQUIRE_POSTGRES").is_none(),
        "PostgreSQL is required"
    );
    if url.is_none() {
        eprintln!("skipped: set AGENT_EFFECTS_POSTGRES_URL");
    }
    url
}

async fn fresh(url: &str) -> PostgresStore {
    PostgresOptions::new()
        .schema(format!(
            "fault_{}",
            agent_effects::EffectId::new().to_string().replace('-', "")
        ))
        .connect(url)
        .await
        .unwrap()
        .with_clock_source(ClockSource::Caller)
}

#[tokio::test]
async fn postgres_storage_failure_matrix() {
    let Some(url) = url() else { return };
    reliability::storage_matrix(|| fresh(&url)).await;
}

#[tokio::test]
async fn postgres_compensation_storage_failure_matrix() {
    let Some(url) = url() else { return };
    reliability::compensation_storage_matrix(|| fresh(&url)).await;
}

#[tokio::test]
async fn postgres_reconnect_and_simultaneous_recovery() {
    let Some(url) = url() else { return };
    reliability::reconnect_with_simultaneous_recovery(|| fresh(&url)).await;
}

#[tokio::test]
async fn closed_driver_pool_after_external_success_recovers_on_reconnect() {
    use agent_effects::testkit::FakeRemote;
    use agent_effects::{EffectHandler, EffectStatus, EffectStore, ManualClock, RuntimeError};
    use reliability::{Durable, Protection, TTL};
    use std::sync::Arc;
    let Some(url) = url() else { return };
    for protection in [Protection::None, Protection::Idempotent] {
        let options = PostgresOptions::new().schema(format!(
            "closed_{}",
            agent_effects::EffectId::new().to_string().replace('-', "")
        ));
        let store = options
            .connect(&url)
            .await
            .unwrap()
            .with_clock_source(ClockSource::Caller);
        let clock = Arc::new(ManualClock::default());
        let remote = FakeRemote::new(clock.clone());
        let rt = reliability::runtime(
            store.clone(),
            clock.clone(),
            remote.clone(),
            protection,
            "disconnected",
        );
        let result = rt
            .effect(Durable::NAME, "closed")
            .input(&"closed")
            .remote_idempotency(protection == Protection::Idempotent)
            .run({
                let (store, remote) = (store.clone(), remote.clone());
                move |ctx| {
                    let (store, remote) = (store.clone(), remote.clone());
                    async move {
                        let value = remote
                            .create(
                                "closed",
                                (protection == Protection::Idempotent)
                                    .then(|| ctx.idempotency_key()),
                            )
                            .await?;
                        store.pool().close().await;
                        Ok::<_, agent_effects::EffectFailure>(value)
                    }
                }
            })
            .await;
        assert!(matches!(result, Err(RuntimeError::Store(_))));
        assert!(store.get_by_key(&reliability::key("closed")).await.is_err());
        clock.advance(TTL + std::time::Duration::from_secs(1));
        let reopened = options
            .connect(&url)
            .await
            .unwrap()
            .with_clock_source(ClockSource::Caller);
        let rt = reliability::runtime(
            reopened.clone(),
            clock,
            remote.clone(),
            protection,
            "reconnected",
        );
        rt.recover().await.unwrap();
        let record = reliability::record(&reopened, "closed").await;
        assert_eq!(
            record.status,
            if protection == Protection::Idempotent {
                EffectStatus::Committed
            } else {
                EffectStatus::NeedsIntervention
            }
        );
        assert_eq!(remote.applications("closed"), 1);
        reliability::assert_not_falsely_failed(&reopened, record.id).await;
    }
}
